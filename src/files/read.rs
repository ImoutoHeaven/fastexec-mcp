use super::path::{display_path, open_regular, resolve, with_path_workspace};
use super::{Outcome, ReadRequest};
use crate::lease::{BytePool, Lease};
use crate::media::{self, CompressFailure, DetectedMedia, MediaKind};
use crate::session::LaunchContext;
use serde_json::{Value, json};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;

pub(super) const MAX_LINES: u64 = 2000;
/// The most bytes a direct result shows.
pub(super) const MAX_BYTES: u64 = 50 * 1024;
/// The most bytes a nested codemode read shows.
pub(super) const NESTED_MAX_BYTES: u64 = 4 << 20;
/// How much of a file is sniffed before deciding whether it is media or text.
pub(super) const SNIFF: usize = media::SNIFF;
pub(super) const MEDIA_READ_CHUNK: usize = 64 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReadLimits {
    max_lines: u64,
    max_bytes: u64,
}

impl ReadLimits {
    pub(crate) const DIRECT: Self = Self {
        max_lines: MAX_LINES,
        max_bytes: MAX_BYTES,
    };
    pub(crate) const NESTED: Self = Self {
        max_lines: u64::MAX,
        max_bytes: NESTED_MAX_BYTES,
    };

    /// How much of one line is kept while it is read: a line that fits whole, and one byte more,
    /// so a longer line is known not to fit.
    const fn keep(self) -> usize {
        self.max_bytes as usize + 1
    }
}

pub(super) enum ReadFailure {
    Error(String),
    Overload(String),
}

impl From<String> for ReadFailure {
    fn from(message: String) -> Self {
        if message.starts_with("Server active memory budget (") {
            Self::Overload(message)
        } else {
            Self::Error(message)
        }
    }
}

pub(super) const TEXT_BLOCK: usize = 8 * 1024;
pub(super) const TEXT_FIXED_WORKSPACE: usize = 512 * 1024;

pub(super) struct TextWorkspace {
    base: usize,
    limit: usize,
    lease: Lease,
}

impl TextWorkspace {
    #[cfg(test)]
    pub(super) fn new(pool: &Arc<BytePool>, path_bytes: usize) -> Result<Self, String> {
        Self::with_base(
            pool,
            TEXT_FIXED_WORKSPACE.saturating_add(path_bytes.saturating_mul(32)),
        )
    }

    fn after_path(pool: &Arc<BytePool>) -> Result<Self, String> {
        // The path owner already accounts for the sniff buffer and path copies.
        Self::with_base(pool, TEXT_FIXED_WORKSPACE.saturating_sub(SNIFF))
    }

    fn with_base(pool: &Arc<BytePool>, base: usize) -> Result<Self, String> {
        // At most one 8 KiB block plus a three-byte UTF-8 carry is decoded.
        // The two Strings and two replacement-position vectors, including old
        // allocations during growth, fit (16 + 5 * sizeof(usize)) * (B + 3) + B.
        // 512 KiB also covers sniffing, the 64 KiB whole-file chunk, and notices.
        let mut workspace = Self {
            base,
            limit: pool.limit(),
            lease: Lease::empty(pool),
        };
        workspace.reserve(0)?;
        Ok(workspace)
    }

    fn reserve(&mut self, retained: usize) -> Result<(), String> {
        // Growing line and window buffers, render copies, and structured text
        // coexist. Charge before each growth; retain the largest admitted peak.
        let target = self.base.saturating_add(retained.saturating_mul(16));
        if target > self.lease.bytes() && !self.lease.try_resize(target) {
            return Err(format!(
                "Server active memory budget ({} bytes) is full while reading file text; retry once in-flight transfers complete.",
                self.limit
            ));
        }
        Ok(())
    }
}

/// The text of one file, cut to the window and the limits a result may show.
#[cfg(test)]
pub(super) fn read_text(
    request: &ReadRequest,
    launch: &LaunchContext,
    transfer: bool,
    limits: ReadLimits,
) -> Result<Outcome, String> {
    read_cancellable(request, launch, transfer, limits, None)
        .run(|read| read)
        .map_err(|failure| match failure {
            ReadFailure::Error(msg) | ReadFailure::Overload(msg) => msg,
        })
}

pub(super) fn count_lines_streaming(
    file: &mut File,
    shown_path: &str,
    cancel: Option<&tokio_util::sync::CancellationToken>,
) -> Result<u64, String> {
    file.seek(SeekFrom::Start(0))
        .map_err(|error| format!("Cannot read {shown_path}: {error}."))?;
    let mut buffer = vec![0_u8; 64 * 1024];
    let mut newlines = 0_u64;
    let mut total_bytes = 0_u64;
    let mut last_byte = b'\n';
    loop {
        if cancel.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
            return Err("Cancelled while reading file.".into());
        }
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("Cannot read {shown_path}: {error}."))?;
        if read == 0 {
            break;
        }
        let chunk = &buffer[..read];
        newlines += bytecount_lf(chunk);
        total_bytes += read as u64;
        last_byte = chunk[read - 1];
    }
    Ok(newlines + u64::from(total_bytes > 0 && last_byte != b'\n'))
}

fn bytecount_lf(bytes: &[u8]) -> u64 {
    bytes.iter().filter(|&&b| b == b'\n').count() as u64
}

pub(crate) enum MediaStart<'a> {
    Prefix(&'a [u8]),
    Rewind,
}

pub(crate) enum MediaBytes {
    Loaded { bytes: Vec<u8>, lease: Lease },
    Oversized { exact_source_bytes: Option<u64> },
}

#[derive(Debug)]
pub(crate) enum MediaReadError {
    Io(io::Error),
    Cancelled,
    Overload,
}

pub(super) const BASE_MEDIA_FRAME_OVERHEAD: usize = 1024;

pub(super) fn media_frame_overhead(path_len: usize) -> usize {
    BASE_MEDIA_FRAME_OVERHEAD.saturating_add(path_len.saturating_mul(10))
}

pub(super) fn peak_media_lease(
    raw_len: usize,
    raw_capacity: usize,
    frame_overhead: Option<usize>,
) -> Option<usize> {
    if let Some(overhead) = frame_overhead {
        let b64 = raw_len.checked_add(2)?.checked_div(3)?.checked_mul(4)?;
        let read_peak = raw_capacity.checked_add(b64)?;
        let outbound_peak = b64.checked_mul(2)?.checked_add(overhead)?;
        Some(read_peak.max(outbound_peak))
    } else {
        Some(raw_capacity)
    }
}

/// Reads a regular file from byte 0 (either reusing an already-read prefix or rewinding via
/// `seek(0)`), bounded to `max_media_bytes + 1` total bytes so growth past the limit is detected
/// without loading the rest of the file.
pub(crate) fn read_media_from_zero(
    reader: &mut (impl Read + Seek),
    initial_len: u64,
    start: MediaStart<'_>,
    max_media_bytes: u64,
    pool: &Arc<BytePool>,
    frame_overhead: Option<usize>,
    cancel: Option<&tokio_util::sync::CancellationToken>,
) -> Result<MediaBytes, MediaReadError> {
    if initial_len > max_media_bytes {
        return Ok(MediaBytes::Oversized {
            exact_source_bytes: Some(initial_len),
        });
    }
    let max = usize::try_from(max_media_bytes).unwrap_or(usize::MAX - 1);
    let prefix_len = match start {
        MediaStart::Prefix(prefix) => prefix.len(),
        MediaStart::Rewind => 0,
    };
    if prefix_len > max {
        return Ok(MediaBytes::Oversized {
            exact_source_bytes: None,
        });
    }
    let expected = (initial_len as usize).max(prefix_len);
    let chunk_len = MEDIA_READ_CHUNK.min(max + 1);
    let initial_peak = peak_media_lease(expected, expected, frame_overhead)
        .and_then(|peak| peak.checked_add(chunk_len))
        .ok_or(MediaReadError::Overload)?;
    let mut lease = pool
        .try_acquire(initial_peak)
        .ok_or(MediaReadError::Overload)?;
    let mut raw = Vec::with_capacity(expected);
    let mut remaining = match start {
        MediaStart::Prefix(prefix) => {
            raw.extend_from_slice(prefix);
            (max + 1) - prefix.len()
        }
        MediaStart::Rewind => {
            if let Err(err) = reader.seek(SeekFrom::Start(0)) {
                drop(raw);
                drop(lease);
                return Err(MediaReadError::Io(err));
            }
            max + 1
        }
    };
    let mut chunk = vec![0_u8; chunk_len];
    while remaining > 0 {
        if cancel.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
            drop(chunk);
            drop(raw);
            drop(lease);
            return Err(MediaReadError::Cancelled);
        }
        let want = remaining.min(chunk.len());
        let read = match reader.read(&mut chunk[..want]) {
            Ok(0) => break,
            Ok(n) => n,
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => {
                drop(chunk);
                drop(raw);
                drop(lease);
                return Err(MediaReadError::Io(err));
            }
        };
        let next_len = raw.len() + read;
        if next_len > max {
            drop(chunk);
            drop(raw);
            drop(lease);
            return Ok(MediaBytes::Oversized {
                exact_source_bytes: None,
            });
        }
        if next_len > raw.capacity() {
            let preferred = raw.capacity().saturating_mul(2).max(next_len).min(max);
            let mut admitted = None;
            for capacity in [preferred, next_len] {
                let needed = peak_media_lease(next_len, capacity, frame_overhead)
                    .map(|peak| peak.max(raw.capacity().saturating_add(capacity)))
                    .and_then(|peak| peak.checked_add(chunk.capacity()));
                if needed.is_some_and(|peak| lease.try_resize(peak)) {
                    admitted = Some(capacity);
                    break;
                }
            }
            let Some(capacity) = admitted else {
                drop(chunk);
                drop(raw);
                drop(lease);
                return Err(MediaReadError::Overload);
            };
            // Both allocations remain charged during replacement.
            let mut replacement = Vec::with_capacity(capacity);
            replacement.extend_from_slice(&raw);
            raw = replacement;
        } else {
            let needed = peak_media_lease(next_len, raw.capacity(), frame_overhead)
                .and_then(|peak| peak.checked_add(chunk.capacity()));
            if !needed.is_some_and(|peak| lease.try_resize(peak)) {
                drop(chunk);
                drop(raw);
                drop(lease);
                return Err(MediaReadError::Overload);
            }
        }
        raw.extend_from_slice(&chunk[..read]);
        remaining -= read;
    }
    drop(chunk);
    let final_peak =
        peak_media_lease(raw.len(), raw.capacity(), frame_overhead).unwrap_or(lease.bytes());
    lease.shrink_to(final_peak);
    Ok(MediaBytes::Loaded { bytes: raw, lease })
}

fn action_hint(transfer: bool) -> &'static str {
    if transfer {
        " Inspect it with exec or download it with transfer."
    } else {
        " Inspect it with exec."
    }
}

fn oversized_note(
    kind: MediaKind,
    shown_path: &str,
    mime: &str,
    source_bytes: Option<u64>,
    max_media_bytes: u64,
    transfer: bool,
) -> String {
    let hint = action_hint(transfer);
    match source_bytes {
        Some(size) => format!(
            "Cannot attach {} file {shown_path} [{mime}]: its size ({size} bytes) exceeds FASTEXEC_MAX_MEDIA_BYTES ({max_media_bytes} bytes).{hint}",
            kind.as_str()
        ),
        None => format!(
            "Cannot attach {} file {shown_path} [{mime}]: it grew past FASTEXEC_MAX_MEDIA_BYTES ({max_media_bytes} bytes) while reading.{hint}",
            kind.as_str()
        ),
    }
}

fn unattached_outcome(
    kind: MediaKind,
    shown_path: String,
    mime: String,
    source_bytes: Option<u64>,
    note: String,
    nested: bool,
) -> Outcome {
    let flag_key = if nested { "attachable" } else { "attached" };
    let mut map = serde_json::Map::new();
    map.insert("ok".into(), json!(true));
    map.insert("kind".into(), json!(kind.as_str()));
    map.insert("path".into(), json!(shown_path));
    map.insert("mimeType".into(), json!(mime));
    map.insert(flag_key.into(), json!(false));
    if let Some(size) = source_bytes {
        map.insert("sourceBytes".into(), json!(size));
    }
    map.insert("note".into(), json!(note.clone()));
    Outcome {
        owner: Default::default(),
        notice: note.clone(),
        text: note,
        body: None,
        attachment: None,
        lease: None,
        structured: Value::Object(map),
    }
}

#[allow(clippy::too_many_arguments)]
fn read_media_outcome(
    file: &mut File,
    initial_len: u64,
    prefix: &[u8],
    resolved: &Path,
    shown_path: String,
    detected: DetectedMedia,
    launch: &LaunchContext,
    transfer: bool,
    limits: ReadLimits,
    cancel: Option<&tokio_util::sync::CancellationToken>,
) -> Result<Outcome, ReadFailure> {
    let max_media_bytes = launch.max_media_bytes().map_err(ReadFailure::Error)?;
    let compress = launch.image_compress().map_err(ReadFailure::Error)?;
    if limits == ReadLimits::NESTED {
        if initial_len > max_media_bytes {
            let exact = file
                .metadata()
                .ok()
                .map(|m| m.len())
                .filter(|&len| len == initial_len);
            let note = oversized_note(
                detected.kind,
                &shown_path,
                detected.mime,
                exact,
                max_media_bytes,
                transfer,
            );
            return Ok(unattached_outcome(
                detected.kind,
                shown_path,
                detected.mime.to_owned(),
                exact,
                note,
                true,
            ));
        }
        let structured = json!({
            "ok": true,
            "kind": detected.kind.as_str(),
            "path": shown_path,
            "mimeType": detected.mime,
            "bytes": initial_len,
            "attachable": true,
        });
        let summary = format!(
            "Read {} file [{}] ({} B)",
            detected.kind.as_str(),
            detected.mime,
            initial_len
        );
        return Ok(Outcome {
            owner: Default::default(),
            notice: summary.clone(),
            text: summary,
            body: None,
            attachment: None,
            lease: None,
            structured,
        });
    }

    let frame_overhead = media_frame_overhead(shown_path.len().max(resolved.as_os_str().len()));
    let loaded = match read_media_from_zero(
        file,
        initial_len,
        MediaStart::Prefix(prefix),
        max_media_bytes,
        launch.memory(),
        Some(frame_overhead),
        cancel,
    ) {
        Ok(MediaBytes::Loaded { bytes, lease }) => (bytes, lease),
        Ok(MediaBytes::Oversized { exact_source_bytes }) => {
            let exact = exact_source_bytes
                .filter(|&len| file.metadata().ok().map(|m| m.len()) == Some(len));
            let note = oversized_note(
                detected.kind,
                &shown_path,
                detected.mime,
                exact,
                max_media_bytes,
                transfer,
            );
            return Ok(unattached_outcome(
                detected.kind,
                shown_path,
                detected.mime.to_owned(),
                exact,
                note,
                false,
            ));
        }
        Err(MediaReadError::Io(error)) => {
            return Err(ReadFailure::Error(format!(
                "Cannot read {shown_path}: {error}."
            )));
        }
        Err(MediaReadError::Cancelled) => {
            return Err(ReadFailure::Error("Cancelled while reading file.".into()));
        }
        Err(MediaReadError::Overload) => {
            return Err(ReadFailure::Overload(format!(
                "Cannot read {shown_path}: server active memory budget ({} bytes) is full; retry once in-flight transfers complete.",
                launch.memory().limit()
            )));
        }
    };

    let (raw, mut lease) = loaded;
    if compress
        && detected.kind == MediaKind::Image
        && media::is_compressible_image(detected.mime, &raw)
    {
        let source_len = raw.len() as u64;
        match media::compress_image(raw, detected.mime, &mut lease) {
            Ok(compressed) => {
                let sent_bytes = compressed.bytes.len() as u64;
                let b64_len = compressed.bytes.len().div_ceil(3) * 4;
                let outbound_peak = b64_len.saturating_mul(2).saturating_add(frame_overhead);
                let needed = compressed
                    .bytes
                    .capacity()
                    .saturating_add(b64_len)
                    .max(outbound_peak);
                if needed > lease.bytes() && !lease.try_resize(needed) {
                    drop(compressed);
                    drop(lease);
                    return Err(ReadFailure::Overload(format!(
                        "Cannot read {shown_path}: server active memory budget ({} bytes) is full; retry once in-flight transfers complete.",
                        launch.memory().limit()
                    )));
                }
                let encoded = media::encode_base64(&compressed.bytes);
                drop(compressed.bytes);
                lease.shrink_to(outbound_peak);
                let mut text = format!("Read image file [{}] ({sent_bytes} B)", compressed.mime);
                for hint in &compressed.hints {
                    text.push('\n');
                    text.push_str(hint);
                }
                let mut map = serde_json::Map::new();
                map.insert("ok".into(), json!(true));
                map.insert("kind".into(), json!("image"));
                map.insert("path".into(), json!(shown_path));
                map.insert("mimeType".into(), json!(compressed.mime.clone()));
                map.insert("bytes".into(), json!(sent_bytes));
                map.insert("attached".into(), json!(true));
                if compressed.transformed {
                    map.insert("originalBytes".into(), json!(compressed.original_bytes));
                    map.insert("originalWidth".into(), json!(compressed.original_width));
                    map.insert("originalHeight".into(), json!(compressed.original_height));
                    map.insert("width".into(), json!(compressed.width));
                    map.insert("height".into(), json!(compressed.height));
                }
                let block =
                    media::content_block(MediaKind::Image, &compressed.mime, encoded, resolved);
                return Ok(Outcome {
                    owner: Default::default(),
                    notice: text.clone(),
                    text,
                    body: None,
                    attachment: Some(block),
                    lease: Some(lease),
                    structured: Value::Object(map),
                });
            }
            Err(CompressFailure::Overload) => {
                drop(lease);
                return Err(ReadFailure::Overload(format!(
                    "Cannot read {shown_path}: server active memory budget ({} bytes) is full; retry once in-flight transfers complete.",
                    launch.memory().limit()
                )));
            }
            Err(CompressFailure::Rejected(reason)) => {
                drop(lease);
                let hint = action_hint(transfer);
                let note = format!(
                    "Cannot attach image file {shown_path} [{}] ({source_len} bytes): {reason}{hint}",
                    detected.mime
                );
                return Ok(unattached_outcome(
                    MediaKind::Image,
                    shown_path,
                    detected.mime.to_owned(),
                    Some(source_len),
                    note,
                    false,
                ));
            }
        }
    }

    let sent_bytes = raw.len() as u64;
    let encoded = media::encode_base64(&raw);
    drop(raw);
    let outbound_peak = encoded
        .len()
        .saturating_mul(2)
        .saturating_add(frame_overhead);
    lease.shrink_to(outbound_peak);
    let text = format!(
        "Read {} file [{}] ({sent_bytes} B)",
        detected.kind.as_str(),
        detected.mime
    );
    let block = media::content_block(detected.kind, detected.mime, encoded, resolved);
    let structured = json!({
        "ok": true,
        "kind": detected.kind.as_str(),
        "path": shown_path,
        "mimeType": detected.mime,
        "bytes": sent_bytes,
        "attached": true,
    });
    Ok(Outcome {
        owner: Default::default(),
        notice: text.clone(),
        text,
        body: None,
        attachment: Some(block),
        lease: Some(lease),
        structured,
    })
}

pub(super) fn read_cancellable(
    request: &ReadRequest,
    launch: &LaunchContext,
    transfer: bool,
    limits: ReadLimits,
    cancel: Option<&tokio_util::sync::CancellationToken>,
) -> crate::ipc::Owned<Result<Outcome, ReadFailure>> {
    match with_path_workspace(&request.path, launch, SNIFF, |path_lease| {
        read_cancellable_reserved(request, launch, transfer, limits, cancel, path_lease)
    }) {
        Ok(mut read) => {
            if let Ok(outcome) = &mut read.value {
                outcome.owner = read.owner.clone();
            }
            read
        }
        Err(message) => {
            crate::ipc::Owned::new(Err(ReadFailure::Overload(message)), Default::default())
        }
    }
}

fn read_cancellable_reserved(
    request: &ReadRequest,
    launch: &LaunchContext,
    transfer: bool,
    limits: ReadLimits,
    cancel: Option<&tokio_util::sync::CancellationToken>,
    path_lease: &mut Lease,
) -> Result<Outcome, ReadFailure> {
    if cancel.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
        return Err(ReadFailure::Error("Cancelled while reading file.".into()));
    }
    if request.offset == Some(0) {
        return Err(ReadFailure::Error(
            "offset must be non-zero (1-indexed from start, or negative from end).".into(),
        ));
    }
    if request.limit == Some(0) {
        return Err(ReadFailure::Error("limit must be at least 1.".into()));
    }
    let mut workspace;
    let resolved = resolve(&request.path, launch).map_err(ReadFailure::Error)?;
    let shown_path = display_path(&resolved);
    let (mut file, metadata) = open_regular(&resolved).map_err(|error| {
        if error.kind() == io::ErrorKind::IsADirectory {
            ReadFailure::Error(format!(
                "Cannot read {shown_path}: {error}; list it with exec."
            ))
        } else {
            ReadFailure::Error(format!("Cannot read {shown_path}: {error}."))
        }
    })?;
    let mut sniff = vec![0u8; SNIFF];
    let sniffed = read_upto(&mut file, &mut sniff)
        .map_err(|error| ReadFailure::Error(format!("Cannot read {shown_path}: {error}.")))?;
    if let Some(detected) = media::detect_path(&sniff[..sniffed], Some(&resolved)) {
        // Invalid media configuration may be echoed in a diagnostic. Reserve its
        // borrowed native text before the validator formats that error.
        let config_bytes = [
            crate::session::IMAGE_COMPRESS,
            crate::session::MAX_MEDIA_BYTES,
        ]
        .iter()
        .filter_map(|name| launch.var(name))
        .map(|value| value.as_encoded_bytes().len())
        .fold(0, usize::saturating_add);
        if !path_lease.try_grow(config_bytes.saturating_mul(16)) {
            return Err(ReadFailure::Overload("Server active memory budget is full while preparing media configuration diagnostics.".into()));
        }
        return read_media_outcome(
            &mut file,
            metadata.len(),
            &sniff[..sniffed],
            &resolved,
            shown_path,
            detected,
            launch,
            transfer,
            limits,
            cancel,
        );
    }
    if sniff[..sniffed].contains(&0) {
        let hint = action_hint(transfer);
        return Err(ReadFailure::Error(format!(
            "{shown_path} is a binary file ({} bytes).{hint}",
            metadata.len()
        )));
    }
    workspace = TextWorkspace::after_path(launch.memory())?;
    if limits == ReadLimits::NESTED && request.offset.is_none() && request.limit.is_none() {
        let mut outcome = read_whole_utf8(
            &mut file,
            metadata.len(),
            &sniff[..sniffed],
            &shown_path,
            limits.max_bytes,
            cancel,
            &mut workspace,
        )
        .map_err(ReadFailure::from)?;
        outcome.lease = Some(workspace.lease);
        return Ok(outcome);
    }
    let tail = if let Some(offset) = request.offset.filter(|off| *off < 0) {
        Some((
            offset.unsigned_abs(),
            count_lines_streaming(&mut file, &shown_path, cancel).map_err(ReadFailure::Error)?,
        ))
    } else {
        None
    };
    let mut scan = |start_line| {
        file.rewind()
            .map_err(|error| format!("Cannot read {shown_path}: {error}."))?;
        Window::scan(
            Source::new(BufReader::with_capacity(TEXT_BLOCK, &mut file), cancel),
            start_line,
            request.limit,
            limits,
            &mut workspace,
        )
    };
    let (start_line, window) = if let Some((distance, total)) = tail {
        scan_from_end(distance, total, &shown_path, scan).map_err(ReadFailure::from)?
    } else {
        let start = request.offset.unwrap_or(1) as u64;
        let window = scan(start).map_err(ReadFailure::from)?;
        if start > 1 && start > window.total_lines {
            return Err(ReadFailure::Error(format!(
                "Offset {start} is beyond end of file ({} lines total)",
                window.total_lines
            )));
        }
        (start, window)
    };
    let mut outcome = window.render(start_line, request.limit, &shown_path, limits);
    outcome.lease = Some(workspace.lease);
    Ok(outcome)
}

/// A successful tail slice must start at the offset implied by its own observed line count.
pub(super) fn scan_from_end(
    distance: u64,
    total: u64,
    path: &str,
    mut scan: impl FnMut(u64) -> Result<Window, String>,
) -> Result<(u64, Window), String> {
    let from_end = |lines: u64| lines.saturating_sub(distance) + 1;
    let mut start = from_end(total);
    for _ in 0..2 {
        let window = scan(start)?;
        let observed_start = from_end(window.total_lines);
        if start == observed_start {
            return Ok((start, window));
        }
        start = observed_start;
    }
    Err(format!(
        "Cannot read {path}: the file changed while reading; retry."
    ))
}

pub(super) fn read_whole_utf8(
    file: &mut File,
    initial_len: u64,
    prefix: &[u8],
    shown_path: &str,
    max_bytes: u64,
    cancel: Option<&tokio_util::sync::CancellationToken>,
    workspace: &mut TextWorkspace,
) -> Result<Outcome, String> {
    if initial_len > max_bytes {
        return Err(format!(
            "Cannot read {shown_path} as a whole file: its size ({initial_len} bytes) exceeds the {max_bytes} byte (4 MiB) limit; pass offset and limit to read it in slices.",
        ));
    }
    let limit = max_bytes as usize;
    let capacity = (initial_len as usize).min(limit).max(prefix.len()) + 1;
    workspace.reserve(capacity)?;
    let mut raw = Vec::with_capacity(capacity);
    raw.extend_from_slice(prefix);
    let mut chunk = vec![0_u8; 64 * 1024];
    while raw.len() <= limit {
        if cancel.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
            return Err("Cancelled while reading file.".into());
        }
        let want = (limit + 1 - raw.len()).min(chunk.len());
        let read = file
            .read(&mut chunk[..want])
            .map_err(|error| format!("Cannot read {shown_path}: {error}."))?;
        if read == 0 {
            break;
        }
        workspace.reserve(raw.len().saturating_add(read))?;
        raw.extend_from_slice(&chunk[..read]);
    }
    if raw.len() > limit {
        let actual = file
            .metadata()
            .map_or(raw.len() as u64, |m| m.len().max(raw.len() as u64));
        return Err(format!(
            "Cannot read {shown_path} as a whole file: its size ({actual} bytes) exceeds the {max_bytes} byte (4 MiB) limit; pass offset and limit to read it in slices.",
        ));
    }
    let body = String::from_utf8(raw).map_err(|_| {
        format!(
            "Cannot read {shown_path} as a whole file: it is not valid UTF-8; pass offset and limit to inspect it with U+FFFD replacements, or process it with exec."
        )
    })?;
    let total_lines =
        bytecount_lf(body.as_bytes()) + u64::from(!body.is_empty() && !body.ends_with('\n'));
    let notice = format!("[read] {shown_path} · {total_lines} lines");
    let end_line = (total_lines > 0).then_some(total_lines);
    let structured = json!({
        "ok": true,
        "kind": "text",
        "path": shown_path,
        "startLine": 1_u64,
        "endLine": end_line,
        "shownLines": total_lines,
        "totalLines": total_lines,
        "truncated": false,
        "nextOffset": Value::Null,
        "lineTooLong": false,
        "lossy": false,
    });
    Ok(Outcome {
        owner: Default::default(),
        text: body.clone(),
        notice,
        body: Some(body),
        attachment: None,
        lease: None,
        structured,
    })
}

/// Reads as much of `into` as the file gives, which is short only at the end.
fn read_upto(file: &mut File, into: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < into.len() {
        match file.read(&mut into[filled..])? {
            0 => break,
            read => filled += read,
        }
    }
    Ok(filled)
}

/// A file's lines: decoded a line at a time while a line is kept, and only scanned for its end
/// while it is not.
pub(super) struct Source<'a> {
    reader: BufReader<&'a mut File>,
    cancel: Option<&'a tokio_util::sync::CancellationToken>,
    /// Decoded text already read and not yet given to a line (`pending[pos..]`), with where each
    /// replacement character it holds sits (`marks[mark_pos..]`).
    pending: String,
    pos: usize,
    marks: Vec<usize>,
    mark_pos: usize,
    /// Bytes of a character the last read cut in two, carried to the next read.
    carry: Vec<u8>,
    /// Set once every line has been answered.
    done: bool,
}

impl<'a> Source<'a> {
    pub(super) fn new(
        reader: BufReader<&'a mut File>,
        cancel: Option<&'a tokio_util::sync::CancellationToken>,
    ) -> Self {
        Self {
            reader,
            cancel,
            pending: String::new(),
            pos: 0,
            marks: Vec::new(),
            mark_pos: 0,
            carry: Vec::new(),
            done: false,
        }
    }

    /// The next line with its `\n` terminator when present, keeping at most `keep` bytes of it;
    /// `None` after the last line (including right after a final `\n` at EOF).
    fn kept(&mut self, keep: usize, workspace: &mut TextWorkspace) -> Result<Option<Line>, String> {
        if self.done {
            return Ok(None);
        }
        let mut line = Line::default();
        loop {
            if let Some(rel) = self.pending[self.pos..].find('\n') {
                let end = self.pos + rel + 1;
                let mut lossy = false;
                while self.mark_pos < self.marks.len() && self.marks[self.mark_pos] < end {
                    lossy = true;
                    self.mark_pos += 1;
                }
                line.push(&self.pending[self.pos..end], lossy, keep, workspace)?;
                self.pos = end;
                if self.pos == self.pending.len() {
                    self.clear_pending();
                }
                return Ok(Some(line));
            }
            // No line end here, so all of it belongs to this line.
            if self.pos < self.pending.len() {
                let lossy = self.mark_pos < self.marks.len();
                line.push(&self.pending[self.pos..], lossy, keep, workspace)?;
                self.clear_pending();
            }
            if self.fill()? {
                // The end: the flushed unfinished character is the rest of this line.
                if self.pos < self.pending.len() {
                    let lossy = self.mark_pos < self.marks.len();
                    line.push(&self.pending[self.pos..], lossy, keep, workspace)?;
                    self.clear_pending();
                }
                self.done = true;
                return Ok((line.length > 0).then_some(line));
            }
        }
    }

    /// The next line's byte length (including its `\n` terminator); `None` after the last.
    fn skipped(&mut self) -> Result<Option<u64>, String> {
        if self.done {
            return Ok(None);
        }
        if let Some(rel) = self.pending[self.pos..].find('\n') {
            let len = rel + 1;
            let end = self.pos + len;
            while self.mark_pos < self.marks.len() && self.marks[self.mark_pos] < end {
                self.mark_pos += 1;
            }
            self.pos = end;
            if self.pos == self.pending.len() {
                self.clear_pending();
            }
            return Ok(Some(len as u64));
        }
        let mut length = (self.pending.len() - self.pos) as u64 + self.carry.len() as u64;
        self.clear_pending();
        self.carry.clear();
        loop {
            if self
                .cancel
                .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
            {
                return Err("Cancelled while reading file.".into());
            }
            let (end, read) = {
                let available = match self.reader.fill_buf() {
                    Ok(available) => available,
                    Err(error) => return Err(format!("Cannot read the file: {error}.")),
                };
                let read = available.len();
                (available.iter().position(|byte| *byte == b'\n'), read)
            };
            if read == 0 {
                self.done = true;
                return Ok((length > 0).then_some(length));
            }
            match end {
                Some(at) => {
                    length += (at + 1) as u64;
                    self.reader.consume(at + 1);
                    return Ok(Some(length));
                }
                None => {
                    length += read as u64;
                    self.reader.consume(read);
                }
            }
        }
    }

    /// Reads one part of the file into `pending`, and answers whether the file ended.
    fn fill(&mut self) -> Result<bool, String> {
        if self
            .cancel
            .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
        {
            return Err("Cancelled while reading file.".into());
        }
        let (data, read) = {
            let available = match self.reader.fill_buf() {
                Ok(available) => available,
                Err(error) => return Err(format!("Cannot read the file: {error}.")),
            };
            if available.is_empty() {
                // The end: what the carry holds is the last, unfinished character.
                if !self.carry.is_empty() {
                    let carry = std::mem::take(&mut self.carry);
                    let (text, marks) = decode_lossy(&carry);
                    self.hold(&text, &marks);
                }
                return Ok(true);
            }
            let mut data = std::mem::take(&mut self.carry);
            data.extend_from_slice(available);
            (data, available.len())
        };
        self.reader.consume(read);
        let unfinished = unfinished_tail(&data);
        let (decoded, rest) = data.split_at(data.len() - unfinished);
        self.carry = rest.to_vec();
        let (text, marks) = decode_lossy(decoded);
        self.hold(&text, &marks);
        Ok(false)
    }

    /// Adds decoded text to `pending`, moving the marks it carries with it.
    fn hold(&mut self, text: &str, marks: &[usize]) {
        let base = self.pending.len();
        self.pending.push_str(text);
        for mark in marks {
            self.marks.push(base + mark);
        }
    }

    fn clear_pending(&mut self) {
        self.pending.clear();
        self.pos = 0;
        self.marks.clear();
        self.mark_pos = 0;
    }
}

/// The selected lines of a file: the text a result may show, and the counts its notices need.
#[derive(Default)]
pub(super) struct Window {
    /// The shown lines concatenated as decoded, keeping their terminators intact.
    pub(super) content: String,
    /// Whether decoding a kept line replaced bytes with U+FFFD.
    pub(super) lossy: bool,
    /// Real lines in the file (`\n`-ended segments plus a non-empty final segment).
    pub(super) total_lines: u64,
    /// The first selected line's length in decoded bytes, for the too-long notice.
    pub(super) first_line_bytes: u64,
    /// Lines and bytes of the selected content, before truncation.
    pub(super) window_lines: u64,
    pub(super) window_bytes: u64,
    /// Lines of the selected content that fit the budget.
    pub(super) shown_lines: u64,
    /// Whether truncation stopped because a line exceeded the byte budget.
    pub(super) stopped_on_bytes: bool,
}

impl Window {
    /// Streams the file, keeping only the lines the result may show and counting the rest.
    pub(super) fn scan(
        mut source: Source<'_>,
        start_line: u64,
        limit: Option<u64>,
        limits: ReadLimits,
        workspace: &mut TextWorkspace,
    ) -> Result<Self, String> {
        let start = start_line.saturating_sub(1);
        let end = limit.map_or(u64::MAX, |limit| start.saturating_add(limit));
        let keep = limits.keep();
        let mut window = Self::default();
        let mut deciding = true;
        loop {
            let in_window = window.total_lines >= start && window.total_lines < end;
            if in_window && deciding {
                let Some(line) = source.kept(keep, workspace)? else {
                    break;
                };
                window.take_line(&line, &mut deciding, limits, workspace)?;
            } else {
                let Some(length) = source.skipped()? else {
                    break;
                };
                window.skip_line(in_window, length);
            }
        }
        Ok(window)
    }

    /// Records a kept line: its counts, and its bytes when it fits the budget.
    fn take_line(
        &mut self,
        line: &Line,
        deciding: &mut bool,
        limits: ReadLimits,
        workspace: &mut TextWorkspace,
    ) -> Result<(), String> {
        self.total_lines += 1;
        self.window_lines += 1;
        self.window_bytes += line.length;
        if self.window_lines == 1 {
            self.first_line_bytes = line.length;
        }
        // A line too large to show is still a line that was decoded, so its replacement
        // characters are known and reported.
        self.lossy |= line.lossy;
        if self.window_bytes > limits.max_bytes {
            self.stopped_on_bytes = true;
            *deciding = false;
            return Ok(());
        }
        workspace.reserve(self.content.len().saturating_add(line.text.len()))?;
        self.content.push_str(&line.text);
        self.shown_lines += 1;
        if self.shown_lines >= limits.max_lines {
            *deciding = false;
        }
        Ok(())
    }

    /// Records a line that is only counted, not shown.
    fn skip_line(&mut self, in_window: bool, length: u64) {
        self.total_lines += 1;
        if !in_window {
            return;
        }
        self.window_bytes += length;
        self.window_lines += 1;
        if self.window_lines == 1 {
            self.first_line_bytes = length;
        }
    }

    /// The result text and structured metadata: the window, cut to the budget, with the notice
    /// that says how to see more.
    pub(super) fn render(
        &self,
        start_line: u64,
        limit: Option<u64>,
        shown_path: &str,
        limits: ReadLimits,
    ) -> Outcome {
        let start = start_line.saturating_sub(1);
        let start_display = start + 1;
        let shown = self.shown_lines;
        let budget_truncated =
            self.window_lines > limits.max_lines || self.window_bytes > limits.max_bytes;
        let line_too_long = budget_truncated && self.first_line_bytes > limits.max_bytes;
        let mut body = String::new();
        let mut notices = Vec::new();
        let mut truncated = budget_truncated;
        let mut next_offset = None;
        let shown_lines;

        if line_too_long {
            shown_lines = 0;
            let shell_path = shown_path.replace('\'', "'\\''");
            notices.push(format!(
                "[Line {start_display} is {}, exceeds {} limit. Use exec: sed -n '{start_display}p' '{shell_path}' | head -c {}]",
                format_size(self.first_line_bytes),
                format_size(limits.max_bytes),
                limits.max_bytes
            ));
        } else if budget_truncated {
            shown_lines = shown;
            let end_display = start_display + shown - 1;
            let next = end_display + 1;
            next_offset = Some(next);
            body.push_str(&self.content);
            if self.stopped_on_bytes {
                notices.push(format!(
                    "[Showing lines {start_display}-{end_display} of {} ({} limit). Use offset={next} to continue.]",
                    self.total_lines,
                    format_size(limits.max_bytes)
                ));
            } else {
                notices.push(format!(
                    "[Showing lines {start_display}-{end_display} of {}. Use offset={next} to continue.]",
                    self.total_lines
                ));
            }
        } else {
            shown_lines = shown;
            body.push_str(&self.content);
            if let Some(limit) = limit {
                let selected_end = start.saturating_add(limit);
                if selected_end < self.total_lines {
                    let remaining = self.total_lines - selected_end;
                    let next = selected_end + 1;
                    truncated = true;
                    next_offset = Some(next);
                    notices.push(format!(
                        "[{remaining} more lines in file. Use offset={next} to continue.]"
                    ));
                }
            }
        }
        if self.lossy {
            notices.push("[Invalid UTF-8 was replaced with U+FFFD.]".to_owned());
        }
        let notice_block = notices.join("\n\n");
        let text = if line_too_long || body.is_empty() {
            notice_block.clone()
        } else if notice_block.is_empty() {
            body.clone()
        } else if body.ends_with('\n') {
            format!("{body}\n{notice_block}")
        } else {
            format!("{body}\n\n{notice_block}")
        };
        let notice = if notice_block.is_empty() {
            format!("[read] {shown_path} · {shown_lines} lines")
        } else {
            notice_block
        };
        let end_line = (shown_lines > 0).then_some(start_display + shown_lines - 1);
        let structured = json!({
            "ok": true,
            "kind": "text",
            "path": shown_path,
            "startLine": start_display,
            "endLine": end_line,
            "shownLines": shown_lines,
            "totalLines": self.total_lines,
            "truncated": truncated,
            "nextOffset": next_offset,
            "lineTooLong": line_too_long,
            "lossy": self.lossy,
        });
        Outcome {
            owner: Default::default(),
            text,
            notice,
            body: Some(body),
            attachment: None,
            lease: None,
            structured,
        }
    }
}

/// One line while it is read: its whole decoded length, and the bytes of it that are kept.
#[derive(Default)]
struct Line {
    /// The decoded bytes of the line, up to the window's `keep` ceiling.
    text: String,
    /// The line's whole length in decoded bytes.
    length: u64,
    /// Whether decoding replaced bytes of this line.
    pub(super) lossy: bool,
}

impl Line {
    /// Adds decoded text, keeping at most `keep` bytes of it.
    fn push(
        &mut self,
        bytes: &str,
        lossy: bool,
        keep: usize,
        workspace: &mut TextWorkspace,
    ) -> Result<(), String> {
        if bytes.is_empty() {
            return Ok(());
        }
        self.length += bytes.len() as u64;
        self.lossy |= lossy;
        if self.text.len() >= keep {
            return Ok(());
        }
        let room = keep - self.text.len();
        let take = bytes.len().min(room);
        // A character cut by the cap leaves this line over the budget, so it is never shown and
        // the whole character is dropped rather than half of it kept.
        if bytes.is_char_boundary(take) {
            workspace.reserve(self.text.len().saturating_add(take))?;
            self.text.push_str(&bytes[..take]);
        }
        Ok(())
    }
}

/// Decodes UTF-8 the way a lossy decode does, naming where each replacement character lands.
fn decode_lossy(data: &[u8]) -> (String, Vec<usize>) {
    let mut text = String::with_capacity(data.len());
    let mut marks = Vec::new();
    let mut rest = data;
    while !rest.is_empty() {
        match std::str::from_utf8(rest) {
            Ok(valid) => {
                text.push_str(valid);
                break;
            }
            Err(error) => {
                let valid = error.valid_up_to();
                if let Ok(prefix) = std::str::from_utf8(&rest[..valid]) {
                    text.push_str(prefix);
                }
                let skip = error.error_len().unwrap_or(rest.len() - valid);
                marks.push(text.len());
                text.push('\u{fffd}');
                rest = &rest[valid + skip..];
            }
        }
    }
    (text, marks)
}

/// How many trailing bytes of `data` begin a character that the next bytes would finish.
fn unfinished_tail(data: &[u8]) -> usize {
    for take in 1..=3.min(data.len()) {
        match std::str::from_utf8(&data[data.len() - take..]) {
            Ok(_) => return 0,
            Err(error) => {
                if error.valid_up_to() == 0 && error.error_len().is_none() {
                    return take;
                }
            }
        }
    }
    0
}

/// Bytes as the reference implementation shows them: whole bytes, and one decimal of a kilobyte
/// or a megabyte, rounded the way `toFixed(1)` rounds.
pub(super) fn format_size(bytes: u64) -> String {
    if bytes < 1024 {
        return format!("{bytes}B");
    }
    let (scale, unit) = if bytes < 1024 * 1024 {
        (1024u128, "KB")
    } else {
        (1024u128 * 1024, "MB")
    };
    let tenths = (bytes as u128 * 10 + scale / 2) / scale;
    format!("{}.{}{unit}", tenths / 10, tenths % 10)
}
