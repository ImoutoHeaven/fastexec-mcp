use super::path::{display_path, open_regular, resolve, with_path_workspace};
use super::{FileError, ReadRequest};
use crate::lease::{BytePool, Lease};
use crate::media::{self, CompressFailure, DetectedMedia, MediaKind};
use crate::session::LaunchContext;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(super) const MAX_LINES: u64 = 2000;
/// The most bytes a direct result shows.
pub(super) const MAX_BYTES: u64 = 50 * 1024;
/// The most bytes a nested codemode read shows.
pub(super) const NESTED_MAX_BYTES: u64 = 4 << 20;
/// How much of a file is sniffed before deciding whether it is media or text.
pub(super) const SNIFF: usize = media::SNIFF;
pub(super) const MEDIA_READ_CHUNK: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MediaReadMode {
    /// Agent path: read and compress/base64-encode for a ContentBlock.
    InlineBlock,
    /// Program path: sniff and validate only; return a descriptor without loading base64 into JS.
    DescriptorOnly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadPolicy {
    pub max_lines: u64,
    pub max_bytes: u64,
    pub media_mode: MediaReadMode,
    /// When true and both `offset` and `limit` are omitted, read the entire file as strict UTF-8
    /// (erroring if > max_bytes or invalid UTF-8 instead of silently truncating or lossy-replacing).
    pub strict_whole_file: bool,
}

impl ReadPolicy {
    pub const AGENT: Self = Self {
        max_lines: MAX_LINES,
        max_bytes: MAX_BYTES,
        media_mode: MediaReadMode::InlineBlock,
        strict_whole_file: false,
    };

    pub const PROGRAM: Self = Self {
        max_lines: u64::MAX,
        max_bytes: NESTED_MAX_BYTES,
        media_mode: MediaReadMode::DescriptorOnly,
        strict_whole_file: true,
    };

    /// How much of one line is kept while it is read: a line that fits whole, and one byte more,
    /// so a longer line is known not to fit.
    const fn keep(self) -> usize {
        self.max_bytes as usize + 1
    }
}

#[derive(Debug)]
pub struct TextSlice {
    pub path: String,
    /// Raw file lines without line-number prefixes or notices.
    pub text: String,
    pub start_line: u64,
    pub end_line: Option<u64>,
    pub shown_lines: u64,
    pub total_lines: u64,
    pub truncated: bool,
    pub next_offset: Option<u64>,
    pub line_too_long: bool,
    pub lossy: bool,
    pub first_line_bytes: u64,
    pub stopped_on_bytes: bool,
    pub max_bytes: u64,
    pub budget_truncated: bool,
    pub lease: Lease,
}

#[derive(Debug)]
pub enum MediaPayload {
    Inline {
        kind: MediaKind,
        resolved_path: PathBuf,
        mime_type: String,
        encoded_base64: String,
        summary_line: String,
        lease: Lease,
    },
    Descriptor {
        kind: MediaKind,
        path: String,
        mime_type: String,
        bytes: Option<u64>,
        source_bytes: Option<u64>,
        attachable: bool,
        note: Option<String>,
    },
}

#[derive(Debug)]
pub enum FileReadData {
    Text(TextSlice),
    Media(MediaPayload),
}

/// Reads one file according to `policy`, returning pure domain data without MCP or JSON wrappers.
pub async fn read_core(
    request: ReadRequest,
    launch: Arc<LaunchContext>,
    transfer: bool,
    policy: ReadPolicy,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    cancel: Option<tokio_util::sync::CancellationToken>,
    input_owner: crate::lease::Owner,
) -> crate::lease::Owned<Result<FileReadData, FileError>> {
    let input = crate::lease::Owned::new(request, input_owner);
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        input
            .flat_map(|request| read_blocking(&request, &launch, transfer, policy, cancel.as_ref()))
    })
    .await
    {
        Ok(outcome) => outcome,
        Err(error) => crate::lease::Owned::new(
            Err(FileError::Io(format!(
                "the file operation did not finish: {error}"
            ))),
            Default::default(),
        ),
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
    pub(super) fn new(pool: &Arc<BytePool>, path_bytes: usize) -> Result<Self, FileError> {
        Self::with_base(
            pool,
            TEXT_FIXED_WORKSPACE.saturating_add(path_bytes.saturating_mul(32)),
        )
    }

    fn after_path(pool: &Arc<BytePool>) -> Result<Self, FileError> {
        // The path owner already accounts for the sniff buffer and path copies.
        Self::with_base(pool, TEXT_FIXED_WORKSPACE.saturating_sub(SNIFF))
    }

    fn with_base(pool: &Arc<BytePool>, base: usize) -> Result<Self, FileError> {
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

    fn reserve(&mut self, retained: usize) -> Result<(), FileError> {
        // Growing source lines, output windows, and response copies coexist.
        // Charge before each growth; retain the largest admitted peak.
        let target = self.base.saturating_add(retained.saturating_mul(16));
        if target > self.lease.bytes() && !self.lease.try_resize(target) {
            return Err(FileError::Overload(format!(
                "Server active memory budget ({} bytes) is full while reading file text; retry once in-flight transfers complete.",
                self.limit
            )));
        }
        Ok(())
    }
}

pub(super) fn count_lines_streaming(
    file: &mut File,
    shown_path: &str,
    cancel: Option<&tokio_util::sync::CancellationToken>,
) -> Result<u64, FileError> {
    file.seek(SeekFrom::Start(0))
        .map_err(|error| FileError::Io(format!("Cannot read {shown_path}: {error}.")))?;
    let mut buffer = vec![0_u8; 64 * 1024];
    let mut newlines = 0_u64;
    let mut total_bytes = 0_u64;
    let mut last_byte = b'\n';
    loop {
        if cancel.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
            return Err(FileError::Cancelled("Cancelled while reading file.".into()));
        }
        let read = file
            .read(&mut buffer)
            .map_err(|error| FileError::Io(format!("Cannot read {shown_path}: {error}.")))?;
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

#[allow(clippy::too_many_arguments)]
fn read_media_core(
    file: &mut File,
    initial_len: u64,
    prefix: &[u8],
    resolved: &Path,
    shown_path: String,
    detected: DetectedMedia,
    launch: &LaunchContext,
    transfer: bool,
    policy: ReadPolicy,
    cancel: Option<&tokio_util::sync::CancellationToken>,
) -> Result<MediaPayload, FileError> {
    let max_media_bytes = launch.max_media_bytes().map_err(FileError::Validation)?;
    let compress = launch.image_compress().map_err(FileError::Validation)?;
    if policy.media_mode == MediaReadMode::DescriptorOnly {
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
            return Ok(MediaPayload::Descriptor {
                kind: detected.kind,
                path: shown_path,
                mime_type: detected.mime.to_owned(),
                bytes: None,
                source_bytes: exact,
                attachable: false,
                note: Some(note),
            });
        }
        return Ok(MediaPayload::Descriptor {
            kind: detected.kind,
            path: shown_path,
            mime_type: detected.mime.to_owned(),
            bytes: Some(initial_len),
            source_bytes: None,
            attachable: true,
            note: None,
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
            return Ok(MediaPayload::Descriptor {
                kind: detected.kind,
                path: shown_path,
                mime_type: detected.mime.to_owned(),
                bytes: None,
                source_bytes: exact,
                attachable: false,
                note: Some(note),
            });
        }
        Err(MediaReadError::Io(error)) => {
            return Err(FileError::Io(format!("Cannot read {shown_path}: {error}.")));
        }
        Err(MediaReadError::Cancelled) => {
            return Err(FileError::Cancelled("Cancelled while reading file.".into()));
        }
        Err(MediaReadError::Overload) => {
            return Err(FileError::Overload(format!(
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
                    return Err(FileError::Overload(format!(
                        "Cannot read {shown_path}: server active memory budget ({} bytes) is full; retry once in-flight transfers complete.",
                        launch.memory().limit()
                    )));
                }
                let encoded = media::encode_base64(&compressed.bytes);
                drop(compressed.bytes);
                lease.shrink_to(outbound_peak);
                let mut summary_line =
                    format!("Read image file [{}] ({sent_bytes} B)", compressed.mime);
                for hint in &compressed.hints {
                    summary_line.push('\n');
                    summary_line.push_str(hint);
                }
                return Ok(MediaPayload::Inline {
                    kind: MediaKind::Image,
                    resolved_path: resolved.to_path_buf(),
                    mime_type: compressed.mime,
                    encoded_base64: encoded,
                    summary_line,
                    lease,
                });
            }
            Err(CompressFailure::Overload) => {
                drop(lease);
                return Err(FileError::Overload(format!(
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
                return Ok(MediaPayload::Descriptor {
                    kind: MediaKind::Image,
                    path: shown_path,
                    mime_type: detected.mime.to_owned(),
                    bytes: None,
                    source_bytes: Some(source_len),
                    attachable: false,
                    note: Some(note),
                });
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
    let summary_line = format!(
        "Read {} file [{}] ({sent_bytes} B)",
        detected.kind.as_str(),
        detected.mime
    );
    Ok(MediaPayload::Inline {
        kind: detected.kind,
        resolved_path: resolved.to_path_buf(),
        mime_type: detected.mime.to_owned(),
        encoded_base64: encoded,
        summary_line,
        lease,
    })
}

pub(crate) fn read_blocking(
    request: &ReadRequest,
    launch: &LaunchContext,
    transfer: bool,
    policy: ReadPolicy,
    cancel: Option<&tokio_util::sync::CancellationToken>,
) -> crate::lease::Owned<Result<FileReadData, FileError>> {
    match with_path_workspace(&request.path, launch, SNIFF, |path_lease| {
        read_blocking_reserved(request, launch, transfer, policy, cancel, path_lease)
    }) {
        Ok(read) => read,
        Err(message) => {
            crate::lease::Owned::new(Err(FileError::Overload(message)), Default::default())
        }
    }
}

fn read_blocking_reserved(
    request: &ReadRequest,
    launch: &LaunchContext,
    transfer: bool,
    policy: ReadPolicy,
    cancel: Option<&tokio_util::sync::CancellationToken>,
    path_lease: &mut Lease,
) -> Result<FileReadData, FileError> {
    if cancel.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
        return Err(FileError::Cancelled("Cancelled while reading file.".into()));
    }
    if request.offset == Some(0) {
        return Err(FileError::Validation(
            "offset must be non-zero (1-indexed from start, or negative from end).".into(),
        ));
    }
    if request.limit == Some(0) {
        return Err(FileError::Validation("limit must be at least 1.".into()));
    }
    let mut workspace;
    let resolved = resolve(&request.path, launch).map_err(FileError::Validation)?;
    let shown_path = display_path(&resolved);
    let (mut file, metadata) = open_regular(&resolved).map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            FileError::NotFound(format!("Cannot read {shown_path}: {error}."))
        } else if error.kind() == io::ErrorKind::IsADirectory {
            FileError::Io(format!(
                "Cannot read {shown_path}: {error}; list it with exec."
            ))
        } else {
            FileError::Io(format!("Cannot read {shown_path}: {error}."))
        }
    })?;
    let mut sniff = vec![0u8; SNIFF];
    let sniffed = read_upto(&mut file, &mut sniff)
        .map_err(|error| FileError::Io(format!("Cannot read {shown_path}: {error}.")))?;
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
            return Err(FileError::Overload(
                "Server active memory budget is full while preparing media configuration diagnostics."
                    .into(),
            ));
        }
        return read_media_core(
            &mut file,
            metadata.len(),
            &sniff[..sniffed],
            &resolved,
            shown_path,
            detected,
            launch,
            transfer,
            policy,
            cancel,
        )
        .map(FileReadData::Media);
    }
    if sniff[..sniffed].contains(&0) {
        let hint = action_hint(transfer);
        return Err(FileError::Io(format!(
            "{shown_path} is a binary file ({} bytes).{hint}",
            metadata.len()
        )));
    }
    workspace = TextWorkspace::after_path(launch.memory())?;
    if policy.strict_whole_file && request.offset.is_none() && request.limit.is_none() {
        let slice = read_whole_utf8(
            &mut file,
            metadata.len(),
            &sniff[..sniffed],
            &shown_path,
            policy.max_bytes,
            cancel,
            workspace,
        )?;
        return Ok(FileReadData::Text(slice));
    }
    let tail = if let Some(offset) = request.offset.filter(|off| *off < 0) {
        Some((
            offset.unsigned_abs(),
            count_lines_streaming(&mut file, &shown_path, cancel)?,
        ))
    } else {
        None
    };
    let mut scan = |start_line| {
        file.rewind()
            .map_err(|error| FileError::Io(format!("Cannot read {shown_path}: {error}.")))?;
        Window::scan(
            Source::new(BufReader::with_capacity(TEXT_BLOCK, &mut file), cancel),
            start_line,
            request.limit,
            policy,
            &mut workspace,
        )
    };
    let (start_line, window) = if let Some((distance, total)) = tail {
        scan_from_end(distance, total, &shown_path, scan)?
    } else {
        let start = request.offset.unwrap_or(1) as u64;
        let window = scan(start)?;
        if start > 1 && start > window.total_lines {
            return Err(FileError::Validation(format!(
                "Offset {start} is beyond end of file ({} lines total)",
                window.total_lines
            )));
        }
        (start, window)
    };
    Ok(FileReadData::Text(window.into_slice(
        start_line,
        request.limit,
        shown_path,
        policy,
        workspace.lease,
    )))
}

/// A successful tail slice must start at the offset implied by its own observed line count.
pub(super) fn scan_from_end(
    distance: u64,
    total: u64,
    path: &str,
    mut scan: impl FnMut(u64) -> Result<Window, FileError>,
) -> Result<(u64, Window), FileError> {
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
    Err(FileError::Io(format!(
        "Cannot read {path}: the file changed while reading; retry."
    )))
}

pub(super) fn read_whole_utf8(
    file: &mut File,
    initial_len: u64,
    prefix: &[u8],
    shown_path: &str,
    max_bytes: u64,
    cancel: Option<&tokio_util::sync::CancellationToken>,
    mut workspace: TextWorkspace,
) -> Result<TextSlice, FileError> {
    if initial_len > max_bytes {
        return Err(FileError::Limit(format!(
            "Cannot read {shown_path} as a whole file: its size ({initial_len} bytes) exceeds the {max_bytes} byte (4 MiB) limit; pass offset and limit to read it in slices.",
        )));
    }
    let limit = max_bytes as usize;
    let capacity = (initial_len as usize).min(limit).max(prefix.len()) + 1;
    workspace.reserve(capacity)?;
    let mut raw = Vec::with_capacity(capacity);
    raw.extend_from_slice(prefix);
    let mut chunk = vec![0_u8; 64 * 1024];
    while raw.len() <= limit {
        if cancel.is_some_and(tokio_util::sync::CancellationToken::is_cancelled) {
            return Err(FileError::Cancelled("Cancelled while reading file.".into()));
        }
        let want = (limit + 1 - raw.len()).min(chunk.len());
        let read = file
            .read(&mut chunk[..want])
            .map_err(|error| FileError::Io(format!("Cannot read {shown_path}: {error}.")))?;
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
        return Err(FileError::Limit(format!(
            "Cannot read {shown_path} as a whole file: its size ({actual} bytes) exceeds the {max_bytes} byte (4 MiB) limit; pass offset and limit to read it in slices.",
        )));
    }
    let body = String::from_utf8(raw).map_err(|_| {
        FileError::Io(format!(
            "Cannot read {shown_path} as a whole file: it is not valid UTF-8; pass offset and limit to inspect it with U+FFFD replacements, or process it with exec."
        ))
    })?;
    let total_lines =
        bytecount_lf(body.as_bytes()) + u64::from(!body.is_empty() && !body.ends_with('\n'));
    let end_line = (total_lines > 0).then_some(total_lines);
    Ok(TextSlice {
        path: shown_path.to_owned(),
        text: body,
        start_line: 1,
        end_line,
        shown_lines: total_lines,
        total_lines,
        truncated: false,
        next_offset: None,
        line_too_long: false,
        lossy: false,
        first_line_bytes: 0,
        stopped_on_bytes: false,
        max_bytes,
        budget_truncated: false,
        lease: workspace.lease,
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
    fn kept(
        &mut self,
        keep: usize,
        workspace: &mut TextWorkspace,
    ) -> Result<Option<Line>, FileError> {
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
    fn skipped(&mut self) -> Result<Option<u64>, FileError> {
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
                return Err(FileError::Cancelled("Cancelled while reading file.".into()));
            }
            let (end, read) = {
                let available = match self.reader.fill_buf() {
                    Ok(available) => available,
                    Err(error) => {
                        return Err(FileError::Io(format!("Cannot read the file: {error}.")));
                    }
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
    fn fill(&mut self) -> Result<bool, FileError> {
        if self
            .cancel
            .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
        {
            return Err(FileError::Cancelled("Cancelled while reading file.".into()));
        }
        let (data, read) = {
            let available = match self.reader.fill_buf() {
                Ok(available) => available,
                Err(error) => {
                    return Err(FileError::Io(format!("Cannot read the file: {error}.")));
                }
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
        policy: ReadPolicy,
        workspace: &mut TextWorkspace,
    ) -> Result<Self, FileError> {
        let start = start_line.saturating_sub(1);
        let end = limit.map_or(u64::MAX, |limit| start.saturating_add(limit));
        let keep = policy.keep();
        let mut window = Self::default();
        let mut deciding = true;
        loop {
            let in_window = window.total_lines >= start && window.total_lines < end;
            if in_window && deciding {
                let Some(line) = source.kept(keep, workspace)? else {
                    break;
                };
                window.take_line(&line, &mut deciding, policy, workspace)?;
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
        policy: ReadPolicy,
        workspace: &mut TextWorkspace,
    ) -> Result<(), FileError> {
        self.total_lines += 1;
        self.window_lines += 1;
        self.window_bytes += line.length;
        if self.window_lines == 1 {
            self.first_line_bytes = line.length;
        }
        // A line too large to show is still a line that was decoded, so its replacement
        // characters are known and reported.
        self.lossy |= line.lossy;
        if self.window_bytes > policy.max_bytes {
            self.stopped_on_bytes = true;
            *deciding = false;
            return Ok(());
        }
        workspace.reserve(self.content.len().saturating_add(line.text.len()))?;
        self.content.push_str(&line.text);
        self.shown_lines += 1;
        if self.shown_lines >= policy.max_lines {
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

    /// Builds the pure domain `TextSlice` without notices or JSON wrappers.
    pub(super) fn into_slice(
        self,
        start_line: u64,
        limit: Option<u64>,
        shown_path: String,
        policy: ReadPolicy,
        lease: Lease,
    ) -> TextSlice {
        let start = start_line.saturating_sub(1);
        let start_display = start + 1;
        let shown = self.shown_lines;
        let budget_truncated =
            self.window_lines > policy.max_lines || self.window_bytes > policy.max_bytes;
        let line_too_long = budget_truncated && self.first_line_bytes > policy.max_bytes;
        let mut truncated = budget_truncated;
        let mut next_offset = None;
        let (text, shown_lines) = if line_too_long {
            (String::new(), 0)
        } else if budget_truncated {
            let end_display = start_display + shown - 1;
            next_offset = Some(end_display + 1);
            (self.content, shown)
        } else {
            if let Some(limit) = limit {
                let selected_end = start.saturating_add(limit);
                if selected_end < self.total_lines {
                    truncated = true;
                    next_offset = Some(selected_end + 1);
                }
            }
            (self.content, shown)
        };
        let end_line = (shown_lines > 0).then_some(start_display + shown_lines - 1);
        TextSlice {
            path: shown_path,
            text,
            start_line: start_display,
            end_line,
            shown_lines,
            total_lines: self.total_lines,
            truncated,
            next_offset,
            line_too_long,
            lossy: self.lossy,
            first_line_bytes: self.first_line_bytes,
            stopped_on_bytes: self.stopped_on_bytes,
            max_bytes: policy.max_bytes,
            budget_truncated,
            lease,
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
    ) -> Result<(), FileError> {
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
