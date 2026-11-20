//! The text file tools: `read`, `write` and `edit`, and the path resolution they share.
//!
//! Every function here blocks: the callers run them on `spawn_blocking`, so one large file never
//! holds a runtime thread. Read streams a file and keeps only the window it may show; write and
//! edit hold the whole file, because they rewrite it.

use crate::lease::{BytePool, Lease};
use crate::media::{self, CompressFailure, DetectedMedia, MediaKind};
use crate::service::{self, OutputMode, Render};
use crate::session::LaunchContext;
use rmcp::model::{CallToolResult, ContentBlock};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
#[cfg(all(unix, not(any(target_os = "macos", target_os = "freebsd"))))]
use std::fs::Metadata;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use unicode_normalization::UnicodeNormalization;
use unicode_normalization::char::canonical_combining_class;

/// The most lines a direct result shows.
const MAX_LINES: u64 = 2000;
/// The most bytes a direct result shows.
const MAX_BYTES: u64 = 50 * 1024;
/// The most bytes a nested codemode read shows.
const NESTED_MAX_BYTES: u64 = 4 << 20;
/// How much of a file is sniffed before deciding whether it is media or text.
const SNIFF: usize = media::SNIFF;
const MEDIA_READ_CHUNK: usize = 64 * 1024;

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

/// The one lock that serializes write and edit, so a read-modify-write cannot interleave with
/// another one. It is held for the whole blocking operation, never across an await.
static MUTATION: Mutex<()> = Mutex::new(());

fn lock() -> MutexGuard<'static, ()> {
    MUTATION
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReadRequest {
    /// Path to the file to read (relative or absolute)
    pub path: String,
    /// Line number to start reading from (1-indexed; negative counts from the end, -1 is the last line)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<i64>,
    /// Maximum number of lines to read (at least 1)
    #[schemars(range(min = 1))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct WriteRequest {
    /// Path to the file to write (relative or absolute)
    pub path: String,
    /// Content to write to the file
    pub content: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EditRequest {
    /// Path to the file to edit (relative or absolute)
    pub path: String,
    /// One or more targeted replacements. Each edit is matched against the original file, not incrementally. Do not include overlapping or nested edits. If two changes touch the same block or nearby lines, merge them into one edit instead.
    pub edits: Vec<Edit>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Edit {
    /// Exact text for one targeted replacement. It must be unique in the original file (or within startLine..=endLine) and must not overlap with any other edits[].oldText in the same call.
    pub old_text: String,
    /// Replacement text for this targeted edit.
    pub new_text: String,
    /// Optional 1-based line range the match must start in (inclusive first line); use it to pick one of several identical matches.
    #[schemars(range(min = 1))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_line: Option<u64>,
    /// Optional 1-based inclusive last line the match must start in.
    #[schemars(range(min = 1))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_line: Option<u64>,
}

#[derive(Debug)]
pub struct TaskMeta {
    pub bare_task_id: String,
    pub log_path: String,
}

pub struct TrackedResult {
    pub result: CallToolResult,
    pub lease: Option<Lease>,
    pub overload: bool,
    pub task_meta: Option<TaskMeta>,
}

impl TrackedResult {
    pub fn plain(result: CallToolResult) -> Self {
        Self {
            result,
            lease: None,
            overload: false,
            task_meta: None,
        }
    }
}

/// Runs one file tool from raw arguments, the way a daemon receives them, with the same
/// validation and errors as a local call.
pub async fn call_tracked(
    tool: &str,
    arguments: Value,
    launch: Arc<LaunchContext>,
    transfer: bool,
    render: Render,
    cancel: Option<tokio_util::sync::CancellationToken>,
) -> TrackedResult {
    match tool {
        "read" => match serde_json::from_value::<ReadRequest>(arguments) {
            Ok(request) => read_tracked(request, launch, transfer, render, cancel).await,
            Err(error) => TrackedResult::plain(schema_failed(format!(
                "failed to deserialize parameters: {error}"
            ))),
        },
        "write" => match serde_json::from_value::<WriteRequest>(arguments) {
            Ok(request) => TrackedResult::plain(write(request, launch, render).await),
            Err(error) => TrackedResult::plain(schema_failed(format!(
                "failed to deserialize parameters: {error}"
            ))),
        },
        "edit" => match serde_json::from_value::<EditRequest>(arguments) {
            Ok(request) => TrackedResult::plain(edit(request, launch, render).await),
            Err(error) => TrackedResult::plain(schema_failed(format!(
                "failed to deserialize parameters: {error}"
            ))),
        },
        other => TrackedResult::plain(schema_failed(format!("unknown tool {other}"))),
    }
}

/// Reads one file: its text, media attachment, or the message saying why it is not shown.
pub async fn read_tracked(
    request: ReadRequest,
    launch: Arc<LaunchContext>,
    transfer: bool,
    render: Render,
    cancel: Option<tokio_util::sync::CancellationToken>,
) -> TrackedResult {
    read_with_limits_and_permit(
        request,
        launch,
        transfer,
        render,
        ReadLimits::DIRECT,
        None,
        cancel,
    )
    .await
}

/// Reads one file for an in-process codemode script: whole-file when neither `offset` nor
/// `limit` is given, or a 4 MiB slice otherwise. `permit` stays inside the blocking worker
/// until disk I/O finishes, even if the awaiting task is cancelled.
pub(crate) async fn read_nested(
    request: ReadRequest,
    launch: Arc<LaunchContext>,
    transfer: bool,
    render: Render,
    permit: tokio::sync::OwnedSemaphorePermit,
    cancel: tokio_util::sync::CancellationToken,
) -> CallToolResult {
    read_with_limits_and_permit(
        request,
        launch,
        transfer,
        render,
        ReadLimits::NESTED,
        Some(permit),
        Some(cancel),
    )
    .await
    .result
}

async fn read_with_limits_and_permit(
    request: ReadRequest,
    launch: Arc<LaunchContext>,
    transfer: bool,
    render: Render,
    limits: ReadLimits,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    cancel: Option<tokio_util::sync::CancellationToken>,
) -> TrackedResult {
    let effective = render.effective();
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        read_cancellable(&request, &launch, transfer, limits, cancel.as_ref())
    })
    .await
    {
        Ok(Ok(outcome)) => outcome.into_tracked(effective),
        Ok(Err(ReadFailure::Overload(message))) => TrackedResult {
            result: service::suppress(failed(message), effective),
            lease: None,
            overload: true,
            task_meta: None,
        },
        Ok(Err(ReadFailure::Error(message))) => {
            TrackedResult::plain(service::suppress(failed(message), effective))
        }
        Err(error) => TrackedResult::plain(service::suppress(
            failed(format!("the file operation did not finish: {error}")),
            effective,
        )),
    }
}

enum ReadFailure {
    Error(String),
    Overload(String),
}

impl From<String> for ReadFailure {
    fn from(message: String) -> Self {
        Self::Error(message)
    }
}

/// What a read answers with: its text, and its media block when a media file is attached.
pub struct Outcome {
    pub text: String,
    pub notice: String,
    pub body: Option<String>,
    pub attachment: Option<ContentBlock>,
    pub lease: Option<Lease>,
    pub structured: Value,
}

impl Outcome {
    fn into_tracked(self, render: Render) -> TrackedResult {
        let mut structured = self.structured;
        if render.output_mode != OutputMode::Text
            && let Some(body) = self.body
        {
            structured["text"] = json!(body);
        }
        let shown = if render.output_mode == OutputMode::Structured && self.attachment.is_none() {
            self.notice
        } else {
            self.text
        };
        let mut blocks = vec![ContentBlock::text(shown)];
        if let Some(attachment) = self.attachment {
            blocks.push(attachment);
        }
        let mut result = CallToolResult::success(blocks);
        result.structured_content = Some(structured);
        TrackedResult {
            result: service::suppress(result, render),
            lease: self.lease,
            overload: false,
            task_meta: None,
        }
    }
}

/// Writes one file, creating its parent directories.
pub async fn write(
    request: WriteRequest,
    launch: Arc<LaunchContext>,
    render: Render,
) -> CallToolResult {
    write_with_permit(request, launch, render, None).await
}

pub(crate) async fn write_nested(
    request: WriteRequest,
    launch: Arc<LaunchContext>,
    render: Render,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> CallToolResult {
    write_with_permit(request, launch, render, Some(permit)).await
}

async fn write_with_permit(
    request: WriteRequest,
    launch: Arc<LaunchContext>,
    render: Render,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> CallToolResult {
    let effective = render.effective();
    let bytes = request.content.len();
    let outcome = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        write_text(&request, &launch)
    })
    .await;
    let result = finished(outcome.map(|res| {
        res.map(|(text, path)| (text, json!({ "ok": true, "path": path, "bytes": bytes })))
            .map_err(|msg| (msg, None))
    }));
    service::suppress(result, effective)
}

/// Applies targeted replacements to one file.
pub async fn edit(
    request: EditRequest,
    launch: Arc<LaunchContext>,
    render: Render,
) -> CallToolResult {
    edit_with_permit(request, launch, render, None).await
}

pub(crate) async fn edit_nested(
    request: EditRequest,
    launch: Arc<LaunchContext>,
    render: Render,
    permit: tokio::sync::OwnedSemaphorePermit,
) -> CallToolResult {
    edit_with_permit(request, launch, render, Some(permit)).await
}

async fn edit_with_permit(
    request: EditRequest,
    launch: Arc<LaunchContext>,
    render: Render,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> CallToolResult {
    let effective = render.effective();
    let result = finished(
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            edit_outcome(&request, &launch)
        })
        .await,
    );
    service::suppress(result, effective)
}

type BlockingOutcome = Result<(String, Value), (String, Option<Value>)>;

/// A blocking operation's answer: its text and structured metadata, or its message as an error.
fn finished(outcome: Result<BlockingOutcome, tokio::task::JoinError>) -> CallToolResult {
    match outcome {
        Ok(Ok((text, structured))) => {
            let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
            result.structured_content = Some(structured);
            result
        }
        Ok(Err((message, Some(structured)))) => {
            let mut result = CallToolResult::error(vec![ContentBlock::text(message)]);
            result.structured_content = Some(structured);
            result
        }
        Ok(Err((message, None))) => failed(message),
        Err(error) => failed(format!("the file operation did not finish: {error}")),
    }
}

fn failed(message: String) -> CallToolResult {
    let mut result = CallToolResult::error(vec![ContentBlock::text(message.clone())]);
    result.structured_content = Some(json!({ "ok": false, "error": message }));
    result
}

fn schema_failed(message: String) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(message)])
}

/// Formats a path for tool output and `structuredContent`: forward slashes on Windows, stripping
/// verbatim `\\?\` prefixes and turning `\\server\share` into `//server/share`.
pub fn display_path(path: &Path) -> String {
    #[cfg(windows)]
    {
        format_windows_display(&path.to_string_lossy())
    }
    #[cfg(not(windows))]
    {
        path.display().to_string()
    }
}

#[cfg(any(windows, test))]
fn format_windows_display(raw: &str) -> String {
    if raw.len() >= 8 && raw.as_bytes()[..8].eq_ignore_ascii_case(b"\\\\?\\UNC\\") {
        return format!("//{}", raw[8..].replace('\\', "/"));
    }
    let drive_path = raw.strip_prefix("\\\\?\\").filter(|rest| {
        rest.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
            && rest.as_bytes().get(1..3) == Some(b":\\")
    });
    // Volume GUID paths have no drive-letter equivalent; //?/Volume{...}/... round-trips.
    let mut shown = drive_path.unwrap_or(raw).replace('\\', "/");
    if shown.as_bytes().get(1) == Some(&b':') && shown.as_bytes()[0].is_ascii_lowercase() {
        let upper = (shown.as_bytes()[0] as char).to_ascii_uppercase();
        shown.replace_range(..1, &upper.to_string());
    }
    shown
}

/// Rewrites Git Bash `/<letter>` and `/<letter>/...` drive paths to `<LETTER>:/...`, and
/// forward-slash UNC `//server/share...` paths to `\\server\share...` so `std::path::Path`
/// parses `Component::Prefix(Prefix::UNC)` on Windows.
#[cfg(any(windows, test))]
fn git_bash_drive_path(path: &str) -> Option<String> {
    if path.starts_with("//") && !path.starts_with("///") {
        let rest = &path[2..];
        if let Some((server, after)) = rest.split_once('/')
            && !server.is_empty()
        {
            let share = after.split('/').next().unwrap_or_default();
            if !share.is_empty() {
                return Some(format!("\\\\{}", rest.replace('/', "\\")));
            }
        }
    }
    let bytes = path.as_bytes();
    if bytes.first() == Some(&b'/') && bytes.len() >= 2 && bytes[1].is_ascii_alphabetic() {
        let drive = (bytes[1] as char).to_ascii_uppercase();
        if bytes.len() == 2 {
            return Some(format!("{drive}:/"));
        }
        if bytes[2] == b'/' {
            return Some(format!("{drive}:/{}", &path[3..]));
        }
    }
    None
}

/// A final separator or dot requires a directory even when the preceding component is a file.
fn ends_in_directory(path: &Path) -> bool {
    let bytes = path.as_os_str().as_encoded_bytes();
    matches!(
        bytes
            .rsplit(|&b| std::path::is_separator(char::from(b)))
            .next(),
        Some(b"" | b"." | b"..")
    )
}

/// Rebuilds paths without `PathBuf::push`, which folds `..` on Windows verbatim paths.
fn clean_components(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut out = std::ffi::OsString::new();
    let mut separator = false;
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Prefix(_) | Component::RootDir => {
                out.push(component.as_os_str());
                separator = false;
            }
            other => {
                if separator {
                    out.push(std::path::MAIN_SEPARATOR_STR);
                }
                out.push(other.as_os_str());
                separator = true;
            }
        }
    }
    if out.is_empty() {
        PathBuf::from(".")
    } else {
        if separator
            && ends_in_directory(path)
            && !matches!(path.components().next_back(), Some(Component::ParentDir))
        {
            out.push(std::path::MAIN_SEPARATOR_STR);
        }
        PathBuf::from(out)
    }
}

/// Joins without folding parent components, including when the base is a verbatim Windows path.
fn append_path(base: &Path, path: &Path) -> PathBuf {
    use std::path::Component;

    if path.is_absolute() || matches!(path.components().next(), Some(Component::Prefix(_))) {
        return clean_components(path);
    }
    // A Windows root-relative target replaces the directory but keeps the drive or UNC prefix.
    let mut joined = if path.has_root() {
        match base.components().next() {
            Some(Component::Prefix(prefix)) => prefix.as_os_str().to_owned(),
            _ => std::ffi::OsString::new(),
        }
    } else {
        base.as_os_str().to_owned()
    };
    if !joined.is_empty()
        && !joined
            .as_encoded_bytes()
            .last()
            .is_some_and(|byte| std::path::is_separator(char::from(*byte)))
    {
        joined.push(std::path::MAIN_SEPARATOR_STR);
    }
    // Parse the appended part first so Windows also accepts its forward slashes.
    joined.push(clean_components(path));
    clean_components(Path::new(&joined))
}

/// Resolves a path parameter against `launch`: expands `~`, maps Git Bash `/c/...` drive roots on
/// Windows, refuses driveless rooted and drive-relative Windows paths, joins relative paths onto
/// `launch.cwd`, and removes `.` and repeated separators while leaving `..` to the filesystem.
pub fn resolve(path: &str, launch: &LaunchContext) -> Result<PathBuf, String> {
    if path.is_empty() {
        return Err("Path must not be empty.".into());
    }
    let home = || {
        launch
            .home()
            .ok_or_else(|| format!("Cannot expand ~ in {path}: the home directory is unknown."))
    };
    if path == "~" {
        return Ok(clean_components(&home()?));
    }
    let home_tail = path.strip_prefix("~/");
    #[cfg(windows)]
    let home_tail = home_tail.or_else(|| path.strip_prefix("~\\"));
    if let Some(rest) = home_tail {
        let relative = Path::new(rest.trim_start_matches(std::path::is_separator));
        if matches!(
            relative.components().next(),
            Some(std::path::Component::Prefix(_))
        ) {
            return Err(format!(
                "Cannot resolve {path}: the path after ~ must be relative to the home directory, without a Windows path prefix."
            ));
        }
        return resolve_from(relative, &home()?);
    }
    resolve_from(Path::new(path), &launch.cwd)
}

#[cfg(any(windows, test))]
fn is_windows_reserved_device_name(component: &str) -> bool {
    let stem = component
        .split(['.', ':'])
        .next()
        .unwrap_or(component)
        .trim_end_matches(' ');
    if stem.eq_ignore_ascii_case("CON")
        || stem.eq_ignore_ascii_case("PRN")
        || stem.eq_ignore_ascii_case("AUX")
        || stem.eq_ignore_ascii_case("NUL")
    {
        return true;
    }
    let Some(prefix) = stem.get(..3) else {
        return false;
    };
    (prefix.eq_ignore_ascii_case("COM") || prefix.eq_ignore_ascii_case("LPT"))
        && matches!(
            &stem[3..],
            "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
        )
}

/// Resolves a native or Git Bash path against a fixed base, without tilde expansion.
pub(crate) fn resolve_from(path: &Path, base: &Path) -> Result<PathBuf, String> {
    #[cfg(windows)]
    {
        use std::path::{Component, Prefix};

        let rewritten = path.to_str().and_then(git_bash_drive_path);
        let candidate = rewritten.as_deref().map_or(path, Path::new);
        let shown = path.to_string_lossy();
        let mut comps = candidate.components();
        let first = comps.next();
        let second = comps.next();
        let resolved = match (first, second) {
            (Some(Component::Prefix(prefix)), second) => match prefix.kind() {
                Prefix::DeviceNS(_) => {
                    return Err(format!(
                        "Cannot resolve {shown}: Windows device namespace paths are not supported."
                    ));
                }
                Prefix::UNC(_, share) | Prefix::VerbatimUNC(_, share)
                    if share.eq_ignore_ascii_case("pipe") =>
                {
                    return Err(format!(
                        "Cannot resolve {shown}: Windows named pipe paths are not supported."
                    ));
                }
                Prefix::Verbatim(name) if name.eq_ignore_ascii_case("pipe") => {
                    return Err(format!(
                        "Cannot resolve {shown}: Windows named pipe paths are not supported."
                    ));
                }
                Prefix::Disk(_) if second != Some(Component::RootDir) => {
                    return Err(format!(
                        "Cannot resolve {shown}: drive-relative paths such as C:file depend on a per-drive current directory; pass C:/... or a relative path."
                    ));
                }
                _ => clean_components(candidate),
            },
            (Some(Component::RootDir), _) => {
                return Err(format!(
                    "Cannot resolve {shown}: driveless paths such as /tmp name Git Bash locations on Windows; pass C:/... or /c/..., or convert it with cygpath -m."
                ));
            }
            _ => append_path(base, candidate),
        };
        for component in candidate.components() {
            if let Component::Normal(name) = component
                && is_windows_reserved_device_name(&name.to_string_lossy())
            {
                return Err(format!(
                    "Cannot resolve {shown}: Windows reserved device names are not supported."
                ));
            }
        }
        Ok(resolved)
    }
    #[cfg(not(windows))]
    {
        Ok(append_path(base, path))
    }
}

/// The text of one file, cut to the window and the limits a result may show.
#[cfg(test)]
fn read_text(
    request: &ReadRequest,
    launch: &LaunchContext,
    transfer: bool,
    limits: ReadLimits,
) -> Result<Outcome, String> {
    read_cancellable(request, launch, transfer, limits, None).map_err(|failure| match failure {
        ReadFailure::Error(msg) | ReadFailure::Overload(msg) => msg,
    })
}

fn count_lines_streaming(
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

const BASE_MEDIA_FRAME_OVERHEAD: usize = 1024;

fn media_frame_overhead(path_len: usize) -> usize {
    BASE_MEDIA_FRAME_OVERHEAD.saturating_add(path_len.saturating_mul(10))
}

fn peak_media_lease(raw_len: usize, frame_overhead: Option<usize>) -> Option<usize> {
    if let Some(overhead) = frame_overhead {
        let b64 = raw_len.checked_add(2)?.checked_div(3)?.checked_mul(4)?;
        let read_peak = raw_len.checked_add(b64)?;
        let outbound_peak = b64.checked_mul(2)?.checked_add(overhead)?;
        Some(read_peak.max(outbound_peak))
    } else {
        Some(raw_len)
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
    let initial_peak =
        peak_media_lease(expected, frame_overhead).ok_or(MediaReadError::Overload)?;
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
    let mut chunk = vec![0_u8; MEDIA_READ_CHUNK];
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
        let Some(needed_peak) = peak_media_lease(next_len, frame_overhead) else {
            drop(chunk);
            drop(raw);
            drop(lease);
            return Err(MediaReadError::Overload);
        };
        if needed_peak > lease.bytes() && !lease.try_resize(needed_peak) {
            drop(chunk);
            drop(raw);
            drop(lease);
            return Err(MediaReadError::Overload);
        }
        raw.extend_from_slice(&chunk[..read]);
        remaining -= read;
    }
    drop(chunk);
    let final_peak = peak_media_lease(raw.len(), frame_overhead).unwrap_or(lease.bytes());
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
                    .len()
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
        notice: text.clone(),
        text,
        body: None,
        attachment: Some(block),
        lease: Some(lease),
        structured,
    })
}

fn read_cancellable(
    request: &ReadRequest,
    launch: &LaunchContext,
    transfer: bool,
    limits: ReadLimits,
    cancel: Option<&tokio_util::sync::CancellationToken>,
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
    if limits == ReadLimits::NESTED && request.offset.is_none() && request.limit.is_none() {
        return read_whole_utf8(
            &mut file,
            metadata.len(),
            &sniff[..sniffed],
            &shown_path,
            limits.max_bytes,
            cancel,
        )
        .map_err(ReadFailure::Error);
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
            Source::new(BufReader::new(&mut file), cancel),
            start_line,
            request.limit,
            limits,
        )
    };
    let (start_line, window) = if let Some((distance, total)) = tail {
        scan_from_end(distance, total, &shown_path, scan).map_err(ReadFailure::Error)?
    } else {
        let start = request.offset.unwrap_or(1) as u64;
        let window = scan(start).map_err(ReadFailure::Error)?;
        if start > 1 && start > window.total_lines {
            return Err(ReadFailure::Error(format!(
                "Offset {start} is beyond end of file ({} lines total)",
                window.total_lines
            )));
        }
        (start, window)
    };
    Ok(window.render(start_line, request.limit, &shown_path, limits))
}

/// A successful tail slice must start at the offset implied by its own observed line count.
fn scan_from_end(
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

fn read_whole_utf8(
    file: &mut File,
    initial_len: u64,
    prefix: &[u8],
    shown_path: &str,
    max_bytes: u64,
    cancel: Option<&tokio_util::sync::CancellationToken>,
) -> Result<Outcome, String> {
    if initial_len > max_bytes {
        return Err(format!(
            "Cannot read {shown_path} as a whole file: its size ({initial_len} bytes) exceeds the {max_bytes} byte (4 MiB) limit; pass offset and limit to read it in slices.",
        ));
    }
    let limit = max_bytes as usize;
    let mut raw = Vec::with_capacity((initial_len as usize).min(limit).max(prefix.len()) + 1);
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
        text: body.clone(),
        notice,
        body: Some(body),
        attachment: None,
        lease: None,
        structured,
    })
}

/// Both text readers reject special files before reading any data. The initial check avoids
/// opening devices at all; the handle check covers replacement between metadata and open. On
/// Unix, a FIFO swapped into that gap must not block open while edit holds the mutation lock.
pub(crate) fn open_regular(path: &Path) -> io::Result<(File, std::fs::Metadata)> {
    open_regular_for(path, false, false)
}

pub(crate) fn open_regular_for_upload(
    path: &Path,
    create: bool,
) -> io::Result<(File, std::fs::Metadata)> {
    open_regular_for(path, true, create)
}

fn open_regular_for(
    path: &Path,
    write: bool,
    create: bool,
) -> io::Result<(File, std::fs::Metadata)> {
    match std::fs::metadata(path) {
        Ok(initial) => check_regular(&initial)?,
        Err(error) if create && error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut options = OpenOptions::new();
    options.read(!write).write(write).create(create);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{SECURITY_ANONYMOUS, SECURITY_SQOS_PRESENT};
        options.security_qos_flags(SECURITY_SQOS_PRESENT | SECURITY_ANONYMOUS);
    }
    let file = options.open(path)?;
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{FILE_TYPE_DISK, GetFileType};
        // SAFETY: file owns this live handle. Reject named pipes/consoles before any read.
        if unsafe { GetFileType(file.as_raw_handle()) } != FILE_TYPE_DISK {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "it is not a regular file",
            ));
        }
    }
    let metadata = file.metadata()?;
    check_regular(&metadata)?;
    Ok((file, metadata))
}

fn check_regular(metadata: &std::fs::Metadata) -> io::Result<()> {
    if metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::IsADirectory,
            "it is a directory",
        ));
    }
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "it is not a regular file",
        ));
    }
    Ok(())
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
struct Source<'a> {
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
    fn new(
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
    fn kept(&mut self, keep: usize) -> Result<Option<Line>, String> {
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
                line.push(&self.pending[self.pos..end], lossy, keep);
                self.pos = end;
                if self.pos == self.pending.len() {
                    self.clear_pending();
                }
                return Ok(Some(line));
            }
            // No line end here, so all of it belongs to this line.
            if self.pos < self.pending.len() {
                let lossy = self.mark_pos < self.marks.len();
                line.push(&self.pending[self.pos..], lossy, keep);
                self.clear_pending();
            }
            if self.fill()? {
                // The end: the flushed unfinished character is the rest of this line.
                if self.pos < self.pending.len() {
                    let lossy = self.mark_pos < self.marks.len();
                    line.push(&self.pending[self.pos..], lossy, keep);
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
struct Window {
    /// The shown lines concatenated as decoded, keeping their terminators intact.
    content: String,
    /// Whether decoding a kept line replaced bytes with U+FFFD.
    lossy: bool,
    /// Real lines in the file (`\n`-ended segments plus a non-empty final segment).
    total_lines: u64,
    /// The first selected line's length in decoded bytes, for the too-long notice.
    first_line_bytes: u64,
    /// Lines and bytes of the selected content, before truncation.
    window_lines: u64,
    window_bytes: u64,
    /// Lines of the selected content that fit the budget.
    shown_lines: u64,
    /// Whether truncation stopped because a line exceeded the byte budget.
    stopped_on_bytes: bool,
}

impl Window {
    /// Streams the file, keeping only the lines the result may show and counting the rest.
    fn scan(
        mut source: Source<'_>,
        start_line: u64,
        limit: Option<u64>,
        limits: ReadLimits,
    ) -> Result<Self, String> {
        let start = start_line.saturating_sub(1);
        let end = limit.map_or(u64::MAX, |limit| start.saturating_add(limit));
        let keep = limits.keep();
        let mut window = Self::default();
        let mut deciding = true;
        loop {
            let in_window = window.total_lines >= start && window.total_lines < end;
            if in_window && deciding {
                let Some(line) = source.kept(keep)? else {
                    break;
                };
                window.take_line(&line, &mut deciding, limits);
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
    fn take_line(&mut self, line: &Line, deciding: &mut bool, limits: ReadLimits) {
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
            return;
        }
        self.content.push_str(&line.text);
        self.shown_lines += 1;
        if self.shown_lines >= limits.max_lines {
            *deciding = false;
        }
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
    fn render(
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
    lossy: bool,
}

impl Line {
    /// Adds decoded text, keeping at most `keep` bytes of it.
    fn push(&mut self, bytes: &str, lossy: bool, keep: usize) {
        if bytes.is_empty() {
            return;
        }
        self.length += bytes.len() as u64;
        self.lossy |= lossy;
        if self.text.len() >= keep {
            return;
        }
        let room = keep - self.text.len();
        let take = bytes.len().min(room);
        // A character cut by the cap leaves this line over the budget, so it is never shown and
        // the whole character is dropped rather than half of it kept.
        if bytes.is_char_boundary(take) {
            self.text.push_str(&bytes[..take]);
        }
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
fn format_size(bytes: u64) -> String {
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

/// Writes one file, creating its parent directories and keeping an existing file's permissions.
fn write_text(request: &WriteRequest, launch: &LaunchContext) -> Result<(String, String), String> {
    let resolved = resolve(&request.path, launch)?;
    let shown_path = display_path(&resolved);
    let _guard = lock();
    let target = create_target_parents(&resolved)
        .map_err(|error| format!("Cannot write {shown_path}: {error}."))?;
    save_target(target, request.content.as_bytes(), None)
        .map_err(|error| format!("Cannot write {shown_path}: {error}."))?;
    Ok((format!("Successfully wrote to {shown_path}"), shown_path))
}

/// Creates the final target's parent directories for a write or an initial upload.
pub(crate) fn create_target_parents(path: &Path) -> io::Result<PathBuf> {
    let target = write_target(path)?;
    if let Some(parent) = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    Ok(target)
}

/// Follows a final-component symlink without replacing the link itself, including a dangling
/// link whose target can be created. Parent-directory symlinks keep their normal filesystem
/// meaning. A finite walk also refuses cycles before any file is changed.
pub(crate) fn write_target(path: &Path) -> io::Result<PathBuf> {
    let mut target = path.to_path_buf();
    for depth in 0..=40 {
        if ends_in_directory(&target) {
            return Err(io::Error::other(
                "the path requires a directory, not a regular file",
            ));
        }
        match std::fs::symlink_metadata(&target) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                // Walking the final links individually would reset the OS's whole-path
                // traversal limit on each step, including links in parent directories.
                if depth == 0
                    && let Err(error) = std::fs::metadata(path)
                    && error.kind() != io::ErrorKind::NotFound
                {
                    return Err(error);
                }
                let link = std::fs::read_link(&target)?;
                target = if link.is_absolute() {
                    link
                } else {
                    append_path(target.parent().unwrap_or_else(|| Path::new(".")), &link)
                };
            }
            Ok(_) => return Ok(target),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(target),
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "too many symbolic links",
    ))
}

/// A fully staged replacement, removed on every failure. Closing before removal also makes
/// cleanup work on Windows.
struct Staged {
    path: PathBuf,
    file: Option<File>,
    remove_on_drop: bool,
    #[cfg(target_os = "freebsd")]
    directory: Option<PathBuf>,
}

impl Drop for Staged {
    fn drop(&mut self) {
        drop(self.file.take());
        if self.remove_on_drop {
            let _ = std::fs::remove_file(&self.path);
        }
        #[cfg(target_os = "freebsd")]
        if let Some(directory) = &self.directory {
            let _ = std::fs::remove_dir(directory);
        }
    }
}

impl Staged {
    fn new(parent: &Path, private: bool) -> io::Result<Self> {
        for _ in 0..32 {
            let mut random = [0u8; 16];
            getrandom::fill(&mut random).map_err(|error| io::Error::other(error.to_string()))?;
            let name = format!(".fastexec-save-{:032x}", u128::from_le_bytes(random));
            let path = append_path(parent, Path::new(&name));
            #[cfg(target_os = "freebsd")]
            let directory = if private {
                match crate::runtime::create_private_new(&path) {
                    Ok(()) => Some(path.clone()),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(error) => return Err(error),
                }
            } else {
                None
            };
            #[cfg(target_os = "freebsd")]
            let path = directory.as_ref().map_or(path, |dir| dir.join("content"));
            match create_staging_file(&path, private) {
                Ok(file) => {
                    return Ok(Self {
                        path,
                        file: Some(file),
                        remove_on_drop: true,
                        #[cfg(target_os = "freebsd")]
                        directory,
                    });
                }
                Err(error) => {
                    #[cfg(target_os = "freebsd")]
                    if let Some(directory) = directory {
                        let _ = std::fs::remove_dir(directory);
                    }
                    if error.kind() != io::ErrorKind::AlreadyExists {
                        return Err(error);
                    }
                }
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not create a unique staging file",
        ))
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
fn create_staging_file(path: &Path, private: bool) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(if private { 0o600 } else { 0o666 })
        .open(path)
}

/// Darwin ACL grants are independent of the mode bits. Suppress inheritance in the creation
/// syscall itself, so another user cannot open an inherited read handle to staged content.
#[cfg(target_os = "macos")]
fn create_staging_file(path: &Path, private: bool) -> io::Result<File> {
    if private {
        macos_access::create_private(path)
    } else {
        OpenOptions::new().write(true).create_new(true).open(path)
    }
}

/// Extended ACLs are not among Darwin's public xattrs. Apple Libc uses fstatx/fchmodx for the
/// ACL APIs, and XNU honors ACL_FLAG_NO_INHERIT on an initial openx_np security descriptor.
#[cfg(target_os = "macos")]
mod macos_access {
    use super::*;
    use std::ffi::{CString, c_char, c_int, c_void};
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;

    const ACL_TYPE_EXTENDED: c_int = 0x100;
    const ACL_FLAG_NO_INHERIT: c_int = 1 << 17;
    const FILESEC_OWNER: c_int = 1;
    const FILESEC_GROUP: c_int = 2;
    const FILESEC_MODE: c_int = 4;
    const FILESEC_ACL: c_int = 5;

    // These public Darwin interfaces are in libSystem but are not declared by the libc crate.
    unsafe extern "C" {
        fn acl_init(count: c_int) -> *mut c_void;
        fn acl_free(acl: *mut c_void) -> c_int;
        fn acl_get_fd_np(fd: c_int, kind: c_int) -> *mut c_void;
        fn acl_get_entry(acl: *mut c_void, id: c_int, entry: *mut *mut c_void) -> c_int;
        fn acl_get_flagset_np(acl: *mut c_void, flags: *mut *mut c_void) -> c_int;
        fn acl_add_flag_np(flags: *mut c_void, flag: c_int) -> c_int;
        fn filesec_init() -> *mut c_void;
        fn filesec_free(security: *mut c_void);
        fn filesec_set_property(
            security: *mut c_void,
            property: c_int,
            value: *const c_void,
        ) -> c_int;
        fn openx_np(path: *const c_char, flags: c_int, security: *mut c_void) -> c_int;
        fn fchmodx_np(fd: c_int, security: *mut c_void) -> c_int;
        #[cfg(test)]
        fn acl_to_text(acl: *mut c_void, length: *mut libc::ssize_t) -> *mut c_char;
    }

    struct Acl(*mut c_void);

    impl Drop for Acl {
        fn drop(&mut self) {
            // SAFETY: Acl exclusively owns an allocation returned by a Darwin ACL function.
            unsafe { acl_free(self.0) };
        }
    }

    impl Acl {
        fn empty(no_inherit: bool) -> io::Result<Self> {
            // SAFETY: zero entries creates an independently allocated, empty ACL.
            let raw = unsafe { acl_init(0) };
            if raw.is_null() {
                return Err(io::Error::last_os_error());
            }
            let acl = Self(raw);
            if no_inherit {
                let mut flags = std::ptr::null_mut();
                // SAFETY: acl owns a live ACL and flags points into it until it is freed.
                if unsafe { acl_get_flagset_np(acl.0, &mut flags) } != 0
                    || unsafe { acl_add_flag_np(flags, ACL_FLAG_NO_INHERIT) } != 0
                {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(acl)
        }

        fn read(file: &File) -> io::Result<Option<Self>> {
            // SAFETY: the descriptor remains open for this synchronous metadata query.
            let raw = unsafe { acl_get_fd_np(file.as_raw_fd(), ACL_TYPE_EXTENDED) };
            if !raw.is_null() {
                return Ok(Some(Self(raw)));
            }
            let error = io::Error::last_os_error();
            // With a live descriptor, Darwin's FILESEC_ACL property reports ENOENT when no
            // ACL exists. Other errors cannot establish absence and must leave the target alone.
            if error.raw_os_error() == Some(libc::ENOENT) {
                Ok(None)
            } else {
                Err(error)
            }
        }

        fn has_entries(&self) -> io::Result<bool> {
            let mut entry = std::ptr::null_mut();
            // SAFETY: ACL_FIRST_ENTRY is 0; Darwin returns 0 for an entry, -1/EINVAL at the end.
            if unsafe { acl_get_entry(self.0, 0, &mut entry) } == 0 {
                return Ok(true);
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINVAL) {
                Ok(false)
            } else {
                Err(error)
            }
        }
    }

    struct Security(*mut c_void);

    impl Drop for Security {
        fn drop(&mut self) {
            // SAFETY: Security owns a successful filesec_init allocation.
            unsafe { filesec_free(self.0) };
        }
    }

    impl Security {
        fn new() -> io::Result<Self> {
            // SAFETY: filesec_init returns a new descriptor, or null on failure.
            let raw = unsafe { filesec_init() };
            if raw.is_null() {
                Err(io::Error::last_os_error())
            } else {
                Ok(Self(raw))
            }
        }

        fn set<T>(&self, property: c_int, value: &T) -> io::Result<()> {
            // SAFETY: callers supply the documented type; filesec copies each value/ACL.
            if unsafe { filesec_set_property(self.0, property, (value as *const T).cast()) } != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
    }

    pub(super) fn create_private(path: &Path) -> io::Result<File> {
        let path_c = CString::new(path.as_os_str().as_bytes()).map_err(io::Error::other)?;
        let acl = Acl::empty(true)?;
        let security = Security::new()?;
        let mode: libc::mode_t = 0o600;
        security.set(FILESEC_MODE, &mode)?;
        security.set(FILESEC_ACL, &acl.0)?;
        // SAFETY: path and security live through the call. Exclusive creation refuses every
        // existing name; the restrictive mode and non-inheriting ACL take effect atomically.
        let fd = unsafe {
            openx_np(
                path_c.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                security.0,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful exclusive openx_np returned a new owned descriptor.
        let file = unsafe { File::from_raw_fd(fd) };
        let verify = (|| {
            let metadata = file.metadata()?;
            // SAFETY: geteuid has no arguments and no side effects.
            if !metadata.is_file()
                || metadata.uid() != unsafe { libc::geteuid() }
                || metadata.mode() & 0o077 != 0
            {
                return Err(io::Error::other("the staging file is not private"));
            }
            let mut filesystem = std::mem::MaybeUninit::<libc::statfs>::uninit();
            // SAFETY: the descriptor is live and fstatfs initializes the output on success.
            if unsafe { libc::fstatfs(file.as_raw_fd(), filesystem.as_mut_ptr()) } != 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: successful fstatfs initialized every field.
            if unsafe { filesystem.assume_init() }.f_flags & libc::MNT_IGNORE_OWNERSHIP as u32 != 0
            {
                return Err(io::Error::other(
                    "the volume ignores ownership and cannot keep staging private",
                ));
            }
            if let Some(acl) = Acl::read(&file)?
                && acl.has_entries()?
            {
                return Err(io::Error::other(
                    "the staging file inherited an access-control entry",
                ));
            }
            Ok(())
        })();
        if let Err(error) = verify {
            // No content has been written, including on a filesystem that ignored the ACL.
            drop(file);
            let _ = std::fs::remove_file(path);
            return Err(error);
        }
        Ok(file)
    }

    pub(super) fn preserve(source: &File, staged: &File) -> io::Result<()> {
        let acl = match Acl::read(source)? {
            Some(acl) => acl,
            // An empty ACL replaces every staging entry, including any inherited grant.
            None => Acl::empty(false)?,
        };
        let metadata = source.metadata()?;
        let mode = (metadata.mode() & 0o7777) as libc::mode_t;
        let security = Security::new()?;
        security.set(FILESEC_OWNER, &metadata.uid())?;
        security.set(FILESEC_GROUP, &metadata.gid())?;
        security.set(FILESEC_MODE, &mode)?;
        security.set(FILESEC_ACL, &acl.0)?;
        // SAFETY: the live file and security descriptor cover this synchronous operation.
        // Set mode, ownership and ACL together: chmod(0644) followed by copying a deny ACL
        // would briefly expose completed content, as would chown before restoring a mode of 0.
        if unsafe { fchmodx_np(staged.as_raw_fd(), security.0) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn acl_text(file: &File) -> io::Result<String> {
        let Some(acl) = Acl::read(file)? else {
            return Ok(String::new());
        };
        // SAFETY: the ACL is live; a null length pointer is allowed by acl_to_text.
        let text = unsafe { acl_to_text(acl.0, std::ptr::null_mut()) };
        if text.is_null() {
            return Err(io::Error::last_os_error());
        }
        let allocation = Acl(text.cast());
        // SAFETY: acl_to_text returns a NUL-terminated string owned by allocation.
        let result = unsafe { std::ffi::CStr::from_ptr(text) }
            .to_string_lossy()
            .into_owned();
        drop(allocation);
        Ok(result)
    }

    #[cfg(test)]
    pub(super) fn has_entries(file: &File) -> io::Result<bool> {
        Acl::read(file)?.map_or(Ok(false), |acl| acl.has_entries())
    }
}

/// Install the restrictive DACL in CreateFile itself. Tightening an inherited DACL after opening
/// an empty file would still let another user open a handle before it is tightened.
#[cfg(windows)]
fn create_staging_file(path: &Path, private: bool) -> io::Result<File> {
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::FromRawHandle;
    use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows_sys::Win32::Storage::FileSystem::{
        CREATE_NEW, CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_GENERIC_WRITE, FILE_READ_ATTRIBUTES,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, WRITE_DAC, WRITE_OWNER,
    };
    if !private {
        return OpenOptions::new().write(true).create_new(true).open(path);
    }
    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let attributes = crate::transport::SecurityAttributes::owner_only(false)?;
    let handle = attributes.with_raw(|raw| {
        // SAFETY: the path and owner-only descriptor live through the synchronous call.
        unsafe {
            CreateFileW(
                wide.as_ptr(),
                FILE_GENERIC_WRITE | FILE_READ_ATTRIBUTES | WRITE_DAC | WRITE_OWNER,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                raw.cast::<windows_sys::Win32::Security::SECURITY_ATTRIBUTES>()
                    .cast_const(),
                CREATE_NEW,
                FILE_ATTRIBUTE_NORMAL,
                std::ptr::null_mut(),
            )
        }
    });
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful CREATE_NEW returned this new, uniquely owned handle.
    Ok(unsafe { File::from_raw_handle(handle) })
}

/// Replacing one hard-linked name would silently detach it from its peers. There is no atomic
/// way to rewrite the shared inode, so require an explicit in-place update through exec instead.
fn single_link(file: &File) -> io::Result<()> {
    #[cfg(unix)]
    let links = {
        use std::os::unix::fs::MetadataExt;
        file.metadata()?.nlink()
    };
    #[cfg(windows)]
    let links = {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
        };
        // SAFETY: the live file owns the handle and the structure is a writable output buffer.
        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 {
            return Err(io::Error::last_os_error());
        }
        u64::from(info.nNumberOfLinks)
    };
    if links > 1 {
        return Err(io::Error::other(format!(
            "the file has {links} hard links; an atomic replacement would split them. Use exec for an explicit in-place update"
        )));
    }
    Ok(())
}

/// Preserves an existing Unix file's owner before restoring its mode. A new inode owned by the
/// server would otherwise take a user's file away from them when a privileged server edits it.
#[cfg(all(unix, not(any(target_os = "macos", target_os = "freebsd"))))]
fn preserve_owner(file: &File, original: &Metadata) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::MetadataExt;
    let current = file.metadata()?;
    if current.uid() != original.uid() || current.gid() != original.gid() {
        // SAFETY: fchown acts on the live staging file, not a path another process can replace.
        if unsafe { libc::fchown(file.as_raw_fd(), original.uid(), original.gid()) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Keeps ACLs and extended attributes on the Unix platforms supported by the server. Linux
/// file capabilities are deliberately not restored: a content write normally clears them.
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn preserve_xattrs(original: &File, staged: &File) -> io::Result<()> {
    use std::ffi::CStr;
    use std::os::fd::AsRawFd;
    let (from, to) = (original.as_raw_fd(), staged.as_raw_fd());
    let list = |fd, buffer: *mut libc::c_char, size: usize| {
        #[cfg(target_os = "linux")]
        // SAFETY: the caller supplies a writable buffer of size bytes, or null for a size query.
        unsafe {
            libc::flistxattr(fd, buffer, size)
        }
        #[cfg(target_os = "macos")]
        // SAFETY: same contract, with no platform-specific flags.
        unsafe {
            libc::flistxattr(fd, buffer, size, 0)
        }
    };
    let count = list(from, std::ptr::null_mut(), 0);
    if count < 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ENOTSUP) {
            return Ok(());
        }
        return Err(error);
    }
    let mut names = vec![0u8; count as usize];
    if !names.is_empty() {
        let count = list(from, names.as_mut_ptr().cast(), names.len());
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        names.truncate(count as usize);
    }
    // The sibling may inherit a default ACL or other directory attributes the original does
    // not have. Remove those first; chmod alone could otherwise activate extra named ACL users.
    let count = list(to, std::ptr::null_mut(), 0);
    if count < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut inherited = vec![0u8; count as usize];
    if !inherited.is_empty() {
        let count = list(to, inherited.as_mut_ptr().cast(), inherited.len());
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        inherited.truncate(count as usize);
    }
    for name in inherited.split_inclusive(|&byte| byte == 0) {
        if names
            .split_inclusive(|&byte| byte == 0)
            .any(|original| original == name)
        {
            continue;
        }
        let name = CStr::from_bytes_with_nul(name).map_err(io::Error::other)?;
        #[cfg(target_os = "linux")]
        // SAFETY: the descriptor is live and the attribute name is terminated.
        let removed = unsafe { libc::fremovexattr(to, name.as_ptr()) };
        #[cfg(target_os = "macos")]
        // SAFETY: same contract, with no platform-specific flags.
        let removed = unsafe { libc::fremovexattr(to, name.as_ptr(), 0) };
        if removed != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    for name in names.split_inclusive(|&byte| byte == 0) {
        let name = CStr::from_bytes_with_nul(name).map_err(io::Error::other)?;
        #[cfg(target_os = "linux")]
        if name.to_bytes() == b"security.capability" {
            continue;
        }
        let read = |buffer: *mut libc::c_void, size: usize| {
            #[cfg(target_os = "linux")]
            // SAFETY: name is terminated and buffer follows the same query/read contract above.
            unsafe {
                libc::fgetxattr(from, name.as_ptr(), buffer, size)
            }
            #[cfg(target_os = "macos")]
            // SAFETY: same contract, with position and flags both zero.
            unsafe {
                libc::fgetxattr(from, name.as_ptr(), buffer, size, 0, 0)
            }
        };
        let size = read(std::ptr::null_mut(), 0);
        if size < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut value = vec![0u8; size as usize];
        let size = read(value.as_mut_ptr().cast(), value.len());
        if size < 0 {
            return Err(io::Error::last_os_error());
        }
        if size as usize > value.len() {
            return Err(io::Error::other(
                "an extended attribute changed while it was being copied",
            ));
        }
        #[cfg(target_os = "linux")]
        // SAFETY: the names and buffers remain valid for the synchronous call.
        let saved =
            unsafe { libc::fsetxattr(to, name.as_ptr(), value.as_ptr().cast(), size as usize, 0) };
        #[cfg(target_os = "macos")]
        // SAFETY: same contract, with position and flags both zero.
        let saved = unsafe {
            libc::fsetxattr(
                to,
                name.as_ptr(),
                value.as_ptr().cast(),
                size as usize,
                0,
                0,
            )
        };
        if saved != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Restore access only after the staging content is complete. ReplaceFile preserves DACLs but
/// does not promise to preserve the owner or group, so copy those explicitly before publishing.
/// A protected staging DACL keeps directory inheritance from widening access to an existing file.
#[cfg(windows)]
fn preserve_windows_access(original: &File, staged: &File, protect: bool) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        GetSecurityInfo, SE_FILE_OBJECT, SetSecurityInfo,
    };
    use windows_sys::Win32::Security::{
        ACL, DACL_SECURITY_INFORMATION, EqualSid, GROUP_SECURITY_INFORMATION,
        GetSecurityDescriptorControl, OWNER_SECURITY_INFORMATION,
        PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SE_DACL_PROTECTED,
        UNPROTECTED_DACL_SECURITY_INFORMATION,
    };
    use windows_sys::Win32::Storage::FileSystem::GetVolumeInformationByHandleW;
    let mut filesystem_flags = 0u32;
    // A volume query is only an optimization for filesystems without ACLs. SMB does not
    // support this API, so an unavailable query must still try the file-security APIs below.
    // SAFETY: only filesystem flags are requested from the volume holding the live handle.
    let volume_known = unsafe {
        GetVolumeInformationByHandleW(
            original.as_raw_handle(),
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut filesystem_flags,
            std::ptr::null_mut(),
            0,
        )
    } != 0;
    // FILE_PERSISTENT_ACLS: FAT/exFAT have no security descriptor to preserve.
    if volume_known && filesystem_flags & 0x00000008 == 0 {
        return Ok(());
    }
    struct Security {
        descriptor: PSECURITY_DESCRIPTOR,
        owner: PSID,
        group: PSID,
        dacl: *mut ACL,
    }
    impl Drop for Security {
        fn drop(&mut self) {
            if !self.descriptor.is_null() {
                // SAFETY: GetSecurityInfo allocated this descriptor with LocalAlloc.
                unsafe { LocalFree(self.descriptor) };
            }
        }
    }
    let read = |file: &File, information| -> io::Result<Security> {
        let mut security = Security {
            descriptor: std::ptr::null_mut(),
            owner: std::ptr::null_mut(),
            group: std::ptr::null_mut(),
            dacl: std::ptr::null_mut(),
        };
        // SAFETY: the file owns the handle and every output is writable; the returned SIDs and
        // ACL remain owned by the descriptor until Security is dropped.
        let result = unsafe {
            GetSecurityInfo(
                file.as_raw_handle(),
                SE_FILE_OBJECT,
                information,
                &mut security.owner,
                &mut security.group,
                &mut security.dacl,
                std::ptr::null_mut(),
                &mut security.descriptor,
            )
        };
        if result != ERROR_SUCCESS {
            return Err(io::Error::from_raw_os_error(result as i32));
        }
        if security.owner.is_null() || security.group.is_null() {
            return Err(io::Error::other("the file's owner or group is missing"));
        }
        Ok(security)
    };
    let ownership = OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION;
    let source = read(original, ownership | DACL_SECURITY_INFORMATION)?;
    let current = read(staged, ownership)?;
    let (mut control, mut revision) = (0u16, 0u32);
    // SAFETY: GetSecurityInfo returned a live security descriptor.
    if unsafe { GetSecurityDescriptorControl(source.descriptor, &mut control, &mut revision) } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let inheritance = if protect || control & SE_DACL_PROTECTED != 0 {
        PROTECTED_DACL_SECURITY_INFORMATION
    } else {
        UNPROTECTED_DACL_SECURITY_INFORMATION
    };
    let mut information = DACL_SECURITY_INFORMATION | inheritance;
    // Avoid an unnecessary ownership change when normal creation already chose the same SID.
    // SAFETY: each non-null SID came from its still-live security descriptor.
    if unsafe { EqualSid(source.owner, current.owner) } == 0 {
        information |= OWNER_SECURITY_INFORMATION;
    }
    // SAFETY: same live-descriptor contract for the primary group SIDs.
    if unsafe { EqualSid(source.group, current.group) } == 0 {
        information |= GROUP_SECURITY_INFORMATION;
    }
    // SAFETY: these SIDs and ACL remain alive during this synchronous file-security call. A
    // caller unable to preserve a different owner/group fails here, before touching the target.
    let result = unsafe {
        SetSecurityInfo(
            staged.as_raw_handle(),
            SE_FILE_OBJECT,
            information,
            source.owner,
            source.group,
            source.dacl,
            std::ptr::null(),
        )
    };
    if result != ERROR_SUCCESS {
        return Err(io::Error::from_raw_os_error(result as i32));
    }
    Ok(())
}

/// The documented ReplaceFile failure state in which the old file has already moved to the
/// backup. Keep this recovery logic independent of Win32 so its destructive failure paths can
/// also be exercised on the other supported platforms.
#[cfg(any(windows, test))]
fn replace_with_backup(
    target: &Path,
    replace: impl FnOnce(&Path) -> io::Result<()>,
    restore: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let parent = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    // Reserve a unique, owner-only name. ReplaceFile replaces this empty placeholder with the
    // original file; no previously existing user's backup is overwritten.
    let mut backup = Staged::new(parent, true)?;
    drop(backup.file.take());
    // A panic or an unexpected unwind must never remove a backup after the native call moved
    // the original into it. Re-enable cleanup only in states that leave a complete target.
    backup.remove_on_drop = false;
    match replace(&backup.path) {
        Ok(()) => {
            backup.remove_on_drop = true;
            Ok(())
        }
        // ERROR_UNABLE_TO_MOVE_REPLACEMENT_2: the original is now at the supplied backup path.
        Err(error) if error.raw_os_error() == Some(1177) => match restore(&backup.path) {
            Ok(()) => {
                backup.remove_on_drop = true;
                Err(error)
            }
            Err(restore_error) => Err(io::Error::new(
                error.kind(),
                format!(
                    "could not replace {} ({error}) or restore it ({restore_error}); the original file is preserved at {}",
                    display_path(target),
                    display_path(&backup.path)
                ),
            )),
        },
        // With a non-null backup path, 1176 and all other documented failures keep the original
        // at its original name. Only an unused backup placeholder needs cleanup in these states.
        Err(error) => {
            backup.remove_on_drop = true;
            Err(error)
        }
    }
}

/// A no-replace move works on filesystems without hard links and cannot overwrite a file that
/// another writer created during a failed replacement's rollback.
#[cfg(windows)]
fn move_new_windows(source: &Path, target: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::MoveFileExW;
    let source: Vec<u16> = source.as_os_str().encode_wide().chain(Some(0)).collect();
    let target: Vec<u16> = target.as_os_str().encode_wide().chain(Some(0)).collect();
    // SAFETY: both paths are terminated; no REPLACE_EXISTING or COPY_ALLOWED flags are set.
    if unsafe { MoveFileExW(source.as_ptr(), target.as_ptr(), 0) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(windows)]
fn windows_file_identity(file: &File) -> io::Result<(u32, u32, u32)> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT,
        GetFileInformationByHandle,
    };
    // SAFETY: the live file owns this handle and info is a writable output buffer.
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 {
        return Err(io::Error::last_os_error());
    }
    if info.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT) != 0 {
        return Err(io::Error::other("the target is not a regular file"));
    }
    Ok((
        info.dwVolumeSerialNumber,
        info.nFileIndexHigh,
        info.nFileIndexLow,
    ))
}

#[cfg(windows)]
fn unchanged_windows_target(original: &File, target: &Path) -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES,
    };
    let now = OpenOptions::new()
        .read(true)
        .access_mode(FILE_READ_ATTRIBUTES)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(target)?;
    if windows_file_identity(original)? != windows_file_identity(&now)? {
        return Err(io::Error::other(
            "the target changed while the replacement was being saved",
        ));
    }
    Ok(())
}

/// Publishes a complete replacement. Windows ReplaceFile preserves the existing DACL and
/// alternate streams; an ordinary rename would discard them. Ownership was copied beforehand.
fn publish(staged: &Path, target: &Path, exists: bool) -> io::Result<()> {
    #[cfg(unix)]
    {
        if exists {
            return std::fs::rename(staged, target);
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            use std::ffi::CString;
            use std::os::unix::ffi::OsStrExt;
            let source = CString::new(staged.as_os_str().as_bytes()).map_err(io::Error::other)?;
            let destination =
                CString::new(target.as_os_str().as_bytes()).map_err(io::Error::other)?;
            #[cfg(target_os = "linux")]
            // Use the kernel entry point: the static musl shipped by supported Rust toolchains
            // need not export the newer renameat2 C wrapper.
            // SAFETY: both paths are terminated; the arguments match Linux renameat2's ABI.
            let moved = unsafe {
                libc::syscall(
                    libc::SYS_renameat2,
                    libc::AT_FDCWD,
                    source.as_ptr(),
                    libc::AT_FDCWD,
                    destination.as_ptr(),
                    libc::RENAME_NOREPLACE,
                )
            };
            #[cfg(target_os = "macos")]
            // SAFETY: both paths are terminated and RENAME_EXCL refuses an existing name.
            let moved = unsafe {
                libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL)
            };
            if moved == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if !matches!(
                error.raw_os_error(),
                Some(libc::ENOSYS | libc::EINVAL | libc::ENOTSUP)
            ) {
                return Err(error);
            }
        }
        // Older kernels/filesystems can still publish the complete inode with a link. Neither
        // route overwrites a file or a symlink created meanwhile.
        std::fs::hard_link(staged, target)
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::ReplaceFileW;
        if !exists {
            return move_new_windows(staged, target);
        }
        let wide_target: Vec<u16> = target.as_os_str().encode_wide().chain(Some(0)).collect();
        let wide_staged: Vec<u16> = staged.as_os_str().encode_wide().chain(Some(0)).collect();
        replace_with_backup(
            target,
            |backup| {
                let backup: Vec<u16> = backup.as_os_str().encode_wide().chain(Some(0)).collect();
                // SAFETY: all three terminated paths remain live for the call. Supplying a backup
                // prevents ERROR_UNABLE_TO_MOVE_REPLACEMENT (1176) from deleting the original.
                if unsafe {
                    ReplaceFileW(
                        wide_target.as_ptr(),
                        wide_staged.as_ptr(),
                        backup.as_ptr(),
                        0,
                        std::ptr::null(),
                        std::ptr::null(),
                    )
                } == 0
                {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            },
            |backup| move_new_windows(backup, target),
        )
    }
}

/// An edit keeps the file it read separate from the path that can be changed meanwhile.
struct EditSource {
    path: PathBuf,
    target: PathBuf,
    file: File,
    content: String,
}

impl EditSource {
    fn read(path: PathBuf) -> io::Result<Self> {
        // A canonicalizer can discard directory requirements that the native open enforces.
        let (mut file, _) = open_regular(&path)?;
        let target = std::fs::canonicalize(&path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let content = String::from_utf8(bytes)
            .map_err(|_| io::Error::other("the file is not valid UTF-8; edit it with exec"))?;
        Ok(Self {
            path,
            target,
            file,
            content,
        })
    }

    fn changed() -> io::Error {
        io::Error::other("the file changed after it was read; read it again and retry the edit")
    }

    fn verify_target(&self, current: Option<&File>) -> io::Result<()> {
        let current = current.ok_or_else(Self::changed)?;
        let (named, _) = open_regular(&self.path)?;
        if std::fs::canonicalize(&self.path)? != self.target {
            return Err(Self::changed());
        }
        #[cfg(unix)]
        let identity = |file: &File| {
            use std::os::unix::fs::MetadataExt;
            file.metadata()
                .map(|metadata| (metadata.dev(), metadata.ino()))
        };
        #[cfg(windows)]
        let identity = windows_file_identity;
        let original = identity(&self.file)?;
        if original != identity(current)? || original != identity(&named)? {
            return Err(Self::changed());
        }
        Ok(())
    }

    fn verify_content(&self, current: &mut File) -> io::Result<()> {
        current.rewind()?;
        let mut offset = 0;
        let mut buffer = [0; 8192];
        loop {
            let read = current.read(&mut buffer)?;
            if read == 0 {
                return if offset == self.content.len() {
                    Ok(())
                } else {
                    Err(Self::changed())
                };
            }
            if self.content.as_bytes().get(offset..offset + read) != Some(&buffer[..read]) {
                return Err(Self::changed());
            }
            offset += read;
        }
    }

    fn save(&self, bytes: &[u8]) -> io::Result<()> {
        save_target(self.target.clone(), bytes, Some(self))
    }
}

/// Stage the complete replacement before publishing, and reject an edit of a changed file.
fn save_target(target: PathBuf, bytes: &[u8], source: Option<&EditSource>) -> io::Result<()> {
    let mut original = match std::fs::symlink_metadata(&target) {
        Ok(metadata) => {
            if !metadata.is_file() {
                return Err(io::Error::other("the target is not a regular file"));
            }
            let mut options = OpenOptions::new();
            options.write(true).read(source.is_some());
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                // A replaced final symlink is not followed, and a raced FIFO cannot block.
                options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
            }
            #[cfg(windows)]
            {
                use std::os::windows::fs::OpenOptionsExt;
                use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
                options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
            }
            let file = options.open(&target)?;
            #[cfg(windows)]
            windows_file_identity(&file)?;
            if !file.metadata()?.is_file() {
                return Err(io::Error::other("the target is not a regular file"));
            }
            single_link(&file)?;
            Some(file)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    if let Some(source) = source {
        source.verify_target(original.as_ref())?;
    }
    let metadata = original.as_ref().map(File::metadata).transpose()?;
    let parent = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut staged = Staged::new(parent, true)?;
    // An empty probe obtains the ordinary create mode/DACL under this directory without ever
    // exposing staged content, and without changing the process-global Unix umask.
    let defaults = if metadata.is_none() {
        Some(Staged::new(parent, false)?)
    } else {
        None
    };
    let file = staged.file.as_mut().expect("a new staging file is open");
    #[cfg(not(any(target_os = "macos", target_os = "freebsd")))]
    let permissions = match &metadata {
        Some(metadata) => metadata.permissions(),
        None => defaults
            .as_ref()
            .unwrap()
            .file
            .as_ref()
            .unwrap()
            .metadata()?
            .permissions(),
    };
    file.write_all(bytes)?;
    #[cfg(windows)]
    {
        let source = original
            .as_ref()
            .or_else(|| defaults.as_ref().and_then(|probe| probe.file.as_ref()))
            .unwrap();
        preserve_windows_access(source, file, original.is_some())?;
    }
    #[cfg(all(unix, not(any(target_os = "macos", target_os = "freebsd"))))]
    if let Some(metadata) = &metadata {
        preserve_owner(file, metadata)?;
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    if let Some(original) = &original {
        preserve_xattrs(original, file)?;
    }
    #[cfg(not(any(target_os = "macos", target_os = "freebsd")))]
    file.set_permissions(permissions)?;
    #[cfg(target_os = "freebsd")]
    {
        let source = original
            .as_ref()
            .or_else(|| defaults.as_ref().and_then(|probe| probe.file.as_ref()))
            .unwrap();
        crate::freebsd_fs::preserve(source, file, original.is_some())?;
    }
    #[cfg(target_os = "macos")]
    {
        let source = original
            .as_ref()
            .or_else(|| defaults.as_ref().and_then(|probe| probe.file.as_ref()))
            .unwrap();
        macos_access::preserve(source, file)?;
    }
    file.sync_all()?;
    if let Some(source) = source {
        source.verify_content(original.as_mut().ok_or_else(EditSource::changed)?)?;
        source.verify_target(original.as_ref())?;
    }
    if let Some(original) = &original {
        single_link(original)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let before = original.metadata()?;
            let now = std::fs::symlink_metadata(&target)?;
            if !now.is_file() || before.dev() != now.dev() || before.ino() != now.ino() {
                return Err(io::Error::other(
                    "the target changed while the replacement was being saved",
                ));
            }
        }
        #[cfg(windows)]
        unchanged_windows_target(original, &target)?;
    }
    drop(staged.file.take());
    publish(&staged.path, &target, original.is_some())
}

/// One replacement matched against the content it is applied to.
struct Matched<'a> {
    /// The edit's position in the request, which names it in errors and in the result.
    index: usize,
    /// Where it matched in the content, and how long the matched text there is.
    at: usize,
    len: usize,
    /// Whether only the fuzzy form matched.
    fuzzy: bool,
    new_text: &'a str,
}

const MAX_REPORTED_ITEMS: usize = 20;

enum EntryProblem {
    Ambiguous { count: usize, lines: Vec<usize> },
    NotFound { lines: Vec<usize>, more: usize },
}

/// Applies every edit to one file: exact matches first, and the fuzzy form when any edit needs it.
fn edit_outcome(
    request: &EditRequest,
    launch: &LaunchContext,
) -> Result<(String, Value), (String, Option<Value>)> {
    if request.edits.is_empty() {
        return Err((
            "Edit tool input is invalid. edits must contain at least one replacement.".to_owned(),
            None,
        ));
    }
    for (index, edit) in request.edits.iter().enumerate() {
        if normalize_to_lf(&edit.old_text).is_empty() {
            return Err((format!("edits[{index}].oldText must not be empty."), None));
        }
        if edit.start_line == Some(0) {
            return Err((
                format!("edits[{index}].startLine must be at least 1."),
                None,
            ));
        }
        if edit.end_line == Some(0) {
            return Err((format!("edits[{index}].endLine must be at least 1."), None));
        }
        if let (Some(start), Some(end)) = (edit.start_line, edit.end_line)
            && start > end
        {
            return Err((
                format!("edits[{index}].startLine ({start}) must not exceed endLine ({end})."),
                None,
            ));
        }
    }
    let resolved = resolve(&request.path, launch).map_err(|msg| (msg, None))?;
    let shown_path = display_path(&resolved);
    let _guard = lock();
    let source = EditSource::read(resolved)
        .map_err(|error| (format!("Could not edit file: {shown_path}. {error}."), None))?;
    let (mut written, starts, fuzzy_flags) = apply_edits(&source.content, request, &shown_path)?;
    if source.content.starts_with('\u{feff}') && !written.starts_with('\u{feff}') {
        written.insert(0, '\u{feff}');
    }
    if written == source.content {
        return Err(edit_no_change(&shown_path));
    }
    source
        .save(written.as_bytes())
        .map_err(|error| (format!("Could not edit file: {shown_path}. {error}."), None))?;
    let mut text = format!(
        "Successfully replaced {} block(s) in {shown_path}.",
        request.edits.len(),
    );
    let mut edits = Vec::with_capacity(request.edits.len());
    for (index, (&at, &fuzzy)) in starts.iter().zip(&fuzzy_flags).enumerate() {
        let line = 1 + line_breaks(&source.content[..at]);
        text.push_str(&format!("\nedits[{index}]: line {line}"));
        if fuzzy {
            text.push_str(" (fuzzy match)");
        }
        edits.push(json!({ "line": line, "fuzzy": fuzzy }));
    }
    let structured = json!({
        "ok": true,
        "path": shown_path,
        "replacements": request.edits.len(),
        "edits": edits,
    });
    Ok((text, structured))
}

#[cfg(test)]
fn edit_text(request: &EditRequest, launch: &LaunchContext) -> Result<String, String> {
    edit_outcome(request, launch)
        .map(|(text, _)| text)
        .map_err(|(msg, _)| msg)
}

fn edit_no_change(path: &str) -> (String, Option<Value>) {
    let error = format!(
        "Cannot edit {path}; nothing was changed: the replacement produced identical content."
    );
    let structured = json!({
        "ok": false,
        "error": error,
        "path": path,
        "failures": [],
    });
    (error, Some(structured))
}

/// Matches in LF-normalized text, collects every entry failure (ambiguous, not_found, overlap),
/// and replaces the corresponding original byte ranges when every entry succeeds.
#[allow(clippy::type_complexity)]
fn apply_edits(
    content: &str,
    request: &EditRequest,
    path: &str,
) -> Result<(String, Vec<usize>, Vec<bool>), (String, Option<Value>)> {
    let total = request.edits.len();
    let normalized = LfText::new(content);
    let ending = detect_line_ending(content);
    let raw_newlines: Vec<usize> = content
        .bytes()
        .enumerate()
        .filter_map(|(i, b)| (b == b'\n').then_some(i))
        .collect();
    let line_at_raw = |raw_pos: usize| 1 + raw_newlines.partition_point(|&nl| nl < raw_pos);
    let line_at_norm = |norm_pos: usize| line_at_raw(normalized.original(norm_pos));

    let mut lazy_fuzzy: Option<(String, Vec<usize>, std::cell::OnceCell<Option<FuzzyMap>>)> = None;
    let edits: Vec<(String, String)> = request
        .edits
        .iter()
        .map(|edit| {
            (
                normalize_to_lf(&edit.old_text),
                normalize_to_lf(&edit.new_text),
            )
        })
        .collect();
    let inserted: Vec<String> = edits
        .iter()
        .map(|(_, new)| restore_line_endings(new, ending))
        .collect();

    let mut problems: Vec<Option<EntryProblem>> = (0..total).map(|_| None).collect();
    let mut matched: Vec<Matched<'_>> = Vec::new();
    let mut fuzzy_flags = vec![false; total];

    for (index, (old, new)) in edits.iter().enumerate() {
        let edit_spec = &request.edits[index];
        let min_line = edit_spec.start_line.unwrap_or(1);
        let max_line = edit_spec.end_line.unwrap_or(u64::MAX);
        let in_line_range = |line: usize| (line as u64) >= min_line && (line as u64) <= max_line;

        let exact =
            starts_positions_filtered(&normalized.text, old, |at| in_line_range(line_at_norm(at)));
        if exact.in_count > 1 {
            let lines = exact.in_starts.iter().map(|&at| line_at_norm(at)).collect();
            problems[index] = Some(EntryProblem::Ambiguous {
                count: exact.in_count,
                lines,
            });
            continue;
        }
        let needle = normalize_for_fuzzy_match(old);
        if needle.is_empty() {
            if let Some(&at) = exact.in_starts.first() {
                let (raw_at, raw_end) =
                    (normalized.original(at), normalized.original(at + old.len()));
                let new_text = if normalized.text[at..at + old.len()] == *new {
                    &content[raw_at..raw_end]
                } else {
                    &inserted[index]
                };
                matched.push(Matched {
                    index,
                    at: raw_at,
                    len: raw_end - raw_at,
                    fuzzy: false,
                    new_text,
                });
            } else {
                let out_lines = exact
                    .out_starts
                    .iter()
                    .map(|&at| line_at_norm(at))
                    .collect();
                problems[index] = Some(EntryProblem::NotFound {
                    lines: out_lines,
                    more: exact.out_count.saturating_sub(MAX_REPORTED_ITEMS),
                });
            }
            continue;
        }

        let (haystack, haystack_newlines, map) = lazy_fuzzy.get_or_insert_with(|| {
            let h = normalize_for_fuzzy_match(&normalized.text);
            // Every `\n` in `normalized.text` maps 1-to-1 in order to `h`; keep only those
            // that came from a real `\n` (or `\r\n`) rather than a lone `\r` in `content`.
            let norm_real_flags: Vec<bool> = normalized
                .text
                .bytes()
                .enumerate()
                .filter(|&(_, b)| b == b'\n')
                .map(|(norm_pos, _)| {
                    let raw = normalized.original(norm_pos);
                    content.as_bytes().get(raw) == Some(&b'\n')
                        || content.as_bytes().get(raw..raw + 2) == Some(b"\r\n")
                })
                .collect();
            let nls = h
                .bytes()
                .enumerate()
                .filter(|&(_, b)| b == b'\n')
                .zip(norm_real_flags)
                .filter_map(|((i, _), is_real)| is_real.then_some(i))
                .collect();
            (h, nls, std::cell::OnceCell::new())
        });
        let line_at_fuzzy =
            |fuzzy_pos: usize| 1 + haystack_newlines.partition_point(|&nl| nl < fuzzy_pos);

        let same_form = *haystack == normalized.text && needle == *old;
        let fuzzy = if same_form {
            exact.clone()
        } else {
            starts_positions_filtered(haystack, &needle, |pos| in_line_range(line_at_fuzzy(pos)))
        };

        if let Some(&exact_at) = exact.in_starts.first() {
            let separate = fuzzy.in_count > 0
                && !same_form
                && !exact_survives_fuzzy(&normalized.text, exact_at, old.len(), &needle);
            let count = fuzzy.in_count + usize::from(separate);
            if count > 1 {
                let mut lines: Vec<_> = fuzzy
                    .in_starts
                    .iter()
                    .map(|&pos| line_at_fuzzy(pos))
                    .collect();
                if separate {
                    let line = line_at_norm(exact_at);
                    lines.insert(lines.partition_point(|&other| other < line), line);
                    lines.truncate(MAX_REPORTED_ITEMS);
                }
                problems[index] = Some(EntryProblem::Ambiguous { count, lines });
                continue;
            }
            let (raw_at, raw_end) = (
                normalized.original(exact_at),
                normalized.original(exact_at + old.len()),
            );
            let new_text = if normalized.text[exact_at..exact_at + old.len()] == *new {
                &content[raw_at..raw_end]
            } else {
                &inserted[index]
            };
            matched.push(Matched {
                index,
                at: raw_at,
                len: raw_end - raw_at,
                fuzzy: false,
                new_text,
            });
            continue;
        }

        let not_found_outside = || {
            let mut lines: Vec<_> = exact
                .out_starts
                .iter()
                .map(|&pos| line_at_norm(pos))
                .collect();
            let mut count = exact.out_count;
            if !same_form
                && fuzzy.out_count != 0
                && let Some(m) = map.get_or_init(|| FuzzyMap::new(&normalized.text)).as_ref()
            {
                // Every exact start is editable. Add only whole-unit fuzzy spans with a
                // separate source occurrence. Trimming can move a fuzzy boundary inside the
                // exact span. Both searches advance in source order.
                let mut exact_starts = matching_starts(&normalized.text, old).peekable();
                let mut extra = 0;
                for pos in matching_starts(haystack, &needle) {
                    let line = line_at_fuzzy(pos);
                    if in_line_range(line) {
                        continue;
                    }
                    let Some((start, len)) = m.span(pos, pos + needle.len()) else {
                        continue;
                    };
                    while exact_starts
                        .next_if(|&exact| exact + old.len() < start + len)
                        .is_some()
                    {}
                    if exact_starts.peek().is_some_and(|&exact| exact <= start) {
                        continue;
                    }
                    if extra < MAX_REPORTED_ITEMS {
                        lines.push(line);
                    }
                    extra += 1;
                }
                count += extra;
                lines.sort_unstable();
                lines.truncate(MAX_REPORTED_ITEMS);
            }
            EntryProblem::NotFound {
                lines,
                more: count.saturating_sub(MAX_REPORTED_ITEMS),
            }
        };
        if fuzzy.in_count == 0 {
            problems[index] = Some(not_found_outside());
            continue;
        }
        if fuzzy.in_count > 1 {
            let lines = fuzzy
                .in_starts
                .iter()
                .map(|&pos| line_at_fuzzy(pos))
                .collect();
            problems[index] = Some(EntryProblem::Ambiguous {
                count: fuzzy.in_count,
                lines,
            });
            continue;
        }
        let Some((at, len)) = map
            .get_or_init(|| FuzzyMap::new(&normalized.text))
            .as_ref()
            .and_then(|m| m.span(fuzzy.in_starts[0], fuzzy.in_starts[0] + needle.len()))
        else {
            problems[index] = Some(not_found_outside());
            continue;
        };
        let (raw_at, raw_end) = (normalized.original(at), normalized.original(at + len));
        let new_text = if normalized.text[at..at + len] == *new {
            &content[raw_at..raw_end]
        } else {
            &inserted[index]
        };
        fuzzy_flags[index] = true;
        matched.push(Matched {
            index,
            at: raw_at,
            len: raw_end - raw_at,
            fuzzy: true,
            new_text,
        });
    }

    matched.sort_by_key(|edit| (edit.at, edit.index));
    let mut overlaps: Vec<Vec<usize>> = vec![Vec::new(); total];
    for i in 0..matched.len() {
        let end_i = matched[i].at + matched[i].len;
        for later in &matched[i + 1..] {
            if later.at >= end_i {
                break;
            }
            overlaps[matched[i].index].push(later.index);
            overlaps[later.index].push(matched[i].index);
        }
    }
    for partners in &mut overlaps {
        partners.sort_unstable();
        partners.dedup();
    }

    if problems.iter().any(Option::is_some) || overlaps.iter().any(|p| !p.is_empty()) {
        let mut bullets = Vec::new();
        let mut failures = Vec::new();
        for index in 0..total {
            if let Some(problem) = &problems[index] {
                match problem {
                    EntryProblem::Ambiguous { count, lines } => {
                        let shown = &lines[..lines.len().min(MAX_REPORTED_ITEMS)];
                        let more = count.saturating_sub(MAX_REPORTED_ITEMS);
                        let list = shown
                            .iter()
                            .map(usize::to_string)
                            .collect::<Vec<_>>()
                            .join(", ");
                        let extra = if more > 0 {
                            format!(" (and {more} more)")
                        } else {
                            String::new()
                        };
                        bullets.push(format!(
                            "- edits[{index}]: oldText occurs {count} times, starting at lines {list}{extra}; add surrounding text to make it unique."
                        ));
                        let mut item = json!({
                            "index": index,
                            "problem": "ambiguous",
                            "lines": shown,
                        });
                        if more > 0 {
                            item["more"] = json!(more);
                        }
                        failures.push(item);
                    }
                    EntryProblem::NotFound { lines, more } => {
                        if lines.is_empty() {
                            bullets.push(format!(
                                "- edits[{index}]: oldText was not found, even ignoring trailing whitespace, Unicode compatibility forms, curly quotes, dashes, and special spaces."
                            ));
                            failures.push(json!({
                                "index": index,
                                "problem": "not_found",
                            }));
                        } else {
                            let shown = &lines[..lines.len().min(MAX_REPORTED_ITEMS)];
                            let list = shown
                                .iter()
                                .map(usize::to_string)
                                .collect::<Vec<_>>()
                                .join(", ");
                            let extra = if *more > 0 {
                                format!(" (and {more} more)")
                            } else {
                                String::new()
                            };
                            bullets.push(format!(
                                "- edits[{index}]: oldText was not found in the requested line range (it occurs outside that range, starting at lines {list}{extra})."
                            ));
                            let mut item = json!({
                                "index": index,
                                "problem": "not_found",
                                "lines": shown,
                            });
                            if *more > 0 {
                                item["more"] = json!(more);
                            }
                            failures.push(item);
                        }
                    }
                }
            } else if !overlaps[index].is_empty() {
                let partners = &overlaps[index];
                let shown = &partners[..partners.len().min(MAX_REPORTED_ITEMS)];
                let more = partners.len().saturating_sub(MAX_REPORTED_ITEMS);
                let labels = shown
                    .iter()
                    .map(|partner| format!("edits[{partner}]"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let extra = if more > 0 {
                    format!(" (and {more} more)")
                } else {
                    String::new()
                };
                bullets.push(format!(
                    "- edits[{index}] overlaps {labels}{extra}; merge them into one edit or target disjoint regions."
                ));
                let mut item = json!({
                    "index": index,
                    "problem": "overlap",
                    "with": shown,
                });
                if more > 0 {
                    item["more"] = json!(more);
                }
                failures.push(item);
            }
        }
        let error = format!(
            "Cannot edit {path}; nothing was changed:\n{}",
            bullets.join("\n")
        );
        let structured = json!({
            "ok": false,
            "error": error,
            "path": path,
            "failures": failures,
        });
        return Err((error, Some(structured)));
    }

    let (replaced, starts) = replace(content, &matched);
    if replaced == content {
        return Err(edit_no_change(path));
    }
    for edit in &matched {
        fuzzy_flags[edit.index] = edit.fuzzy;
    }
    Ok((replaced, starts, fuzzy_flags))
}

/// Normalization can absorb the sole exact match into an adjacent character (e + acute → é).
/// Count it separately when its source units contribute none of the fuzzy copies.
fn exact_survives_fuzzy(text: &str, at: usize, len: usize, needle: &str) -> bool {
    let after = at + len;
    let bytes = text.as_bytes();
    // A following ASCII character cannot combine backwards across a boundary.
    if (at == 0 || bytes[at].is_ascii() || bytes[at - 1] == b'\n')
        && (after == bytes.len() || bytes[after].is_ascii())
    {
        return true;
    }
    // ASCII starts a new normalization unit. Keep a graphic character or newline on the
    // right so the enclosing line's trailing-whitespace rule is also preserved.
    let start = bytes[..=at].iter().rposition(u8::is_ascii).unwrap_or(0);
    let end = bytes[after..]
        .iter()
        .position(|&b| b.is_ascii_graphic() || b == b'\n')
        .map_or(text.len(), |pos| after + pos + 1);
    let context = &text[start..end];
    let mut normalized: Vec<_> = context.nfkc().map(|ch| (ch, true)).collect();
    // Both strings have the same canonically ordered compatibility decomposition. Pairing
    // those characters identifies every source byte range contributing to each NFKC character,
    // including a mark that reorders past another before it composes with the starter.
    let sources = tagged_nfkd(
        context
            .char_indices()
            .map(|(pos, ch)| (ch, (at - start..after - start).contains(&pos))),
    );
    let outputs = tagged_nfkd(
        normalized
            .iter()
            .enumerate()
            .map(|(index, &(ch, _))| (ch, index)),
    );
    debug_assert_eq!(sources.len(), outputs.len());
    for ((source, inside), (output, index)) in sources.into_iter().zip(outputs) {
        debug_assert_eq!(source, output);
        normalized[index].1 &= inside;
    }
    let mut owned = String::new();
    for line in normalized.split_inclusive(|&(ch, _)| ch == '\n') {
        let newline = line.last().filter(|&&(ch, _)| ch == '\n');
        let body = &line[..line.len() - usize::from(newline.is_some())];
        let kept = body
            .iter()
            .rposition(|&(ch, _)| !is_js_whitespace(ch))
            .map_or(0, |pos| pos + 1);
        for &(ch, inside) in body[..kept].iter().chain(newline) {
            if inside {
                owned.push(mapped_character(ch));
            } else {
                if owned.contains(needle) {
                    return true;
                }
                owned.clear();
            }
        }
    }
    owned.contains(needle)
}

/// NFKD with a source label carried through decomposition and stable canonical ordering.
fn tagged_nfkd<T: Copy>(chars: impl Iterator<Item = (char, T)>) -> Vec<(char, T)> {
    let mut parts = Vec::new();
    for (ch, tag) in chars {
        unicode_normalization::char::decompose_compatible(ch, |part| parts.push((part, tag)));
    }
    for marks in parts.split_mut(|&(ch, _)| canonical_combining_class(ch) == 0) {
        marks.sort_by_key(|&(ch, _)| canonical_combining_class(ch));
    }
    parts
}

#[derive(Clone, Default)]
struct FilteredStarts {
    in_count: usize,
    in_starts: Vec<usize>,
    out_count: usize,
    out_starts: Vec<usize>,
}

/// Counts overlapping occurrences in one pass, partitioning in-range and out-of-range starts.
fn starts_positions_filtered(
    haystack: &str,
    needle: &str,
    mut in_range: impl FnMut(usize) -> bool,
) -> FilteredStarts {
    let mut out = FilteredStarts::default();
    for pos in matching_starts(haystack, needle) {
        let (count, starts) = if in_range(pos) {
            (&mut out.in_count, &mut out.in_starts)
        } else {
            (&mut out.out_count, &mut out.out_starts)
        };
        *count += 1;
        if starts.len() < MAX_REPORTED_ITEMS {
            starts.push(pos);
        }
    }
    out
}

/// Overlapping matches in ascending byte order, in linear time.
fn matching_starts<'a>(haystack: &'a str, needle: &'a str) -> impl Iterator<Item = usize> + 'a {
    let needle = needle.as_bytes();
    let mut table = vec![0usize; needle.len()];
    let mut matched = 0;
    for at in 1..needle.len() {
        while matched > 0 && needle[at] != needle[matched] {
            matched = table[matched - 1];
        }
        if needle[at] == needle[matched] {
            matched += 1;
        }
        table[at] = matched;
    }
    matched = 0;
    let haystack = if needle.is_empty() { "" } else { haystack };
    haystack.bytes().enumerate().filter_map(move |(idx, byte)| {
        while matched > 0 && byte != needle[matched] {
            matched = table[matched - 1];
        }
        if byte == needle[matched] {
            matched += 1;
        }
        if matched == needle.len() {
            matched = table[matched - 1];
            Some(idx + 1 - needle.len())
        } else {
            None
        }
    })
}

/// Applies matches to `content`, retaining each edit's start in the original file.
fn replace(content: &str, matched: &[Matched<'_>]) -> (String, Vec<usize>) {
    let mut starts = vec![0usize; matched.len()];
    for edit in matched {
        starts[edit.index] = edit.at;
    }
    let mut result = content.to_owned();
    for edit in matched.iter().rev() {
        result.replace_range(edit.at..edit.at + edit.len, edit.new_text);
    }
    (result, starts)
}

/// A normalized view and the positions where a CRLF lost its CR. A boundary before the LF
/// maps before the original CR; a boundary after it maps after the complete CRLF.
struct LfText {
    text: String,
    crlf: Vec<usize>,
}

impl LfText {
    fn new(content: &str) -> Self {
        let crlf: Vec<usize> = content
            .match_indices("\r\n")
            .enumerate()
            .map(|(removed, (at, _))| at - removed)
            .collect();
        Self {
            text: normalize_to_lf(content),
            crlf,
        }
    }

    fn original(&self, offset: usize) -> usize {
        offset + self.crlf.partition_point(|&at| at < offset)
    }
}

/// Counts `\n` line breaks, matching `read`'s line numbering (lone `\r` is ordinary content).
fn line_breaks(text: &str) -> usize {
    text.bytes().filter(|&byte| byte == b'\n').count()
}

/// The first line ending is the convention used only for inserted text.
fn detect_line_ending(content: &str) -> &'static str {
    match content.find(['\r', '\n']) {
        Some(at) if content[at..].starts_with("\r\n") => "\r\n",
        Some(at) if content.as_bytes()[at] == b'\r' => "\r",
        _ => "\n",
    }
}

pub(crate) fn normalize_to_lf(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

fn restore_line_endings(text: &str, ending: &str) -> String {
    if ending != "\n" {
        text.replace('\n', ending)
    } else {
        text.to_owned()
    }
}

/// The form matching falls back to: NFKC, no trailing whitespace, and ASCII quotes, dashes and
/// spaces, so text pasted from a document still finds its source.
fn normalize_for_fuzzy_match(text: &str) -> String {
    let normalized = nfkc(text);
    mapped(
        &normalized
            .split('\n')
            .map(|line| line.trim_end_matches(is_js_whitespace))
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

fn nfkc(text: &str) -> String {
    text.nfkc().collect()
}

/// The characters the fuzzy form rewrites, one for one.
fn mapped(text: &str) -> String {
    text.chars().map(mapped_character).collect()
}

fn mapped_character(character: char) -> char {
    match character {
        '\u{2018}' | '\u{2019}' | '\u{201a}' | '\u{201b}' => '\'',
        '\u{201c}' | '\u{201d}' | '\u{201e}' | '\u{201f}' => '"',
        '\u{2010}'..='\u{2015}' | '\u{2212}' => '-',
        '\u{00a0}' | '\u{2002}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}' => ' ',
        other => other,
    }
}

/// Where the fuzzy form of a text came from: a match found in that form names a span of the text
/// itself, so only the matched bytes are ever replaced.
struct FuzzyMap {
    /// For every byte of the trimmed fuzzy form, the byte of the untrimmed form it comes from.
    origin: Vec<usize>,
    /// The untrimmed byte the trimmed fuzzy form ends at.
    end: usize,
    /// Every unit of the text: its start and end in the untrimmed fuzzy form, then in the text.
    units: Vec<(usize, usize, usize, usize)>,
}

impl FuzzyMap {
    /// Builds the map of `text`, or nothing when its units do not reproduce the fuzzy form.
    fn new(text: &str) -> Option<Self> {
        let mut form = String::with_capacity(text.len());
        let mut units = Vec::new();
        for (at, end) in units_of(text) {
            let unit = mapped(&nfkc(&text[at..end]));
            if unit.is_empty() {
                return None;
            }
            units.push((form.len(), form.len() + unit.len(), at, end));
            form.push_str(&unit);
        }
        let (trimmed, origin, end) = trimmed_form(&form);
        if trimmed != normalize_for_fuzzy_match(text) {
            return None;
        }
        Some(Self { origin, end, units })
    }

    /// The span of the text the fuzzy form's bytes `[from, to)` stand for, when both ends fall on a
    /// unit boundary.
    fn span(&self, from: usize, to: usize) -> Option<(usize, usize)> {
        let start = self.start_of(from)?;
        let end = self.end_of(to)?;
        (end > start).then_some((start, end - start))
    }

    /// The byte of the text the unit starting at the byte of the fuzzy form at `from` starts at.
    fn start_of(&self, from: usize) -> Option<usize> {
        let at = match self.origin.get(from) {
            Some(at) => *at,
            None if from == self.origin.len() => self.end,
            None => return None,
        };
        self.units
            .binary_search_by_key(&at, |unit| unit.0)
            .ok()
            .map(|found| self.units[found].2)
    }

    /// The byte of the text just after the unit ending at the byte of the fuzzy form after `to`, so
    /// trailing whitespace the trim removed stays outside the span.
    fn end_of(&self, to: usize) -> Option<usize> {
        let at = match to.checked_sub(1).and_then(|last| self.origin.get(last)) {
            Some(last) => last + 1,
            None if to == 0 => 0,
            None => return None,
        };
        self.units
            .binary_search_by_key(&at, |unit| unit.1)
            .ok()
            .map(|found| self.units[found].3)
    }
}

/// The most characters one unit may hold. UAX #15's stream-safe format bounds a run of
/// non-starters at 30, so a longer run is split; if that splits a real composition the form check
/// refuses the map instead of guessing a span.
const MAX_UNIT: usize = 32;

/// Splits `text` into the units normalization works on: one character each, extended while the
/// next character can reorder with the marks already in the unit, or composes with them, up to
/// `MAX_UNIT` characters.
fn units_of(text: &str) -> Vec<(usize, usize)> {
    let mut units = Vec::new();
    let mut characters = text.char_indices();
    while let Some((at, first)) = characters.next() {
        let mut end = at + first.len_utf8();
        let mut length = 1;
        while length < MAX_UNIT {
            let Some(next) = characters.as_str().chars().next() else {
                break;
            };
            // A mark that starts a unit can reorder with every later mark, so it stays in the unit
            // whether or not the two compose now. Whitespace keeps its own unit, because the trim
            // removes it on its own.
            let extends = (!is_js_whitespace(first) && starts_with_a_mark(next))
                || composes(&text[at..end], next);
            if !extends {
                break;
            }
            end += next.len_utf8();
            length += 1;
            characters.next();
        }
        units.push((at, end));
    }
    units
}

/// Whether `character` decomposes to a leading non-starter, which reorders with the marks before
/// it under NFKC.
fn starts_with_a_mark(character: char) -> bool {
    character
        .nfkd()
        .next()
        .is_some_and(|first| canonical_combining_class(first) != 0)
}

/// Whether `next` composes with `unit` under NFKC, which keeps the two one unit.
fn composes(unit: &str, next: char) -> bool {
    let mut joined = String::with_capacity(unit.len() + next.len_utf8());
    joined.push_str(unit);
    joined.push(next);
    let mut single = String::with_capacity(next.len_utf8());
    single.push(next);
    nfkc(&joined) != nfkc(unit) + &nfkc(&single)
}

/// The fuzzy form without the trailing whitespace of each line, where every byte comes from in the
/// form it trims, and where the kept bytes end.
fn trimmed_form(form: &str) -> (String, Vec<usize>, usize) {
    let mut trimmed = String::with_capacity(form.len());
    let mut origin = Vec::with_capacity(form.len());
    let mut end = 0;
    let mut line_start = 0;
    for line in form.split_inclusive('\n') {
        let (body, newline) = match line.strip_suffix('\n') {
            Some(body) => (body, true),
            None => (line, false),
        };
        let kept = body.trim_end_matches(is_js_whitespace);
        for (offset, character) in kept.char_indices() {
            for byte in 0..character.len_utf8() {
                origin.push(line_start + offset + byte);
            }
            trimmed.push(character);
        }
        end = line_start + kept.len();
        if newline {
            // The kept newline is the physical one, past every byte the trim removed.
            origin.push(line_start + body.len());
            trimmed.push('\n');
            end = line_start + line.len();
        }
        line_start += line.len();
    }
    (trimmed, origin, end)
}

/// The whitespace the reference implementation strips, which includes U+FEFF and not U+0085.
fn is_js_whitespace(character: char) -> bool {
    matches!(
        character,
        '\t' | '\n' | '\u{000b}' | '\u{000c}' | '\r' | ' ' | '\u{00a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    /// A directory of its own under the temp directory, removed when the test ends.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("fastexec-files-{}-{name}", std::process::id()));
            let _ = std::fs::remove_dir_all(&path);
            std::fs::create_dir_all(&path).expect("a scratch directory");
            Self(path)
        }

        fn file(&self, name: &str, content: &[u8]) -> PathBuf {
            let path = self.0.join(name);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).expect("a parent directory");
            }
            std::fs::write(&path, content).expect("a file");
            path
        }

        fn read(&self, name: &str) -> Vec<u8> {
            std::fs::read(self.0.join(name)).expect("the file")
        }

        fn path(&self, name: &str) -> String {
            display_path(&self.0.join(name))
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A launch context whose working directory is `cwd` and whose home is `home`.
    fn context(cwd: &Path, home: &Path) -> LaunchContext {
        LaunchContext::new(
            cwd.to_path_buf(),
            vec![
                ("HOME".into(), home.as_os_str().to_owned()),
                ("USERPROFILE".into(), home.as_os_str().to_owned()),
            ],
        )
    }

    fn whole(path: &str, launch: &LaunchContext, transfer: bool) -> Result<String, String> {
        read_text(
            &ReadRequest {
                path: path.to_owned(),
                offset: None,
                limit: None,
            },
            launch,
            transfer,
            ReadLimits::DIRECT,
        )
        .map(|outcome| outcome.text)
    }

    fn window(
        path: &str,
        offset: Option<i64>,
        limit: Option<u64>,
        launch: &LaunchContext,
    ) -> Result<String, String> {
        read_text(
            &ReadRequest {
                path: path.to_owned(),
                offset,
                limit,
            },
            launch,
            false,
            ReadLimits::DIRECT,
        )
        .map(|outcome| outcome.text)
    }

    fn save_text(path: &Path, bytes: &[u8]) -> io::Result<()> {
        save_target(write_target(path)?, bytes, None)
    }

    fn change(
        path: &str,
        edits: &[(&str, &str)],
        launch: &LaunchContext,
    ) -> Result<String, String> {
        edit_text(
            &EditRequest {
                path: path.to_owned(),
                edits: edits
                    .iter()
                    .map(|(old, new)| Edit {
                        old_text: (*old).to_owned(),
                        new_text: (*new).to_owned(),
                        start_line: None,
                        end_line: None,
                    })
                    .collect(),
            },
            launch,
        )
    }

    #[test]
    fn paths_resolve_against_the_launch_context() {
        let scratch = Scratch::new("resolve");
        let home = scratch.0.join("home");
        std::fs::create_dir_all(&home).unwrap();
        let launch = context(&scratch.0, &home);
        assert_eq!(resolve("~", &launch).unwrap(), home);
        assert_eq!(
            resolve("~/notes/a.txt", &launch).unwrap(),
            home.join("notes/a.txt")
        );
        assert_eq!(
            resolve("rel.txt", &launch).unwrap(),
            scratch.0.join("rel.txt")
        );
        let absolute = scratch.0.join("abs.txt");
        assert_eq!(
            resolve(&absolute.display().to_string(), &launch).unwrap(),
            absolute
        );
        // A home nobody knows is an error rather than a silent fallback.
        let homeless = LaunchContext::new(scratch.0.clone(), Vec::new());
        let message = resolve("~/x", &homeless).unwrap_err();
        assert!(
            message.contains("the home directory is unknown"),
            "{message}"
        );
    }

    #[test]
    fn a_relative_home_stays_anchored_to_the_session_directory() {
        let scratch = Scratch::new("relative-home");
        let launch = context(&scratch.0, Path::new("local-home"));
        let nested = launch.with_cwd(scratch.0.join("other"));
        let file = scratch.file("local-home/notes", b"session home");
        let home = scratch.0.join("local-home");
        assert_eq!(nested.home().as_deref(), Some(home.as_path()));
        assert!(crate::host_line(&nested).contains(&format!(" · home {} · ", display_path(&home))));
        assert_eq!(resolve("~/notes", &nested).unwrap(), file);
        assert_eq!(whole("~/notes", &nested, false).unwrap(), "session home");
    }

    #[test]
    fn home_expansion_keeps_repeated_separators_under_home() {
        let scratch = Scratch::new("home-separators");
        let home = scratch.0.join("home");
        std::fs::create_dir_all(home.join("notes")).unwrap();
        let path = home.join("notes/a.txt");
        std::fs::write(&path, b"home file\n").unwrap();
        let launch = context(&scratch.0, &home);
        for input in ["~/notes/a.txt", "~//notes///a.txt", "~///notes/a.txt"] {
            assert_eq!(resolve(input, &launch).unwrap(), path);
            assert_eq!(whole(input, &launch, false).unwrap(), "home file\n");
        }
        #[cfg(windows)]
        for input in [r"~\\notes\\a.txt", r"~\\/notes//a.txt"] {
            assert_eq!(resolve(input, &launch).unwrap(), path);
        }
        write_text(
            &WriteRequest {
                path: "~//notes/a.txt".into(),
                content: "updated\n".into(),
            },
            &launch,
        )
        .unwrap();
        assert_eq!(std::fs::read(path).unwrap(), b"updated\n");
    }

    #[cfg(windows)]
    #[test]
    fn home_expansion_refuses_windows_prefixes_after_separators() {
        let launch = context(Path::new(r"C:\cwd"), Path::new(r"D:\home"));
        for input in [
            "~/C:notes.txt",
            "~/D:/value",
            "~//C:notes.txt",
            "~///D:/value",
            r"~\C:notes.txt",
            r"~\\D:\value",
            r"~\/C:notes.txt",
        ] {
            let error = resolve(input, &launch).unwrap_err();
            assert!(
                error.contains("the path after ~ must be relative to the home directory"),
                "{input}: {error}"
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn home_expansion_keeps_colons_in_unix_file_names() {
        let launch = context(Path::new("/cwd"), Path::new("/home/test"));
        for input in ["C:notes.txt", "D:/value"] {
            assert_eq!(
                resolve(&format!("~/{input}"), &launch).unwrap(),
                Path::new("/home/test").join(input)
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn verbatim_paths_keep_parent_components_when_resolved_and_appended() {
        for base in [r"\\?\C:\base", r"\\?\UNC\server\share\base"] {
            let launch = context(Path::new(base), Path::new(base));
            let expected = PathBuf::from(format!(r"{base}\link\..\value"));
            let absolute = format!(r"{base}\.\link\\..\value");
            assert_eq!(resolve(&absolute, &launch).unwrap(), expected);
            for input in [
                "link/../value",
                r"link\..\value",
                "./link//../value",
                "~/link/../value",
                r"~\link\..\value",
            ] {
                assert_eq!(resolve(input, &launch).unwrap(), expected, "{input}");
            }
            let parent = PathBuf::from(format!(r"{base}\link\.."));
            assert_eq!(
                append_path(&parent, Path::new("staged")),
                PathBuf::from(format!(r"{base}\link\..\staged"))
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn appending_windows_paths_preserves_roots_and_parent_components() {
        for (base, prefix) in [
            (r"C:\base\dir", r"C:"),
            (r"\\?\C:\base\dir", r"\\?\C:"),
            (r"\\server\share\base\dir", r"\\server\share"),
            (r"\\?\UNC\server\share\base\dir", r"\\?\UNC\server\share"),
        ] {
            for rooted in [r"\other\link\..\value", "/other/link/../value"] {
                assert_eq!(
                    append_path(Path::new(base), Path::new(rooted)),
                    PathBuf::from(format!(r"{prefix}\other\link\..\value")),
                    "{base} + {rooted}"
                );
            }
            for replacement in [
                r"D:\other\link\..\value",
                r"\\?\D:\other\link\..\value",
                r"\\other\share\link\..\value",
                r"\\?\UNC\other\share\link\..\value",
                r"D:link\..\value",
            ] {
                assert_eq!(
                    append_path(Path::new(base), Path::new(replacement)),
                    PathBuf::from(replacement),
                    "{base} + {replacement}"
                );
            }
        }
    }

    #[test]
    fn read_returns_the_window_it_was_asked_for() {
        let scratch = Scratch::new("window");
        let text: String = (1..=10).map(|line| format!("line {line}\n")).collect();
        scratch.file("ten.txt", text.as_bytes());
        let launch = context(&scratch.0, &scratch.0);
        let path = scratch.path("ten.txt");
        // Ten terminated lines are ten real lines, with terminators kept intact.
        assert_eq!(whole(&path, &launch, false).unwrap(), text);
        assert_eq!(
            window(&path, Some(2), Some(3), &launch).unwrap(),
            "line 2\nline 3\nline 4\n\n[6 more lines in file. Use offset=5 to continue.]"
        );
        assert_eq!(
            window(&path, None, Some(0), &launch).unwrap_err(),
            "limit must be at least 1."
        );
        assert_eq!(window(&path, Some(10), None, &launch).unwrap(), "line 10\n");
        assert_eq!(
            window(&path, Some(11), None, &launch).unwrap_err(),
            "Offset 11 is beyond end of file (10 lines total)"
        );
    }

    #[test]
    fn read_counts_selected_empty_lines_before_the_end_of_file() {
        let scratch = Scratch::new("selected-empty-lines");
        let launch = context(&scratch.0, &scratch.0);
        scratch.file("three.txt", b"a\n\nb");
        for (offset, limit, body, shown, end) in [(1, 2, "a\n\n", 2, 2), (2, 1, "\n", 1, 2)] {
            let outcome = read_text(
                &ReadRequest {
                    path: scratch.path("three.txt"),
                    offset: Some(offset),
                    limit: Some(limit),
                },
                &launch,
                false,
                ReadLimits::DIRECT,
            )
            .unwrap();
            assert_eq!(outcome.body.as_deref(), Some(body));
            assert_eq!(outcome.structured["shownLines"], shown);
            assert_eq!(outcome.structured["endLine"], end);
            assert_eq!(outcome.structured["nextOffset"], 3);
        }
        scratch.file("empty.txt", b"");
        let empty = read_text(
            &ReadRequest {
                path: scratch.path("empty.txt"),
                offset: None,
                limit: None,
            },
            &launch,
            false,
            ReadLimits::DIRECT,
        )
        .unwrap();
        assert_eq!(empty.structured["shownLines"], 0);
        assert!(empty.structured["endLine"].is_null());
    }

    #[test]
    fn read_cuts_at_the_line_and_byte_budgets() {
        let scratch = Scratch::new("budgets");
        let launch = context(&scratch.0, &scratch.0);
        let many: String = (1..=2500).map(|line| format!("line {line}\n")).collect();
        scratch.file("many.txt", many.as_bytes());
        let path = scratch.path("many.txt");
        let text = whole(&path, &launch, false).unwrap();
        assert!(
            text.ends_with("[Showing lines 1-2000 of 2500. Use offset=2001 to continue.]"),
            "{text}"
        );
        assert!(text.starts_with("line 1\nline 2\n"), "{text}");
        assert!(text.contains("line 2000\n\n"), "{text}");
        assert!(!text.contains("line 2001"), "{text}");

        // Fifty-nine-byte lines: the byte budget runs out at the 853rd line.
        let wide: String = (0..900).map(|_| format!("{}\n", "x".repeat(59))).collect();
        scratch.file("wide.txt", wide.as_bytes());
        let path = scratch.path("wide.txt");
        let text = whole(&path, &launch, false).unwrap();
        assert!(
            text.ends_with(
                "[Showing lines 1-853 of 900 (50.0KB limit). Use offset=854 to continue.]"
            ),
            "{text}"
        );

        // A first line over the budget gets the shell hint instead of a body.
        let huge = format!("{}\n", "y".repeat(MAX_BYTES as usize + 1));
        scratch.file("huge.txt", huge.as_bytes());
        let path = scratch.path("huge.txt");
        assert_eq!(
            whole(&path, &launch, false).unwrap(),
            format!(
                "[Line 1 is 50.0KB, exceeds 50.0KB limit. Use exec: sed -n '1p' '{path}' | head -c {MAX_BYTES}]"
            )
        );
        let nested_huge = read_text(
            &ReadRequest {
                path: path.clone(),
                offset: None,
                limit: None,
            },
            &launch,
            false,
            ReadLimits::NESTED,
        )
        .unwrap();
        assert_eq!(nested_huge.body.as_deref(), Some(huge.as_str()));
        assert_eq!(nested_huge.structured["truncated"], false);
        assert_eq!(nested_huge.structured["lineTooLong"], false);
        assert_eq!(nested_huge.structured["shownLines"], 1);

        // P0-2: nested whole-file reads have no line cap up to 4 MiB.
        let nested_many = "x\n".repeat(50_003);
        scratch.file("nested-many.txt", nested_many.as_bytes());
        let nested_many_path = scratch.path("nested-many.txt");
        let nested_whole = read_text(
            &ReadRequest {
                path: nested_many_path,
                offset: None,
                limit: None,
            },
            &launch,
            false,
            ReadLimits::NESTED,
        )
        .unwrap();
        assert_eq!(nested_whole.body.as_deref(), Some(nested_many.as_str()));
        assert_eq!(nested_whole.structured["totalLines"], 50_003);
        assert_eq!(nested_whole.structured["shownLines"], 50_003);
        assert_eq!(nested_whole.structured["truncated"], false);
        assert!(nested_whole.structured["nextOffset"].is_null());

        // First line 1,048,577 B + "\ny\n" succeeds whole in nested whole-file mode.
        let one_mib_plus = format!("{}\ny\n", "a".repeat(1_048_577));
        scratch.file("one-mib-plus.txt", one_mib_plus.as_bytes());
        let whole_mib = read_text(
            &ReadRequest {
                path: scratch.path("one-mib-plus.txt"),
                offset: None,
                limit: None,
            },
            &launch,
            false,
            ReadLimits::NESTED,
        )
        .unwrap();
        assert_eq!(whole_mib.body.as_deref(), Some(one_mib_plus.as_str()));
        assert_eq!(whole_mib.structured["totalLines"], 2);

        // 4 MiB + 1 B rejects whole-file mode naming offset & limit; slice mode works.
        let over_nested = "z".repeat(NESTED_MAX_BYTES as usize + 1);
        scratch.file("over-nested.txt", over_nested.as_bytes());
        let over_nested_path = scratch.path("over-nested.txt");
        let whole_err = read_text(
            &ReadRequest {
                path: over_nested_path.clone(),
                offset: None,
                limit: None,
            },
            &launch,
            false,
            ReadLimits::NESTED,
        )
        .err()
        .unwrap();
        assert!(
            whole_err.contains("offset")
                && whole_err.contains("limit")
                && whole_err.contains("4 MiB"),
            "{whole_err}"
        );
        let slice_long = read_text(
            &ReadRequest {
                path: over_nested_path.clone(),
                offset: Some(1),
                limit: None,
            },
            &launch,
            false,
            ReadLimits::NESTED,
        )
        .unwrap();
        assert_eq!(slice_long.structured["lineTooLong"], true);
        assert_eq!(slice_long.structured["shownLines"], 0);

        // Invalid UTF-8 (6f ff 6b 0a) rejects whole-file mode, succeeds with lossy: true on offset: 1.
        scratch.file("invalid-utf8.txt", &[0x6f, 0xff, 0x6b, 0x0a]);
        let inv_path = scratch.path("invalid-utf8.txt");
        let inv_err = read_text(
            &ReadRequest {
                path: inv_path.clone(),
                offset: None,
                limit: None,
            },
            &launch,
            false,
            ReadLimits::NESTED,
        )
        .err()
        .unwrap();
        assert!(
            inv_err.contains("not valid UTF-8")
                && inv_err.contains("offset")
                && inv_err.contains("limit"),
            "{inv_err}"
        );
        let inv_slice = read_text(
            &ReadRequest {
                path: inv_path,
                offset: Some(1),
                limit: None,
            },
            &launch,
            false,
            ReadLimits::NESTED,
        )
        .unwrap();
        assert_eq!(inv_slice.structured["lossy"], true);
        assert_eq!(inv_slice.body.as_deref(), Some("o\u{fffd}k\n"));

        // 5 MiB multi-line file with offset: 1, limit: 100 returns lines 1-100 with nextOffset: 101.
        let five_mib: String = (1..=100_000)
            .map(|i| format!("row-{i:06}-padding-abcdefghij-0123456789-xyz-0123456789\n"))
            .collect();
        assert!(five_mib.len() > 5 << 20);
        scratch.file("five-mib.txt", five_mib.as_bytes());
        let five_slice = read_text(
            &ReadRequest {
                path: scratch.path("five-mib.txt"),
                offset: Some(1),
                limit: Some(100),
            },
            &launch,
            false,
            ReadLimits::NESTED,
        )
        .unwrap();
        assert_eq!(five_slice.structured["startLine"], 1);
        assert_eq!(five_slice.structured["endLine"], 100);
        assert_eq!(five_slice.structured["shownLines"], 100);
        assert_eq!(five_slice.structured["truncated"], true);
        assert_eq!(five_slice.structured["nextOffset"], 101);
        assert_eq!(
            nested_whole
                .structured
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>(),
            five_slice
                .structured
                .as_object()
                .unwrap()
                .keys()
                .collect::<Vec<_>>()
        );
    }

    #[cfg(unix)]
    #[test]
    fn the_long_line_hint_reads_paths_with_spaces_and_quotes() {
        let scratch = Scratch::new("quoted-line");
        let launch = context(&scratch.0, &scratch.0);
        let body = "a".repeat(MAX_BYTES as usize + 100);
        let path = scratch.file("O'Reilly notes.txt", body.as_bytes());
        let answer = whole(path.to_str().unwrap(), &launch, false).unwrap();
        let (_, command) = answer.split_once("Use exec: ").unwrap();
        let output = std::process::Command::new("sh")
            .args(["-c", command.strip_suffix(']').unwrap()])
            .output()
            .unwrap();
        assert!(
            output.status.success() && output.stderr.is_empty(),
            "{output:?}"
        );
        assert_eq!(output.stdout, body.as_bytes()[..MAX_BYTES as usize]);
    }

    #[test]
    fn read_names_a_binary_file_and_its_transfer_hint() {
        let scratch = Scratch::new("binary");
        scratch.file("blob.bin", b"\x00\x01\x02\x03");
        let launch = context(&scratch.0, &scratch.0);
        let path = scratch.path("blob.bin");
        assert_eq!(
            whole(&path, &launch, false).unwrap_err(),
            format!("{path} is a binary file (4 bytes). Inspect it with exec.")
        );
        assert_eq!(
            whole(&path, &launch, true).unwrap_err(),
            format!(
                "{path} is a binary file (4 bytes). Inspect it with exec or download it with transfer."
            )
        );
    }

    #[test]
    fn read_refuses_a_directory_and_notes_replaced_bytes() {
        let scratch = Scratch::new("lossy");
        let launch = context(&scratch.0, &scratch.0);
        let directory = scratch.0.display().to_string();
        let shown_dir = display_path(&scratch.0);
        assert_eq!(
            whole(&directory, &launch, false).unwrap_err(),
            format!("Cannot read {shown_dir}: it is a directory; list it with exec.")
        );
        scratch.file("bad.txt", b"ok\n\xffbad\n");
        let path = scratch.path("bad.txt");
        assert_eq!(
            whole(&path, &launch, false).unwrap(),
            "ok\n\u{fffd}bad\n\n[Invalid UTF-8 was replaced with U+FFFD.]"
        );
        let missing = scratch.path("missing.txt");
        let message = whole(&missing, &launch, false).unwrap_err();
        assert!(
            message.starts_with(&format!("Cannot read {missing}: ")),
            "{message}"
        );
    }

    #[test]
    fn read_keeps_a_character_split_by_a_read_boundary() {
        let scratch = Scratch::new("boundary");
        let launch = context(&scratch.0, &scratch.0);
        // The reader's buffer is 8 KiB, so a character starting at its last byte is cut in two.
        let line = format!("{}{}tail\n", "a".repeat(8191), "€");
        scratch.file("split.txt", line.as_bytes());
        let path = scratch.path("split.txt");
        let text = whole(&path, &launch, false).unwrap();
        assert_eq!(text, line);
        assert!(!text.contains("U+FFFD"), "{text}");
    }

    #[test]
    fn read_flushes_an_unfinished_character_at_the_end() {
        let scratch = Scratch::new("unfinished");
        let launch = context(&scratch.0, &scratch.0);
        // An incomplete character at the end is one replacement character, never dropped.
        for (name, bytes) in [("c2.txt", vec![0xc2u8]), ("e282.txt", vec![0xe2u8, 0x82])] {
            scratch.file(name, &bytes);
            let path = scratch.path(name);
            assert_eq!(
                whole(&path, &launch, false).unwrap(),
                "\u{fffd}\n\n[Invalid UTF-8 was replaced with U+FFFD.]"
            );
        }
        // The same after a whole read buffer: the carry is flushed at the real end of the file.
        let mut long = vec![b'a'; 8192];
        long.push(0xe2);
        scratch.file("long.txt", &long);
        let path = scratch.path("long.txt");
        assert_eq!(
            whole(&path, &launch, false).unwrap(),
            format!(
                "{}\u{fffd}\n\n[Invalid UTF-8 was replaced with U+FFFD.]",
                "a".repeat(8192)
            )
        );
    }

    #[test]
    fn read_keeps_a_final_newline_without_a_false_notice() {
        let scratch = Scratch::new("final-newline");
        let launch = context(&scratch.0, &scratch.0);
        // 2000 terminated lines are 2000 lines, not 2001: nothing is truncated.
        let text: String = "x\n".repeat(2000);
        scratch.file("many.txt", text.as_bytes());
        let path = scratch.path("many.txt");
        assert_eq!(whole(&path, &launch, false).unwrap(), text);
        // The last 2000 lines of a longer file are a window, not a truncation.
        let long: String = (1..=2001).map(|line| format!("line {line}\n")).collect();
        scratch.file("long.txt", long.as_bytes());
        let path = scratch.path("long.txt");
        let tail: String = (2..=2001).map(|line| format!("line {line}\n")).collect();
        assert_eq!(window(&path, Some(2), None, &launch).unwrap(), tail);
    }

    #[test]
    fn read_names_a_first_line_at_the_byte_limit_as_a_line_count() {
        let scratch = Scratch::new("at-limit");
        let launch = context(&scratch.0, &scratch.0);
        // A line whose terminator brings it to MAX_BYTES fits; a second line triggers the notice.
        let line = "z".repeat(MAX_BYTES as usize - 1);
        scratch.file("at.txt", format!("{line}\nsecond\n").as_bytes());
        let path = display_path(&scratch.0.join("at.txt"));
        assert_eq!(
            whole(&path, &launch, false).unwrap(),
            format!("{line}\n\n[Showing lines 1-1 of 2 (50.0KB limit). Use offset=2 to continue.]")
        );
        // A terminator straddling MAX_BYTES pushes line 1 to MAX_BYTES + 1 bytes -> lineTooLong.
        let straddle = "z".repeat(MAX_BYTES as usize);
        scratch.file("straddle.txt", format!("{straddle}\n").as_bytes());
        let straddle_path = display_path(&scratch.0.join("straddle.txt"));
        assert_eq!(
            whole(&straddle_path, &launch, false).unwrap(),
            format!(
                "[Line 1 is 50.0KB, exceeds 50.0KB limit. Use exec: sed -n '1p' '{straddle_path}' | head -c {MAX_BYTES}]"
            )
        );
    }

    #[test]
    fn sizes_round_the_way_a_fixed_decimal_rounds() {
        assert_eq!(format_size(1023), "1023B");
        assert_eq!(format_size(1024), "1.0KB");
        assert_eq!(format_size(51200), "50.0KB");
        assert_eq!(format_size(51456), "50.3KB");
        assert_eq!(format_size(80000), "78.1KB");
        assert_eq!(format_size(1310720), "1.3MB");
    }

    #[test]
    fn read_accepts_the_largest_offset_and_limit() {
        let scratch = Scratch::new("max-limit");
        let launch = context(&scratch.0, &scratch.0);
        scratch.file("two.txt", b"one\ntwo\n");
        let path = scratch.path("two.txt");
        assert_eq!(
            window(&path, Some(2), Some(u64::MAX), &launch).unwrap(),
            "two\n"
        );
    }

    #[test]
    fn edit_counts_a_whitespace_needle_as_the_content_spells_it() {
        let scratch = Scratch::new("blank-needle");
        let launch = context(&scratch.0, &scratch.0);
        let shown_a = display_path(&scratch.0.join("a.txt"));
        let shown_b = display_path(&scratch.0.join("b.txt"));
        let shown_c = display_path(&scratch.0.join("c.txt"));
        // A unique blank is a real target, and it is counted where the content spells it.
        scratch.file("a.txt", b"a b");
        assert_eq!(
            change("a.txt", &[(" ", "X")], &launch).unwrap(),
            format!("Successfully replaced 1 block(s) in {shown_a}.\nedits[0]: line 1")
        );
        assert_eq!(scratch.read("a.txt"), b"aXb");
        // Two blanks are two candidates.
        scratch.file("c.txt", b"a b c");
        assert_eq!(
            change("c.txt", &[(" ", "X")], &launch).unwrap_err(),
            format!(
                "Cannot edit {shown_c}; nothing was changed:\n- edits[0]: oldText occurs 2 times, starting at lines 1, 1; add surrounding text to make it unique."
            )
        );
        assert_eq!(scratch.read("c.txt"), b"a b c");
        // A blank the content does not have is not a target at all.
        scratch.file("b.txt", "\u{1f600}\u{1f600}".as_bytes());
        assert_eq!(
            change("b.txt", &[("\u{3000}", "X")], &launch).unwrap_err(),
            format!(
                "Cannot edit {shown_b}; nothing was changed:\n- edits[0]: oldText was not found, even ignoring trailing whitespace, Unicode compatibility forms, curly quotes, dashes, and special spaces."
            )
        );
        assert_eq!(scratch.read("b.txt"), "\u{1f600}\u{1f600}".as_bytes());
    }

    #[test]
    fn write_creates_parents_and_overwrites_in_place() {
        let scratch = Scratch::new("write");
        let launch = context(&scratch.0, &scratch.0);
        let (written, shown) = write_text(
            &WriteRequest {
                path: "deep/er/file.txt".to_owned(),
                content: "first".to_owned(),
            },
            &launch,
        )
        .unwrap();
        assert_eq!(shown, display_path(&scratch.0.join("deep/er/file.txt")));
        assert_eq!(written, format!("Successfully wrote to {shown}"));
        assert_eq!(scratch.read("deep/er/file.txt"), b"first");
        write_text(
            &WriteRequest {
                path: "deep/er/file.txt".to_owned(),
                content: "second".to_owned(),
            },
            &launch,
        )
        .unwrap();
        assert_eq!(scratch.read("deep/er/file.txt"), b"second");
    }

    #[cfg(unix)]
    #[test]
    fn write_keeps_an_existing_files_mode() {
        use std::os::unix::fs::PermissionsExt;
        let scratch = Scratch::new("mode");
        let launch = context(&scratch.0, &scratch.0);
        let path = scratch.file("script.sh", b"#!/bin/sh\n");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o750)).unwrap();
        write_text(
            &WriteRequest {
                path: "script.sh".to_owned(),
                content: "#!/bin/sh\necho hi\n".to_owned(),
            },
            &launch,
        )
        .unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o750);
    }

    #[test]
    fn edit_maps_cross_line_matches_and_reports_cr_lines() {
        let scratch = Scratch::new("mapped-endings");
        let launch = context(&scratch.0, &scratch.0);
        scratch.file("mixed.txt", "é\r\n中\nneedle\rtail\r\nend\n".as_bytes());
        let reply = change(
            "mixed.txt",
            &[("中\nneedle", "新\nvalue"), ("end", "END")],
            &launch,
        )
        .unwrap();
        assert!(reply.contains("edits[0]: line 2"), "{reply}");
        assert!(reply.contains("edits[1]: line 4"), "{reply}");
        assert_eq!(
            scratch.read("mixed.txt"),
            "é\r\n新\r\nvalue\rtail\r\nEND\n".as_bytes()
        );
        scratch.file("same.txt", b"a\r\nb\nc\r");
        assert!(change("same.txt", &[("a\nb\nc\n", "a\nb\nc\n")], &launch).is_err());
        assert_eq!(scratch.read("same.txt"), b"a\r\nb\nc\r");
    }

    #[cfg(unix)]
    #[test]
    fn atomic_saves_follow_existing_and_dangling_symlinks() {
        use std::os::unix::fs::symlink;
        let scratch = Scratch::new("save-links");
        let launch = context(&scratch.0, &scratch.0);
        let target = scratch.file("target.txt", b"old");
        let link = scratch.0.join("link.txt");
        symlink("target.txt", &link).unwrap();
        change("link.txt", &[("old", "new")], &launch).unwrap();
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(std::fs::read(&target).unwrap(), b"new");
        save_text(&link, b"again").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"again");
        let dangling = scratch.0.join("dangling.txt");
        symlink("created.txt", &dangling).unwrap();
        save_text(&dangling, b"created").unwrap();
        assert!(
            std::fs::symlink_metadata(&dangling)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(scratch.read("created.txt"), b"created");
    }

    fn file_symlink(target: &Path, link: &Path) -> bool {
        #[cfg(unix)]
        let result = std::os::unix::fs::symlink(target, link);
        #[cfg(windows)]
        let result = std::os::windows::fs::symlink_file(target, link);
        match result {
            Ok(()) => true,
            Err(error) => {
                #[cfg(windows)]
                if error.raw_os_error() == Some(1314) && std::env::var_os("CI").is_none() {
                    eprintln!(
                        "skipped: creating a Windows symlink requires Developer Mode or its privilege"
                    );
                    return false;
                }
                panic!("cannot create fixture symlink {}: {error}", link.display());
            }
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_root_relative_symlink_write_matches_native_resolution() {
        // A root-relative target uses the current drive, which may differ from the temp drive.
        let scratch = Scratch(std::env::current_dir().unwrap().join(format!(
            "fastexec-rooted-link-{}",
            crate::tasks::random_chars(8).unwrap()
        )));
        std::fs::create_dir(&scratch.0).unwrap();
        let launch = context(&scratch.0, &scratch.0);
        let target = scratch.file("target.txt", b"before");
        let shown = display_path(&target);
        assert_eq!(shown.as_bytes().get(1), Some(&b':'), "{shown}");
        let rooted = PathBuf::from(shown[2..].replace('/', "\\"));
        let link = scratch.0.join("link.txt");
        if !file_symlink(&rooted, &link) {
            return;
        }
        assert_eq!(std::fs::read(&link).unwrap(), b"before");
        write_text(
            &WriteRequest {
                path: display_path(&link),
                content: "after".into(),
            },
            &launch,
        )
        .unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"after");
        change("link.txt", &[("after", "edited")], &launch).unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"edited");
        // Recreating a dangling root-relative target must still leave the link in place.
        std::fs::remove_file(&target).unwrap();
        write_text(
            &WriteRequest {
                path: display_path(&link),
                content: "created".into(),
            },
            &launch,
        )
        .unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"created");
        assert_eq!(std::fs::read(&link).unwrap(), b"created");
        assert!(
            std::fs::symlink_metadata(link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[test]
    fn edit_rejects_a_retargeted_symlink_without_changing_either_file() {
        let scratch = Scratch::new("edit-retargeted-link");
        let first = scratch.file("first.txt", b"old content");
        let other = scratch.file("other.txt", b"must stay intact");
        let link = scratch.0.join("link.txt");
        if !file_symlink(&first, &link) {
            return;
        }
        let source = EditSource::read(link.clone()).unwrap();
        std::fs::remove_file(&link).unwrap();
        assert!(file_symlink(&other, &link));
        let error = source.save(b"edited content").unwrap_err();
        assert!(
            error.to_string().contains("changed after it was read"),
            "{error}"
        );
        assert_eq!(scratch.read("first.txt"), b"old content");
        assert_eq!(scratch.read("other.txt"), b"must stay intact");
    }

    #[test]
    fn edit_rejects_another_inode_even_when_its_content_matches() {
        let scratch = Scratch::new("edit-replaced-inode");
        let path = scratch.file("target.txt", b"old content");
        let source = EditSource::read(path.clone()).unwrap();
        std::fs::rename(&path, scratch.0.join("retired.txt")).unwrap();
        std::fs::write(&path, b"old content").unwrap();
        let error = source.save(b"edited content").unwrap_err();
        assert!(
            error.to_string().contains("changed after it was read"),
            "{error}"
        );
        assert_eq!(scratch.read("target.txt"), b"old content");
        assert_eq!(scratch.read("retired.txt"), b"old content");
        assert_eq!(std::fs::read_dir(&scratch.0).unwrap().count(), 2);
    }

    #[test]
    fn edit_rejects_in_place_changes_to_the_file_it_read() {
        let scratch = Scratch::new("edit-changed-content");
        for changed in [b"other value".as_slice(), b"old content plus bytes", b"old"] {
            let path = scratch.file("target.txt", b"old content");
            let source = EditSource::read(path.clone()).unwrap();
            std::fs::write(&path, changed).unwrap();
            let error = source.save(b"edited content").unwrap_err();
            assert!(
                error.to_string().contains("changed after it was read"),
                "{error}"
            );
            assert_eq!(scratch.read("target.txt"), changed);
            assert_eq!(std::fs::read_dir(&scratch.0).unwrap().count(), 1);
        }
    }

    #[cfg(unix)]
    #[test]
    fn edit_rejects_a_retargeted_parent_directory_link() {
        use std::os::unix::fs::symlink;
        let scratch = Scratch::new("edit-retargeted-parent");
        scratch.file("first/target.txt", b"old content");
        scratch.file("other/target.txt", b"must stay intact");
        let link = scratch.0.join("directory");
        symlink(scratch.0.join("first"), &link).unwrap();
        let source = EditSource::read(link.join("target.txt")).unwrap();
        std::fs::remove_file(&link).unwrap();
        symlink(scratch.0.join("other"), &link).unwrap();
        let error = source.save(b"edited content").unwrap_err();
        assert!(
            error.to_string().contains("changed after it was read"),
            "{error}"
        );
        assert_eq!(scratch.read("first/target.txt"), b"old content");
        assert_eq!(scratch.read("other/target.txt"), b"must stay intact");
    }

    #[test]
    fn write_creates_dangling_symlink_target_parents() {
        let scratch = Scratch::new("write-link-parents");
        let launch = context(&scratch.0, &scratch.0);
        let links = scratch.0.join("links");
        std::fs::create_dir(&links).unwrap();
        for (name, absolute, chained) in [
            ("relative", false, false),
            ("absolute", true, false),
            ("chained", false, true),
        ] {
            let target = scratch.0.join(name).join("missing").join("nested.txt");
            let stored = if absolute {
                target.clone()
            } else {
                Path::new("..")
                    .join(name)
                    .join("missing")
                    .join("nested.txt")
            };
            let direct = links.join(format!("{name}.link"));
            if !file_symlink(&stored, &direct) {
                return;
            }
            let direct_before = std::fs::read_link(&direct).unwrap();
            let link = if chained {
                let link = links.join("chain.link");
                if !file_symlink(direct.file_name().unwrap().as_ref(), &link) {
                    return;
                }
                link
            } else {
                direct.clone()
            };
            let link_before = std::fs::read_link(&link).unwrap();
            let path = link.to_string_lossy().into_owned();
            write_text(
                &WriteRequest {
                    path: path.clone(),
                    content: "created".to_owned(),
                },
                &launch,
            )
            .unwrap();
            assert_eq!(std::fs::read(&target).unwrap(), b"created");
            assert_eq!(std::fs::read(&link).unwrap(), b"created");
            assert_eq!(std::fs::read_link(&direct).unwrap(), direct_before);
            assert_eq!(std::fs::read_link(&link).unwrap(), link_before);
            assert!(
                std::fs::symlink_metadata(&link)
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );
            change(&path, &[("created", "edited")], &launch).unwrap();
            assert_eq!(std::fs::read(&target).unwrap(), b"edited");
            assert_eq!(std::fs::read_link(&direct).unwrap(), direct_before);
            assert_eq!(std::fs::read_link(&link).unwrap(), link_before);
            assert!(
                std::fs::symlink_metadata(&link)
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn directory_only_spellings_never_overwrite_regular_files() {
        let scratch = Scratch::new("directory-suffix");
        let launch = context(&scratch.0, &scratch.0);
        let original = scratch.file("target", b"original");
        for suffix in ["/", "/."] {
            let name = format!("target{suffix}");
            let link = scratch.0.join("link");
            std::os::unix::fs::symlink(&name, &link).unwrap();
            for requested in [&name[..], "link"] {
                assert!(
                    whole(requested, &launch, false).is_err(),
                    "{name}: {requested}"
                );
                assert!(
                    change(requested, &[("original", "bad")], &launch).is_err(),
                    "{name}: {requested}"
                );
                assert!(
                    crate::service::validate_cwd(requested, &launch).is_err(),
                    "{name}: {requested}"
                );
                assert!(
                    write_text(
                        &WriteRequest {
                            path: requested.into(),
                            content: "bad".into()
                        },
                        &launch
                    )
                    .is_err()
                );
            }
            assert_eq!(std::fs::read(&original).unwrap(), b"original");
            assert_eq!(
                std::fs::read_link(&link).unwrap().as_os_str(),
                Path::new(&name).as_os_str()
            );
            std::fs::remove_file(&link).unwrap();
            std::os::unix::fs::symlink("target", &link).unwrap();
            let opened = EditSource::read(link.clone()).unwrap();
            std::fs::remove_file(&link).unwrap();
            std::os::unix::fs::symlink(&name, &link).unwrap();
            assert!(opened.save(b"bad").is_err(), "{name}");
            assert_eq!(std::fs::read(&original).unwrap(), b"original");
            std::fs::remove_file(link).unwrap();
        }
        let sibling = scratch.file("sibling.txt", b"keep sibling");
        let invalid = "target/../sibling.txt";
        assert!(whole(invalid, &launch, false).is_err());
        assert!(change(invalid, &[("keep sibling", "bad")], &launch).is_err());
        assert_eq!(std::fs::read(sibling).unwrap(), b"keep sibling");
    }

    #[test]
    fn edit_does_not_create_dangling_symlink_target_parents() {
        let scratch = Scratch::new("edit-link-parents");
        let launch = context(&scratch.0, &scratch.0);
        let link = scratch.0.join("link.txt");
        if !file_symlink(&Path::new("missing").join("nested.txt"), &link) {
            return;
        }
        let link_before = std::fs::read_link(&link).unwrap();
        assert!(change("link.txt", &[("old", "new")], &launch).is_err());
        assert!(!scratch.0.join("missing").exists());
        assert_eq!(std::fs::read_link(&link).unwrap(), link_before);
    }

    #[test]
    fn write_does_not_replace_a_symlink_targets_non_directory_parent() {
        let scratch = Scratch::new("write-link-obstruction");
        let launch = context(&scratch.0, &scratch.0);
        scratch.file("blocked", b"keep this file");
        let link = scratch.0.join("link.txt");
        let target = Path::new("blocked").join("missing").join("file.txt");
        if !file_symlink(&target, &link) {
            return;
        }
        let link_before = std::fs::read_link(&link).unwrap();
        assert!(
            write_text(
                &WriteRequest {
                    path: "link.txt".to_owned(),
                    content: "new".to_owned()
                },
                &launch,
            )
            .is_err()
        );
        assert_eq!(scratch.read("blocked"), b"keep this file");
        assert_eq!(std::fs::read_link(&link).unwrap(), link_before);
    }

    #[test]
    fn atomic_saves_refuse_to_silently_split_hard_links() {
        let scratch = Scratch::new("save-hard-links");
        let launch = context(&scratch.0, &scratch.0);
        let original = scratch.file("original.txt", b"original");
        std::fs::hard_link(&original, scratch.0.join("alias.txt")).unwrap();
        let error = save_text(&original, b"replacement")
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("hard links") && error.contains("exec"),
            "{error}"
        );
        assert!(change("original.txt", &[("original", "replacement")], &launch).is_err());
        assert_eq!(scratch.read("original.txt"), b"original");
        assert_eq!(scratch.read("alias.txt"), b"original");
    }

    #[cfg(target_os = "freebsd")]
    fn freebsd_acl_fixture(scratch: &Scratch) -> Option<bool> {
        let kind = crate::freebsd_fs::test_acl_is_nfs4(&File::open(&scratch.0).unwrap()).unwrap();
        if kind.is_none() {
            assert_ne!(
                std::env::var("FASTEXEC_REQUIRE_ACL_TEST").as_deref(),
                Ok("1"),
                "the mandatory FreeBSD ACL fixture does not support ACLs"
            );
            eprintln!("ACL filesystem cases run in scripts/ci/freebsd-acl-tests.sh");
        }
        if kind.is_some() {
            // SAFETY: geteuid has no preconditions; UID 1 is the separate ACL test subject.
            assert_ne!(
                unsafe { libc::geteuid() },
                1,
                "the ACL fixture must not run as its peer UID"
            );
        }
        kind
    }

    #[cfg(target_os = "freebsd")]
    fn freebsd_set_acl(path: &Path, args: &[&str]) {
        let output = std::process::Command::new("setfacl")
            .args(args)
            .arg(path)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "setfacl failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(target_os = "freebsd")]
    fn freebsd_inherit_acl(path: &Path, nfs4: bool) {
        if nfs4 {
            freebsd_set_acl(
                path,
                &[
                    "-a",
                    "0",
                    "owner@:rwxpDdaARWcCos:fd:allow,user:1:rwxpDdaARWcCos:fd:allow",
                ],
            );
        } else {
            freebsd_set_acl(path, &["-d", "-m", "u::rwx,u:1:rwx,g::r-x,m::rwx,o::---"]);
        }
    }

    #[cfg(target_os = "freebsd")]
    #[test]
    fn freebsd_user_attributes_survive_write_and_edit() {
        use std::os::fd::AsRawFd;
        let scratch = Scratch::new("save-freebsd-attributes");
        let path = scratch.file("original.txt", b"original");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let value = b"binary\0attribute\xff";
        // SAFETY: the descriptor, terminated name and byte buffer are live.
        assert_eq!(
            unsafe {
                libc::extattr_set_fd(
                    file.as_raw_fd(),
                    libc::EXTATTR_NAMESPACE_USER,
                    c"fastexec-test".as_ptr(),
                    value.as_ptr().cast(),
                    value.len(),
                )
            },
            value.len() as isize
        );
        save_text(&path, b"replacement").unwrap();
        let launch = context(&scratch.0, &scratch.0);
        change("original.txt", &[("replacement", "edited")], &launch).unwrap();
        let saved = File::open(&path).unwrap();
        let mut actual = [0u8; 64];
        // SAFETY: the live descriptor names the replacement and the output buffer is valid.
        let count = unsafe {
            libc::extattr_get_fd(
                saved.as_raw_fd(),
                libc::EXTATTR_NAMESPACE_USER,
                c"fastexec-test".as_ptr(),
                actual.as_mut_ptr().cast(),
                actual.len(),
            )
        };
        assert_eq!(count, value.len() as isize);
        assert_eq!(&actual[..value.len()], value);
        assert_eq!(scratch.read("original.txt"), b"edited");
        assert_eq!(
            std::fs::read_dir(&scratch.0).unwrap().count(),
            1,
            "staging leaked"
        );
    }

    #[cfg(target_os = "freebsd")]
    #[test]
    fn freebsd_acl_original_survives_write_and_edit() {
        let scratch = Scratch::new("save-freebsd-acl");
        let Some(nfs4) = freebsd_acl_fixture(&scratch) else {
            return;
        };
        let path = scratch.file("original.txt", b"original");
        if nfs4 {
            freebsd_set_acl(&path, &["-a", "0", "user:1:r-----a-R-c--s::allow"]);
        } else {
            freebsd_set_acl(&path, &["-m", "u:1:r--,m::r--"]);
        }
        let original = File::open(&path).unwrap();
        save_text(&path, b"replacement").unwrap();
        let launch = context(&scratch.0, &scratch.0);
        change("original.txt", &[("replacement", "edited")], &launch).unwrap();
        assert!(crate::freebsd_fs::test_equal_acl(&original, &File::open(&path).unwrap()).unwrap());
        assert_eq!(scratch.read("original.txt"), b"edited");
    }

    #[cfg(target_os = "freebsd")]
    #[test]
    fn freebsd_acl_staging_is_private_and_new_files_keep_the_default_acl() {
        use std::os::unix::fs::PermissionsExt;
        let scratch = Scratch::new("save-freebsd-inherit");
        let Some(nfs4) = freebsd_acl_fixture(&scratch) else {
            return;
        };
        freebsd_inherit_acl(&scratch.0, nfs4);
        let probe = Staged::new(&scratch.0, false).unwrap();
        let private = Staged::new(&scratch.0, true).unwrap();
        let directory = private.directory.as_ref().unwrap();
        crate::runtime::create_private_unix_check(directory).unwrap();
        assert_eq!(
            private
                .file
                .as_ref()
                .unwrap()
                .metadata()
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        let complete = scratch.0.join("complete.txt");
        save_text(&complete, b"complete content").unwrap();
        assert!(
            crate::freebsd_fs::test_equal_acl(
                probe.file.as_ref().unwrap(),
                &File::open(&complete).unwrap(),
            )
            .unwrap()
        );
        assert_eq!(std::fs::read(&complete).unwrap(), b"complete content");
        drop(private);
        drop(probe);
        assert_eq!(
            std::fs::read_dir(&scratch.0).unwrap().count(),
            1,
            "staging leaked"
        );
    }

    #[cfg(target_os = "freebsd")]
    #[test]
    fn freebsd_acl_existing_runtime_policy_is_refused_without_changing_it() {
        let scratch = Scratch::new("runtime-freebsd-existing-acl");
        let Some(nfs4) = freebsd_acl_fixture(&scratch) else {
            return;
        };
        let runtime = scratch.0.join("runtime");
        crate::runtime::create_private_new(&runtime).unwrap();
        if nfs4 {
            freebsd_set_acl(&runtime, &["-a", "0", "user:1:r-x---a-R-c--s::allow"]);
        } else {
            freebsd_set_acl(&runtime, &["-m", "u:1:r-x,m::---"]);
        }
        let listing = |path: &Path| {
            let output = std::process::Command::new("getfacl")
                .arg("-q")
                .arg(path)
                .output()
                .unwrap();
            assert!(output.status.success());
            output.stdout
        };
        let before = listing(&runtime);
        assert!(crate::runtime::create_private(&runtime).is_err());
        assert_eq!(listing(&runtime), before);
    }

    #[cfg(target_os = "freebsd")]
    #[test]
    fn freebsd_acl_new_runtime_directories_block_inheritance() {
        let scratch = Scratch::new("runtime-freebsd-inherit");
        let Some(nfs4) = freebsd_acl_fixture(&scratch) else {
            return;
        };
        freebsd_inherit_acl(&scratch.0, nfs4);
        let middle = scratch.0.join("middle");
        let leaf = middle.join("runtime");
        crate::runtime::create_private(&leaf).unwrap();
        crate::runtime::create_private_unix_check(&middle).unwrap();
        crate::runtime::create_private_unix_check(&leaf).unwrap();
        let child = leaf.join("child");
        crate::runtime::create_private_new(&child).unwrap();
        crate::runtime::create_private_unix_check(&child).unwrap();
    }

    #[cfg(target_os = "freebsd")]
    #[test]
    fn freebsd_acl_an_inaccessible_new_directory_is_refused_before_use() {
        let scratch = Scratch::new("runtime-freebsd-denied-inherit");
        let Some(nfs4) = freebsd_acl_fixture(&scratch) else {
            return;
        };
        let require_denied =
            std::env::var("FASTEXEC_REQUIRE_DENIED_ACL_TEST").as_deref() == Ok("1");
        if !nfs4 {
            assert!(
                !require_denied,
                "the required denied-ACL fixture needs NFSv4 ACLs"
            );
            return;
        }
        let target = scratch.file("original.txt", b"original");
        let original = File::open(&target).unwrap();
        freebsd_set_acl(&scratch.0, &["-a", "0", "owner@:r-x-----------:di:deny"]);
        // UFS may constrain inherited denies by the requested mode; ZFS restricted preserves them.
        let probe = scratch.0.join("probe");
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&probe)
            .unwrap();
        let readable = File::open(&probe).is_ok();
        if require_denied {
            assert!(
                !readable,
                "the mandatory non-root restricted fixture must deny directory reads"
            );
        }
        let denied = scratch.0.join("denied");
        let result = crate::runtime::create_private_new(&denied);
        if readable {
            result.unwrap();
            crate::runtime::create_private_unix_check(&denied).unwrap();
            save_text(&target, b"replacement").unwrap();
            assert_eq!(std::fs::read(&target).unwrap(), b"replacement");
        } else {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
            assert!(
                !denied.exists(),
                "an unusable empty directory was not cleaned up"
            );
            assert!(save_text(&target, b"replacement").is_err());
            assert_eq!(std::fs::read(&target).unwrap(), b"original");
            assert!(
                crate::freebsd_fs::test_equal_acl(&original, &File::open(&target).unwrap())
                    .unwrap()
            );
            assert_eq!(
                std::fs::read_dir(&scratch.0).unwrap().count(),
                2,
                "failed staging leaked"
            );
        }
    }

    #[cfg(target_os = "macos")]
    fn macos_chmod_acl(path: &Path, entry: Option<&str>) {
        let mut command = std::process::Command::new("/bin/chmod");
        if let Some(entry) = entry {
            command.args(["+a", entry]);
        } else {
            command.arg("-N");
        }
        let output = command.arg(path).output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_staging_rejects_parent_acl_inheritance_at_creation() {
        use std::os::unix::fs::MetadataExt;
        let scratch = Scratch::new("save-macos-private-acl");
        macos_chmod_acl(&scratch.0, Some("everyone allow read,file_inherit"));
        let ordinary_path = scratch.0.join("ordinary");
        let ordinary = create_staging_file(&ordinary_path, false).unwrap();
        assert!(
            macos_access::has_entries(&ordinary).unwrap(),
            "fixture must inherit its parent ACL"
        );
        let private_path = scratch.0.join("private");
        let private = create_staging_file(&private_path, true).unwrap();
        assert_eq!(private.metadata().unwrap().mode() & 0o077, 0);
        assert!(!macos_access::has_entries(&private).unwrap());
        // Final new files acquire ordinary defaults only after staging has finished.
        let final_path = scratch.0.join("final");
        save_text(&final_path, b"complete").unwrap();
        let final_file = File::open(&final_path).unwrap();
        assert_eq!(
            macos_access::acl_text(&final_file).unwrap(),
            macos_access::acl_text(&ordinary).unwrap()
        );
        assert_eq!(std::fs::read(final_path).unwrap(), b"complete");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_atomic_save_preserves_an_explicit_deny_acl() {
        let scratch = Scratch::new("save-macos-deny-acl");
        let path = scratch.file("target", b"original");
        macos_chmod_acl(&path, Some("everyone deny read"));
        let original = OpenOptions::new().write(true).open(&path).unwrap();
        let before = macos_access::acl_text(&original).unwrap();
        assert!(before.contains("deny"));
        save_text(&path, b"replacement").unwrap();
        let replaced = OpenOptions::new().write(true).open(&path).unwrap();
        assert_eq!(macos_access::acl_text(&replaced).unwrap(), before);
        // Remove the test's deny entry only after checking it survived the replacement.
        macos_chmod_acl(&path, None);
        assert_eq!(std::fs::read(path).unwrap(), b"replacement");
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_atomic_save_does_not_add_parent_grants_to_a_file_without_an_acl() {
        let scratch = Scratch::new("save-macos-no-acl");
        macos_chmod_acl(&scratch.0, Some("everyone allow read,file_inherit"));
        let path = scratch.file("target", b"original");
        macos_chmod_acl(&path, None);
        let original = File::open(&path).unwrap();
        assert!(!macos_access::has_entries(&original).unwrap());
        save_text(&path, b"replacement").unwrap();
        let replaced = File::open(&path).unwrap();
        assert!(!macos_access::has_entries(&replaced).unwrap());
        assert_eq!(std::fs::read(path).unwrap(), b"replacement");
    }

    #[cfg(windows)]
    fn windows_security_text(path: &Path) -> String {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::{ERROR_SUCCESS, HLOCAL, LocalFree};
        use windows_sys::Win32::Security::Authorization::{
            ConvertSecurityDescriptorToStringSecurityDescriptorW, GetSecurityInfo, SDDL_REVISION_1,
            SE_FILE_OBJECT,
        };
        use windows_sys::Win32::Security::{
            DACL_SECURITY_INFORMATION, GROUP_SECURITY_INFORMATION, OWNER_SECURITY_INFORMATION,
        };
        struct Allocation(HLOCAL);
        impl Drop for Allocation {
            fn drop(&mut self) {
                // SAFETY: both security APIs below return LocalAlloc-owned buffers.
                unsafe { LocalFree(self.0) };
            }
        }
        let file = File::open(path).unwrap();
        let mut descriptor = std::ptr::null_mut();
        let information =
            OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION;
        // SAFETY: the file and the descriptor output live for this synchronous query.
        let result = unsafe {
            GetSecurityInfo(
                file.as_raw_handle(),
                SE_FILE_OBJECT,
                information,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut descriptor,
            )
        };
        assert_eq!(result, ERROR_SUCCESS);
        let _descriptor = Allocation(descriptor);
        let (mut text, mut length) = (std::ptr::null_mut(), 0u32);
        // SAFETY: descriptor is live, and both outputs are writable.
        assert_ne!(
            unsafe {
                ConvertSecurityDescriptorToStringSecurityDescriptorW(
                    descriptor,
                    SDDL_REVISION_1,
                    information,
                    &mut text,
                    &mut length,
                )
            },
            0
        );
        let _text = Allocation(text.cast());
        // SAFETY: the conversion returned length UTF-16 units, including the terminator.
        let units = unsafe { std::slice::from_raw_parts(text, length as usize) };
        String::from_utf16(units.strip_suffix(&[0]).unwrap_or(units)).unwrap()
    }

    /// Windows may set the auto-inheritance bookkeeping flag when applying a DACL, without
    /// changing owner, group, protection, or any ACE. Compare all of those security properties.
    #[cfg(windows)]
    fn windows_without_auto_inherited(sddl: &str) -> String {
        let Some(start) = sddl.find("D:") else {
            return sddl.to_owned();
        };
        let flags = start + 2;
        let end = sddl[flags..].find('(').map_or(sddl.len(), |at| flags + at);
        format!(
            "{}{}{}",
            &sddl[..flags],
            sddl[flags..end].replace("AI", ""),
            &sddl[end..]
        )
    }

    #[cfg(windows)]
    #[test]
    fn windows_atomic_save_preserves_owner_group_dacl_and_named_streams() {
        let scratch = Scratch::new("save-windows-metadata");
        let target = scratch.0.join("target.txt");
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::GetVolumeInformationByHandleW;
        let mut original = create_staging_file(&target, true).unwrap();
        original.write_all(b"original").unwrap();
        let mut flags = 0u32;
        // SAFETY: only flags are requested from the volume holding this live file handle.
        assert_ne!(
            unsafe {
                GetVolumeInformationByHandleW(
                    original.as_raw_handle(),
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    &mut flags,
                    std::ptr::null_mut(),
                    0,
                )
            },
            0
        );
        drop(original);
        let stream = PathBuf::from(format!("{}:fastexec-test", target.display()));
        let named_streams = flags & 0x00040000 != 0;
        if named_streams {
            std::fs::write(&stream, b"a named stream").unwrap();
        }
        let before = (flags & 0x00000008 != 0).then(|| windows_security_text(&target));
        save_text(&target, b"replacement").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"replacement");
        if named_streams {
            assert_eq!(std::fs::read(&stream).unwrap(), b"a named stream");
        }
        if let Some(before) = before {
            assert_eq!(
                windows_without_auto_inherited(&windows_security_text(&target)),
                windows_without_auto_inherited(&before)
            );
        }
        assert_eq!(std::fs::read_dir(&scratch.0).unwrap().count(), 1);
    }

    #[cfg(windows)]
    #[test]
    fn windows_publish_checks_the_open_originals_identity() {
        let scratch = Scratch::new("save-windows-identity");
        let target = scratch.file("target.txt", b"original");
        let original = File::open(&target).unwrap();
        unchanged_windows_target(&original, &target).unwrap();
        std::fs::rename(&target, scratch.0.join("retired.txt")).unwrap();
        std::fs::write(&target, b"a different file").unwrap();
        let error = unchanged_windows_target(&original, &target).unwrap_err();
        assert!(error.to_string().contains("target changed"), "{error}");
        assert_eq!(scratch.read("target.txt"), b"a different file");
        assert_eq!(scratch.read("retired.txt"), b"original");
    }

    #[test]
    fn replacement_backup_is_removed_after_success() {
        let scratch = Scratch::new("replace-backup-success");
        let target = scratch.file("target.txt", b"original");
        let staged = scratch.file("staged.txt", b"replacement");
        replace_with_backup(
            &target,
            |backup| {
                std::fs::rename(&target, backup)?;
                std::fs::rename(&staged, &target)
            },
            |_| panic!("successful publication must not roll back"),
        )
        .unwrap();
        assert_eq!(scratch.read("target.txt"), b"replacement");
        assert_eq!(std::fs::read_dir(&scratch.0).unwrap().count(), 1);
    }

    #[test]
    fn replacement_failure_before_move_keeps_the_original() {
        for code in [1175, 1176, 5] {
            let scratch = Scratch::new(&format!("replace-backup-failed-{code}"));
            let target = scratch.file("target.txt", b"original");
            let staged = scratch.file("staged.txt", b"replacement");
            let error = replace_with_backup(
                &target,
                |_| Err(io::Error::from_raw_os_error(code)),
                |_| panic!("this failure leaves the original in place"),
            )
            .unwrap_err();
            assert_eq!(error.raw_os_error(), Some(code));
            assert_eq!(scratch.read("target.txt"), b"original");
            assert_eq!(std::fs::read(staged).unwrap(), b"replacement");
            assert_eq!(std::fs::read_dir(&scratch.0).unwrap().count(), 2);
        }
    }

    #[test]
    fn replacement_failure_after_move_restores_the_original() {
        let scratch = Scratch::new("replace-backup-restored");
        let target = scratch.file("target.txt", b"original");
        let staged = scratch.file("staged.txt", b"replacement");
        let error = replace_with_backup(
            &target,
            |backup| {
                std::fs::rename(&target, backup)?;
                Err(io::Error::from_raw_os_error(1177))
            },
            |backup| publish(backup, &target, false),
        )
        .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(1177));
        assert_eq!(scratch.read("target.txt"), b"original");
        assert_eq!(std::fs::read(staged).unwrap(), b"replacement");
        assert_eq!(std::fs::read_dir(&scratch.0).unwrap().count(), 2);
    }

    #[test]
    fn failed_replacement_and_rollback_keep_a_recoverable_original() {
        let scratch = Scratch::new("replace-backup-retained");
        let target = scratch.file("target.txt", b"original");
        let mut staged = Staged::new(&scratch.0, true).unwrap();
        staged
            .file
            .as_mut()
            .unwrap()
            .write_all(b"replacement")
            .unwrap();
        let recovery = std::cell::RefCell::new(PathBuf::new());
        let error = replace_with_backup(
            &target,
            |backup| {
                *recovery.borrow_mut() = backup.to_path_buf();
                std::fs::rename(&target, backup)?;
                Err(io::Error::from_raw_os_error(1177))
            },
            |_| {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected rollback failure",
                ))
            },
        )
        .unwrap_err();
        drop(staged);
        assert!(!target.exists());
        let recovery = recovery.into_inner();
        assert_eq!(std::fs::read(&recovery).unwrap(), b"original");
        assert!(
            error.to_string().contains(&display_path(&recovery)),
            "{error}"
        );
        assert_eq!(std::fs::read_dir(&scratch.0).unwrap().count(), 1);
    }

    #[test]
    fn replacement_rollback_does_not_clobber_a_concurrent_target() {
        let scratch = Scratch::new("replace-backup-concurrent");
        let target = scratch.file("target.txt", b"original");
        let recovery = std::cell::RefCell::new(PathBuf::new());
        let error = replace_with_backup(
            &target,
            |backup| {
                *recovery.borrow_mut() = backup.to_path_buf();
                std::fs::rename(&target, backup)?;
                std::fs::write(&target, b"a concurrent writer")?;
                Err(io::Error::from_raw_os_error(1177))
            },
            |backup| publish(backup, &target, false),
        )
        .unwrap_err();
        assert_eq!(scratch.read("target.txt"), b"a concurrent writer");
        let recovery = recovery.into_inner();
        assert_eq!(std::fs::read(&recovery).unwrap(), b"original");
        assert!(
            error.to_string().contains(&display_path(&recovery)),
            "{error}"
        );
    }

    #[test]
    fn exclusive_publish_does_not_replace_a_raced_target() {
        let scratch = Scratch::new("exclusive-publish");
        let staged = scratch.file("staged.txt", b"new");
        let target = scratch.file("target.txt", b"another writer");
        assert!(publish(&staged, &target, false).is_err());
        assert_eq!(scratch.read("target.txt"), b"another writer");
        assert_eq!(scratch.read("staged.txt"), b"new");
    }

    #[cfg(unix)]
    #[test]
    fn atomic_saves_refuse_special_files_without_opening_them() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::FileTypeExt;
        let scratch = Scratch::new("save-special");
        let fifo = scratch.0.join("fifo");
        let name = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        // SAFETY: the terminated path is owned by the test.
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        assert!(save_text(&fifo, b"must not replace the FIFO").is_err());
        assert!(
            std::fs::symlink_metadata(&fifo)
                .unwrap()
                .file_type()
                .is_fifo()
        );
        assert!(save_text(&scratch.0, b"must not replace the directory").is_err());
        assert!(scratch.0.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn privileged_save_preserves_owner_group_and_mode() {
        use std::os::fd::AsRawFd;
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        // SAFETY: getuid has no side effects.
        if unsafe { libc::geteuid() } != 0 {
            return;
        }
        let scratch = Scratch::new("save-owner");
        let path = scratch.file("owned.txt", b"original");
        let file = File::open(&path).unwrap();
        // SAFETY: only this test's file is changed; the numeric IDs need no account lookup.
        assert_eq!(unsafe { libc::fchown(file.as_raw_fd(), 65534, 65534) }, 0);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        save_text(&path, b"changed").unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        assert_eq!(
            (metadata.uid(), metadata.gid(), metadata.mode() & 0o777),
            (65534, 65534, 0o640)
        );
    }

    #[cfg(target_os = "linux")]
    fn set_test_xattr(path: &Path, name: &std::ffi::CStr, value: &[u8]) {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        // SAFETY: all buffers and terminated strings remain live for the call.
        assert_eq!(
            unsafe {
                libc::setxattr(
                    path.as_ptr(),
                    name.as_ptr(),
                    value.as_ptr().cast(),
                    value.len(),
                    0,
                )
            },
            0,
            "{}",
            io::Error::last_os_error()
        );
    }

    #[cfg(target_os = "linux")]
    fn test_xattr(path: &Path, name: &std::ffi::CStr) -> Option<Vec<u8>> {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        let path = CString::new(path.as_os_str().as_bytes()).unwrap();
        let mut value = vec![0u8; 1024];
        // SAFETY: the value buffer is writable and the strings are terminated.
        let size = unsafe {
            libc::getxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_mut_ptr().cast(),
                value.len(),
            )
        };
        if size < 0 {
            assert_eq!(
                io::Error::last_os_error().raw_os_error(),
                Some(libc::ENODATA)
            );
            return None;
        }
        value.truncate(size as usize);
        Some(value)
    }

    #[cfg(target_os = "linux")]
    fn test_acl() -> Vec<u8> {
        // Linux POSIX ACL xattr version 2: owner, named user, group, mask, other.
        let mut acl = 2u32.to_le_bytes().to_vec();
        for (tag, permissions, id) in [
            (1u16, 7u16, u32::MAX),
            (2, 6, 65534),
            (4, 0, u32::MAX),
            (16, 6, u32::MAX),
            (32, 0, u32::MAX),
        ] {
            acl.extend_from_slice(&tag.to_le_bytes());
            acl.extend_from_slice(&permissions.to_le_bytes());
            acl.extend_from_slice(&id.to_le_bytes());
        }
        acl
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn atomic_save_preserves_existing_acl_and_xattrs() {
        let scratch = Scratch::new("save-attributes");
        let path = scratch.file("target.txt", b"original");
        let acl = test_acl();
        set_test_xattr(&path, c"user.fastexec-test", b"retained");
        set_test_xattr(&path, c"system.posix_acl_access", &acl);
        save_text(&path, b"changed").unwrap();
        assert_eq!(
            test_xattr(&path, c"user.fastexec-test"),
            Some(b"retained".to_vec())
        );
        assert_eq!(test_xattr(&path, c"system.posix_acl_access"), Some(acl));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn atomic_save_does_not_gain_the_directories_default_acl() {
        use std::os::unix::fs::PermissionsExt;
        let scratch = Scratch::new("save-inherited-acl");
        let path = scratch.file("target.txt", b"original");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert!(test_xattr(&path, c"system.posix_acl_access").is_none());
        set_test_xattr(&scratch.0, c"system.posix_acl_default", &test_acl());
        save_text(&path, b"changed").unwrap();
        assert!(test_xattr(&path, c"system.posix_acl_access").is_none());
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o640
        );
    }

    #[test]
    fn regression_edit_preserves_unmatched_line_endings() {
        let scratch = Scratch::new("mixed-endings");
        let launch = context(&scratch.0, &scratch.0);
        let original = b"\xef\xbb\xbffirst\r\nneedle\r\nlast\ncarriage\rreturn\n";
        scratch.file("mixed.txt", original);
        change("mixed.txt", &[("needle", "CHANGED")], &launch).unwrap();
        assert_eq!(
            scratch.read("mixed.txt"),
            b"\xef\xbb\xbffirst\r\nCHANGED\r\nlast\ncarriage\rreturn\n"
        );
        scratch.file("cr.txt", b"one\rtwo\rthree\r");
        let answer = change("cr.txt", &[("two", "TWO")], &launch).unwrap();
        assert!(answer.ends_with("line 1"), "{answer}");
        assert_eq!(scratch.read("cr.txt"), b"one\rTWO\rthree\r");
    }

    #[cfg(unix)]
    fn failed_save_preserves_original(operation: &str) {
        use std::os::unix::process::CommandExt;
        let scratch = Scratch::new(&format!("failed-{operation}"));
        let original = format!("{}needle\n", "original ".repeat(50));
        let path = scratch.file("target.txt", original.as_bytes());
        let mut worker = std::process::Command::new(std::env::current_exe().unwrap());
        worker
            .arg("--exact")
            .arg("files::tests::failed_save_worker")
            .arg("--nocapture")
            .env("FASTEXEC_TEST_SAVE_FAILURE_PATH", &path)
            .env("FASTEXEC_TEST_SAVE_FAILURE_OPERATION", operation);
        // The limit belongs only to a fresh test child, never to other tests or the MCP host.
        unsafe {
            worker.pre_exec(|| {
                libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
                let limit = libc::rlimit {
                    rlim_cur: 1024,
                    rlim_max: 1024,
                };
                if libc::setrlimit(libc::RLIMIT_FSIZE, &limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let output = worker.output().unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            scratch.read("target.txt"),
            original.as_bytes(),
            "{operation} reported an error but changed the original file"
        );
        assert_eq!(
            std::fs::read_dir(&scratch.0).unwrap().count(),
            1,
            "no staging file is left"
        );
    }

    #[cfg(unix)]
    #[test]
    fn failed_save_worker() {
        let Some(path) = std::env::var_os("FASTEXEC_TEST_SAVE_FAILURE_PATH") else {
            return;
        };
        let path = PathBuf::from(path);
        let launch = context(path.parent().unwrap(), path.parent().unwrap());
        let operation = std::env::var("FASTEXEC_TEST_SAVE_FAILURE_OPERATION").unwrap();
        let result = if operation == "write" {
            write_text(
                &WriteRequest {
                    path: path.display().to_string(),
                    content: "N".repeat(2048),
                },
                &launch,
            )
            .map(|(text, _)| text)
        } else {
            change(
                &path.display().to_string(),
                &[("needle", &"N".repeat(2048))],
                &launch,
            )
        };
        assert!(
            result.is_err(),
            "the OS file-size limit must reject the save"
        );
    }

    #[cfg(unix)]
    #[test]
    fn regression_failed_write_preserves_original() {
        failed_save_preserves_original("write");
    }

    #[cfg(unix)]
    #[test]
    fn regression_failed_edit_preserves_original() {
        failed_save_preserves_original("edit");
    }

    #[test]
    fn edit_replaces_exact_text_and_names_the_line() {
        let scratch = Scratch::new("exact");
        let launch = context(&scratch.0, &scratch.0);
        let shown = display_path(&scratch.0.join("a.txt"));
        scratch.file("a.txt", b"one\ntwo\nthree\n");
        assert_eq!(
            change("a.txt", &[("two", "TWO")], &launch).unwrap(),
            format!("Successfully replaced 1 block(s) in {shown}.\nedits[0]: line 2")
        );
        assert_eq!(scratch.read("a.txt"), b"one\nTWO\nthree\n");
    }

    #[test]
    fn edit_reports_original_lines_after_an_earlier_edit_adds_lines() {
        let scratch = Scratch::new("shift");
        let launch = context(&scratch.0, &scratch.0);
        let shown = display_path(&scratch.0.join("a.txt"));
        scratch.file("a.txt", b"1\n2\n3\n4\n");
        assert_eq!(
            change("a.txt", &[("2", "2\n2b\n2c"), ("4", "4x")], &launch).unwrap(),
            format!(
                "Successfully replaced 2 block(s) in {shown}.\nedits[0]: line 2\nedits[1]: line 4"
            )
        );
        assert_eq!(scratch.read("a.txt"), b"1\n2\n2b\n2c\n3\n4x\n");
    }

    #[test]
    fn edit_reports_original_lines_after_deletion_and_bom_restoration() {
        let scratch = Scratch::new("original-lines");
        let launch = context(&scratch.0, &scratch.0);
        scratch.file(
            "mixed.txt",
            "\u{feff}before\r\nmiddle\ron same\nlast\r\n".as_bytes(),
        );
        let (_, result) = edit_outcome(
            &EditRequest {
                path: "mixed.txt".into(),
                edits: vec![
                    Edit {
                        old_text: "last".into(),
                        new_text: "LAST".into(),
                        start_line: None,
                        end_line: None,
                    },
                    Edit {
                        old_text: "\u{feff}before\n".into(),
                        new_text: String::new(),
                        start_line: None,
                        end_line: None,
                    },
                ],
            },
            &launch,
        )
        .unwrap();
        assert_eq!(
            result["edits"],
            json!([{"line": 3, "fuzzy": false}, {"line": 1, "fuzzy": false}])
        );
        assert_eq!(
            scratch.read("mixed.txt"),
            "\u{feff}middle\ron same\nLAST\r\n".as_bytes()
        );
    }

    #[test]
    fn edit_refuses_input_it_cannot_apply() {
        let scratch = Scratch::new("refuse");
        let launch = context(&scratch.0, &scratch.0);
        let shown_a = display_path(&scratch.0.join("a.txt"));
        let shown_u = display_path(&scratch.0.join("u.txt"));
        let shown_b = display_path(&scratch.0.join("b.txt"));
        scratch.file("a.txt", b"x\nx\n");
        assert_eq!(
            change("a.txt", &[], &launch).unwrap_err(),
            "Edit tool input is invalid. edits must contain at least one replacement."
        );
        assert_eq!(
            change("a.txt", &[("", "y")], &launch).unwrap_err(),
            "edits[0].oldText must not be empty."
        );
        assert_eq!(
            change("a.txt", &[("zzz", "y")], &launch).unwrap_err(),
            format!(
                "Cannot edit {shown_a}; nothing was changed:\n- edits[0]: oldText was not found, even ignoring trailing whitespace, Unicode compatibility forms, curly quotes, dashes, and special spaces."
            )
        );
        assert_eq!(
            change("a.txt", &[("zzz", "y"), ("qqq", "z")], &launch).unwrap_err(),
            format!(
                "Cannot edit {shown_a}; nothing was changed:\n- edits[0]: oldText was not found, even ignoring trailing whitespace, Unicode compatibility forms, curly quotes, dashes, and special spaces.\n- edits[1]: oldText was not found, even ignoring trailing whitespace, Unicode compatibility forms, curly quotes, dashes, and special spaces."
            )
        );
        assert_eq!(
            change("a.txt", &[("x", "y")], &launch).unwrap_err(),
            format!(
                "Cannot edit {shown_a}; nothing was changed:\n- edits[0]: oldText occurs 2 times, starting at lines 1, 2; add surrounding text to make it unique."
            )
        );
        assert_eq!(
            change("a.txt", &[("x", "y"), ("x", "z")], &launch).unwrap_err(),
            format!(
                "Cannot edit {shown_a}; nothing was changed:\n- edits[0]: oldText occurs 2 times, starting at lines 1, 2; add surrounding text to make it unique.\n- edits[1]: oldText occurs 2 times, starting at lines 1, 2; add surrounding text to make it unique."
            )
        );
        scratch.file("u.txt", b"unique\n");
        assert_eq!(
            change("u.txt", &[("unique", "unique")], &launch).unwrap_err(),
            format!(
                "Cannot edit {shown_u}; nothing was changed: the replacement produced identical content."
            )
        );
        scratch.file("b.txt", b"abc\n");
        assert_eq!(
            change("b.txt", &[("ab", "X"), ("bc", "Y")], &launch).unwrap_err(),
            format!(
                "Cannot edit {shown_b}; nothing was changed:\n- edits[0] overlaps edits[1]; merge them into one edit or target disjoint regions.\n- edits[1] overlaps edits[0]; merge them into one edit or target disjoint regions."
            )
        );
        // Nothing above wrote anything.
        assert_eq!(scratch.read("a.txt"), b"x\nx\n");
        assert_eq!(scratch.read("b.txt"), b"abc\n");
    }

    #[test]
    fn edit_keeps_crlf_and_a_bom() {
        let scratch = Scratch::new("endings");
        let launch = context(&scratch.0, &scratch.0);
        scratch.file("crlf.txt", b"one\r\ntwo\r\n");
        change("crlf.txt", &[("two", "TWO")], &launch).unwrap();
        assert_eq!(scratch.read("crlf.txt"), b"one\r\nTWO\r\n");
        scratch.file("bom.txt", "\u{feff}one\ntwo\n".as_bytes());
        change("bom.txt", &[("two", "TWO")], &launch).unwrap();
        assert_eq!(scratch.read("bom.txt"), "\u{feff}one\nTWO\n".as_bytes());
    }

    #[test]
    fn edit_accepts_the_bom_returned_by_read() {
        let scratch = Scratch::new("read-edit-bom");
        let launch = context(&scratch.0, &scratch.0);
        scratch.file("bom.txt", "\u{feff}one\ntwo\n".as_bytes());
        let read_back = whole(&scratch.path("bom.txt"), &launch, false).unwrap();
        change(
            "bom.txt",
            &[(&read_back, &read_back.replace("one", "ONE"))],
            &launch,
        )
        .unwrap();
        assert_eq!(scratch.read("bom.txt"), "\u{feff}ONE\ntwo\n".as_bytes());
        change("bom.txt", &[("\u{feff}ONE", "one")], &launch).unwrap();
        assert_eq!(scratch.read("bom.txt"), "\u{feff}one\ntwo\n".as_bytes());
        assert!(change("bom.txt", &[("\u{feff}one", "one")], &launch).is_err());
        scratch.file("repeated.txt", "\u{feff}one\none\n".as_bytes());
        change("repeated.txt", &[("\u{feff}one", "\u{feff}ONE")], &launch).unwrap();
        assert_eq!(
            scratch.read("repeated.txt"),
            "\u{feff}ONE\none\n".as_bytes()
        );
        scratch.file("internal.txt", "\u{feff}one\nx\u{feff}two\n".as_bytes());
        change("internal.txt", &[("one", "ONE")], &launch).unwrap();
        assert_eq!(
            scratch.read("internal.txt"),
            "\u{feff}ONE\nx\u{feff}two\n".as_bytes()
        );
        scratch.file("only-bom.txt", "\u{feff}".as_bytes());
        change("only-bom.txt", &[("\u{feff}", "\u{feff}one")], &launch).unwrap();
        assert_eq!(scratch.read("only-bom.txt"), "\u{feff}one".as_bytes());
    }

    #[test]
    fn edit_matches_a_typographic_copy_and_keeps_the_bytes_around_it() {
        let scratch = Scratch::new("fuzzy");
        let launch = context(&scratch.0, &scratch.0);
        let shown = display_path(&scratch.0.join("a.txt"));
        scratch.file(
            "a.txt",
            "note: \u{201c}keep\u{201d} it\u{2019}s here   \nsecond line\n".as_bytes(),
        );
        assert_eq!(
            change("a.txt", &[("it's here", "it's fine")], &launch).unwrap(),
            format!("Successfully replaced 1 block(s) in {shown}.\nedits[0]: line 1 (fuzzy match)")
        );
        // Only the matched bytes change: the quotes before them and the trailing whitespace stay.
        assert_eq!(
            scratch.read("a.txt"),
            "note: \u{201c}keep\u{201d} it's fine   \nsecond line\n".as_bytes()
        );
    }

    #[test]
    fn edit_changes_only_the_bytes_a_fuzzy_match_names() {
        let scratch = Scratch::new("fuzzy-json");
        let launch = context(&scratch.0, &scratch.0);
        let shown = display_path(&scratch.0.join("case.json"));
        let original =
            "{\"quote\":\"She said \u{201c}hello\u{201d}\",\"author\":\"O\u{2019}Reilly\"}";
        scratch.file("case.json", original.as_bytes());
        assert_eq!(
            change("case.json", &[("O'Reilly", "Publisher")], &launch).unwrap(),
            format!("Successfully replaced 1 block(s) in {shown}.\nedits[0]: line 1 (fuzzy match)")
        );
        let edited = scratch.read("case.json");
        assert_eq!(
            edited,
            "{\"quote\":\"She said \u{201c}hello\u{201d}\",\"author\":\"Publisher\"}".as_bytes()
        );
        let parsed: serde_json::Value =
            serde_json::from_str(&String::from_utf8(edited).unwrap()).unwrap();
        assert_eq!(parsed["author"], "Publisher");
    }

    #[test]
    fn edit_keeps_an_exact_edits_line_while_another_edit_needs_normalizing() {
        let scratch = Scratch::new("fuzzy-mixed");
        let launch = context(&scratch.0, &scratch.0);
        let shown = display_path(&scratch.0.join("a.txt"));
        scratch.file(
            "a.txt",
            "label = \"\u{2460}\"; enabled = false\nname = \u{201c}old\u{201d}   \n".as_bytes(),
        );
        assert_eq!(
            change(
                "a.txt",
                &[("false", "true"), ("\"old\"", "\"new\"")],
                &launch
            )
            .unwrap(),
            format!(
                "Successfully replaced 2 block(s) in {shown}.\nedits[0]: line 1\nedits[1]: line 2 (fuzzy match)"
            )
        );
        assert_eq!(
            scratch.read("a.txt"),
            "label = \"\u{2460}\"; enabled = true\nname = \"new\"   \n".as_bytes()
        );
    }

    #[test]
    fn edit_refuses_a_needle_with_two_overlapping_starts() {
        let scratch = Scratch::new("overlap");
        let launch = context(&scratch.0, &scratch.0);
        let shown_a = display_path(&scratch.0.join("a.txt"));
        let shown_b = display_path(&scratch.0.join("b.txt"));
        let shown_c = display_path(&scratch.0.join("c.txt"));
        scratch.file("a.txt", b"banana\n");
        assert_eq!(
            change("a.txt", &[("ana", "X")], &launch).unwrap_err(),
            format!(
                "Cannot edit {shown_a}; nothing was changed:\n- edits[0]: oldText occurs 2 times, starting at lines 1, 1; add surrounding text to make it unique."
            )
        );
        assert_eq!(scratch.read("a.txt"), b"banana\n");
        scratch.file("b.txt", b"aaaa\n");
        assert_eq!(
            change("b.txt", &[("aaa", "X")], &launch).unwrap_err(),
            format!(
                "Cannot edit {shown_b}; nothing was changed:\n- edits[0]: oldText occurs 2 times, starting at lines 1, 1; add surrounding text to make it unique."
            )
        );
        assert_eq!(scratch.read("b.txt"), b"aaaa\n");
        scratch.file("c.txt", "\u{e9}\u{e9}\u{e9}\n".as_bytes());
        assert_eq!(
            change("c.txt", &[("\u{e9}\u{e9}", "X")], &launch).unwrap_err(),
            format!(
                "Cannot edit {shown_c}; nothing was changed:\n- edits[0]: oldText occurs 2 times, starting at lines 1, 1; add surrounding text to make it unique."
            )
        );
        assert_eq!(scratch.read("c.txt"), "\u{e9}\u{e9}\u{e9}\n".as_bytes());
    }

    #[test]
    fn edit_replaces_a_whitespace_only_target() {
        let scratch = Scratch::new("whitespace");
        let launch = context(&scratch.0, &scratch.0);
        let shown_tab = display_path(&scratch.0.join("tab.txt"));
        let shown_space = display_path(&scratch.0.join("space.txt"));
        let shown_indent = display_path(&scratch.0.join("indent.txt"));
        let shown_none = display_path(&scratch.0.join("none.txt"));
        scratch.file("tab.txt", b"if x:\n\tfoo()\n");
        assert_eq!(
            change("tab.txt", &[("\t", "    ")], &launch).unwrap(),
            format!("Successfully replaced 1 block(s) in {shown_tab}.\nedits[0]: line 2")
        );
        assert_eq!(scratch.read("tab.txt"), b"if x:\n    foo()\n");
        scratch.file("space.txt", b"foo bar\n");
        assert_eq!(
            change("space.txt", &[(" ", "_")], &launch).unwrap(),
            format!("Successfully replaced 1 block(s) in {shown_space}.\nedits[0]: line 1")
        );
        assert_eq!(scratch.read("space.txt"), b"foo_bar\n");
        scratch.file("indent.txt", b"if x:\n    foo()\n");
        assert_eq!(
            change("indent.txt", &[("    ", "\t")], &launch).unwrap(),
            format!("Successfully replaced 1 block(s) in {shown_indent}.\nedits[0]: line 2")
        );
        assert_eq!(scratch.read("indent.txt"), b"if x:\n\tfoo()\n");
        // A target the content does not hold writes nothing.
        scratch.file("none.txt", b"ab\n");
        assert_eq!(
            change("none.txt", &[(" ", "X")], &launch).unwrap_err(),
            format!(
                "Cannot edit {shown_none}; nothing was changed:\n- edits[0]: oldText was not found, even ignoring trailing whitespace, Unicode compatibility forms, curly quotes, dashes, and special spaces."
            )
        );
        assert_eq!(scratch.read("none.txt"), b"ab\n");
    }

    #[test]
    fn edit_refuses_a_fuzzy_match_that_ends_inside_a_unit() {
        let scratch = Scratch::new("unit-end");
        let launch = context(&scratch.0, &scratch.0);
        let shown = display_path(&scratch.0.join("a.txt"));
        // NFKC turns the ligature into two characters, so a match that ends between them names no
        // bytes of the content.
        scratch.file("a.txt", "\u{2019}\u{fb00}tail\n".as_bytes());
        assert_eq!(
            change("a.txt", &[("'f", "X")], &launch).unwrap_err(),
            format!(
                "Cannot edit {shown}; nothing was changed:\n- edits[0]: oldText was not found, even ignoring trailing whitespace, Unicode compatibility forms, curly quotes, dashes, and special spaces."
            )
        );
        assert_eq!(scratch.read("a.txt"), "\u{2019}\u{fb00}tail\n".as_bytes());
        // A match that covers whole units names them all and replaces them.
        assert_eq!(
            change("a.txt", &[("'ff", "'ff")], &launch).unwrap(),
            format!("Successfully replaced 1 block(s) in {shown}.\nedits[0]: line 1 (fuzzy match)")
        );
        assert_eq!(scratch.read("a.txt"), b"'fftail\n");
    }

    #[test]
    fn edit_matches_a_line_end_that_lost_trailing_whitespace() {
        let scratch = Scratch::new("trimmed-line-end");
        let launch = context(&scratch.0, &scratch.0);
        let shown = display_path(&scratch.0.join("a.txt"));
        // The match includes the physical newline; the later edit reports its original line.
        scratch.file("a.txt", "it\u{2019}s   \nnext\nmore\n".as_bytes());
        assert_eq!(
            change("a.txt", &[("it's\n", "X"), ("more", "MORE")], &launch).unwrap(),
            format!(
                "Successfully replaced 2 block(s) in {shown}.\nedits[0]: line 1 (fuzzy match)\nedits[1]: line 3"
            )
        );
        assert_eq!(scratch.read("a.txt"), b"Xnext\nMORE\n");
    }

    #[test]
    fn edit_matches_from_a_line_end_that_lost_trailing_whitespace() {
        let scratch = Scratch::new("trimmed-line-start");
        let launch = context(&scratch.0, &scratch.0);
        let shown_a = display_path(&scratch.0.join("a.txt"));
        let shown_b = display_path(&scratch.0.join("b.txt"));
        // The match starts on the newline the trim kept, so the removed spaces stay before it.
        scratch.file("a.txt", "a   \n\u{201c}b\u{201d}".as_bytes());
        assert_eq!(
            change("a.txt", &[("\n\"b\"", "X")], &launch).unwrap(),
            format!(
                "Successfully replaced 1 block(s) in {shown_a}.\nedits[0]: line 1 (fuzzy match)"
            )
        );
        assert_eq!(scratch.read("a.txt"), b"a   X");
        // A match that keeps the newline keeps it where it was.
        scratch.file("b.txt", "a   \n\u{201c}b\u{201d}\n".as_bytes());
        assert_eq!(
            change("b.txt", &[("\n\"b\"", "\nB")], &launch).unwrap(),
            format!(
                "Successfully replaced 1 block(s) in {shown_b}.\nedits[0]: line 1 (fuzzy match)"
            )
        );
        assert_eq!(scratch.read("b.txt"), b"a   \nB\n");
    }

    #[test]
    fn edit_refuses_a_run_longer_than_a_unit_holds_promptly() {
        let scratch = Scratch::new("long-run");
        let launch = context(&scratch.0, &scratch.0);
        let shown = display_path(&scratch.0.join("a.txt"));
        // A run of reordering non-starters longer than one unit may hold: the units no longer
        // reproduce the whole-file form, so the edit is refused instead of guessed.
        let content = format!("{}\nit\u{2019}s\n", "\u{0f73}".repeat(8192));
        scratch.file("a.txt", content.as_bytes());
        let started = Instant::now();
        assert_eq!(
            change("a.txt", &[("it's", "X")], &launch).unwrap_err(),
            format!(
                "Cannot edit {shown}; nothing was changed:\n- edits[0]: oldText was not found, even ignoring trailing whitespace, Unicode compatibility forms, curly quotes, dashes, and special spaces."
            )
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(scratch.read("a.txt"), content.as_bytes());
        // An exact edit of the same file still applies.
        assert_eq!(
            change("a.txt", &[("\nit\u{2019}s\n", "\nit's\n")], &launch).unwrap(),
            format!("Successfully replaced 1 block(s) in {shown}.\nedits[0]: line 1")
        );
    }

    #[test]
    fn edit_matches_across_a_short_combining_run() {
        let scratch = Scratch::new("short-run");
        let launch = context(&scratch.0, &scratch.0);
        let shown = display_path(&scratch.0.join("a.txt"));
        scratch.file("a.txt", "cafe\u{301} \u{201c}x\u{201d}\n".as_bytes());
        assert_eq!(
            change("a.txt", &[("caf\u{e9} \"x\"", "caf\u{e9} \"y\"")], &launch).unwrap(),
            format!("Successfully replaced 1 block(s) in {shown}.\nedits[0]: line 1 (fuzzy match)")
        );
        assert_eq!(scratch.read("a.txt"), "caf\u{e9} \"y\"\n".as_bytes());
    }

    /// The form the units reproduce, the way the map builds and checks it.
    fn unit_form(text: &str) -> String {
        let form: String = units_of(text)
            .into_iter()
            .map(|(at, end)| mapped(&nfkc(&text[at..end])))
            .collect();
        trimmed_form(&form).0
    }

    #[test]
    fn units_reproduce_the_fuzzy_form_of_their_text() {
        let samples = [
            "author = O\u{2019}Reilly\nnote = a\u{327}\u{301}\n",
            "title = \u{201c}hello\u{201d}\nword = \u{628}\u{650}\u{651}\u{64e}\n",
            "cafe\u{301} \u{201c}x\u{201d}\n",
            "e\u{301}\u{323}\u{327}\n",
            "\u{1112}\u{1161}\u{11ab}\u{1100}\u{1173}",
            "\u{fb01} \u{2460} \u{216b} \u{ff76}\u{ff9e}",
            "x\u{301}   \ny\t\n   \n",
            "a\u{fe00}b\u{200d}c\u{1f600}",
        ];
        for sample in samples {
            assert_eq!(
                unit_form(sample),
                normalize_for_fuzzy_match(sample),
                "{sample:?}"
            );
        }
    }

    #[test]
    fn edit_matches_a_mark_that_reorders_inside_its_unit() {
        let scratch = Scratch::new("reorder");
        let launch = context(&scratch.0, &scratch.0);
        let shown_a = display_path(&scratch.0.join("a.txt"));
        let shown_b = display_path(&scratch.0.join("b.txt"));
        let shown_c = display_path(&scratch.0.join("c.txt"));
        let shown_d = display_path(&scratch.0.join("d.txt"));
        // A cedilla between a base and an acute is below the acute's combining class, so the whole
        // run is one unit and an edit elsewhere in the file still matches.
        scratch.file(
            "a.txt",
            "author = O\u{2019}Reilly\nnote = a\u{327}\u{301}\n".as_bytes(),
        );
        assert_eq!(
            change("a.txt", &[("O'Reilly", "Publisher")], &launch).unwrap(),
            format!(
                "Successfully replaced 1 block(s) in {shown_a}.\nedits[0]: line 1 (fuzzy match)"
            )
        );
        assert_eq!(
            scratch.read("a.txt"),
            "author = Publisher\nnote = a\u{327}\u{301}\n".as_bytes()
        );
        // The run itself is one unit, so its own normalized form names it.
        scratch.file("b.txt", "note = a\u{327}\u{301}\n".as_bytes());
        assert_eq!(
            change("b.txt", &[("\u{e1}\u{327}", "X")], &launch).unwrap(),
            format!(
                "Successfully replaced 1 block(s) in {shown_b}.\nedits[0]: line 1 (fuzzy match)"
            )
        );
        assert_eq!(scratch.read("b.txt"), b"note = X\n");
        // The same for marks that reorder without composing, as in vocalized Arabic.
        scratch.file(
            "c.txt",
            "title = \u{201c}hello\u{201d}\nword = \u{628}\u{650}\u{651}\u{64e}\n".as_bytes(),
        );
        assert_eq!(
            change("c.txt", &[("\"hello\"", "\"world\"")], &launch).unwrap(),
            format!(
                "Successfully replaced 1 block(s) in {shown_c}.\nedits[0]: line 1 (fuzzy match)"
            )
        );
        assert_eq!(
            scratch.read("c.txt"),
            "title = \"world\"\nword = \u{628}\u{650}\u{651}\u{64e}\n".as_bytes()
        );
        scratch.file("d.txt", "word = \u{628}\u{650}\u{651}\u{64e}\n".as_bytes());
        assert_eq!(
            change("d.txt", &[("\u{628}\u{64e}\u{650}\u{651}", "X")], &launch).unwrap(),
            format!(
                "Successfully replaced 1 block(s) in {shown_d}.\nedits[0]: line 1 (fuzzy match)"
            )
        );
        assert_eq!(scratch.read("d.txt"), b"word = X\n");
    }

    #[test]
    fn edit_counts_overlapping_starts_in_one_pass() {
        let scratch = Scratch::new("overlap-cost");
        let launch = context(&scratch.0, &scratch.0);
        let shown = display_path(&scratch.0.join("a.txt"));
        let content = "a".repeat(131_072);
        scratch.file("a.txt", content.as_bytes());
        let needle = "a".repeat(65_536);
        let started = Instant::now();
        assert_eq!(
            change("a.txt", &[(needle.as_str(), "X")], &launch).unwrap_err(),
            format!(
                "Cannot edit {shown}; nothing was changed:\n- edits[0]: oldText occurs 65537 times, starting at lines 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 (and 65517 more); add surrounding text to make it unique."
            )
        );
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(scratch.read("a.txt"), content.as_bytes());
    }

    #[test]
    fn edit_refuses_a_file_that_is_not_utf8() {
        let scratch = Scratch::new("not-utf8");
        let launch = context(&scratch.0, &scratch.0);
        let shown = display_path(&scratch.0.join("a.txt"));
        scratch.file("a.txt", b"one\n\xfftwo\n");
        assert_eq!(
            change("a.txt", &[("two", "TWO")], &launch).unwrap_err(),
            format!(
                "Could not edit file: {shown}. the file is not valid UTF-8; edit it with exec."
            )
        );
    }

    #[test]
    fn the_mutation_lock_keeps_every_edit_of_one_file() {
        let scratch = Scratch::new("mutex");
        let launch = context(&scratch.0, &scratch.0);
        const WORKERS: usize = 4;
        const ROUNDS: usize = 25;
        let start: String = (0..WORKERS)
            .map(|worker| format!("worker-{worker}=0\n"))
            .collect();
        scratch.file("counters.txt", start.as_bytes());
        let path = scratch.path("counters.txt");
        std::thread::scope(|scope| {
            for worker in 0..WORKERS {
                let path = path.clone();
                let launch = &launch;
                scope.spawn(move || {
                    for round in 0..ROUNDS {
                        let old = format!("worker-{worker}={round}");
                        let request = EditRequest {
                            path: path.clone(),
                            edits: vec![Edit {
                                old_text: old.clone(),
                                new_text: format!("worker-{worker}={}", round + 1),
                                start_line: None,
                                end_line: None,
                            }],
                        };
                        edit_text(&request, launch)
                            .unwrap_or_else(|error| panic!("{old} was lost: {error}"));
                    }
                });
            }
        });
        let mut expected = String::new();
        for worker in 0..WORKERS {
            expected.push_str(&format!("worker-{worker}={ROUNDS}\n"));
        }
        assert_eq!(
            String::from_utf8(scratch.read("counters.txt")).unwrap(),
            expected
        );
    }
    #[test]
    fn read_restores_the_final_newline_after_an_empty_last_line() {
        let scratch = Scratch::new("final-empty");
        let launch = context(&scratch.0, &scratch.0);
        // 2000 selected lines whose last one is empty, and the newline after it: the selected
        // content ends in two newlines, and the second one is the empty element.
        let text = format!("{}\n", "x\n".repeat(1999));
        scratch.file("empty-last.txt", text.as_bytes());
        let path = scratch.path("empty-last.txt");
        assert_eq!(whole(&path, &launch, false).unwrap(), text);
    }

    #[test]
    fn read_counts_the_final_separator_against_the_byte_budget() {
        let scratch = Scratch::new("separator");
        let launch = context(&scratch.0, &scratch.0);
        // 1999 lines of 25 bytes (49975 bytes) plus a 2000th line of 1226 bytes (1225 'z' + LF)
        // total 51201 bytes: line 2000's terminator puts it 1 byte over the 50KB budget.
        let mut text = String::new();
        for _ in 0..1999 {
            text.push_str(&"y".repeat(24));
            text.push('\n');
        }
        let shown_prefix = text.clone();
        text.push_str(&"z".repeat(1225));
        text.push('\n');
        scratch.file("at-cap.txt", text.as_bytes());
        let path = scratch.path("at-cap.txt");
        assert_eq!(
            whole(&path, &launch, false).unwrap(),
            format!(
                "{shown_prefix}\n[Showing lines 1-1999 of 2000 (50.0KB limit). Use offset=2000 to continue.]"
            )
        );
    }
    #[test]
    fn read_keeps_an_empty_line_when_a_carry_holds_a_later_character() {
        let scratch = Scratch::new("carry-skip");
        let launch = context(&scratch.0, &scratch.0);
        // The first byte of the last line's `é` is the final byte of the first 8192-byte read,
        // so the decoder holds it while the line before it is still empty: that empty line is
        // real content and must remain available after the 2000-line budget is reached.
        let text = format!("{}\n{}{}tail", "x\n".repeat(2000), "a".repeat(4190), 'é');
        scratch.file("carry.txt", text.as_bytes());
        let path = scratch.path("carry.txt");
        assert_eq!(
            window(&path, None, Some(2001), &launch).unwrap(),
            format!(
                "{}\n[Showing lines 1-2000 of 2002. Use offset=2001 to continue.]",
                "x\n".repeat(2000)
            )
        );
    }
    /// A small PNG built in memory, so no binary file is committed.
    fn png_bytes(width: u32, height: u32) -> Vec<u8> {
        let image = image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(
            width,
            height,
            image::Rgb([7, 8, 9]),
        ));
        let mut bytes = Vec::new();
        image
            .write_to(
                &mut std::io::Cursor::new(&mut bytes),
                image::ImageFormat::Png,
            )
            .expect("the fixture encodes");
        bytes
    }

    #[test]
    fn read_answers_a_png_with_its_bytes_and_its_mime_type() {
        let scratch = Scratch::new("image");
        let launch = context(&scratch.0, &scratch.0);
        let png = png_bytes(8, 6);
        scratch.file("shot.png", &png);
        let path = scratch.path("shot.png");
        let outcome = read_text(
            &ReadRequest {
                path,
                offset: Some(5),
                limit: Some(10),
            },
            &launch,
            false,
            ReadLimits::DIRECT,
        )
        .unwrap();
        assert_eq!(
            outcome.text,
            format!("Read image file [image/png] ({} B)", png.len())
        );
        assert_eq!(outcome.structured["bytes"], png.len() as u64);
        assert_eq!(outcome.structured["attached"], true);
        assert!(outcome.structured.get("width").is_none());
        let block = outcome.attachment.expect("the image travels with the text");
        let img = block.as_image().expect("image block");
        assert_eq!(img.mime_type, "image/png");
        assert_eq!(
            img.data,
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &png)
        );
    }

    #[test]
    fn read_attaches_animated_png_by_default_and_passes_through_under_compress_mode() {
        let scratch = Scratch::new("animated");
        let png = png_bytes(4, 4);
        let mut animated = png[..33].to_vec();
        animated.extend_from_slice(&[0, 0, 0, 8]);
        animated.extend_from_slice(b"acTL");
        animated.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 1]);
        animated.extend_from_slice(&[0, 0, 0, 0]);
        animated.extend_from_slice(&png[33..]);
        scratch.file("moving.png", &animated);
        let path = scratch.path("moving.png");

        for compress in ["false", "true"] {
            let launch = LaunchContext::new(
                scratch.0.clone(),
                vec![
                    ("HOME".into(), scratch.0.as_os_str().to_owned()),
                    ("FASTEXEC_IMAGE_COMPRESS".into(), compress.into()),
                ],
            );
            let outcome = read_text(
                &ReadRequest {
                    path: path.clone(),
                    offset: None,
                    limit: None,
                },
                &launch,
                false,
                ReadLimits::DIRECT,
            )
            .unwrap();
            assert_eq!(outcome.structured["attached"], true);
            assert_eq!(outcome.structured["bytes"], animated.len() as u64);
            let img = outcome.attachment.unwrap();
            assert_eq!(
                img.as_image().unwrap().data,
                base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &animated)
            );
        }
    }

    #[test]
    fn media_reads_count_from_byte_zero_for_both_prefix_and_rewind_and_handle_growth_and_io_errors()
    {
        // Test exact N-1, N pass and N+1 reject from byte 0 on both Prefix and Rewind paths.
        let total = 10_000usize;
        let payload: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();
        let pool = BytePool::new(1 << 20);

        for use_prefix in [true, false] {
            for (max_cap, expect_ok) in [
                ((total + 1) as u64, true),
                (total as u64, true),
                ((total - 1) as u64, false),
            ] {
                let mut cursor = std::io::Cursor::new(payload.clone());
                let mut prefix = vec![0u8; SNIFF];
                let sniffed = cursor.read(&mut prefix).unwrap();
                assert_eq!(sniffed, SNIFF);
                let start = if use_prefix {
                    MediaStart::Prefix(&prefix[..sniffed])
                } else {
                    MediaStart::Rewind
                };
                // Pass initial_len = SNIFF as u64 when testing N+1 growth detection so the streaming
                // loop exercises the exact max + 1 - sniffed / max + 1 byte cap from byte 0.
                let initial_len = if expect_ok {
                    payload.len() as u64
                } else {
                    SNIFF as u64
                };
                match read_media_from_zero(
                    &mut cursor,
                    initial_len,
                    start,
                    max_cap,
                    &pool,
                    Some(BASE_MEDIA_FRAME_OVERHEAD),
                    None,
                )
                .unwrap()
                {
                    MediaBytes::Loaded { bytes, lease } => {
                        assert!(expect_ok, "expected rejection for max_cap={max_cap}");
                        assert_eq!(bytes, payload);
                        drop(bytes);
                        drop(lease);
                    }
                    MediaBytes::Oversized { exact_source_bytes } => {
                        assert!(!expect_ok, "expected pass for max_cap={max_cap}");
                        assert_eq!(
                            exact_source_bytes, None,
                            "concurrent growth past limit must not claim sample length as exact"
                        );
                    }
                }
                assert_eq!(pool.used(), 0);
            }
        }

        // True I/O failure mid-stream returns MediaReadError::Io, never Oversized/unattached.
        struct MidFailReader {
            emitted: usize,
        }
        impl Read for MidFailReader {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.emitted == 0 {
                    buf[0] = 1;
                    self.emitted = 1;
                    Ok(1)
                } else {
                    Err(io::Error::other("disk read fault"))
                }
            }
        }
        impl Seek for MidFailReader {
            fn seek(&mut self, _: SeekFrom) -> io::Result<u64> {
                Ok(0)
            }
        }
        let mut failing = MidFailReader { emitted: 0 };
        assert!(matches!(
            read_media_from_zero(
                &mut failing,
                100,
                MediaStart::Rewind,
                1000,
                &pool,
                Some(BASE_MEDIA_FRAME_OVERHEAD),
                None,
            ),
            Err(MediaReadError::Io(_))
        ));
        assert_eq!(pool.used(), 0);

        // Cancellation mid-stream aborts at the 64 KiB chunk boundary and releases the lease.
        let token = tokio_util::sync::CancellationToken::new();
        struct CancelAfterFirstChunkReader {
            read_calls: usize,
            token: tokio_util::sync::CancellationToken,
        }
        impl Read for CancelAfterFirstChunkReader {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                self.read_calls += 1;
                buf.fill(0x55);
                if self.read_calls == 1 {
                    self.token.cancel();
                }
                Ok(buf.len())
            }
        }
        impl Seek for CancelAfterFirstChunkReader {
            fn seek(&mut self, _: SeekFrom) -> io::Result<u64> {
                Ok(0)
            }
        }
        let mut cancellable = CancelAfterFirstChunkReader {
            read_calls: 0,
            token: token.clone(),
        };
        assert!(matches!(
            read_media_from_zero(
                &mut cancellable,
                256 * 1024,
                MediaStart::Rewind,
                1024 * 1024,
                &pool,
                Some(BASE_MEDIA_FRAME_OVERHEAD),
                Some(&token),
            ),
            Err(MediaReadError::Cancelled)
        ));
        assert_eq!(
            cancellable.read_calls, 1,
            "must stop at next 64 KiB chunk boundary"
        );
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn multimodal_read_enforces_zero_guards_nested_descriptors_and_output_modes() {
        let scratch = Scratch::new("multimodal-modes");
        let pdf = b"%PDF-1.7\n%\xE2\xE3\xCF\xD3\n1 0 obj\n<< /Type /Catalog >>\nendobj\n";
        scratch.file("doc.pdf", pdf);
        let mut wav = b"RIFF\x24\x00\x00\x00WAVEfmt \x10\x00\x00\x00".to_vec();
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&44_100u32.to_le_bytes());
        wav.extend_from_slice(&[0u8; 8]);
        scratch.file("tone.wav", &wav);
        let mut mp4 = Vec::new();
        mp4.extend_from_slice(&20u32.to_be_bytes());
        mp4.extend_from_slice(b"ftypisom\0\0\0\0mp42");
        scratch.file("clip.mp4", &mp4);

        let launch = context(&scratch.0, &scratch.0);

        // offset: 0 and limit: 0 fail on media files just like text files.
        assert!(
            read_text(
                &ReadRequest {
                    path: scratch.path("doc.pdf"),
                    offset: Some(0),
                    limit: None
                },
                &launch,
                false,
                ReadLimits::DIRECT
            )
            .is_err()
        );
        assert!(
            read_text(
                &ReadRequest {
                    path: scratch.path("doc.pdf"),
                    offset: None,
                    limit: Some(0)
                },
                &launch,
                false,
                ReadLimits::DIRECT
            )
            .is_err()
        );

        // Valid offset/limit are silently ignored on PDF, WAV, and MP4, and all output modes omit structured["text"].
        for (file, expected_kind, expected_mime) in [
            ("doc.pdf", "document", "application/pdf"),
            ("tone.wav", "audio", "audio/wav"),
            ("clip.mp4", "video", "video/mp4"),
        ] {
            for mode in [OutputMode::Text, OutputMode::Both, OutputMode::Structured] {
                for sc in [true, false] {
                    let out = read_text(
                        &ReadRequest {
                            path: scratch.path(file),
                            offset: Some(-5),
                            limit: Some(2),
                        },
                        &launch,
                        true,
                        ReadLimits::DIRECT,
                    )
                    .unwrap();
                    let tracked = out.into_tracked(
                        Render {
                            output_mode: mode,
                            structured_content: sc,
                        }
                        .effective(),
                    );
                    assert_eq!(tracked.result.content.len(), 2);
                    let head = tracked.result.content[0].as_text().unwrap();
                    assert!(
                        head.text
                            .starts_with(&format!("Read {expected_kind} file [{expected_mime}]")),
                        "{}",
                        head.text
                    );
                    if sc {
                        let meta = tracked.result.structured_content.as_ref().unwrap();
                        assert_eq!(meta["kind"], expected_kind);
                        assert_eq!(meta["mimeType"], expected_mime);
                        assert_eq!(meta["attached"], true);
                        assert!(meta.get("text").is_none());
                    } else {
                        assert!(tracked.result.structured_content.is_none());
                    }
                }
            }

            // Nested codemode read returns attachable descriptor without base64 attachment.
            let nested = read_text(
                &ReadRequest {
                    path: scratch.path(file),
                    offset: None,
                    limit: None,
                },
                &launch,
                false,
                ReadLimits::NESTED,
            )
            .unwrap();
            assert!(nested.attachment.is_none());
            assert_eq!(nested.structured["kind"], expected_kind);
            assert_eq!(nested.structured["mimeType"], expected_mime);
            assert_eq!(nested.structured["attachable"], true);
            assert!(nested.structured["bytes"].as_u64().is_some());
        }
    }

    #[test]
    fn read_real_line_table_pagination_and_empty_offsets() {
        let scratch = Scratch::new("p1-2-table");
        let launch = context(&scratch.0, &scratch.0);
        for (idx, (sample, expected_total)) in [
            ("", 0_u64),
            ("a", 1),
            ("a\n", 1),
            ("a\nb", 2),
            ("a\n\n", 2),
            ("a\r\nb\r\n", 2),
            ("a\rb\r", 1),
        ]
        .into_iter()
        .enumerate()
        {
            let name = format!("case-{idx}.txt");
            scratch.file(&name, sample.as_bytes());
            let out = read_text(
                &ReadRequest {
                    path: name.clone(),
                    offset: Some(1),
                    limit: None,
                },
                &launch,
                false,
                ReadLimits::DIRECT,
            )
            .unwrap();
            assert_eq!(out.structured["totalLines"], expected_total, "{sample:?}");
            assert_eq!(out.body.as_deref(), Some(sample), "{sample:?}");
            assert_eq!(out.structured["truncated"], false, "{sample:?}");
            if expected_total == 0 {
                assert_eq!(out.structured["shownLines"], 0);
                assert!(out.structured["endLine"].is_null());
            } else {
                let mut joined = String::new();
                let mut next = Some(1_i64);
                let mut pages = 0_u64;
                while let Some(off) = next {
                    let page = read_text(
                        &ReadRequest {
                            path: name.clone(),
                            offset: Some(off),
                            limit: Some(1),
                        },
                        &launch,
                        false,
                        ReadLimits::DIRECT,
                    )
                    .unwrap();
                    joined.push_str(page.body.as_deref().unwrap());
                    next = page.structured["nextOffset"].as_i64();
                    pages += 1;
                }
                assert_eq!(pages, expected_total, "{sample:?}");
                assert_eq!(joined, sample, "{sample:?}");
            }
        }
        scratch.file("empty.txt", b"");
        assert!(window("empty.txt", Some(0), None, &launch).is_err());
        assert_eq!(window("empty.txt", Some(1), None, &launch).unwrap(), "");
        assert_eq!(
            window("empty.txt", Some(2), None, &launch).unwrap_err(),
            "Offset 2 is beyond end of file (0 lines total)"
        );
    }

    #[test]
    fn edit_collects_all_failures_containment_overlaps_nfkc_cr_lines_and_caps() {
        let scratch = Scratch::new("p1-3-failures");
        let launch = context(&scratch.0, &scratch.0);
        scratch.file("mix.txt", b"dup\ndup\nabcdef\n");
        let err = edit_outcome(
            &EditRequest {
                path: "mix.txt".into(),
                edits: vec![
                    Edit {
                        old_text: "dup".into(),
                        new_text: "D".into(),
                        start_line: None,
                        end_line: None,
                    },
                    Edit {
                        old_text: "missing".into(),
                        new_text: "M".into(),
                        start_line: None,
                        end_line: None,
                    },
                    Edit {
                        old_text: "abcdef".into(),
                        new_text: "X".into(),
                        start_line: None,
                        end_line: None,
                    },
                    Edit {
                        old_text: "bc".into(),
                        new_text: "Y".into(),
                        start_line: None,
                        end_line: None,
                    },
                    Edit {
                        old_text: "ef".into(),
                        new_text: "Z".into(),
                        start_line: None,
                        end_line: None,
                    },
                ],
            },
            &launch,
        )
        .unwrap_err()
        .1
        .unwrap();
        assert_eq!(
            err["failures"],
            json!([
                {"index": 0, "problem": "ambiguous", "lines": [1, 2]},
                {"index": 1, "problem": "not_found"},
                {"index": 2, "problem": "overlap", "with": [3, 4]},
                {"index": 3, "problem": "overlap", "with": [2]},
                {"index": 4, "problem": "overlap", "with": [2]},
            ])
        );
        assert_eq!(scratch.read("mix.txt"), b"dup\ndup\nabcdef\n");

        scratch.file("nfkc.txt", "a\rf\n\u{fb00}\rb\n".as_bytes());
        let nfkc_err = edit_outcome(
            &EditRequest {
                path: "nfkc.txt".into(),
                edits: vec![Edit {
                    old_text: "f".into(),
                    new_text: "F".into(),
                    start_line: None,
                    end_line: None,
                }],
            },
            &launch,
        )
        .unwrap_err()
        .1
        .unwrap();
        assert_eq!(
            nfkc_err["failures"],
            json!([{"index": 0, "problem": "ambiguous", "lines": [1, 2, 2]}])
        );

        let alphabet: String = (0..25).map(|i| format!("tok{i:02} ")).collect();
        scratch.file("many-ov.txt", alphabet.as_bytes());
        let mut ov_edits = vec![Edit {
            old_text: alphabet.clone(),
            new_text: "ALL".into(),
            start_line: None,
            end_line: None,
        }];
        for i in 0..22 {
            ov_edits.push(Edit {
                old_text: format!("tok{i:02}"),
                new_text: "T".into(),
                start_line: None,
                end_line: None,
            });
        }
        let ov_err = edit_outcome(
            &EditRequest {
                path: "many-ov.txt".into(),
                edits: ov_edits,
            },
            &launch,
        )
        .unwrap_err()
        .1
        .unwrap();
        assert_eq!(ov_err["failures"][0]["with"].as_array().unwrap().len(), 20);
        assert_eq!(ov_err["failures"][0]["more"], 2);
    }

    #[test]
    fn edit_counts_exact_matches_absorbed_by_normalization_and_partial_fuzzy_copies() {
        let request = EditRequest {
            path: "sources.txt".into(),
            edits: vec![Edit {
                old_text: "e".into(),
                new_text: "E".into(),
                start_line: None,
                end_line: None,
            }],
        };
        for (sample, lines) in [
            ("e\u{301}\nｅ\nｅ\n", vec![1, 2, 3]),
            ("e\u{301} ｅ ｅ", vec![1, 1, 1]),
            ("ｅ\ne\u{301}\nｅ\n", vec![1, 2, 3]),
            ("e\u{301}\n⒠", vec![1, 2]), // A fuzzy start inside the expansion "(e)".
        ] {
            let failure = apply_edits(sample, &request, "sources.txt")
                .unwrap_err()
                .1
                .unwrap();
            assert_eq!(failure["failures"][0]["lines"], json!(lines), "{sample:?}");
        }
        let same_line = format!("e\u{301} ｅ Z{}\u{0323}\n", "\u{301}".repeat(36));
        let failure = apply_edits(&same_line, &request, "sources.txt")
            .unwrap_err()
            .1
            .unwrap();
        assert_eq!(failure["failures"][0]["lines"], json!([1, 1]));
        let mark_request = EditRequest {
            path: "marks.txt".into(),
            edits: vec![Edit {
                old_text: "\u{301}".into(),
                new_text: "X".into(),
                start_line: None,
                end_line: None,
            }],
        };
        let failure = apply_edits("a\u{301}\u{341}", &mark_request, "marks.txt")
            .unwrap_err()
            .1
            .unwrap();
        assert_eq!(failure["failures"][0]["lines"], json!([1, 1]));
        // A mark that moved past a different mark still comes from the exact span.
        assert_eq!(
            apply_edits("q\u{301}\u{327}", &mark_request, "marks.txt")
                .unwrap()
                .0,
            "qX\u{327}"
        );
        let trimmed_request = EditRequest {
            path: "trim.txt".into(),
            edits: vec![Edit {
                old_text: "é ".into(),
                new_text: "E".into(),
                start_line: None,
                end_line: None,
            }],
        };
        assert_eq!(
            apply_edits("é \u{301}!", &trimmed_request, "trim.txt")
                .unwrap()
                .0,
            "E\u{301}!"
        );
        let sample = format!("e\u{301}\n{}", "ｅ\n".repeat(23));
        let failure = apply_edits(&sample, &request, "sources.txt")
            .unwrap_err()
            .1
            .unwrap();
        assert_eq!(
            failure["failures"][0]["lines"],
            json!((1..=20).collect::<Vec<_>>())
        );
        assert_eq!(failure["failures"][0]["more"], 4);
    }

    #[test]
    fn windows_display_and_git_bash_drive_helpers_format_every_form() {
        let volume = r"\\?\Volume{01234567-89ab-cdef-0123-456789abcdef}\dir\file";
        let shown = format_windows_display(volume);
        assert_eq!(git_bash_drive_path(&shown).as_deref(), Some(volume));
        #[cfg(windows)]
        {
            let launch = context(Path::new(r"C:\cwd"), Path::new(r"C:\home"));
            assert_eq!(
                resolve(&shown, &launch).unwrap().as_os_str(),
                resolve(volume, &launch).unwrap().as_os_str()
            );
        }
        assert_eq!(git_bash_drive_path("/c"), Some("C:/".into()));
        assert_eq!(
            git_bash_drive_path("/d/work/a.txt"),
            Some("D:/work/a.txt".into())
        );
        assert_eq!(git_bash_drive_path("/tmp/x"), None);
        assert_eq!(
            git_bash_drive_path("//srv/share/dir/a.txt"),
            Some(r"\\srv\share\dir\a.txt".into())
        );
        assert_eq!(git_bash_drive_path("///tmp"), None);
        assert_eq!(
            format_windows_display(r"c:\Users\Eden\file.txt"),
            "C:/Users/Eden/file.txt"
        );
        assert_eq!(
            format_windows_display(r"\\?\c:\Users\file.txt"),
            "C:/Users/file.txt"
        );
        assert_eq!(
            format_windows_display(r"\\server\share\dir\f"),
            "//server/share/dir/f"
        );
        assert_eq!(
            format_windows_display(r"\\?\UNC\server\share\dir\f"),
            "//server/share/dir/f"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn write_accepts_forty_symlinks_and_refuses_the_next() {
        use std::os::unix::fs::symlink;
        let scratch = Scratch::new("symlink-limit");
        let launch = context(&scratch.0, &scratch.0);
        scratch.file("target", b"before\n");
        for index in 1..=41 {
            let target = if index == 1 {
                "target".to_owned()
            } else {
                format!("link{}", index - 1)
            };
            symlink(target, scratch.0.join(format!("link{index}"))).unwrap();
        }
        assert_eq!(whole("link40", &launch, false).unwrap(), "before\n");
        write_text(
            &WriteRequest {
                path: "link40".into(),
                content: "after\n".into(),
            },
            &launch,
        )
        .unwrap();
        symlink(".", scratch.0.join("alias")).unwrap();
        for path in ["link41", "alias/link40"] {
            let native = std::fs::metadata(scratch.0.join(path)).unwrap_err();
            assert_eq!(native.raw_os_error(), Some(libc::ELOOP));
            let error = write_text(
                &WriteRequest {
                    path: path.into(),
                    content: "wrong\n".into(),
                },
                &launch,
            )
            .unwrap_err();
            assert!(error.contains(&native.to_string()), "{error}");
        }
        assert_eq!(scratch.read("target"), b"after\n");
        assert!(
            std::fs::symlink_metadata(scratch.0.join("link40"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }

    #[cfg(unix)]
    #[test]
    fn resolve_keeps_dotdot_for_kernel_symlink_traversal() {
        use std::os::unix::fs::symlink;
        let scratch = Scratch::new("symlink-dotdot");
        std::fs::create_dir_all(scratch.0.join("base")).unwrap();
        std::fs::create_dir_all(scratch.0.join("target/sub")).unwrap();
        std::fs::write(scratch.0.join("base/value"), b"wrong-base\n").unwrap();
        std::fs::write(scratch.0.join("target/value"), b"right-target\n").unwrap();
        symlink("../target/sub", scratch.0.join("base/link")).unwrap();
        let launch = context(&scratch.0, &scratch.0);
        let resolved = resolve("./base//link/../value", &launch).unwrap();
        assert!(
            resolved.to_string_lossy().ends_with("base/link/../value"),
            "{resolved:?}"
        );
        assert_eq!(
            whole("./base/link/../value", &launch, false).unwrap(),
            "right-target\n"
        );
        change(
            "./base/link/../value",
            &[("right-target", "updated-target")],
            &launch,
        )
        .unwrap();
        assert_eq!(
            std::fs::read(scratch.0.join("target/value")).unwrap(),
            b"updated-target\n"
        );
        assert_eq!(
            std::fs::read(scratch.0.join("base/value")).unwrap(),
            b"wrong-base\n"
        );
    }

    #[tokio::test]
    async fn cancelled_nested_file_read_retains_io_permit_until_blocking_worker_finishes() {
        let scratch = Scratch::new("nested-read-permit");
        let path = scratch.0.join("sample.txt");
        std::fs::write(&path, "alpha\nbeta\n").unwrap();
        let launch = Arc::new(context(&scratch.0, &scratch.0));
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let render = Render {
            structured_content: true,
            output_mode: OutputMode::Text,
        };

        // Hold MUTATION on a dedicated OS thread while write_nested enters spawn_blocking, then
        // abort the awaiting future and verify the permit stays taken until MUTATION unlocks.
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = std::thread::spawn(move || {
            let _guard = MUTATION.lock().unwrap();
            locked_tx.send(()).unwrap();
            let _ = release_rx.recv();
        });
        locked_rx.recv().unwrap();

        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let write_launch = Arc::clone(&launch);
        let write_path = path.display().to_string();
        let mut fut = Box::pin(write_nested(
            WriteRequest {
                path: write_path,
                content: "updated\n".into(),
            },
            write_launch,
            render,
            permit,
        ));
        let polled_once =
            std::future::poll_fn(|cx| std::task::Poll::Ready(fut.as_mut().poll(cx).is_pending()))
                .await;
        assert!(
            polled_once,
            "write_nested must be pending while MUTATION is held"
        );
        drop(fut);
        assert_eq!(
            semaphore.available_permits(),
            0,
            "dropping the awaiting future must not release the permit before blocking I/O finishes"
        );
        release_tx.send(()).unwrap();
        blocker.join().unwrap();
        let reacquired =
            tokio::time::timeout(std::time::Duration::from_secs(2), semaphore.acquire_owned())
                .await
                .expect("permit released once blocking worker finishes")
                .unwrap();
        drop(reacquired);
    }

    #[test]
    fn read_supports_negative_offsets_across_sizes_empty_files_and_line_endings() {
        let scratch = Scratch::new("negative-offset");
        let launch = context(&scratch.0, &scratch.0);

        let thousand_path = scratch.0.join("thousand.txt");
        let mut thousand = String::new();
        for i in 1..=1000 {
            thousand.push_str(&format!("line-{i}\n"));
        }
        std::fs::write(&thousand_path, &thousand).unwrap();
        for limits in [ReadLimits::DIRECT, ReadLimits::NESTED] {
            let tail = read_text(
                &ReadRequest {
                    path: "thousand.txt".into(),
                    offset: Some(-50),
                    limit: None,
                },
                &launch,
                false,
                limits,
            )
            .unwrap();
            assert_eq!(tail.structured["startLine"], 951);
            assert_eq!(tail.structured["endLine"], 1000);
            assert_eq!(tail.structured["shownLines"], 50);
            assert_eq!(tail.structured["nextOffset"], Value::Null);
            assert!(tail.body.as_deref().unwrap().starts_with("line-951\n"));
            assert!(tail.body.as_deref().unwrap().ends_with("line-1000\n"));
        }

        let big_path = scratch.0.join("hundred_k.txt");
        let mut big = String::with_capacity(900_000);
        for i in 1..=100_000 {
            big.push_str(&format!("r{i}\n"));
        }
        std::fs::write(&big_path, &big).unwrap();
        let slice = read_text(
            &ReadRequest {
                path: "hundred_k.txt".into(),
                offset: Some(-90_000),
                limit: Some(2),
            },
            &launch,
            false,
            ReadLimits::DIRECT,
        )
        .unwrap();
        assert_eq!(slice.structured["startLine"], 10_001);
        assert_eq!(slice.structured["endLine"], 10_002);
        assert_eq!(slice.body.as_deref(), Some("r10001\nr10002\n"));

        let ten_path = scratch.0.join("ten.txt");
        std::fs::write(
            &ten_path,
            (1..=10).map(|i| format!("{i}\n")).collect::<String>(),
        )
        .unwrap();
        for neg in [-5000_i64, i64::MIN] {
            let clamped = read_text(
                &ReadRequest {
                    path: "ten.txt".into(),
                    offset: Some(neg),
                    limit: Some(3),
                },
                &launch,
                false,
                ReadLimits::DIRECT,
            )
            .unwrap();
            assert_eq!(clamped.structured["startLine"], 1);
            assert_eq!(clamped.structured["endLine"], 3);
        }

        let empty_path = scratch.0.join("empty.txt");
        std::fs::write(&empty_path, b"").unwrap();
        let empty_neg = read_text(
            &ReadRequest {
                path: "empty.txt".into(),
                offset: Some(-1),
                limit: None,
            },
            &launch,
            false,
            ReadLimits::DIRECT,
        )
        .unwrap();
        let empty_pos = read_text(
            &ReadRequest {
                path: "empty.txt".into(),
                offset: Some(1),
                limit: None,
            },
            &launch,
            false,
            ReadLimits::DIRECT,
        )
        .unwrap();
        assert_eq!(empty_neg.structured, empty_pos.structured);
        assert_eq!(empty_neg.body.as_deref(), Some(""));
        assert_eq!(empty_neg.structured["startLine"], 1);
        assert_eq!(empty_neg.structured["shownLines"], 0);

        // Mixed LF, CRLF, lone CR, and unterminated final segment: negative offset matches positive.
        let mixed_path = scratch.0.join("mixed.txt");
        std::fs::write(&mixed_path, b"one\ntwo\r\nthree\rfour\nfive").unwrap();
        for (neg, pos) in [(-1, 4), (-2, 3), (-3, 2), (-4, 1)] {
            let a = read_text(
                &ReadRequest {
                    path: "mixed.txt".into(),
                    offset: Some(neg),
                    limit: None,
                },
                &launch,
                false,
                ReadLimits::DIRECT,
            )
            .unwrap();
            let b = read_text(
                &ReadRequest {
                    path: "mixed.txt".into(),
                    offset: Some(pos),
                    limit: None,
                },
                &launch,
                false,
                ReadLimits::DIRECT,
            )
            .unwrap();
            assert_eq!(a.structured, b.structured, "neg={neg} pos={pos}");
            assert_eq!(a.body, b.body, "neg={neg} pos={pos}");
        }
    }

    #[test]
    fn a_tail_slice_rechecks_its_own_line_count_after_growth_or_shrinkage() {
        let scratch = Scratch::new("changing-tail");
        let path = scratch.0.join("changes.txt");
        for (later, expected, start) in
            [("1\n2\n3\n4\n5\n", "4\n5\n", 4), ("1\n2\n3\n", "2\n3\n", 2)]
        {
            std::fs::write(&path, "1\n2\n3\n4\n").unwrap();
            let mut file = File::open(&path).unwrap();
            let initial = count_lines_streaming(&mut file, "changes.txt", None).unwrap();
            std::fs::write(&path, later).unwrap();
            let (observed, window) = scan_from_end(2, initial, "changes.txt", |from| {
                file.rewind().unwrap();
                Window::scan(
                    Source::new(BufReader::new(&mut file), None),
                    from,
                    None,
                    ReadLimits::DIRECT,
                )
            })
            .unwrap();
            assert_eq!(observed, start);
            let result = window.render(observed, None, "changes.txt", ReadLimits::DIRECT);
            assert_eq!(result.body.as_deref(), Some(expected));
        }

        let mut totals = [5, 6].into_iter();
        let error = scan_from_end(2, 4, "changes.txt", |_| {
            Ok(Window {
                total_lines: totals.next().unwrap(),
                ..Default::default()
            })
        })
        .err()
        .unwrap();
        assert!(error.contains("changed while reading; retry"));
    }

    #[test]
    fn outside_range_hints_count_exact_and_editable_fuzzy_sources_once() {
        let outside = |source: &str, old: &str, line| {
            let request = EditRequest {
                path: "sources.txt".into(),
                edits: vec![Edit {
                    old_text: old.into(),
                    new_text: "updated".into(),
                    start_line: Some(line),
                    end_line: Some(line),
                }],
            };
            apply_edits(source, &request, "sources.txt")
                .unwrap_err()
                .1
                .unwrap()["failures"][0]
                .clone()
        };
        for (source, old, line, starts) in [
            ("q\u{301}\n“a”\n", "q", 2, vec![1]),
            ("x \nfoo\nz\n", " \nfoo", 3, vec![1]),
            ("e\nｅ\n⒠\ne\u{301}\n", "e", 5, vec![1, 2, 4]),
        ] {
            let failure = outside(source, old, line);
            assert_eq!(failure["lines"], json!(starts));
            assert!(failure.get("more").is_none());
        }
        let failure = outside(&"e\u{301}\n".repeat(23), "e", 24);
        assert_eq!(failure["lines"], json!((1..=20).collect::<Vec<_>>()));
        assert_eq!(failure["more"], 3);
    }

    #[test]
    fn edit_start_line_and_end_line_disambiguate_report_outside_and_detect_overlap() {
        let scratch = Scratch::new("edit-line-range");
        let launch = context(&scratch.0, &scratch.0);

        let lines_path = scratch.0.join("hundred.txt");
        let original: String = (1..=100)
            .map(|line| {
                if matches!(line, 10 | 50 | 90) {
                    "value = 1;\n".to_owned()
                } else {
                    format!("line {line}\n")
                }
            })
            .collect();
        std::fs::write(&lines_path, &original).unwrap();

        let amb = edit_outcome(
            &EditRequest {
                path: "hundred.txt".into(),
                edits: vec![Edit {
                    old_text: "value = 1;".into(),
                    new_text: "value = 2;".into(),
                    start_line: None,
                    end_line: None,
                }],
            },
            &launch,
        )
        .unwrap_err()
        .1
        .unwrap();
        assert_eq!(amb["failures"][0]["problem"], "ambiguous");
        assert_eq!(amb["failures"][0]["lines"], json!([10, 50, 90]));

        let miss = edit_outcome(
            &EditRequest {
                path: "hundred.txt".into(),
                edits: vec![Edit {
                    old_text: "value = 1;".into(),
                    new_text: "value = 2;".into(),
                    start_line: Some(20),
                    end_line: Some(30),
                }],
            },
            &launch,
        )
        .unwrap_err()
        .1
        .unwrap();
        assert_eq!(miss["failures"][0]["problem"], "not_found");
        assert_eq!(miss["failures"][0]["lines"], json!([10, 50, 90]));
        assert_eq!(std::fs::read_to_string(&lines_path).unwrap(), original);

        let half = edit_outcome(
            &EditRequest {
                path: "hundred.txt".into(),
                edits: vec![Edit {
                    old_text: "value = 1;".into(),
                    new_text: "value = 2;".into(),
                    start_line: Some(50),
                    end_line: None,
                }],
            },
            &launch,
        )
        .unwrap_err()
        .1
        .unwrap();
        assert_eq!(half["failures"][0]["problem"], "ambiguous");
        assert_eq!(half["failures"][0]["lines"], json!([50, 90]));

        let inverted = edit_outcome(
            &EditRequest {
                path: "hundred.txt".into(),
                edits: vec![Edit {
                    old_text: "value = 1;".into(),
                    new_text: "value = 2;".into(),
                    start_line: Some(60),
                    end_line: Some(50),
                }],
            },
            &launch,
        )
        .unwrap_err();
        assert!(inverted.0.contains("startLine"), "{inverted:?}");

        // Overlap between a ranged edit and an unranged edit is still rejected cleanly.
        let overlap = edit_outcome(
            &EditRequest {
                path: "hundred.txt".into(),
                edits: vec![
                    Edit {
                        old_text: "value = 1;".into(),
                        new_text: "value = 2;".into(),
                        start_line: Some(45),
                        end_line: Some(55),
                    },
                    Edit {
                        old_text: "line 49\nvalue = 1;\nline 51".into(),
                        new_text: "block".into(),
                        start_line: None,
                        end_line: None,
                    },
                ],
            },
            &launch,
        )
        .unwrap_err()
        .1
        .unwrap();
        assert_eq!(overlap["failures"][0]["problem"], "overlap");
        assert_eq!(std::fs::read_to_string(&lines_path).unwrap(), original);

        // Scoped edit updates only line 50; multi-line oldText starting on endLine also succeeds.
        let ok = edit_outcome(
            &EditRequest {
                path: "hundred.txt".into(),
                edits: vec![Edit {
                    old_text: "value = 1;\nline 51".into(),
                    new_text: "value = 2;\nline 51".into(),
                    start_line: Some(45),
                    end_line: Some(50),
                }],
            },
            &launch,
        )
        .unwrap();
        assert_eq!(ok.1["edits"][0]["line"], 50);
        let updated: Vec<String> = std::fs::read_to_string(&lines_path)
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect();
        assert_eq!(updated[9], "value = 1;");
        assert_eq!(updated[49], "value = 2;");
        assert_eq!(updated[89], "value = 1;");

        // Mixed LF, CRLF, and lone CR line endings count LF breaks consistently.
        let mixed = scratch.0.join("mixed_endings.txt");
        std::fs::write(&mixed, b"same\nsame\r\nsame\rtail\nsame\n").unwrap();
        edit_outcome(
            &EditRequest {
                path: "mixed_endings.txt".into(),
                edits: vec![Edit {
                    old_text: "same".into(),
                    new_text: "picked".into(),
                    start_line: Some(3),
                    end_line: Some(3),
                }],
            },
            &launch,
        )
        .unwrap();
        assert_eq!(
            std::fs::read(&mixed).unwrap(),
            b"same\nsame\r\npicked\rtail\nsame\n"
        );

        // Concurrent growth past 4 MiB after metadata check (ending mid-UTF-8 codepoint at
        // max_bytes + 1) reports the size limit with actual byte count rather than a UTF-8 error.
        let grown = scratch.0.join("grown.txt");
        let mut multibyte_over_4m = "a".repeat(NESTED_MAX_BYTES as usize - 1);
        multibyte_over_4m.push('中'); // 4_194_303 + 3 = 4_194_306 bytes
        std::fs::write(&grown, &multibyte_over_4m).unwrap();
        let mut f = File::open(&grown).unwrap();
        let grown_err = read_whole_utf8(
            &mut f,
            0, // simulates initial metadata.len() <= 4 MiB before concurrent append
            &[],
            "grown.txt",
            NESTED_MAX_BYTES,
            None,
        )
        .err()
        .unwrap();
        assert!(
            grown_err.contains("4194306 bytes")
                && grown_err.contains("exceeds the 4194304 byte (4 MiB) limit"),
            "{grown_err}"
        );

        // Invalid UTF-8 file <= 4 MiB on disk whose lossy U+FFFD expansion would exceed 4 MiB
        // (2,000,000 x 0xff -> 6,000,000 B if decoded lossily) must report invalid UTF-8, not size.
        let bad_2m = scratch.0.join("bad_single_2m.txt");
        std::fs::write(&bad_2m, vec![0xff_u8; 2_000_000]).unwrap();
        let bad_2m_err = read_text(
            &ReadRequest {
                path: "bad_single_2m.txt".into(),
                offset: None,
                limit: None,
            },
            &launch,
            false,
            ReadLimits::NESTED,
        )
        .err()
        .unwrap();
        assert!(
            bad_2m_err.contains("not valid UTF-8") && !bad_2m_err.contains("exceeds"),
            "{bad_2m_err}"
        );

        // Linear-time streaming over many short invalid-UTF-8 lines (previously O(N^2) in Source).
        let many_lossy = scratch.0.join("many_lossy.txt");
        std::fs::write(&many_lossy, b"\xff\n".repeat(200_000)).unwrap();
        let started = std::time::Instant::now();
        let lossy_slice = read_text(
            &ReadRequest {
                path: "many_lossy.txt".into(),
                offset: Some(1),
                limit: None,
            },
            &launch,
            false,
            ReadLimits::NESTED,
        )
        .unwrap();
        assert!(
            started.elapsed() < std::time::Duration::from_secs(3),
            "took {:?}",
            started.elapsed()
        );
        assert_eq!(lossy_slice.structured["totalLines"], 200_000);
        assert_eq!(lossy_slice.structured["lossy"], true);

        // Partial compatibility decomposition inside or outside range (⒠ -> "(e)") rejected by
        // FuzzyMap::span never counts as an out-of-range candidate, while real matches do.
        let compat_path = scratch.0.join("compat.txt");
        std::fs::write(&compat_path, "alpha\n\u{24a0}\n\u{24a0}\ne\n").unwrap();
        let compat_miss = edit_outcome(
            &EditRequest {
                path: "compat.txt".into(),
                edits: vec![Edit {
                    old_text: "e".into(),
                    new_text: "E".into(),
                    start_line: Some(2),
                    end_line: Some(2),
                }],
            },
            &launch,
        )
        .unwrap_err()
        .1
        .unwrap();
        assert_eq!(compat_miss["failures"][0]["problem"], "not_found");
        assert_eq!(compat_miss["failures"][0]["lines"], json!([4]));
    }

    #[test]
    fn open_regular_reports_directory_as_is_a_directory_without_exec_hint() {
        let scratch = Scratch::new("open-regular-dir");
        let err = open_regular(&scratch.0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::IsADirectory);
        assert_eq!(err.to_string(), "it is a directory");
    }

    #[test]
    fn windows_reserved_device_names_match_dos_rules() {
        for reserved in [
            "CON",
            "con",
            "CON:",
            "PRN",
            "AUX",
            "NUL",
            "NUL:foo",
            "COM1",
            "COM9",
            "LPT1",
            "LPT9",
            "COM1.txt",
            "nul.log",
            "CON .txt",
            "NUL   ",
            "COM¹",
            "COM².txt",
            "com³:stream",
            "LPT¹",
            "lpt².log",
            "LPT³",
        ] {
            assert!(
                is_windows_reserved_device_name(reserved),
                "expected reserved: {reserved}"
            );
        }
        for normal in [
            "COM0",
            "COM10",
            "LPT0",
            "LPT10",
            "CONSOLE",
            "NULL",
            "a.NUL",
            "COM⁴",
            "LPT¹0",
            "日本語",
        ] {
            assert!(
                !is_windows_reserved_device_name(normal),
                "expected normal: {normal}"
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn resolve_from_rejects_windows_device_paths_named_pipes_and_reserved_names() {
        let base = Path::new(r"C:\work");
        for rejected in [
            r"\\.\pipe\demo",
            "//./pipe/demo",
            "//server/pipe/demo",
            r"\\server\pipe\demo",
            r"\\.\NUL",
            r"\\.\PhysicalDrive0",
            "NUL",
            "CON",
            "COM1.txt",
            r"sub\LPT9.log",
            r"C:\work\aux",
        ] {
            assert!(
                resolve_from(Path::new(rejected), base).is_err(),
                "expected rejection for {rejected}"
            );
        }
    }
}
