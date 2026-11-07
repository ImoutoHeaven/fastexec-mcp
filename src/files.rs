//! The text file tools: `read`, `write` and `edit`, and the path resolution they share.
//!
//! Every function here blocks: the callers run them on `spawn_blocking`, so one large file never
//! holds a runtime thread. Read streams a file and keeps only the window it may show; write and
//! edit hold the whole file, because they rewrite it.

use crate::images;
use crate::session::LaunchContext;
use rmcp::model::{CallToolResult, ContentBlock};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(all(unix, not(any(target_os = "macos", target_os = "freebsd"))))]
use std::fs::Metadata;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use unicode_normalization::UnicodeNormalization;
use unicode_normalization::char::canonical_combining_class;

/// The most lines a result shows.
const MAX_LINES: u64 = 2000;
/// The most bytes a result shows.
const MAX_BYTES: u64 = 50 * 1024;
/// How much of one line is kept while it is read: a line that fits whole, and one byte more, so a
/// longer line is known not to fit.
const KEEP: usize = MAX_BYTES as usize + 1;
/// How much of a file is sniffed before deciding whether it is text.
const SNIFF: usize = 8 * 1024;

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
    /// Line number to start reading from (1-indexed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<u64>,
    /// Maximum number of lines to read
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
    /// Exact text for one targeted replacement. It must be unique in the original file and must not overlap with any other edits[].oldText in the same call.
    pub old_text: String,
    /// Replacement text for this targeted edit.
    pub new_text: String,
}

/// Runs one file tool from raw arguments, the way a daemon receives them, with the same
/// validation and errors as a local call.
pub async fn call(
    tool: &str,
    arguments: Value,
    launch: Arc<LaunchContext>,
    http: bool,
) -> CallToolResult {
    match tool {
        "read" => match serde_json::from_value::<ReadRequest>(arguments) {
            Ok(request) => read(request, launch, http).await,
            Err(error) => failed(format!("failed to deserialize parameters: {error}")),
        },
        "write" => match serde_json::from_value::<WriteRequest>(arguments) {
            Ok(request) => write(request, launch).await,
            Err(error) => failed(format!("failed to deserialize parameters: {error}")),
        },
        "edit" => match serde_json::from_value::<EditRequest>(arguments) {
            Ok(request) => edit(request, launch).await,
            Err(error) => failed(format!("failed to deserialize parameters: {error}")),
        },
        other => failed(format!("unknown tool {other}")),
    }
}

/// Reads one file: its text, or the message saying why it is not shown.
pub async fn read(request: ReadRequest, launch: Arc<LaunchContext>, http: bool) -> CallToolResult {
    match tokio::task::spawn_blocking(move || read_text(&request, &launch, http)).await {
        Ok(Ok(outcome)) => CallToolResult::success(outcome.blocks()),
        Ok(Err(message)) => failed(message),
        Err(error) => failed(format!("the file operation did not finish: {error}")),
    }
}

/// What a read answers with: its text, and the image that text names when the file is an image.
pub struct Outcome {
    pub text: String,
    pub image: Option<(String, String)>,
}

impl Outcome {
    fn text(text: String) -> Self {
        Self { text, image: None }
    }

    /// The result's content: the text, then the image when there is one.
    fn blocks(self) -> Vec<ContentBlock> {
        let mut blocks = vec![ContentBlock::text(self.text)];
        if let Some((data, mime)) = self.image {
            blocks.push(ContentBlock::image(data, mime));
        }
        blocks
    }
}

/// Writes one file, creating its parent directories.
pub async fn write(request: WriteRequest, launch: Arc<LaunchContext>) -> CallToolResult {
    finished(tokio::task::spawn_blocking(move || write_text(&request, &launch)).await)
}

/// Applies targeted replacements to one file.
pub async fn edit(request: EditRequest, launch: Arc<LaunchContext>) -> CallToolResult {
    finished(tokio::task::spawn_blocking(move || edit_text(&request, &launch)).await)
}

/// A blocking operation's answer: its text, or its message as the error result.
fn finished(outcome: Result<Result<String, String>, tokio::task::JoinError>) -> CallToolResult {
    match outcome {
        Ok(Ok(text)) => CallToolResult::success(vec![ContentBlock::text(text)]),
        Ok(Err(message)) => failed(message),
        Err(error) => failed(format!("the file operation did not finish: {error}")),
    }
}

fn failed(message: String) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(message)])
}

/// The path a request names: `~` and `~/` expand to the launch context's home, a relative path
/// joins its working directory, and an absolute path is used as it is.
pub fn resolve(path: &str, launch: &LaunchContext) -> Result<PathBuf, String> {
    let home = || {
        launch
            .home()
            .ok_or_else(|| format!("Cannot expand ~ in {path}: the home directory is unknown."))
    };
    if path == "~" {
        return home();
    }
    if let Some(rest) = path.strip_prefix("~/") {
        return Ok(home()?.join(rest));
    }
    #[cfg(windows)]
    {
        if let Some(rest) = path.strip_prefix("~\\") {
            return Ok(home()?.join(rest));
        }
    }
    let named = PathBuf::from(path);
    if named.is_absolute() {
        Ok(named)
    } else {
        Ok(launch.cwd.join(named))
    }
}

/// The text of one file, cut to the window and the limits a result may show.
fn read_text(request: &ReadRequest, launch: &LaunchContext, http: bool) -> Result<Outcome, String> {
    let resolved = resolve(&request.path, launch)?;
    let (mut file, metadata) = open_regular(&resolved)
        .map_err(|error| format!("Cannot read {}: {error}.", request.path))?;
    let mut sniff = vec![0u8; SNIFF];
    let sniffed = read_upto(&mut file, &mut sniff)
        .map_err(|error| format!("Cannot read {}: {error}.", request.path))?;
    if let Some(mime) = images::detect(&sniff[..sniffed]) {
        // The encoded image is read within the ceiling the decoder allocates under, so a file
        // far larger than any inline image is never loaded.
        let bytes = images::read_encoded(&mut file, metadata.len())
            .map_err(|error| format!("Cannot read {}: {error}.", request.path))?;
        let Some(bytes) = bytes else {
            let omitted = images::oversized(mime);
            return Ok(Outcome {
                text: omitted.text,
                image: omitted.image,
            });
        };
        let processed = images::process(bytes, mime);
        return Ok(Outcome {
            text: processed.text,
            image: processed.image,
        });
    }
    if sniff[..sniffed].contains(&0) {
        let hint = if http {
            " Inspect it with exec or download it with transfer."
        } else {
            " Inspect it with exec."
        };
        return Err(format!(
            "{} is a binary file ({} bytes).{hint}",
            request.path,
            metadata.len()
        ));
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|error| format!("Cannot read {}: {error}.", request.path))?;
    let source = Source::new(BufReader::new(file));
    Ok(Outcome::text(
        Window::scan(source, request)?.render(request),
    ))
}

/// Both text readers reject special files before reading any data. The initial check avoids
/// opening devices at all; the handle check covers replacement between metadata and open. On
/// Unix, a FIFO swapped into that gap must not block open while edit holds the mutation lock.
fn open_regular(path: &Path) -> io::Result<(File, std::fs::Metadata)> {
    if !std::fs::metadata(path)?.is_file() {
        return Err(io::Error::other("it is not a regular file"));
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY);
    }
    let file = options.open(path)?;
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{FILE_TYPE_DISK, GetFileType};
        // SAFETY: file owns this live handle. Reject named pipes/consoles before any read.
        if unsafe { GetFileType(file.as_raw_handle()) } != FILE_TYPE_DISK {
            return Err(io::Error::other("it is not a regular file"));
        }
    }
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::other("it is not a regular file"));
    }
    Ok((file, metadata))
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
struct Source {
    reader: BufReader<File>,
    /// Decoded text already read and not yet given to a line, with where each replacement
    /// character it holds sits.
    pending: String,
    marks: Vec<usize>,
    /// Bytes of a character the last read cut in two, carried to the next read.
    carry: Vec<u8>,
    /// Set once every line has been answered.
    done: bool,
}

impl Source {
    fn new(reader: BufReader<File>) -> Self {
        Self {
            reader,
            pending: String::new(),
            marks: Vec::new(),
            carry: Vec::new(),
            done: false,
        }
    }

    /// The next line as decoded text, keeping at most `KEEP` bytes of it; `None` after the last.
    fn kept(&mut self) -> Result<Option<Line>, String> {
        if self.done {
            return Ok(None);
        }
        let mut line = Line::default();
        loop {
            if let Some(at) = self.pending.find('\n') {
                let lossy = self.marks.iter().any(|mark| *mark < at);
                line.push(&self.pending[..at], lossy);
                self.take(at + 1);
                return Ok(Some(line));
            }
            // No line end here, so all of it belongs to this line.
            let text = std::mem::take(&mut self.pending);
            let lossy = !self.marks.is_empty();
            line.push(&text, lossy);
            self.marks.clear();
            if self.fill()? {
                // The end: the flushed unfinished character is the rest of this line.
                let text = std::mem::take(&mut self.pending);
                let lossy = !self.marks.is_empty();
                line.push(&text, lossy);
                self.marks.clear();
                self.done = true;
                return Ok(Some(line));
            }
        }
    }

    /// The next line, looked at only for its end; `None` after the last.
    fn skipped(&mut self) -> Result<Option<u64>, String> {
        if self.done {
            return Ok(None);
        }
        // A line already decoded in `pending` ends this one, and the unfinished character the
        // decoder holds comes after every byte in `pending`, so it is no part of this line.
        if let Some(at) = self.pending.find('\n') {
            let length = self.pending[..at].len() as u64;
            self.take(at + 1);
            return Ok(Some(length));
        }
        // No line end in `pending`, so all of it belongs to this line, and so does the
        // unfinished character after it: its bytes are already read and would otherwise be lost.
        let mut length = self.pending.len() as u64 + self.carry.len() as u64;
        self.pending.clear();
        self.marks.clear();
        self.carry.clear();
        loop {
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
                return Ok(Some(length));
            }
            match end {
                Some(at) => {
                    length += at as u64;
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

    /// Drops `count` bytes from the front of `pending`, marks included.
    fn take(&mut self, count: usize) {
        self.pending.drain(..count);
        self.marks.retain(|mark| *mark >= count);
        for mark in &mut self.marks {
            *mark -= count;
        }
    }
}

/// The selected lines of a file: the text a result may show, and the counts its notices need.
#[derive(Default)]
struct Window {
    /// The shown lines joined by `\n`, exactly as they were decoded, with no `\n` of its own
    /// after the last one.
    content: String,
    /// Whether decoding a kept line replaced bytes with U+FFFD.
    lossy: bool,
    /// Lines in the file: one more than its `\n` bytes.
    total_lines: u64,
    /// The first selected line's length in decoded bytes, for the too-long notice.
    first_line_bytes: u64,
    /// Lines and bytes of the selected content, before truncation.
    window_lines: u64,
    window_bytes: u64,
    /// Lines of the selected content that fit the budget.
    shown_lines: u64,
    /// Whether the last selected line was empty, which is the element a final `\n` adds.
    last_line_empty: bool,
    /// The selected line that was refused for its size, rather than the line budget running out.
    tripped_at: Option<u64>,
}

impl Window {
    /// Streams the file, keeping only the lines the result may show and counting the rest.
    fn scan(mut source: Source, request: &ReadRequest) -> Result<Self, String> {
        let start = request.offset.map_or(0, |offset| offset.saturating_sub(1));
        let end = request
            .limit
            .map_or(u64::MAX, |limit| start.saturating_add(limit));
        let mut window = Self::default();
        let mut deciding = true;
        loop {
            let in_window = window.total_lines >= start && window.total_lines < end;
            if in_window && deciding {
                let Some(line) = source.kept()? else {
                    break;
                };
                window.take_line(&line, &mut deciding);
            } else {
                let Some(length) = source.skipped()? else {
                    break;
                };
                window.skip_line(in_window, length);
            }
        }
        if start >= window.total_lines {
            return Err(format!(
                "Offset {} is beyond end of file ({} lines total)",
                request.offset.unwrap_or(0),
                window.total_lines
            ));
        }
        Ok(window)
    }

    /// Records a kept line: its counts, and its bytes when it fits the budget.
    fn take_line(&mut self, line: &Line, deciding: &mut bool) {
        self.total_lines += 1;
        let bytes = line.length + u64::from(self.window_lines > 0);
        self.window_lines += 1;
        self.window_bytes += bytes;
        self.last_line_empty = line.length == 0;
        if self.window_lines == 1 {
            self.first_line_bytes = line.length;
        }
        // A line too large to show is still a line that was decoded, so its replacement
        // characters are known and reported.
        self.lossy |= line.lossy;
        if self.window_bytes > MAX_BYTES {
            self.tripped_at = Some(self.window_lines);
            *deciding = false;
            return;
        }
        if self.shown_lines > 0 {
            self.content.push('\n');
        }
        self.content.push_str(&line.text);
        self.shown_lines += 1;
        if self.shown_lines >= MAX_LINES {
            *deciding = false;
        }
    }

    /// Records a line that is only counted, not shown.
    fn skip_line(&mut self, in_window: bool, length: u64) {
        self.total_lines += 1;
        if !in_window {
            return;
        }
        // The byte total still grows: the truncation compares it against its budget, and the
        // empty element a final newline adds is a byte of the selected content too.
        self.window_bytes += length + u64::from(self.window_lines > 0);
        self.window_lines += 1;
        self.last_line_empty = length == 0;
        if self.window_lines == 1 {
            self.first_line_bytes = length;
        }
    }

    /// Whether the selected content ends with the empty element a final `\n` adds, which the
    /// truncation does not count as a line.
    fn trailing_element(&self) -> bool {
        self.window_lines > 1 && self.last_line_empty
    }

    /// The lines of the selected content, counted the way the truncation counts them.
    fn content_lines(&self) -> u64 {
        if self.window_lines == 1 && self.last_line_empty {
            return 0;
        }
        self.window_lines - u64::from(self.trailing_element())
    }

    /// Whether the truncation stopped on a line's size. The empty element a final newline adds
    /// is not a line the truncation counts, so it cannot be the one that stopped it.
    fn stopped_on_bytes(&self) -> bool {
        match self.tripped_at {
            Some(at) if at == self.window_lines && self.trailing_element() => false,
            at => at.is_some(),
        }
    }

    /// The result text: the window, cut to the budget, with the notice that says how to see more.
    fn render(&self, request: &ReadRequest) -> String {
        let start = request.offset.map_or(0, |offset| offset.saturating_sub(1));
        let start_display = start + 1;
        let trailing = self.trailing_element();
        let shown = self.shown_lines - u64::from(trailing && self.shown_lines == self.window_lines);
        let truncated = self.content_lines() > MAX_LINES || self.window_bytes > MAX_BYTES;
        let mut text = String::new();
        if truncated && self.first_line_bytes > MAX_BYTES {
            text.push_str(&format!(
                "[Line {start_display} is {}, exceeds {} limit. Use exec: sed -n '{start_display}p' {} | head -c {MAX_BYTES}]",
                format_size(self.first_line_bytes),
                format_size(MAX_BYTES),
                request.path
            ));
        } else if truncated {
            let end_display = start_display + shown - 1;
            let next = end_display + 1;
            text.push_str(&self.content);
            if self.stopped_on_bytes() {
                text.push_str(&format!(
                    "\n\n[Showing lines {start_display}-{end_display} of {} ({} limit). Use offset={next} to continue.]",
                    self.total_lines,
                    format_size(MAX_BYTES)
                ));
            } else {
                text.push_str(&format!(
                    "\n\n[Showing lines {start_display}-{end_display} of {}. Use offset={next} to continue.]",
                    self.total_lines
                ));
            }
        } else {
            text.push_str(&self.content);
            // The selected content keeps the file's final newline, even when the line budget
            // stopped the scan right before the empty line that newline adds.
            if trailing && self.shown_lines != self.window_lines {
                text.push('\n');
            }
            if let Some(limit) = request.limit {
                let selected_end = start.saturating_add(limit);
                if selected_end < self.total_lines {
                    let remaining = self.total_lines - selected_end;
                    let next = selected_end + 1;
                    text.push_str(&format!(
                        "\n\n[{remaining} more lines in file. Use offset={next} to continue.]"
                    ));
                }
            }
        }
        if self.lossy {
            text.push_str("\n\n[Invalid UTF-8 was replaced with U+FFFD.]");
        }
        text
    }
}

/// One line while it is read: its whole decoded length, and the bytes of it that are kept.
#[derive(Default)]
struct Line {
    /// The decoded bytes of the line, up to `KEEP`.
    text: String,
    /// The line's whole length in decoded bytes.
    length: u64,
    /// Whether decoding replaced bytes of this line.
    lossy: bool,
}

impl Line {
    /// Adds decoded text, keeping at most `KEEP` bytes of it.
    fn push(&mut self, bytes: &str, lossy: bool) {
        if bytes.is_empty() {
            return;
        }
        self.length += bytes.len() as u64;
        self.lossy |= lossy;
        if self.text.len() >= KEEP {
            return;
        }
        let room = KEEP - self.text.len();
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
fn write_text(request: &WriteRequest, launch: &LaunchContext) -> Result<String, String> {
    let resolved = resolve(&request.path, launch)?;
    let _guard = lock();
    let target = write_target(&resolved)
        .map_err(|error| format!("Cannot write {}: {error}.", request.path))?;
    if let Some(parent) = target.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("Cannot write {}: {error}.", request.path))?;
    }
    save_target(target, request.content.as_bytes())
        .map_err(|error| format!("Cannot write {}: {error}.", request.path))?;
    Ok(format!("Successfully wrote to {}", request.path))
}

/// Follows a final-component symlink without replacing the link itself, including a dangling
/// link whose target can be created. Parent-directory symlinks keep their normal filesystem
/// meaning. A finite walk also refuses cycles before any file is changed.
fn write_target(path: &Path) -> io::Result<PathBuf> {
    let mut target = path.to_path_buf();
    for _ in 0..40 {
        match std::fs::symlink_metadata(&target) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                let link = std::fs::read_link(&target)?;
                target = if link.is_absolute() {
                    link
                } else {
                    target.parent().unwrap_or_else(|| Path::new(".")).join(link)
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
            let path = parent.join(name);
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
                    target.display(),
                    backup.path.display()
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

/// Saves before touching the old inode: ENOSPC, quotas, failed syncs, and failed metadata
/// preservation all leave the original intact. This requires a writable parent directory.
fn save_text(path: &Path, bytes: &[u8]) -> io::Result<()> {
    save_target(write_target(path)?, bytes)
}

/// Writes to the resolved target without following a final link again after creating parents.
fn save_target(target: PathBuf, bytes: &[u8]) -> io::Result<()> {
    let original = match std::fs::symlink_metadata(&target) {
        Ok(metadata) => {
            if !metadata.is_file() {
                return Err(io::Error::other("the target is not a regular file"));
            }
            let mut options = OpenOptions::new();
            options.write(true);
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

/// Applies every edit to one file: exact matches first, and the fuzzy form when any edit needs it.
fn edit_text(request: &EditRequest, launch: &LaunchContext) -> Result<String, String> {
    if request.edits.is_empty() {
        return Err(
            "Edit tool input is invalid. edits must contain at least one replacement.".to_owned(),
        );
    }
    let resolved = resolve(&request.path, launch)?;
    let _guard = lock();
    let (mut file, _) = open_regular(&resolved)
        .map_err(|error| format!("Could not edit file: {}. {error}.", request.path))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| format!("Could not edit file: {}. {error}.", request.path))?;
    let raw = String::from_utf8(bytes)
        .map_err(|_| format!("{} is not valid UTF-8. Edit it with exec.", request.path))?;
    let (bom, content) = match raw.strip_prefix('\u{feff}') {
        Some(rest) => ("\u{feff}", rest),
        None => ("", raw.as_str()),
    };
    let (replaced, lines) = apply_edits(content, request, &request.path)?;
    let written = format!("{bom}{replaced}");
    save_text(&resolved, written.as_bytes())
        .map_err(|error| format!("Could not edit file: {}. {error}.", request.path))?;
    let mut text = format!(
        "Successfully replaced {} block(s) in {}.",
        request.edits.len(),
        request.path
    );
    for (index, (line, fuzzy)) in lines.iter().enumerate() {
        text.push_str(&format!("\nedits[{index}]: line {line}"));
        if *fuzzy {
            text.push_str(" (fuzzy match)");
        }
    }
    Ok(text)
}

/// Matches in LF-normalized text but replaces the corresponding original byte ranges. Bytes
/// outside those ranges, including mixed line endings, stay untouched.
fn apply_edits(
    content: &str,
    request: &EditRequest,
    path: &str,
) -> Result<(String, Vec<(usize, bool)>), String> {
    let total = request.edits.len();
    let normalized = LfText::new(content);
    let ending = detect_line_ending(content);
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
    for (index, (old, _)) in edits.iter().enumerate() {
        if old.is_empty() {
            return Err(format!("edits[{index}].oldText must not be empty."));
        }
    }
    let inserted: Vec<String> = edits
        .iter()
        .map(|(_, new)| restore_line_endings(new, ending))
        .collect();
    // Fuzzy matching first maps back to normalized bytes; the newline map then names the
    // original bytes, including both bytes of a CRLF when the match includes that newline.
    let mut matched: Vec<Matched<'_>> = Vec::new();
    for (index, (old, new)) in edits.iter().enumerate() {
        let Some((at, len, fuzzy)) = find_span(&normalized.text, old) else {
            return Err(not_found(path, index, total));
        };
        let count = occurrences(&normalized.text, old);
        if count > 1 {
            return Err(duplicate(path, index, total, count));
        }
        let (raw_at, raw_end) = (normalized.original(at), normalized.original(at + len));
        // An unchanged block must not normalize its own mixed newlines as a side effect.
        let new_text = if normalized.text[at..at + len] == *new {
            &content[raw_at..raw_end]
        } else {
            &inserted[index]
        };
        matched.push(Matched {
            index,
            at: raw_at,
            len: raw_end - raw_at,
            fuzzy,
            new_text,
        });
    }
    matched.sort_by_key(|edit| edit.at);
    for pair in matched.windows(2) {
        if pair[0].at + pair[0].len > pair[1].at {
            return Err(format!(
                "edits[{}] and edits[{}] overlap in {path}. Merge them into one edit or target disjoint regions.",
                pair[0].index, pair[1].index
            ));
        }
    }
    let (replaced, starts) = replace(content, &matched);
    if replaced == content {
        return Err(no_change(path, total));
    }
    let mut lines = vec![(0usize, false); total];
    for edit in &matched {
        let at = starts[edit.index];
        lines[edit.index] = (1 + line_breaks(&replaced[..at]), edit.fuzzy);
    }
    Ok((replaced, lines))
}

/// Finds `old` in `content`: as it is first, then through the fuzzy form, answering with the span
/// of `content` the edit replaces and whether only the fuzzy form matched.
fn find_span(content: &str, old: &str) -> Option<(usize, usize, bool)> {
    if let Some(at) = content.find(old) {
        return Some((at, old.len(), false));
    }
    let (at, len) = fuzzy_span(content, old)?;
    Some((at, len, true))
}

/// Maps the first match in the fuzzy form of `content` back to the bytes of `content` it stands for.
fn fuzzy_span(content: &str, old: &str) -> Option<(usize, usize)> {
    let needle = normalize_for_fuzzy_match(old);
    if needle.is_empty() {
        // A target without a character of its own is no target: it would match everywhere.
        return None;
    }
    let haystack = normalize_for_fuzzy_match(content);
    let at = haystack.find(&needle)?;
    FuzzyMap::new(content)?.span(at, at + needle.len())
}

/// Counts exact duplicates first, then fuzzy copies; both counts include overlapping starts.
fn occurrences(content: &str, old: &str) -> usize {
    let exact = starts_in(content, old);
    // Normalization can compose across the target's boundary and erase exact candidates.
    if exact > 1 {
        return exact;
    }
    let needle = normalize_for_fuzzy_match(old);
    if needle.is_empty() {
        // A target that normalizes to nothing is counted as the content spells it.
        return exact;
    }
    let haystack = normalize_for_fuzzy_match(content);
    if haystack == content && needle == old {
        return exact;
    }
    let count = starts_in(&haystack, &needle);
    // Trim can shorten the exact target's fuzzy span; compare its mapped form.
    if exact == 1
        && count == 1
        && let Some(exact_at) = content.find(old)
        && let Some(fuzzy_at) = haystack.find(&needle)
        && let Some(fuzzy_span) =
            FuzzyMap::new(content).and_then(|map| map.span(fuzzy_at, fuzzy_at + needle.len()))
        && let Some((offset, len)) = FuzzyMap::new(old).and_then(|map| map.span(0, needle.len()))
        && fuzzy_span != (exact_at + offset, len)
    {
        return 2;
    }
    exact.max(count)
}

/// How many starts `needle` has in `haystack`, counting overlapping occurrences in one pass.
///
/// A match of a nonempty UTF-8 needle in UTF-8 text always starts on a character boundary, because
/// a lead byte is never a continuation byte, so the bytes need no boundary check.
fn starts_in(haystack: &str, needle: &str) -> usize {
    let (haystack, needle) = (haystack.as_bytes(), needle.as_bytes());
    if needle.is_empty() {
        return 0;
    }
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
    let mut count = 0;
    matched = 0;
    for byte in haystack {
        while matched > 0 && *byte != needle[matched] {
            matched = table[matched - 1];
        }
        if *byte == needle[matched] {
            matched += 1;
        }
        if matched == needle.len() {
            count += 1;
            matched = table[matched - 1];
        }
    }
    count
}

fn not_found(path: &str, index: usize, total: usize) -> String {
    if total == 1 {
        "Could not find the exact text in {path}. The old text must match exactly including all whitespace and newlines."
            .replace("{path}", path)
    } else {
        format!(
            "Could not find edits[{index}] in {path}. The oldText must match exactly including all whitespace and newlines."
        )
    }
}

fn duplicate(path: &str, index: usize, total: usize, count: usize) -> String {
    if total == 1 {
        format!(
            "Found {count} occurrences of the text in {path}. The text must be unique. Please provide more context to make it unique."
        )
    } else {
        format!(
            "Found {count} occurrences of edits[{index}] in {path}. Each oldText must be unique. Please provide more context to make it unique."
        )
    }
}

fn no_change(path: &str, total: usize) -> String {
    if total == 1 {
        format!(
            "No changes made to {path}. The replacement produced identical content. This might indicate an issue with special characters or the text not existing as expected."
        )
    } else {
        format!("No changes made to {path}. The replacements produced identical content.")
    }
}

/// Applies matches to `content`, answering with the new content and each edit's start in it.
fn replace(content: &str, matched: &[Matched<'_>]) -> (String, Vec<usize>) {
    let mut starts = vec![0usize; matched.len()];
    let mut shift: i64 = 0;
    for edit in matched {
        starts[edit.index] = (edit.at as i64 + shift) as usize;
        shift += edit.new_text.len() as i64 - edit.len as i64;
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

/// Counts CRLF as one line break, along with individual CR and LF.
fn line_breaks(text: &str) -> usize {
    text.bytes()
        .filter(|&byte| byte == b'\r' || byte == b'\n')
        .count()
        - text.matches("\r\n").count()
}

/// The first line ending is the convention used only for inserted text.
fn detect_line_ending(content: &str) -> &'static str {
    match content.find(['\r', '\n']) {
        Some(at) if content[at..].starts_with("\r\n") => "\r\n",
        Some(at) if content.as_bytes()[at] == b'\r' => "\r",
        _ => "\n",
    }
}

fn normalize_to_lf(text: &str) -> String {
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
            self.0.join(name).display().to_string()
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

    fn whole(path: &str, launch: &LaunchContext, http: bool) -> Result<String, String> {
        read_text(
            &ReadRequest {
                path: path.to_owned(),
                offset: None,
                limit: None,
            },
            launch,
            http,
        )
        .map(|outcome| outcome.text)
    }

    fn window(
        path: &str,
        offset: Option<u64>,
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
        )
        .map(|outcome| outcome.text)
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
    fn read_returns_the_window_it_was_asked_for() {
        let scratch = Scratch::new("window");
        let text: String = (1..=10).map(|line| format!("line {line}\n")).collect();
        scratch.file("ten.txt", text.as_bytes());
        let launch = context(&scratch.0, &scratch.0);
        let path = scratch.path("ten.txt");
        // Ten lines and the empty one after the final newline: eleven in all.
        assert_eq!(whole(&path, &launch, false).unwrap(), text);
        assert_eq!(
            window(&path, Some(2), Some(3), &launch).unwrap(),
            "line 2\nline 3\nline 4\n\n[7 more lines in file. Use offset=5 to continue.]"
        );
        // A limit of zero shows nothing and still says where the rest is.
        assert_eq!(
            window(&path, None, Some(0), &launch).unwrap(),
            "\n\n[11 more lines in file. Use offset=1 to continue.]"
        );
        assert_eq!(window(&path, Some(11), None, &launch).unwrap(), "");
        assert_eq!(
            window(&path, Some(12), None, &launch).unwrap_err(),
            "Offset 12 is beyond end of file (11 lines total)"
        );
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
            text.ends_with("[Showing lines 1-2000 of 2501. Use offset=2001 to continue.]"),
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
                "[Showing lines 1-853 of 901 (50.0KB limit). Use offset=854 to continue.]"
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
                "[Line 1 is 50.0KB, exceeds 50.0KB limit. Use exec: sed -n '1p' {path} | head -c {MAX_BYTES}]"
            )
        );
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
        assert_eq!(
            whole(&directory, &launch, false).unwrap_err(),
            format!("Cannot read {directory}: it is not a regular file.")
        );
        scratch.file("bad.txt", b"ok\n\xffbad\n");
        let path = scratch.path("bad.txt");
        assert_eq!(
            whole(&path, &launch, false).unwrap(),
            "ok\n\u{fffd}bad\n\n\n[Invalid UTF-8 was replaced with U+FFFD.]"
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
        // Exactly the budget, then the newline: the line fits, so the notice counts lines.
        let line = "z".repeat(MAX_BYTES as usize);
        scratch.file("at.txt", format!("{line}\n").as_bytes());
        let path = scratch.path("at.txt");
        assert_eq!(
            whole(&path, &launch, false).unwrap(),
            format!("{line}\n\n[Showing lines 1-1 of 2. Use offset=2 to continue.]")
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
        // A unique blank is a real target, and it is counted where the content spells it.
        scratch.file("a.txt", b"a b");
        assert_eq!(
            change("a.txt", &[(" ", "X")], &launch).unwrap(),
            "Successfully replaced 1 block(s) in a.txt.\nedits[0]: line 1"
        );
        assert_eq!(scratch.read("a.txt"), b"aXb");
        // Two blanks are two candidates.
        scratch.file("c.txt", b"a b c");
        assert_eq!(
            change("c.txt", &[(" ", "X")], &launch).unwrap_err(),
            "Found 2 occurrences of the text in c.txt. The text must be unique. Please provide more context to make it unique."
        );
        assert_eq!(scratch.read("c.txt"), b"a b c");
        // A blank the content does not have is not a target at all.
        scratch.file("b.txt", "\u{1f600}\u{1f600}".as_bytes());
        assert_eq!(
            change("b.txt", &[("\u{3000}", "X")], &launch).unwrap_err(),
            "Could not find the exact text in b.txt. The old text must match exactly including all whitespace and newlines."
        );
        assert_eq!(scratch.read("b.txt"), "\u{1f600}\u{1f600}".as_bytes());
    }

    #[test]
    fn write_creates_parents_and_overwrites_in_place() {
        let scratch = Scratch::new("write");
        let launch = context(&scratch.0, &scratch.0);
        let written = write_text(
            &WriteRequest {
                path: "deep/er/file.txt".to_owned(),
                content: "first".to_owned(),
            },
            &launch,
        )
        .unwrap();
        assert_eq!(written, "Successfully wrote to deep/er/file.txt");
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
        assert!(reply.contains("edits[1]: line 5"), "{reply}");
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
            error.to_string().contains(recovery.to_str().unwrap()),
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
            error.to_string().contains(recovery.to_str().unwrap()),
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
        assert!(answer.ends_with("line 2"), "{answer}");
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
        scratch.file("a.txt", b"one\ntwo\nthree\n");
        assert_eq!(
            change("a.txt", &[("two", "TWO")], &launch).unwrap(),
            "Successfully replaced 1 block(s) in a.txt.\nedits[0]: line 2"
        );
        assert_eq!(scratch.read("a.txt"), b"one\nTWO\nthree\n");
    }

    #[test]
    fn edit_reports_a_later_edits_line_after_an_earlier_one_adds_lines() {
        let scratch = Scratch::new("shift");
        let launch = context(&scratch.0, &scratch.0);
        scratch.file("a.txt", b"1\n2\n3\n4\n");
        assert_eq!(
            change("a.txt", &[("2", "2\n2b\n2c"), ("4", "4x")], &launch).unwrap(),
            "Successfully replaced 2 block(s) in a.txt.\nedits[0]: line 2\nedits[1]: line 6"
        );
        assert_eq!(scratch.read("a.txt"), b"1\n2\n2b\n2c\n3\n4x\n");
    }

    #[test]
    fn edit_refuses_input_it_cannot_apply() {
        let scratch = Scratch::new("refuse");
        let launch = context(&scratch.0, &scratch.0);
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
            "Could not find the exact text in a.txt. The old text must match exactly including all whitespace and newlines."
        );
        assert_eq!(
            change("a.txt", &[("zzz", "y"), ("qqq", "z")], &launch).unwrap_err(),
            "Could not find edits[0] in a.txt. The oldText must match exactly including all whitespace and newlines."
        );
        assert_eq!(
            change("a.txt", &[("x", "y")], &launch).unwrap_err(),
            "Found 2 occurrences of the text in a.txt. The text must be unique. Please provide more context to make it unique."
        );
        assert_eq!(
            change("a.txt", &[("x", "y"), ("x", "z")], &launch).unwrap_err(),
            "Found 2 occurrences of edits[0] in a.txt. Each oldText must be unique. Please provide more context to make it unique."
        );
        scratch.file("u.txt", b"unique\n");
        assert_eq!(
            change("u.txt", &[("unique", "unique")], &launch).unwrap_err(),
            "No changes made to u.txt. The replacement produced identical content. This might indicate an issue with special characters or the text not existing as expected."
        );
        scratch.file("b.txt", b"abc\n");
        assert_eq!(
            change("b.txt", &[("ab", "X"), ("bc", "Y")], &launch).unwrap_err(),
            "edits[0] and edits[1] overlap in b.txt. Merge them into one edit or target disjoint regions."
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
    fn edit_matches_a_typographic_copy_and_keeps_the_bytes_around_it() {
        let scratch = Scratch::new("fuzzy");
        let launch = context(&scratch.0, &scratch.0);
        scratch.file(
            "a.txt",
            "note: \u{201c}keep\u{201d} it\u{2019}s here   \nsecond line\n".as_bytes(),
        );
        assert_eq!(
            change("a.txt", &[("it's here", "it's fine")], &launch).unwrap(),
            "Successfully replaced 1 block(s) in a.txt.\nedits[0]: line 1 (fuzzy match)"
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
        let original =
            "{\"quote\":\"She said \u{201c}hello\u{201d}\",\"author\":\"O\u{2019}Reilly\"}";
        scratch.file("case.json", original.as_bytes());
        assert_eq!(
            change("case.json", &[("O'Reilly", "Publisher")], &launch).unwrap(),
            "Successfully replaced 1 block(s) in case.json.\nedits[0]: line 1 (fuzzy match)"
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
            "Successfully replaced 2 block(s) in a.txt.\nedits[0]: line 1\nedits[1]: line 2 (fuzzy match)"
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
        scratch.file("a.txt", b"banana\n");
        assert_eq!(
            change("a.txt", &[("ana", "X")], &launch).unwrap_err(),
            "Found 2 occurrences of the text in a.txt. The text must be unique. Please provide more context to make it unique."
        );
        assert_eq!(scratch.read("a.txt"), b"banana\n");
        scratch.file("b.txt", b"aaaa\n");
        assert_eq!(
            change("b.txt", &[("aaa", "X")], &launch).unwrap_err(),
            "Found 2 occurrences of the text in b.txt. The text must be unique. Please provide more context to make it unique."
        );
        assert_eq!(scratch.read("b.txt"), b"aaaa\n");
        scratch.file("c.txt", "\u{e9}\u{e9}\u{e9}\n".as_bytes());
        assert_eq!(
            change("c.txt", &[("\u{e9}\u{e9}", "X")], &launch).unwrap_err(),
            "Found 2 occurrences of the text in c.txt. The text must be unique. Please provide more context to make it unique."
        );
        assert_eq!(scratch.read("c.txt"), "\u{e9}\u{e9}\u{e9}\n".as_bytes());
    }

    #[test]
    fn edit_replaces_a_whitespace_only_target() {
        let scratch = Scratch::new("whitespace");
        let launch = context(&scratch.0, &scratch.0);
        scratch.file("tab.txt", b"if x:\n\tfoo()\n");
        assert_eq!(
            change("tab.txt", &[("\t", "    ")], &launch).unwrap(),
            "Successfully replaced 1 block(s) in tab.txt.\nedits[0]: line 2"
        );
        assert_eq!(scratch.read("tab.txt"), b"if x:\n    foo()\n");
        scratch.file("space.txt", b"foo bar\n");
        assert_eq!(
            change("space.txt", &[(" ", "_")], &launch).unwrap(),
            "Successfully replaced 1 block(s) in space.txt.\nedits[0]: line 1"
        );
        assert_eq!(scratch.read("space.txt"), b"foo_bar\n");
        scratch.file("indent.txt", b"if x:\n    foo()\n");
        assert_eq!(
            change("indent.txt", &[("    ", "\t")], &launch).unwrap(),
            "Successfully replaced 1 block(s) in indent.txt.\nedits[0]: line 2"
        );
        assert_eq!(scratch.read("indent.txt"), b"if x:\n\tfoo()\n");
        // A target the content does not hold writes nothing.
        scratch.file("none.txt", b"ab\n");
        assert_eq!(
            change("none.txt", &[(" ", "X")], &launch).unwrap_err(),
            "Could not find the exact text in none.txt. The old text must match exactly including all whitespace and newlines."
        );
        assert_eq!(scratch.read("none.txt"), b"ab\n");
    }

    #[test]
    fn edit_refuses_a_fuzzy_match_that_ends_inside_a_unit() {
        let scratch = Scratch::new("unit-end");
        let launch = context(&scratch.0, &scratch.0);
        // NFKC turns the ligature into two characters, so a match that ends between them names no
        // bytes of the content.
        scratch.file("a.txt", "\u{2019}\u{fb00}tail\n".as_bytes());
        assert_eq!(
            change("a.txt", &[("'f", "X")], &launch).unwrap_err(),
            "Could not find the exact text in a.txt. The old text must match exactly including all whitespace and newlines."
        );
        assert_eq!(scratch.read("a.txt"), "\u{2019}\u{fb00}tail\n".as_bytes());
        // A match that covers whole units names them all and replaces them.
        assert_eq!(
            change("a.txt", &[("'ff", "'ff")], &launch).unwrap(),
            "Successfully replaced 1 block(s) in a.txt.\nedits[0]: line 1 (fuzzy match)"
        );
        assert_eq!(scratch.read("a.txt"), b"'fftail\n");
    }

    #[test]
    fn edit_matches_a_line_end_that_lost_trailing_whitespace() {
        let scratch = Scratch::new("trimmed-line-end");
        let launch = context(&scratch.0, &scratch.0);
        // The match ends on the newline the trim kept, so the removed spaces stay outside it and
        // the edit after it keeps its line.
        scratch.file("a.txt", "it\u{2019}s   \nnext\nmore\n".as_bytes());
        assert_eq!(
            change("a.txt", &[("it's\n", "X"), ("more", "MORE")], &launch).unwrap(),
            "Successfully replaced 2 block(s) in a.txt.\nedits[0]: line 1 (fuzzy match)\nedits[1]: line 2"
        );
        assert_eq!(scratch.read("a.txt"), b"Xnext\nMORE\n");
    }

    #[test]
    fn edit_matches_from_a_line_end_that_lost_trailing_whitespace() {
        let scratch = Scratch::new("trimmed-line-start");
        let launch = context(&scratch.0, &scratch.0);
        // The match starts on the newline the trim kept, so the removed spaces stay before it.
        scratch.file("a.txt", "a   \n\u{201c}b\u{201d}".as_bytes());
        assert_eq!(
            change("a.txt", &[("\n\"b\"", "X")], &launch).unwrap(),
            "Successfully replaced 1 block(s) in a.txt.\nedits[0]: line 1 (fuzzy match)"
        );
        assert_eq!(scratch.read("a.txt"), b"a   X");
        // A match that keeps the newline keeps it where it was.
        scratch.file("b.txt", "a   \n\u{201c}b\u{201d}\n".as_bytes());
        assert_eq!(
            change("b.txt", &[("\n\"b\"", "\nB")], &launch).unwrap(),
            "Successfully replaced 1 block(s) in b.txt.\nedits[0]: line 1 (fuzzy match)"
        );
        assert_eq!(scratch.read("b.txt"), b"a   \nB\n");
    }

    #[test]
    fn edit_refuses_a_run_longer_than_a_unit_holds_promptly() {
        let scratch = Scratch::new("long-run");
        let launch = context(&scratch.0, &scratch.0);
        // A run of reordering non-starters longer than one unit may hold: the units no longer
        // reproduce the whole-file form, so the edit is refused instead of guessed.
        let content = format!("{}\nit\u{2019}s\n", "\u{0f73}".repeat(8192));
        scratch.file("a.txt", content.as_bytes());
        let started = Instant::now();
        assert_eq!(
            change("a.txt", &[("it's", "X")], &launch).unwrap_err(),
            "Could not find the exact text in a.txt. The old text must match exactly including all whitespace and newlines."
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
            "Successfully replaced 1 block(s) in a.txt.\nedits[0]: line 1"
        );
    }

    #[test]
    fn edit_matches_across_a_short_combining_run() {
        let scratch = Scratch::new("short-run");
        let launch = context(&scratch.0, &scratch.0);
        scratch.file("a.txt", "cafe\u{301} \u{201c}x\u{201d}\n".as_bytes());
        assert_eq!(
            change("a.txt", &[("caf\u{e9} \"x\"", "caf\u{e9} \"y\"")], &launch).unwrap(),
            "Successfully replaced 1 block(s) in a.txt.\nedits[0]: line 1 (fuzzy match)"
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
        // A cedilla between a base and an acute is below the acute's combining class, so the whole
        // run is one unit and an edit elsewhere in the file still matches.
        scratch.file(
            "a.txt",
            "author = O\u{2019}Reilly\nnote = a\u{327}\u{301}\n".as_bytes(),
        );
        assert_eq!(
            change("a.txt", &[("O'Reilly", "Publisher")], &launch).unwrap(),
            "Successfully replaced 1 block(s) in a.txt.\nedits[0]: line 1 (fuzzy match)"
        );
        assert_eq!(
            scratch.read("a.txt"),
            "author = Publisher\nnote = a\u{327}\u{301}\n".as_bytes()
        );
        // The run itself is one unit, so its own normalized form names it.
        scratch.file("b.txt", "note = a\u{327}\u{301}\n".as_bytes());
        assert_eq!(
            change("b.txt", &[("\u{e1}\u{327}", "X")], &launch).unwrap(),
            "Successfully replaced 1 block(s) in b.txt.\nedits[0]: line 1 (fuzzy match)"
        );
        assert_eq!(scratch.read("b.txt"), b"note = X\n");
        // The same for marks that reorder without composing, as in vocalized Arabic.
        scratch.file(
            "c.txt",
            "title = \u{201c}hello\u{201d}\nword = \u{628}\u{650}\u{651}\u{64e}\n".as_bytes(),
        );
        assert_eq!(
            change("c.txt", &[("\"hello\"", "\"world\"")], &launch).unwrap(),
            "Successfully replaced 1 block(s) in c.txt.\nedits[0]: line 1 (fuzzy match)"
        );
        assert_eq!(
            scratch.read("c.txt"),
            "title = \"world\"\nword = \u{628}\u{650}\u{651}\u{64e}\n".as_bytes()
        );
        scratch.file("d.txt", "word = \u{628}\u{650}\u{651}\u{64e}\n".as_bytes());
        assert_eq!(
            change("d.txt", &[("\u{628}\u{64e}\u{650}\u{651}", "X")], &launch).unwrap(),
            "Successfully replaced 1 block(s) in d.txt.\nedits[0]: line 1 (fuzzy match)"
        );
        assert_eq!(scratch.read("d.txt"), b"word = X\n");
    }

    #[test]
    fn edit_counts_overlapping_starts_in_one_pass() {
        let scratch = Scratch::new("overlap-cost");
        let launch = context(&scratch.0, &scratch.0);
        let content = "a".repeat(131_072);
        scratch.file("a.txt", content.as_bytes());
        let needle = "a".repeat(65_536);
        let started = Instant::now();
        assert_eq!(
            change("a.txt", &[(needle.as_str(), "X")], &launch).unwrap_err(),
            "Found 65537 occurrences of the text in a.txt. The text must be unique. Please provide more context to make it unique."
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
        scratch.file("a.txt", b"one\n\xfftwo\n");
        assert_eq!(
            change("a.txt", &[("two", "TWO")], &launch).unwrap_err(),
            "a.txt is not valid UTF-8. Edit it with exec."
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
        // 2000 lines of exactly 51200 bytes together, then the newline that takes the selected
        // content one byte over the budget.
        let mut text = String::new();
        for _ in 0..1999 {
            text.push_str(&"y".repeat(24));
            text.push('\n');
        }
        text.push_str(&"z".repeat(1225));
        text.push('\n');
        scratch.file("at-cap.txt", text.as_bytes());
        let path = scratch.path("at-cap.txt");
        let body = text.trim_end_matches('\n').to_owned();
        assert_eq!(
            whole(&path, &launch, false).unwrap(),
            format!("{body}\n\n[Showing lines 1-2000 of 2001. Use offset=2001 to continue.]")
        );
    }
    #[test]
    fn read_keeps_an_empty_line_when_a_carry_holds_a_later_character() {
        let scratch = Scratch::new("carry-skip");
        let launch = context(&scratch.0, &scratch.0);
        // The first byte of the last line's `é` is the final byte of the first 8192-byte read,
        // so the decoder holds it while the line before it is still empty: that empty line is
        // the selected window's last element, not a line of its own.
        let text = format!("{}\n{}{}tail", "x\n".repeat(2000), "a".repeat(4190), 'é');
        scratch.file("carry.txt", text.as_bytes());
        let path = scratch.path("carry.txt");
        assert_eq!(
            window(&path, None, Some(2001), &launch).unwrap(),
            format!(
                "{}\n\n[1 more lines in file. Use offset=2002 to continue.]",
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
                offset: None,
                limit: None,
            },
            &launch,
            false,
        )
        .unwrap();
        assert_eq!(outcome.text, "Read image file [image/png]");
        let (data, mime) = outcome.image.expect("the image travels with the text");
        assert_eq!(mime, "image/png");
        assert_eq!(
            data,
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &png)
        );
    }

    #[test]
    fn read_reports_an_animated_png_as_a_binary_file() {
        let scratch = Scratch::new("animated");
        let launch = context(&scratch.0, &scratch.0);
        let png = png_bytes(4, 4);
        let mut animated = png[..33].to_vec();
        animated.extend_from_slice(&[0, 0, 0, 8]);
        animated.extend_from_slice(b"acTL");
        animated.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 1]);
        animated.extend_from_slice(&[0, 0, 0, 0]);
        animated.extend_from_slice(&png[33..]);
        scratch.file("moving.png", &animated);
        let path = scratch.path("moving.png");
        let message = read_text(
            &ReadRequest {
                path,
                offset: None,
                limit: None,
            },
            &launch,
            false,
        )
        .err()
        .expect("an animated PNG is not an image a model reads");
        assert!(message.contains("is a binary file"), "{message}");
    }
}
