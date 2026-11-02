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
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use unicode_normalization::UnicodeNormalization;

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
    let metadata = std::fs::metadata(&resolved)
        .map_err(|error| format!("Cannot read {}: {error}.", request.path))?;
    if !metadata.is_file() {
        return Err(format!(
            "Cannot read {}: it is not a regular file.",
            request.path
        ));
    }
    let mut file =
        File::open(&resolved).map_err(|error| format!("Cannot read {}: {error}.", request.path))?;
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
    if let Some(parent) = resolved.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|error| format!("Cannot write {}: {error}.", request.path))?;
    }
    std::fs::write(&resolved, request.content.as_bytes())
        .map_err(|error| format!("Cannot write {}: {error}.", request.path))?;
    Ok(format!("Successfully wrote to {}", request.path))
}

/// One replacement matched against the content it is applied to.
struct Matched<'a> {
    /// The edit's position in the request, which names it in errors and in the result.
    index: usize,
    /// Where it matched, and how long the matched text is.
    at: usize,
    len: usize,
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
    let bytes = std::fs::read(&resolved)
        .map_err(|error| format!("Could not edit file: {}. {error}.", request.path))?;
    let raw = String::from_utf8(bytes)
        .map_err(|_| format!("{} is not valid UTF-8. Edit it with exec.", request.path))?;
    let (bom, content) = match raw.strip_prefix('\u{feff}') {
        Some(rest) => ("\u{feff}", rest),
        None => ("", raw.as_str()),
    };
    let ending = detect_line_ending(content);
    let normalized = normalize_to_lf(content);
    let (replaced, lines) = apply_edits(&normalized, request, &request.path)?;
    let written = format!("{bom}{}", restore_line_endings(&replaced, ending));
    std::fs::write(&resolved, written.as_bytes())
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

/// Applies the edits to LF-normalized content, and answers with the new content and each edit's
/// line in it, in the request's order.
fn apply_edits(
    content: &str,
    request: &EditRequest,
    path: &str,
) -> Result<(String, Vec<(usize, bool)>), String> {
    let total = request.edits.len();
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
    // Every edit is first looked for in the content as it is; one that needs the fuzzy form makes
    // the whole call work in that form, so no edit is matched against a half-changed file.
    let first: Vec<Found> = edits.iter().map(|(old, _)| find(content, old)).collect();
    let fuzzy = first.iter().any(|found| found.fuzzy);
    let base = if fuzzy {
        normalize_for_fuzzy_match(content)
    } else {
        content.to_owned()
    };
    let mut matched: Vec<Matched<'_>> = Vec::new();
    for (index, (old, new)) in edits.iter().enumerate() {
        let found = find(&base, old);
        let Some(at) = found.at else {
            return Err(not_found(path, index, total));
        };
        let count = occurrences(&base, old);
        if count > 1 {
            return Err(duplicate(path, index, total, count));
        }
        matched.push(Matched {
            index,
            at,
            len: found.len,
            new_text: new,
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
    let (replaced, starts) = if fuzzy {
        preserve_unchanged_lines(content, &base, &matched)?
    } else {
        replace(&base, &matched)
    };
    if replaced == content {
        return Err(no_change(path, total));
    }
    let mut lines = vec![(0usize, false); total];
    for edit in &matched {
        let at = starts[edit.index];
        lines[edit.index] = (
            1 + replaced[..at].matches('\n').count(),
            first[edit.index].fuzzy,
        );
    }
    Ok((replaced, lines))
}

/// One edit's place in the content it was looked for in.
struct Found {
    /// Where the text is, when it is there at all.
    at: Option<usize>,
    len: usize,
    /// Whether only the fuzzy form matched.
    fuzzy: bool,
}

/// Finds `old` in `content`: as it is first, then in the fuzzy form.
fn find(content: &str, old: &str) -> Found {
    if let Some(at) = content.find(old) {
        return Found {
            at: Some(at),
            len: old.len(),
            fuzzy: false,
        };
    }
    let fuzzy_content = normalize_for_fuzzy_match(content);
    let fuzzy_old = normalize_for_fuzzy_match(old);
    Found {
        at: fuzzy_content.find(&fuzzy_old),
        len: fuzzy_old.len(),
        fuzzy: true,
    }
}

/// How often `old` occurs in `content`, counted in the fuzzy form so a near copy is ambiguous too.
fn occurrences(content: &str, old: &str) -> usize {
    let fuzzy_content = normalize_for_fuzzy_match(content);
    let fuzzy_old = normalize_for_fuzzy_match(old);
    if fuzzy_old.is_empty() {
        // A needle that normalizes to nothing splits the content into its code units, so any
        // content of two units or more is as ambiguous as a repeated needle.
        return fuzzy_content.encode_utf16().count().saturating_sub(1);
    }
    fuzzy_content.match_indices(&fuzzy_old).count()
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

/// Applies matches to `base`, answering with the new content and each edit's start in it.
fn replace(base: &str, matched: &[Matched<'_>]) -> (String, Vec<usize>) {
    let mut starts = vec![0usize; matched.len()];
    let mut shift: i64 = 0;
    for edit in matched {
        starts[edit.index] = (edit.at as i64 + shift) as usize;
        shift += edit.new_text.len() as i64 - edit.len as i64;
    }
    let mut result = base.to_owned();
    for edit in matched.iter().rev() {
        result.replace_range(edit.at..edit.at + edit.len, edit.new_text);
    }
    (result, starts)
}

/// A run of lines one or more replacements touch, and those replacements.
struct Group<'a> {
    start: usize,
    end: usize,
    members: Vec<&'a Matched<'a>>,
}

/// Applies matches found in the fuzzy form of `original` while every line they do not touch is
/// copied from `original` byte for byte.
fn preserve_unchanged_lines(
    original: &str,
    base: &str,
    matched: &[Matched<'_>],
) -> Result<(String, Vec<usize>), String> {
    let original_lines = split_lines_with_endings(original);
    let spans = line_spans(base);
    if original_lines.len() != spans.len() {
        return Err(
            "Cannot preserve unchanged lines because the base content has a different line count."
                .to_owned(),
        );
    }
    let mut groups: Vec<Group<'_>> = Vec::new();
    for edit in matched {
        let (start, end) = line_range(&spans, edit.at, edit.len)?;
        if let Some(current) = groups.last_mut()
            && start < current.end
        {
            current.end = current.end.max(end);
            current.members.push(edit);
            continue;
        }
        groups.push(Group {
            start,
            end,
            members: vec![edit],
        });
    }
    let mut starts = vec![0usize; matched.len()];
    let mut result = String::new();
    let mut line = 0;
    for group in &groups {
        for untouched in &original_lines[line..group.start] {
            result.push_str(untouched);
        }
        let from = spans[group.start].0;
        let to = spans[group.end - 1].1;
        let group_start = result.len();
        let mut shift: i64 = 0;
        for edit in &group.members {
            starts[edit.index] = (group_start as i64 + (edit.at - from) as i64 + shift) as usize;
            shift += edit.new_text.len() as i64 - edit.len as i64;
        }
        let mut text = base[from..to].to_owned();
        for edit in group.members.iter().rev() {
            let at = edit.at - from;
            text.replace_range(at..at + edit.len, edit.new_text);
        }
        result.push_str(&text);
        line = group.end;
    }
    for untouched in &original_lines[line..] {
        result.push_str(untouched);
    }
    Ok((result, starts))
}

/// The lines of `content` with their endings, and no line after a final `\n`.
fn split_lines_with_endings(content: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    while start < content.len() {
        match content[start..].find('\n') {
            Some(at) => {
                let end = start + at + 1;
                lines.push(&content[start..end]);
                start = end;
            }
            None => {
                lines.push(&content[start..]);
                start = content.len();
            }
        }
    }
    lines
}

/// Where each line of `content` starts and ends.
fn line_spans(content: &str) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut start = 0;
    while start < content.len() {
        let end = match content[start..].find('\n') {
            Some(at) => start + at + 1,
            None => content.len(),
        };
        spans.push((start, end));
        start = end;
    }
    spans
}

/// The whole lines a replacement touches.
fn line_range(spans: &[(usize, usize)], at: usize, len: usize) -> Result<(usize, usize), String> {
    let end = at + len;
    let outside = || "Replacement range is outside the base content.".to_owned();
    let start = spans
        .iter()
        .position(|(from, to)| at >= *from && at < *to)
        .ok_or_else(outside)?;
    let mut last = start;
    while last < spans.len() && spans[last].1 < end {
        last += 1;
    }
    if last >= spans.len() {
        return Err(outside());
    }
    Ok((start, last + 1))
}

/// The line ending the content uses: CRLF only when its first `\n` follows a `\r`.
fn detect_line_ending(content: &str) -> &'static str {
    let Some(lf) = content.find('\n') else {
        return "\n";
    };
    match content.find("\r\n") {
        Some(crlf) if crlf < lf => "\r\n",
        _ => "\n",
    }
}

fn normalize_to_lf(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

fn restore_line_endings(text: &str, ending: &str) -> String {
    if ending == "\r\n" {
        text.replace('\n', "\r\n")
    } else {
        text.to_owned()
    }
}

/// The form matching falls back to: NFKC, no trailing whitespace, and ASCII quotes, dashes and
/// spaces, so text pasted from a document still finds its source.
fn normalize_for_fuzzy_match(text: &str) -> String {
    let normalized: String = text.nfkc().collect();
    normalized
        .split('\n')
        .map(|line| line.trim_end_matches(is_js_whitespace))
        .collect::<Vec<_>>()
        .join("\n")
        .chars()
        .map(|character| match character {
            '\u{2018}' | '\u{2019}' | '\u{201a}' | '\u{201b}' => '\'',
            '\u{201c}' | '\u{201d}' | '\u{201e}' | '\u{201f}' => '"',
            '\u{2010}'..='\u{2015}' | '\u{2212}' => '-',
            '\u{00a0}' | '\u{2002}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}' => ' ',
            other => other,
        })
        .collect()
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
    fn edit_counts_a_needle_that_normalizes_to_nothing() {
        let scratch = Scratch::new("blank-needle");
        let launch = context(&scratch.0, &scratch.0);
        scratch.file("a.txt", b"a b");
        assert_eq!(
            change("a.txt", &[(" ", "X")], &launch).unwrap_err(),
            "Found 2 occurrences of the text in a.txt. The text must be unique. Please provide more context to make it unique."
        );
        // A non-BMP character is two code units, so two of them are three units apart.
        scratch.file("b.txt", "\u{1f600}\u{1f600}".as_bytes());
        assert_eq!(
            change("b.txt", &[("\u{3000}", "X")], &launch).unwrap_err(),
            "Found 3 occurrences of the text in b.txt. The text must be unique. Please provide more context to make it unique."
        );
        assert_eq!(scratch.read("a.txt"), b"a b");
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
    fn edit_matches_a_typographic_copy_and_leaves_other_lines_alone() {
        let scratch = Scratch::new("fuzzy");
        let launch = context(&scratch.0, &scratch.0);
        scratch.file("a.txt", "it\u{2019}s here\nuntouched   \n".as_bytes());
        assert_eq!(
            change("a.txt", &[("it's here", "it's fine")], &launch).unwrap(),
            "Successfully replaced 1 block(s) in a.txt.\nedits[0]: line 1 (fuzzy match)"
        );
        // The touched line is rewritten from the normalized form; the other keeps its bytes.
        assert_eq!(scratch.read("a.txt"), b"it's fine\nuntouched   \n");
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
