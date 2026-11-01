//! Model-facing output: streaming terminal cleaning and byte-budgeted line windows.
//!
//! The ANSI CSI/OSC state machine follows FastCtx `src/shell/normalize.rs` and the head/tail
//! split follows FastCtx `src/shell/output.rs` (Apache-2.0, Copyright 2026 yc-duan), modified for fastexec.

use encoding_rs::Encoding;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Bytes kept per line in bounded windows; the rest of an oversized line is counted, then cut.
const LINE_BYTES_LIMIT: usize = 64 * 1024;
/// Characters kept per cleaned line in bounded windows. `Truncate::None` keeps whole lines.
const LINE_CHARS_LIMIT: usize = 2000;

#[derive(
    Clone, Copy, Debug, Default, Deserialize, Serialize, schemars::JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum Truncate {
    /// First lines and last lines with the middle omitted.
    #[default]
    HeadTail,
    /// First lines only.
    Head,
    /// Last lines only.
    Tail,
    /// Every unseen line whole, up to 4 MiB; the rest is reported as an omitted range.
    None,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum EscState {
    #[default]
    Text,
    Esc,
    Csi,
    Osc,
    OscEsc,
}

/// Per-task cleaning state carried between windows so sequences split across reads stay intact.
#[derive(Clone, Debug, Default)]
pub struct Cleaner {
    escape: EscState,
    pending_cr: bool,
    /// A CR ended the previous window's last line; a leading LF only completes that CRLF.
    swallow_lf: bool,
    /// The previous window ended inside a line; an empty remainder of it is not shown again.
    continued: bool,
}

/// What one output byte does to the cleaned line, after clearing it when `Cleaner::step` says so.
enum Step {
    Nothing,
    /// The byte is text.
    Append,
    /// A control byte: it starts the line without adding text.
    Control,
    EndLine,
    /// The LF completing a CRLF whose line the previous window already showed.
    SwallowedLf,
}

impl Cleaner {
    /// Advances the ANSI CSI/OSC parser; returns true when `byte` belongs to a sequence.
    fn escape(&mut self, byte: u8) -> bool {
        self.escape = match (self.escape, byte) {
            (EscState::Text, 0x1b) => EscState::Esc,
            (EscState::Text, _) => return false,
            (EscState::Esc, b'[') => EscState::Csi,
            (EscState::Esc, b']') => EscState::Osc,
            (EscState::Esc, _) | (EscState::Csi, 0x40..=0x7e) => EscState::Text,
            (EscState::Csi, _) => EscState::Csi,
            (EscState::Osc, 0x07) | (EscState::OscEsc, b'\\') => EscState::Text,
            (EscState::Osc, 0x1b) | (EscState::OscEsc, 0x1b) => EscState::OscEsc,
            (EscState::Osc, _) | (EscState::OscEsc, _) => EscState::Osc,
        };
        true
    }

    /// Cleans one byte: returns whether the line is cleared first, and what the byte does.
    fn step(&mut self, byte: u8) -> (bool, Step) {
        if std::mem::take(&mut self.swallow_lf) && byte == b'\n' {
            return (false, Step::SwallowedLf);
        }
        let mut clear = false;
        if self.escape == EscState::Csi && matches!(byte, b'K' | b'J') && self.pending_cr {
            // An erase rewrites the line; SGR, OSC and cursor visibility leave its text intact.
            self.pending_cr = false;
            clear = true;
        }
        if self.escape(byte) {
            return (clear, Step::Nothing);
        }
        if std::mem::take(&mut self.pending_cr) {
            if byte == b'\n' {
                return (false, Step::EndLine);
            }
            // A Unix PTY sends `\r\n` as `\r\r\n`: a CR after a CR moves nothing.
            if byte == b'\r' {
                self.pending_cr = true;
                return (false, Step::Nothing);
            }
            // A lone CR returns the cursor to column 0: the text that follows replaces the line.
            clear = true;
        }
        let step = match byte {
            b'\n' => Step::EndLine,
            b'\r' => {
                self.pending_cr = true;
                Step::Control
            }
            b'\t' => Step::Append,
            0x00..=0x1f | 0x7f => Step::Control,
            _ => Step::Append,
        };
        (clear, step)
    }
}

/// Finds literal texts in output cleaned as windows clean it, fed in pieces of any size. Every
/// state a cleaned line passes through counts, before a carriage return rewrites it and before
/// trailing spaces are trimmed, so how the output is split never changes the answer.
pub struct Matcher {
    /// The texts sought, any of which ends the search.
    needles: Vec<String>,
    /// The lowest index of a needle found so far.
    best: Option<usize>,
    /// The longest needle, in bytes.
    longest: usize,
    encoding: &'static Encoding,
    cleaner: Cleaner,
    decoder: encoding_rs::Decoder,
    /// The decoded end of the current line, long enough to hold a match that ends later.
    line: String,
    /// Text bytes of the current line not decoded yet.
    pending: Vec<u8>,
}

impl Matcher {
    /// A matcher that continues from `cleaner`, the cleaning state where its output starts.
    pub fn new(needles: &[String], encoding: &'static Encoding, cleaner: Cleaner) -> Self {
        Self {
            needles: needles.to_vec(),
            best: None,
            longest: needles.iter().map(String::len).max().unwrap_or(0),
            encoding,
            cleaner,
            decoder: encoding.new_decoder_without_bom_handling(),
            line: String::new(),
            pending: Vec::new(),
        }
    }

    /// Feeds the next output bytes; returns the lowest index of a needle the output fed so far
    /// contains. Reading stops early only at the first needle, which nothing can precede.
    pub fn push(&mut self, bytes: &[u8]) -> Option<usize> {
        for &byte in bytes {
            let (clear, step) = self.cleaner.step(byte);
            if clear {
                self.found(true);
                self.new_line();
            }
            match step {
                Step::Append => self.pending.push(byte),
                Step::EndLine => {
                    self.found(true);
                    self.new_line();
                }
                Step::Nothing | Step::Control | Step::SwallowedLf => {}
            }
            if self.best == Some(0) {
                return self.best;
            }
        }
        self.found(false);
        self.best
    }

    /// The lowest index of a needle the output fed so far contains.
    pub fn best(&self) -> Option<usize> {
        self.best
    }

    /// Ends the output: an unfinished last line ends as a window shows it. Returns the lowest
    /// needle found.
    pub fn finish(&mut self) -> Option<usize> {
        self.found(true);
        self.new_line();
        self.best
    }

    fn new_line(&mut self) {
        self.line.clear();
        self.pending.clear();
        self.decoder = self.encoding.new_decoder_without_bom_handling();
    }

    /// Decodes the pending bytes and searches the line, then keeps only the end of the line
    /// that a later match could start in. `last` ends the line's text, so an unfinished
    /// character decodes as the replacement character a window shows.
    fn found(&mut self, last: bool) {
        let pending = std::mem::take(&mut self.pending);
        if let Some(room) = self.decoder.max_utf8_buffer_length(pending.len()) {
            self.line.reserve(room);
        }
        let _ = self
            .decoder
            .decode_to_string(&pending, &mut self.line, last);
        // Only needles before the best one found can improve it.
        let before = self.best.unwrap_or(self.needles.len());
        if let Some(found) = self.needles[..before]
            .iter()
            .position(|needle| self.line.contains(needle.as_str()))
        {
            self.best = Some(found);
        }
        let mut cut = self.line.len().saturating_sub(self.longest);
        while !self.line.is_char_boundary(cut) {
            cut -= 1;
        }
        self.line.drain(..cut);
    }
}

/// One line ready for the window, with its 1-based log line number.
#[derive(Clone)]
struct Line {
    number: u64,
    text: String,
    /// The line lost part of its text to a per-line limit or the budget.
    cut: bool,
}

pub struct WindowSpec {
    pub truncate: Truncate,
    /// Byte budget for the window body.
    pub budget: usize,
    pub raw: bool,
    pub encoding: &'static Encoding,
    /// What the line numbers count, named in the omission marker: "log" or "transcript".
    pub source: &'static str,
}

pub struct Window {
    /// Candidate lines retain their source numbers so final fitting can account for omissions.
    lines: Vec<Line>,
    truncate: Truncate,
    source: &'static str,
    /// Inclusive line range left out of the window.
    pub omitted: Option<(u64, u64)>,
    /// Lines that held byte sequences invalid in the selected encoding.
    pub bad_lines: u64,
    /// LF bytes consumed, for the caller's line cursor.
    pub newlines: u64,
}

impl Window {
    fn marker(&self, omitted: Option<(u64, u64)>) -> String {
        omitted.map_or(String::new(), |(first, last)| {
            let count = last - first + 1;
            format!(
                "... [{count} lines omitted: {} lines {first}-{last}] ...",
                self.source
            )
        })
    }

    fn render(&self, marker: &str) -> (String, u64, Option<(u64, u64)>) {
        let mut lines: Vec<&str> = self.lines.iter().map(|line| line.text.as_str()).collect();
        if let Some((first, _)) = self.omitted.filter(|_| !marker.is_empty()) {
            let at = self.lines.partition_point(|line| line.number < first);
            lines.insert(at, marker);
        }
        let cuts = self.lines.iter().filter(|line| line.cut).count() as u64;
        (lines.join("\n"), cuts, self.omitted)
    }

    /// Fits the final body using the requested ends, returning its actual omitted range.
    pub fn fit(&self, room: usize) -> (String, u64, Option<(u64, u64)>) {
        let full = self.render(&self.marker(self.omitted));
        if full.0.len() <= room {
            return full;
        }
        let mut cleaner = Cleaner::default();
        let mut builder = WindowBuilder::new(
            WindowSpec {
                truncate: if self.truncate == Truncate::None {
                    Truncate::Head
                } else {
                    self.truncate
                },
                budget: 0,
                raw: true,
                encoding: encoding_rs::UTF_8,
                source: self.source,
            },
            &mut cleaner,
            1,
        );
        builder.omitted = self.omitted;
        // Size the full marker conservatively from the entire range, then prefer actual
        // endpoint text over a marker when the body is too small to carry both.
        let mut all = self.omitted;
        for line in &self.lines {
            merge_omitted(&mut all, line.number, line.number);
        }
        let widest = self.marker(all);
        let marker_limit = widest.len()
            + all.map_or(0, |(first, last)| {
                last.to_string().len() - first.to_string().len()
            });
        let first = self.lines.first().map_or(0, |line| line.text.len() + 1);
        let last = self.lines.last().map_or(0, |line| line.text.len() + 1);
        let content = match self.truncate {
            Truncate::Head | Truncate::None => first,
            Truncate::Tail => last,
            Truncate::HeadTail if self.lines.len() > 1 => first + last,
            Truncate::HeadTail => first,
        };
        let marker_room = if room >= marker_limit.saturating_add(content) {
            marker_limit
        } else if room >= 3_usize.saturating_add(content) {
            3
        } else {
            0
        };
        builder.spec.budget = if marker_room == 0 {
            room.saturating_add(1)
        } else {
            room - marker_room
        };
        if room == 0 {
            builder.omitted = all;
        } else {
            for line in &self.lines {
                builder.place(line.clone());
            }
        }
        let fitted = builder.finish();
        let marker = if marker_room == 0 {
            String::new()
        } else if marker_room == 3 {
            "...".to_owned()
        } else {
            fitted.marker(fitted.omitted)
        };
        fitted.render(&marker)
    }
}

fn merge_omitted(range: &mut Option<(u64, u64)>, first: u64, last: u64) {
    *range = Some(range.map_or((first, last), |(old_first, old_last)| {
        (old_first.min(first), old_last.max(last))
    }));
}

/// Turns a byte range of the log into a bounded window. Feed chunks, then call `finish`.
pub struct WindowBuilder<'a> {
    spec: WindowSpec,
    cleaner: &'a mut Cleaner,
    first_line: u64,
    newlines: u64,
    line: Vec<u8>,
    line_overflow: usize,
    line_started: bool,
    head: Vec<Line>,
    head_bytes: usize,
    head_open: bool,
    tail: VecDeque<Line>,
    tail_bytes: usize,
    omitted: Option<(u64, u64)>,
    bad_lines: u64,
}

impl<'a> WindowBuilder<'a> {
    /// `first_line` is the log line number at which the byte range starts.
    pub fn new(spec: WindowSpec, cleaner: &'a mut Cleaner, first_line: u64) -> Self {
        Self {
            spec,
            cleaner,
            first_line,
            newlines: 0,
            line: Vec::new(),
            line_overflow: 0,
            line_started: false,
            head: Vec::new(),
            head_bytes: 0,
            head_open: true,
            tail: VecDeque::new(),
            tail_bytes: 0,
            omitted: None,
            bad_lines: 0,
        }
    }

    pub fn push(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            if self.spec.raw {
                self.raw_byte(byte);
            } else {
                self.clean_byte(byte);
            }
        }
    }

    fn raw_byte(&mut self, byte: u8) {
        // The parser still advances so a later cleaned window starts in the right state.
        self.cleaner.escape(byte);
        if byte == b'\n' {
            self.end_line(true);
        } else {
            self.append(byte);
        }
    }

    fn clean_byte(&mut self, byte: u8) {
        let (clear, step) = self.cleaner.step(byte);
        if clear {
            self.line.clear();
            self.line_overflow = 0;
        }
        match step {
            Step::Nothing => {}
            Step::Append => self.append(byte),
            Step::Control => self.line_started = true,
            Step::EndLine => self.end_line(true),
            Step::SwallowedLf => self.newlines += 1,
        }
    }

    /// The bytes one line may keep: 64 KiB, or a `Truncate::None` window's whole cap.
    fn line_limit(&self) -> usize {
        match self.spec.truncate {
            Truncate::None => self.spec.budget,
            _ => LINE_BYTES_LIMIT,
        }
    }

    fn append(&mut self, byte: u8) {
        self.line_started = true;
        if self.line.len() < self.line_limit() {
            self.line.push(byte);
        } else {
            self.line_overflow += 1;
        }
    }

    fn end_line(&mut self, terminated: bool) {
        let number = self.first_line + self.newlines;
        if terminated {
            self.newlines += 1;
        }
        let mut bytes = std::mem::take(&mut self.line);
        let overflow = std::mem::take(&mut self.line_overflow);
        self.line_started = false;
        if std::mem::take(&mut self.cleaner.continued) && bytes.is_empty() && overflow == 0 {
            return;
        }
        let mut dropped = overflow;
        if overflow > 0 {
            // A clipped prefix can end inside a character, and decoding it would report an
            // error the output does not have; only that unfinished tail is dropped. Bytes
            // that are genuinely invalid elsewhere stay, so their error still shows.
            let suffix = if self.spec.encoding == encoding_rs::UTF_8 {
                incomplete_utf8_suffix(&bytes)
            } else {
                incomplete_legacy_suffix(&bytes, self.spec.encoding)
            };
            if suffix > 0 {
                bytes.truncate(bytes.len() - suffix);
                dropped += suffix;
            }
        }
        let (decoded, had_errors) = self.spec.encoding.decode_without_bom_handling(&bytes);
        if had_errors {
            self.bad_lines += 1;
        }
        let mut text = decoded.into_owned();
        let limit = self.line_limit();
        let mut cut = dropped > 0;
        if !self.spec.raw {
            // Terminals (ConPTY in particular) pad lines with spaces.
            text.truncate(text.trim_end_matches(' ').len());
        }
        if !self.spec.raw && self.spec.truncate != Truncate::None {
            let chars = text.chars().count();
            if chars > LINE_CHARS_LIMIT {
                cut = true;
                let cut = text
                    .char_indices()
                    .nth(LINE_CHARS_LIMIT)
                    .map_or(text.len(), |(i, _)| i);
                text.truncate(cut);
                text.push_str(&format!(
                    " … [line cut at {LINE_CHARS_LIMIT} of {chars} chars]"
                ));
            }
        }
        // Decoded text can be longer than its source bytes, so a `None` window's cap is
        // enforced on the text too, on a character boundary.
        if self.spec.truncate == Truncate::None && text.len() > limit {
            let mut end = limit;
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            dropped += text.len() - end;
            text.truncate(end);
            cut = true;
        }
        if dropped > 0 {
            text.push_str(&format!(
                " … [line cut at {}; {dropped} more bytes]",
                brief_bytes(limit)
            ));
        }
        self.place(Line { number, text, cut });
    }

    fn place(&mut self, mut line: Line) {
        let (head_budget, tail_budget) = match self.spec.truncate {
            // A `None` window stores whole lines up to its cap and reports the rest as omitted,
            // so nothing is buffered past it.
            Truncate::None => (self.line_limit(), 0),
            Truncate::Head => (self.spec.budget, 0),
            Truncate::Tail => (0, self.spec.budget),
            Truncate::HeadTail => {
                // Keep both ends at small budgets, instead of spending less than one line on
                // the head even when the first and last lines fit together.
                let first = self.head.first().unwrap_or(&line).text.len() + 1;
                let head = (self.spec.budget / 10).max(first.min(self.spec.budget / 2));
                (head, self.spec.budget - head)
            }
        };
        let cost = line.text.len() + 1;
        if self.head_open {
            if self.head_bytes + cost <= head_budget {
                self.head_bytes += cost;
                self.head.push(line);
                return;
            }
            self.head_open = false;
            // The first line of a bounded window is shown cut rather than left out, so a single
            // line longer than the whole budget still shows its beginning.
            if self.head.is_empty() && head_budget > 1 && self.spec.truncate != Truncate::Tail {
                // A `None` line is already bounded by the cap, with the note that says so.
                if self.spec.truncate != Truncate::None {
                    line.cut |= cut_to(&mut line.text, head_budget);
                }
                if line.cut && line.text.is_empty() {
                    merge_omitted(&mut self.omitted, line.number, line.number);
                    return;
                }
                self.head_bytes += line.text.len() + 1;
                self.head.push(line);
                return;
            }
        }
        if line.text.len() + 1 > tail_budget && tail_budget > 0 {
            line.cut |= cut_to(&mut line.text, tail_budget);
        }
        if line.cut && line.text.is_empty() {
            merge_omitted(&mut self.omitted, line.number, line.number);
            return;
        }
        self.tail_bytes += line.text.len() + 1;
        self.tail.push_back(line);
        while self.tail_bytes > tail_budget {
            let Some(dropped) = self.tail.pop_front() else {
                break;
            };
            self.tail_bytes -= dropped.text.len() + 1;
            merge_omitted(&mut self.omitted, dropped.number, dropped.number);
        }
    }

    pub fn finish(mut self) -> Window {
        if self.line_started || !self.line.is_empty() {
            // The unterminated last line is shown now; a trailing CR may still pair with a LF.
            if self.cleaner.pending_cr {
                self.cleaner.pending_cr = false;
                self.cleaner.swallow_lf = true;
            }
            self.end_line(false);
            self.cleaner.continued = true;
        }
        let mut lines = self.head;
        lines.extend(self.tail);
        Window {
            lines,
            truncate: self.spec.truncate,
            source: self.spec.source,
            omitted: self.omitted,
            bad_lines: self.bad_lines,
            newlines: self.newlines,
        }
    }
}

/// A byte count for a note: `64 KiB`, or `999 bytes` below a kibibyte.
fn brief_bytes(bytes: usize) -> String {
    if bytes >= 1024 && bytes.is_multiple_of(1024) {
        format!("{} KiB", bytes >> 10)
    } else {
        format!("{bytes} bytes")
    }
}

/// Shortens `text` to at most `budget - 1` bytes, with a cut marker when it fits, on a char
/// boundary. Returns whether it cut anything.
pub fn cut_to(text: &mut String, budget: usize) -> bool {
    if text.len() < budget {
        return false;
    }
    let limit = budget.saturating_sub(1);
    if limit < 2 * MARK.len() {
        // Too little room for a marker: keep only what fits.
        let mut end = limit;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        return true;
    }
    let keep = limit - MARK.len();
    let mut end = keep;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text.push_str(MARK);
    true
}

/// What a cut line ends with, so a reader knows the rest is elsewhere.
pub const MARK: &str = " … [cut]";

/// Length of an incomplete UTF-8 sequence at the end of `tail` (the last 1..=3 bytes).
pub fn incomplete_utf8_suffix(tail: &[u8]) -> usize {
    for back in 1..=tail.len().min(3) {
        let byte = tail[tail.len() - back];
        if byte & 0xc0 == 0x80 {
            continue; // continuation byte; keep looking for the lead byte
        }
        let needed = match byte {
            0xc0..=0xdf => 2,
            0xe0..=0xef => 3,
            0xf0..=0xf7 => 4,
            _ => return 0,
        };
        return if back < needed { back } else { 0 };
    }
    0
}

/// Length of an unfinished `encoding` character at the end of `line`, which starts on a
/// character boundary: the 1..=3 trailing bytes whose removal lets the rest decode cleanly.
/// A line with an invalid sequence elsewhere reports 0, so its replacement shows at once.
pub fn incomplete_legacy_suffix(line: &[u8], encoding: &'static encoding_rs::Encoding) -> usize {
    let decodes = |bytes: &[u8]| {
        encoding
            .decode_without_bom_handling_and_without_replacement(bytes)
            .is_some()
    };
    if decodes(line) {
        return 0;
    }
    (1..=line.len().min(3))
        .find(|&back| decodes(&line[..line.len() - back]))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A budget the fixtures do not mean to bound a `Truncate::None` window with.
    const UNBOUNDED: usize = usize::MAX;

    fn text(window: &Window) -> String {
        window.fit(usize::MAX).0
    }

    fn window(chunks: &[&[u8]], truncate: Truncate, budget: usize, raw: bool) -> Window {
        let mut cleaner = Cleaner::default();
        let spec = WindowSpec {
            truncate,
            budget,
            raw,
            encoding: encoding_rs::UTF_8,
            source: "log",
        };
        let mut builder = WindowBuilder::new(spec, &mut cleaner, 1);
        for chunk in chunks {
            builder.push(chunk);
        }
        builder.finish()
    }

    #[test]
    fn cleaning_strips_escapes_and_keeps_text_after_the_last_carriage_return() {
        let out = window(
            &[
                b"\x1b[32mok\x1b[0m\r\r\n",
                b"10%\r50%\r\x1b[",
                b"2K100%\n",
                b"\x1b]0;title\x07done",
            ],
            Truncate::None,
            UNBOUNDED,
            false,
        );
        assert_eq!(text(&out), "ok\n100%\ndone");
        assert_eq!(out.newlines, 2);
    }

    #[test]
    fn carriage_returns_survive_non_erasing_escapes() {
        for (input, expected) in [
            (&b"kept\r\x1b[0m\nNEXT\n"[..], "kept\nNEXT"),
            (&b"kept\r\x1b[?25l\x1b[?25h\n"[..], "kept"),
            (&b"kept\r\x1b]0;title\x07\n"[..], "kept"),
            (&b"old\r\x1b[32mnew\n"[..], "new"),
            (&b"old\r\x1b[2K\n"[..], ""),
        ] {
            for chunks in [vec![input], input.chunks(1).collect()] {
                let out = window(&chunks, Truncate::None, UNBOUNDED, false);
                assert_eq!(text(&out), expected, "{input:?}");
            }
        }
    }

    #[test]
    fn utf8_split_across_chunks_decodes_cleanly() {
        let input = "中文😀\n".as_bytes();
        let out = window(
            &[&input[..2], &input[2..5], &input[5..]],
            Truncate::None,
            UNBOUNDED,
            false,
        );
        assert_eq!(text(&out), "中文😀");
        assert_eq!(out.bad_lines, 0);
    }

    #[test]
    fn raw_mode_keeps_escapes_and_carriage_returns() {
        let out = window(&[b"\x1b[1mA\r\nB\rC\n"], Truncate::None, UNBOUNDED, true);
        assert_eq!(text(&out), "\x1b[1mA\r\nB\rC");
        // A raw window that ends inside a sequence leaves a cleaned window parsing it.
        let mut cleaner = Cleaner::default();
        let spec = |raw| WindowSpec {
            truncate: Truncate::None,
            budget: UNBOUNDED,
            raw,
            encoding: encoding_rs::UTF_8,
            source: "log",
        };
        let mut first = WindowBuilder::new(spec(true), &mut cleaner, 1);
        first.push(b"x\x1b[");
        first.finish();
        let mut second = WindowBuilder::new(spec(false), &mut cleaner, 1);
        second.push(b"31mRED\n");
        assert_eq!(text(&second.finish()), "RED");
    }

    #[test]
    fn windows_respect_the_budget_and_name_the_omitted_log_lines() {
        let input: String = (1..=100).map(|n| format!("line{n:03}\n")).collect();
        for (mode, budget) in [
            (Truncate::HeadTail, 200),
            (Truncate::Head, 200),
            (Truncate::Tail, 200),
        ] {
            let out = window(&[input.as_bytes()], mode, budget, false);
            let (first, last) = out.omitted.expect("lines must be omitted");
            let rendered = text(&out);
            let shown: Vec<&str> = rendered.lines().filter(|l| l.starts_with("line")).collect();
            assert!(
                shown.iter().map(|l| l.len() + 1).sum::<usize>() <= budget,
                "{mode:?}"
            );
            assert_eq!(shown.len() as u64 + (last - first + 1), 100, "{mode:?}");
            match mode {
                Truncate::Head => assert_eq!((shown[0], last), ("line001", 100)),
                Truncate::Tail => assert_eq!((first, *shown.last().unwrap()), (1, "line100")),
                _ => assert_eq!((shown[0], *shown.last().unwrap()), ("line001", "line100")),
            }
            assert!(text(&out).contains(&format!("log lines {first}-{last}")));
        }
    }

    #[test]
    fn lines_split_between_windows_yield_no_phantom_lines() {
        let mut cleaner = Cleaner::default();
        let spec = || WindowSpec {
            truncate: Truncate::None,
            budget: UNBOUNDED,
            raw: false,
            encoding: encoding_rs::UTF_8,
            source: "log",
        };
        let mut first = WindowBuilder::new(spec(), &mut cleaner, 1);
        first.push(b"Password:\r");
        assert_eq!(text(&first.finish()), "Password:");
        let mut second = WindowBuilder::new(spec(), &mut cleaner, 1);
        second.push(b"\nnext\n");
        let out = second.finish();
        assert_eq!(text(&out), "next");
        assert_eq!(out.newlines, 2);
    }

    #[test]
    fn matcher_finds_text_in_cleaned_output_however_it_is_split() {
        let utf8 = encoding_rs::UTF_8;
        // Output, needles, encoding, and the needle found.
        type Case<'a> = (&'a [u8], &'a [&'a str], &'static Encoding, Option<usize>);
        let cases: [Case; 9] = [
            (
                b"\x1b[32mServer\x1b[0m listening\r\n",
                &["Server listening"],
                utf8,
                Some(0),
            ),
            // Text a carriage return rewrites still counts.
            (b"10%\r50%\r100%\r\x1b[Kdone", &["100%"], utf8, Some(0)),
            (b"Password: ", &["Password: "], utf8, Some(0)),
            (b"SERVER READY", &["server ready"], utf8, None),
            // Any needle ends the search; the line keeps enough text for the longest one.
            (
                b"boot\nerror: port in use",
                &["ready", "error: port"],
                utf8,
                Some(1),
            ),
            (
                b"\xd6\xd0\xce\xc4 ok",
                &["\u{4e2d}\u{6587} ok"],
                encoding_rs::GBK,
                Some(0),
            ),
            (b"ab\ncd", &["bc"], utf8, None),
            // A line ending inside a character shows, and matches, the replacement character.
            (b"\xe2\n", &["\u{fffd}"], utf8, Some(0)),
            (b"\xe2", &["\u{fffd}"], utf8, Some(0)),
        ];
        for (output, needles, encoding, expected) in cases {
            let needles: Vec<String> = needles.iter().map(|needle| needle.to_string()).collect();
            let matcher = || Matcher::new(&needles, encoding, Cleaner::default());
            for split in 0..=output.len() {
                let mut two = matcher();
                let found = two
                    .push(&output[..split])
                    .or_else(|| two.push(&output[split..]))
                    .or_else(|| two.finish());
                assert_eq!(found, expected, "{needles:?} split at {split}");
            }
            let mut bytes = matcher();
            let found = output
                .iter()
                .find_map(|byte| bytes.push(&[*byte]))
                .or_else(|| bytes.finish());
            assert_eq!(found, expected, "{needles:?} byte by byte");
        }
        // Across lines, the lowest needle the output holds wins, wherever it stands.
        let needles = ["first".to_string(), "second".to_string()];
        let mut matcher = Matcher::new(&needles, utf8, Cleaner::default());
        assert_eq!(matcher.push(b"second\nfirst\n"), Some(0));
    }

    #[test]
    fn incomplete_utf8_suffix_detects_only_unfinished_sequences() {
        let emoji = "😀".as_bytes();
        assert_eq!(incomplete_utf8_suffix(&emoji[..3]), 3);
        assert_eq!(incomplete_utf8_suffix(emoji), 0);
        assert_eq!(incomplete_utf8_suffix(b"ab"), 0);
        assert_eq!(incomplete_utf8_suffix(&"中".as_bytes()[..1]), 1);
    }

    #[test]
    fn a_none_window_stops_at_its_cap_and_reports_the_rest() {
        let lines: Vec<u8> = (0..40)
            .flat_map(|line| format!("line {line:02}\n").into_bytes())
            .collect();
        let out = window(&[&lines], Truncate::None, 100, false);
        let shown = text(&out);
        assert!(shown.contains("lines omitted"), "{shown}");
        assert!(!shown.contains("line 20"), "{shown}");
        assert!(shown.len() <= 100 + 64, "{} bytes: {shown}", shown.len());

        // A line longer than the whole cap shows its beginning, cut, and says how much is gone.
        let long = format!("{}\n", "x".repeat(200));
        let out = window(&[long.as_bytes()], Truncate::None, 100, false);
        let shown = text(&out);
        assert!(
            shown.contains("line cut at 100 bytes; 100 more bytes"),
            "{shown}"
        );
        assert!(out.omitted.is_none(), "{shown}");

        // A valid multibyte line crossing the cap stays valid: the unfinished character at the
        // cut is dropped rather than decoded into a false encoding error.
        let mut multibyte = "中".repeat(400).into_bytes();
        multibyte.push(0x0a);
        let out = window(&[&multibyte], Truncate::None, 100, false);
        assert_eq!(out.bad_lines, 0);
        let shown = text(&out);
        assert!(shown.contains("line cut at 100 bytes"), "{shown}");
        assert!(shown.len() <= 100 + 64, "{} bytes: {shown}", shown.len());

        // Bytes that are invalid on their own decode to 3-byte replacement characters: the cap
        // holds on the text too, and the error is still reported.
        let mut invalid = vec![0x80_u8; 100];
        invalid.push(0x0a);
        let out = window(&[&invalid], Truncate::None, 100, false);
        let shown = text(&out);
        assert!(shown.contains("line cut at 100 bytes"), "{shown}");
        assert!(shown.len() <= 100 + 64, "{} bytes: {shown}", shown.len());
        assert_eq!(out.bad_lines, 1);
    }

    #[test]
    fn a_small_final_tail_window_keeps_the_actual_last_line() {
        let lines: Vec<u8> = (1..=50)
            .flat_map(|line| format!("line{line:03}\n").into_bytes())
            .collect();
        let out = window(&[&lines], Truncate::Tail, 256, false);
        let fitted = out.fit(60);
        assert!(fitted.0.ends_with("line050"), "{}", fitted.0);
    }

    #[test]
    fn final_windows_report_the_lines_they_actually_omit() {
        let lines: Vec<u8> = (1..=50)
            .flat_map(|line| format!("line{line:03}\n").into_bytes())
            .collect();
        for (mode, expected, omitted) in [
            (Truncate::Head, "line001\nline002\n...", (3, 50)),
            (Truncate::Tail, "...\nline049\nline050", (1, 48)),
            (Truncate::HeadTail, "line001\n...\nline050", (2, 49)),
        ] {
            let out = window(&[&lines], mode, 256, false);
            let fitted = out.fit(21);
            assert_eq!(fitted.0, expected);
            assert_eq!(fitted.1, 0);
            assert_eq!(fitted.2, Some(omitted));
            for room in 0..=300 {
                let fitted = out.fit(room);
                assert!(fitted.0.len() <= room, "{mode:?}, {room}: {}", fitted.0);
            }
        }
        // None's caller normally does not refit; a finite room still needs truthful omissions.
        let out = window(&[&lines], Truncate::None, 256, false);
        let fitted = out.fit(21);
        assert_eq!(fitted.0, "line001\nline002\n...");
        assert_eq!(fitted.2, Some((3, 50)));

        for mode in [
            Truncate::Head,
            Truncate::Tail,
            Truncate::HeadTail,
            Truncate::None,
        ] {
            let long = format!("{}\n", "x".repeat(80));
            let out = window(&[long.as_bytes()], mode, 256, false);
            let fitted = out.fit(20);
            assert!(!fitted.0.is_empty(), "{mode:?}");
            assert!(fitted.0.len() <= 20);
            assert_eq!(fitted.1, 1);
            assert_eq!(fitted.2, None);
        }
        let out = window(&["中\n".as_bytes()], Truncate::Tail, 256, false);
        let fitted = out.fit(1);
        assert_eq!(fitted.0, "");
        assert_eq!(fitted.2, Some((1, 1)));
    }

    #[test]
    fn fitting_a_window_counts_each_shown_cut_line_once() {
        let line = format!(
            "{}
",
            "x".repeat(3000)
        );
        let out = window(
            &[
                line.as_bytes(),
                b"tail
",
            ],
            Truncate::HeadTail,
            16_000,
            false,
        );
        assert_eq!(out.fit(usize::MAX).1, 1, "the 2000-char cap cut one line");
        let (fitted, cut_lines, _) = out.fit(500);
        assert!(fitted.len() <= 500 && fitted.ends_with("tail"), "{fitted}");
        assert_eq!(cut_lines, 1, "a line cut twice still counts once");
    }
}
