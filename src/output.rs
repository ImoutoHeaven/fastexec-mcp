//! Model-facing output: streaming terminal cleaning and byte-budgeted line windows.
//!
//! The ANSI CSI/OSC state machine follows FastCtx `src/shell/normalize.rs` and the head/tail
//! split follows FastCtx `src/shell/output.rs` (Apache-2.0, Copyright 2026 yc-duan), modified for fastexec.

use encoding_rs::Encoding;
use serde::Deserialize;
use std::collections::VecDeque;

/// Bytes kept per line in bounded windows; the rest of an oversized line is counted, then cut.
const LINE_BYTES_LIMIT: usize = 64 * 1024;
/// Characters kept per cleaned line in bounded windows. `Truncate::None` keeps whole lines.
const LINE_CHARS_LIMIT: usize = 2000;

#[derive(Clone, Copy, Debug, Default, Deserialize, schemars::JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Truncate {
    /// First lines and last lines with the middle omitted.
    #[default]
    HeadTail,
    /// First lines only.
    Head,
    /// Last lines only.
    Tail,
    /// Every line; the host's own output limit applies.
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
        if byte == 0x1b && self.escape == EscState::Text && self.pending_cr {
            // CR then an escape sequence (often ESC[K) starts rewriting the line.
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

/// Finds literal text in output cleaned as windows clean it, fed in pieces of any size. Every
/// state a cleaned line passes through counts, before a carriage return rewrites it and before
/// trailing spaces are trimmed, so how the output is split never changes the answer.
pub struct Matcher {
    /// The text sought, ASCII-lowercased when the search ignores case.
    needle: String,
    ignore_case: bool,
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
    pub fn new(
        needle: &str,
        case_sensitive: bool,
        encoding: &'static Encoding,
        cleaner: Cleaner,
    ) -> Self {
        Self {
            needle: if case_sensitive {
                needle.to_string()
            } else {
                needle.to_ascii_lowercase()
            },
            ignore_case: !case_sensitive,
            encoding,
            cleaner,
            decoder: encoding.new_decoder_without_bom_handling(),
            line: String::new(),
            pending: Vec::new(),
        }
    }

    /// Feeds the next output bytes; returns true once the text has appeared.
    pub fn push(&mut self, bytes: &[u8]) -> bool {
        for &byte in bytes {
            let (clear, step) = self.cleaner.step(byte);
            if clear && self.found(true) {
                return true;
            }
            if clear {
                self.new_line();
            }
            match step {
                Step::Append => self.pending.push(byte),
                Step::EndLine => {
                    if self.found(true) {
                        return true;
                    }
                    self.new_line();
                }
                Step::Nothing | Step::Control | Step::SwallowedLf => {}
            }
        }
        self.found(false)
    }

    /// Ends the output: an unfinished last line ends as a window shows it. Returns true when
    /// the text appears; a repeated call finds nothing more.
    pub fn finish(&mut self) -> bool {
        let found = self.found(true);
        self.new_line();
        found
    }

    fn new_line(&mut self) {
        self.line.clear();
        self.pending.clear();
        self.decoder = self.encoding.new_decoder_without_bom_handling();
    }

    /// Decodes the pending bytes and searches the line, then keeps only the end of the line
    /// that a later match could start in. `last` ends the line's text, so an unfinished
    /// character decodes as the replacement character a window shows.
    fn found(&mut self, last: bool) -> bool {
        let pending = std::mem::take(&mut self.pending);
        if let Some(room) = self.decoder.max_utf8_buffer_length(pending.len()) {
            self.line.reserve(room);
        }
        let _ = self
            .decoder
            .decode_to_string(&pending, &mut self.line, last);
        let found = if self.ignore_case {
            self.line.to_ascii_lowercase().contains(&self.needle)
        } else {
            self.line.contains(&self.needle)
        };
        let mut cut = self.line.len().saturating_sub(self.needle.len());
        while !self.line.is_char_boundary(cut) {
            cut -= 1;
        }
        self.line.drain(..cut);
        found
    }
}

/// One line ready for the window, with its 1-based log line number.
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
    /// Lines in display order, omission marker included, each with whether it lost text.
    lines: Vec<(String, bool)>,
    /// Inclusive line range left out of the window.
    pub omitted: Option<(u64, u64)>,
    /// Lines that held byte sequences invalid in the selected encoding.
    pub bad_lines: u64,
    /// LF bytes consumed, for the caller's line cursor.
    pub newlines: u64,
}

impl Window {
    /// Joins the lines within `room` bytes. The line that crosses the limit is cut and later
    /// lines are left out. Returns the text and how many shown lines lost part of their text.
    pub fn fit(&self, room: usize) -> (String, u64) {
        let mut text = String::new();
        let mut cut_lines = 0;
        for (index, (line, cut)) in self.lines.iter().enumerate() {
            if index > 0 {
                if text.len() >= room {
                    break;
                }
                text.push('\n');
            }
            let available = room - text.len();
            let trimmed = line.len() > available;
            if trimmed {
                let mut shown = line.clone();
                cut_to(&mut shown, available + 1);
                text.push_str(&shown);
            } else {
                text.push_str(line);
            }
            if *cut || trimmed {
                cut_lines += 1;
            }
            if trimmed {
                break;
            }
        }
        (text, cut_lines)
    }
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

    fn append(&mut self, byte: u8) {
        self.line_started = true;
        if self.line.len() < LINE_BYTES_LIMIT || self.spec.truncate == Truncate::None {
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
        let bytes = std::mem::take(&mut self.line);
        let overflow = std::mem::take(&mut self.line_overflow);
        self.line_started = false;
        if std::mem::take(&mut self.cleaner.continued) && bytes.is_empty() && overflow == 0 {
            return;
        }
        let (decoded, had_errors) = self.spec.encoding.decode_without_bom_handling(&bytes);
        if had_errors {
            self.bad_lines += 1;
        }
        let mut text = decoded.into_owned();
        let mut cut = overflow > 0;
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
        if overflow > 0 {
            text.push_str(&format!(" … [line cut at 64 KiB; {overflow} more bytes]"));
        }
        self.place(Line { number, text, cut });
    }

    fn place(&mut self, mut line: Line) {
        let (head_budget, tail_budget) = match self.spec.truncate {
            Truncate::None => (usize::MAX, usize::MAX),
            Truncate::Head => (self.spec.budget, 0),
            Truncate::Tail => (0, self.spec.budget),
            Truncate::HeadTail => (
                self.spec.budget / 10,
                self.spec.budget - self.spec.budget / 10,
            ),
        };
        let cost = line.text.len() + 1;
        if self.head_open {
            if self.head_bytes + cost <= head_budget {
                self.head_bytes += cost;
                self.head.push(line);
                return;
            }
            self.head_open = false;
            if self.head.is_empty() && self.spec.truncate == Truncate::Head {
                line.cut |= cut_to(&mut line.text, head_budget);
                self.head_bytes += line.text.len() + 1;
                self.head.push(line);
                return;
            }
        }
        if line.text.len() + 1 > tail_budget && tail_budget > 0 {
            line.cut |= cut_to(&mut line.text, tail_budget);
        }
        self.tail_bytes += line.text.len() + 1;
        self.tail.push_back(line);
        while self.tail_bytes > tail_budget {
            let Some(dropped) = self.tail.pop_front() else {
                break;
            };
            self.tail_bytes -= dropped.text.len() + 1;
            let first = self.omitted.map_or(dropped.number, |(first, _)| first);
            self.omitted = Some((first, dropped.number));
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
        let mut lines: Vec<(String, bool)> = self
            .head
            .into_iter()
            .map(|line| (line.text, line.cut))
            .collect();
        if let Some((first, last)) = self.omitted {
            let count = last - first + 1;
            let source = self.spec.source;
            let marker = format!("... [{count} lines omitted: {source} lines {first}-{last}] ...");
            lines.push((marker, false));
        }
        lines.extend(self.tail.into_iter().map(|line| (line.text, line.cut)));
        Window {
            lines,
            omitted: self.omitted,
            bad_lines: self.bad_lines,
            newlines: self.newlines,
        }
    }
}

/// Shortens `text` to at most `budget - 1` bytes, with a cut marker when it fits, on a char
/// boundary. Returns whether it cut anything.
pub fn cut_to(text: &mut String, budget: usize) -> bool {
    const MARK: &str = " … [cut]";
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
            0,
            false,
        );
        assert_eq!(text(&out), "ok\n100%\ndone");
        assert_eq!(out.newlines, 2);
    }

    #[test]
    fn utf8_split_across_chunks_decodes_cleanly() {
        let input = "中文😀\n".as_bytes();
        let out = window(
            &[&input[..2], &input[2..5], &input[5..]],
            Truncate::None,
            0,
            false,
        );
        assert_eq!(text(&out), "中文😀");
        assert_eq!(out.bad_lines, 0);
    }

    #[test]
    fn raw_mode_keeps_escapes_and_carriage_returns() {
        let out = window(&[b"\x1b[1mA\r\nB\rC\n"], Truncate::None, 0, true);
        assert_eq!(text(&out), "\x1b[1mA\r\nB\rC");
        // A raw window that ends inside a sequence leaves a cleaned window parsing it.
        let mut cleaner = Cleaner::default();
        let spec = |raw| WindowSpec {
            truncate: Truncate::None,
            budget: 0,
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
            budget: 0,
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
        let cases: [(&[u8], &str, bool, &'static Encoding, bool); 10] = [
            (
                b"\x1b[32mServer\x1b[0m listening\r\n",
                "Server listening",
                true,
                utf8,
                true,
            ),
            // Text a carriage return rewrites still counts.
            (b"10%\r50%\r100%\r\x1b[Kdone", "100%", true, utf8, true),
            (b"Password: ", "Password: ", true, utf8, true),
            (b"SERVER READY", "server ready", false, utf8, true),
            (b"SERVER READY", "server ready", true, utf8, false),
            // Only ASCII letters fold.
            ("\u{c9}COLE".as_bytes(), "\u{e9}cole", false, utf8, false),
            (
                b"\xd6\xd0\xce\xc4 ok",
                "\u{4e2d}\u{6587} ok",
                true,
                encoding_rs::GBK,
                true,
            ),
            (b"ab\ncd", "bc", true, utf8, false),
            // A line ending inside a character shows, and matches, the replacement character.
            (b"\xe2\n", "\u{fffd}", true, utf8, true),
            (b"\xe2", "\u{fffd}", true, utf8, true),
        ];
        for (output, needle, case_sensitive, encoding, expected) in cases {
            let matcher = || Matcher::new(needle, case_sensitive, encoding, Cleaner::default());
            for split in 0..=output.len() {
                let mut two = matcher();
                let found =
                    two.push(&output[..split]) || two.push(&output[split..]) || two.finish();
                assert_eq!(found, expected, "{needle:?} split at {split}");
            }
            let mut bytes = matcher();
            let found = output.iter().any(|byte| bytes.push(&[*byte])) || bytes.finish();
            assert_eq!(found, expected, "{needle:?} byte by byte");
        }
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
        let (fitted, cut_lines) = out.fit(500);
        assert!(fitted.len() <= 500 && !fitted.contains("tail"), "{fitted}");
        assert_eq!(cut_lines, 1, "a line cut twice still counts once");
    }
}
