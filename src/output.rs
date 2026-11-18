//! Model-facing output: streaming terminal cleaning and byte-budgeted line windows.
//!
//! The ANSI CSI/OSC state machine follows FastCtx `src/shell/normalize.rs` and the head/tail
//! split follows FastCtx `src/shell/output.rs` (Apache-2.0, Copyright 2026 yc-duan), modified for fastexec.

use encoding_rs::Encoding;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

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
    fn step(&mut self, byte: u8, keep_nul: bool) -> (bool, Step) {
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
            0x00 if keep_nul => Step::Append,
            0x00..=0x1f | 0x7f => Step::Control,
            _ => Step::Append,
        };
        (clear, step)
    }
}

/// Finds literal texts in output as windows show it, fed in pieces of any size. With a cleaner,
/// every state a line passes through counts before carriage-return rewrites and trailing-space
/// trimming. Without one, only LF ends a line and every other byte is matched unchanged.
pub struct Matcher {
    /// The texts sought, any of which ends the search.
    needles: Vec<String>,
    /// The lowest index of a needle found so far.
    best: Option<usize>,
    /// The longest needle, in bytes.
    longest: usize,
    encoding: &'static Encoding,
    cleaner: Option<Cleaner>,
    decoder: encoding_rs::Decoder,
    /// The decoded end of the current line, long enough to hold a match that ends later.
    line: String,
    /// Text bytes of the current line not decoded yet.
    pending: Vec<u8>,
}

impl Matcher {
    /// Continues the cleaning state where output starts, or matches unchanged bytes with `None`.
    pub fn new(needles: &[String], encoding: &'static Encoding, cleaner: Option<Cleaner>) -> Self {
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
            let (clear, step) = match &mut self.cleaner {
                Some(cleaner) => cleaner.step(byte, false),
                None => (
                    false,
                    if byte == b'\n' {
                        Step::EndLine
                    } else {
                        Step::Append
                    },
                ),
            };
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

/// One candidate line retained while streaming: its UTF-8 prefix (up to `spec.budget` bytes)
/// and how many decoded UTF-8 bytes followed that prefix.
struct Candidate {
    prefix: String,
    dropped: usize,
}

pub struct WindowSpec {
    pub truncate: Truncate,
    /// Byte budget for the window body.
    pub budget: usize,
    pub raw: bool,
    pub clean: bool,
    pub keep_nul: bool,
    pub encoding: &'static Encoding,
    /// What the line numbers count, named in the omission marker: "log" or "transcript".
    pub source: &'static str,
}

pub struct Window {
    pub text: String,
    pub cut_lines: u64,
    /// Inclusive line range left out of the window.
    pub omitted: Option<(u64, u64)>,
    /// Lines that held byte sequences invalid in the selected encoding.
    pub bad_lines: u64,
    pub encoding: &'static Encoding,
    /// LF bytes consumed, for the caller's line cursor.
    pub newlines: u64,
    empty: bool,
}

impl Window {
    pub fn is_empty(&self) -> bool {
        self.empty
    }
}

fn format_marker(source: &str, first: u64, last: u64) -> String {
    let count = last - first + 1;
    format!("... [{count} lines omitted: {source} lines {first}-{last}] ...")
}

/// A large line is decoded in chunks. Only its UTF-8 prefix up to `budget` is retained;
/// the full decoded length accounts for transcoding, invalid bytes and trailing padding.
struct DecodedLine {
    decoder: encoding_rs::Decoder,
    prefix: String,
    bytes: usize,
    had_errors: bool,
}

impl DecodedLine {
    fn new(encoding: &'static Encoding) -> Self {
        Self {
            decoder: encoding.new_decoder_without_bom_handling(),
            prefix: String::new(),
            bytes: 0,
            had_errors: false,
        }
    }

    fn push(&mut self, mut input: &[u8], last: bool, budget: usize) {
        let mut output = [0; 4096];
        loop {
            let (state, read, written, errors) =
                self.decoder.decode_to_utf8(input, &mut output, last);
            self.had_errors |= errors;
            if self.prefix.len() == self.bytes && self.bytes < budget {
                let chunk = std::str::from_utf8(&output[..written]).expect("decoder emits UTF-8");
                let mut keep = written.min(budget - self.bytes);
                while !chunk.is_char_boundary(keep) {
                    keep -= 1;
                }
                self.prefix.push_str(&chunk[..keep]);
            }
            self.bytes += written;
            input = &input[read..];
            if state == encoding_rs::CoderResult::InputEmpty {
                break;
            }
        }
    }
}

/// Turns a byte range of the log into a bounded window. Feed chunks, then call `finish`.
pub struct WindowBuilder<'a> {
    spec: WindowSpec,
    cleaner: &'a mut Cleaner,
    first_line: u64,
    newlines: u64,
    total_lines: u64,
    line: Vec<u8>,
    decoded: Option<DecodedLine>,
    trailing_spaces: usize,
    line_started: bool,
    head: Vec<Candidate>,
    head_bytes: usize,
    head_open: bool,
    tail: VecDeque<Candidate>,
    tail_bytes: usize,
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
            total_lines: 0,
            line: Vec::new(),
            decoded: None,
            trailing_spaces: 0,
            line_started: false,
            head: Vec::new(),
            head_bytes: 0,
            head_open: true,
            tail: VecDeque::new(),
            tail_bytes: 0,
            bad_lines: 0,
        }
    }

    pub fn push(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            if !self.spec.clean {
                if byte == b'\n' {
                    self.end_line(true);
                } else {
                    self.append(byte);
                }
            } else if self.spec.raw {
                self.raw_byte(byte);
            } else {
                self.clean_byte(byte);
            }
        }
    }

    fn raw_byte(&mut self, byte: u8) {
        // Advance the full cleaning state for later windows, ignoring only its rendering decision.
        let _ = self.cleaner.step(byte, self.spec.keep_nul);
        if byte == b'\n' {
            self.end_line(true);
        } else {
            self.append(byte);
        }
    }

    fn clean_byte(&mut self, byte: u8) {
        let (clear, step) = self.cleaner.step(byte, self.spec.keep_nul);
        if clear {
            self.line.clear();
            self.decoded = None;
            self.trailing_spaces = 0;
        }
        match step {
            Step::Nothing => {}
            Step::Append => {
                self.trailing_spaces = if byte == b' ' {
                    self.trailing_spaces + 1
                } else {
                    0
                };
                self.append(byte);
            }
            Step::Control => self.line_started = true,
            Step::EndLine => self.end_line(true),
            Step::SwallowedLf => self.newlines += 1,
        }
    }

    fn append(&mut self, byte: u8) {
        self.line_started = true;
        if self.line.len() == 8192 {
            self.decoded
                .get_or_insert_with(|| DecodedLine::new(self.spec.encoding))
                .push(&self.line, false, self.spec.budget);
            self.line.clear();
        }
        self.line.push(byte);
    }

    fn end_line(&mut self, terminated: bool) {
        if terminated {
            self.newlines += 1;
        }
        let bytes = std::mem::take(&mut self.line);
        self.line_started = false;
        if std::mem::take(&mut self.cleaner.continued) && bytes.is_empty() && self.decoded.is_none()
        {
            return;
        }
        let trailing_spaces = std::mem::take(&mut self.trailing_spaces);
        let limit = self.spec.budget;
        let (mut prefix, mut dropped, had_errors) = if let Some(mut decoded) = self.decoded.take() {
            decoded.push(&bytes, true, limit);
            let dropped = decoded.bytes - decoded.prefix.len();
            (decoded.prefix, dropped, decoded.had_errors)
        } else {
            let (decoded, errors) = self.spec.encoding.decode_without_bom_handling(&bytes);
            let mut text = decoded.into_owned();
            let full = text.len();
            if full > limit {
                let mut keep = limit;
                while !text.is_char_boundary(keep) {
                    keep -= 1;
                }
                text.truncate(keep);
            }
            let dropped = full - text.len();
            (text, dropped, errors)
        };
        if had_errors {
            self.bad_lines += 1;
        }
        // Spaces before omitted text are significant. Trim only the actual end of the line,
        // including terminal padding beyond the retained prefix.
        let stored_padding = trailing_spaces.saturating_sub(dropped);
        prefix.truncate(prefix.len() - stored_padding);
        dropped = dropped.saturating_sub(trailing_spaces);
        self.place(Candidate { prefix, dropped });
    }

    fn place(&mut self, cand: Candidate) {
        self.total_lines += 1;
        let total = self.spec.budget.saturating_add(1);
        if self.head_open {
            if cand.dropped == 0 {
                let cost = cand.prefix.len().saturating_add(1);
                if self.head_bytes.saturating_add(cost) <= total {
                    self.head_bytes += cost;
                    self.head.push(cand);
                    return;
                }
            }
            self.head_open = false;
            if self.head.is_empty() {
                self.head_bytes = cand.prefix.len().saturating_add(1);
                self.head.push(cand);
                return;
            }
        }
        if self.spec.truncate == Truncate::Head {
            return;
        }
        if cand.dropped > 0 {
            self.tail.clear();
            self.tail_bytes = cand.prefix.len().saturating_add(1);
            self.tail.push_back(cand);
            return;
        }
        if self.tail.back().is_some_and(|back| back.dropped > 0) {
            self.tail.clear();
            self.tail_bytes = 0;
        }
        let cost = cand.prefix.len().saturating_add(1);
        self.tail_bytes = self.tail_bytes.saturating_add(cost);
        self.tail.push_back(cand);
        while self.tail.len() > 1 && self.tail_bytes > total {
            let dropped = self.tail.pop_front().expect("tail has multiple items");
            self.tail_bytes -= dropped.prefix.len().saturating_add(1);
        }
    }

    pub fn finish(mut self) -> Window {
        if self.line_started || !self.line.is_empty() {
            // The unterminated last line is shown now; a trailing CR may still pair with a LF.
            if self.spec.clean && self.cleaner.pending_cr {
                self.cleaner.pending_cr = false;
                self.cleaner.swallow_lf = true;
            }
            self.end_line(false);
            self.cleaner.continued = true;
        }
        let budget = self.spec.budget;
        let empty = self.total_lines == 0;
        let (text, cut_lines, omitted) = self.assemble();
        debug_assert!(
            text.len() <= budget,
            "window body {} exceeds budget {budget}",
            text.len()
        );
        Window {
            text,
            cut_lines,
            omitted,
            bad_lines: self.bad_lines,
            encoding: self.spec.encoding,
            newlines: self.newlines,
            empty,
        }
    }

    fn assemble(&mut self) -> (String, u64, Option<(u64, u64)>) {
        let budget = self.spec.budget;
        if self.total_lines == 0 {
            return (String::new(), 0, None);
        }
        if self.head_open {
            let text = self
                .head
                .iter()
                .map(|cand| cand.prefix.as_str())
                .collect::<Vec<_>>()
                .join("\n");
            return (text, 0, None);
        }
        let first_num = self.first_line;
        let last_num = self.first_line + self.total_lines - 1;
        if self.total_lines == 1 {
            if budget == 0 {
                return (String::new(), 0, Some((first_num, first_num)));
            }
            let mut text = self.head[0].prefix.clone();
            cut_line_to_budget(&mut text, budget, self.head[0].dropped);
            if text.is_empty() {
                return (String::new(), 0, Some((first_num, first_num)));
            }
            return (text, 1, None);
        }

        let source = self.spec.source;
        let fallback_all = || {
            let mut marker = format_marker(source, first_num, last_num);
            cut_to(&mut marker, budget.saturating_add(1));
            (marker, 0, Some((first_num, last_num)))
        };

        if self.spec.truncate == Truncate::Head {
            let mut head_used = 0usize;
            let mut h = 0usize;
            for cand in &self.head {
                if h as u64 + 1 >= self.total_lines || cand.dropped > 0 {
                    break;
                }
                let next_used = head_used + cand.prefix.len() + 1;
                let m_len = format_marker(source, first_num + (h + 1) as u64, last_num).len();
                if next_used + m_len <= budget {
                    head_used = next_used;
                    h += 1;
                } else {
                    break;
                }
            }
            if h > 0 {
                let omitted = (first_num + h as u64, last_num);
                let marker = format_marker(source, omitted.0, omitted.1);
                let mut lines: Vec<&str> =
                    self.head[..h].iter().map(|c| c.prefix.as_str()).collect();
                lines.push(&marker);
                return (lines.join("\n"), 0, Some(omitted));
            }
            let omitted = (first_num + 1, last_num);
            let marker = format_marker(source, omitted.0, omitted.1);
            if budget > marker.len() + 1 {
                let avail = budget - marker.len() - 1;
                let mut line0 = self.head[0].prefix.clone();
                cut_line_to_budget(&mut line0, avail, self.head[0].dropped);
                if !line0.is_empty() {
                    return (format!("{line0}\n{marker}"), 1, Some(omitted));
                }
            }
            return fallback_all();
        }

        // Suffix lines after line 0, ordered from the last line backwards.
        let contiguous = self.head.len() as u64 + self.tail.len() as u64 == self.total_lines;
        let mut suffix_rev: Vec<&Candidate> = self.tail.iter().rev().collect();
        if contiguous && self.head.len() > 1 {
            suffix_rev.extend(self.head[1..].iter().rev());
        }

        if self.spec.truncate == Truncate::Tail {
            let mut tail_used = 0usize;
            let mut t = 0usize;
            for cand in suffix_rev.iter().copied() {
                if cand.dropped > 0 {
                    break;
                }
                let next_used = tail_used + cand.prefix.len() + 1;
                let m_len = format_marker(source, first_num, last_num - (t + 1) as u64).len();
                if next_used + m_len <= budget {
                    tail_used = next_used;
                    t += 1;
                } else {
                    break;
                }
            }
            if t > 0 {
                let omitted = (first_num, last_num - t as u64);
                let marker = format_marker(source, omitted.0, omitted.1);
                let mut lines = Vec::with_capacity(t + 1);
                lines.push(marker.as_str());
                for cand in suffix_rev[..t].iter().rev() {
                    lines.push(cand.prefix.as_str());
                }
                return (lines.join("\n"), 0, Some(omitted));
            }
            let omitted = (first_num, last_num - 1);
            let marker = format_marker(source, omitted.0, omitted.1);
            if budget > marker.len() + 1 {
                let avail = budget - marker.len() - 1;
                let last_cand = suffix_rev[0];
                let mut last_text = last_cand.prefix.clone();
                cut_line_to_budget(&mut last_text, avail, last_cand.dropped);
                if !last_text.is_empty() {
                    return (format!("{marker}\n{last_text}"), 1, Some(omitted));
                }
            }
            return fallback_all();
        }

        // Truncate::HeadTail multi-line overflow (Rule 5).
        let max_marker_len = format!(
            "... [{} lines omitted: {source} lines {last_num}-{last_num}] ...",
            self.total_lines
        )
        .len();
        if budget <= max_marker_len + 2 {
            return fallback_all();
        }
        let head_reserve = budget / 10;
        let after_marker = budget - max_marker_len;
        let tail_budget = after_marker.saturating_sub(head_reserve);
        if tail_budget <= 1 {
            return fallback_all();
        }

        let mut tail_kept: Vec<String> = Vec::new();
        let mut tail_used = 0usize;
        let mut tail_cuts = 0u64;
        for cand in suffix_rev.iter().copied() {
            if cand.dropped > 0 {
                break;
            }
            let cost = cand.prefix.len() + 1;
            if tail_used + cost <= tail_budget {
                tail_used += cost;
                tail_kept.push(cand.prefix.clone());
            } else {
                break;
            }
        }
        tail_kept.reverse();
        if tail_kept.is_empty() {
            let last_cand = suffix_rev[0];
            let mut last_text = last_cand.prefix.clone();
            cut_line_to_budget(&mut last_text, tail_budget - 1, last_cand.dropped);
            if !last_text.is_empty() {
                tail_used = last_text.len() + 1;
                tail_kept.push(last_text);
                tail_cuts = 1;
            }
        }

        let first_tail_num = last_num + 1 - tail_kept.len() as u64;
        let before_tail = (first_tail_num - first_num) as usize;
        let mut head_used = 0usize;
        let mut h = 0usize;
        for cand in self.head.iter().take(before_tail) {
            if cand.dropped > 0 {
                break;
            }
            let next_used = head_used + cand.prefix.len() + 1;
            let rem = before_tail - (h + 1);
            let fits = if rem == 0 {
                next_used + tail_used <= budget + 1
            } else {
                let m_len =
                    format_marker(source, first_num + (h + 1) as u64, first_tail_num - 1).len();
                next_used + m_len + tail_used <= budget
            };
            if fits {
                head_used = next_used;
                h += 1;
            } else {
                break;
            }
        }

        if h == before_tail {
            let mut lines: Vec<&str> = self.head[..h].iter().map(|c| c.prefix.as_str()).collect();
            lines.extend(tail_kept.iter().map(String::as_str));
            return (lines.join("\n"), tail_cuts, None);
        }
        if h > 0 {
            let omitted = (first_num + h as u64, first_tail_num - 1);
            let marker = format_marker(source, omitted.0, omitted.1);
            let mut lines: Vec<&str> = self.head[..h].iter().map(|c| c.prefix.as_str()).collect();
            lines.push(&marker);
            lines.extend(tail_kept.iter().map(String::as_str));
            return (lines.join("\n"), tail_cuts, Some(omitted));
        }

        if before_tail == 1 && !tail_kept.is_empty() {
            let avail = budget.saturating_sub(tail_used);
            if avail > 0 {
                let mut line0 = self.head[0].prefix.clone();
                cut_line_to_budget(&mut line0, avail, self.head[0].dropped);
                if !line0.is_empty() {
                    let mut lines = Vec::with_capacity(1 + tail_kept.len());
                    lines.push(line0.as_str());
                    lines.extend(tail_kept.iter().map(String::as_str));
                    return (lines.join("\n"), 1 + tail_cuts, None);
                }
            }
            let marker = format_marker(source, first_num, first_num);
            if marker.len() + tail_used <= budget {
                let mut lines = Vec::with_capacity(1 + tail_kept.len());
                lines.push(marker.as_str());
                lines.extend(tail_kept.iter().map(String::as_str));
                return (lines.join("\n"), tail_cuts, Some((first_num, first_num)));
            }
            return fallback_all();
        }

        let omitted = (first_num + 1, first_tail_num - 1);
        let marker = format_marker(source, omitted.0, omitted.1);
        if budget > marker.len() + tail_used + 1 {
            let avail = budget - marker.len() - tail_used - 1;
            let mut line0 = self.head[0].prefix.clone();
            cut_line_to_budget(&mut line0, avail, self.head[0].dropped);
            if !line0.is_empty() {
                let mut lines = Vec::with_capacity(2 + tail_kept.len());
                lines.push(line0.as_str());
                lines.push(marker.as_str());
                lines.extend(tail_kept.iter().map(String::as_str));
                return (lines.join("\n"), 1 + tail_cuts, Some(omitted));
            }
        }
        let omitted_all = (first_num, first_tail_num - 1);
        let marker_all = format_marker(source, omitted_all.0, omitted_all.1);
        if !tail_kept.is_empty() && marker_all.len() + tail_used <= budget {
            let mut lines = Vec::with_capacity(1 + tail_kept.len());
            lines.push(marker_all.as_str());
            lines.extend(tail_kept.iter().map(String::as_str));
            return (lines.join("\n"), tail_cuts, Some(omitted_all));
        }
        fallback_all()
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

/// Cuts `text` on a character boundary so `text` and its `… [line cut at <budget>; <n> more
/// bytes]` marker fit within `limit` bytes together, falling back to `cut_to` when `limit` is too
/// small to hold the full marker.
fn cut_line_to_budget(text: &mut String, limit: usize, already_dropped: usize) {
    let budget_label = brief_bytes(limit);
    let max_dropped = already_dropped + text.len();
    let max_marker = format!(" … [line cut at {budget_label}; {max_dropped} more bytes]");
    if limit < max_marker.len() {
        if already_dropped == 0 && text.len() <= limit {
            return;
        }
        if limit < 2 * MARK.len() {
            let mut end = limit.min(text.len());
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            text.truncate(end);
            return;
        }
        let mut end = (limit - MARK.len()).min(text.len());
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push_str(MARK);
        return;
    }
    let mut keep = (limit - max_marker.len()).min(text.len());
    while !text.is_char_boundary(keep) {
        keep -= 1;
    }
    let dropped = already_dropped + (text.len() - keep);
    let mut marker = format!(" … [line cut at {budget_label}; {dropped} more bytes]");
    if marker.len() < max_marker.len() {
        let mut alt_keep = (limit - marker.len()).min(text.len());
        while !text.is_char_boundary(alt_keep) {
            alt_keep -= 1;
        }
        if alt_keep > keep {
            let alt_dropped = already_dropped + (text.len() - alt_keep);
            let alt_marker = format!(" … [line cut at {budget_label}; {alt_dropped} more bytes]");
            if alt_keep + alt_marker.len() <= limit {
                keep = alt_keep;
                marker = alt_marker;
            }
        }
    }
    text.truncate(keep);
    text.push_str(&marker);
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

/// What a cut line ends with when the full byte-count note cannot fit.
const MARK: &str = " … [cut]";

/// Length of an incomplete UTF-8 sequence at the end of `tail` (the last 1..=3 bytes).
pub fn incomplete_utf8_suffix(tail: &[u8]) -> usize {
    let suffix = &tail[tail.len().saturating_sub(3)..];
    let Some(lead) = suffix.iter().rposition(|&b| b & 0xc0 != 0x80) else {
        return 0;
    };
    let candidate = &suffix[lead..];
    match std::str::from_utf8(candidate) {
        Err(error) if error.error_len().is_none() => candidate.len(),
        _ => 0,
    }
}

/// Bytes still held by the decoder at a live window boundary. Earlier malformed sequences
/// were already emitted as replacements and do not affect a later unfinished character.
pub fn incomplete_legacy_suffix(line: &[u8], encoding: &'static encoding_rs::Encoding) -> usize {
    let mut decoder = encoding.new_decoder_without_bom_handling();
    let mut buffer = [0; 4096];
    let mut input = line;
    loop {
        let (result, consumed, _, _) = decoder.decode_to_utf8(input, &mut buffer, false);
        input = &input[consumed..];
        if result == encoding_rs::CoderResult::InputEmpty {
            break;
        }
    }
    // Flushing now can only fail on the incomplete suffix.
    match decoder
        .decode_to_utf8_without_replacement(&[], &mut buffer, true)
        .0
    {
        encoding_rs::DecoderResult::Malformed(len, _) => usize::from(len),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LARGE_BUDGET: usize = 4 << 20;

    fn window(chunks: &[&[u8]], truncate: Truncate, budget: usize, raw: bool) -> Window {
        let mut cleaner = Cleaner::default();
        let spec = WindowSpec {
            truncate,
            budget,
            raw,
            clean: true,
            keep_nul: false,
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
            Truncate::HeadTail,
            LARGE_BUDGET,
            false,
        );
        assert_eq!(out.text, "ok\n100%\ndone");
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
                let out = window(&chunks, Truncate::HeadTail, LARGE_BUDGET, false);
                assert_eq!(out.text, expected, "{input:?}");
            }
        }
    }

    #[test]
    fn utf8_split_across_chunks_decodes_cleanly() {
        let input = "中文😀\n".as_bytes();
        let out = window(
            &[&input[..2], &input[2..5], &input[5..]],
            Truncate::HeadTail,
            LARGE_BUDGET,
            false,
        );
        assert_eq!(out.text, "中文😀");
        assert_eq!(out.bad_lines, 0);
    }

    #[test]
    fn raw_mode_keeps_escapes_and_carriage_returns() {
        let out = window(
            &[b"\x1b[1mA\r\nB\rC\n"],
            Truncate::HeadTail,
            LARGE_BUDGET,
            true,
        );
        assert_eq!(out.text, "\x1b[1mA\r\nB\rC");
        let mut cleaner = Cleaner::default();
        let spec = |raw| WindowSpec {
            truncate: Truncate::HeadTail,
            budget: LARGE_BUDGET,
            raw,
            clean: true,
            keep_nul: false,
            encoding: encoding_rs::UTF_8,
            source: "log",
        };
        let mut first = WindowBuilder::new(spec(true), &mut cleaner, 1);
        first.push(b"x\x1b[");
        first.finish();
        let mut second = WindowBuilder::new(spec(false), &mut cleaner, 1);
        second.push(b"31mRED\n");
        assert_eq!(second.finish().text, "RED");
    }

    #[test]
    fn raw_windows_consume_pending_crlf_state() {
        let mut cleaner = Cleaner::default();
        let spec = |raw| WindowSpec {
            truncate: Truncate::HeadTail,
            budget: LARGE_BUDGET,
            raw,
            clean: true,
            keep_nul: false,
            encoding: encoding_rs::UTF_8,
            source: "log",
        };
        let mut first = WindowBuilder::new(spec(false), &mut cleaner, 1);
        first.push(b"first\r");
        assert_eq!(first.finish().text, "first");
        let mut raw = WindowBuilder::new(spec(true), &mut cleaner, 1);
        raw.push(b"RAW\n");
        assert_eq!(raw.finish().text, "RAW");
        let mut last = WindowBuilder::new(spec(false), &mut cleaner, 2);
        last.push(b"\nnext\n");
        let out = last.finish();
        assert_eq!(out.text, "\nnext");
        assert_eq!(out.newlines, 2);

        let mut raw = WindowBuilder::new(spec(true), &mut cleaner, 4);
        raw.push(b"raw\r");
        assert_eq!(raw.finish().text, "raw\r");
        let mut last = WindowBuilder::new(spec(false), &mut cleaner, 4);
        last.push(b"\nnext\n");
        let out = last.finish();
        assert_eq!(out.text, "next");
        assert_eq!(out.newlines, 2);
    }

    #[test]
    fn keep_nul_preserves_nul_only_when_requested() {
        let sample = b"a b\0c\nd\0";
        for (keep_nul, expected) in [(false, "a bc\nd"), (true, "a b\0c\nd\0")] {
            let mut cleaner = Cleaner::default();
            let mut builder = WindowBuilder::new(
                WindowSpec {
                    truncate: Truncate::Head,
                    budget: LARGE_BUDGET,
                    raw: false,
                    clean: true,
                    keep_nul,
                    encoding: encoding_rs::UTF_8,
                    source: "log",
                },
                &mut cleaner,
                1,
            );
            builder.push(sample);
            let out = builder.finish();
            assert_eq!(out.text, expected, "keep_nul={keep_nul}");
        }
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
            assert!(out.text.len() <= budget, "{mode:?}: {}", out.text.len());
            let shown: Vec<&str> = out.text.lines().filter(|l| l.starts_with("line")).collect();
            assert_eq!(shown.len() as u64 + (last - first + 1), 100, "{mode:?}");
            match mode {
                Truncate::Head => assert_eq!((shown[0], last), ("line001", 100)),
                Truncate::Tail => assert_eq!((first, *shown.last().unwrap()), (1, "line100")),
                Truncate::HeadTail => {
                    assert_eq!((shown[0], *shown.last().unwrap()), ("line001", "line100"));
                }
            }
            assert!(out.text.contains(&format!("log lines {first}-{last}")));
        }
    }

    #[test]
    fn lines_split_between_windows_yield_no_phantom_lines() {
        let mut cleaner = Cleaner::default();
        let spec = || WindowSpec {
            truncate: Truncate::HeadTail,
            budget: LARGE_BUDGET,
            raw: false,
            clean: true,
            keep_nul: false,
            encoding: encoding_rs::UTF_8,
            source: "log",
        };
        let mut first = WindowBuilder::new(spec(), &mut cleaner, 1);
        first.push(b"Password:\r");
        assert_eq!(first.finish().text, "Password:");
        let mut second = WindowBuilder::new(spec(), &mut cleaner, 1);
        second.push(b"\nnext\n");
        let out = second.finish();
        assert_eq!(out.text, "next");
        assert_eq!(out.newlines, 2);
    }

    #[test]
    fn matcher_finds_text_in_cleaned_output_however_it_is_split() {
        let utf8 = encoding_rs::UTF_8;
        type Case<'a> = (&'a [u8], &'a [&'a str], &'static Encoding, Option<usize>);
        let cases: [Case; 9] = [
            (
                b"\x1b[32mServer\x1b[0m listening\r\n",
                &["Server listening"],
                utf8,
                Some(0),
            ),
            (b"10%\r50%\r100%\r\x1b[Kdone", &["100%"], utf8, Some(0)),
            (b"Password: ", &["Password: "], utf8, Some(0)),
            (b"SERVER READY", &["server ready"], utf8, None),
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
            (b"\xe2\n", &["\u{fffd}"], utf8, Some(0)),
            (b"\xe2", &["\u{fffd}"], utf8, Some(0)),
        ];
        for (output, needles, encoding, expected) in cases {
            let needles: Vec<String> = needles.iter().map(|needle| needle.to_string()).collect();
            let matcher = || Matcher::new(&needles, encoding, Some(Cleaner::default()));
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
        let needles = ["first".to_string(), "second".to_string()];
        let mut matcher = Matcher::new(&needles, utf8, Some(Cleaner::default()));
        assert_eq!(matcher.push(b"second\nfirst\n"), Some(0));
    }

    #[test]
    fn matcher_preserves_codemode_control_bytes_at_every_chunk_boundary() {
        for (output, needle, expected) in [
            ("\x1b[31mready\n", "\x1b[31mready", Some(0)),
            ("left\x1b[31mright\n", "leftright", None),
            ("first\rsecond  \n", "first\rsecond  ", Some(0)),
            ("first\nsecond\n", "firstsecond", None),
        ] {
            let needles = [needle.to_owned()];
            for split in 0..=output.len() {
                let mut matcher = Matcher::new(&needles, encoding_rs::UTF_8, None);
                let found = matcher
                    .push(&output.as_bytes()[..split])
                    .or_else(|| matcher.push(&output.as_bytes()[split..]))
                    .or_else(|| matcher.finish());
                assert_eq!(found, expected, "{needle:?} split at {split}");
            }
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
    fn live_windows_hold_only_prefixes_that_more_bytes_can_complete() {
        for invalid in [
            &[0xc0][..],
            &[0xf5],
            &[0xe0, 0x80],
            &[0xf4, 0x90],
            &[0xed, 0xa0],
        ] {
            assert_eq!(incomplete_utf8_suffix(invalid), 0, "{invalid:x?}");
        }
        assert_eq!(incomplete_utf8_suffix(&[0xff, 0xf0, 0x90]), 2);
        assert_eq!(incomplete_legacy_suffix(&[0xff], encoding_rs::BIG5), 0);
        assert_eq!(
            incomplete_legacy_suffix(&[0xff, 0xa4], encoding_rs::BIG5),
            1
        );
        for len in 1..4 {
            let prefix = [0xff, 0x81, 0x30, 0x81, 0x30];
            assert_eq!(
                incomplete_legacy_suffix(&prefix[..=len], encoding_rs::GB18030),
                len
            );
        }
        assert_eq!(
            incomplete_legacy_suffix(&[0x81, 0x30, 0x81, 0x30], encoding_rs::GB18030),
            0
        );
    }

    #[test]
    fn unified_window_rules_cover_all_five_cases_and_exact_boundaries() {
        let json_10k = format!("{{\"payload\":\"{}\"}}", "a".repeat(9_986));
        assert_eq!(json_10k.len(), 10_000);
        for mode in [Truncate::HeadTail, Truncate::Head, Truncate::Tail] {
            let out = window(&[json_10k.as_bytes(), b"\n"], mode, 16_384, false);
            assert_eq!(out.text, json_10k);
            assert_eq!(out.cut_lines, 0);
            assert_eq!(out.omitted, None);
        }

        let exact_180 = format!("{}\n", "E".repeat(180));
        let over_181 = format!("{}\n", "E".repeat(181));
        for mode in [Truncate::Head, Truncate::Tail, Truncate::HeadTail] {
            let fit = window(&[exact_180.as_bytes()], mode, 180, false);
            assert_eq!(fit.text.len(), 180);
            assert_eq!(fit.cut_lines, 0);
            assert_eq!(fit.omitted, None);

            let cut = window(&[over_181.as_bytes()], mode, 180, false);
            assert_eq!(cut.text.len(), 180);
            assert_eq!(cut.cut_lines, 1);
            assert_eq!(cut.omitted, None);
            assert!(cut.text.contains("line cut at 180 bytes;"), "{}", cut.text);
        }

        let two_exact = b"aaa\nbbb\n";
        for mode in [Truncate::Head, Truncate::Tail, Truncate::HeadTail] {
            let fit = window(&[two_exact], mode, 7, false);
            assert_eq!(fit.text, "aaa\nbbb");
            assert_eq!(fit.cut_lines, 0);
            assert_eq!(fit.omitted, None);
        }

        let multi_200k = format!("{}\n", "🚀中".repeat(30_000));
        assert!(multi_200k.len() > 200 * 1024);
        for mode in [Truncate::Head, Truncate::Tail, Truncate::HeadTail] {
            let out = window(&[multi_200k.as_bytes()], mode, 16_384, false);
            assert!(out.text.len() <= 16_384);
            assert_eq!(out.cut_lines, 1);
            assert_eq!(out.omitted, None);
            assert!(out.text.contains("line cut at 16 KiB;"), "{}", out.text);
        }

        let head_over = format!("{}\nsecond\nthird\n", "H".repeat(300));
        let out_h = window(&[head_over.as_bytes()], Truncate::Head, 150, false);
        assert!(out_h.text.len() <= 150);
        assert_eq!(out_h.cut_lines, 1);
        assert_eq!(out_h.omitted, Some((2, 3)));
        assert!(out_h.text.contains("2 lines omitted: log lines 2-3"));

        let tail_over = format!("first\nsecond\n{}\n", "T".repeat(300));
        let out_t = window(&[tail_over.as_bytes()], Truncate::Tail, 150, false);
        assert!(out_t.text.len() <= 150);
        assert_eq!(out_t.cut_lines, 1);
        assert_eq!(out_t.omitted, Some((1, 2)));
        assert!(
            out_t
                .text
                .starts_with("... [2 lines omitted: log lines 1-2] ...\n")
        );

        let err_tail = format!("{}\nERROR: final line\n", "x".repeat(20_000));
        let out_ht = window(&[err_tail.as_bytes()], Truncate::HeadTail, 16_384, false);
        assert!(out_ht.text.len() <= 16_384);
        assert!(
            out_ht.text.ends_with("\nERROR: final line"),
            "{}",
            out_ht.text
        );
        assert_eq!(out_ht.cut_lines, 1);
        assert_eq!(out_ht.omitted, None);

        let mid_omit = format!(
            "{}\n{}\nEND-LINE-IMPORTANT\nRET\n",
            "x".repeat(20_000),
            "M".repeat(20_000)
        );
        let out_mid = window(&[mid_omit.as_bytes()], Truncate::HeadTail, 16_384, false);
        assert!(out_mid.text.len() <= 16_384);
        assert_eq!(out_mid.omitted, Some((2, 2)));
        assert_eq!(out_mid.cut_lines, 1);
        assert!(
            out_mid
                .text
                .contains("... [1 lines omitted: log lines 2-2] ...")
        );
        assert!(out_mid.text.ends_with("\nEND-LINE-IMPORTANT\nRET"));

        let ret_line = format!("{{\"tail\":\"{}\",\"done\":\"END\"}}", "V".repeat(12_000));
        let script2 = format!("{}\n{ret_line}\n", "H".repeat(15_000));
        let out_s2 = window(&[script2.as_bytes()], Truncate::HeadTail, 16_384, false);
        assert!(out_s2.text.len() <= 16_384);
        assert!(out_s2.text.ends_with(&format!("\n{ret_line}")));
        assert_eq!(out_s2.cut_lines, 1);
        assert_eq!(out_s2.omitted, None);
    }

    #[test]
    fn cleaned_and_transcoded_windows_count_omitted_display_bytes() {
        let budget = 250;
        let build = |input: &[u8], raw: bool, encoding| {
            let mut cleaner = Cleaner::default();
            let mut window = WindowBuilder::new(
                WindowSpec {
                    truncate: Truncate::HeadTail,
                    budget,
                    raw,
                    clean: true,
                    keep_nul: false,
                    encoding,
                    source: "log",
                },
                &mut cleaner,
                1,
            );
            window.push(input);
            window.finish()
        };
        for count in [500, 16000] {
            let mut padded = b"AB".to_vec();
            padded.resize(2 + count, b' ');
            let trimmed = build(&padded, false, encoding_rs::UTF_8);
            assert_eq!(
                (trimmed.text.as_str(), trimmed.cut_lines, trimmed.omitted),
                ("AB", 0, None)
            );

            padded.push(b'Z');
            let mut single_byte = vec![0xe9; count];
            single_byte.push(b'Z');
            for (line, raw, encoding, shown) in [
                (
                    &padded,
                    false,
                    encoding_rs::UTF_8,
                    String::from_utf8(padded.clone()).unwrap(),
                ),
                (
                    &single_byte,
                    true,
                    encoding_rs::WINDOWS_1252,
                    format!("{}Z", "é".repeat(count)),
                ),
            ] {
                let out = build(line, raw, encoding);
                let (kept, marker) = out.text.split_once(" … [").unwrap();
                assert!(shown.starts_with(kept));
                assert!(kept.len() + 50 > budget, "{}", out.text);
                assert_eq!(
                    marker,
                    format!(
                        "line cut at {budget} bytes; {} more bytes]",
                        shown.len() - kept.len()
                    )
                );
                assert_eq!((out.cut_lines, out.omitted), (1, None));
            }
        }

        let reset = format!("{}\rOK  \n", "x".repeat(16000));
        let out = build(reset.as_bytes(), false, encoding_rs::UTF_8);
        assert_eq!(
            (out.text.as_str(), out.cut_lines, out.omitted),
            ("OK", 0, None)
        );

        // When a multi-byte character crosses a small budget (< max_marker.len()),
        // prefix.len() < budget while dropped > 0: the cut marker must still appear.
        let mut cleaner = Cleaner::default();
        let mut small = WindowBuilder::new(
            WindowSpec {
                truncate: Truncate::Head,
                budget: 25,
                raw: false,
                clean: true,
                keep_nul: false,
                encoding: encoding_rs::UTF_8,
                source: "log",
            },
            &mut cleaner,
            1,
        );
        small.push(format!("{}🦀\n", "a".repeat(24)).as_bytes());
        let small_out = small.finish();
        assert_eq!(small_out.cut_lines, 1);
        assert!(small_out.text.ends_with(MARK), "{:?}", small_out.text);
        assert!(small_out.text.len() <= 25);
    }
}
