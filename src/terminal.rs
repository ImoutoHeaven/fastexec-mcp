//! The terminal emulator of a PTY task. One engine shows the live screen, reports the
//! program's keyboard modes, answers its terminal queries, and replays the stored log into a
//! transcript.
//!
//! The transcript keeps a scrollback history, so the lines a program pushed off the screen,
//! by scrolling, by inserting through a scroll region, or by a full redraw that clears the
//! history first, come back once each, in the order a user scrolling up would read them.

use crate::keys::Modes;
use crate::process::{PTY_COLS, PTY_ROWS};
use alacritty_terminal::event::{Event, EventListener, VoidListener};
use alacritty_terminal::grid::{Dimensions, Grid, Row};
use alacritty_terminal::index::{Column, Line};
use alacritty_terminal::term::cell::{Cell, Flags};
use alacritty_terminal::term::{Config, Term, TermMode};
use alacritty_terminal::vte::ansi::Processor;
use alacritty_terminal::vte::{self, Params};
use std::io::Read;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use unicode_width::UnicodeWidthChar;

/// Scrollback history kept while rendering a transcript; older lines are dropped.
pub const HISTORY_LINES: usize = 10_000;

/// One transcript render at a time: the history holds up to `HISTORY_LINES` rows of cells.
static RENDERING: Mutex<()> = Mutex::new(());

/// Separates the normal screen's history from the alternate screen in a transcript.
pub const ALTERNATE_MARKER: &str = "--- alternate screen ---";

struct Size;

impl Dimensions for Size {
    fn total_lines(&self) -> usize {
        self.screen_lines()
    }

    fn screen_lines(&self) -> usize {
        usize::from(PTY_ROWS)
    }

    fn columns(&self) -> usize {
        usize::from(PTY_COLS)
    }
}

/// The emulator a PTY task's output feeds as it arrives.
pub struct Live {
    term: Term<Replies>,
    parser: Processor,
    limiter: Limiter,
    replies: Replies,
}

/// The emulator's replies to the program, such as cursor-position reports. Requests that need
/// a host, such as the clipboard or colors, go unanswered.
#[derive(Clone, Default)]
struct Replies(Arc<Mutex<Vec<u8>>>);

impl EventListener for Replies {
    fn send_event(&self, event: Event) {
        if let Event::PtyWrite(text) = event {
            let mut replies = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            replies.extend_from_slice(text.as_bytes());
        }
    }
}

impl Live {
    pub fn new() -> Live {
        let replies = Replies::default();
        let config = Config {
            scrolling_history: 0,
            ..Config::default()
        };
        Live {
            term: Term::new(config, &Size, replies.clone()),
            parser: Processor::new(),
            limiter: Limiter::default(),
            replies,
        }
    }

    /// Reads one piece of output and returns the replies it asked for.
    pub fn process(&mut self, output: &[u8]) -> Vec<u8> {
        run(&mut self.term, &mut self.parser, &mut self.limiter, output);
        let mut replies = self
            .replies
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        std::mem::take(&mut *replies)
    }

    /// The program's keyboard modes.
    pub fn modes(&self) -> Modes {
        let mode = self.term.mode();
        Modes {
            cursor: mode.contains(TermMode::APP_CURSOR),
            keypad: mode.contains(TermMode::APP_KEYPAD),
        }
    }

    /// The visible rows without trailing spaces, blank rows at the bottom dropped, and the
    /// zero-based cursor row and column.
    pub fn screen(&self) -> (Vec<String>, (u16, u16)) {
        let grid = self.term.grid();
        let mut rows: Vec<String> = (0..grid.screen_lines() as i32)
            .map(|line| {
                let mut text = row_text(&grid[Line(line)]);
                text.truncate(text.trim_end().len());
                text
            })
            .collect();
        while rows.last().is_some_and(String::is_empty) {
            rows.pop();
        }
        let point = grid.cursor.point;
        (rows, (point.line.0 as u16, point.column.0 as u16))
    }
}

/// Runs one piece of output through the limiter into the emulator, keeping each cell to its
/// text after every part.
fn run<L: EventListener>(
    term: &mut Term<L>,
    parser: &mut Processor,
    limiter: &mut Limiter,
    output: &[u8],
) {
    limiter.feed(output);
    for part in limiter.parts() {
        parser.advance(term, part);
        // ponytail: a synchronized update ends with each part, so a screen can show a frame
        // partly drawn; holding it until its end or a 150 ms timeout needs a timer.
        parser.stop_sync(term);
        keep_text(term.grid_mut());
    }
}

pub struct Transcript {
    /// Logical lines: rows the terminal wrapped are joined, trailing spaces trimmed.
    pub text: String,
    pub lines: usize,
    /// The program ended on the alternate screen, which has no history.
    pub alternate_screen: bool,
    /// The history reached `HISTORY_LINES`, so older lines may be missing.
    pub history_full: bool,
}

/// Renders the first `len` bytes of `log` and writes the text to `path`.
pub fn render(mut log: impl Read, len: u64, path: &Path) -> std::io::Result<Transcript> {
    let _rendering = RENDERING.lock().unwrap_or_else(PoisonError::into_inner);
    let config = Config {
        scrolling_history: HISTORY_LINES,
        ..Config::default()
    };
    let mut term = Term::new(config, &Size, VoidListener);
    let mut parser: Processor = Processor::new();
    let mut limiter = Limiter::default();
    let mut buffer = vec![0_u8; 64 * 1024];
    let mut remaining = len;
    while remaining > 0 {
        let want = remaining.min(buffer.len() as u64) as usize;
        let read = log.read(&mut buffer[..want])?;
        if read == 0 {
            break;
        }
        run(&mut term, &mut parser, &mut limiter, &buffer[..read]);
        remaining -= read as u64;
    }
    let alternate_screen = term.mode().contains(TermMode::ALT_SCREEN);
    let alternate = alternate_screen.then(|| {
        let lines = lines(term.grid());
        // The normal screen and its history sit behind the alternate screen.
        term.swap_alt();
        lines
    });
    let history_full = term.grid().history_size() >= HISTORY_LINES;
    let mut lines = lines(term.grid());
    if let Some(alternate) = alternate {
        lines.push(ALTERNATE_MARKER.to_string());
        lines.extend(alternate);
    }
    let mut text = lines.join("\n");
    text.push('\n');
    std::fs::write(path, &text)?;
    Ok(Transcript {
        text,
        lines: lines.len(),
        alternate_screen,
        history_full,
    })
}

/// A grid's history and screen as logical lines, without trailing blank lines.
fn lines(grid: &Grid<Cell>) -> Vec<String> {
    let last = Column(grid.columns() - 1);
    let mut lines = Vec::new();
    let mut line = String::new();
    for index in -(grid.history_size() as i32)..grid.screen_lines() as i32 {
        let row = &grid[Line(index)];
        line.push_str(&row_text(row));
        if !row[last].flags.contains(Flags::WRAPLINE) {
            line.truncate(line.trim_end().len());
            lines.push(std::mem::take(&mut line));
        }
    }
    if !line.is_empty() {
        line.truncate(line.trim_end().len());
        lines.push(line);
    }
    while lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    lines
}

/// A row as shown: a wide character once, a tab as a space, combining marks after their base.
fn row_text(row: &Row<Cell>) -> String {
    let mut text = String::new();
    let mut after_wide = false;
    let last = row.len() - 1;
    for (column, cell) in row[..].iter().enumerate() {
        // A spacer holds the second column of the wide character before it, or the last
        // column of a row whose wide character wrapped; an edit can move one elsewhere,
        // where it shows as the blank it is.
        let spacer = (column == last && cell.flags.contains(Flags::LEADING_WIDE_CHAR_SPACER))
            || (after_wide && cell.flags.contains(Flags::WIDE_CHAR_SPACER));
        after_wide = cell.flags.contains(Flags::WIDE_CHAR);
        if spacer {
            continue;
        }
        // A tab leaves `\t` in its first cell and blanks up to the tab stop.
        text.push(if cell.c == '\t' { ' ' } else { cell.c });
        text.extend(cell.zerowidth().into_iter().flatten());
    }
    text
}

/// Most zero-width characters a cell keeps.
const MAX_ZERO_WIDTH: usize = 32;

/// Keeps each visible cell to its text: at most `MAX_ZERO_WIDTH` zero-width characters, and
/// no hyperlink or underline color, which text shows neither of. Zero-width characters stack
/// on their cell without limit and a program can return to a cell, so this runs after each
/// part of output, and before the screen switches, while its cells are still reachable; a cell
/// that part scrolled into history keeps at most the part's worth.
fn keep_text(grid: &mut Grid<Cell>) {
    grid.cursor.template.extra = None;
    grid.saved_cursor.template.extra = None;
    for line in 0..grid.screen_lines() as i32 {
        for cell in &mut grid[Line(line)][..] {
            let Some(marks) = cell.zerowidth() else {
                continue;
            };
            let kept = marks[..marks.len().min(MAX_ZERO_WIDTH)].to_vec();
            if kept.len() == marks.len()
                && cell.hyperlink().is_none()
                && cell.underline_color().is_none()
            {
                continue;
            }
            cell.extra = None;
            for mark in kept {
                cell.push_zerowidth(mark);
            }
        }
    }
}

/// Longest repeat (`CSI n b`) and backward tabulation (`CSI n Z`) passed on: one row.
const MAX_COUNT: u16 = PTY_COLS;
/// Most bytes passed on without the parser acting on any, as inside a string (OSC) that the
/// parsers collect until it ends.
const MAX_STRING: usize = 1 << 20;
/// A CSI final byte the emulator ignores, which ends a sequence that is cut.
const IGNORED_FINAL: u8 = b'Y';

/// Bounds what a few bytes of output can cost the emulator, which acts on counts up to 65535
/// and keeps what it is told. A repeat (`CSI n b`) or backward tabulation (`CSI n Z`) longer
/// than a row becomes a row, a repeat of a zero-width character is dropped, a title pushed
/// onto the title stack (`CSI 22 t`) is dropped, and a string that runs past `MAX_STRING`
/// bytes is ended with BEL. The emulator's own parser reads every byte here too, so each cut
/// lands exactly where the emulator would act; every other byte passes unchanged. The output
/// is split in parts right before the final byte of each switch between the normal and the
/// alternate screen.
#[derive(Default)]
struct Limiter {
    parser: vte::Parser,
    watch: Watch,
    out: Vec<u8>,
    /// Where the parts of `out` end, besides its end.
    splits: Vec<usize>,
    /// Bytes read since the parser last acted on one.
    quiet: usize,
}

#[derive(Default)]
struct Watch {
    /// The last printed character has no width.
    last_zero_width: bool,
    /// The parser acted on the byte it just read.
    acted: bool,
    /// What replaces a sequence whose final byte the parser just read, after a final byte
    /// that ends it without effect.
    cut: Option<String>,
    /// The sequence whose final byte the parser just read switches screens.
    switch: bool,
}

impl vte::Perform for Watch {
    fn print(&mut self, c: char) {
        self.acted = true;
        self.last_zero_width = c.width() == Some(0);
    }

    fn execute(&mut self, _: u8) {
        self.acted = true;
    }

    fn hook(&mut self, _: &Params, _: &[u8], _: bool, _: char) {
        self.acted = true;
    }

    fn put(&mut self, _: u8) {
        self.acted = true;
    }

    fn unhook(&mut self) {
        self.acted = true;
    }

    fn osc_dispatch(&mut self, _: &[&[u8]], _: bool) {
        self.acted = true;
    }

    fn esc_dispatch(&mut self, _: &[u8], _: bool, _: u8) {
        self.acted = true;
    }

    fn csi_dispatch(&mut self, params: &Params, intermediates: &[u8], ignore: bool, action: char) {
        self.acted = true;
        if ignore {
            return;
        }
        if intermediates == b"?" && matches!(action, 'h' | 'l') {
            self.switch = params
                .iter()
                .any(|param| matches!(param[0], 47 | 1047 | 1049));
            return;
        }
        if !intermediates.is_empty() {
            return;
        }
        // As the emulator reads a count, a missing or zero count meaning 1.
        let count = params.iter().next().map_or(0, |param| param[0]).max(1);
        self.cut = match action {
            'b' if self.last_zero_width => Some(String::new()),
            'b' | 'Z' if count > MAX_COUNT => Some(format!("\x1b[{MAX_COUNT}{action}")),
            't' if count == 22 => Some(String::new()),
            _ => None,
        };
    }
}

impl Limiter {
    /// Takes `bytes` in, with the cuts made, for `parts` to pass on.
    fn feed(&mut self, bytes: &[u8]) {
        self.out.clear();
        self.splits.clear();
        for &byte in bytes {
            self.parser.advance(&mut self.watch, &[byte]);
            if std::mem::take(&mut self.watch.switch) {
                self.splits.push(self.out.len());
            }
            self.quiet = if std::mem::take(&mut self.watch.acted) {
                0
            } else {
                self.quiet + 1
            };
            match self.watch.cut.take() {
                Some(replacement) => {
                    self.out.push(IGNORED_FINAL);
                    self.out.extend_from_slice(replacement.as_bytes());
                }
                None => self.out.push(byte),
            }
            if self.quiet > MAX_STRING {
                self.parser.advance(&mut self.watch, b"\x07");
                self.watch.acted = false;
                self.quiet = 0;
                self.out.push(0x07);
            }
        }
    }

    /// The output of the last `feed`, in its parts.
    fn parts(&self) -> impl Iterator<Item = &[u8]> {
        let mut start = 0;
        let ends = self.splits.iter().copied().chain([self.out.len()]);
        ends.map(move |end| {
            let part = &self.out[start..end];
            start = end;
            part
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Renders `log` as the transcript action does, reading it in 64 KiB pieces.
    fn render_text(log: &[u8]) -> String {
        let path = std::env::temp_dir().join(format!(
            "fastexec-transcript-test-{}-{}.txt",
            std::process::id(),
            log.len()
        ));
        let transcript = render(log, log.len() as u64, &path).unwrap();
        let _ = std::fs::remove_file(&path);
        transcript.text
    }

    #[test]
    fn a_cell_keeps_its_bounded_marks_behind_the_alternate_screen() {
        let log = format!("e{}\x1b[?1049h", "\u{301}".repeat(100));
        let text = render_text(log.as_bytes());
        assert_eq!(
            text,
            format!("e{}\n{ALTERNATE_MARKER}\n", "\u{301}".repeat(32))
        );
    }

    #[test]
    fn a_moved_wide_character_placeholder_reads_as_a_blank() {
        // The wide character does not fit in the last column, which holds its placeholder;
        // deleting a character moves the placeholder left, and `z` lands in the last column.
        let log = format!("{}\u{4e2d}\x1b[1;1H\x1b[P\x1b[120Gz", "x".repeat(119));
        let text = render_text(log.as_bytes());
        assert_eq!(text, format!("{} z\n\u{4e2d}\n", "x".repeat(118)));
    }

    #[test]
    fn repeats_are_cut_where_the_emulator_runs_them_and_other_bytes_pass() {
        // A long repeat prints one row; a short one prints as asked.
        let text = render_text(b"x\x1b[65535b\r\ny\x1b[5b");
        assert_eq!(text, format!("{}\nyyyyyy\n", "x".repeat(121)));
        // A repeat of a zero-width character is dropped, also behind a byte the parser
        // ignores (DEL after ESC) and across the renderer's 64 KiB reads.
        let mut log = vec![b' '; 64 * 1024 - 4];
        log.extend_from_slice("e\u{301}\x1b\x7f[65535b.".as_bytes());
        assert!(render_text(&log).ends_with("e\u{301}.\n"));
        // A combining mark inside a DCS string is not printed, so the repeat after it repeats
        // the `x`; the line break after an ESC inside the string survives.
        let text = render_text(
            "x\x1bPq\x07\u{301}\x1b\\\x1b[5b.\r\nhello\x1bPq\x1b\r\n[0mworld".as_bytes(),
        );
        assert_eq!(text, "xxxxxx.\nhello\nworld\n");
    }
}
