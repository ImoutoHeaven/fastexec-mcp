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
use std::io::Read;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;

/// Scrollback history kept while rendering a transcript; older lines are dropped.
pub const HISTORY_LINES: usize = 10_000;

/// One transcript render at a time: the history holds up to `HISTORY_LINES` rows of cells.
static RENDERING: Mutex<()> = Mutex::new(());

/// Separates the normal screen's history from the alternate screen in a transcript.
pub const ALTERNATE_MARKER: &str = "--- alternate screen ---";

const STOPPED: &str = "The terminal emulator of this task stopped after an internal failure;";

/// Kitty keyboard protocol enhancements, in the order of their flag bits (1, 2, 4, 8, 16).
const KITTY_FLAGS: [TermMode; 5] = [
    TermMode::DISAMBIGUATE_ESC_CODES,
    TermMode::REPORT_EVENT_TYPES,
    TermMode::REPORT_ALTERNATE_KEYS,
    TermMode::REPORT_ALL_KEYS_AS_ESC,
    TermMode::REPORT_ASSOCIATED_TEXT,
];

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
    replies: Replies,
    /// Output bytes read, including those a synchronized update still holds.
    read: u64,
    /// The emulator panicked on some output; it stays stopped and the task runs without it.
    stopped: bool,
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
            kitty_keyboard: true,
            ..Config::default()
        };
        Live {
            term: Term::new(config, &Size, replies.clone()),
            parser: Processor::new(),
            replies,
            read: 0,
            stopped: false,
        }
    }

    /// Reads one piece of output and returns the replies it asked for. A synchronized update
    /// (mode 2026) past its deadline ends first, so it cannot outlast its timeout while output
    /// keeps arriving.
    pub fn process(&mut self, output: &[u8]) -> Vec<u8> {
        self.read += output.len() as u64;
        let expired = self
            .sync_deadline()
            .is_some_and(|deadline| deadline <= Instant::now());
        self.run(|term, parser| {
            if expired {
                parser.stop_sync(term);
            }
            parser.advance(term, output);
        })
    }

    /// When the synchronized update in progress times out, if one is in progress.
    pub fn sync_deadline(&self) -> Option<Instant> {
        if self.stopped {
            return None;
        }
        self.parser.sync_timeout().sync_timeout()
    }

    /// Ends the synchronized update in progress, applying the output it held, and returns the
    /// replies that output asked for.
    pub fn end_sync(&mut self) -> Vec<u8> {
        self.run(|term, parser| parser.stop_sync(term))
    }

    fn run(&mut self, step: impl FnOnce(&mut Term<Replies>, &mut Processor)) -> Vec<u8> {
        if !self.stopped {
            let (term, parser) = (&mut self.term, &mut self.parser);
            // A panic would end the capture thread, and the program would block on a full PTY.
            self.stopped = catch_unwind(AssertUnwindSafe(|| step(term, parser))).is_err();
        }
        let mut replies = self
            .replies
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        std::mem::take(&mut *replies)
    }

    /// The program's keyboard modes, which a stopped emulator no longer knows.
    pub fn modes(&self) -> Result<Modes, String> {
        if self.stopped {
            return Err(format!(
                "{STOPPED} keys need its keyboard modes; send the bytes with input instead."
            ));
        }
        let mode = self.term.mode();
        Ok(Modes {
            cursor: mode.contains(TermMode::APP_CURSOR),
            keypad: mode.contains(TermMode::APP_KEYPAD),
            kitty: KITTY_FLAGS
                .iter()
                .enumerate()
                .filter(|(_, flag)| mode.contains(**flag))
                .fold(0, |flags, (bit, _)| flags | 1 << bit),
        })
    }

    /// Output bytes the screen shows: those read, without the ones a synchronized update holds.
    pub fn shown(&self) -> u64 {
        self.read - self.parser.sync_bytes_count() as u64
    }

    /// The visible rows without trailing spaces, blank rows at the bottom dropped, and the
    /// zero-based cursor row and column.
    pub fn screen(&self) -> Result<(Vec<String>, (u16, u16)), String> {
        if self.stopped {
            return Err(format!(
                "{STOPPED} poll and transcript still read its output."
            ));
        }
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
        Ok((rows, (point.line.0 as u16, point.column.0 as u16)))
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
    let mut buffer = vec![0_u8; 64 * 1024];
    let mut remaining = len;
    while remaining > 0 {
        let want = remaining.min(buffer.len() as u64) as usize;
        let read = log.read(&mut buffer[..want])?;
        if read == 0 {
            break;
        }
        parser.advance(&mut term, &buffer[..read]);
        remaining -= read as u64;
    }
    // The log keeps no timing: a synchronized update still open at its end shows what it holds.
    parser.stop_sync(&mut term);
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
    fn a_moved_wide_character_placeholder_reads_as_a_blank() {
        // The wide character does not fit in the last column, which holds its placeholder;
        // deleting a character moves the placeholder left, and `z` lands in the last column.
        let log = format!("{}\u{4e2d}\x1b[1;1H\x1b[P\x1b[120Gz", "x".repeat(119));
        let text = render_text(log.as_bytes());
        assert_eq!(text, format!("{} z\n\u{4e2d}\n", "x".repeat(118)));
    }

    #[test]
    fn output_renders_with_its_full_terminal_meaning() {
        let osc = format!("\x1b]0;{}\x07ok", "t".repeat((1 << 20) + 10));
        let dcs = format!("\x1bPq{}\x1b\\ok", "d".repeat((1 << 20) + 10));
        for (log, expected) in [
            // A repeat runs its whole count, wrapping onto the next rows and scrolling the
            // screen into history.
            (
                "x\x1b[4000b\r\ny".to_string(),
                format!("{}\ny\n", "x".repeat(4001)),
            ),
            // A repeat of a combining mark stacks the mark on its base.
            (
                "e\u{301}\x1b[3b.".to_string(),
                format!("e{}.\n", "\u{301}".repeat(4)),
            ),
            (
                format!("e{}", "\u{301}".repeat(100)),
                format!("e{}\n", "\u{301}".repeat(100)),
            ),
            // Strings longer than a MiB end where the program ends them, not earlier.
            (osc, "ok\n".to_string()),
            (dcs, "ok\n".to_string()),
        ] {
            let text = render_text(log.as_bytes());
            assert!(text == expected, "{:?}", &text[..text.len().min(200)]);
        }
    }

    #[test]
    fn a_synchronized_update_shows_once_it_ends_across_reads() {
        let mut live = Live::new();
        live.process(b"before\r\n\x1b[?2026h");
        live.process(b"held");
        let (rows, _) = live.screen().unwrap();
        assert_eq!(rows, ["before"]);
        assert_eq!(live.shown(), 16, "the held bytes are not shown yet");
        assert!(live.sync_deadline().is_some());
        live.process(b"\x1b[?2026l");
        let (rows, _) = live.screen().unwrap();
        assert_eq!(rows, ["before", "held"]);
        assert_eq!(live.shown(), 28);
        assert!(live.sync_deadline().is_none());
    }
}
