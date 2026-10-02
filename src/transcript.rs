//! Renders a PTY task's log as a terminal shows it: the scrollback history, then the screen.
//!
//! A terminal emulator replays every stored byte, so the lines a program pushed off the screen,
//! by scrolling, by inserting through a scroll region, or by a full redraw that clears the
//! history first, come back once each, in the order a user scrolling up would read them.

use crate::process::{PTY_COLS, PTY_ROWS};
use alacritty_terminal::event::VoidListener;
use alacritty_terminal::grid::Dimensions;
use alacritty_terminal::index::{Column, Line};
use alacritty_terminal::term::cell::Flags;
use alacritty_terminal::term::{Config, Term, TermMode};
use alacritty_terminal::vte::ansi::Processor;
use alacritty_terminal::vte::{self, Params};
use std::io::Read;
use std::path::Path;
use std::sync::{Mutex, PoisonError};
use unicode_width::UnicodeWidthChar;

/// Scrollback history kept while rendering; older lines are dropped.
pub const HISTORY_LINES: usize = 10_000;

/// One render at a time: the history holds up to `HISTORY_LINES` rows of 24-byte cells.
static RENDERING: Mutex<()> = Mutex::new(());

pub struct Transcript {
    /// Logical lines: rows the terminal wrapped are joined, trailing spaces trimmed.
    pub text: String,
    pub lines: usize,
    /// The program ended on the alternate screen, which has no history.
    pub alternate_screen: bool,
    /// The history reached `HISTORY_LINES`, so older lines may be missing.
    pub history_full: bool,
}

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
    let mut limited = Vec::with_capacity(buffer.len());
    let mut remaining = len;
    while remaining > 0 {
        let want = remaining.min(buffer.len() as u64) as usize;
        let read = log.read(&mut buffer[..want])?;
        if read == 0 {
            break;
        }
        limited.clear();
        limiter.feed(&buffer[..read], &mut limited);
        parser.advance(&mut term, &limited);
        remaining -= read as u64;
    }
    // Output held back for a synchronized update that never ended is shown as received.
    parser.stop_sync(&mut term);
    let alternate_screen = term.mode().contains(TermMode::ALT_SCREEN);
    let alternate = alternate_screen.then(|| {
        let rows = rows(&term);
        // The normal screen and its history sit behind the alternate screen.
        term.swap_alt();
        rows
    });
    let history_full = term.grid().history_size() >= HISTORY_LINES;
    let mut lines = rows(&term);
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

/// The active grid's history and screen as logical lines, without trailing blank lines.
fn rows(term: &Term<VoidListener>) -> Vec<String> {
    let grid = term.grid();
    let columns = grid.columns();
    let mut lines = Vec::new();
    let mut line = String::new();
    for index in -(grid.history_size() as i32)..grid.screen_lines() as i32 {
        let row = &grid[Line(index)];
        for column in 0..columns {
            let cell = &row[Column(column)];
            if cell
                .flags
                .intersects(Flags::WIDE_CHAR_SPACER | Flags::LEADING_WIDE_CHAR_SPACER)
            {
                continue;
            }
            // A tab leaves `\t` in its first cell and blanks up to the tab stop.
            line.push(if cell.c == '\t' { ' ' } else { cell.c });
            line.extend(cell.zerowidth().into_iter().flatten());
        }
        if !row[Column(columns - 1)].flags.contains(Flags::WRAPLINE) {
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

/// Longest repeat (`CSI n b`) passed on: one row.
const MAX_REPEAT: u16 = PTY_COLS;
/// A CSI final byte the emulator ignores, which ends a repeat sequence that is dropped.
const IGNORED_FINAL: u8 = b'Y';

/// Cuts repeats (`CSI n b`), which print the last character up to 65535 times: a few bytes of
/// them could cost minutes of rendering, and a repeated zero-width character stacks on one cell
/// without limit. A repeat longer than a row becomes a row, and a repeat of a zero-width
/// character is dropped. The emulator's own parser reads every byte here too, so a repeat is
/// found exactly where the emulator would run it; every other byte passes unchanged.
#[derive(Default)]
struct Limiter {
    parser: vte::Parser,
    watch: Watch,
}

#[derive(Default)]
struct Watch {
    /// The last printed character, which a repeat prints again, has no width.
    last_zero_width: bool,
    /// The count of a repeat whose final byte the parser just read.
    repeat: Option<u16>,
}

impl vte::Perform for Watch {
    fn print(&mut self, c: char) {
        self.last_zero_width = c.width() == Some(0);
    }

    fn csi_dispatch(&mut self, params: &Params, intermediates: &[u8], ignore: bool, action: char) {
        // As the emulator reads a repeat, a missing or zero count meaning 1.
        if action == 'b' && intermediates.is_empty() && !ignore {
            let count = params.iter().next().map_or(0, |param| param[0]);
            self.repeat = Some(count.max(1));
        }
    }
}

impl Limiter {
    fn feed(&mut self, bytes: &[u8], out: &mut Vec<u8>) {
        for &byte in bytes {
            self.parser.advance(&mut self.watch, &[byte]);
            match self.watch.repeat.take() {
                Some(count) if self.watch.last_zero_width || count > MAX_REPEAT => {
                    // The emulator has read the sequence up to its final byte; another final
                    // ends it without effect, and a row-long repeat follows when one fits.
                    out.push(IGNORED_FINAL);
                    if !self.watch.last_zero_width {
                        out.extend_from_slice(format!("\x1b[{MAX_REPEAT}b").as_bytes());
                    }
                }
                _ => out.push(byte),
            }
        }
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
