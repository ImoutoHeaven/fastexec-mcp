//! The terminal emulator of a PTY task. One engine shows the live screen, reports the
//! program's keyboard modes, answers its terminal queries, and records the transcript as the
//! output arrives.
//!
//! The transcript holds each line a program pushed off the normal screen once, in the order a
//! user scrolling up would read them, whether it scrolled away, moved up through a scroll
//! region, or was cleared with the screen; a reprint of written lines after the program
//! cleared the history is no new text. The alternate screen keeps no history, so each
//! alternate-screen session is merged from its frames. Lines are written out once they
//! settle; the rest, such as the screen, is pending until it settles or the output ends.

mod osc;
mod recorder;
#[cfg(test)]
mod tests;

use crate::term::frames::History;
use crate::term::keys::Modes;
use alacritty_terminal::grid::Dimensions;
use alacritty_terminal::term::{Config, Term, TermMode};
use alacritty_terminal::vte::ansi::Processor;
use osc::{CELL_COMBINING_LIMIT, OscGuard, Replies, Size};
use recorder::{Recorder, Reprint, frame_row};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::PoisonError;
use std::time::Instant;

const STOPPED: &str = "The terminal emulator of this task stopped after an internal failure;";

/// Kitty keyboard protocol enhancements, in the order of their flag bits (1, 2, 4, 8, 16).
const KITTY_FLAGS: [TermMode; 5] = [
    TermMode::DISAMBIGUATE_ESC_CODES,
    TermMode::REPORT_EVENT_TYPES,
    TermMode::REPORT_ALTERNATE_KEYS,
    TermMode::REPORT_ALL_KEYS_AS_ESC,
    TermMode::REPORT_ASSOCIATED_TEXT,
];

/// The emulator a PTY task's output feeds as it arrives.
pub struct Live {
    recorder: Recorder,
    parser: Processor,
    osc: OscGuard,
    replies: Replies,
    /// Output bytes read, including those a synchronized update still holds.
    read: u64,
    /// The emulator panicked on some output; it stays stopped and the task runs without it.
    stopped: bool,
}

impl Live {
    /// An emulator for a terminal of `cols` by `rows`.
    pub fn at(cols: u16, rows: u16) -> Live {
        let replies = Replies::default();
        let config = Config {
            // One operation pushes at most a screen of rows, and the recorder takes the history
            // after each.
            scrolling_history: usize::from(rows),
            kitty_keyboard: true,
            ..Config::default()
        };
        let size = Size {
            columns: cols,
            lines: rows,
        };
        Live {
            recorder: Recorder::new(Term::new(config.clone(), &size, replies.clone()), config),
            parser: Processor::new(),
            osc: OscGuard::default(),
            replies,
            read: 0,
            stopped: false,
        }
    }

    /// The terminal's size in columns and rows, as the emulator has it.
    pub fn size(&self) -> (u16, u16) {
        let (cols, rows) = self.recorder.size();
        (cols as u16, rows as u16)
    }

    /// The terminal is `cols` by `rows` from now on. The emulator is at the new size when this
    /// returns: the transcript settles at the old size, the reflow happens, and the rows it
    /// pushed off are written out. A stopped emulator keeps the size it had. The replies the
    /// output applied at the old size asked for are returned, for the caller to send to the
    /// program as it sends the replies of any other read.
    pub fn resize(&mut self, cols: u16, rows: u16) -> Vec<u8> {
        if self.stopped {
            return Vec::new();
        }
        let (cols, rows) = (usize::from(cols), usize::from(rows));
        // Output a synchronized update holds is old-size output: it is applied, and settled, at
        // the size the screen had when it arrived, before the emulator resizes.
        self.run(|recorder, parser| {
            recorder.stop_sync(parser);
            recorder.commit();
            recorder.resize(cols, rows);
        })
    }

    /// Reads one piece of output, read from the program at `arrived` after the output paused
    /// if `pause` says so, long or not, and returns the replies it asked for. A synchronized
    /// update (mode 2026) past its deadline ends first, so it cannot outlast its timeout while
    /// output keeps arriving.
    pub fn process(&mut self, output: &[u8], arrived: Instant, pause: Option<bool>) -> Vec<u8> {
        self.read += output.len() as u64;
        let expired = self
            .sync_deadline()
            .is_some_and(|deadline| deadline <= arrived);
        let output = self.osc.filter(output);
        self.run(|recorder, parser| {
            if expired {
                recorder.stop_sync(parser);
            }
            if let Some(long) = pause {
                recorder.pause(long);
            }
            parser.advance(recorder, &output);
            recorder.commit();
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
        self.run(|recorder, parser| {
            recorder.stop_sync(parser);
            recorder.commit();
        })
    }

    /// The output ended: every pending line is written out.
    pub fn finish(&mut self) {
        self.run(|recorder, _| recorder.finish());
    }

    /// The transcript lines written out since the last call, each ending in a newline.
    pub fn take_written(&mut self) -> String {
        std::mem::take(&mut self.recorder.out)
    }

    fn screen_text_bytes(&self) -> usize {
        let (cols, rows) = self.recorder.size();
        // One base character and 32 combining characters, at most four UTF-8 bytes each.
        cols.saturating_mul(rows)
            .saturating_mul(4 * (1 + CELL_COMBINING_LIMIT))
            .saturating_add(rows)
    }

    /// Text workspace for rows, joins, rendered copies, and String growth.
    pub(crate) fn screen_snapshot_bytes(&self) -> usize {
        if self.stopped {
            return 0;
        }
        self.screen_text_bytes()
            .saturating_mul(8)
            .saturating_add(128 << 10)
    }

    /// Text workspace for the pending snapshot's clones, reconciliation, and final join.
    pub(crate) fn pending_snapshot_bytes(&self) -> usize {
        if self.stopped {
            return 0;
        }
        let recorder = &self.recorder;
        let retained = if recorder.alternate() {
            recorder
                .session
                .as_ref()
                .map_or(0, History::snapshot_text_bytes)
        } else {
            let held = match &recorder.reprint {
                Reprint::Anchoring { held, .. } => held.iter().fold(0usize, |bytes, text| {
                    bytes.saturating_add(text.capacity()).saturating_add(1)
                }),
                _ => 0,
            };
            held.saturating_add(
                recorder
                    .partial
                    .as_ref()
                    .map_or(0, |(text, _)| text.capacity().saturating_add(1)),
            )
        };
        // Capacity and a newline per retained string cover empty rows too. The factor
        // covers each phase's text copies and geometric String growth, not total RSS.
        retained
            .saturating_add(self.screen_text_bytes())
            .saturating_mul(16)
            .saturating_add(128 << 10)
    }

    /// The lines not written out yet, as they would be written if the output ended now, each
    /// ending in a newline.
    pub fn pending(&mut self) -> String {
        if self.stopped {
            return String::new();
        }
        let recorder = &self.recorder;
        match catch_unwind(AssertUnwindSafe(|| recorder.pending())) {
            Ok(text) => text,
            Err(_) => {
                self.stopped = true;
                String::new()
            }
        }
    }

    /// The emulator stopped after an internal failure, and with it the transcript.
    pub fn stopped(&self) -> bool {
        self.stopped
    }

    fn run(&mut self, step: impl FnOnce(&mut Recorder, &mut Processor)) -> Vec<u8> {
        if !self.stopped {
            let (recorder, parser) = (&mut self.recorder, &mut self.parser);
            // A panic would end the capture thread, and the program would block on a full PTY.
            self.stopped = catch_unwind(AssertUnwindSafe(|| step(recorder, parser))).is_err();
        }
        let shown = self.osc.forwarded - self.parser.sync_bytes_count() as u64;
        self.osc.retire(shown);
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
        let mode = self.recorder.term.mode();
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
        let held = self.parser.sync_bytes_count() as u64;
        if held == 0 {
            self.read
        } else {
            self.osc.raw_position(self.osc.forwarded - held)
        }
    }

    /// The visible rows without trailing spaces, blank rows at the bottom dropped, and the
    /// zero-based cursor row and column.
    pub fn screen(&self) -> Result<(Vec<String>, (u16, u16)), String> {
        if self.stopped {
            return Err(format!("{STOPPED} poll still reads its output."));
        }
        let grid = self.recorder.term.grid();
        let mut rows: Vec<String> = (0..grid.screen_lines())
            .map(|line| frame_row(grid, line).text)
            .collect();
        while rows.last().is_some_and(String::is_empty) {
            rows.pop();
        }
        let point = grid.cursor.point;
        Ok((rows, (point.line.0 as u16, point.column.0 as u16)))
    }
}
