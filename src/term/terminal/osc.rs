//! OSC framing guard, terminal dimensions, reply sink, and shared transcript limits and marks.

use alacritty_terminal::event::{Event, EventListener};
use alacritty_terminal::grid::Dimensions;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, PoisonError};

/// Lines an alternate-screen session keeps to match redrawn rows against; older lines are
/// written out before they leave.
pub(super) const SESSION_LINES: usize = 10_000;
/// Normal-screen lines written out that a reprint after a cleared history is matched against.
pub(super) const REPRINT_REACH: usize = 100_000;
/// Lines with text that must repeat written lines in a row before a reprint is taken for one:
/// a screen of them, more than a command run again after `clear` usually prints, and never
/// more than the screen holds.
pub(super) const REPRINT_CONFIRM_LIMIT: usize = 30;

/// A line whose rows wrap for longer is written out in pieces this long, so a program that
/// prints without line breaks holds no more than this in memory.
pub(super) const LINE_LIMIT: usize = 1 << 20;

/// More than the 30 non-starters in Unicode's stream-safe normalization limit, while bounding
/// a cell that never advances the cursor. Private transcript marks do not consume this budget.
pub(super) const CELL_COMBINING_LIMIT: usize = 32;
/// OSC metadata does not need megabytes. Buffer at most this much before discarding the whole
/// control string, including across reads; the unabridged bytes still reach the task's raw log.
pub(super) const OSC_BYTES_LIMIT: usize = 8 * 1024;
/// Saved keyboard modes per screen. Programs normally need one entry per nested terminal
/// application. On overflow the oldest saved mode is discarded.
pub(super) const KEYBOARD_STACK_LIMIT: usize = 128;

#[derive(Default)]
enum OscState {
    #[default]
    Ground,
    Escape,
    Collect,
    Discard,
}

/// vte's std-enabled OSC parser has no allocation ceiling. Hold an OSC until it ends, then feed
/// it whole, or cancel the pending ESC and discard an oversized string through its terminator.
/// The terminator never turns discarded title/clipboard/hyperlink data into visible text.
#[derive(Default)]
pub(super) struct OscGuard {
    state: OscState,
    pub(super) pending: Vec<u8>,
    /// Forwarded parser offsets and raw-log offsets differ after dropping an oversized OSC.
    pub(super) forwarded: u64,
    discarded: u64,
    base_discarded: u64,
    /// (Forwarded position, cumulative discarded bytes). Adjacent gaps coalesce, and entries
    /// before the parser's synchronized-update buffer are retired after every read.
    pub(super) gaps: VecDeque<(u64, u64)>,
}

impl OscGuard {
    pub(super) fn filter<'a>(&mut self, bytes: &'a [u8]) -> Cow<'a, [u8]> {
        if matches!(self.state, OscState::Ground) && !bytes.contains(&0x1b) {
            self.forwarded += bytes.len() as u64;
            return Cow::Borrowed(bytes);
        }
        let mut output = Vec::with_capacity(bytes.len() + self.pending.len());
        for &byte in bytes {
            match self.state {
                OscState::Ground => {
                    output.push(byte);
                    if byte == 0x1b {
                        self.state = OscState::Escape;
                    }
                }
                OscState::Escape => {
                    if byte == b']' {
                        self.pending.push(byte);
                        self.state = OscState::Collect;
                    } else {
                        output.push(byte);
                        // vte executes C0 controls and ignores DEL/non-ASCII while waiting for
                        // an ESC final. An intermediate prevents ']' from starting an OSC.
                        if matches!(byte, 0x18 | 0x1a | 0x20..=0x7e) {
                            self.state = OscState::Ground;
                        }
                    }
                }
                OscState::Collect => {
                    if matches!(byte, 0x07 | 0x18 | 0x1a | 0x1b) {
                        output.append(&mut self.pending);
                        output.push(byte);
                        self.state = if byte == 0x1b {
                            OscState::Escape
                        } else {
                            OscState::Ground
                        };
                    } else if self.pending.len() < OSC_BYTES_LIMIT {
                        self.pending.push(byte);
                    } else {
                        // The current raw byte becomes CAN; the buffered bytes disappear.
                        self.skip(output.len(), self.pending.len());
                        self.pending.clear();
                        output.push(0x18); // Cancel the ESC already fed to the parser.
                        self.state = OscState::Discard;
                    }
                }
                OscState::Discard => match byte {
                    0x1b => {
                        output.push(byte);
                        self.state = OscState::Escape;
                    }
                    0x07 => {
                        self.skip(output.len(), 1);
                        self.state = OscState::Ground;
                    }
                    0x18 | 0x1a => {
                        output.push(byte);
                        self.state = OscState::Ground;
                    }
                    _ => self.skip(output.len(), 1),
                },
            }
        }
        self.forwarded += output.len() as u64;
        Cow::Owned(output)
    }

    fn skip(&mut self, output: usize, bytes: usize) {
        self.discarded += bytes as u64;
        let position = self.forwarded + output as u64;
        if let Some((last, discarded)) = self.gaps.back_mut()
            && *last == position
        {
            *discarded = self.discarded;
        } else {
            self.gaps.push_back((position, self.discarded));
        }
    }

    pub(super) fn raw_position(&self, forwarded: u64) -> u64 {
        let discarded = self
            .gaps
            .iter()
            .rev()
            .find(|&&(at, _)| at <= forwarded)
            .map_or(self.base_discarded, |&(_, discarded)| discarded);
        forwarded + discarded
    }

    pub(super) fn retire(&mut self, through: u64) {
        while self.gaps.front().is_some_and(|&(at, _)| at <= through) {
            self.base_discarded = self.gaps.pop_front().unwrap().1;
        }
    }
}

/// Begins each alternate-screen session in a transcript, among the normal screen's lines where
/// the session began.
pub const ALTERNATE_MARKER: &str = "--- alternate screen ---";
/// Follows an alternate-screen session when normal-screen lines come after it.
pub const NORMAL_MARKER: &str = "--- normal screen ---";

/// The normal-screen lines written out while still on screen, as an alternate-screen session
/// begins, carry a zero-width character from Unicode's private use planes 15 and 16 on their
/// first row: a mark moves with its row, a program never stores one as zero-width, and text
/// written over the row erases it.
const PLANE: u32 = 0xFFFE;
const MARK_PLANES: [u32; 2] = [0xF0000, 0x100000];

/// Mark number `serial`, reusing the marks after the last.
pub(super) fn mark_char(serial: usize) -> char {
    let serial = serial % (MARK_PLANES.len() * PLANE as usize);
    let (plane, index) = (serial / PLANE as usize, serial % PLANE as usize);
    char::from_u32(MARK_PLANES[plane] + index as u32).expect("a private use character")
}

pub(super) fn is_mark(c: char) -> bool {
    MARK_PLANES.iter().any(|&base| {
        (c as u32)
            .checked_sub(base)
            .is_some_and(|index| index < PLANE)
    })
}

/// The size of a task's terminal: its columns and screen rows.
#[derive(Clone, Copy)]
pub(super) struct Size {
    pub(super) columns: u16,
    pub(super) lines: u16,
}

impl Dimensions for Size {
    fn total_lines(&self) -> usize {
        self.screen_lines()
    }

    fn screen_lines(&self) -> usize {
        usize::from(self.lines)
    }

    fn columns(&self) -> usize {
        usize::from(self.columns)
    }
}

/// Rows drawn since the session's history last read the screen: none, one row, or more than one,
/// which is all any reader needs to know.
#[derive(Clone, Default, PartialEq, Eq, Debug)]
pub(super) enum Drawn {
    #[default]
    None,
    One(usize),
    /// More than one row, which is all any reader needs to know.
    Many,
}

impl Drawn {
    /// Records that row `row` was written.
    pub(super) fn add(&mut self, row: usize) {
        match self {
            Drawn::None => *self = Drawn::One(row),
            Drawn::One(first) if *first == row => {}
            Drawn::One(_) => *self = Drawn::Many,
            Drawn::Many => {}
        }
    }

    pub(super) fn is_none(&self) -> bool {
        matches!(self, Drawn::None)
    }

    /// Only row `row` was written.
    pub(super) fn only(&self, row: usize) -> bool {
        matches!(self, Drawn::One(only) if *only == row)
    }

    pub(super) fn clear(&mut self) {
        *self = Drawn::None;
    }
}

/// The emulator's replies to the program, such as cursor-position reports. Requests that need
/// a host, such as the clipboard or colors, go unanswered.
#[derive(Clone, Default)]
pub(super) struct Replies(pub(super) Arc<Mutex<Vec<u8>>>);

impl EventListener for Replies {
    fn send_event(&self, event: Event) {
        if let Event::PtyWrite(text) = event {
            let mut replies = self.0.lock().unwrap_or_else(PoisonError::into_inner);
            replies.extend_from_slice(text.as_bytes());
        }
    }
}
