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

use crate::frames::{History, Row as FrameRow};
use crate::keys::Modes;
use crate::process::{PTY_COLS, PTY_ROWS};
use alacritty_terminal::event::{Event, EventListener};
use alacritty_terminal::grid::{Dimensions, Grid, Row};
use alacritty_terminal::index::{Column, Line};
use alacritty_terminal::term::cell::{Cell, Flags};
use alacritty_terminal::term::{Config, Term, TermMode};
use alacritty_terminal::vte::ansi::cursor_icon::CursorIcon;
use alacritty_terminal::vte::ansi::{
    Attr, CharsetIndex, ClearMode, CursorShape, CursorStyle, Handler, Hyperlink, KeyboardModes,
    KeyboardModesApplyBehavior, LineClearMode, Mode, ModifyOtherKeys, NamedPrivateMode,
    PrivateMode, Processor, Rgb, ScpCharPath, ScpUpdateMode, StandardCharset, TabulationClearMode,
};
use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;
use unicode_width::UnicodeWidthChar;

/// Lines an alternate-screen session keeps to match redrawn rows against; older lines are
/// written out before they leave.
const SESSION_LINES: usize = 10_000;
/// Normal-screen lines written out that a reprint after a cleared history is matched against.
const REPRINT_REACH: usize = 100_000;
/// Lines with text that must repeat written lines in a row before a reprint is taken for one:
/// a screen of them, more than a command run again after `clear` usually prints, and never
/// more than the screen holds.
const REPRINT_CONFIRM_LIMIT: usize = 30;

/// A line whose rows wrap for longer is written out in pieces this long, so a program that
/// prints without line breaks holds no more than this in memory.
const LINE_LIMIT: usize = 1 << 20;

/// More than the 30 non-starters in Unicode's stream-safe normalization limit, while bounding
/// a cell that never advances the cursor. Private transcript marks do not consume this budget.
const CELL_COMBINING_LIMIT: usize = 32;
/// OSC metadata does not need megabytes. Buffer at most this much before discarding the whole
/// control string, including across reads; the unabridged bytes still reach the task's raw log.
const OSC_BYTES_LIMIT: usize = 8 * 1024;
/// Saved keyboard modes per screen. Programs normally need one entry per nested terminal
/// application. On overflow the oldest saved mode is discarded.
const KEYBOARD_STACK_LIMIT: usize = 128;

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
struct OscGuard {
    state: OscState,
    pending: Vec<u8>,
    /// Forwarded parser offsets and raw-log offsets differ after dropping an oversized OSC.
    forwarded: u64,
    discarded: u64,
    base_discarded: u64,
    /// (Forwarded position, cumulative discarded bytes). Adjacent gaps coalesce, and entries
    /// before the parser's synchronized-update buffer are retired after every read.
    gaps: VecDeque<(u64, u64)>,
}

impl OscGuard {
    fn filter<'a>(&mut self, bytes: &'a [u8]) -> Cow<'a, [u8]> {
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

    fn raw_position(&self, forwarded: u64) -> u64 {
        let discarded = self
            .gaps
            .iter()
            .rev()
            .find(|&&(at, _)| at <= forwarded)
            .map_or(self.base_discarded, |&(_, discarded)| discarded);
        forwarded + discarded
    }

    fn retire(&mut self, through: u64) {
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
fn mark_char(serial: usize) -> char {
    let serial = serial % (MARK_PLANES.len() * PLANE as usize);
    let (plane, index) = (serial / PLANE as usize, serial % PLANE as usize);
    char::from_u32(MARK_PLANES[plane] + index as u32).expect("a private use character")
}

fn is_mark(c: char) -> bool {
    MARK_PLANES.iter().any(|&base| {
        (c as u32)
            .checked_sub(base)
            .is_some_and(|index| index < PLANE)
    })
}

const STOPPED: &str = "The terminal emulator of this task stopped after an internal failure;";

/// Kitty keyboard protocol enhancements, in the order of their flag bits (1, 2, 4, 8, 16).
const KITTY_FLAGS: [TermMode; 5] = [
    TermMode::DISAMBIGUATE_ESC_CODES,
    TermMode::REPORT_EVENT_TYPES,
    TermMode::REPORT_ALTERNATE_KEYS,
    TermMode::REPORT_ALL_KEYS_AS_ESC,
    TermMode::REPORT_ASSOCIATED_TEXT,
];

/// The size of a task's terminal: its columns and screen rows.
#[derive(Clone, Copy)]
struct Size {
    columns: u16,
    lines: u16,
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
enum Drawn {
    #[default]
    None,
    One(usize),
    /// More than one row, which is all any reader needs to know.
    Many,
}

impl Drawn {
    /// Records that row `row` was written.
    fn add(&mut self, row: usize) {
        match self {
            Drawn::None => *self = Drawn::One(row),
            Drawn::One(first) if *first == row => {}
            Drawn::One(_) => *self = Drawn::Many,
            Drawn::Many => {}
        }
    }

    fn is_none(&self) -> bool {
        matches!(self, Drawn::None)
    }

    /// Only row `row` was written.
    fn only(&self, row: usize) -> bool {
        matches!(self, Drawn::One(only) if *only == row)
    }

    fn clear(&mut self) {
        *self = Drawn::None;
    }
}

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
    /// An emulator at the default size, which is what a task that names no size gets.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn new() -> Live {
        Live::at(PTY_COLS, PTY_ROWS)
    }

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

/// Digests of the normal-screen lines written out, the last `REPRINT_REACH` of them, by the
/// index of each among all.
#[derive(Default)]
struct Written {
    digests: VecDeque<u64>,
    first: usize,
}

impl Written {
    fn push(&mut self, text: &str) {
        self.digests.push_back(digest(text));
        if self.digests.len() > REPRINT_REACH {
            self.digests.pop_front();
            self.first += 1;
        }
    }

    fn get(&self, index: usize) -> Option<u64> {
        let k = index.checked_sub(self.first)?;
        self.digests.get(k).copied()
    }

    fn find(&self, digest: u64) -> Vec<usize> {
        self.digests
            .iter()
            .enumerate()
            .filter(|&(_, &d)| d == digest)
            .map(|(k, _)| self.first + k)
            .collect()
    }
}

/// A program that clears the history may print the lines written out again, as a full redraw
/// does: lines repeating written ones from some line on are copies, once `REPRINT_CONFIRM` with
/// text did in a row, until a line differs.
#[derive(Clone, Default)]
enum Reprint {
    #[default]
    Off,
    /// The history was cleared: lines that repeat written lines from each of `starts` on are
    /// held, `texts` of them with text.
    Anchoring {
        held: Vec<String>,
        starts: Vec<usize>,
        texts: usize,
    },
    /// A reprint is under way: the index of the written line the next line repeats.
    Following(usize),
}

impl Reprint {
    fn anchoring() -> Reprint {
        Reprint::Anchoring {
            held: Vec::new(),
            starts: Vec::new(),
            texts: 0,
        }
    }

    /// The lines `line` lets through to be written out: none while it repeats written lines.
    fn step(&mut self, written: &Written, line: String, confirm: usize) -> Vec<String> {
        let line_digest = digest(&line);
        match self {
            Reprint::Off => vec![line],
            Reprint::Following(at) => {
                if written.get(*at) == Some(line_digest) {
                    *at += 1;
                    Vec::new()
                } else {
                    *self = Reprint::Off;
                    vec![line]
                }
            }
            Reprint::Anchoring {
                held,
                starts,
                texts,
            } => {
                if held.is_empty() {
                    *starts = written.find(line_digest);
                } else {
                    let k = held.len();
                    starts.retain(|&start| written.get(start + k) == Some(line_digest));
                }
                if starts.is_empty() {
                    let mut out = std::mem::take(held);
                    out.push(line);
                    *self = Reprint::Off;
                    return out;
                }
                *texts += usize::from(!blank(&line));
                held.push(line);
                if *texts >= confirm.max(1) {
                    *self = Reprint::Following(starts[0] + held.len());
                }
                Vec::new()
            }
        }
    }

    /// Ends the matching: the lines held are no reprint after all.
    fn release(&mut self) -> Vec<String> {
        match std::mem::take(self) {
            Reprint::Anchoring { held, .. } => held,
            _ => Vec::new(),
        }
    }
}

/// A normal-screen line still on screen, or whose first rows left it.
struct ScreenLine {
    text: String,
    /// Bytes of `text` from rows that already left the screen.
    left: usize,
    /// The mark the line was written out with, if it carries one.
    mark: Option<char>,
    /// The row it starts on, and that row's mark.
    row: usize,
    row_mark: Option<char>,
}

/// The current kitty flags and the modes a push will later restore, independently per screen.
#[derive(Default)]
struct KeyboardStack {
    current: KeyboardModes,
    saved: VecDeque<KeyboardModes>,
}

impl KeyboardStack {
    fn push(&mut self, mode: KeyboardModes) {
        if self.saved.len() == KEYBOARD_STACK_LIMIT {
            self.saved.pop_front();
        }
        self.saved.push_back(self.current);
        self.current = mode;
    }

    fn pop(&mut self, count: u16) {
        let count = usize::from(count);
        if count > self.saved.len() {
            self.saved.clear();
            self.current = KeyboardModes::NO_MODE;
        } else if count != 0 {
            let restore = self.saved.len() - count;
            self.current = self.saved[restore];
            self.saved.truncate(restore);
        }
    }
}

/// The terminal as the program drives it, recording the transcript: the rows it pushes off the
/// normal screen, and the scrolls and frames of each alternate-screen session.
struct Recorder {
    term: Term<Replies>,
    keyboard: [KeyboardStack; 2],
    /// The history of the alternate-screen session on screen.
    session: Option<History>,
    /// The scroll region's first and last rows, zero-based.
    region: (usize, usize),
    /// Inside a synchronized update, whose drawing no one sees until it ends.
    held: bool,
    /// Times the program showed the cursor again after hiding it.
    redraws: usize,
    /// Rows written since the session's history last read the screen.
    drawn: Drawn,
    /// The frame read from the rows written alone, kept to reuse its memory.
    frame: Vec<FrameRow>,
    /// A line whose first rows left the screen while its last one wraps onto it, and the mark
    /// of its first row.
    partial: Option<(String, Option<char>)>,
    /// The screen row where the partial logical line continues.
    partial_row: usize,
    /// The text of the lines written out while on screen, by the mark on their first row.
    marked: HashMap<char, String>,
    /// Marks issued.
    marks: usize,
    /// An alternate-screen session ended, and no normal-screen line was written out since.
    after_session: bool,
    written: Written,
    reprint: Reprint,
    /// Lines with text that must repeat written ones before a reprint is taken for one.
    confirm: usize,
    /// The emulator's options, kept so the history limit follows the screen's height.
    config: Config,
    /// Lines written out and not taken yet, each ending in a newline.
    out: String,
}

impl Recorder {
    fn new(term: Term<Replies>, config: Config) -> Recorder {
        let rows = term.grid().screen_lines();
        Recorder {
            term,
            keyboard: Default::default(),
            session: None,
            region: (0, rows.saturating_sub(1)),
            held: false,
            redraws: 0,
            drawn: Drawn::None,
            frame: Vec::new(),
            partial: None,
            partial_row: 0,
            marked: HashMap::new(),
            marks: 0,
            after_session: false,
            written: Written::default(),
            reprint: Reprint::Off,
            confirm: rows.min(REPRINT_CONFIRM_LIMIT),
            config,
            out: String::new(),
        }
    }

    /// The terminal is `cols` by `rows` from now on. The transcript never loses text: the screen
    /// as it stands is written out at the old size, the scrollback that remains is cleared, and
    /// the recorder starts fresh at the new size. A row still on the screen after the resize can
    /// therefore reach the transcript a second time, which is what the contract allows.
    fn resize(&mut self, cols: usize, rows: usize) {
        let (old_cols, old_rows) = (self.term.grid().columns(), self.term.grid().screen_lines());
        if (cols, rows) == (old_cols, old_rows) || rows == 0 || cols == 0 {
            return;
        }
        let size = Size {
            columns: cols as u16,
            lines: rows as u16,
        };
        // Lines held as a possible reprint were not a reprint after all: the rows around them
        // moved.
        for line in self.reprint.release() {
            self.write_normal(&line);
        }
        if self.alternate() {
            // The alternate screen as it stands is one frame, written whole; its session ends
            // here and a new one starts at the new size.
            self.present();
            self.end_session(true);
            // Rows the inactive normal grid's history holds came from rows the session start
            // already wrote, so they are cleared rather than written twice.
            self.term.grid_mut().clear_history();
            self.term.resize(size);
            self.term.grid_mut().clear_history();
            self.session();
        } else {
            // Every row the screen pushed off, then the screen itself: the logical lines joined
            // over WRAPLINE, the partial line included.
            self.spill();
            self.write_screen();
            self.term.grid_mut().clear_history();
            self.term.resize(size);
            // The reflow's rows are rows already written.
            self.term.grid_mut().clear_history();
        }
        // The limit the terminal keeps rows under is the screen's height: one operation pushes at
        // most a screen of rows and the recorder takes them after each, so a pass through a
        // taller screen must not be truncated by the height it had before.
        self.set_history(rows);
        // Fresh means fresh: the marks written at the old size stay on those cells, so the map
        // starts empty and the counter at zero rather than carrying the old associations.
        self.marked.clear();
        self.marks = 0;
        self.partial = None;
        self.partial_row = 0;
        self.region = (0, rows - 1);
        self.drawn.clear();
        self.confirm = rows.min(REPRINT_CONFIRM_LIMIT);
    }

    /// The emulator's history limit changes; the rows it already holds stay.
    fn set_history(&mut self, lines: usize) {
        if self.config.scrolling_history == lines {
            return;
        }
        self.config.scrolling_history = lines;
        self.term.set_options(self.config.clone());
    }

    /// The terminal's size in columns and rows.
    fn size(&self) -> (usize, usize) {
        (self.term.grid().columns(), self.term.grid().screen_lines())
    }

    fn alternate(&self) -> bool {
        self.term.mode().contains(TermMode::ALT_SCREEN)
    }

    /// Keep only the current mode in the native stack. Its overflow branch in alacritty 0.26
    /// removes a title instead of a keyboard entry and can panic; nesting lives in our bounded
    /// stack. Updating the top also makes reports and screen swaps reflect direct flag changes.
    fn sync_keyboard_mode(&mut self) {
        let mode = self.keyboard[usize::from(self.alternate())].current;
        Handler::pop_keyboard_modes(&mut self.term, u16::MAX);
        Handler::push_keyboard_mode(&mut self.term, mode);
    }

    fn push(&mut self, line: &str) {
        self.out.push_str(line);
        self.out.push('\n');
    }

    /// Writes out the lines settled since the last call.
    fn commit(&mut self) {
        self.spill();
        self.prune_marks();
        let lines = self.session.as_mut().map(History::commit);
        for line in lines.into_iter().flatten() {
            self.push(&line);
        }
    }

    /// A program can overwrite a marked row without scrolling it out. Keep only associations
    /// that the normal grid or its unfinished logical line can still reference. While the
    /// alternate grid is active the unchanged normal grid is inaccessible, so leave its marks.
    fn prune_marks(&mut self) {
        if self.alternate() || self.marked.is_empty() {
            return;
        }
        let grid = self.term.grid();
        let mut present: HashSet<char> = (0..grid.screen_lines())
            .filter_map(|line| row_mark(&grid[Line(line as i32)]))
            .collect();
        if let Some((_, Some(mark))) = &self.partial {
            present.insert(*mark);
        }
        self.marked.retain(|mark, _| present.contains(mark));
    }

    /// The output ended: the session on screen ends, and the normal screen's lines are written.
    fn finish(&mut self) {
        self.present();
        if self.session.is_some() {
            self.end_session(true);
        } else {
            self.spill();
            self.write_screen();
        }
        for line in self.reprint.release() {
            self.write_normal(&line);
        }
    }

    /// The lines `finish` would write out now, leaving everything as it is.
    fn pending(&self) -> String {
        let lines = if self.alternate() {
            // No session is on screen once the output ended.
            self.session.clone().map_or_else(Vec::new, |mut session| {
                if self.shows_frame(false) {
                    session.present(&frame_rows(self.term.grid()));
                }
                session.finish()
            })
        } else {
            let mut reprint = self.reprint.clone();
            let mut lines = Vec::new();
            let confirm = self.confirm;
            for line in self.screen_lines() {
                if !self.already_written(&line) {
                    lines.extend(reprint.step(&self.written, line.text, confirm));
                }
            }
            lines.extend(reprint.release());
            if self.after_session && !lines.is_empty() {
                lines.insert(0, NORMAL_MARKER.to_string());
            }
            lines
        };
        lines.iter().map(|line| format!("{line}\n")).collect()
    }

    /// Writes out a normal-screen line that passed the reprint check.
    fn write_normal(&mut self, line: &str) {
        if std::mem::take(&mut self.after_session) {
            self.push(NORMAL_MARKER);
        }
        self.written.push(line);
        self.push(line);
    }

    /// Writes out a normal-screen line unless it repeats written lines.
    fn write_checked(&mut self, line: String) {
        let confirm = self.confirm;
        for line in self.reprint.step(&self.written, line, confirm) {
            self.write_normal(&line);
        }
    }

    /// Writes out the rows the terminal pushed into its history, as lines; a line whose last
    /// row wraps onto the screen waits in `partial`. A line written out while on screen, still
    /// as it was, is not written again.
    fn spill(&mut self) {
        let grid = self.term.grid();
        let size = grid.history_size();
        if size == 0 {
            return;
        }
        let rows: Vec<(String, bool, Option<char>)> = (1..=size as i32)
            .rev()
            .map(|k| {
                let row = &grid[Line(-k)];
                (row_text(row), wraps(grid, row), row_mark(row))
            })
            .collect();
        self.term.grid_mut().clear_history();
        self.write_rows(rows, 0);
    }

    /// Writes normal-screen rows using the same logical-line and reprint rules, whether they
    /// entered emulator history or left a partial scroll region without entering it.
    fn write_rows(&mut self, rows: Vec<(String, bool, Option<char>)>, continuation: usize) {
        for (text, wraps, mark) in rows {
            let (mut line, mark) = self.partial.take().unwrap_or((String::new(), mark));
            line.push_str(&text);
            if wraps && line.len() < LINE_LIMIT {
                self.partial = Some((line, mark));
                self.partial_row = continuation;
                continue;
            }
            if !wraps {
                line.truncate(line.trim_end().len());
            }
            let known = mark.and_then(|mark| self.marked.remove(&mark));
            if known.as_ref() != Some(&line) {
                self.write_checked(line);
            }
        }
    }

    /// The normal screen's lines, after `partial`, up to the last row with text.
    fn screen_lines(&self) -> Vec<ScreenLine> {
        let grid = self.term.grid();
        let last_text = (0..grid.screen_lines())
            .rev()
            .find(|&line| !blank(&row_text(&grid[Line(line as i32)])))
            .into_iter()
            .chain(self.partial.as_ref().map(|_| self.partial_row))
            .max();
        let mut lines = Vec::new();
        let Some(last_text) = last_text else {
            return lines;
        };
        let (mut text, mut mark) = (String::new(), None);
        let mut left = 0;
        let mut start = 0;
        for line in 0..=last_text {
            if line == self.partial_row
                && let Some((prefix, prefix_mark)) = &self.partial
            {
                text.push_str(prefix);
                mark = *prefix_mark;
                left = prefix.len();
            }
            let row = &grid[Line(line as i32)];
            if text.is_empty() && left == 0 {
                mark = row_mark(row);
            }
            text.push_str(&row_text(row));
            if wraps(grid, row) && line < last_text {
                continue;
            }
            text.truncate(text.trim_end().len());
            lines.push(ScreenLine {
                text: std::mem::take(&mut text),
                left,
                mark,
                row: start,
                row_mark: row_mark(&grid[Line(start as i32)]),
            });
            (left, start, mark) = (0, line + 1, None);
        }
        lines
    }

    fn already_written(&self, line: &ScreenLine) -> bool {
        line.mark
            .is_some_and(|mark| self.marked.get(&mark) == Some(&line.text))
    }

    /// Writes out the normal screen's lines, as an alternate-screen session begins or the
    /// output ends, and marks each, so it is not written again when it leaves the screen.
    fn write_screen(&mut self) {
        let lines = self.screen_lines();
        self.partial = None;
        self.write_screen_lines(lines);
    }

    /// Settles the partial line before an unrelated scroll can move its continuation.
    fn flush_partial(&mut self) {
        if let Some(line) = self.screen_lines().into_iter().find(|line| line.left != 0) {
            self.partial = None;
            self.write_screen_lines(vec![line]);
        }
    }

    fn write_screen_lines(&mut self, lines: Vec<ScreenLine>) {
        for line in lines {
            if !self.already_written(&line) {
                self.write_checked(line.text.clone());
            }
            let on_screen = line.text.get(line.left..).unwrap_or_default().to_string();
            if line
                .row_mark
                .is_some_and(|mark| self.marked.get(&mark) == Some(&on_screen))
            {
                continue;
            }
            self.marks += 1;
            let mark = mark_char(self.marks);
            let row = &mut self.term.grid_mut()[Line(line.row as i32)];
            clear_row_marks(row);
            // A glyph is a base character other than whitespace, or a combining mark on a blank;
            // a blank row carries its mark in its first cell.
            let column = row[..]
                .iter()
                .position(|cell| {
                    (!cell.c.is_whitespace() && cell.c != '\0')
                        || cell
                            .zerowidth()
                            .is_some_and(|marks| marks.iter().any(|&c| !is_mark(c)))
                })
                .unwrap_or(0);
            row[Column(column)].push_zerowidth(mark);
            self.marked.insert(mark, on_screen);
        }
        self.prune_marks();
    }

    /// The history of the alternate-screen session on screen.
    fn session(&mut self) -> Option<&mut History> {
        if !self.alternate() {
            return None;
        }
        let height = self.term.grid().screen_lines();
        Some(
            self.session
                .get_or_insert_with(|| History::new(height, SESSION_LINES)),
        )
    }

    /// The session ends once the alternate screen is gone, or with `now`: its lines are
    /// written out.
    fn end_session(&mut self, now: bool) {
        if !now && self.alternate() {
            return;
        }
        if let Some(session) = self.session.take() {
            for line in session.finish() {
                self.push(&line);
            }
            self.after_session = true;
        }
    }

    /// The cursor's row is being written.
    fn draw(&mut self) {
        self.drawn.add(self.cursor_line());
    }

    /// The screen is a finished frame, unless a synchronized update holds its drawing.
    fn present(&mut self) {
        if self.held || !self.alternate() {
            return;
        }
        let rows = frame_rows(self.term.grid());
        if let Some(session) = self.session() {
            session.present(&rows);
        }
        self.drawn.clear();
    }

    /// Where the output paused the screen shows a finished frame, unless the program hides the
    /// cursor while it draws, as it has before, and the cursor is hidden: then the pause fell
    /// inside a redraw, and the frame that shows the cursor again holds it whole. A long pause
    /// showed the screen long enough to read either way.
    fn shows_frame(&self, long: bool) -> bool {
        let hidden = !self.term.mode().contains(TermMode::SHOW_CURSOR);
        !self.held && (long || !hidden || self.redraws < 2)
    }

    fn pause(&mut self, long: bool) {
        if self.shows_frame(long) {
            self.present();
        }
    }

    /// Rows `top..end` scroll by `n` rows.
    fn shift(&mut self, top: usize, end: usize, n: usize, up: bool) {
        if !self.alternate() {
            if self.partial.is_some() && (!up || self.partial_row != top) {
                self.flush_partial();
            }
            // Only a region starting at row zero enters alacritty's scrollback.
            if up && top != 0 {
                let grid = self.term.grid();
                let rows = (top..top + n.min(end.saturating_sub(top)))
                    .map(|line| {
                        let row = &grid[Line(line as i32)];
                        (row_text(row), wraps(grid, row), row_mark(row))
                    })
                    .collect();
                self.write_rows(rows, top);
            }
            return;
        }
        // Rows written since the last frame show before they move: a frame of the screen, read
        // from the rows written alone when that is the cursor's row, as a program printing
        // lines writes it.
        if !self.held && !self.drawn.is_none() {
            let cursor = self.cursor_line();
            if self.drawn.only(cursor) {
                let row = frame_row(self.term.grid(), cursor);
                let mut frame = std::mem::take(&mut self.frame);
                if let Some(session) = self.session() {
                    let known = session.rows();
                    frame.truncate(known.len());
                    for (mine, known) in frame.iter_mut().zip(known) {
                        mine.clone_from(known);
                    }
                    frame.extend_from_slice(&known[frame.len()..]);
                    frame[cursor] = row;
                    session.present(&frame);
                }
                self.frame = frame;
                self.drawn.clear();
            } else {
                self.present();
            }
        }
        let n = n.min(end.saturating_sub(top));
        let gone = if up { top..top + n } else { end - n..end };
        let leaving: Option<Vec<FrameRow>> = (!self.held).then(|| {
            let grid = self.term.grid();
            gone.map(|line| frame_row(grid, line)).collect()
        });
        if let Some(session) = self.session() {
            session.shift(top, end, n, up, leaving.as_deref());
        }
    }

    /// One row scrolls up within the scroll region, as a line feed at its bottom does.
    fn feed(&mut self) {
        if self.cursor_line() == self.region.1 {
            self.shift(self.region.0, self.region.1 + 1, 1, true);
        }
    }

    fn cursor_line(&self) -> usize {
        self.term.grid().cursor.point.line.0 as usize
    }

    /// The next character or tab past the last column wraps, if the terminal wraps lines.
    fn wraps(&self) -> bool {
        self.term.grid().cursor.input_needs_wrap && self.term.mode().contains(TermMode::LINE_WRAP)
    }
}

/// Forwards `Handler` methods to the terminal unchanged.
macro_rules! forward {
    ($($name:ident($($arg:ident: $ty:ty),*);)*) => {
        $(fn $name(&mut self, $($arg: $ty),*) { Handler::$name(&mut self.term, $($arg),*) })*
    };
}

// The operations that can push rows into the normal screen's history write them out after.
impl Handler for Recorder {
    fn input(&mut self, c: char) {
        // As the terminal does: a character with width wraps past the last column, and a wide
        // character wraps from it; wrapping at the scroll region's bottom scrolls.
        let width = c.width().unwrap_or(0);
        if c.width() == Some(0) {
            // Match alacritty's zero-width placement, including a wide glyph's spacer and a
            // pending wrap. Only user text counts, so transcript row marks keep their meaning.
            let grid = self.term.grid();
            let mut column = grid.cursor.point.column;
            if !grid.cursor.input_needs_wrap {
                column.0 = column.0.saturating_sub(1);
            }
            let row = &grid[grid.cursor.point.line];
            if row[column].flags.contains(Flags::WIDE_CHAR_SPACER) {
                column.0 = column.0.saturating_sub(1);
            }
            let count = row[column]
                .zerowidth()
                .into_iter()
                .flatten()
                .filter(|&&mark| !is_mark(mark))
                .count();
            if count >= CELL_COMBINING_LIMIT {
                return;
            }
        }
        let last_column = self.term.grid().cursor.point.column.0 + 1 >= self.term.columns();
        let wide_wraps =
            width == 2 && last_column && self.term.mode().contains(TermMode::LINE_WRAP);
        if width > 0 && (self.wraps() || wide_wraps) {
            self.feed();
        }
        Handler::input(&mut self.term, c);
        self.draw();
        self.spill();
    }

    fn put_tab(&mut self, count: u16) {
        // A tab past the last column wraps.
        if self.wraps() {
            self.feed();
        }
        Handler::put_tab(&mut self.term, count);
        self.draw();
        self.spill();
    }

    fn insert_blank(&mut self, count: usize) {
        self.draw();
        Handler::insert_blank(&mut self.term, count);
    }

    fn erase_chars(&mut self, count: usize) {
        self.draw();
        Handler::erase_chars(&mut self.term, count);
    }

    fn delete_chars(&mut self, count: usize) {
        self.draw();
        Handler::delete_chars(&mut self.term, count);
    }

    fn clear_line(&mut self, mode: LineClearMode) {
        self.draw();
        Handler::clear_line(&mut self.term, mode);
    }

    fn linefeed(&mut self) {
        self.feed();
        Handler::linefeed(&mut self.term);
        self.spill();
    }

    fn newline(&mut self) {
        self.feed();
        Handler::newline(&mut self.term);
        self.spill();
    }

    fn reverse_index(&mut self) {
        if self.cursor_line() == self.region.0 {
            self.shift(self.region.0, self.region.1 + 1, 1, false);
        }
        Handler::reverse_index(&mut self.term);
    }

    fn scroll_up(&mut self, lines: usize) {
        self.shift(self.region.0, self.region.1 + 1, lines, true);
        Handler::scroll_up(&mut self.term, lines);
        self.spill();
    }

    fn scroll_down(&mut self, lines: usize) {
        self.shift(self.region.0, self.region.1 + 1, lines, false);
        Handler::scroll_down(&mut self.term, lines);
    }

    fn insert_blank_lines(&mut self, lines: usize) {
        let origin = self.cursor_line();
        if (self.region.0..=self.region.1).contains(&origin) {
            self.shift(origin, self.region.1 + 1, lines, false);
        }
        Handler::insert_blank_lines(&mut self.term, lines);
    }

    fn delete_lines(&mut self, lines: usize) {
        let origin = self.cursor_line();
        if (self.region.0..=self.region.1).contains(&origin) {
            self.shift(origin, self.region.1 + 1, lines, true);
        }
        Handler::delete_lines(&mut self.term, lines);
        self.spill();
    }

    // Erasing destroys what the screen showed, so the frame before it is recorded. Clearing
    // the normal screen pushes its rows into the history; clearing the history starts to
    // match a reprint against the lines written out.
    fn clear_screen(&mut self, mode: ClearMode) {
        self.present();
        if !self.alternate() && self.partial.is_some() && self.partial_row != 0 {
            self.flush_partial();
        }
        if matches!(mode, ClearMode::Saved) && !self.alternate() {
            for line in self.reprint.release() {
                self.write_normal(&line);
            }
            self.reprint = Reprint::anchoring();
        }
        Handler::clear_screen(&mut self.term, mode);
        self.spill();
    }

    fn decaln(&mut self) {
        self.present();
        Handler::decaln(&mut self.term);
    }

    // A reset erases the normal screen, so what it showed is written out first.
    fn reset_state(&mut self) {
        self.present();
        self.spill();
        if !self.alternate() {
            self.write_screen();
        }
        Handler::reset_state(&mut self.term);
        self.keyboard = Default::default();
        let (_, rows) = self.size();
        self.region = (0, rows.saturating_sub(1));
        self.held = false;
        self.end_session(false);
    }

    // Programs hide the cursor before they draw and show it once done, switch screens between
    // frames, and hold a frame's drawing in a synchronized update.
    fn set_private_mode(&mut self, mode: PrivateMode) {
        self.mode_change(mode, true);
        if mode == PrivateMode::Named(NamedPrivateMode::SwapScreenAndSetRestoreCursor)
            && !self.alternate()
        {
            self.write_screen();
            // Lines held as a possible reprint precede the session.
            for line in self.reprint.release() {
                self.write_normal(&line);
            }
            self.push(ALTERNATE_MARKER);
        }
        Handler::set_private_mode(&mut self.term, mode);
        self.session();
    }

    fn unset_private_mode(&mut self, mode: PrivateMode) {
        self.mode_change(mode, false);
        Handler::unset_private_mode(&mut self.term, mode);
        if mode == PrivateMode::Named(NamedPrivateMode::SwapScreenAndSetRestoreCursor) {
            // Rows the inactive grid's history holds came from rows the session start already
            // wrote, so they are dropped rather than written twice.
            self.term.grid_mut().clear_history();
        }
        self.end_session(false);
    }

    fn set_scrolling_region(&mut self, top: usize, bottom: Option<usize>) {
        Handler::set_scrolling_region(&mut self.term, top, bottom);
        // As the terminal does: 1-based rows, ignored unless the top is above the bottom.
        let (_, rows) = self.size();
        let bottom = bottom.unwrap_or(rows);
        if top < bottom {
            self.region = (top.saturating_sub(1).min(rows), bottom.min(rows) - 1);
        }
    }

    fn push_keyboard_mode(&mut self, mode: KeyboardModes) {
        let screen = usize::from(self.alternate());
        self.keyboard[screen].push(mode);
        self.sync_keyboard_mode();
    }

    fn pop_keyboard_modes(&mut self, to_pop: u16) {
        let screen = usize::from(self.alternate());
        self.keyboard[screen].pop(to_pop);
        self.sync_keyboard_mode();
    }

    fn set_keyboard_mode(&mut self, mode: KeyboardModes, behavior: KeyboardModesApplyBehavior) {
        let screen = usize::from(self.alternate());
        let current = &mut self.keyboard[screen].current;
        *current = match behavior {
            KeyboardModesApplyBehavior::Replace => mode,
            KeyboardModesApplyBehavior::Union => *current | mode,
            KeyboardModesApplyBehavior::Difference => *current & !mode,
        };
        self.sync_keyboard_mode();
    }

    // There is no window, clipboard, or hyperlink UI in this emulator. Keeping their payloads
    // (and thousands of stacked titles or distinct per-cell links) buys no observable behavior.
    fn set_title(&mut self, _title: Option<String>) {}
    fn push_title(&mut self) {}
    fn pop_title(&mut self) {}
    fn set_hyperlink(&mut self, _link: Option<Hyperlink>) {}
    fn clipboard_store(&mut self, _clipboard: u8, _data: &[u8]) {}

    forward! {
        set_cursor_style(style: Option<CursorStyle>);
        set_cursor_shape(shape: CursorShape);
        goto(line: i32, col: usize);
        goto_line(line: i32);
        goto_col(col: usize);
        move_up(count: usize);
        move_down(count: usize);
        identify_terminal(intermediate: Option<char>);
        device_status(arg: usize);
        move_forward(col: usize);
        move_backward(col: usize);
        move_down_and_cr(row: usize);
        move_up_and_cr(row: usize);
        backspace();
        carriage_return();
        bell();
        substitute();
        set_horizontal_tabstop();
        move_backward_tabs(count: u16);
        move_forward_tabs(count: u16);
        save_cursor_position();
        restore_cursor_position();
        clear_tabs(mode: TabulationClearMode);
        set_tabs(interval: u16);
        terminal_attribute(attr: Attr);
        set_mode(mode: Mode);
        unset_mode(mode: Mode);
        report_mode(mode: Mode);
        report_private_mode(mode: PrivateMode);
        set_keypad_application_mode();
        unset_keypad_application_mode();
        set_active_charset(index: CharsetIndex);
        configure_charset(index: CharsetIndex, charset: StandardCharset);
        set_color(index: usize, color: Rgb);
        dynamic_color_sequence(prefix: String, index: usize, terminator: &str);
        reset_color(index: usize);
        clipboard_load(clipboard: u8, terminator: &str);
        text_area_size_pixels();
        text_area_size_chars();
        set_mouse_cursor_icon(icon: CursorIcon);
        report_keyboard_mode();
        set_modify_other_keys(mode: ModifyOtherKeys);
        report_modify_other_keys();
        set_scp(char_path: ScpCharPath, update_mode: ScpUpdateMode);
    }
}

impl Recorder {
    /// Ends a synchronized update the program no longer holds, after writing out the lines the
    /// update already applied. A timeout can split one redraw into two numeric-only regions, and
    /// the drawing that ends it would condense rows the transcript never received.
    fn stop_sync(&mut self, parser: &mut Processor) {
        if self.held {
            let lines = self.session.as_mut().map(History::flush);
            for line in lines.into_iter().flatten() {
                self.push(&line);
            }
        }
        parser.stop_sync(self);
    }

    /// Before private mode `mode` is set or reset: the frame it ends, if it ends one.
    fn mode_change(&mut self, mode: PrivateMode, set: bool) {
        let PrivateMode::Named(named) = mode else {
            return;
        };
        match named {
            NamedPrivateMode::ShowCursor => {
                self.present();
                if set && !self.term.mode().contains(TermMode::SHOW_CURSOR) {
                    self.redraws += 1;
                }
            }
            NamedPrivateMode::SyncUpdate => {
                self.present();
                self.held = set;
                self.present();
            }
            // Switching screens ends the frame on screen; column mode clears it.
            NamedPrivateMode::SwapScreenAndSetRestoreCursor => self.present(),
            NamedPrivateMode::ColumnMode => {
                self.present();
                // The frame is cleared at the height this terminal has, not the default one: a
                // region past the last row indexes a row the emulator does not have, and a region
                // short of it leaves the rows below scrolling in place.
                let (_, rows) = self.size();
                self.region = (0, rows.saturating_sub(1));
            }
            _ => {}
        }
    }
}

fn blank(text: &str) -> bool {
    text.trim().is_empty()
}

fn digest(text: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    text.hash(&mut hasher);
    hasher.finish()
}

/// The screen's rows as a frame.
fn frame_rows(grid: &Grid<Cell>) -> Vec<FrameRow> {
    (0..grid.screen_lines())
        .map(|line| frame_row(grid, line))
        .collect()
}

fn frame_row(grid: &Grid<Cell>, line: usize) -> FrameRow {
    let row = &grid[Line(line as i32)];
    let wraps = wraps(grid, row);
    let mut text = row_text(row);
    if !wraps {
        text.truncate(text.trim_end().len());
    }
    FrameRow { text, wraps }
}

/// The terminal wrapped the row onto the next one.
fn wraps(grid: &Grid<Cell>, row: &Row<Cell>) -> bool {
    row[Column(grid.columns() - 1)]
        .flags
        .contains(Flags::WRAPLINE)
}

/// The last mark a row carries.
fn row_mark(row: &Row<Cell>) -> Option<char> {
    row[..]
        .iter()
        .flat_map(|cell| cell.zerowidth().into_iter().flatten())
        .copied()
        .rfind(|&c| is_mark(c))
}

/// Replace a row's internal mark rather than adding one forever to an unchanged first glyph.
/// Rebuild only the rare extra storage, preserving user combining marks and style attributes.
fn clear_row_marks(row: &mut Row<Cell>) {
    for cell in row[..].iter_mut() {
        if !cell
            .zerowidth()
            .is_some_and(|marks| marks.iter().any(|&mark| is_mark(mark)))
        {
            continue;
        }
        let kept: Vec<char> = cell
            .zerowidth()
            .into_iter()
            .flatten()
            .copied()
            .filter(|&mark| !is_mark(mark))
            .collect();
        let underline = cell.underline_color();
        let hyperlink = cell.hyperlink();
        cell.extra = None;
        cell.set_underline_color(underline);
        cell.set_hyperlink(hyperlink);
        for mark in kept {
            cell.push_zerowidth(mark);
        }
    }
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
        let marks = cell.zerowidth().into_iter().flatten();
        text.extend(marks.filter(|&&c| !is_mark(c)));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What happened to the output at a point of a test log.
    #[derive(Clone, Copy, Debug)]
    enum Mark {
        /// The output paused, long enough to read the screen or not.
        Pause(bool),
        /// A synchronized update ended at its timeout.
        SyncEnd,
    }

    /// The transcript of `log`, read in pieces split where `marks` fall: the lines written out,
    /// then those pending. `closed` ends the output.
    fn record(log: &[u8], marks: &[(usize, Mark)], closed: bool) -> String {
        let mut live = Live::new();
        let mut text = String::new();
        let (mut start, mut pause) = (0, None);
        for &(at, mark) in marks {
            live.process(&log[start..at], Instant::now(), pause.take());
            start = at;
            match mark {
                Mark::Pause(long) => pause = Some(long),
                Mark::SyncEnd => {
                    live.end_sync();
                }
            }
        }
        live.process(&log[start..], Instant::now(), pause);
        if closed {
            live.end_sync();
            live.finish();
        }
        text.push_str(&live.take_written());
        text.push_str(&live.pending());
        assert!(!live.stopped());
        text
    }

    fn render_text(log: &[u8]) -> String {
        record(log, &[], true)
    }

    #[test]
    fn an_internal_panic_stops_emulation_without_stopping_raw_progress() {
        let mut live = Live::new();
        live.process(b"before\r\n", Instant::now(), None);
        let before = live.read;
        // Inject at the existing panic boundary; no production input needs to be able to
        // crash the emulator for the capture thread's failure isolation to remain covered.
        let replies = live.run(|_, _| panic!("injected terminal failure"));
        assert!(replies.is_empty());
        assert!(live.stopped());
        assert!(live.screen().unwrap_err().contains(STOPPED));
        assert!(live.modes().err().unwrap().contains(STOPPED));
        assert!(live.pending().is_empty());
        assert!(live.sync_deadline().is_none());

        let output = b"still captured\r\n\x1b[6n";
        assert!(live.process(output, Instant::now(), None).is_empty());
        assert_eq!(live.read, before + output.len() as u64);
        assert_eq!(live.shown(), live.read);
        let mut reentered = false;
        live.run(|_, _| reentered = true);
        assert!(
            !reentered,
            "a failed emulator must never resume on later output"
        );
        assert!(live.stopped());
    }

    #[test]
    fn regression_keyboard_push_overflow_keeps_the_emulator_alive() {
        let mut live = Live::new();
        live.process("\x1b[>1u".repeat(4097).as_bytes(), Instant::now(), None);
        assert!(
            !live.stopped(),
            "keyboard stack exhaustion must not stop all terminal features"
        );
        assert_eq!(live.modes().unwrap().kitty, 1);
        assert_eq!(live.recorder.keyboard[0].saved.len(), KEYBOARD_STACK_LIMIT);
        assert_eq!(live.process(b"\x1b[?u", Instant::now(), None), b"\x1b[?1u");
        live.process(b"\x1b[<65535u", Instant::now(), None);
        assert_eq!(live.modes().unwrap().kitty, 0);
        assert!(live.recorder.keyboard[0].saved.is_empty());
    }

    #[test]
    fn keyboard_queries_follow_direct_changes_and_nested_restores() {
        let mut live = Live::new();
        for (input, flags) in [
            ("\x1b[=3u", 3),
            ("\x1b[=4;2u", 7),
            ("\x1b[=2;3u", 5),
            ("\x1b[>8u", 8),
            ("\x1b[>16u", 16),
            ("\x1b[<u", 8),
            ("\x1b[=9u", 9),
            ("\x1b[<u", 5),
            ("\x1b[<u", 0),
        ] {
            let replies = live.process(format!("{input}\x1b[?u").as_bytes(), Instant::now(), None);
            assert_eq!(live.modes().unwrap().kitty, flags, "{input:?}");
            assert_eq!(replies, format!("\x1b[?{flags}u").as_bytes(), "{input:?}");
        }
    }

    #[test]
    fn keyboard_stacks_are_per_screen_and_reset_together() {
        let mut live = Live::new();
        for (input, flags) in [
            ("\x1b[=3u\x1b[>5u", 5),
            ("\x1b[?1049h", 0),
            ("\x1b[>8u", 8),
            ("\x1b[?1049l", 5),
            ("\x1b[<u", 3),
            ("\x1b[?1049h", 8),
            ("\x1bc", 0),
            ("\x1b[?1049h", 0),
            ("\x1b[?1049l\x1b[<u", 0),
        ] {
            let replies = live.process(format!("{input}\x1b[?u").as_bytes(), Instant::now(), None);
            assert_eq!(live.modes().unwrap().kitty, flags, "{input:?}");
            assert_eq!(replies, format!("\x1b[?{flags}u").as_bytes(), "{input:?}");
        }
        assert!(
            live.recorder
                .keyboard
                .iter()
                .all(|stack| stack.saved.is_empty())
        );
    }

    #[test]
    fn regression_osc_guard_preserves_the_raw_sync_cursor() {
        let mut live = Live::new();
        let arrived = Instant::now();
        // An earlier discarded OSC makes raw and forwarded offsets differ before the sync.
        let prefix = format!(
            "\x1b]0;{}\x07before\r\n\x1b[?2026h",
            "A".repeat(OSC_BYTES_LIMIT * 2)
        );
        live.process(prefix.as_bytes(), arrived, None);
        live.process(b"held\x1b]0;partial", arrived, None);
        assert_eq!(
            live.shown(),
            prefix.len() as u64,
            "held OSC bytes must not consume unseen screen output"
        );
        let payload = [b'B'; 1024];
        for _ in 0..32 {
            live.process(&payload, arrived, None);
            assert_eq!(
                live.shown(),
                prefix.len() as u64,
                "discarded OSC bytes are raw bytes too"
            );
        }
        live.process(b"\x07\x1b[?2026lafter", arrived, None);
        assert_eq!(live.shown(), live.read);
        assert_eq!(live.screen().unwrap().0, ["before", "heldafter"]);
        assert!(!live.stopped());
    }

    #[test]
    fn osc_offsets_survive_sync_end_resize_timeout_and_buffer_limit() {
        for ending in ["explicit", "resize", "timeout", "buffer"] {
            let mut live = Live::new();
            let arrived = Instant::now();
            live.process(b"A\x1b[?2026hB", arrived, None);
            for _ in 0..12 {
                let osc = format!("\x1b]0;{}\x07", "x".repeat(OSC_BYTES_LIMIT + 10));
                live.process(osc.as_bytes(), arrived, None);
                assert_eq!(live.shown(), 9);
            }
            assert!(!live.osc.gaps.is_empty());
            match ending {
                "explicit" => {
                    live.end_sync();
                }
                "resize" => {
                    live.resize(80, 25);
                }
                "timeout" => {
                    let deadline = live.sync_deadline().unwrap();
                    live.process(b"C", deadline, None);
                }
                "buffer" => {
                    live.process(&vec![b'C'; 0x20_0000], arrived, None);
                }
                _ => unreachable!(),
            }
            assert_eq!(live.shown(), live.read, "{ending}");
            assert!(
                live.osc.gaps.is_empty(),
                "{ending}: consumed gaps must not accumulate"
            );
            assert!(!live.stopped());
            assert!(live.pending().contains("AB") || live.take_written().contains("AB"));
        }
    }

    #[test]
    fn sync_raw_offsets_do_not_depend_on_osc_chunk_boundaries() {
        let held = b"B\x1b]0;small title\x07C";
        for split in 0..=held.len() {
            let mut live = Live::new();
            let arrived = Instant::now();
            live.process(b"A\x1b[?2026h", arrived, None);
            live.process(&held[..split], arrived, None);
            live.process(&held[split..], arrived, None);
            assert_eq!(live.shown(), 9, "split {split}");
            assert_eq!(live.screen().unwrap().0, ["A"]);
            live.process(b"\x1b[?2026l", arrived, None);
            assert_eq!(live.screen().unwrap().0, ["ABC"]);
            assert_eq!(live.shown(), live.read);
        }
    }

    #[test]
    fn oversized_osc_is_bounded_across_chunks_and_does_not_become_text() {
        for prefix in ["0;", "8;;https://example.test/", "52;c;"] {
            for terminator in ["\x07", "\x1b\\", "\x18"] {
                let mut live = Live::new();
                live.process(
                    format!("before\x1b]{prefix}").as_bytes(),
                    Instant::now(),
                    None,
                );
                let payload = [b'A'; 1024];
                for _ in 0..32 {
                    live.process(&payload, Instant::now(), None);
                    assert!(live.osc.pending.len() <= OSC_BYTES_LIMIT);
                }
                live.process(
                    format!("{terminator}after").as_bytes(),
                    Instant::now(),
                    None,
                );
                assert_eq!(live.screen().unwrap().0, ["beforeafter"]);
                assert!(!live.stopped());
                live.finish();
                assert_eq!(live.take_written(), "beforeafter\n");
            }
        }
    }

    #[test]
    fn osc_guard_preserves_normal_sequences_at_every_split() {
        for bytes in [
            &b"before\x1b]0;title\x07after"[..],
            &b"\x1b]8;;https://example.test/\x1b\\link\x1b]8;;\x1b\\"[..],
            &b"\x1b\0]0;title\x07\x1b[31mred\x1b[0m"[..],
            &b"\x1b ]ordinary\x1b[?2026hframe\x1b[?2026l"[..],
        ] {
            for size in 1..=bytes.len() {
                let mut guard = OscGuard::default();
                let mut actual = Vec::new();
                for chunk in bytes.chunks(size) {
                    actual.extend_from_slice(&guard.filter(chunk));
                }
                assert_eq!(actual, bytes, "chunk size {size}");
            }
        }
    }

    #[test]
    fn combining_limit_keeps_wide_glyphs_wraps_and_internal_marks() {
        for prefix in ["中".to_string(), format!("{}a", "x".repeat(19))] {
            let mut live = Live::at(20, 5);
            live.process(prefix.as_bytes(), Instant::now(), None);
            live.process(
                "\u{301}\x1b[?1049h\x1b[?1049l".as_bytes(),
                Instant::now(),
                None,
            );
            live.process("\u{301}".repeat(256).as_bytes(), Instant::now(), None);
            let grid = live.recorder.term.grid();
            let cell = if prefix == "中" {
                &grid[Line(0)][Column(0)]
            } else {
                &grid[Line(0)][Column(19)]
            };
            assert_eq!(
                cell.zerowidth()
                    .unwrap()
                    .iter()
                    .filter(|&&mark| !is_mark(mark))
                    .count(),
                CELL_COMBINING_LIMIT
            );
            let before = live.screen().unwrap().0.join("");
            assert!(before.starts_with(&prefix));
            live.process(b"B", Instant::now(), None);
            assert!(live.screen().unwrap().0.join("").ends_with('B'));
            assert!(!live.stopped());
        }
    }

    #[test]
    fn regression_internal_row_marks_remain_bounded() {
        let mut live = Live::new();
        live.process(b"a", Instant::now(), None);
        for n in 0..256 {
            let frame = format!("\x1b[1;2H{n:04}\x1b[?1049h\x1b[?1049l");
            live.process(frame.as_bytes(), Instant::now(), None);
            live.take_written();
        }
        assert!(
            live.recorder.marked.len() <= usize::from(PTY_ROWS) + 1,
            "orphaned marker entries must retire"
        );
        let grid = live.recorder.term.grid();
        assert!(
            grid[Line(0)][Column(0)]
                .zerowidth()
                .unwrap_or_default()
                .len()
                <= 1,
            "repeated marks must replace old marks"
        );
    }

    #[test]
    fn regression_combining_marks_are_bounded_per_cell() {
        let mut live = Live::new();
        live.process(b"a", Instant::now(), None);
        let marks = "\u{301}".repeat(4096);
        for _ in 0..16 {
            live.process(marks.as_bytes(), Instant::now(), None);
        }
        let (rows, _) = live.screen().unwrap();
        assert!(
            rows[0].chars().count() <= 65,
            "one cell must not hold an unlimited combining run"
        );
        assert!(!live.stopped());
        live.process(b"OK", Instant::now(), None);
        assert!(live.screen().unwrap().0[0].ends_with("OK"));
    }

    #[test]
    fn soft_wrapped_transcript_keeps_spaces_inside_logical_lines() {
        for prefix in ["", "\x1b[?1049h"] {
            for line in [format!("{} next", "a".repeat(119)), "token".repeat(30)] {
                let text = render_text(format!("{prefix}{line}").as_bytes());
                let lines: Vec<_> = text
                    .lines()
                    .filter(|line| *line != ALTERNATE_MARKER)
                    .collect();
                assert_eq!(lines, [line.as_str()]);
            }
        }
    }

    #[test]
    fn normal_partial_scrolls_preserve_the_rows_that_leave_once() {
        for log in [
            "\x1b[2;4r\x1b[2;1Halpha\r\nbeta\r\ngamma\r\ndelta\r\nepsilon".to_string(),
            "\x1b[1;3ralpha\r\nbeta\r\ngamma\r\ndelta\r\nepsilon".to_string(),
            "\x1b[2;4r\x1b[2;1Halpha\r\nbeta\r\ngamma\x1b[2S\x1b[3;1Hdelta\r\nepsilon".to_string(),
            "\x1b[2;4r\x1b[2;1Halpha\r\nbeta\r\ngamma\x1b[2;1H\x1b[2M\x1b[3;1Hdelta\r\nepsilon"
                .to_string(),
        ] {
            let text = render_text(log.as_bytes());
            let lines: Vec<_> = text.lines().filter(|line| !line.is_empty()).collect();
            assert_eq!(lines, ["alpha", "beta", "gamma", "delta", "epsilon"]);
        }
        let line = format!("{} next", "a".repeat(119));
        for (suffix, expected) in [
            ("\r\n", vec![line.as_str()]),
            ("\r\nlast", vec![line.as_str(), "last"]),
            ("\r\nlast\r\nend", vec![line.as_str(), "last", "end"]),
            ("\r\n\x1b[2J\x1b[Hafter", vec![line.as_str(), "after"]),
            (
                "\r\n\x1b[3;5r\x1b[3;1Hp\r\nq\r\nr\r\ns",
                vec![line.as_str(), "p", "q", "r", "s"],
            ),
        ] {
            let log = format!("\x1b[2;3r\x1b[2;1H{line}{suffix}");
            let text = render_text(log.as_bytes());
            let lines: Vec<_> = text.lines().filter(|line| !line.is_empty()).collect();
            assert_eq!(lines, expected);
        }

        let log = b"\x1b[2;4r\x1b[2;1Halpha\r\nbeta\r\ngamma\x1b[?1049h\x1b[Hinside\x1b[?1049l\r\ndelta\r\nepsilon";
        let text = render_text(log);
        let lines: Vec<_> = text
            .lines()
            .filter(|line| !matches!(*line, "" | ALTERNATE_MARKER | NORMAL_MARKER))
            .collect();
        assert_eq!(
            lines,
            ["alpha", "beta", "gamma", "inside", "delta", "epsilon"]
        );
    }

    #[test]
    fn alternate_screen_sessions_appear_where_they_ran() {
        let session = |k: usize| format!("\x1b[?1049h\x1b[Hinside {k}\x1b[?1049l");
        let ran = format!("before\r\n{}first\r\n{}second\r\n", session(1), session(2));
        let cleared = format!("before\r\n{}\x1b[H\x1b[2J\x1b[3Jafter\r\n", session(1));
        let rewritten = format!("first\r\nprompt{}\r\x1b[2Klast\r\n", session(1));
        let beside = format!("before\r{}\r\nafter\r\n", session(1));
        let moved = format!("a\r\nb\r\nc\r\n{}\x1b[2;1H{}", session(1), session(2));
        let erased = format!(
            "first\r\n{}between\r\nanchor\r\nprompt{}\x1b[3;1H\x1b[2K\x1b[4;1H\x1b[2Kafter\r\n",
            session(1),
            session(2)
        );
        let above = format!("a\r\nb\r\nc\r\n\x1b[H{}", session(1));
        let combining = format!(" \u{301}{}\r\nafter\r\n", session(1));
        let wide_blank = format!("before\r\n\u{3000}{}after\r\n", session(1));
        let indented = format!(
            "  before\r\nmiddle\r\nlast{}\x1b[1;3H\x1b[Kafter\x1b[2;1H\x1b[2K\x1b[3;1H\x1b[2K",
            session(1)
        );
        let scrolled = format!("a\r\nb{}{}", session(1), "\r\nmore".repeat(40));
        for (log, expected) in [
            // Text below the cursor was shown before the session too.
            (above, vec!["a", "b", "c", ALTERNATE_MARKER, "inside 1"]),
            // A combining mark on a blank is text; an ideographic space is not.
            (
                combining,
                vec![
                    " \u{301}",
                    ALTERNATE_MARKER,
                    "inside 1",
                    NORMAL_MARKER,
                    "after",
                ],
            ),
            (
                wide_blank,
                vec![
                    "before",
                    ALTERNATE_MARKER,
                    "inside 1",
                    NORMAL_MARKER,
                    "\u{3000}after",
                ],
            ),
            // Text written over a row after the session is new; the rows erased stay written.
            (
                indented,
                vec![
                    "  before",
                    "middle",
                    "last",
                    ALTERNATE_MARKER,
                    "inside 1",
                    NORMAL_MARKER,
                    "  after",
                ],
            ),
            (
                ran,
                vec![
                    "before",
                    ALTERNATE_MARKER,
                    "inside 1",
                    NORMAL_MARKER,
                    "first",
                    ALTERNATE_MARKER,
                    "inside 2",
                    NORMAL_MARKER,
                    "second",
                ],
            ),
            // The screen the session followed was cleared after it, and its history too.
            (
                cleared,
                vec![
                    "before",
                    ALTERNATE_MARKER,
                    "inside 1",
                    NORMAL_MARKER,
                    "after",
                ],
            ),
            // The program wrote over the cursor's row after the session.
            (
                rewritten,
                vec![
                    "first",
                    "prompt",
                    ALTERNATE_MARKER,
                    "inside 1",
                    NORMAL_MARKER,
                    "last",
                ],
            ),
            // The session began on the row of the text before the cursor's column.
            (
                beside,
                vec![
                    "before",
                    ALTERNATE_MARKER,
                    "inside 1",
                    NORMAL_MARKER,
                    "after",
                ],
            ),
            // The cursor moved up between sessions; they keep their order.
            (
                moved,
                vec![
                    "a",
                    "b",
                    "c",
                    ALTERNATE_MARKER,
                    "inside 1",
                    ALTERNATE_MARKER,
                    "inside 2",
                ],
            ),
            // Rows written before the second session were erased or written over after it.
            (
                erased,
                vec![
                    "first",
                    ALTERNATE_MARKER,
                    "inside 1",
                    NORMAL_MARKER,
                    "between",
                    "anchor",
                    "prompt",
                    ALTERNATE_MARKER,
                    "inside 2",
                    NORMAL_MARKER,
                    "",
                    "after",
                ],
            ),
        ] {
            let text = render_text(log.as_bytes());
            assert_eq!(text.lines().collect::<Vec<_>>(), expected, "{text}");
        }
        // Rows written before a session scroll away after it, written once.
        let text = render_text(scrolled.as_bytes());
        let mut expected = vec!["a", "b", ALTERNATE_MARKER, "inside 1", NORMAL_MARKER];
        expected.extend(["more"; 40]);
        assert_eq!(text.lines().collect::<Vec<_>>(), expected, "{text}");
    }

    #[test]
    fn a_reprint_after_a_cleared_history_is_no_new_text() {
        let lines = |range: std::ops::RangeInclusive<usize>| -> String {
            range.map(|k| format!("line {k}\r\n")).collect()
        };
        // A full redraw clears the screen and its history, then prints every line again, one
        // more after them.
        let log = format!("{}\x1b[2J\x1b[3J\x1b[H{}", lines(1..=50), lines(1..=51));
        let text = render_text(log.as_bytes());
        let expected: Vec<String> = (1..=51).map(|k| format!("line {k}")).collect();
        assert_eq!(text.lines().collect::<Vec<_>>(), expected, "{text}");
        // Lines that may begin a reprint come before a session that begins after them.
        let log = format!(
            "{}\x1b[2J\x1b[3J\x1b[H{}\x1b[?1049hinside",
            lines(1..=50),
            lines(1..=3)
        );
        let text = render_text(log.as_bytes());
        let mut expected: Vec<String> =
            (1..=50).chain(1..=3).map(|k| format!("line {k}")).collect();
        expected.extend([ALTERNATE_MARKER.to_string(), "inside".into()]);
        assert_eq!(text.lines().collect::<Vec<_>>(), expected, "{text}");
        // Fewer lines repeating written ones than a screen are new, even repeating every line
        // written.
        let log = "a\r\nb\r\nc\r\n\x1b[2J\x1b[3J\x1b[Ha\r\nb\r\nc\r\nd";
        let text = render_text(log.as_bytes());
        assert_eq!(
            text.lines().collect::<Vec<_>>(),
            ["a", "b", "c", "a", "b", "c", "d"],
            "{text}"
        );
        // `clear` keeps what the screen showed; what follows is new, even where it repeats an
        // older line or two.
        let log = "a\r\nb\r\n$ ls\r\nx\r\n$ clear\r\n\x1b[H\x1b[2J\x1b[3J$ ls\r\nx\r\n$ ";
        let text = render_text(log.as_bytes());
        assert_eq!(
            text.lines().collect::<Vec<_>>(),
            ["a", "b", "$ ls", "x", "$ clear", "$ ls", "x", "$"],
            "{text}"
        );
    }

    /// A synchronized update that times out must not cost the rows it already applied: the timeout
    /// splits one redraw into two numeric-only regions, and each would condense rows the transcript
    /// never received. An empty piece arriving past the deadline is what forces the end.
    #[test]
    fn a_synchronized_update_that_times_out_keeps_the_rows_it_applied() {
        fn rows(first: usize, last: usize) -> String {
            (first..last)
                .map(|k| format!("[{};1H[2Kframed {k:05}", k % 5 + 2))
                .collect()
        }
        let mut live = Live::new();
        let mut text = String::new();
        let arrived = Instant::now();
        // A whole frame of five rows on the alternate screen.
        let first = format!("[?1049h[?2026h[H[2Kheader{}[?2026l", rows(0, 5));
        live.process(first.as_bytes(), arrived, None);
        text += &live.take_written();
        // The next frame begins and stays held, then the update times out.
        let prefix = format!("[?2026h[H[2Kheader{}", rows(5, 7));
        live.process(prefix.as_bytes(), arrived, None);
        let deadline = live.sync_deadline().expect("the second frame is held");
        let after = deadline + std::time::Duration::from_nanos(1);
        live.process(b"", after, None);
        text += &live.take_written();
        let rest = format!("{}[?2026l", rows(7, 10));
        live.process(rest.as_bytes(), after, None);
        text += &live.take_written();
        live.finish();
        text += &live.take_written();
        let framed: Vec<&str> = text
            .lines()
            .filter(|line| line.starts_with("framed "))
            .collect();
        let expected: Vec<String> = (0..10).map(|k| format!("framed {k:05}")).collect();
        assert_eq!(
            framed,
            expected.iter().map(String::as_str).collect::<Vec<_>>(),
            "{text}"
        );
    }

    /// Column mode clears the frame at the height the terminal has: a short screen must not index
    /// a row it does not have. The tall half only shows that a screen taller than the default
    /// survives the same sequence; it reads the emulator's grid, so it says nothing about how the
    /// recorder attributes rows.
    #[test]
    fn column_mode_keeps_the_screens_own_height() {
        let mut short = Live::at(20, 5);
        short.process(b"[?1049h[?3h[T", Instant::now(), None);
        let (rows, _) = short
            .screen()
            .expect("a five-row screen survives column mode");
        assert_eq!(short.size(), (20, 5));
        assert!(rows.is_empty(), "{rows:?}");

        let mut tall = Live::at(20, 40);
        let frame = "[?1049h[?3h[1;1Htop[40;1Hbottom";
        tall.process(frame.as_bytes(), Instant::now(), None);
        tall.process(b"[T", Instant::now(), None);
        let (rows, _) = tall.screen().expect("a forty-row screen survives");
        assert_eq!(tall.size(), (20, 40));
        assert_eq!(rows.get(1).map(String::as_str), Some("top"), "{rows:?}");
        assert!(
            !rows.iter().any(|row| row == "bottom"),
            "the bottom row scrolled out: {rows:?}"
        );
    }

    #[test]
    fn long_output_is_written_out_whole() {
        // Lines scrolling the normal screen, then the alternate screen, then pages a program
        // redraws in place, each frame a synchronized update showing the next rows.
        let mut log: String = (0..15_000).map(|k| format!("normal {k}\r\n")).collect();
        log += "\x1b[?1049h";
        log += &(0..15_000)
            .map(|k| format!("\r\nscrolled {k}"))
            .collect::<String>();
        log += "\x1b[?1049l\x1b[?1049h";
        for k in 0..2_100 {
            log += "\x1b[?2026h\x1b[H\x1b[2Kheader";
            for (row, item) in (k * 5..k * 5 + 5).enumerate() {
                log += &format!("\x1b[{};1H\x1b[2Kframed {item:05}", row + 2);
            }
            log += "\x1b[?2026l";
        }
        // Read in pieces, as a program's output arrives.
        let mut live = Live::new();
        let mut text = String::new();
        // One arrival for every piece: the chunks are one program's output, and a fresh instant
        // per chunk would let a scheduling gap expire a synchronized update the fixture means to
        // keep whole.
        let arrived = Instant::now();
        for piece in log.as_bytes().chunks(4096) {
            live.process(piece, arrived, None);
            text += &live.take_written();
        }
        live.finish();
        text += &live.take_written();
        for (prefix, count) in [
            ("normal ", 15_000),
            ("scrolled ", 15_000),
            ("framed ", 10_500),
        ] {
            let found: Vec<&str> = text
                .lines()
                .filter(|line| line.starts_with(prefix))
                .collect();
            let expected: Vec<String> = (0..count)
                .map(|k| match prefix {
                    "framed " => format!("{prefix}{k:05}"),
                    _ => format!("{prefix}{k}"),
                })
                .collect();
            assert!(found == expected, "{prefix}: {} lines", found.len());
        }
    }

    #[test]
    fn a_reset_or_a_line_without_end_loses_nothing() {
        // A reset erases the screen it showed and a line wrapped onto it.
        let log = format!("a\r\n{}\x1bcc", "w".repeat(120 * 31));
        let text = render_text(log.as_bytes());
        let expected = ["a".to_string(), "w".repeat(120 * 31), "c".to_string()];
        assert_eq!(text.lines().collect::<Vec<_>>(), expected);
        // Output without line breaks is written out in pieces, spaces and all.
        // The first piece ends with the row that reaches 1 MiB, here a space.
        let log = format!("{} {}", "x".repeat(1_048_679), "y".repeat(4000));
        let text = render_text(log.as_bytes());
        assert!(text.lines().count() > 1, "one line");
        assert_eq!(text.lines().collect::<String>(), log);
    }

    #[test]
    fn written_lines_and_pending_lines_make_the_transcript() {
        let mut live = Live::new();
        let log: String = (0..100).map(|k| format!("line {k}\r\n")).collect();
        live.process(log.as_bytes(), Instant::now(), None);
        let written = live.take_written();
        // The screen's rows are pending: they may still change.
        assert_eq!(written.lines().count(), 71, "{written}");
        let pending = live.pending();
        assert_eq!(pending.lines().count(), 29, "{pending}");
        assert_eq!(live.pending(), pending, "pending changes nothing");
        live.process(b"\x1b[Hchanged", Instant::now(), None);
        assert!(live.pending().starts_with("changed"));
        live.finish();
        let rest = live.take_written();
        assert_eq!(format!("{written}{rest}").lines().count(), 100);
        assert!(live.pending().is_empty());
    }

    /// The transcript lines with text of an alternate-screen session drawing each of `frames`
    /// as a synchronized update: a header, the frame's rows from row 20 on, and a footer.
    fn streamed(frames: &[[&str; 5]]) -> Vec<String> {
        let mut log = String::from("\x1b[?1049h");
        for rows in frames {
            log.push_str("\x1b[?2026h\x1b[1;1H\x1b[2Kheader");
            for (k, row) in rows.iter().enumerate() {
                log.push_str(&format!("\x1b[{};1H\x1b[2K{row}", 20 + k));
            }
            log.push_str("\x1b[27;1H\x1b[2Kfooter\x1b[?2026l");
        }
        render_text(log.as_bytes())
            .lines()
            .filter(|line| !matches!(*line, "" | "header" | "footer" | ALTERNATE_MARKER))
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn a_streamed_preview_leaves_no_copy_behind() {
        // A reply's last row previews the sentence being streamed; it moves on to the next
        // sentence before the one it previewed, without its final full stop, shows in full.
        let [s1, s2, s3, s4] = [
            "第一句話寫進終端機。",
            "第二句話重建歷史。",
            "第三句話說明預覽。",
            "第四句話結束說明。",
        ];
        let (p3, p4) = (s3.trim_end_matches('。'), "第四句話");
        let frames = [
            ["", "", s1, s2, p3],
            ["", "", s1, s2, p4],
            ["", s1, s2, s3, p4],
            [s1, s2, s3, s4, ""],
        ];
        assert_eq!(streamed(&frames), [s1, s2, s3, s4]);
    }

    #[test]
    fn a_preview_finished_after_another_page_continues_its_line() {
        // A reply's last row previews a sentence all but its full stop; another page shows,
        // then the reply again with the sentence finished and more below it.
        let s = [
            "第一句話寫進終端機。",
            "第二句話重建歷史。",
            "第三句話說明預覽。",
            "第四句話結束說明。",
            "第五句話收尾完成。",
            "第六句話繼續寫。",
            "第七句話還沒完。",
            "第八句話到此為止。",
        ];
        let page = ["page 1", "page 2", "page 3", "page 4", "page 5"];
        let frames = [
            ["", s[0], s[1], s[2], s[3].trim_end_matches('。')],
            page,
            [s[0], s[1], s[2], s[3], ""],
            [s[1], s[2], s[3], s[4], ""],
            [s[2], s[3], s[4], s[5], ""],
            [s[3], s[4], s[5], s[6], ""],
            [s[4], s[5], s[6], s[7], ""],
        ];
        let mut reply = streamed(&frames);
        reply.retain(|line| !line.starts_with("page"));
        assert_eq!(reply, s);
    }

    #[test]
    fn a_preview_cleared_and_shown_finished_stays_one_line() {
        // The reply's last row previews line 5 in full, clears, and starts line 6 with the
        // word every line starts with; then line 5 shows finished above line 6.
        let rows: Vec<String> = (3..=6)
            .map(|n| format!("ITEM {n:03} - the value of item {n:03} is {n:03}"))
            .collect();
        let [r3, r4, r5, r6] = [0, 1, 2, 3].map(|k| rows[k].as_str());
        let frames = [
            ["", "", "", r3, r4],
            ["", "", r3, r4, r5],
            ["", "", r3, r4, "ITEM"],
            ["", r3, r4, r5, "ITEM 006 - the"],
            [r3, r4, r5, r6, ""],
        ];
        assert_eq!(streamed(&frames), rows);
    }

    #[test]
    fn printed_lines_count_as_frames_for_a_line_that_left() {
        // `cat` leaves the screen; lines then print into a scroll region frame by frame, the
        // last of them longer than `cat`, before an unrelated row changes.
        let mut log = String::from("\x1b[?1049h");
        for tail in ["cat", "seed"] {
            log.push_str(&format!(
                "\x1b[?2026h\x1b[1;1H\x1b[2Kheader\x1b[30;1H\x1b[2K{tail}\x1b[?2026l"
            ));
        }
        log.push_str("\x1b[2;30r\x1b[30;1H");
        for k in 1..=7 {
            let text = if k < 7 {
                format!("noise {k}")
            } else {
                "catalog".into()
            };
            log.push_str(&format!("\x1b[?2026h\r\n{text}\x1b[?2026l"));
        }
        log.push_str("\x1b[?2026h\x1b[1;1H\x1b[2Kheader done\x1b[?2026l");
        let text = render_text(log.as_bytes());
        assert!(text.lines().any(|line| line == "cat"), "{text}");
    }

    #[test]
    fn a_preview_shows_again_when_its_finished_line_leaves() {
        // A preview's finished line shows, then reflows into a changed condition, so the
        // preview holds the only copy of the old one.
        let frames = [
            [
                "",
                "alpha one apple",
                "beta two pear",
                "gamma three plum",
                "delta four grape",
            ],
            [
                "",
                "beta two pear",
                "gamma three plum",
                "delta four grape",
                "check left == right",
            ],
            [
                "",
                "alpha one apple",
                "beta two pear",
                "gamma three plum",
                "delta four grape",
            ],
            ["", "gamma three plum", "delta four grape", "check", ""],
            [
                "",
                "beta two pear",
                "gamma three plum",
                "delta four grape",
                "check left == right done",
            ],
            [
                "",
                "beta two pear",
                "gamma three plum",
                "delta four grape",
                "check left != right done",
            ],
        ];
        let reply = streamed(&frames);
        assert!(reply.iter().any(|line| line.contains("==")), "{reply:?}");
    }

    #[test]
    fn an_entry_below_a_fixed_header_is_no_preview() {
        // A list's entries give way to a new page under a fixed header, and a longer entry is
        // inserted at the top of the list where `cat` showed before.
        let mut log = String::from("\x1b[?1049h");
        for frame in [
            [
                "files",
                "",
                "old one apple",
                "old two pear",
                "old three plum",
                "cat",
                "footer",
            ],
            ["files", "", "cat", "", "", "", "footer"],
            [
                "files",
                "",
                "new one red",
                "new two green",
                "new three blue",
                "new four white",
                "footer",
            ],
        ] {
            log.push_str("\x1b[?2026h");
            for (k, row) in frame.iter().enumerate() {
                log.push_str(&format!("\x1b[{};1H\x1b[2K{row}", k + 1));
            }
            log.push_str("\x1b[?2026l");
        }
        log.push_str("\x1b[?2026h\x1b[2;7r\x1b[2;1H\x1b[1Lcatalog\x1b[?2026l");
        let text = render_text(log.as_bytes());
        assert!(text.lines().any(|line| line == "cat"), "{text}");
    }

    #[test]
    fn a_numbered_line_shown_late_keeps_its_place() {
        // A reply's last row moves on to line 55 before line 54 shows; the rows differ only in
        // numbers, as a counter's do.
        let rows: Vec<String> = (52..=56)
            .map(|n| {
                format!("ITEM-{n:03} | the quick brown fox jumps over the lazy dog | END-{n:03}")
            })
            .collect();
        let [r52, r53, r54, r55, r56] = [0, 1, 2, 3, 4].map(|k| rows[k].as_str());
        let frames = [
            ["", "", r52, r53, r54],
            ["", "", r52, r53, r55],
            ["", "", r53, r54, r56],
            ["", "", r54, r55, r56],
        ];
        assert_eq!(streamed(&frames), rows);
    }

    #[test]
    fn a_pause_inside_a_redraw_is_a_frame_only_when_long() {
        // The program hides the cursor while it draws, as it has twice before; a pause leaves
        // a row half drawn.
        let pages: String = (0..2)
            .map(|k| format!("\x1b[?25l\x1b[H\x1b[2Kpage {k}\x1b[?25h"))
            .collect();
        let torn = format!("\x1b[?1049h{pages}\x1b[?25l\x1b[H\x1b[2Kdrawing row");
        let log = format!("{torn}\x1b[H\x1b[2Kfinal row\x1b[?25h");
        for (long, shown) in [(false, false), (true, true)] {
            let text = record(log.as_bytes(), &[(torn.len(), Mark::Pause(long))], true);
            assert_eq!(text.contains("drawing row"), shown, "long {long}: {text}");
            assert!(text.contains("final row"), "{text}");
        }
    }

    #[test]
    fn synchronized_updates_end_where_the_terminal_ended_them() {
        // The first page shows at the update's timeout, then the second replaces it before the
        // update ends; a running task's update still open at the end shows nothing.
        let first = "\x1b[?1049h\x1b[?2026h\x1b[Hfirst page";
        let log = format!("{first}\x1b[Hsecond page\x1b[?2026l\x1b[?2026h\x1b[H\x1b[2Kheld page");
        let timeout = (first.len(), Mark::SyncEnd);
        let lines = |text: String| -> Vec<String> {
            text.lines()
                .filter(|line| line.ends_with("page"))
                .map(str::to_string)
                .collect()
        };
        let cases = [
            (vec![timeout], false, vec!["first page", "second page"]),
            (vec![], false, vec!["second page"]),
            (
                vec![timeout],
                true,
                vec!["first page", "second page", "held page"],
            ),
        ];
        for (marks, closed, expected) in cases {
            let text = record(log.as_bytes(), &marks, closed);
            assert_eq!(lines(text), expected, "marks {marks:?}, closed {closed}");
        }
    }

    #[test]
    fn alternate_screen_rows_pushed_off_stay_in_the_transcript() {
        // Full-width rows, all read at once with no pause, each scrolling the screen: by
        // wrapping, written back to back, followed by a tab, carrying a combining mark that
        // lands in the last column, or ending in a wide character that does not fit, which
        // joins them into one line as the terminal wrapped them; and printed on new lines below
        // a fixed header, also after column mode reset a smaller scroll region.
        let rows: Vec<String> = (0..80)
            .map(|k| format!("row {k:03} {}", "x".repeat(112)))
            .collect();
        let accented: Vec<String> = rows.iter().map(|row| format!("{row}\u{301}")).collect();
        let wide: Vec<String> = (0..80)
            .map(|k| format!("row {k:03} {}\u{4e2d}", "x".repeat(111)))
            .collect();
        let lines = |prefix: &str| {
            let lines: String = rows.iter().map(|row| format!("\r\n{row}")).collect();
            format!("\x1b[?1049h{prefix}{lines}")
        };
        for (log, expected) in [
            (format!("\x1b[?1049h{}", rows.concat()), vec![rows.concat()]),
            (
                format!("\x1b[?1049h{}", rows.join("\t")),
                vec![rows.concat()],
            ),
            (
                format!("\x1b[?1049h{}", accented.concat()),
                vec![accented.concat()],
            ),
            (format!("\x1b[?1049h{}", wide.concat()), vec![wide.concat()]),
            (lines("header\x1b[2;30r\x1b[30;1H"), rows.clone()),
            (lines("\x1b[2;20r\x1b[?3l\x1b[H"), rows.clone()),
        ] {
            let text = render_text(log.as_bytes());
            let session: Vec<&str> = text
                .lines()
                .skip_while(|line| *line != ALTERNATE_MARKER)
                .filter(|line| line.starts_with("row "))
                .collect();
            assert_eq!(session, expected, "{text}");
        }
        // Scrolling back: a character wraps from the uncovered top row into the next one.
        let screen: String = (1..=30).map(|k| format!("\x1b[{k};1Hline{k:02}")).collect();
        let log = format!(
            "\x1b[?1049h{screen}\x1b[H\x1bM{}Y\x1b[H{}",
            "x".repeat(120),
            "\x1bM".repeat(30)
        );
        let text = render_text(log.as_bytes());
        assert!(text.lines().any(|line| line == "Yine01"), "{text}");
        // Each new line marks the one above it done before the next scroll.
        let mut log = String::from("\x1b[?1049h");
        for k in 0..80 {
            log += &format!("\r\nitem {k:03}");
            if k > 0 {
                log += &format!("\x1b[A\ritem {:03} done\x1b[B", k - 1);
            }
        }
        let text = render_text(log.as_bytes());
        let items: Vec<&str> = text
            .lines()
            .filter(|line| line.starts_with("item "))
            .collect();
        let expected: Vec<String> = (0..80)
            .map(|k| match k {
                79 => "item 079".to_string(),
                _ => format!("item {k:03} done"),
            })
            .collect();
        assert_eq!(items, expected, "{text}");
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
                // Pathological runs are capped; ordinary combining sequences above remain whole.
                format!("e{}", "\u{301}".repeat(100)),
                format!("e{}\n", "\u{301}".repeat(CELL_COMBINING_LIMIT)),
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
        live.process(b"before\r\n\x1b[?2026h", Instant::now(), None);
        live.process(b"held", Instant::now(), None);
        let (rows, _) = live.screen().unwrap();
        assert_eq!(rows, ["before"]);
        assert_eq!(live.shown(), 16, "the held bytes are not shown yet");
        assert!(live.sync_deadline().is_some());
        live.process(b"\x1b[?2026l", Instant::now(), None);
        let (rows, _) = live.screen().unwrap();
        assert_eq!(rows, ["before", "held"]);
        assert_eq!(live.shown(), 28);
        assert!(live.sync_deadline().is_none());
    }

    #[test]
    fn arbitrary_output_never_stops_the_recorder() {
        // Pieces of terminal output in random order, read in random pieces with random pauses.
        const PIECES: [&str; 32] = [
            "text ",
            "\r\n",
            "\n",
            "\r",
            "\t",
            "中文",
            "e\u{301}",
            "\x1b[?1049h",
            "\x1b[?1049l",
            "\x1b[?2026h",
            "\x1b[?2026l",
            "\x1b[?25l",
            "\x1b[?25h",
            "\x1b[H",
            "\x1b[2J",
            "\x1b[3J",
            "\x1b[K",
            "\x1b[2K",
            "\x1b[5;20r",
            "\x1b[r",
            "\x1b[3L",
            "\x1b[2M",
            "\x1bM",
            "\x1b[4S",
            "\x1b[2T",
            "\x1b[12;40H",
            "\x1b[200b",
            "\x1bc",
            "\x1b[?3l",
            "\x1b[P",
            "\x1b[4@",
            "\x1b[30;1H",
        ];
        let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = |bound: usize| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % bound as u64) as usize
        };
        for _ in 0..150 {
            let log: String = (0..400).map(|_| PIECES[next(PIECES.len())]).collect();
            let log = log.as_bytes();
            let mut live = Live::new();
            let mut start = 0;
            while start < log.len() {
                let end = (start + 1 + next(200)).min(log.len());
                let pause = [None, Some(false), Some(true)][next(3)];
                live.process(&log[start..end], Instant::now(), pause);
                start = end;
                if next(10) == 0 {
                    live.pending();
                }
            }
            live.end_sync();
            live.finish();
            live.take_written();
            assert!(!live.stopped(), "{:?}", String::from_utf8_lossy(log));
        }
    }
}

/// The resize contract: a resize never loses transcript text. The screen as it stands is written
/// out at the old size, the recorder starts fresh at the new size, and a row still visible after
/// the resize may therefore reach the transcript twice.
#[cfg(test)]
mod resize {
    use super::*;

    /// One task's emulator and the transcript it has written out so far.
    struct Written {
        live: Live,
        text: String,
    }

    impl Written {
        fn new(cols: u16, rows: u16) -> Written {
            Written {
                live: Live::at(cols, rows),
                text: String::new(),
            }
        }

        fn drain(&mut self) {
            self.text.push_str(&self.live.take_written());
        }

        fn feed(&mut self, bytes: &str) {
            self.live.process(bytes.as_bytes(), Instant::now(), None);
            self.drain();
        }

        fn resize(&mut self, cols: u16, rows: u16) {
            let replies = self.live.resize(cols, rows);
            assert!(replies.is_empty(), "no query was asked at the resize");
            self.drain();
        }

        fn finish(&mut self) -> String {
            self.live.finish();
            self.drain();
            let pending = self.live.pending();
            self.text.clone() + &pending
        }
    }

    /// Every non-blank line of a transcript.
    fn lines(text: &str) -> Vec<String> {
        text.lines()
            .filter(|line| !line.is_empty())
            .map(str::to_string)
            .collect()
    }

    /// The contract: no text is lost, and what a resize writes is written whole. Every expected
    /// line appears, as one line, in this order; it may appear more than once only because a row
    /// still on the screen after the resize can be written again, which is why appearance is
    /// checked in order rather than counted.
    fn assert_no_loss(text: &str, expected: &[&str]) {
        let written = lines(text);
        let mut at = 0;
        for want in expected {
            let found = written[at..]
                .iter()
                .position(|line| line == want)
                .unwrap_or_else(|| {
                    panic!("{want:?} is written whole, after {at} lines: {written:?}")
                });
            at += found + 1;
        }
    }

    #[test]
    fn normal_text_reflows_without_splitting_the_line() {
        let mut task = Written::new(20, 6);
        task.feed("abcdefghijklmnopqrs\r\n");
        task.resize(10, 6);
        task.feed("X\r\n");
        assert_no_loss(&task.finish(), &["abcdefghijklmnopqrs", "X"]);
    }

    #[test]
    fn a_row_shrink_writes_every_line_once_in_order() {
        let mut task = Written::new(40, 10);
        task.feed("l1\r\nl2\r\nl3\r\nl4\r\nl5\r\nl6\r\nl7\r\nl8\r\n");
        task.resize(20, 5);
        assert_no_loss(
            &task.finish(),
            &["l1", "l2", "l3", "l4", "l5", "l6", "l7", "l8"],
        );
    }

    /// Exact multiplicity: two rows are not one. With one bit per row and rows above 63 sharing
    /// the last bit, a write on each of two such rows read as the cursor's row alone.
    #[test]
    fn rows_above_sixty_three_are_not_one_row() {
        let mut drawn = Drawn::default();
        drawn.add(65);
        drawn.add(70);
        assert!(!drawn.only(70), "two rows are not one row");
        assert!(!drawn.only(65), "nor the other of the two");
        let mut single = Drawn::default();
        single.add(70);
        assert!(single.only(70), "one row is that row");
        assert!(!single.only(65), "and not another");
    }

    /// Two distinct rows above 63 are two rows. With one bit per row aliased to the last one,
    /// both writes look like the cursor's row, and the frame a following scroll writes for them
    /// holds only that row.
    #[test]
    fn writes_on_two_rows_above_sixty_three_are_two_rows() {
        let mut task = Written::new(40, 100);
        task.feed("\x1b[?1049h");
        task.feed("\x1b[65;1Hrow sixty five");
        task.feed("\x1b[70;1Hrow seventy");
        // A scroll of the alternate screen is where the rows written since the last frame show.
        task.feed("\x1b[1S");
        let text = task.finish();
        assert!(!task.live.stopped(), "the emulator never stops");
        for row in ["row sixty five", "row seventy"] {
            assert!(
                lines(&text).iter().any(|line| line == row),
                "{row} reaches the transcript: {:?}",
                lines(&text)
            );
        }
    }

    /// Combining marks survive a width change: the logical line is written whole, so no mark is
    /// left behind on a row border.
    #[test]
    fn combining_characters_survive_a_width_change() {
        let line = "e\u{301}".repeat(12);
        let mut task = Written::new(20, 6);
        task.feed(&line);
        task.resize(12, 6);
        task.feed("\r\nX\r\n");
        assert_no_loss(&task.finish(), &[line.as_str(), "X"]);
    }

    /// A line whose rows straddle the history and the screen is written as one line.
    #[test]
    fn a_normal_line_straddling_history_and_screen_is_written_whole() {
        let line = "abcdefghijklmnopqrstuvwxyz0123456789".repeat(6);
        assert_eq!(line.len(), 216);
        let mut task = Written::new(40, 10);
        task.feed(&format!("{line}\r\n\x1b[?1049hALT"));
        task.resize(20, 5);
        task.feed("\x1b[?1049l");
        let text = task.finish();
        assert!(!task.live.stopped(), "the emulator never stops");
        assert_no_loss(&text, &[line.as_str(), "ALT"]);
    }

    /// A partial line spilled from a scroll region that does not start at row zero is written
    /// whole, beside the header above it.
    #[test]
    fn a_partial_line_from_a_scroll_region_is_written_whole_beside_the_header() {
        let line = "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxy";
        assert_eq!(line.len(), 61);
        let mut task = Written::new(40, 6);
        task.feed("HEADER\r\n");
        // Scroll region rows 2..3, so the rows leaving it never enter history.
        task.feed("\x1b[2;3r\x1b[2;1H");
        task.feed(&format!("\r\n{line}"));
        task.resize(20, 6);
        let text = task.finish();
        assert_no_loss(&text, &["HEADER", line]);
        for written in lines(&text) {
            assert!(
                !written.starts_with("HEADERab"),
                "the header does not take the line's prefix: {written:?}"
            );
        }
    }

    /// 199 independent long lines on a full 500-column screen keep every line across the
    /// narrowest reflow.
    #[test]
    fn hard_lines_keep_every_byte_across_the_narrowest_reflow() {
        let line = format!("{}bbbbbb", format!("a{}", "界".repeat(9)).repeat(26));
        let mut task = Written::new(500, 200);
        task.feed(&format!("{line}\r\n").repeat(199));
        task.resize(20, 5);
        let text = task.finish();
        let written = lines(&text);
        assert_eq!(
            written.iter().filter(|written| *written == &line).count(),
            199,
            "all 199 lines are written whole"
        );
    }

    #[test]
    fn wide_text_survives_the_narrowest_reflow() {
        let line = ("a".to_string() + &"界".repeat(9)).repeat(5250);
        assert_eq!(line.len() + 1, 147_001);
        let mut task = Written::new(500, 200);
        task.feed(&format!("{line}\r\n"));
        task.resize(20, 5);
        assert_no_loss(&task.finish(), &[line.as_str()]);
    }

    /// The normal screen's rows survive a resize taken while the alternate screen is active:
    /// `Term::resize` reflows the inactive grid too, and its history is the rows a session start
    /// already wrote.
    #[test]
    fn a_shrink_while_the_alternate_screen_is_active_keeps_the_normal_text() {
        let mut task = Written::new(40, 10);
        task.feed(
            "n1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\r\n\
             n2 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\r\n\
             n3\r\nn4\r\nn5\r\nn6\r\nn7\r\nn8\r\n",
        );
        task.feed("\x1b[?1049h");
        task.feed("a1\r\na2");
        task.resize(20, 5);
        task.feed("\r\na3");
        task.feed("\x1b[?1049l");
        let text = task.finish();
        assert!(!task.live.stopped(), "the emulator never stops");
        assert_no_loss(
            &text,
            &[
                "n1 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "n2 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "n3",
                "n4",
                "n5",
                "n6",
                "n7",
                "n8",
                "a1",
                "a2",
                "a3",
            ],
        );
    }

    #[test]
    fn the_alternate_screen_keeps_every_row_across_a_resize() {
        let mut task = Written::new(40, 6);
        task.feed("n1\r\nn2\r\n");
        task.feed("\x1b[?1049h");
        task.feed("a1\r\na2\r\na3");
        task.resize(20, 6);
        task.feed("\r\na4");
        task.feed("\x1b[?1049l");
        let text = task.finish();
        assert!(!task.live.stopped(), "the emulator never stops");
        assert_no_loss(&text, &["n1", "n2", "a1", "a2", "a3", "a4"]);
    }

    /// Ten rows, the cursor on the last, a shrink to five rows, then a character appended to the
    /// row that survives.
    #[test]
    fn alt_shrink_keeps_every_row_and_the_edit() {
        let rows = "alpha\r\nbravo\r\ncharlie\r\ndelta\r\necho\r\nfoxtrot\r\ngolf\r\nhotel\r\nindia\r\njuliet";
        // The cursor's position drives which side alacritty retains when the shrink drops rows,
        // so both sides are covered: it arrived on the last row, or it was moved to the first.
        for (side, move_cursor) in [("bottom", ""), ("top", "\x1b[1;1H")] {
            let mut task = Written::new(40, 10);
            task.feed("\x1b[?1049h");
            task.feed(rows);
            task.feed(move_cursor);
            task.resize(40, 5);
            task.feed("\x1b[5;7HX");
            task.feed("\x1b[?1049l");
            let text = task.finish();
            assert!(!task.live.stopped(), "the emulator never stops: {side}");
            assert_no_loss(
                &text,
                &[
                    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
                    "india", "juliet",
                ],
            );
            assert!(
                lines(&text).iter().any(|line| line.ends_with('X')),
                "the row that survives the shrink keeps the edit, cursor at the {side}: {:?}",
                lines(&text)
            );
        }
    }

    /// Ten rows that read the same are ten rows, not one repeated.
    /// A synchronized update holds the bytes that follow it, declarations included: nothing in
    /// them is the terminal's until the update ends. The resize is what ends a held one, so a
    /// keyboard mode declared inside it takes effect during the resize, and a reader after the
    /// resize sees it. Nothing else here waits on a clock.
    #[test]
    fn a_resize_applies_a_keyboard_mode_a_synchronized_update_holds() {
        let mut task = Written::new(100, 40);
        task.feed("\x1b[?2026h\x1b[?1h");
        assert!(
            task.live.sync_deadline().is_some(),
            "the update is held, not ended"
        );
        assert!(
            !task.live.modes().expect("a live terminal has modes").cursor,
            "application cursor keys are not the terminal's while the update holds them"
        );
        task.resize(80, 30);
        assert!(
            task.live.sync_deadline().is_none(),
            "the resize ended the held update"
        );
        assert!(
            task.live.modes().expect("a live terminal has modes").cursor,
            "application cursor keys are the terminal's after the resize settled them"
        );
    }

    #[test]
    fn alt_repeated_and_wrapped_rows_keep_every_row() {
        let same = "same\r\n".repeat(9) + "same";
        let mut task = Written::new(40, 10);
        task.feed("\x1b[?1049h");
        task.feed(&same);
        task.resize(40, 5);
        task.feed("X");
        task.feed("\x1b[?1049l");
        let text = task.finish();
        assert!(!task.live.stopped(), "the emulator never stops");
        let written = lines(&text);
        let same_rows = written
            .iter()
            .filter(|line| line.as_str() == "same" || line.as_str() == "sameX")
            .count();
        assert!(
            same_rows >= 10,
            "all ten rows reach the transcript: {written:?}"
        );
        assert_eq!(
            written
                .iter()
                .filter(|line| line.as_str() == "sameX")
                .count(),
            1,
            "the edited row is written once: {written:?}"
        );
        for line in &written {
            assert!(
                line.as_str() == "same" || line.as_str() == "sameX" || line.starts_with("---"),
                "an extra row is a copy of a row visible at the resize: {line:?}"
            );
        }

        let mut wrapped = Written::new(20, 6);
        wrapped.feed("\x1b[?1049h0123456789abcdefghijklmnop");
        wrapped.resize(40, 6);
        wrapped.feed("\x1b[?1049l");
        let text = wrapped.finish();
        assert_no_loss(&text, &["0123456789abcdefghijklmnop"]);
    }

    /// A query the output asks while a synchronized update holds the frame is answered by the
    /// resize that settles it: the reply is returned, not dropped, for the caller to send.
    #[test]
    fn a_query_held_across_a_resize_is_answered_by_the_resize() {
        let mut live = Live::at(40, 10);
        live.process(b"\x1b[?2026h\x1b[6n", Instant::now(), None);
        let held = live.take_written();
        let replies = live.resize(20, 5);
        assert_eq!(
            replies, b"\x1b[1;1R",
            "the held query is answered at the resize, as end_sync answers it: {held:?}"
        );
        // Nothing is left for a later read to hand over.
        assert!(live.end_sync().is_empty(), "the reply is not queued twice");
        assert!(
            live.process(b"", Instant::now(), None).is_empty(),
            "the next read has no reply left"
        );
        assert!(!live.stopped(), "the emulator never stops");
    }

    /// A small held frame: the rows a synchronized update holds arrive, across a resize.
    #[test]
    fn a_held_row_reaches_the_transcript_across_a_resize() {
        let mut task = Written::new(40, 6);
        task.feed("\x1b[?1049h\x1b[?2026hsync text");
        task.resize(20, 6);
        task.feed("\x1b[?2026l\x1b[?1049l");
        let text = task.finish();
        assert!(!task.live.stopped(), "the emulator never stops");
        assert_no_loss(&text, &["sync text"]);
    }

    /// The contract allows a row still on the screen after the resize to reach the transcript
    /// twice. That is what it looks like: the row's text as it stood at the resize, then the same
    /// row as it stands when it finally leaves.
    #[test]
    fn a_row_visible_at_the_resize_that_changes_is_written_twice() {
        let mut task = Written::new(20, 6);
        task.feed("one\r\ntwo\r\n");
        task.resize(10, 6);
        // The row is still on the screen and the program writes over it.
        task.feed("\x1b[1;1Honetwo");
        // Enough lines to push it off the screen.
        task.feed("\r\nthree\r\nfour\r\nfive\r\nsix\r\nseven");
        assert_eq!(
            task.finish(),
            "one\ntwo\nonetwo\nthree\nfour\nfive\nsix\nseven\n"
        );
    }

    /// Output a synchronized update holds when the resize arrives is applied at the old size, so
    /// the rows it prints are not lost at the new height.
    #[test]
    fn a_held_frame_before_a_resize_is_written_at_the_old_size() {
        let rows = "alpha\r\nbravo\r\ncharlie\r\ndelta\r\necho\r\nfoxtrot\r\ngolf\r\nhotel\r\nindia\r\njuliet";
        let want = [
            "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india",
            "juliet",
        ];
        for shrunk in [false, true] {
            let mut task = Written::new(40, 10);
            task.feed("\x1b[?1049h\x1b[?2026h");
            task.feed(rows);
            if shrunk {
                task.resize(40, 5);
            }
            task.feed("\x1b[?2026l\x1b[?1049l");
            let text = task.finish();
            assert!(!task.live.stopped(), "the emulator never stops");
            let written = lines(&text);
            for row in want {
                assert!(
                    written.iter().any(|line| line == row),
                    "{row} is written{}: {written:?}",
                    if shrunk { " across the resize" } else { "" }
                );
            }
        }
    }

    /// A screen that grew from five rows to ten must not truncate a ten-row scroll against the
    /// height it had before.
    #[test]
    fn a_full_screen_scroll_after_a_growth_writes_every_row() {
        let rows: String = (1..=10).map(|k| format!("row{k:02}\r\n")).collect();
        for grown in [false, true] {
            let mut task = if grown {
                let mut task = Written::new(40, 5);
                task.resize(40, 10);
                task
            } else {
                Written::new(40, 10)
            };
            task.feed(&rows);
            // One native scroll of a full screen: the rows leave without the recorder seeing
            // each one.
            task.feed("\x1b[10S");
            let text = task.finish();
            let written = lines(&text);
            for k in 1..=10 {
                let row = format!("row{k:02}");
                assert!(
                    written.contains(&row),
                    "{row} survives the scroll{}: {written:?}",
                    if grown { " after the growth" } else { "" }
                );
            }
        }
    }
}
