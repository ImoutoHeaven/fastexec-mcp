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
use std::collections::{HashMap, VecDeque};
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
/// a screen of them, more than a command run again after `clear` usually prints.
const REPRINT_CONFIRM: usize = PTY_ROWS as usize;
/// A line whose rows wrap for longer is written out in pieces this long, so a program that
/// prints without line breaks holds no more than this in memory.
const LINE_LIMIT: usize = 1 << 20;

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
    recorder: Recorder,
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
            // One operation pushes at most a screen of rows, and the recorder takes the history
            // after each.
            scrolling_history: usize::from(PTY_ROWS),
            kitty_keyboard: true,
            ..Config::default()
        };
        Live {
            recorder: Recorder::new(Term::new(config, &Size, replies.clone())),
            parser: Processor::new(),
            replies,
            read: 0,
            stopped: false,
        }
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
        self.run(|recorder, parser| {
            if expired {
                parser.stop_sync(recorder);
            }
            if let Some(long) = pause {
                recorder.pause(long);
            }
            parser.advance(recorder, output);
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
            parser.stop_sync(recorder);
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
        self.read - self.parser.sync_bytes_count() as u64
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
    fn step(&mut self, written: &Written, line: String) -> Vec<String> {
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
                if *texts >= REPRINT_CONFIRM {
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

/// The terminal as the program drives it, recording the transcript: the rows it pushes off the
/// normal screen, and the scrolls and frames of each alternate-screen session.
struct Recorder {
    term: Term<Replies>,
    /// The history of the alternate-screen session on screen.
    session: Option<History>,
    /// The scroll region's first and last rows, zero-based.
    region: (usize, usize),
    /// Inside a synchronized update, whose drawing no one sees until it ends.
    held: bool,
    /// Times the program showed the cursor again after hiding it.
    redraws: usize,
    /// Rows written since the session's history last read the screen, one bit per row.
    drawn: u64,
    /// The frame read from the rows written alone, kept to reuse its memory.
    frame: Vec<FrameRow>,
    /// A line whose first rows left the screen while its last one wraps onto it, and the mark
    /// of its first row.
    partial: Option<(String, Option<char>)>,
    /// The text of the lines written out while on screen, by the mark on their first row.
    marked: HashMap<char, String>,
    /// Marks issued.
    marks: usize,
    /// An alternate-screen session ended, and no normal-screen line was written out since.
    after_session: bool,
    written: Written,
    reprint: Reprint,
    /// Lines written out and not taken yet, each ending in a newline.
    out: String,
}

impl Recorder {
    fn new(term: Term<Replies>) -> Recorder {
        Recorder {
            term,
            session: None,
            region: (0, usize::from(PTY_ROWS) - 1),
            held: false,
            redraws: 0,
            drawn: 0,
            frame: Vec::new(),
            partial: None,
            marked: HashMap::new(),
            marks: 0,
            after_session: false,
            written: Written::default(),
            reprint: Reprint::Off,
            out: String::new(),
        }
    }

    fn alternate(&self) -> bool {
        self.term.mode().contains(TermMode::ALT_SCREEN)
    }

    fn push(&mut self, line: &str) {
        self.out.push_str(line);
        self.out.push('\n');
    }

    /// Writes out the lines settled since the last call.
    fn commit(&mut self) {
        self.spill();
        let lines = self.session.as_mut().map(History::commit);
        for line in lines.into_iter().flatten() {
            self.push(&line);
        }
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
            for line in self.screen_lines() {
                if !self.already_written(&line) {
                    lines.extend(reprint.step(&self.written, line.text));
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
        for line in self.reprint.step(&self.written, line) {
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
        for (text, wraps, mark) in rows {
            let (mut line, mark) = self.partial.take().unwrap_or((String::new(), mark));
            line.push_str(&text);
            if wraps && line.len() < LINE_LIMIT {
                self.partial = Some((line, mark));
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
            .find(|&line| !blank(&row_text(&grid[Line(line as i32)])));
        let mut lines = Vec::new();
        let Some(last_text) = last_text else {
            if let Some((text, mark)) = &self.partial {
                lines.push(ScreenLine {
                    text: text.trim_end().to_string(),
                    left: text.len(),
                    mark: *mark,
                    row: 0,
                    row_mark: None,
                });
            }
            return lines;
        };
        let (mut text, mut mark) = self.partial.clone().unwrap_or_default();
        let mut left = text.len();
        let mut start = 0;
        for line in 0..=last_text {
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
        self.drawn |= 1 << self.cursor_line().min(63);
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
        self.drawn = 0;
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
            return;
        }
        // Rows written since the last frame show before they move: a frame of the screen, read
        // from the rows written alone when that is the cursor's row, as a program printing
        // lines writes it.
        if !self.held && self.drawn != 0 {
            let cursor = self.cursor_line();
            if self.drawn == 1 << cursor.min(63) {
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
                self.drawn = 0;
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
        self.region = (0, usize::from(PTY_ROWS) - 1);
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
        self.end_session(false);
    }

    fn set_scrolling_region(&mut self, top: usize, bottom: Option<usize>) {
        Handler::set_scrolling_region(&mut self.term, top, bottom);
        // As the terminal does: 1-based rows, ignored unless the top is above the bottom.
        let rows = usize::from(PTY_ROWS);
        let bottom = bottom.unwrap_or(rows);
        if top < bottom {
            self.region = (top.saturating_sub(1).min(rows), bottom.min(rows) - 1);
        }
    }

    forward! {
        set_title(title: Option<String>);
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
        clipboard_store(clipboard: u8, data: &[u8]);
        clipboard_load(clipboard: u8, terminator: &str);
        push_title();
        pop_title();
        text_area_size_pixels();
        text_area_size_chars();
        set_hyperlink(link: Option<Hyperlink>);
        set_mouse_cursor_icon(icon: CursorIcon);
        report_keyboard_mode();
        push_keyboard_mode(mode: KeyboardModes);
        pop_keyboard_modes(to_pop: u16);
        set_keyboard_mode(mode: KeyboardModes, behavior: KeyboardModesApplyBehavior);
        set_modify_other_keys(mode: ModifyOtherKeys);
        report_modify_other_keys();
        set_scp(char_path: ScpCharPath, update_mode: ScpUpdateMode);
    }
}

impl Recorder {
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
                self.region = (0, usize::from(PTY_ROWS) - 1);
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
    let mut text = row_text(row);
    text.truncate(text.trim_end().len());
    FrameRow {
        text,
        wraps: wraps(grid, row),
    }
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
        for piece in log.as_bytes().chunks(4096) {
            live.process(piece, Instant::now(), None);
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
