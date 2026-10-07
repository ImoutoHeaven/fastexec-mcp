//! The terminal emulator of a PTY task. One engine shows the live screen, reports the
//! program's keyboard modes, answers its terminal queries, and replays the stored log into a
//! transcript.
//!
//! The transcript keeps a scrollback history, so the lines a program pushed off the screen,
//! by scrolling, by inserting through a scroll region, or by a full redraw that clears the
//! history first, come back once each, in the order a user scrolling up would read them.
//! The alternate screen keeps no history, so the replay records its frames and merges each
//! alternate-screen session into one.

use crate::frames::{History, Row as FrameRow};
use crate::keys::Modes;
use crate::process::{PTY_COLS, PTY_ROWS};
use alacritty_terminal::event::{Event, EventListener, VoidListener};
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
use std::io::Read;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Instant;
use unicode_width::UnicodeWidthChar;

/// Scrollback history kept while rendering a transcript; older lines are dropped.
pub const HISTORY_LINES: usize = 10_000;

/// One transcript render at a time: the history holds up to `HISTORY_LINES` rows of cells.
static RENDERING: Mutex<()> = Mutex::new(());

/// Marks: a log offset in the low bits and what the screen did there in the high bits.
/// The output paused, so the screen showed a finished frame.
pub const PAUSE: u64 = 1 << 61;
/// The pause lasted long enough to read the frame.
pub const LONG: u64 = 1 << 62;
/// A synchronized update ended at its timeout and showed the output it held.
pub const SYNC_END: u64 = 1 << 63;

pub fn mark_offset(mark: u64) -> u64 {
    mark & (PAUSE - 1)
}

/// Begins each alternate-screen session in a transcript, among the normal screen's lines where
/// the session began.
pub const ALTERNATE_MARKER: &str = "--- alternate screen ---";
/// Follows an alternate-screen session when normal-screen lines come after it.
pub const NORMAL_MARKER: &str = "--- normal screen ---";

/// The replay marks the normal-screen rows shown when an alternate-screen session began with
/// a zero-width character from Unicode's private use planes 15 and 16, one per session: a mark
/// moves with its row through the history, and a program never stores one as zero-width.
const PLANE: u32 = 0xFFFE;
const SESSION_MARKS: [u32; 2] = [0xF0000, 0x100000];

/// The mark of session `session`, if one is left.
fn session_char(session: usize) -> Option<char> {
    let plane = SESSION_MARKS.get(session / PLANE as usize)?;
    char::from_u32(plane + (session % PLANE as usize) as u32)
}

/// The session a mark names.
fn session_mark(c: char) -> Option<usize> {
    SESSION_MARKS.iter().enumerate().find_map(|(plane, &base)| {
        let index = (c as u32).checked_sub(base)?;
        (index < PLANE).then_some(plane * PLANE as usize + index as usize)
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

    /// Reads one piece of output, read from the program at `arrived`, and returns the replies
    /// it asked for and whether a synchronized update (mode 2026) past its deadline ended
    /// first, so it cannot outlast its timeout while output keeps arriving.
    pub fn process(&mut self, output: &[u8], arrived: Instant) -> (Vec<u8>, bool) {
        self.read += output.len() as u64;
        let expired = self
            .sync_deadline()
            .is_some_and(|deadline| deadline <= arrived);
        let replies = self.run(|term, parser| {
            if expired {
                parser.stop_sync(term);
            }
            parser.advance(term, output);
        });
        (replies, expired)
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
        let mut rows = screen_rows(grid);
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
    /// Alternate-screen sessions, each after an `ALTERNATE_MARKER` line.
    pub alternate_sessions: usize,
    /// Each session's lines in `text`, one-based, from its marker to its last line.
    pub alternate_ranges: Vec<(usize, usize)>,
    /// A history reached `HISTORY_LINES`, so older lines may be missing.
    pub history_full: bool,
}

/// Renders the first `len` bytes of `log` and writes the text to `path`, replaying `marks`
/// where they fall. `closed` says the log is complete: a synchronized update still open at
/// its end shows what it holds, as the live terminal does once output ends.
pub fn render(
    mut log: impl Read,
    len: u64,
    marks: &[u64],
    closed: bool,
    path: &Path,
) -> std::io::Result<Transcript> {
    let _rendering = RENDERING.lock().unwrap_or_else(PoisonError::into_inner);
    let config = Config {
        scrolling_history: HISTORY_LINES,
        ..Config::default()
    };
    let mut replay = Replay::new(Term::new(config, &Size, VoidListener));
    let mut parser: Processor = Processor::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    let mut offset = 0;
    let mut marks = marks.iter().copied().peekable();
    while offset < len {
        let want = (len - offset).min(buffer.len() as u64) as usize;
        let read = log.read(&mut buffer[..want])?;
        if read == 0 {
            break;
        }
        let mut start = 0;
        while let Some(mark) = marks.next_if(|&mark| mark_offset(mark) <= offset + read as u64) {
            let end = mark_offset(mark).saturating_sub(offset) as usize;
            if end > start {
                parser.advance(&mut replay, &buffer[start..end]);
                start = end;
            }
            if mark & SYNC_END != 0 {
                parser.stop_sync(&mut replay);
            }
            if mark & PAUSE != 0 {
                replay.pause(mark & LONG != 0);
            }
        }
        parser.advance(&mut replay, &buffer[start..read]);
        offset += read as u64;
    }
    if closed {
        parser.stop_sync(&mut replay);
    }
    replay.present();
    let mut term = replay.term;
    if term.mode().contains(TermMode::ALT_SCREEN) {
        // The normal screen and its history sit behind the alternate screen.
        term.swap_alt();
    }
    let history_full = term.grid().history_size() >= HISTORY_LINES
        || replay.sessions.iter().any(|session| session.truncated);
    let (normal, marks) = lines(term.grid());
    // Each session follows the last line still marked of those shown when it began, and the
    // session before it. A session whose marked lines are all gone, cleared, past the
    // history's limit, or written over, follows the session before it, or comes first.
    let mut after: Vec<Option<usize>> = vec![None; replay.sessions.len()];
    for (line, session) in marks {
        if let Some(after) = after.get_mut(session) {
            *after = Some(line + 1);
        }
    }
    let mut previous = 0;
    let after: Vec<usize> = after
        .into_iter()
        .map(|after| {
            previous = previous.max(after.unwrap_or(previous));
            previous
        })
        .collect();
    let mut lines: Vec<String> = Vec::new();
    let mut alternate_ranges = Vec::new();
    for at in 0..=normal.len() {
        let mut began = false;
        for (session, _) in after
            .iter()
            .enumerate()
            .filter(|&(_, &after)| after.min(normal.len()) == at)
        {
            lines.push(ALTERNATE_MARKER.to_string());
            let first = lines.len();
            lines.extend(replay.sessions[session].text());
            alternate_ranges.push((first, lines.len()));
            began = true;
        }
        if let Some(line) = normal.get(at) {
            if began {
                lines.push(NORMAL_MARKER.to_string());
            }
            lines.push(line.clone());
        }
    }
    let mut text = lines.join("\n");
    text.push('\n');
    std::fs::write(path, &text)?;
    Ok(Transcript {
        text,
        lines: lines.len(),
        alternate_sessions: replay.sessions.len(),
        alternate_ranges,
        history_full,
    })
}

/// The replayed terminal, telling each alternate-screen session's history what its screen
/// does: the rows each scroll moves, and the frames the program finished drawing.
struct Replay {
    term: Term<VoidListener>,
    /// One history per alternate-screen session.
    sessions: Vec<History>,
    /// The last session is still on screen.
    open: bool,
    /// The scroll region's first and last rows, zero-based.
    region: (usize, usize),
    /// Inside a synchronized update, whose drawing no one sees until it ends.
    held: bool,
    /// Times the program showed the cursor again after hiding it.
    redraws: usize,
    /// Rows written since the history last read the screen, one bit per row.
    written: u64,
    /// The frame read from the rows written alone, kept to reuse its memory.
    frame: Vec<FrameRow>,
}

impl Replay {
    fn new(term: Term<VoidListener>) -> Replay {
        Replay {
            term,
            sessions: Vec::new(),
            open: false,
            region: (0, usize::from(PTY_ROWS) - 1),
            held: false,
            redraws: 0,
            written: 0,
            frame: Vec::new(),
        }
    }

    /// The cursor's row is being written.
    fn write(&mut self) {
        self.written |= 1 << self.cursor_line().min(63);
    }

    /// The history of the alternate screen on screen; leaving it ends the session.
    fn session(&mut self) -> Option<&mut History> {
        if !self.term.mode().contains(TermMode::ALT_SCREEN) {
            self.open = false;
            return None;
        }
        if !self.open {
            self.sessions
                .push(History::new(self.term.grid().screen_lines(), HISTORY_LINES));
            self.open = true;
        }
        self.sessions.last_mut()
    }

    /// The screen is a finished frame, unless a synchronized update holds its drawing.
    fn present(&mut self) {
        if self.held || !self.term.mode().contains(TermMode::ALT_SCREEN) {
            return;
        }
        let rows = frame_rows(self.term.grid());
        if let Some(session) = self.session() {
            session.present(&rows);
        }
        self.written = 0;
    }

    /// Where the output paused the screen shows a finished frame, unless the program hides the
    /// cursor while it draws, as it has before, and the cursor is hidden: then the pause fell
    /// inside a redraw, and the frame that shows the cursor again holds it whole. A long pause
    /// showed the screen long enough to read either way.
    fn pause(&mut self, long: bool) {
        let hidden = !self.term.mode().contains(TermMode::SHOW_CURSOR);
        if long || !hidden || self.redraws < 2 {
            self.present();
        }
    }

    /// Rows `top..end` scroll by `n` rows.
    fn shift(&mut self, top: usize, end: usize, n: usize, up: bool) {
        if !self.term.mode().contains(TermMode::ALT_SCREEN) {
            return;
        }
        // Rows written since the last frame show before they move: a frame of the screen, read
        // from the rows written alone when that is the cursor's row, as a program printing
        // lines writes it.
        if !self.held && self.written != 0 {
            let cursor = self.cursor_line();
            if self.written == 1 << cursor.min(63) {
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
                self.written = 0;
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

/// Forwards `Handler` methods to the replayed terminal unchanged.
macro_rules! forward {
    ($($name:ident($($arg:ident: $ty:ty),*);)*) => {
        $(fn $name(&mut self, $($arg: $ty),*) { Handler::$name(&mut self.term, $($arg),*) })*
    };
}

impl Handler for Replay {
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
        self.write();
    }

    fn put_tab(&mut self, count: u16) {
        // A tab past the last column wraps.
        if self.wraps() {
            self.feed();
        }
        Handler::put_tab(&mut self.term, count);
        self.write();
    }

    fn insert_blank(&mut self, count: usize) {
        self.write();
        Handler::insert_blank(&mut self.term, count);
    }

    fn erase_chars(&mut self, count: usize) {
        self.write();
        Handler::erase_chars(&mut self.term, count);
    }

    fn delete_chars(&mut self, count: usize) {
        self.write();
        Handler::delete_chars(&mut self.term, count);
    }

    fn clear_line(&mut self, mode: LineClearMode) {
        self.write();
        Handler::clear_line(&mut self.term, mode);
    }

    fn linefeed(&mut self) {
        self.feed();
        Handler::linefeed(&mut self.term);
    }

    fn newline(&mut self) {
        self.feed();
        Handler::newline(&mut self.term);
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
    }

    // Erasing destroys what the screen showed, so the frame before it is recorded.
    fn clear_screen(&mut self, mode: ClearMode) {
        self.present();
        Handler::clear_screen(&mut self.term, mode);
    }

    fn decaln(&mut self) {
        self.present();
        Handler::decaln(&mut self.term);
    }

    fn reset_state(&mut self) {
        self.present();
        Handler::reset_state(&mut self.term);
        self.region = (0, usize::from(PTY_ROWS) - 1);
        self.held = false;
    }

    // Programs hide the cursor before they draw and show it once done, switch screens between
    // frames, and hold a frame's drawing in a synchronized update.
    fn set_private_mode(&mut self, mode: PrivateMode) {
        self.mode_change(mode, true);
        let entering = mode == PrivateMode::Named(NamedPrivateMode::SwapScreenAndSetRestoreCursor)
            && !self.term.mode().contains(TermMode::ALT_SCREEN);
        if entering {
            self.mark_session_start();
        }
        Handler::set_private_mode(&mut self.term, mode);
        if entering {
            self.open = false;
            self.session();
        }
    }

    fn unset_private_mode(&mut self, mode: PrivateMode) {
        self.mode_change(mode, false);
        Handler::unset_private_mode(&mut self.term, mode);
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

impl Replay {
    /// Marks the normal-screen text shown before the session about to begin: the first
    /// character of each screen row with text, and the last line of the history, which no
    /// program writes over. Text the program erases or writes over later loses its mark; the
    /// last mark left tells where the session began.
    fn mark_session_start(&mut self) {
        let Some(mark) = session_char(self.sessions.len()) else {
            return;
        };
        let grid = self.term.grid_mut();
        if grid.history_size() > 0 {
            grid[Line(-1)][Column(0)].push_zerowidth(mark);
        }
        for line in 0..grid.screen_lines() as i32 {
            let row = &mut grid[Line(line)];
            // A glyph is a base character other than whitespace, or a combining mark on a blank;
            // a combining mark precedes any session marks on its cell.
            let text = row[..].iter().position(|cell| {
                (!cell.c.is_whitespace() && cell.c != '\0')
                    || cell
                        .zerowidth()
                        .is_some_and(|marks| marks.iter().any(|&c| session_mark(c).is_none()))
            });
            if let Some(column) = text {
                row[Column(column)].push_zerowidth(mark);
            }
        }
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
                self.region = (0, usize::from(PTY_ROWS) - 1);
            }
            _ => {}
        }
    }
}

/// The screen's rows without trailing spaces.
fn screen_rows(grid: &Grid<Cell>) -> Vec<String> {
    (0..grid.screen_lines())
        .map(|line| frame_row(grid, line).text)
        .collect()
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
        wraps: row[Column(grid.columns() - 1)]
            .flags
            .contains(Flags::WRAPLINE),
    }
}

/// A grid's history and screen as logical lines, without trailing blank lines, and the
/// session marks they carry: the index of each marked line and the session.
fn lines(grid: &Grid<Cell>) -> (Vec<String>, Vec<(usize, usize)>) {
    let last = Column(grid.columns() - 1);
    let mut lines = Vec::new();
    let mut marks = Vec::new();
    let mut line = String::new();
    for index in -(grid.history_size() as i32)..grid.screen_lines() as i32 {
        let row = &grid[Line(index)];
        line.push_str(&row_text(row));
        marks.extend(
            row[..]
                .iter()
                .flat_map(|cell| cell.zerowidth().into_iter().flatten())
                .filter_map(|&c| session_mark(c))
                .map(|mark| (lines.len(), mark)),
        );
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
    (lines, marks)
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
        text.extend(marks.filter(|&&c| session_mark(c).is_none()));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Renders `log` as the transcript action does, reading it in 64 KiB pieces.
    fn render_text(log: &[u8]) -> String {
        render_marked(log, &[], true)
    }

    fn render_marked(log: &[u8], marks: &[u64], closed: bool) -> String {
        let path = std::env::temp_dir().join(format!(
            "fastexec-transcript-test-{}-{}-{}.txt",
            std::process::id(),
            log.len(),
            marks.len()
        ));
        let transcript = render(log, log.len() as u64, marks, closed, &path).unwrap();
        let _ = std::fs::remove_file(&path);
        transcript.text
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
            // Erasing the text of a row erases its mark, though its indentation stays.
            (
                indented,
                vec![ALTERNATE_MARKER, "inside 1", NORMAL_MARKER, "  after"],
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
            // The history the session followed was cleared after it.
            (
                cleared,
                vec![ALTERNATE_MARKER, "inside 1", NORMAL_MARKER, "after"],
            ),
            // The program wrote over the cursor's row after the session.
            (
                rewritten,
                vec!["first", ALTERNATE_MARKER, "inside 1", NORMAL_MARKER, "last"],
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
            // The rows the second session began below were erased or written over; the line
            // above them still precedes it.
            (
                erased,
                vec![
                    "first",
                    ALTERNATE_MARKER,
                    "inside 1",
                    NORMAL_MARKER,
                    "between",
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
    }

    /// The transcript lines with text of an alternate-screen session drawing each of `frames`
    /// as a synchronized update: a header, the frame's rows from row 20 on, and a footer.
    fn streamed(frames: &[[&str; 5]]) -> Vec<String> {
        let mut log = String::from("[?1049h");
        for rows in frames {
            log.push_str("[?2026h[1;1H[2Kheader");
            for (k, row) in rows.iter().enumerate() {
                log.push_str(&format!("[{};1H[2K{row}", 20 + k));
            }
            log.push_str("[27;1H[2Kfooter[?2026l");
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
        let pause = torn.len() as u64 | PAUSE;
        for (marks, shown) in [(vec![pause], false), (vec![pause | LONG], true)] {
            let text = render_marked(log.as_bytes(), &marks, true);
            assert_eq!(text.contains("drawing row"), shown, "{marks:?}: {text}");
            assert!(text.contains("final row"), "{text}");
        }
    }

    #[test]
    fn the_replay_ends_synchronized_updates_where_the_terminal_did() {
        // The first page shows at the update's timeout, then the second replaces it before the
        // update ends; a running task's update still open at the end of its log shows nothing.
        let first = "\x1b[?1049h\x1b[?2026h\x1b[Hfirst page";
        let log = format!("{first}\x1b[Hsecond page\x1b[?2026l\x1b[?2026h\x1b[H\x1b[2Kheld page");
        let timeout = first.len() as u64 | SYNC_END;
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
            let text = render_marked(log.as_bytes(), &marks, closed);
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
        live.process(b"before\r\n\x1b[?2026h", Instant::now());
        live.process(b"held", Instant::now());
        let (rows, _) = live.screen().unwrap();
        assert_eq!(rows, ["before"]);
        assert_eq!(live.shown(), 16, "the held bytes are not shown yet");
        assert!(live.sync_deadline().is_some());
        live.process(b"\x1b[?2026l", Instant::now());
        let (rows, _) = live.screen().unwrap();
        assert_eq!(rows, ["before", "held"]);
        assert_eq!(live.shown(), 28);
        assert!(live.sync_deadline().is_none());
    }
}
