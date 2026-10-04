//! The terminal emulator of a PTY task. One engine shows the live screen, reports the
//! program's keyboard modes, answers its terminal queries, and replays the stored log into a
//! transcript.
//!
//! The transcript keeps a scrollback history, so the lines a program pushed off the screen,
//! by scrolling, by inserting through a scroll region, or by a full redraw that clears the
//! history first, come back once each, in the order a user scrolling up would read them.
//! The alternate screen keeps no history, so the replay records its frames and merges each
//! alternate-screen session into one.

use crate::frames::History;
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

/// Scrollback history kept while rendering a transcript; older lines are dropped.
pub const HISTORY_LINES: usize = 10_000;

/// One transcript render at a time: the history holds up to `HISTORY_LINES` rows of cells.
static RENDERING: Mutex<()> = Mutex::new(());

/// Begins each alternate-screen session in a transcript, after the normal screen's history.
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
    /// A history reached `HISTORY_LINES`, so older lines may be missing.
    pub history_full: bool,
}

/// Renders the first `len` bytes of `log` and writes the text to `path`. `pauses` are log
/// offsets where the output paused, so the screen showed a finished frame there.
pub fn render(
    mut log: impl Read,
    len: u64,
    pauses: &[u64],
    path: &Path,
) -> std::io::Result<Transcript> {
    let _rendering = RENDERING.lock().unwrap_or_else(PoisonError::into_inner);
    let config = Config {
        scrolling_history: HISTORY_LINES,
        ..Config::default()
    };
    let mut replay = Replay {
        term: Term::new(config, &Size, VoidListener),
        sessions: Vec::new(),
        open: false,
        region: (0, usize::from(PTY_ROWS) - 1),
        last_scroll: None,
    };
    let mut parser: Processor = Processor::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    let mut offset = 0;
    let mut pauses = pauses.iter().copied().peekable();
    while offset < len {
        let want = (len - offset).min(buffer.len() as u64) as usize;
        let read = log.read(&mut buffer[..want])?;
        if read == 0 {
            break;
        }
        let mut start = 0;
        while let Some(pause) = pauses.next_if(|&pause| pause <= offset + read as u64) {
            let end = pause.saturating_sub(offset) as usize;
            if end > start {
                parser.advance(&mut replay, &buffer[start..end]);
                start = end;
            }
            replay.frame();
        }
        parser.advance(&mut replay, &buffer[start..read]);
        offset += read as u64;
    }
    // The log keeps no timing: a synchronized update still open at its end shows what it holds.
    parser.stop_sync(&mut replay);
    replay.frame();
    let mut term = replay.term;
    if term.mode().contains(TermMode::ALT_SCREEN) {
        // The normal screen and its history sit behind the alternate screen.
        term.swap_alt();
    }
    let history_full = term.grid().history_size() >= HISTORY_LINES
        || replay.sessions.iter().any(|session| session.truncated);
    let mut lines = lines(term.grid());
    for session in &replay.sessions {
        lines.push(ALTERNATE_MARKER.to_string());
        lines.extend(session.text());
    }
    let mut text = lines.join("\n");
    text.push('\n');
    std::fs::write(path, &text)?;
    Ok(Transcript {
        text,
        lines: lines.len(),
        alternate_sessions: replay.sessions.len(),
        history_full,
    })
}

/// The replayed terminal, recording the alternate screen's frames: before each operation that
/// ends a frame or destroys visible text, and where the output paused.
struct Replay {
    term: Term<VoidListener>,
    /// One history per alternate-screen session.
    sessions: Vec<History>,
    /// The last frame was on the alternate screen, so the next one continues its session.
    open: bool,
    /// The scroll region's first and last rows, zero-based.
    region: (usize, usize),
    /// The rows of the frame taken before the last scroll by one row, upward or not, while the
    /// only change since is to the row that scroll uncovered.
    last_scroll: Option<(Vec<String>, bool)>,
}

impl Replay {
    /// Whether the terminal is on the alternate screen; leaving it ends the session.
    fn alternate(&mut self) -> bool {
        let alternate = self.term.mode().contains(TermMode::ALT_SCREEN);
        if !alternate {
            self.open = false;
            self.last_scroll = None;
        }
        alternate
    }

    fn record(&mut self, rows: &[String]) {
        if !self.open {
            self.sessions.push(History::default());
            self.open = true;
        }
        if let Some(session) = self.sessions.last_mut() {
            session.show(rows, HISTORY_LINES);
        }
    }

    fn frame(&mut self) {
        self.last_scroll = None;
        if self.alternate() {
            let rows = screen_rows(self.term.grid());
            self.record(&rows);
        }
    }

    /// A frame before the scroll region scrolls by one row. Every scroll takes one, so each
    /// row is recorded before it leaves; while a program only scrolls and writes the row each
    /// scroll uncovers, the frame is the last one shifted with that row read again.
    fn scroll(&mut self, up: bool) {
        if !self.alternate() {
            return;
        }
        let (top, bottom) = self.region;
        let rows = match self.last_scroll.take() {
            Some((mut rows, last_up)) if last_up == up && top < bottom && bottom < rows.len() => {
                let uncovered = if up {
                    rows[top..=bottom].rotate_left(1);
                    bottom
                } else {
                    rows[top..=bottom].rotate_right(1);
                    top
                };
                rows[uncovered] = screen_row(self.term.grid(), uncovered);
                rows
            }
            _ => screen_rows(self.term.grid()),
        };
        self.record(&rows);
        self.last_scroll = Some((rows, up));
    }

    /// The next character or tab wraps, which scrolls at the scroll region's bottom.
    fn wraps_at_bottom(&self) -> bool {
        self.term.grid().cursor.input_needs_wrap
            && self.term.mode().contains(TermMode::LINE_WRAP)
            && self.cursor_line() == self.region.1
    }

    /// Setting or resetting column mode (DECCOLM) resets the scroll region, as the terminal does.
    fn column_mode(&mut self, mode: PrivateMode) {
        if mode == PrivateMode::Named(NamedPrivateMode::ColumnMode) {
            self.region = (0, usize::from(PTY_ROWS) - 1);
        }
    }

    /// Before or after a change to the cursor's row.
    fn draw(&mut self) {
        if let Some((_, up)) = self.last_scroll {
            let uncovered = if up { self.region.1 } else { self.region.0 };
            if self.cursor_line() != uncovered {
                self.last_scroll = None;
            }
        }
    }

    fn cursor_line(&self) -> usize {
        self.term.grid().cursor.point.line.0 as usize
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
        // A character past the last column wraps first, which scrolls at the region's bottom.
        // A character not ASCII may be wide and wrap from the last column, or a combining mark
        // that does not wrap, so only a full frame is certain to be right before it.
        if self.wraps_at_bottom() {
            match c.is_ascii() {
                true => self.scroll(true),
                false => self.frame(),
            }
        } else if !c.is_ascii()
            && self.cursor_line() == self.region.1
            && self.term.grid().cursor.point.column.0 + 1 >= usize::from(PTY_COLS)
        {
            self.frame();
        }
        self.draw();
        Handler::input(&mut self.term, c);
        // A wrap may have written to the next row.
        self.draw();
    }

    fn put_tab(&mut self, count: u16) {
        // A tab past the last column wraps.
        if self.wraps_at_bottom() {
            self.scroll(true);
        }
        Handler::put_tab(&mut self.term, count);
        self.draw();
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

    fn decaln(&mut self) {
        self.frame();
        Handler::decaln(&mut self.term);
    }

    fn linefeed(&mut self) {
        if self.cursor_line() == self.region.1 {
            self.scroll(true);
        }
        Handler::linefeed(&mut self.term);
    }

    fn newline(&mut self) {
        if self.cursor_line() == self.region.1 {
            self.scroll(true);
        }
        Handler::newline(&mut self.term);
    }

    fn reverse_index(&mut self) {
        if self.cursor_line() == self.region.0 {
            self.scroll(false);
        }
        Handler::reverse_index(&mut self.term);
    }

    fn scroll_up(&mut self, lines: usize) {
        self.frame();
        Handler::scroll_up(&mut self.term, lines);
    }

    fn scroll_down(&mut self, lines: usize) {
        self.frame();
        Handler::scroll_down(&mut self.term, lines);
    }

    fn insert_blank_lines(&mut self, lines: usize) {
        self.frame();
        Handler::insert_blank_lines(&mut self.term, lines);
    }

    fn delete_lines(&mut self, lines: usize) {
        self.frame();
        Handler::delete_lines(&mut self.term, lines);
    }

    fn clear_screen(&mut self, mode: ClearMode) {
        self.frame();
        Handler::clear_screen(&mut self.term, mode);
    }

    fn reset_state(&mut self) {
        self.frame();
        Handler::reset_state(&mut self.term);
        self.region = (0, usize::from(PTY_ROWS) - 1);
    }

    // Programs switch screens, hide the cursor, and begin synchronized updates between frames.
    fn set_private_mode(&mut self, mode: PrivateMode) {
        self.frame();
        self.column_mode(mode);
        Handler::set_private_mode(&mut self.term, mode);
    }

    fn unset_private_mode(&mut self, mode: PrivateMode) {
        self.frame();
        self.column_mode(mode);
        Handler::unset_private_mode(&mut self.term, mode);
    }

    fn set_scrolling_region(&mut self, top: usize, bottom: Option<usize>) {
        Handler::set_scrolling_region(&mut self.term, top, bottom);
        self.last_scroll = None;
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

/// The screen's rows without trailing spaces.
fn screen_rows(grid: &Grid<Cell>) -> Vec<String> {
    (0..grid.screen_lines())
        .map(|line| screen_row(grid, line))
        .collect()
}

fn screen_row(grid: &Grid<Cell>, line: usize) -> String {
    let mut text = row_text(&grid[Line(line as i32)]);
    text.truncate(text.trim_end().len());
    text
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
        let transcript = render(log, log.len() as u64, &[], &path).unwrap();
        let _ = std::fs::remove_file(&path);
        transcript.text
    }

    #[test]
    fn alternate_screen_rows_pushed_off_stay_in_the_transcript() {
        // Full-width rows, all read at once with no pause, each scrolling the screen: by
        // wrapping, written back to back, followed by a tab, or carrying a combining mark that
        // lands in the last column; and printed on new lines below a fixed header, also after
        // column mode reset a smaller scroll region.
        let rows: Vec<String> = (0..80)
            .map(|k| format!("row {k:03} {}", "x".repeat(112)))
            .collect();
        let accented: Vec<String> = rows.iter().map(|row| format!("{row}\u{301}")).collect();
        let lines = |prefix: &str| {
            let lines: String = rows.iter().map(|row| format!("\r\n{row}")).collect();
            format!("\x1b[?1049h{prefix}{lines}")
        };
        for (log, expected) in [
            (format!("\x1b[?1049h{}", rows.concat()), &rows),
            (format!("\x1b[?1049h{}", rows.join("\t")), &rows),
            (format!("\x1b[?1049h{}", accented.concat()), &accented),
            (lines("header\x1b[2;30r\x1b[30;1H"), &rows),
            (lines("\x1b[2;20r\x1b[?3l\x1b[H"), &rows),
        ] {
            let text = render_text(log.as_bytes());
            let session: Vec<&str> = text
                .lines()
                .skip_while(|line| *line != ALTERNATE_MARKER)
                .filter(|line| line.starts_with("row "))
                .collect();
            assert_eq!(&session, expected, "{text}");
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
