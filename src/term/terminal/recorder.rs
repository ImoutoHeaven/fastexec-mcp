//! Transcript and alternate-screen recorder implementing `alacritty_terminal`'s VTE handler.

use super::osc::{
    ALTERNATE_MARKER, CELL_COMBINING_LIMIT, Drawn, KEYBOARD_STACK_LIMIT, LINE_LIMIT, NORMAL_MARKER,
    REPRINT_CONFIRM_LIMIT, REPRINT_REACH, Replies, SESSION_LINES, Size, is_mark, mark_char,
};
use crate::term::frames::{History, Row as FrameRow};
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
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use unicode_width::UnicodeWidthChar;

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
pub(super) enum Reprint {
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
pub(super) struct KeyboardStack {
    current: KeyboardModes,
    pub(super) saved: VecDeque<KeyboardModes>,
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
pub(super) struct Recorder {
    pub(super) term: Term<Replies>,
    pub(super) keyboard: [KeyboardStack; 2],
    /// The history of the alternate-screen session on screen.
    pub(super) session: Option<History>,
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
    pub(super) partial: Option<(String, Option<char>)>,
    /// The screen row where the partial logical line continues.
    partial_row: usize,
    /// The text of the lines written out while on screen, by the mark on their first row.
    pub(super) marked: HashMap<char, String>,
    /// Marks issued.
    marks: usize,
    /// An alternate-screen session ended, and no normal-screen line was written out since.
    after_session: bool,
    written: Written,
    pub(super) reprint: Reprint,
    /// Lines with text that must repeat written ones before a reprint is taken for one.
    confirm: usize,
    /// The emulator's options, kept so the history limit follows the screen's height.
    config: Config,
    /// Lines written out and not taken yet, each ending in a newline.
    pub(super) out: String,
}

impl Recorder {
    pub(super) fn new(term: Term<Replies>, config: Config) -> Recorder {
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
    pub(super) fn resize(&mut self, cols: usize, rows: usize) {
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
    pub(super) fn size(&self) -> (usize, usize) {
        (self.term.grid().columns(), self.term.grid().screen_lines())
    }

    pub(super) fn alternate(&self) -> bool {
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
    pub(super) fn commit(&mut self) {
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
    pub(super) fn finish(&mut self) {
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
    pub(super) fn pending(&self) -> String {
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

    pub(super) fn pause(&mut self, long: bool) {
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
    pub(super) fn stop_sync(&mut self, parser: &mut Processor) {
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

pub(super) fn frame_row(grid: &Grid<Cell>, line: usize) -> FrameRow {
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
