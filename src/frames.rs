//! The scrollback the alternate screen lacks, built from what an alternate-screen session
//! showed.
//!
//! The screen is a window onto a document that grows as the program runs. The history holds
//! one line per row the screen showed, in document order, and knows which line each row
//! shows. Two kinds of events update it:
//!
//! - A scroll moves rows by a known distance (`shift`). The rows that leave keep their lines,
//!   which take the text the rows hold as they leave; the rows it uncovers get placeholder
//!   lines beside the rows they join, filled once a frame shows them.
//! - A frame (`present`) is the screen as the program finished drawing it. A row that still
//!   shows its line's text keeps the line, as does a row whose text moved with the rows around
//!   it. Any other row is reconciled with the lines around it: it continues a line no longer
//!   shown, shows again a stretch of lines beside its neighbours, updates the line it replaced,
//!   or adds a new line.
//!
//! Every row that leaves the screen stays in the history. When a row may be a new line or an
//! old one changed, it is a new line: a duplicate is preferable to a lost line.

use std::collections::HashSet;
use std::ops::Range;

/// Rows with text that show a stretch of the history again away from their neighbours' lines.
const STRETCH: usize = 3;
/// Lines with text between a neighbour's line and the stretch beside it, such as a status
/// row a pager redraws below each scroll.
const NEAR: usize = 3;
/// Lines a row may continue, counted from the line next to it.
const CONTINUE_REACH: usize = 8;
/// Rows of one changed region whose numbers may change in place; more is a different page.
// ponytail: fixed count; a region of many counters updating at once keeps their old values.
const MAX_NUMBER_UPDATES: usize = 3;
/// Lines around a row's line where a shorter copy of it is an echo.
const ECHO_REACH: usize = 3;
/// Rows around a changed region whose words can absorb a reflowed row.
const REFLOW_CONTEXT: usize = 2;
/// Words a reflowed row needs, so a short row is never taken for part of another.
const REFLOW_WORDS: usize = 3;

/// One screen row: its text without trailing spaces, and whether the terminal wrapped it
/// onto the next row.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct Row {
    pub text: String,
    pub wraps: bool,
}

struct Line {
    text: String,
    wraps: bool,
    /// Out of the history: reflowed or redrawn elsewhere, a blank row its old line showed
    /// again, a placeholder whose row showed another line, or past the limit.
    dropped: bool,
    /// Uncovered by a scroll and not yet shown by a frame or read as it left.
    placeholder: bool,
    /// The first and last frames that showed the line; `first` is `usize::MAX` until one does.
    first: usize,
    last: usize,
    /// Left the screen by a scroll, so no later line is a version of it.
    scrolled: bool,
    /// Lines added before it, counting lines out of the history; ids are reused, serials not.
    serial: u64,
}

pub struct History {
    lines: Vec<Line>,
    /// Line ids in document order, and each line's index in it.
    order: Vec<usize>,
    pos: Vec<usize>,
    /// The line each screen row shows, and the row as the history last knew it.
    screen: Vec<usize>,
    rows: Vec<Row>,
    frame: usize,
    /// Lines the history holds before the oldest leave.
    limit: usize,
    /// Ids of lines that left the history, for new lines to reuse.
    free: Vec<usize>,
    /// Lines added so far.
    added: u64,
    /// Lines left the history once it held its limit.
    pub truncated: bool,
}

/// How a row of a changed region relates to the line of the row it replaced.
#[derive(Clone, Copy, PartialEq)]
enum Update {
    /// A blank row filled, or blank still.
    Fill,
    /// The text grew or lost a little of its end: nothing is lost.
    Extend,
    /// Only numbers or symbols changed, as a counter, timer, or spinner does: the old values go.
    Numbers,
}

impl History {
    /// A session whose screen of `height` rows starts blank, holding up to `limit` lines.
    pub fn new(height: usize, limit: usize) -> History {
        let mut history = History {
            lines: Vec::new(),
            order: Vec::new(),
            pos: Vec::new(),
            screen: Vec::new(),
            rows: vec![Row::default(); height],
            frame: 0,
            limit,
            free: Vec::new(),
            added: 0,
            truncated: false,
        };
        history.screen = (0..height).map(|k| history.insert(k, "", true)).collect();
        history
    }

    /// Rows `top..end` scroll by `n` rows, up or down. `leaving` holds the rows that leave, as
    /// the screen shows them now, top first; `None` when no one saw them, inside a
    /// synchronized update.
    pub fn shift(&mut self, top: usize, end: usize, n: usize, up: bool, leaving: Option<&[Row]>) {
        let n = n.min(end.saturating_sub(top));
        if n == 0 {
            return;
        }
        let gone = if up { top..top + n } else { end - n..end };
        for (k, row) in gone.clone().enumerate() {
            let id = self.screen[row];
            match leaving {
                Some(rows) => {
                    let line = self.take(id, &rows[k]);
                    self.lines[line].scrolled = true;
                }
                None if self.lines[id].placeholder => self.lines[id].dropped = true,
                None => self.lines[id].scrolled = true,
            }
        }
        // Uncovered rows join the band beside its last row, scrolling up, or its first.
        let band = if up {
            self.screen[end - 1]
        } else {
            self.screen[top]
        };
        let at = self.pos[band] + usize::from(up);
        let fresh: Vec<usize> = (0..n).map(|k| self.insert(at + k, "", true)).collect();
        let (screen, rows) = (&mut self.screen[top..end], &mut self.rows[top..end]);
        if up {
            screen.rotate_left(n);
            rows.rotate_left(n);
        } else {
            screen.rotate_right(n);
            rows.rotate_right(n);
        }
        let new = if up { end - n..end } else { top..top + n };
        for (row, id) in new.zip(fresh) {
            self.screen[row] = id;
            self.rows[row] = Row::default();
        }
        self.limit();
    }

    /// The screen as the history knows it: the last frame, moved by the scrolls since.
    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// The row of line `id` shows `row` now, without a frame around it: the line that holds
    /// the row's text from now on.
    fn take(&mut self, id: usize, row: &Row) -> usize {
        let line = &self.lines[id];
        if line.placeholder {
            // Scrolling over lines shown before: the row shows the line beside its own.
            if !blank(&row.text) {
                for up in [true, false] {
                    if let Some(seen) = self.beside(id, up)
                        && self.lines[seen].text == row.text
                    {
                        self.resolve(id, seen, up);
                        return seen;
                    }
                }
            }
            let line = &mut self.lines[id];
            line.placeholder = false;
            line.text.clone_from(&row.text);
            line.wraps = row.wraps;
            return id;
        }
        if line.text == row.text
            || matches!(
                update(&line.text, &row.text),
                Some(Update::Fill | Update::Extend)
            )
        {
            // Without a frame around it, a row whose numbers changed may be another row.
            let line = &mut self.lines[id];
            line.text.clone_from(&row.text);
            line.wraps = row.wraps;
            return id;
        }
        // The row now shows another line, which follows the one it showed before.
        let new = self.insert(self.pos[id] + 1, &row.text, false);
        self.lines[new].wraps = row.wraps;
        new
    }

    /// The first line after placeholder `id`, or before it, that no row shows; placeholders
    /// and dropped lines are passed over.
    fn beside(&self, id: usize, up: bool) -> Option<usize> {
        let at = self.pos[id];
        let positions: Box<dyn Iterator<Item = usize>> = if up {
            Box::new(at + 1..self.order.len())
        } else {
            Box::new((0..at).rev())
        };
        let id = positions
            .map(|pos| self.order[pos])
            .find(|&other| !self.lines[other].dropped && !self.lines[other].placeholder)?;
        (!self.screen.contains(&id)).then_some(id)
    }

    /// Placeholder `id` showed line `seen`: it drops, and the placeholders between them move
    /// past `seen`, so the rows uncovered after it meet the lines after `seen`.
    fn resolve(&mut self, id: usize, seen: usize, up: bool) {
        self.lines[id].dropped = true;
        let (a, b) = (
            self.pos[id].min(self.pos[seen]),
            self.pos[id].max(self.pos[seen]),
        );
        let between: Vec<usize> = self.order[a + 1..b]
            .iter()
            .copied()
            .filter(|&other| self.lines[other].placeholder && !self.lines[other].dropped)
            .collect();
        if between.is_empty() {
            return;
        }
        self.order.retain(|other| !between.contains(other));
        let at = self
            .order
            .iter()
            .position(|&other| other == seen)
            .expect("seen is listed");
        let at = if up { at + 1 } else { at };
        self.order.splice(at..at, between);
        self.reindex(a);
    }

    /// The screen as the program finished drawing it.
    pub fn present(&mut self, rows: &[Row]) {
        if rows == self.rows {
            return;
        }
        // A program printing lines writes each into the row its last scroll uncovered, past
        // every line the history holds off screen: that row alone changed, and fills its
        // placeholder.
        let mut changed = (0..rows.len()).filter(|&j| rows[j] != self.rows[j]);
        if let (Some(j), None) = (changed.next(), changed.next()) {
            let id = self.screen[j];
            if self.lines[id].placeholder
                && !blank(&rows[j].text)
                && self.beside(id, true).is_none()
                && self.beside(id, false).is_none()
            {
                let line = &mut self.lines[id];
                line.placeholder = false;
                line.text.clone_from(&rows[j].text);
                line.wraps = rows[j].wraps;
                self.rows[j].clone_from(&rows[j]);
                self.limit();
                return;
            }
        }
        self.frame += 1;
        // Lines from before this frame.
        let known = self.added;
        let mut shown: Vec<Option<usize>> = vec![None; rows.len()];
        // Each changed region: the lines of the rows it replaces, and its rows.
        let mut regions: Vec<(Vec<usize>, Range<usize>)> = Vec::new();
        let mut fills: Vec<(usize, usize)> = Vec::new();
        // A row a scroll uncovered shows a line the screen did not show before, or one the
        // history holds elsewhere, never a line of another row: it aligns with nothing.
        let uncovered: Vec<bool> = self
            .screen
            .iter()
            .zip(rows)
            .map(|(&id, row)| self.lines[id].placeholder && !blank(&row.text))
            .collect();
        let old: Vec<&str> = self.rows.iter().map(|row| row.text.as_str()).collect();
        let new: Vec<&str> = rows
            .iter()
            .zip(&uncovered)
            .map(|(row, &uncovered)| match uncovered {
                true => UNALIGNED,
                false => row.text.as_str(),
            })
            .collect();
        let (mut i0, mut j0) = (0, 0);
        for (i, j) in align(&old, &new)
            .into_iter()
            .chain([(old.len(), new.len())])
        {
            if i > i0 || j > j0 {
                let gone = self.screen[i0..i].to_vec();
                self.update_region(&gone, rows, &uncovered, j0..j, &mut shown, &mut fills);
                regions.push((gone, j0..j));
            }
            if j < new.len() {
                shown[j] = Some(self.screen[i]);
            }
            (i0, j0) = (i + 1, j + 1);
        }
        self.reuse(rows, &mut shown);
        self.continue_lines(rows, &mut shown);
        self.fill(rows, &mut shown, &fills);
        for (gone, new) in &regions {
            self.reflow(rows, &shown, &uncovered, gone, new.clone());
        }
        for (gone, new) in regions {
            self.add(rows, &mut shown, &gone, new);
        }
        // A row blanked between two frames that shows its old line again held nothing.
        for (row, &id) in self.screen.iter().enumerate() {
            if blank(&self.rows[row].text)
                && blank(&self.lines[id].text)
                && !shown.contains(&Some(id))
                && shown[row].is_some_and(|now| self.lines[now].serial < known && now != id)
            {
                self.lines[id].dropped = true;
            }
        }
        let old_screen = std::mem::take(&mut self.screen);
        self.screen = shown
            .into_iter()
            .map(|id| id.expect("every row has a line"))
            .collect();
        for &id in &old_screen {
            if self.lines[id].placeholder && !self.screen.contains(&id) {
                self.lines[id].dropped = true;
            }
        }
        for (&id, row) in self.screen.iter().zip(rows) {
            let line = &mut self.lines[id];
            if line.text != row.text {
                line.text.clone_from(&row.text);
            }
            line.wraps = row.wraps;
            line.placeholder = false;
            line.first = line.first.min(self.frame);
            line.last = self.frame;
        }
        self.rows.clone_from_slice(rows);
        self.limit();
    }

    /// Rows `new` replace the lines `gone`: a row that updates a replaced line takes it, and a
    /// blank line a row fills waits in `fills` until rows that show other lines are placed.
    /// Numbers change in place only where every row with text updates a replaced line and few
    /// do so: a status area ticking, not a page of similar rows drawn over another. A row a
    /// scroll uncovered updates no line: it fills its placeholder.
    fn update_region(
        &mut self,
        gone: &[usize],
        rows: &[Row],
        uncovered: &[bool],
        new: Range<usize>,
        shown: &mut [Option<usize>],
        fills: &mut Vec<(usize, usize)>,
    ) {
        let old: Vec<&str> = gone
            .iter()
            .map(|&id| self.lines[id].text.as_str())
            .collect();
        let pairs = pair(&old, &rows[new.clone()], &uncovered[new.clone()]);
        for j in new.clone().filter(|&j| uncovered[j]) {
            if gone.contains(&self.screen[j]) {
                fills.push((self.screen[j], j));
            }
        }
        let numbers = pairs
            .iter()
            .filter(|(_, _, kind)| *kind == Update::Numbers)
            .count();
        let filled = rows[new.clone()]
            .iter()
            .filter(|row| !blank(&row.text))
            .count();
        let updated = pairs
            .iter()
            .filter(|&&(_, j, _)| !blank(&rows[new.start + j].text))
            .count();
        let numbers = if updated < filled {
            usize::MAX
        } else {
            numbers
        };
        for (k, j, kind) in pairs {
            match kind {
                // A blank row over a blank line holds nothing to place.
                Update::Fill if blank(&rows[new.start + j].text) => {
                    shown[new.start + j] = Some(gone[k]);
                }
                Update::Fill => fills.push((gone[k], new.start + j)),
                Update::Numbers if numbers > MAX_NUMBER_UPDATES => {}
                _ => shown[new.start + j] = Some(gone[k]),
            }
        }
    }

    /// Runs of rows without lines show stretches of the history again. A run's rows with
    /// text, from its first down or from its last up, take consecutive lines with text between
    /// the lines of the rows around the run: the stretch near one of those lines, or one
    /// anywhere between them that holds half the run and at least `STRETCH` rows, or the
    /// whole run. The longest match wins, and what it leaves of the run tries
    /// again beside it.
    fn reuse(&self, rows: &[Row], shown: &mut [Option<usize>]) {
        if shown.iter().all(Option::is_some) {
            return;
        }
        // The lines a row may show again, in order: their positions, ids, and texts.
        let mut positions = Vec::new();
        let mut ids = Vec::new();
        let mut texts = Vec::new();
        for (pos, &id) in self.order.iter().enumerate() {
            let line = &self.lines[id];
            if !line.dropped && !line.placeholder && !blank(&line.text) {
                positions.push(pos);
                ids.push(id);
                texts.push(line.text.as_str());
            }
        }
        while self.reuse_pass(rows, shown, &positions, &ids, &texts) {}
    }

    /// One `reuse` pass over the runs; whether it bound a row.
    fn reuse_pass(
        &self,
        rows: &[Row],
        shown: &mut [Option<usize>],
        positions: &[usize],
        ids: &[usize],
        texts: &[&str],
    ) -> bool {
        let mut bound = false;
        let mut j = 0;
        while j < rows.len() {
            if shown[j].is_some() {
                j += 1;
                continue;
            }
            let start = j;
            while j < rows.len() && shown[j].is_none() {
                j += 1;
            }
            let run: Vec<usize> = (start..j).filter(|&row| !blank(&rows[row].text)).collect();
            if run.is_empty() {
                continue;
            }
            let (lo, hi, above, below) = self.bounds(rows, shown, start, j);
            let span = positions.partition_point(|&pos| pos < lo)
                ..positions.partition_point(|&pos| pos < hi);
            let (window, texts) = (&ids[span.clone()], &texts[span]);
            let text = |k: usize| rows[run[k]].text.as_str();
            let free = |p: usize| !shown.contains(&Some(window[p]));
            // (rows matched, beside a neighbour, first window index, from the first row down)
            let mut best: Option<(usize, bool, usize, bool)> = None;
            let (first, last) = (text(0), text(run.len() - 1));
            for p in 0..window.len() {
                if texts[p] != first && texts[p] != last {
                    continue;
                }
                let down = (0..run.len().min(window.len() - p))
                    .take_while(|&k| texts[p + k] == text(k) && free(p + k))
                    .count();
                let up = (0..run.len().min(p + 1))
                    .take_while(|&k| texts[p - k] == text(run.len() - 1 - k) && free(p - k))
                    .count();
                for (len, first, from_top, beside) in [
                    (down, p, true, above && p < NEAR),
                    (
                        up,
                        p + 1 - up.max(1),
                        false,
                        below && p + NEAR >= window.len(),
                    ),
                ] {
                    let strong = len >= STRETCH.min(run.len()) && len * 2 >= run.len();
                    if len > 0
                        && (beside || strong)
                        && best.is_none_or(|(most, near, ..)| (len, beside) > (most, near))
                    {
                        best = Some((len, beside, first, from_top));
                    }
                }
            }
            if let Some((len, _, first, from_top)) = best {
                let rows_matched = match from_top {
                    true => &run[..len],
                    false => &run[run.len() - len..],
                };
                for (k, &row) in rows_matched.iter().enumerate() {
                    shown[row] = Some(window[first + k]);
                }
                bound = true;
            }
        }
        bound
    }

    /// A row that grew from a line near its neighbours, no longer shown and not scrolled away,
    /// continues that line, as a streamed reply redrawn below the row it started on.
    fn continue_lines(&mut self, rows: &[Row], shown: &mut [Option<usize>]) {
        for j in 0..rows.len() {
            if shown[j].is_some() || blank(&rows[j].text) {
                continue;
            }
            let (lo, hi, above, below) = self.bounds(rows, shown, j, j + 1);
            let row = core(&rows[j].text);
            let grows = |&id: &usize| {
                let line = core(&self.lines[id].text);
                !self.lines[id].scrolled
                    && line.chars().count() >= 3
                    && row.len() > line.len()
                    && row.starts_with(line)
            };
            let near = |range: Box<dyn Iterator<Item = usize>>| {
                range
                    .map(|pos| self.order[pos])
                    .filter(|&id| self.usable(id, shown))
                    .take(CONTINUE_REACH)
                    .find(grows)
            };
            let after = above.then(|| near(Box::new(lo..hi))).flatten();
            if let Some(id) =
                after.or_else(|| below.then(|| near(Box::new((lo..hi).rev()))).flatten())
            {
                shown[j] = Some(id);
            }
        }
    }

    /// A row filling the placeholder of the row a scroll uncovered takes it. A row filling a
    /// blank line takes it when the nearest row above or below that shows text shows the
    /// nearest line before or after it: the row continues what is around it, not a new page
    /// laid over an old one.
    fn fill(&mut self, rows: &[Row], shown: &mut [Option<usize>], fills: &[(usize, usize)]) {
        let mut waiting: Vec<(usize, usize)> = fills
            .iter()
            .copied()
            .filter(|&(_, row)| shown[row].is_none())
            .collect();
        // Top down, then bottom up, so filled rows anchor the rows after them.
        for upward in [false, true] {
            if upward {
                waiting.reverse();
            }
            let mut rest = Vec::new();
            for (id, row) in waiting {
                if shown.contains(&Some(id)) {
                    continue;
                }
                // A row a scroll uncovered is its placeholder's line.
                let anchored = self.lines[id].placeholder
                    || [false, true].into_iter().any(|after| {
                        let neighbour = self.filled_neighbour(rows, shown, row, after);
                        neighbour.is_some() && neighbour == self.text_beside(id, after)
                    });
                if anchored {
                    shown[row] = Some(id);
                    let line = &mut self.lines[id];
                    line.text.clone_from(&rows[row].text);
                    line.placeholder = false;
                } else {
                    rest.push((id, row));
                }
            }
            waiting = rest;
        }
    }

    /// The line of the nearest row above or below `row` that shows text.
    fn filled_neighbour(
        &self,
        rows: &[Row],
        shown: &[Option<usize>],
        row: usize,
        after: bool,
    ) -> Option<usize> {
        let range: Box<dyn Iterator<Item = usize>> = match after {
            true => Box::new(row + 1..rows.len()),
            false => Box::new((0..row).rev()),
        };
        range
            .filter(|&k| !blank(&rows[k].text))
            .find_map(|k| shown[k])
    }

    /// The nearest line before or after line `id` that holds text.
    fn text_beside(&self, id: usize, after: bool) -> Option<usize> {
        let at = self.pos[id];
        let range: Box<dyn Iterator<Item = usize>> = match after {
            true => Box::new(at + 1..self.order.len()),
            false => Box::new((0..at).rev()),
        };
        range.map(|pos| self.order[pos]).find(|&other| {
            let line = &self.lines[other];
            !line.dropped && !line.placeholder && !blank(&line.text)
        })
    }

    /// A replaced line whose words reappear together in the rows around its replacement was
    /// reflowed there, as text rewrapped when a reply completes; it drops.
    ///
    /// A replaced line whose text a row the scroll uncovered shows was redrawn there, as a
    /// status row a pager draws again below the rows it scrolled; it drops too.
    fn reflow(
        &mut self,
        rows: &[Row],
        shown: &[Option<usize>],
        uncovered: &[bool],
        gone: &[usize],
        new: Range<usize>,
    ) {
        for &id in gone {
            let line = &self.lines[id];
            if !shown.contains(&Some(id))
                && !blank(&line.text)
                && rows
                    .iter()
                    .zip(uncovered)
                    .any(|(row, &uncovered)| uncovered && row.text == line.text)
            {
                self.lines[id].dropped = true;
            }
        }
        if gone.is_empty() || new.is_empty() {
            return;
        }
        let context =
            new.start.saturating_sub(REFLOW_CONTEXT)..(new.end + REFLOW_CONTEXT).min(rows.len());
        let around: Vec<&str> = rows[context]
            .iter()
            .flat_map(|row| words(&row.text))
            .collect();
        for &id in gone {
            if shown.contains(&Some(id)) || self.lines[id].placeholder {
                continue;
            }
            let line = words(&self.lines[id].text);
            if line.len() >= REFLOW_WORDS && contains_run(&around, &line) {
                self.lines[id].dropped = true;
            }
        }
    }

    /// Rows `new` still without lines add them: after the rows above and the lines the region
    /// replaced, before the row below.
    fn add(
        &mut self,
        rows: &[Row],
        shown: &mut [Option<usize>],
        gone: &[usize],
        new: Range<usize>,
    ) {
        let after_gone = gone
            .iter()
            .filter(|&&id| {
                // A reflowed line still marks where the region's text was.
                !shown.contains(&Some(id)) && !self.lines[id].placeholder
            })
            .map(|&id| self.pos[id])
            .max();
        for j in new {
            if shown[j].is_some() {
                continue;
            }
            let prev = shown[..j].iter().flatten().map(|&id| self.pos[id]).max();
            // A blank row below a line takes the blank line after it, if no row shows that.
            if blank(&rows[j].text)
                && let Some(after) = prev.and_then(|pos| {
                    self.order[pos + 1..]
                        .iter()
                        .copied()
                        .find(|&id| !self.lines[id].dropped && !self.lines[id].placeholder)
                })
                && blank(&self.lines[after].text)
                && !shown.contains(&Some(after))
            {
                shown[j] = Some(after);
                continue;
            }
            let next = self
                .filled_neighbour(rows, shown, j, true)
                .map(|id| self.pos[id]);
            let floor = prev.map_or(0, |pos| pos + 1);
            let mut at = match prev.max(after_gone) {
                Some(pos) => pos + 1,
                None => next.unwrap_or(self.order.len()),
            };
            if let Some(next) = next.filter(|&next| next >= floor) {
                at = at.min(next);
            }
            shown[j] = Some(self.insert(at, &rows[j].text, false));
        }
    }

    /// Where rows `start..end` may find their lines again: between the lines of the nearest
    /// rows above and below that show text, and whether each of those rows exists.
    fn bounds(
        &self,
        rows: &[Row],
        shown: &[Option<usize>],
        start: usize,
        end: usize,
    ) -> (usize, usize, bool, bool) {
        let above = self.filled_neighbour(rows, shown, start, false);
        let below = self.filled_neighbour(rows, shown, end - 1, true);
        let lo = above.map_or(0, |id| self.pos[id] + 1);
        let hi = below.map_or(self.order.len(), |id| self.pos[id]);
        (lo, hi.max(lo), above.is_some(), below.is_some())
    }

    /// A line a row may show again: in the history and not shown by another row.
    fn usable(&self, id: usize, shown: &[Option<usize>]) -> bool {
        let line = &self.lines[id];
        !line.dropped && !line.placeholder && !shown.contains(&Some(id))
    }

    fn insert(&mut self, at: usize, text: &str, placeholder: bool) -> usize {
        let line = Line {
            text: text.to_string(),
            wraps: false,
            dropped: false,
            placeholder,
            first: usize::MAX,
            last: 0,
            scrolled: false,
            serial: self.added,
        };
        self.added += 1;
        let id = match self.free.pop() {
            Some(id) => {
                self.lines[id] = line;
                id
            }
            None => {
                self.lines.push(line);
                self.pos.push(0);
                self.lines.len() - 1
            }
        };
        self.order.insert(at, id);
        self.reindex(at);
        id
    }

    fn reindex(&mut self, from: usize) {
        for (pos, &id) in self.order.iter().enumerate().skip(from) {
            self.pos[id] = pos;
        }
    }

    /// Past `limit` lines, drops the oldest lines the screen no longer shows.
    /// Past the limit, drops the oldest lines the screen no longer shows, and frees the ids of
    /// lines out of the history.
    fn limit(&mut self) {
        // Trimming in batches keeps the reindexing rare.
        if self.order.len() <= self.limit + self.limit / 8 {
            return;
        }
        let on_screen: HashSet<usize> = self.screen.iter().copied().collect();
        let lines = &mut self.lines;
        let kept = self.order.iter().filter(|&&id| !lines[id].dropped).count();
        let over = kept.saturating_sub(self.limit);
        let mut excess = over;
        let free = &mut self.free;
        self.order.retain(|&id| {
            if on_screen.contains(&id) {
                return true;
            }
            let oldest = excess > 0 && !lines[id].dropped;
            if oldest {
                excess -= 1;
            }
            if oldest || lines[id].dropped {
                lines[id].text = String::new();
                free.push(id);
                return false;
            }
            true
        });
        self.truncated |= over > 0;
        self.reindex(0);
    }

    /// The history in order: rows the terminal wrapped joined into one line, runs of blank
    /// lines collapsed to one, without blank lines at the start or end.
    pub fn text(&self) -> Vec<String> {
        let echoes = self.echoes();
        let mut out: Vec<String> = Vec::new();
        let mut joined = false;
        for &id in &self.order {
            let line = &self.lines[id];
            if line.dropped || line.placeholder || echoes.contains(&id) {
                continue;
            }
            if joined && let Some(last) = out.last_mut() {
                last.push_str(&line.text);
            } else if !blank(&line.text) || out.last().is_some_and(|last| !blank(last)) {
                out.push(line.text.clone());
            }
            joined = line.wraps;
        }
        while out.last().is_some_and(|last| blank(last)) {
            out.pop();
        }
        out
    }

    /// Lines no longer shown, and not scrolled away, that a longer line near them continues,
    /// where frames never showed the two together: a reply redrawn while it streams leaves its
    /// shorter copies behind.
    fn echoes(&self) -> HashSet<usize> {
        let kept: Vec<usize> = self
            .order
            .iter()
            .copied()
            .filter(|&id| {
                let line = &self.lines[id];
                !line.dropped && !line.placeholder && !blank(&line.text)
            })
            .collect();
        let mut echoes = HashSet::new();
        for (k, &id) in kept.iter().enumerate() {
            let line = &self.lines[id];
            let text = core(&line.text);
            if self.screen.contains(&id)
                || line.first == usize::MAX
                || line.scrolled
                || text.chars().count() < 3
            {
                continue;
            }
            let near = &kept[k.saturating_sub(ECHO_REACH)..(k + ECHO_REACH + 1).min(kept.len())];
            if near.iter().any(|&other| {
                let longer = &self.lines[other];
                let apart = line.last < longer.first || longer.last < line.first;
                let continues = core(&longer.text);
                longer.first != usize::MAX
                    && apart
                    && continues.len() > text.len()
                    && continues.starts_with(text)
            }) {
                echoes.insert(id);
            }
        }
        echoes
    }
}

/// A text no row has: the terminal never puts NUL in a cell.
const UNALIGNED: &str = "\0";

/// The order-preserving pairs of equal rows of `old` and `new` that moved together. As in
/// patience diff, rows unique to both frames anchor, the longest order-preserving set of them,
/// and the regions between anchors are aligned again by the rows unique within them. Each
/// anchor, and the top and bottom of the screen as unmoved anchors, extends over equal
/// neighbours at its own displacement, so a repeated row binds only through such a run.
fn align(old: &[&str], new: &[&str]) -> Vec<(usize, usize)> {
    // Rows unchanged at the top and bottom, most of a frame, need no hashing.
    let top = old.iter().zip(new).take_while(|(a, b)| a == b).count();
    let bottom = old[top..]
        .iter()
        .rev()
        .zip(new[top..].iter().rev())
        .take_while(|(a, b)| a == b)
        .count();
    let mut out: Vec<(usize, usize)> = (0..top).map(|k| (k, k)).collect();
    align_box(
        old,
        new,
        top..old.len() - bottom,
        top..new.len() - bottom,
        &mut out,
    );
    out.extend((1..=bottom).rev().map(|k| (old.len() - k, new.len() - k)));
    out
}

fn align_box(
    old: &[&str],
    new: &[&str],
    a: Range<usize>,
    b: Range<usize>,
    out: &mut Vec<(usize, usize)>,
) {
    use std::collections::HashMap;
    if a.is_empty() || b.is_empty() {
        return;
    }
    // Per text: count in `a`, count in `b`, and the last index in each.
    let mut seen: HashMap<&str, (u32, u32, usize, usize)> = HashMap::new();
    for i in a.clone() {
        let entry = seen.entry(old[i]).or_insert((0, 0, i, 0));
        entry.0 += 1;
    }
    for j in b.clone() {
        if let Some(entry) = seen.get_mut(new[j]) {
            entry.1 += 1;
            entry.3 = j;
        }
    }
    let mut unique: Vec<(usize, usize)> = seen
        .iter()
        .filter(|(text, entry)| entry.0 == 1 && entry.1 == 1 && !blank(text))
        .map(|(_, entry)| (entry.2, entry.3))
        .collect();
    unique.sort_unstable_by_key(|&(_, j)| j);
    let mut bounds: Vec<(isize, isize)> = vec![(a.start as isize - 1, b.start as isize - 1)];
    bounds.extend(
        lis(&unique)
            .into_iter()
            .map(|(i, j)| (i as isize, j as isize)),
    );
    bounds.push((a.end as isize, b.end as isize));
    for pair in bounds.windows(2) {
        let ((pi, pj), (ni, nj)) = (pair[0], pair[1]);
        // The upper bound extends down, the lower bound up, each at its displacement.
        let (mut i, mut j) = (pi + 1, pj + 1);
        while i < ni && j < nj && old[i as usize] == new[j as usize] {
            out.push((i as usize, j as usize));
            i += 1;
            j += 1;
        }
        let (mut k, mut l) = (ni - 1, nj - 1);
        let mut up = Vec::new();
        while k >= i && l >= j && old[k as usize] == new[l as usize] {
            up.push((k as usize, l as usize));
            k -= 1;
            l -= 1;
        }
        let inner_a = i as usize..(k + 1) as usize;
        let inner_b = j as usize..(l + 1) as usize;
        if inner_a.len() < a.len() || inner_b.len() < b.len() {
            align_box(old, new, inner_a, inner_b, out);
        }
        out.extend(up.into_iter().rev());
        if (ni as usize) < a.end {
            out.push((ni as usize, nj as usize));
        }
    }
}

/// The longest subsequence of `pairs`, which are sorted by their second index, increasing in
/// the first.
fn lis(pairs: &[(usize, usize)]) -> Vec<(usize, usize)> {
    let mut tails: Vec<usize> = Vec::new();
    let mut prev = vec![usize::MAX; pairs.len()];
    for (k, &(i, _)) in pairs.iter().enumerate() {
        let at = tails.partition_point(|&t| pairs[t].0 < i);
        if at > 0 {
            prev[k] = tails[at - 1];
        }
        if at == tails.len() {
            tails.push(k);
        } else {
            tails[at] = k;
        }
    }
    let mut out = Vec::new();
    let mut k = tails.last().copied().unwrap_or(usize::MAX);
    while k != usize::MAX {
        out.push(pairs[k]);
        k = prev[k];
    }
    out.reverse();
    out
}

/// The order-preserving pairing of lines `old` and rows `new`, but for rows `skip` marks,
/// with the greatest total weight, as (old index, new index, kind).
fn pair(old: &[&str], new: &[Row], skip: &[bool]) -> Vec<(usize, usize, Update)> {
    let (m, n) = (old.len(), new.len());
    let new: Vec<Key> = new.iter().map(|row| Key::new(&row.text)).collect();
    let kinds: Vec<Vec<Option<Update>>> = old
        .iter()
        .map(|line| {
            let line = Key::new(line);
            new.iter()
                .zip(skip)
                .map(|(row, &skip)| (!skip).then(|| line.update(row)).flatten())
                .collect()
        })
        .collect();
    let weight = |kind: Option<Update>| match kind {
        Some(Update::Extend) => 3,
        Some(Update::Numbers) => 2,
        Some(Update::Fill) => 1,
        None => 0,
    };
    let mut best = vec![vec![0_u32; n + 1]; m + 1];
    for i in (0..m).rev() {
        for j in (0..n).rev() {
            let w = weight(kinds[i][j]);
            let take = if w > 0 { w + best[i + 1][j + 1] } else { 0 };
            best[i][j] = take.max(best[i + 1][j]).max(best[i][j + 1]);
        }
    }
    let (mut i, mut j, mut pairs) = (0, 0, Vec::new());
    while i < m && j < n {
        let w = weight(kinds[i][j]);
        if w > 0 && best[i][j] == w + best[i + 1][j + 1] {
            pairs.push((i, j, kinds[i][j].expect("weighted")));
            i += 1;
            j += 1;
        } else if best[i][j] == best[i + 1][j] {
            i += 1;
        } else {
            j += 1;
        }
    }
    pairs
}

/// How row text `new` updates line text `old` in place, if it does. Equal texts are not an
/// update: a row equal to a line elsewhere shows that line, which `align` and `reuse` find.
fn update(old: &str, new: &str) -> Option<Update> {
    Key::new(old).update(&Key::new(new))
}

/// What `update` compares of a text, computed once per text.
struct Key<'a> {
    text: &'a str,
    blank: bool,
    core: &'a str,
    len: usize,
    shape: String,
}

impl<'a> Key<'a> {
    fn new(text: &'a str) -> Key<'a> {
        let core = core(text);
        Key {
            text,
            blank: blank(text),
            core,
            len: core.chars().count(),
            shape: shape(text),
        }
    }

    /// How `new` updates this text in place, if it does.
    fn update(&self, new: &Key) -> Option<Update> {
        if self.blank {
            return Some(Update::Fill);
        }
        if new.blank || self.text == new.text {
            return None;
        }
        let grown = self.len >= 3 && new.core.starts_with(self.core);
        // Backspacing drops a little; a short prefix of a long row is a different row.
        let trimmed =
            new.len >= 3 && self.core.starts_with(new.core) && new.len * 5 >= self.len * 4;
        if grown || trimmed {
            return Some(Update::Extend);
        }
        let shape = &self.shape;
        (*shape == new.shape
            && (shape.is_empty() || shape.chars().any(|c| c == '#' || c.is_alphabetic())))
        .then_some(Update::Numbers)
    }
}

fn blank(text: &str) -> bool {
    text.trim().is_empty()
}

fn words(text: &str) -> Vec<&str> {
    text.split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|word| !word.is_empty())
        .collect()
}

/// `part` occurs as one run within `whole`; its last word may be cut short, as a reply still
/// streaming leaves it.
fn contains_run(whole: &[&str], part: &[&str]) -> bool {
    let last = part.len() - 1;
    whole.windows(part.len()).any(|window| {
        window
            .iter()
            .zip(part)
            .enumerate()
            .all(|(k, (w, p))| w == p || (k == last && w.starts_with(p)))
    })
}

/// A row without a trailing cursor glyph, border, or padding.
fn core(row: &str) -> &str {
    row.trim_end_matches(|c: char| !c.is_alphanumeric() && !c.is_ascii_punctuation())
}

/// A row with each run of digits as `#` and of spaces as one space, without a leading
/// one-character symbol and symbols outside ASCII, such as spinner glyphs and borders: a
/// counter, timer, or spinner keeps its shape, and operators and punctuation still tell rows
/// apart.
fn shape(row: &str) -> String {
    let row = row.trim_start();
    let row = match row.split_once(char::is_whitespace) {
        Some((first, rest))
            if first.chars().count() == 1 && !first.chars().all(char::is_alphanumeric) =>
        {
            rest
        }
        _ => row,
    };
    let mut shape = String::new();
    for c in row.chars() {
        if c.is_ascii_digit() {
            if !shape.ends_with('#') {
                shape.push('#');
            }
        } else if c.is_whitespace() {
            if !shape.ends_with(' ') {
                shape.push(' ');
            }
        } else if c.is_alphanumeric() || c.is_ascii_punctuation() {
            shape.push(c);
        }
    }
    shape.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(rows: &[&str]) -> Vec<Row> {
        rows.iter()
            .map(|text| Row {
                text: text.to_string(),
                wraps: false,
            })
            .collect()
    }

    /// The history of `frames`, each shown in turn on a screen as tall as the first.
    fn merge(frames: &[Vec<Row>]) -> Vec<String> {
        let mut history = History::new(frames[0].len(), 10_000);
        for rows in frames {
            history.present(rows);
        }
        history.text()
    }

    fn lines(lines: &[&str]) -> Vec<String> {
        lines.iter().map(|line| line.to_string()).collect()
    }

    #[test]
    fn frames_merge_into_one_history() {
        // A pager over code whose `}` lines repeat, redrawn in full: down a row at a time,
        // back up, down again.
        let code: Vec<String> = (0..6)
            .flat_map(|i| {
                [
                    format!("fn f{i}() {{"),
                    format!("    body {i}"),
                    "}".to_string(),
                ]
            })
            .collect();
        let window = |top: usize| {
            let rows: Vec<&str> = code[top..top + 4].iter().map(String::as_str).collect();
            frame(&rows)
        };
        let pager: Vec<Vec<Row>> = (0..=14)
            .chain((0..14).rev())
            .chain(0..=14)
            .map(window)
            .collect();
        let tab_a = frame(&["Tabs", "alpha 1 red", "alpha 2 green", "status"]);
        let tab_b = frame(&["Tabs", "beta one", "beta two", "status"]);
        let table = |pids: [u32; 4]| {
            let rows: Vec<String> = pids
                .iter()
                .map(|pid| format!("PID {pid} running"))
                .collect();
            let rows: Vec<&str> = rows.iter().map(String::as_str).collect();
            frame(&rows)
        };
        let cases = vec![
            (
                "rows updated in place keep their last text",
                vec![
                    frame(&[
                        "Monitor",
                        "> Analy",
                        "Downloading 17%",
                        "⠋ Working (1s)",
                        "? fix the bugs",
                    ]),
                    frame(&[
                        "Monitor",
                        "> Analyzing",
                        "Downloading 23%",
                        "⠙ Working (1s)",
                        "? fix the bugs",
                    ]),
                    frame(&[
                        "Monitor",
                        "> Analyzing files",
                        "Downloading 100%",
                        "⠹ Working (2s)",
                        "? fix the bug",
                    ]),
                ],
                lines(&[
                    "Monitor",
                    "> Analyzing files",
                    "Downloading 100%",
                    "⠹ Working (2s)",
                    "? fix the bug",
                ]),
            ),
            (
                "rows written into blank rows leave no blank lines between them",
                vec![
                    frame(&["log", "", "", "", ""]),
                    frame(&["log", "one", "two", "", ""]),
                    frame(&["log", "one", "two", "three", "four"]),
                ],
                lines(&["log", "one", "two", "three", "four"]),
            ),
            (
                "scrolling back and forth shows each line once, in order",
                pager,
                code.clone(),
            ),
            (
                "a page shown again reuses its lines",
                vec![tab_a.clone(), tab_b.clone(), tab_a, tab_b],
                lines(&[
                    "Tabs",
                    "alpha 1 red",
                    "alpha 2 green",
                    "beta one",
                    "beta two",
                    "status",
                ]),
            ),
            (
                "a row reflowed into the rows around it is dropped",
                vec![
                    frame(&[
                        "Items",
                        "end_sync - applies the held output and returns any resulting",
                        "replies. [Code:127]",
                        "done",
                    ]),
                    frame(&[
                        "Items",
                        "end_sync - applies the held output and returns any",
                        "resulting replies. Code:127 (src/terminal.rs:127)",
                        "done",
                    ]),
                ],
                lines(&[
                    "Items",
                    "end_sync - applies the held output and returns any",
                    "resulting replies. Code:127 (src/terminal.rs:127)",
                    "done",
                ]),
            ),
            (
                "a row whose words reappear apart is not a reflow",
                vec![
                    frame(&["Errors", "failed to open file", "end"]),
                    frame(&["Errors", "failed after trying to open another file", "end"]),
                ],
                lines(&[
                    "Errors",
                    "failed to open file",
                    "failed after trying to open another file",
                    "end",
                ]),
            ),
            (
                "a new page follows the old one instead of filling its blank rows",
                vec![
                    frame(&["", "", "LOGO", "", "> ask anything"]),
                    frame(&["session", "you: hello", "bot: hi there", "", ""]),
                ],
                lines(&[
                    "LOGO",
                    "",
                    "> ask anything",
                    "session",
                    "you: hello",
                    "bot: hi there",
                ]),
            ),
            (
                "a row erased in place stays in the history",
                vec![frame(&["title", "error: disk full"]), frame(&["title", ""])],
                lines(&["title", "error: disk full"]),
            ),
            (
                "a row shown again near its line reuses it; lines never move",
                vec![
                    frame(&["title", "saved", "counter 1", "ending"]),
                    frame(&["title", "counter 1", "other", "ending"]),
                    frame(&["title", "counter 2", "saved", "ending"]),
                ],
                lines(&[
                    "title",
                    "counter 2",
                    "saved",
                    "counter 1",
                    "other",
                    "ending",
                ]),
            ),
            (
                "operators and punctuation tell rows apart",
                vec![frame(&["a", "x == y", ":"]), frame(&["a", "x != y", "}"])],
                lines(&["a", "x == y", ":", "x != y", "}"]),
            ),
            (
                "a page of similar rows drawn over another keeps both",
                vec![table([11, 12, 13, 14]), table([21, 22, 23, 24])],
                [11, 12, 13, 14, 21, 22, 23, 24]
                    .iter()
                    .map(|pid| format!("PID {pid} running"))
                    .collect(),
            ),
            (
                "a streamed row redrawn below continues its line",
                vec![
                    frame(&["reply", "L003 three", "", ""]),
                    frame(&["reply", "", "", ""]),
                    frame(&["reply", "L003 three three", "L004", ""]),
                ],
                lines(&["reply", "L003 three three", "L004"]),
            ),
        ];
        for (name, frames, expected) in cases {
            assert_eq!(merge(&frames), expected, "{name}");
        }
    }

    #[test]
    fn scrolled_rows_keep_their_lines() {
        let row = |text: &str| frame(&[text]);
        // Printing lines: each leaves with the text it had, with no frame shown.
        let mut history = History::new(3, 100);
        for k in 0..6 {
            history.shift(0, 3, 1, true, Some(&row(&format!("line {k}"))));
        }
        let expected: Vec<String> = (0..6).map(|k| format!("line {k}")).collect();
        assert_eq!(history.text(), expected);
        // A pager scrolls down two rows and back up, showing only some frames: the rows it
        // uncovers going back show the lines that left, which they reuse.
        let mut history = History::new(3, 100);
        history.present(&frame(&["a", "b", "c"]));
        history.shift(0, 3, 1, true, Some(&row("a")));
        history.present(&frame(&["b", "c", "d"]));
        history.shift(0, 3, 1, true, Some(&row("b")));
        history.shift(0, 3, 1, false, Some(&row("e")));
        history.shift(0, 3, 1, false, Some(&row("d")));
        history.present(&frame(&["a", "b", "c"]));
        assert_eq!(history.text(), lines(&["a", "b", "c", "d", "e"]));
        // A line that scrolled away stays, whatever text a later line starts with.
        let mut history = History::new(3, 100);
        history.present(&frame(&["cat", "middle", "tail"]));
        history.shift(0, 3, 1, true, Some(&row("cat")));
        history.present(&frame(&["middle", "tail!", "catalog"]));
        assert_eq!(
            history.text(),
            lines(&["cat", "middle", "tail!", "catalog"])
        );
        let mut history = History::new(3, 100);
        history.present(&frame(&["cat", "middle", "tail"]));
        history.shift(0, 3, 1, true, Some(&row("cat")));
        history.present(&frame(&["catalog", "middle", "tail"]));
        assert!(
            history.text().contains(&"cat".to_string()),
            "{:?}",
            history.text()
        );
        // A row a scroll uncovered is a new line, even when only its numbers differ from a
        // line the frame no longer shows.
        let mut history = History::new(2, 100);
        history.present(&frame(&["", "item 1"]));
        history.shift(0, 2, 1, true, Some(&row("")));
        history.present(&frame(&["", "item 2"]));
        assert_eq!(history.text(), lines(&["item 1", "", "item 2"]));
        // A line added over a reused id is still new: the blank row before it stays and ends
        // the wrapped row above.
        let mut history = History::new(3, 16);
        for _ in 0..20 {
            history.shift(0, 3, 1, true, Some(&row("")));
        }
        let mut rows = frame(&["", &"A".repeat(120), ""]);
        rows[1].wraps = true;
        history.present(&rows);
        history.present(&frame(&["", "", ""]));
        history.present(&frame(&["", "entry", ""]));
        assert_eq!(history.text(), ["A".repeat(120), "entry".to_string()]);
        // A blank screen scrolling for long holds no more than its limit.
        let mut history = History::new(30, 100);
        for up in [true, false] {
            for _ in 0..10_000 {
                history.shift(0, 30, 1, up, Some(&row("")));
            }
        }
        assert!(
            history.lines.len() <= 100 + 100 / 8 + 31,
            "{}",
            history.lines.len()
        );
        // Rows the terminal wrapped join into one line.
        let mut history = History::new(2, 100);
        let mut rows = frame(&["abc", "def"]);
        rows[0].wraps = true;
        history.present(&rows);
        assert_eq!(history.text(), lines(&["abcdef"]));
    }

    #[test]
    fn only_shown_lines_count_toward_the_limit() {
        // Each switch reflows a row away, which leaves the history no longer.
        let mut history = History::new(3, 16);
        history.present(&frame(&["header", "important archive", ""]));
        for _ in 0..200 {
            history.present(&frame(&["header", "one two three", "four five six seven"]));
            history.present(&frame(&["header", "one two three four", "five six seven"]));
        }
        let text = history.text();
        assert!(text.contains(&"important archive".to_string()), "{text:?}");
        assert!(!history.truncated);
        // Past the limit, the oldest lines leave and the screen's lines stay.
        let log: Vec<String> = (0..40_u8)
            .map(|k| format!("log {}{}", (b'a' + k / 10) as char, (b'a' + k % 10) as char))
            .collect();
        for line in &log {
            history.present(&frame(&["header", line, ""]));
        }
        let text = history.text();
        assert!(history.truncated && text.len() <= 16 + 16 / 8, "{text:?}");
        assert_eq!(text.first().map(String::as_str), Some("header"));
        assert!(text.ends_with(&log[log.len() - 8..]), "{text:?}");
    }
}
