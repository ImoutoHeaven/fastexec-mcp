//! Merges the frames an alternate-screen program showed into one history.
//!
//! The screen is a window onto a document that grows as the program runs. Each frame is
//! aligned with the frame before it: a row still shown keeps its line, a row rewritten in place
//! (a growing reply, a progress counter, a spinner) updates its line, a row re-rendered
//! elsewhere (reflowed text) is dropped because its words remain on screen, and a row shown
//! again (scrolling back, switching back to a page) reuses its line. Every other row that
//! leaves the screen stays in the history: a duplicate is preferable to a lost line.

use similar::{Algorithm, DiffOp, capture_diff_slices};
use std::collections::HashSet;
use std::ops::Range;

/// Rows around a rewritten region whose words can absorb a re-rendered row.
const REFLOW_CONTEXT: usize = 2;

/// The old rows a region of change replaces, its new rows, and the pairs of them rewritten
/// in place.
type Change = (Range<usize>, Range<usize>, Vec<(usize, usize)>);

struct Line {
    text: String,
    /// Re-rendered elsewhere; the history leaves it out.
    dropped: bool,
}

#[derive(Default)]
pub struct History {
    lines: Vec<Line>,
    /// Line ids in document order, and each line's index in it.
    order: Vec<usize>,
    pos: Vec<usize>,
    /// The line each row of the last frame shows.
    screen: Vec<usize>,
    rows: Vec<String>,
    /// Lines that left the history once it held `limit` lines.
    pub truncated: bool,
}

impl History {
    /// Adds the next frame: every row of the screen, trailing spaces trimmed.
    pub fn show(&mut self, rows: &[String], limit: usize) {
        if rows == self.rows {
            return;
        }
        if self.order.is_empty() {
            self.screen = rows
                .iter()
                .map(|row| self.insert(self.order.len(), row))
                .collect();
            self.rows = rows.to_vec();
            return;
        }
        let old = std::mem::take(&mut self.rows);
        let ids = std::mem::take(&mut self.screen);
        // Scrolling by one row, the common frame, needs no alignment.
        if let Some((mut shown, uncovered)) = scrolled(&old, rows, &ids) {
            self.rewrite(rows, &mut shown, &[], uncovered..uncovered + 1, &[], None);
            self.finish(rows, shown, limit);
            return;
        }
        let mut shown: Vec<Option<usize>> = vec![None; rows.len()];
        // Each change: the old rows it replaces, the new rows, and the pairs of them that are
        // rewritten in place.
        let mut changes: Vec<Change> = Vec::new();
        for op in capture_diff_slices(Algorithm::Patience, &old, rows) {
            match op {
                DiffOp::Equal {
                    old_index,
                    new_index,
                    len,
                } => {
                    for k in 0..len {
                        shown[new_index + k] = Some(ids[old_index + k]);
                    }
                }
                DiffOp::Insert {
                    old_index,
                    new_index,
                    new_len,
                } => changes.push((old_index..old_index, new_index..new_index + new_len, vec![])),
                DiffOp::Replace {
                    old_index,
                    old_len,
                    new_index,
                    new_len,
                } => {
                    let gone = old_index..old_index + old_len;
                    let new = new_index..new_index + new_len;
                    let pairs = pair(&old[gone.clone()], &rows[new.clone()]);
                    changes.push((gone, new, pairs));
                }
                DiffOp::Delete { .. } => {}
            }
        }
        // A frame that keeps or rewrites in place little of the last one is a new page. Rows it
        // keeps may match by coincidence and blank rows of the old layout would pin its content
        // among the old lines, so the whole page is matched against the history and follows
        // the old page.
        let rewritten: usize = changes
            .iter()
            .map(|(gone, new, pairs)| {
                pairs
                    .iter()
                    .filter(|&&(i, j)| !blank(&old[gone.start + i]) && !blank(&rows[new.start + j]))
                    .count()
            })
            .sum();
        let kept = rewritten
            + (0..rows.len())
                .filter(|&j| shown[j].is_some() && !blank(&rows[j]))
                .count();
        let filled = rows.iter().filter(|row| !blank(row)).count();
        let mut floor = None;
        if kept * 4 < filled {
            shown.fill(None);
            changes = vec![(0..0, 0..rows.len(), vec![])];
            floor = ids.iter().copied().max_by_key(|&id| self.pos[id]);
        }
        for (gone, new, pairs) in changes {
            self.rewrite(rows, &mut shown, &ids[gone], new, &pairs, floor);
        }
        self.finish(rows, shown, limit);
    }

    fn finish(&mut self, rows: &[String], shown: Vec<Option<usize>>, limit: usize) {
        self.screen = shown
            .into_iter()
            .map(|id| id.expect("every row has a line"))
            .collect();
        self.rows = rows.to_vec();
        self.limit(limit);
    }

    /// Places the rows `new` of `rows`, which replace the lines `gone` of the last frame and
    /// rewrite the `pairs` of them in place.
    fn rewrite(
        &mut self,
        rows: &[String],
        shown: &mut [Option<usize>],
        gone: &[usize],
        new: Range<usize>,
        pairs: &[(usize, usize)],
        floor: Option<usize>,
    ) {
        // A row rewritten in place updates its line.
        for &(k, j) in pairs {
            self.lines[gone[k]].text.clone_from(&rows[new.start + j]);
            shown[new.start + j] = Some(gone[k]);
        }
        // A row shown again reuses its line: the nearest copy between the lines of the rows
        // around it, walking away from the side the region is anchored on, so a repeated line
        // such as `}` binds to the copy beside its neighbours.
        let upward = shown[..new.start].iter().all(Option::is_none)
            && shown[new.end..].iter().any(Option::is_some);
        let mut free: Vec<usize> = new
            .clone()
            .filter(|&j| shown[j].is_none() && !blank(&rows[j]))
            .collect();
        if upward {
            free.reverse();
        }
        for j in free {
            let lo = self.neighbour(shown, j, false).map_or(0, |pos| pos + 1);
            let hi = self
                .neighbour(shown, j, true)
                .unwrap_or(self.order.len())
                .max(lo);
            let found = |pos: &usize| {
                let id = self.order[*pos];
                !self.lines[id].dropped
                    && self.lines[id].text == rows[j]
                    && !shown.contains(&Some(id))
            };
            let pos = if upward {
                (lo..hi).rev().find(found)
            } else {
                (lo..hi).find(found)
            };
            if let Some(pos) = pos {
                shown[j] = Some(self.order[pos]);
            }
        }
        // A replaced row whose words all reappear, in order, in the rows around its
        // replacement was re-rendered there, such as text reflowed when a reply completes.
        let context =
            new.start.saturating_sub(REFLOW_CONTEXT)..(new.end + REFLOW_CONTEXT).min(rows.len());
        let around: Vec<&str> = match gone.is_empty() {
            true => Vec::new(),
            false => rows[context].iter().flat_map(|row| words(row)).collect(),
        };
        for &id in gone.iter().filter(|&&id| !shown.contains(&Some(id))) {
            let line = words(&self.lines[id].text);
            if line.len() >= 2 && in_order(&line, &around) {
                self.lines[id].dropped = true;
            }
        }
        // New lines follow the row before them and the old lines they replace, and precede
        // the row after them.
        let after_gone = gone
            .iter()
            .filter(|&&id| !pairs.iter().any(|&(k, _)| gone[k] == id))
            .map(|&id| self.pos[id])
            .chain(floor.map(|id| self.pos[id]))
            .max();
        for j in new {
            if shown[j].is_some() {
                continue;
            }
            let prev = self.neighbour(shown, j, false);
            let next = self.neighbour(shown, j, true);
            let at = match prev.max(after_gone) {
                Some(pos) => pos + 1,
                None => next.unwrap_or(self.order.len()),
            };
            shown[j] = Some(self.insert(next.map_or(at, |next| at.min(next)), &rows[j]));
        }
    }

    /// The position of the line shown by the nearest row before or after row `j` that has one.
    fn neighbour(&self, shown: &[Option<usize>], j: usize, after: bool) -> Option<usize> {
        let id = if after {
            shown[j + 1..].iter().flatten().next()
        } else {
            shown[..j].iter().rev().flatten().next()
        };
        id.map(|&id| self.pos[id])
    }

    fn insert(&mut self, at: usize, text: &str) -> usize {
        let id = self.lines.len();
        self.lines.push(Line {
            text: text.to_string(),
            dropped: false,
        });
        self.pos.push(0);
        self.order.insert(at, id);
        for (pos, &id) in self.order.iter().enumerate().skip(at) {
            self.pos[id] = pos;
        }
        id
    }

    /// Past `limit` lines, drops the oldest lines the screen no longer shows.
    fn limit(&mut self, limit: usize) {
        // Trimming in batches keeps the reindexing rare.
        if self.order.len() <= limit + limit / 8 {
            return;
        }
        let on_screen: HashSet<usize> = self.screen.iter().copied().collect();
        let lines = &mut self.lines;
        let kept = self.order.iter().filter(|&&id| !lines[id].dropped).count();
        let over = kept.saturating_sub(limit);
        let mut excess = over;
        self.order.retain(|&id| {
            let oldest = excess > 0 && !on_screen.contains(&id);
            if oldest {
                excess -= 1;
            }
            if oldest || lines[id].dropped {
                lines[id] = Line {
                    text: String::new(),
                    dropped: true,
                };
                return false;
            }
            true
        });
        self.truncated |= over > 0;
        for (pos, &id) in self.order.iter().enumerate() {
            self.pos[id] = pos;
        }
    }

    /// The history in order, runs of blank lines collapsed to one, without blank lines at the
    /// start or end.
    pub fn text(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for &id in &self.order {
            let line = &self.lines[id];
            if line.dropped || (blank(&line.text) && out.last().is_none_or(|last| blank(last))) {
                continue;
            }
            out.push(line.text.clone());
        }
        while out.last().is_some_and(|last| blank(last)) {
            out.pop();
        }
        out
    }
}

/// The lines `new` shows when it is `old` scrolled by one row within a band of two rows or
/// more, and the row the scroll uncovered.
fn scrolled(old: &[String], new: &[String], ids: &[usize]) -> Option<(Vec<Option<usize>>, usize)> {
    if old.len() != new.len() {
        return None;
    }
    let first = (0..new.len()).find(|&i| old[i] != new[i])?;
    let last = (0..new.len()).rfind(|&i| old[i] != new[i])?;
    if first == last {
        return None;
    }
    let mut shown: Vec<Option<usize>> = ids.iter().copied().map(Some).collect();
    let uncovered = if new[first..last] == old[first + 1..=last] {
        shown[first..=last].rotate_left(1);
        last
    } else if new[first + 1..=last] == old[first..last] {
        shown[first..=last].rotate_right(1);
        first
    } else {
        return None;
    };
    shown[uncovered] = None;
    Some((shown, uncovered))
}

fn blank(row: &str) -> bool {
    row.trim().is_empty()
}

fn words(text: &str) -> Vec<&str> {
    text.split(|c: char| !c.is_alphanumeric() && c != '_')
        .filter(|word| !word.is_empty())
        .collect()
}

/// `part` occurs in order within `whole`; its last word may be cut short, as a reply still
/// streaming leaves it.
fn in_order(part: &[&str], whole: &[&str]) -> bool {
    let mut whole = whole.iter();
    let last = part.len() - 1;
    part.iter()
        .enumerate()
        .all(|(i, word)| whole.any(|w| w == word || (i == last && w.starts_with(word))))
}

/// A row without a trailing cursor glyph, border, or padding.
fn core(row: &str) -> &str {
    row.trim_end_matches(|c: char| !c.is_alphanumeric() && !c.is_ascii_punctuation())
}

/// A row's letters with digit runs as `#`, so a counter, timer, or spinner keeps its shape.
fn shape(row: &str) -> String {
    let mut shape = String::new();
    for c in row.chars() {
        if c.is_ascii_digit() {
            if !shape.ends_with('#') {
                shape.push('#');
            }
        } else if c.is_alphabetic() {
            shape.push(c);
        } else if c.is_whitespace() && !shape.ends_with(' ') {
            shape.push(' ');
        }
    }
    shape.trim().to_string()
}

/// How surely `new` is the row `old` rewritten in place: 0 not, 1 a blank row filled, 3 the
/// row grown, slightly trimmed, or of the same shape.
fn update_weight(old: &Key, new: &Key) -> u32 {
    if old.blank {
        return 1;
    }
    if new.blank {
        return 0;
    }
    let grown = old.len >= 3 && new.core.starts_with(old.core);
    // Backspacing drops a little; a short prefix of a long row is a different row.
    let trimmed = new.len >= 3 && old.core.starts_with(new.core) && new.len * 5 >= old.len * 4;
    if grown || trimmed || old.shape == new.shape {
        3
    } else {
        0
    }
}

/// What `update_weight` compares of a row.
struct Key<'a> {
    blank: bool,
    core: &'a str,
    len: usize,
    shape: String,
}

impl<'a> Key<'a> {
    fn new(row: &'a str) -> Self {
        let core = core(row);
        Key {
            blank: blank(row),
            core,
            len: core.chars().count(),
            shape: shape(row),
        }
    }
}

/// The order-preserving pairing of `old` and `new` rows with the greatest total weight.
fn pair(old: &[String], new: &[String]) -> Vec<(usize, usize)> {
    let (m, n) = (old.len(), new.len());
    let new_keys: Vec<Key> = new.iter().map(|row| Key::new(row)).collect();
    let weights: Vec<Vec<u32>> = old
        .iter()
        .map(|row| {
            let old = Key::new(row);
            new_keys
                .iter()
                .map(|new| update_weight(&old, new))
                .collect()
        })
        .collect();
    let mut best = vec![vec![0_u32; n + 1]; m + 1];
    for i in (0..m).rev() {
        for j in (0..n).rev() {
            let w = weights[i][j];
            let take = if w > 0 { w + best[i + 1][j + 1] } else { 0 };
            best[i][j] = take.max(best[i + 1][j]).max(best[i][j + 1]);
        }
    }
    let (mut i, mut j, mut pairs) = (0, 0, Vec::new());
    while i < m && j < n {
        let w = weights[i][j];
        if w > 0 && best[i][j] == w + best[i + 1][j + 1] {
            pairs.push((i, j));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn merge(frames: &[Vec<String>]) -> Vec<String> {
        let mut history = History::default();
        for frame in frames {
            history.show(frame, 10_000);
        }
        history.text()
    }

    fn rows(rows: &[&str]) -> Vec<String> {
        rows.iter().map(|row| row.to_string()).collect()
    }

    #[test]
    fn frames_merge_into_one_history() {
        // A pager over code whose `}` lines repeat: down a row at a time, back up, down again.
        let code: Vec<String> = (0..6)
            .flat_map(|i| {
                [
                    format!("fn f{i}() {{"),
                    format!("    body {i}"),
                    "}".to_string(),
                ]
            })
            .collect();
        let window = |top: usize| code[top..top + 4].to_vec();
        let pager: Vec<Vec<String>> = (0..=14)
            .chain((0..14).rev())
            .chain(0..=14)
            .map(window)
            .collect();
        let tab_a = rows(&["Tabs", "alpha 1 red", "alpha 2 green", "status"]);
        let tab_b = rows(&["Tabs", "beta one", "beta two", "status"]);
        let cases = [
            (
                "rows rewritten in place keep their last text",
                vec![
                    rows(&[
                        "Monitor",
                        "> Analy",
                        "Downloading 17%",
                        "⠋ Working (1s)",
                        "? fix the bugs",
                    ]),
                    rows(&[
                        "Monitor",
                        "> Analyzing",
                        "Downloading 23%",
                        "⠙ Working (1s)",
                        "? fix the bugs",
                    ]),
                    rows(&[
                        "Monitor",
                        "> Analyzing files",
                        "Downloading 100%",
                        "⠹ Working (2s)",
                        "? fix the bug",
                    ]),
                ],
                rows(&[
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
                    rows(&["log", "", "", "", ""]),
                    rows(&["log", "one", "two", "", ""]),
                    rows(&["log", "one", "two", "three", "four"]),
                ],
                rows(&["log", "one", "two", "three", "four"]),
            ),
            (
                "scrolling back and forth shows each line once, in order",
                pager,
                code.clone(),
            ),
            (
                "a page shown again reuses its lines",
                vec![tab_a.clone(), tab_b.clone(), tab_a.clone(), tab_b],
                rows(&[
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
                    rows(&[
                        "Items",
                        "end_sync - applies the held output and returns any resulting",
                        "replies. [Code:127]",
                        "done",
                    ]),
                    rows(&[
                        "Items",
                        "end_sync - applies the held output and returns any",
                        "resulting replies. Code:127 (src/terminal.rs:127)",
                        "done",
                    ]),
                ],
                rows(&[
                    "Items",
                    "end_sync - applies the held output and returns any",
                    "resulting replies. Code:127 (src/terminal.rs:127)",
                    "done",
                ]),
            ),
            (
                "a new page follows the old one instead of filling its blank rows",
                vec![
                    rows(&["", "", "LOGO", "", "> ask anything"]),
                    rows(&["session", "you: hello", "bot: hi there", "", ""]),
                ],
                rows(&[
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
                vec![rows(&["title", "error: disk full"]), rows(&["title", ""])],
                rows(&["title", "error: disk full"]),
            ),
            (
                "a row shown again out of order repeats instead of moving",
                vec![
                    rows(&["title", "saved", "counter 1", "ending"]),
                    rows(&["title", "counter 1", "other", "ending"]),
                    rows(&["title", "counter 2", "saved", "ending"]),
                ],
                rows(&["title", "saved", "counter 2", "other", "saved", "ending"]),
            ),
        ];
        for (name, frames, expected) in cases {
            assert_eq!(merge(&frames), expected, "{name}");
        }
    }

    #[test]
    fn only_shown_lines_count_toward_the_limit() {
        // Each switch reflows a row away, which leaves the history no longer.
        let mut history = History::default();
        history.show(&rows(&["header", "important archive"]), 16);
        for _ in 0..200 {
            history.show(&rows(&["header", "alpha beta", "gamma delta epsilon"]), 16);
            history.show(&rows(&["header", "alpha beta gamma", "delta epsilon"]), 16);
        }
        let text = history.text();
        assert!(text.contains(&"important archive".to_string()), "{text:?}");
        assert!(!history.truncated);
        // Past the limit, the oldest lines leave and the screen's lines stay.
        // Distinct words, so no line updates the one before it.
        let log: Vec<String> = (0..40_u8)
            .map(|k| format!("log {}{}", (b'a' + k / 10) as char, (b'a' + k % 10) as char))
            .collect();
        for line in &log {
            history.show(&rows(&["header", line]), 16);
        }
        let text = history.text();
        assert!(history.truncated && text.len() <= 16 + 16 / 8, "{text:?}");
        assert_eq!(text.first().map(String::as_str), Some("header"));
        assert!(text.ends_with(&log[log.len() - 8..]), "{text:?}");
    }
}
