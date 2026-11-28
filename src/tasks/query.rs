//! Task listing, session entry pruning, and background footer notice generation.

use super::quota::lock;
use super::session::{OutsideClaim, Session};
use super::state::{Root, Snapshot, Task, copy_snapshot_text, mark_tree_ended, pids, set_held};
use super::store::Tasks;
use crate::platform::members::Member;
use crate::platform::process::{Tree, clean_member_name};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

/// Maximum indented active children shown inline beneath a single `codemode` parent.
pub const MAX_INLINE_ACTIVE: usize = 8;

#[derive(Default)]
pub struct ChildSummary {
    pub total: usize,
    pub done: usize,
    pub failed: usize,
    pub warned: usize,
    pub tree_ended: usize,
    pub active: usize,
    pub queued: usize,
    pub cancelled: usize,
    /// Up to `MAX_INLINE_ACTIVE` active children (newest first), empty when `state == "finished"`.
    pub active_tasks: Vec<(Arc<Task>, Snapshot)>,
    /// Folded finished children (`task`, `tree_ended`) for post-delivery `mark_reported`.
    pub folded_finished: Vec<(Arc<Task>, bool)>,
}

impl ChildSummary {
    pub fn should_show(&self, running: bool) -> bool {
        self.total > 0 || (running && self.queued > 0) || (!running && self.cancelled > 0)
    }
}

pub struct ListedTask {
    pub task: Arc<Task>,
    pub snapshot: Snapshot,
    /// Set on drill-down rows and promoted orphan rows; `None` on top-level parents.
    pub parent_task_id: Option<String>,
    /// Present on top-level `codemode` parents in default mode; `None` otherwise.
    pub children: Option<ChildSummary>,
}

pub struct ListQuery<'a> {
    pub all_scope: bool,
    pub active_filter: Option<bool>,
    pub parent_task_id: Option<&'a str>,
    pub limit: usize,
}

pub struct ListedResult {
    pub total_count: usize,
    pub items: Vec<ListedTask>,
}

/// A task and the snapshot a result showed of it.
pub type Shown = (Arc<Task>, Snapshot);
pub type BackgroundNotice<'a> = (Option<(String, Vec<Shown>)>, OutsideClaim<'a>);

/// Reserves metadata workspace and captures a task snapshot under the registry lock.
fn snapshot_listed_task(
    task: &Task,
    reserve: &mut impl FnMut(usize) -> Result<(), String>,
) -> Result<Snapshot, String> {
    let (end, tree_done) = {
        let end = lock(&task.end);
        let tree_done = *task.tree_done.borrow();
        // Immutable text and per-row keys are admitted before any snapshot,
        // display path, JSON field or formatted row copies them.
        let text = task
            .owner
            .len()
            .saturating_add(task.id.len())
            .saturating_add(
                task.started_by
                    .as_ref()
                    .map_or(0, |(session, _)| session.len()),
            )
            .saturating_add(task.parent_task_id.as_ref().map_or(0, String::len))
            .saturating_add(
                task.command
                    .chars()
                    .take(120)
                    .map(char::len_utf8)
                    .sum::<usize>(),
            )
            .saturating_add(
                task.log_path
                    .as_os_str()
                    .as_encoded_bytes()
                    .len()
                    .saturating_mul(3),
            )
            .saturating_add(512);
        reserve(text)?;
        let end = end
            .as_ref()
            .map(|end| end.clone_reserved(reserve))
            .transpose()?;
        (end, tree_done)
    };
    task.snapshot_at_reserved(end, tree_done, reserve)
}

impl Tasks {
    /// Lists tasks for `exec list` under the registry-then-session lock order.
    ///
    /// When `parent_task_id` is `Some(pid)`, drills down into flat children of `pid` (returning
    /// `Ok(None)` when neither `pid` nor any child of `pid` exists in the registry, and an empty
    /// list when `pid` is a bash task). When `parent_task_id` is `None`, folds children under
    /// eligible top-level parents, promotes orphans whose parent is absent from this view, and
    /// sorts parents by `max(parent_pos, newest_active_child_pos)` unless `active_filter == Some(false)`.
    /// Held-process inspection (`members(tree)`) runs after dropping the registry lock.
    pub fn listed(
        &self,
        session: &Session,
        query: ListQuery<'_>,
        reserve: &mut impl FnMut(usize) -> Result<(), String>,
        members: &mut impl FnMut(&Tree) -> Result<Option<Vec<Member>>, String>,
    ) -> Result<Option<ListedResult>, String> {
        let ListQuery {
            all_scope,
            active_filter,
            parent_task_id,
            limit,
        } = query;
        let mut result = {
            let registry = lock(&self.registry);
            if let Some(pid) = parent_task_id {
                if let Some(parent) = registry.tasks.iter().find(|task| task.id == pid) {
                    if !parent.is_script() {
                        return Ok(Some(ListedResult {
                            total_count: 0,
                            items: Vec::new(),
                        }));
                    }
                } else if !registry
                    .tasks
                    .iter()
                    .any(|task| task.parent_task_id.as_deref() == Some(pid))
                {
                    return Ok(None);
                }
                let touched = (!all_scope).then(|| session.touched(&registry.tasks));
                let mut total_count = 0usize;
                let mut items = Vec::with_capacity(limit.min(registry.tasks.len()));
                let mut consider = |task: &Arc<Task>| -> Result<(), String> {
                    if task.parent_task_id.as_deref() != Some(pid) {
                        return Ok(());
                    }
                    let active = {
                        let end = lock(&task.end);
                        let tree_done = *task.tree_done.borrow();
                        end.as_ref().is_none_or(|end| end.kept && !tree_done)
                    };
                    if active_filter.is_some_and(|want| want != active) {
                        return Ok(());
                    }
                    total_count += 1;
                    if items.len() == limit {
                        return Ok(());
                    }
                    let snapshot = snapshot_listed_task(task, reserve)?;
                    items.push(ListedTask {
                        task: Arc::clone(task),
                        snapshot,
                        parent_task_id: Some(pid.to_owned()),
                        children: None,
                    });
                    Ok(())
                };
                if let Some(touched) = &touched {
                    for (task, _) in touched {
                        consider(task)?;
                    }
                } else {
                    for task in registry.tasks.iter().rev() {
                        consider(task)?;
                    }
                }
                ListedResult { total_count, items }
            } else {
                let touched = (!all_scope).then(|| session.touched(&registry.tasks));
                let touched_ids: Option<HashSet<&str>> = touched
                    .as_ref()
                    .map(|items| items.iter().map(|(task, _)| task.id.as_str()).collect());
                struct ParentFold {
                    summary: ChildSummary,
                    active_children: Vec<Arc<Task>>,
                    newest_active_child_pos: usize,
                }
                enum Candidate {
                    Parent {
                        task: Arc<Task>,
                        pos: usize,
                    },
                    Orphan {
                        task: Arc<Task>,
                        parent_id: String,
                        pos: usize,
                    },
                }
                let mut eligible_parents: HashMap<&str, ParentFold> = HashMap::new();
                let mut candidates: Vec<Candidate> = Vec::new();
                for (pos, task) in registry.tasks.iter().enumerate() {
                    if touched_ids
                        .as_ref()
                        .is_some_and(|ids| !ids.contains(task.id.as_str()))
                    {
                        continue;
                    }
                    let (active, exit_code, tree_ended) = {
                        let end = lock(&task.end);
                        let tree_done = *task.tree_done.borrow();
                        let active = end.as_ref().is_none_or(|end| end.kept && !tree_done);
                        let exit_code = end.as_ref().and_then(|end| end.exit_code);
                        let tree_ended = end.as_ref().is_some_and(|end| end.kept && tree_done);
                        (active, exit_code, tree_ended)
                    };
                    match task.parent_task_id.as_deref() {
                        None => {
                            if active_filter.is_some_and(|want| want != active) {
                                continue;
                            }
                            eligible_parents.insert(
                                task.id.as_str(),
                                ParentFold {
                                    summary: ChildSummary::default(),
                                    active_children: Vec::new(),
                                    newest_active_child_pos: 0,
                                },
                            );
                            candidates.push(Candidate::Parent {
                                task: Arc::clone(task),
                                pos,
                            });
                        }
                        Some(pid) => {
                            if let Some(fold) = eligible_parents.get_mut(pid) {
                                fold.summary.total += 1;
                                if active {
                                    fold.summary.active += 1;
                                    fold.active_children.push(Arc::clone(task));
                                    fold.newest_active_child_pos =
                                        fold.newest_active_child_pos.max(pos);
                                } else {
                                    if exit_code == Some(0) {
                                        fold.summary.done += 1;
                                    } else {
                                        fold.summary.failed += 1;
                                    }
                                    if task.has_warnings_unlocked() {
                                        fold.summary.warned += 1;
                                    }
                                    if tree_ended {
                                        fold.summary.tree_ended += 1;
                                    }
                                    fold.summary
                                        .folded_finished
                                        .push((Arc::clone(task), tree_ended));
                                }
                            } else if active_filter.is_none_or(|want| want == active) {
                                candidates.push(Candidate::Orphan {
                                    task: Arc::clone(task),
                                    parent_id: pid.to_owned(),
                                    pos,
                                });
                            }
                        }
                    }
                }
                let sort_key = |candidate: &Candidate| -> (usize, usize) {
                    match candidate {
                        Candidate::Parent { task, pos } => {
                            let sort_pos = if active_filter != Some(false) {
                                let newest = eligible_parents
                                    .get(task.id.as_str())
                                    .map_or(0, |fold| fold.newest_active_child_pos);
                                (*pos).max(newest)
                            } else {
                                *pos
                            };
                            (sort_pos, *pos)
                        }
                        Candidate::Orphan { pos, .. } => (*pos, *pos),
                    }
                };
                candidates.sort_by_key(|candidate| std::cmp::Reverse(sort_key(candidate)));
                let total_count = candidates.len();
                candidates.truncate(limit);
                let mut items = Vec::with_capacity(candidates.len());
                for candidate in candidates {
                    match candidate {
                        Candidate::Orphan {
                            task, parent_id, ..
                        } => {
                            let snapshot = snapshot_listed_task(&task, reserve)?;
                            items.push(ListedTask {
                                task,
                                snapshot,
                                parent_task_id: Some(parent_id),
                                children: None,
                            });
                        }
                        Candidate::Parent { task, .. } => {
                            let snapshot = snapshot_listed_task(&task, reserve)?;
                            let fold = eligible_parents.remove(task.id.as_str());
                            let children = if let Some(mut fold) = fold
                                && (task.is_script() || fold.summary.total > 0)
                            {
                                if let Some(script) = task.script() {
                                    let (queued, cancelled) = script.list_counts();
                                    fold.summary.queued = queued;
                                    fold.summary.cancelled = cancelled;
                                }
                                if active_filter != Some(false) {
                                    for child in fold
                                        .active_children
                                        .into_iter()
                                        .rev()
                                        .take(MAX_INLINE_ACTIVE)
                                    {
                                        let child_snapshot = snapshot_listed_task(&child, reserve)?;
                                        fold.summary.active_tasks.push((child, child_snapshot));
                                    }
                                }
                                Some(fold.summary)
                            } else {
                                None
                            };
                            items.push(ListedTask {
                                task,
                                snapshot,
                                parent_task_id: None,
                                children,
                            });
                        }
                    }
                }
                ListedResult { total_count, items }
            }
        };
        let mut populate_held = |task: &Task, snapshot: &mut Snapshot| -> Result<(), String> {
            if !snapshot.kept {
                return Ok(());
            }
            let listed = match &task.root {
                Root::Process(tree) => members(tree)?,
                Root::Script(_) => Some(Vec::new()),
            };
            set_held(snapshot, listed);
            if !snapshot.held_known && snapshot.kill_error.is_none() && !*task.tree_done.borrow() {
                snapshot.kill_error = copy_snapshot_text(&lock(&task.kill_error), reserve)?;
            }
            Ok(())
        };
        for item in &mut result.items {
            populate_held(&item.task, &mut item.snapshot)?;
            if let Some(children) = &mut item.children {
                for (child, snapshot) in &mut children.active_tasks {
                    populate_held(child, snapshot)?;
                }
            }
        }
        Ok(Some(result))
    }

    /// Drops retired entries under the registry-then-session lock order.
    pub fn prune_entries(&self, session: &Session) {
        let registry = lock(&self.registry);
        session.touched(&registry.tasks);
    }

    /// Returns the bounded notice, only the tasks actually named in it, and this call's
    /// once-per-session outside-task reservation (released automatically unless committed).
    pub fn background<'a>(
        &self,
        session: &'a Session,
        exclude: &str,
        cap: usize,
        reserve: &mut impl FnMut(usize) -> Result<(), String>,
        members: &mut impl FnMut(&Tree) -> Result<Option<Vec<Member>>, String>,
    ) -> Result<BackgroundNotice<'a>, String> {
        let mut claim = session.claim_outside();
        // Registry and session locks are taken together, in that order, so a task that starts
        // meanwhile cannot be pruned as retired; both are released before any snapshot.
        let (touched, others) = {
            let registry = lock(&self.registry);
            let touched = session.touched(&registry.tasks);
            let others: Vec<_> = if claim.is_claimed() {
                let own: HashSet<_> = touched.iter().map(|(task, _)| task.id.as_str()).collect();
                registry
                    .tasks
                    .iter()
                    .filter(|task| task.id != exclude && !own.contains(task.id.as_str()))
                    .cloned()
                    .collect()
            } else {
                Vec::new()
            };
            (touched, others)
        };
        let others = others
            .iter()
            .filter(|task| {
                let (state, kept) = task.report_state();
                state == "running" || kept
            })
            .count();
        // Rank 0: failed or expired; 1: other finished, or held processes ended; 2: running or
        // holding processes. Newest first within a rank.
        #[derive(PartialEq)]
        enum Notice {
            Root,
            TreeEnded,
            Keeps,
            Running,
        }
        // Collect running codemode parents before filtering `exclude` so polling a running
        // script does not surface its own running children in the background footer.
        let running_scripts: HashSet<String> = touched
            .iter()
            .filter(|(task, _)| task.is_script() && task.is_running())
            .map(|(task, _)| task.id.clone())
            .collect();
        let mut entries = Vec::new();
        for (task, entry) in touched.into_iter().filter(|(task, _)| task.id != exclude) {
            let mut snapshot = task.notice_snapshot();
            if snapshot.state == "running"
                && task
                    .parent_task_id
                    .as_deref()
                    .is_some_and(|pid| running_scripts.contains(pid))
            {
                continue;
            }
            let (rank, notice) = match snapshot.state {
                "running" => (2, Notice::Running),
                _ if !entry.is_reported() => {
                    let failed = snapshot.state == "exited" && snapshot.exit_code != Some(0)
                        || snapshot.expired;
                    (if failed { 0 } else { 1 }, Notice::Root)
                }
                _ if snapshot.tree_ended && !entry.tree_end_reported() => (1, Notice::TreeEnded),
                _ if snapshot.kept => {
                    let listed = match &task.root {
                        Root::Process(tree) => members(tree)?,
                        Root::Script(_) => Some(Vec::new()),
                    };
                    set_held(&mut snapshot, listed);
                    if *task.tree_done.borrow() {
                        if entry.tree_end_reported() {
                            continue;
                        }
                        mark_tree_ended(&mut snapshot);
                        (1, Notice::TreeEnded)
                    } else {
                        if !snapshot.held_known
                            && snapshot.kill_error.is_none()
                            && lock(&task.kill_error).is_some()
                        {
                            snapshot.kill_error = Some(String::new());
                        }
                        // The current PID vector fits the admitted member workspace; the
                        // previous reported vector may be longer and is only borrowed.
                        if snapshot.held_known
                            && snapshot.kill_error.is_none()
                            && entry.keeps_reported_eq(&pids(&snapshot.held))
                        {
                            continue;
                        }
                        (2, Notice::Keeps)
                    }
                }
                _ => continue,
            };
            entries.push((rank, notice, task, snapshot));
        }
        if entries.is_empty() && others == 0 {
            return Ok((None, claim));
        }
        entries.sort_by_key(|(rank, ..)| *rank);
        for (index, (_, notice, task, snapshot)) in entries.iter_mut().enumerate() {
            if *notice != Notice::Root || !snapshot.kept {
                continue;
            }
            if *task.tree_done.borrow() {
                mark_tree_ended(snapshot);
                continue;
            }
            if index < 3 {
                let listed = match &task.root {
                    Root::Process(tree) => members(tree)?,
                    Root::Script(_) => Some(Vec::new()),
                };
                set_held(snapshot, listed);
                if *task.tree_done.borrow() {
                    mark_tree_ended(snapshot);
                } else if !snapshot.held_known
                    && snapshot.kill_error.is_none()
                    && lock(&task.kill_error).is_some()
                {
                    snapshot.kill_error = Some(String::new());
                }
            }
        }
        // Counts/labels are fixed-size metadata. Only three handles enter formatting;
        // process names were admitted while querying the relevant held trees.
        reserve(256)?;
        for (_, _, task, _) in entries.iter().take(3) {
            reserve(task.id.capacity().saturating_add(128))?;
        }
        let failed = entries.iter().filter(|entry| entry.0 == 0).count();
        let running = entries
            .iter()
            .filter(|entry| entry.1 == Notice::Running)
            .count();
        let keeping = entries
            .iter()
            .filter(|entry| entry.1 == Notice::Keeps || (entry.1 == Notice::Root && entry.3.kept))
            .count();
        let finished = entries.len() - running - keeping;
        let mut counts = Vec::new();
        for (number, label) in [
            (running, "running"),
            (keeping, "keeping processes"),
            (finished, "finished"),
            (failed, "failed"),
        ] {
            if number > 0 {
                counts.push(format!("{number} {label}"));
            }
        }
        let counts = counts.join(", ");
        let shown_tasks = |entries: &mut Vec<(u8, Notice, Arc<Task>, Snapshot)>, shown: usize| {
            entries
                .drain(..shown)
                .map(|(_, _, task, snapshot)| (task, snapshot))
                .collect()
        };
        let format_named = |shown: usize| -> Vec<String> {
            entries[..shown]
                .iter()
                .map(|(_, notice, task, snapshot)| {
                    let id = &task.id;
                    match (notice, snapshot.exit_code) {
                        (Notice::Running, _) => {
                            format!("{id} running {}", short(snapshot.elapsed))
                        }
                        (Notice::Keeps, _) => format!("{id} keeps {}", held_brief(snapshot)),
                        (Notice::TreeEnded, _) => format!("{id} kept processes ended"),
                        (_, code) => {
                            let root = match code {
                                Some(code) if snapshot.state == "exited" => {
                                    format!("exited {code}")
                                }
                                _ if snapshot.expired => "killed by killAfterMs".into(),
                                _ => "killed".into(),
                            };
                            if snapshot.kept {
                                format!("{id} {root}, keeps {}", held_brief(snapshot))
                            } else if snapshot.tree_ended {
                                format!("{id} {root} and its kept processes ended")
                            } else {
                                format!("{id} {root}")
                            }
                        }
                    }
                })
                .collect()
        };
        if others > 0 {
            let outside_text = if others == 1 {
                "1 task running outside this session".to_owned()
            } else {
                format!("{others} tasks running outside this session")
            };
            for shown in (1..=entries.len().min(3)).rev() {
                let named = format_named(shown);
                let rest = entries.len() - shown;
                let line = if rest == 0 {
                    format!(
                        "(Background: {}; {outside_text}; use list with scope \"all\".)",
                        named.join(", ")
                    )
                } else {
                    format!(
                        "(Background: {}; {rest} more; {counts}; {outside_text}; use list with scope \"all\".)",
                        named.join(", ")
                    )
                };
                if line.len() <= cap {
                    return Ok((Some((line, shown_tasks(&mut entries, shown))), claim));
                }
            }
            let line = if entries.is_empty() {
                format!("(Background: {outside_text}; use list with scope \"all\".)")
            } else {
                format!("(Background: {counts}; {outside_text}; use list with scope \"all\".)")
            };
            if line.len() <= cap {
                return Ok((Some((line, Vec::new())), claim));
            }
            // The budget could not fit the outside-task count: leave the slot for a later call.
            claim.release();
        }
        if entries.is_empty() {
            return Ok((None, claim));
        }
        for shown in (1..=entries.len().min(3)).rev() {
            let named = format_named(shown);
            let rest = entries.len() - shown;
            let line = if rest == 0 {
                format!("(Background: {}.)", named.join(", "))
            } else {
                format!(
                    "(Background: {}; {rest} more; {counts}; use list.)",
                    named.join(", ")
                )
            };
            if line.len() <= cap {
                return Ok((Some((line, shown_tasks(&mut entries, shown))), claim));
            }
        }
        let line = format!("(Background: {counts}; use list.)");
        if line.len() <= cap {
            return Ok((Some((line, Vec::new())), claim));
        }
        let total = entries.len();
        let plural = if total == 1 { "" } else { "s" };
        let line = format!("({total} task{plural}; use list.)");
        Ok(((line.len() <= cap).then(|| (line, Vec::new())), claim))
    }
}

/// The first held process and how many more: `adb.exe +2`.
fn brief(held: &[Member]) -> String {
    let label = |member: &Member| {
        clean_member_name(&member.name).unwrap_or_else(|| format!("pid {}", member.pid))
    };
    match held {
        [] => "processes".into(),
        [one] => label(one),
        [first, rest @ ..] => format!("{} +{}", label(first), rest.len()),
    }
}

pub(super) fn held_brief(snapshot: &Snapshot) -> String {
    if !snapshot.held_known {
        if snapshot.kill_error.is_some() {
            "processes (unconfirmed, kill failed)".into()
        } else {
            "processes (unconfirmed)".into()
        }
    } else if snapshot.kill_error.is_some() {
        format!("{} (kill failed)", brief(&snapshot.held))
    } else {
        brief(&snapshot.held)
    }
}

/// Elapsed time in its largest two units: `42s`, `4m3s`, `2h5m`.
fn short(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs();
    match seconds {
        0..60 => format!("{seconds}s"),
        60..3600 => format!("{}m{}s", seconds / 60, seconds % 60),
        _ => format!("{}h{}m", seconds / 3600, seconds % 3600 / 60),
    }
}
