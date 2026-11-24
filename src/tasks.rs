//! Task registry: spawn, log capture, unseen-output cursors, input queues, and shutdown.

pub mod state;
#[cfg(test)]
mod tests;
mod workers;

use self::state::{
    AttachmentMeta, AttachmentSlot, AttachmentSlotState, Final, Input, Pty, Root, Snapshot, Spool,
    Task,
};
use self::workers::{
    CaptureWork, InputWork, OutputWorkers, ReadWork, WaitWork, Workers, WriterDone, drain,
};
use crate::codemode;
use crate::media::MediaKind;
use crate::platform::process::{self, Launch, Member, Tree};
use crate::session::{LaunchContext, ProbeWarning, Reader, Session, TaskEntry};
use crate::term::terminal::Live;
use encoding_rs::Encoding;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, mpsc};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::watch;

/// What a server admits and keeps: how many tasks may run at once, how many finished tasks stay
/// in the registry, and how long a finished one is kept.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_running: usize,
    pub max_scripts: usize,
    pub max_finished: usize,
    pub finished_ttl: Option<Duration>,
}

impl Limits {
    /// A stdio server's limits: a session ends with the client, so finished tasks are kept and
    /// never expire.
    pub const OFF: Limits = Limits {
        max_running: 16,
        max_scripts: 4,
        max_finished: 64,
        finished_ttl: None,
    };

    /// A daemon's limits: it serves more than one client and lives longer, so it admits more
    /// tasks and lets finished ones go after a day.
    pub const ON: Limits = Limits {
        max_running: 64,
        max_scripts: 8,
        max_finished: 256,
        finished_ttl: Some(Duration::from_secs(24 * 60 * 60)),
    };
}

pub(crate) const TASK_LOG_LIMIT: u64 = 64 << 20;
pub(crate) const MAX_TASK_ATTACHMENTS: usize = 1024;
const TOTAL_LOG_LIMIT: u64 = 4 << 30;
const INPUT_QUEUE_LIMIT: usize = 1 << 20;
const INPUT_WRITE_LIMIT: Duration = Duration::from_secs(5);
/// How long output may stay open after the root exits and its tree is killed.
pub const DRAIN_CAP: Duration = Duration::from_secs(2);

/// Runs before a Windows ConPTY root's command, on its first line, so the script's exit status is
/// on disk as the script ends. The root can deadlock inside its own exit; this file is what lets
/// its task end anyway. A user's own EXIT cleanup is never overwritten, and a command that
/// replaces the trap or execs keeps the plain wait.
const EXIT_TRAP: &str = r#"[ -n "${FASTEXEC_EXIT_STATUS-}" ] && [ "${BASH_SUBSHELL-0}" -eq 0 ] && builtin export -n FASTEXEC_EXIT_STATUS && [ -z "$(builtin trap -p EXIT)" ] && builtin trap 'builtin printf "%s\n" "$?" 2>/dev/null > "$FASTEXEC_EXIT_STATUS" || :' EXIT || :; "#;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct PendingDelete {
    path: PathBuf,
    bytes: u64,
}

#[cfg(test)]
type RemoveHook = Arc<dyn Fn(&Path) -> std::io::Result<()> + Send + Sync>;
#[cfg(test)]
type LogWriteHook = Arc<dyn Fn(&mut File, &[u8]) -> std::io::Result<()> + Send + Sync>;
#[cfg(test)]
type LogTruncateHook = Arc<dyn Fn(&File, u64) -> std::io::Result<()> + Send + Sync>;

pub struct Tasks {
    dir: PathBuf,
    /// Arms `killAfterMs`: the deadline belongs to the Core, not to the starting request.
    handle: Handle,
    /// This server's ID epoch: 40 bits from the OS random source as 8 Crockford base32
    /// characters. Every task ID starts with it, so an ID from another server is told apart.
    epoch: String,
    /// The counter value of the next ID; IDs never repeat within an epoch.
    next: AtomicU64,
    registry: Mutex<Registry>,
    limits: Limits,
    log_bytes: AtomicU64,
    /// [`TOTAL_LOG_LIMIT`]; debug builds take a smaller one from
    /// `FASTEXEC_TEST_TOTAL_LOG_LIMIT`, so tests reach it without writing 4 GiB.
    log_limit: u64,
    /// Files whose unlink failed keep their byte charge here until a later unlink succeeds.
    pending_deletes: Mutex<Vec<PendingDelete>>,
    delete_retry_gate: Mutex<()>,
    event_bus: OnceLock<Arc<crate::events::EventBus>>,
    #[cfg(test)]
    remove_hook: Mutex<Option<RemoveHook>>,
    #[cfg(test)]
    log_write_hook: Mutex<Option<LogWriteHook>>,
    #[cfg(test)]
    log_truncate_hook: Mutex<Option<LogTruncateHook>>,
}

#[derive(Default)]
struct Registry {
    tasks: Vec<Arc<Task>>,
    /// Starts admitted but not yet inserted; they count against [`Limits::max_running`].
    pending: usize,
    pending_scripts: usize,
    /// Set by [`Tasks::shutdown`]: no start is admitted from then on.
    stopping: bool,
}

/// A start in flight: it holds a place against [`Limits::max_running`] until it is inserted
/// or fails.
struct Pending<'a> {
    registry: &'a Mutex<Registry>,
    counted: bool,
    script: bool,
}

impl Pending<'_> {
    /// Gives the place back inside the caller's critical section: its own drop must never
    /// re-lock a registry mutex the caller still holds.
    fn release(&mut self, registry: &mut Registry) {
        if self.counted {
            registry.pending -= 1;
            if self.script {
                registry.pending_scripts -= 1;
            }
            self.counted = false;
        }
    }
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if self.counted {
            let mut registry = lock(self.registry);
            self.release(&mut registry);
        }
    }
}

/// Crockford's base32 alphabet in lowercase.
pub(crate) const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// `value` in Crockford's base32, most significant digit first, no padding beyond `digits`.
fn base32(mut value: u128, digits: usize) -> String {
    let digits = digits.max(1);
    let mut out = Vec::with_capacity(digits);
    loop {
        out.push(ALPHABET[(value & 31) as usize] as char);
        value >>= 5;
        if value == 0 && out.len() >= digits {
            break;
        }
    }
    out.iter().rev().collect()
}

/// An epoch and a counter value as a task ID.
fn task_id(epoch: &str, number: u64) -> String {
    format!("{epoch}-{}", base32(number as u128, 1))
}

/// `digits` characters (at most 26) from 128 random bits, for the epoch and session names.
pub fn random_chars(digits: usize) -> std::io::Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| {
        std::io::Error::other(format!("Cannot read the OS random source: {error}."))
    })?;
    let out = base32(u128::from_be_bytes(bytes), 26);
    Ok(out[26 - digits..].into())
}

/// A fresh 40-bit epoch from the OS random source: 8 Crockford base32 characters.
fn new_epoch() -> std::io::Result<String> {
    random_chars(8)
}

/// Whether `id` has the bare task ID shape `<8-char base32 epoch>-<base32 counter>`.
pub fn is_valid_task_id(id: &str) -> bool {
    let Some((epoch, counter)) = id.split_once('-') else {
        return false;
    };
    epoch.len() == 8
        && !counter.is_empty()
        && counter.len() <= 13
        && epoch.bytes().all(|byte| ALPHABET.contains(&byte))
        && counter.bytes().all(|byte| ALPHABET.contains(&byte))
}

/// The error a transcript read returns for a task whose files the total log limit evicted.
pub const EVICTED_TRANSCRIPT: &str =
    "The files of this task were evicted under the 4 GiB total limit; its transcript is gone.";

/// Maximum indented active children shown inline beneath a single `codemode` parent.
pub const MAX_INLINE_ACTIVE: usize = 8;

#[derive(Default)]
pub struct ChildSummary {
    pub total: usize,
    pub done: usize,
    pub failed: usize,
    pub warned: usize,
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

pub type Started = (Arc<Task>, Arc<TaskEntry>, Arc<Reader>);

pub struct StartArgs {
    pub command: String,
    pub cwd: PathBuf,
    pub pty: bool,
    /// The terminal's size: columns, then screen rows. A pipe task ignores them.
    pub cols: u16,
    pub rows: u16,
    pub login: bool,
    pub keep: bool,
    pub input: Option<String>,
    pub eof: Option<bool>,
    pub encoding: &'static Encoding,
    /// `killAfterMs`: end the task's tree this many ms after launch; 0 or None means no limit.
    pub kill_after_ms: Option<u64>,
    /// The session that starts the task, shown as its owner.
    pub owner: String,
    /// The session ID and daemon call that started the task, shown by `list`; `None` for a
    /// stdio call, which has neither.
    pub started_by: Option<(String, u64)>,
    /// Parent codemode task ID when started via nested `exec start`.
    pub parent_task_id: Option<String>,
    /// Releases the parent script's `queued_starts` counter once the task enters `registry.tasks`.
    pub queued_guard: Option<codemode::control::QueuedStartGuard>,
    /// What the task inherits: its environment, and the bash found in it.
    pub launch: Arc<LaunchContext>,
    pub input_owner: crate::lease::Owner,
}

pub(crate) struct ScriptArgs {
    pub code: String,
    pub launch: Arc<LaunchContext>,
    pub kill_after_ms: Option<u64>,
    pub transfer: bool,
    pub call_id: Option<u64>,
    pub service: Arc<crate::service::Service>,
    pub input_owner: crate::lease::Owner,
}

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

/// The process-backed tests serialize their shared fixtures.
#[cfg(test)]
pub(crate) static ONE_TASKS: Mutex<()> = Mutex::new(());

impl Tasks {
    pub fn new(limits: Limits) -> std::io::Result<Arc<Self>> {
        let temp = std::env::temp_dir();
        remove_stale_dirs(&temp);
        // A predictable remove-then-create path lets another user win the creation race in
        // a shared temp directory. mkdir itself reserves a fresh random name atomically.
        for _ in 0..8 {
            let dir = temp.join(format!(
                "{}{}-{}",
                dir_prefix(),
                std::process::id(),
                random_chars(12)?
            ));
            match crate::platform::runtime::create_private_new(&dir) {
                Ok(()) => return Self::new_in(limits, dir),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "cannot reserve a fresh private task log directory",
        ))
    }

    /// A registry whose task logs live in `dir`, which is created if it is missing.
    pub fn new_in(limits: Limits, dir: PathBuf) -> std::io::Result<Arc<Self>> {
        let epoch = new_epoch()?;
        let handle = Handle::try_current().map_err(|error| {
            std::io::Error::other(format!(
                "Tasks needs a tokio runtime for the killAfterMs timer: {error}."
            ))
        })?;
        crate::platform::runtime::create_private(&dir)?;
        Ok(Arc::new(Self {
            dir,
            handle,
            epoch,
            next: AtomicU64::new(1),
            registry: Mutex::default(),
            limits,
            log_bytes: AtomicU64::new(0),
            log_limit: std::env::var("FASTEXEC_TEST_TOTAL_LOG_LIMIT")
                .ok()
                .and_then(|limit| limit.parse().ok())
                .filter(|limit| cfg!(debug_assertions) && (1..=TOTAL_LOG_LIMIT).contains(limit))
                .unwrap_or(TOTAL_LOG_LIMIT),
            pending_deletes: Mutex::default(),
            delete_retry_gate: Mutex::new(()),
            event_bus: OnceLock::new(),
            #[cfg(test)]
            remove_hook: Mutex::new(None),
            #[cfg(test)]
            log_write_hook: Mutex::new(None),
            #[cfg(test)]
            log_truncate_hook: Mutex::new(None),
        }))
    }

    #[cfg(test)]
    pub(crate) fn set_log_limit(&mut self, limit: u64) {
        self.log_limit = limit;
    }

    #[cfg(test)]
    pub(crate) fn set_remove_hook(&self, hook: Option<RemoveHook>) {
        *lock(&self.remove_hook) = hook;
    }

    fn write_log(&self, file: &mut File, bytes: &[u8]) -> std::io::Result<()> {
        #[cfg(test)]
        if let Some(hook) = lock(&self.log_write_hook).clone() {
            return hook(file, bytes);
        }
        file.write_all(bytes)
    }

    fn truncate_log(&self, file: &File, bytes: u64) -> std::io::Result<()> {
        #[cfg(test)]
        if let Some(hook) = lock(&self.log_truncate_hook).clone() {
            return hook(file, bytes);
        }
        file.set_len(bytes)
    }

    /// Attaches the shared event bus once so task exits emit `task.exited`.
    pub fn attach_event_bus(&self, bus: Arc<crate::events::EventBus>) {
        let _ = self.event_bus.set(bus);
    }

    fn emit_task_exited(&self, task: &Task) {
        let Some(bus) = self.event_bus.get() else {
            return;
        };
        let input = {
            let end = lock(&task.end);
            let Some(end) = end.as_ref() else { return };
            let tree_done = *task.tree_done.borrow();
            crate::events::TaskExitedInput {
                task_id: task.id.clone(),
                session_id: task.session_id.clone(),
                state: if end.killed { "killed" } else { "exited" }.to_owned(),
                exit_code: end.exit_code,
                expired: end.expired,
                kept_processes: if end.kept && !tree_done {
                    end.leftovers.len() as u64
                } else {
                    0
                },
                tree_ended: end.kept && tree_done,
                log_path: Some(crate::files::path::display_path(&task.log_path)),
            }
        };
        let _ = bus.publish_task_exited(input);
    }

    pub fn find(&self, id: &str) -> Option<Arc<Task>> {
        lock(&self.registry)
            .tasks
            .iter()
            .find(|task| task.id == id)
            .cloned()
    }

    /// This server's ID epoch, the prefix every task ID of this server carries.
    pub fn epoch(&self) -> &str {
        &self.epoch
    }

    /// The next task ID: this epoch and the next counter value.
    fn next_id(&self) -> String {
        let number = self.next.fetch_add(1, Ordering::Relaxed);
        task_id(&self.epoch, number)
    }

    /// Every task, newest first.
    pub fn all(&self) -> Vec<Arc<Task>> {
        lock(&self.registry).tasks.iter().rev().cloned().collect()
    }

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
                                        let child_snapshot =
                                            snapshot_listed_task(&child, reserve)?;
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
        for item in &mut result.items {
            if item.snapshot.kept {
                item.snapshot.held = match &item.task.root {
                    Root::Process(tree) => members(tree)?.unwrap_or_default(),
                    Root::Script(_) => Vec::new(),
                };
            }
            if let Some(children) = &mut item.children {
                for (child, snapshot) in &mut children.active_tasks {
                    if snapshot.kept {
                        snapshot.held = match &child.root {
                            Root::Process(tree) => members(tree)?.unwrap_or_default(),
                            Root::Script(_) => Vec::new(),
                        };
                    }
                }
            }
        }
        Ok(Some(result))
    }

    /// Takes a place against [`Limits::max_running`] and the next task ID. The guard gives the
    /// place back on drop, so every failed start releases it.
    fn admit(&self, script: bool) -> Result<(String, Pending<'_>), String> {
        let mut registry = lock(&self.registry);
        if registry.stopping {
            return Err("The server is shutting down; the task was not started.".into());
        }
        let running = registry
            .tasks
            .iter()
            .filter(|task| !task.is_finished())
            .count();
        let limit = self.limits.max_running;
        if running + registry.pending >= limit {
            return Err(format!(
                "TOO_MANY_TASKS: {limit} tasks are running or keep processes. Wait for one to finish or kill one, then retry."
            ));
        }
        if script {
            let running = registry
                .tasks
                .iter()
                .filter(|task| task.is_script() && task.is_running())
                .count();
            let limit = self.limits.max_scripts;
            if running + registry.pending_scripts >= limit {
                return Err(format!(
                    "TOO_MANY_SCRIPTS: {limit} codemode tasks are running. Wait for one to finish or kill one, then retry."
                ));
            }
            registry.pending_scripts += 1;
        }
        registry.pending += 1;
        Ok((
            self.next_id(),
            Pending {
                registry: &self.registry,
                counted: true,
                script,
            },
        ))
    }

    /// Ends the task when `killAfterMs` elapses. The deadline belongs to the task, not to the
    /// request that started it: it survives that request's cancellation and every later wait,
    /// and bounds kept processes.
    fn arm_deadline(&self, task: &Arc<Task>, kill_after_ms: Option<u64>) {
        let Some(limit) = kill_after_ms.filter(|&limit| limit > 0) else {
            return;
        };
        let Some(deadline) = tokio::time::Instant::now().checked_add(Duration::from_millis(limit))
        else {
            return;
        };
        let (expiring, mut done) = (Arc::clone(task), task.tree_done());
        self.handle.spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => expiring.expire(),
                _ = done.wait_for(|done| *done) => {}
            }
        });
    }

    pub fn start(
        self: &Arc<Self>,
        args: StartArgs,
        session: &Session,
        key: String,
    ) -> Result<(Started, Option<ProbeWarning>), String> {
        let mut queued_guard = args.queued_guard;
        let kill_after_ms = args.kill_after_ms;
        // Admission holds a place against the running limit, so the bash lookup, the file
        // creation, and the spawn below all run without the registry lock.
        let (pipe_stdin, initial_input, keep_stdin_open) = if args.pty {
            (true, None, true)
        } else {
            let close_after = args.eof != Some(false);
            match args.input {
                None if close_after => (false, None, false),
                None => (true, None, true),
                Some(text) if text.is_empty() && close_after => (true, None, false),
                Some(text) if text.is_empty() => (true, None, true),
                Some(text) => (true, Some((text.into_bytes(), close_after)), !close_after),
            }
        };
        let need_input_worker = args.pty || keep_stdin_open || initial_input.is_some();
        let (id, mut pending) = self.admit(false)?;
        let bash = args.launch.bash()?;
        let (base_env, _used_snapshot, probe_warning) = args.launch.task_env(&bash, args.login);
        let workers = Workers::new(need_input_worker)
            .map_err(|error| format!("Cannot prepare the task workers: {error}."))?;
        let log_path = self.dir.join(format!("{id}.log"));
        let log = private_file(&log_path)
            .map_err(|error| format!("Cannot create the task log: {error}."))?;
        let script_path = self.dir.join(format!("{id}.sh"));
        // A Windows ConPTY root can deadlock inside its own exit and never terminate. Its script
        // records its status as it ends, so the task can end anyway; a command that replaces the
        // trap or execs keeps the plain wait.
        let exit_status = (args.pty && cfg!(windows)).then(|| self.dir.join(format!("{id}.exit")));
        let command = match exit_status {
            Some(_) => format!("{EXIT_TRAP}{}", args.command),
            None => args.command.clone(),
        };
        let script = if command.len() > process::SCRIPT_THRESHOLD_BYTES {
            if let Err(error) =
                private_file(&script_path).and_then(|mut file| file.write_all(command.as_bytes()))
            {
                let _ = std::fs::remove_file(&log_path);
                let _ = std::fs::remove_file(&script_path);
                return Err(format!("Cannot write the command script: {error}."));
            }
            Some(script_path.as_path())
        } else {
            None
        };
        let launch = Launch {
            bash: &bash,
            command: &command,
            script,
            cwd: &args.cwd,
            base_env,
            #[cfg(windows)]
            used_snapshot: _used_snapshot,
            login: args.login,
            pty: args.pty,
            pipe_stdin,
            cols: args.cols,
            rows: args.rows,
            exit_status: exit_status.as_deref(),
        };
        let mut spawned = match process::spawn(&launch) {
            Ok(spawned) => spawned,
            Err(error) => {
                let _ = std::fs::remove_file(&log_path);
                let _ = std::fs::remove_file(&script_path);
                return Err(format!("Cannot start the command: {error}."));
            }
        };
        if !need_input_worker {
            // Explicit empty pipe (`input: ""`) closes its write end immediately here.
            drop(spawned.input.take());
        }
        let (input_tx, input_rx) = mpsc::channel();
        let mut initial_queued = 0usize;
        if let Some((bytes, eof)) = initial_input {
            initial_queued = bytes.len();
            input_tx
                .send(Input {
                    bytes,
                    eof,
                    initial: true,
                    done: None,
                    _input_owner: args.input_owner.clone(),
                })
                .expect("input_rx is held in this frame");
        }
        let writer_done = WriterDone::new(!need_input_worker);
        let mut initial_transcript_file = None;
        let terminal = args.pty.then(|| {
            let mut spool = Spool {
                bytes: 0,
                lines: 0,
                pending: 0,
                stopped: None,
            };
            if args.encoding == encoding_rs::UTF_8 {
                match private_file(&self.dir.join(format!("{id}.transcript.txt"))) {
                    Ok(file) => initial_transcript_file = Some(file),
                    Err(error) => spool.stopped = Some(format!("it cannot be created: {error}")),
                }
            }
            Mutex::new(Pty {
                live: Live::at(args.cols, args.rows),
                spool,
            })
        });
        let task = Arc::new(Task {
            id,
            command: args.command,
            pty: args.pty,
            log_path,
            encoding: args.encoding,
            keep: args.keep,
            owner: args.owner,
            session_id: Some(session.id.clone()),
            started_by: args.started_by,
            parent_task_id: args.parent_task_id,
            started: Instant::now(),
            root: Root::Process(spawned.tree),
            tree_done: watch::Sender::new(false),
            tree_ended_at: OnceLock::new(),
            kill_error: Mutex::new(None),
            kill_requested: AtomicBool::new(false),
            expired: AtomicBool::new(false),
            out: Mutex::default(),
            end: Mutex::new(None),
            done: watch::Sender::new(false),
            activity: watch::Sender::new(()),
            input: Mutex::new(keep_stdin_open.then(|| input_tx.clone())),
            writer_done: Arc::clone(&writer_done),
            input_warning: Mutex::new(None),
            input_error: Mutex::new(None),
            queued: AtomicUsize::new(initial_queued),
            input_gate: tokio::sync::Mutex::new(()),
            terminal,
            transcript_file: Mutex::new(initial_transcript_file),
            size: Mutex::new((args.cols, args.rows)),
            pty_control: Mutex::new(spawned.pty),
            size_error: Mutex::new(None),
        });
        let mut registry = lock(&self.registry);
        let entry = session.entry(&task);
        let reader = entry.start_reader(key);
        registry.tasks.push(Arc::clone(&task));
        if let Some(guard) = queued_guard.as_mut() {
            guard.disarm();
        }
        pending.release(&mut registry);
        let stopping = registry.stopping;
        drop(registry);
        if stopping {
            // The server began stopping while this tree was created: it ends now, and it joins
            // the registry anyway so the capture, input, and waiter threads clean up as usual.
            task.kill_requested.store(true, Ordering::SeqCst);
            let _ = task.root.kill();
        }
        self.arm_deadline(&task, kill_after_ms);

        let (eof_tx, eof_rx) = mpsc::channel::<()>();
        let (chunks, received) = mpsc::sync_channel(1);
        // A prepared worker waits for exactly one job and cannot fail before receiving it.
        workers
            .output
            .reader
            .send(ReadWork {
                output: spawned.output,
                chunks,
                pty: args.pty,
            })
            .unwrap_or_else(|_| unreachable!("prepared output worker stopped"));
        workers
            .output
            .capture
            .send(CaptureWork {
                shared: Arc::clone(self),
                task: Arc::clone(&task),
                received,
                log,
                answer: args.pty.then(|| input_tx.clone()),
                eof: eof_tx,
            })
            .unwrap_or_else(|_| unreachable!("prepared capture worker stopped"));
        drop(input_tx);
        if let Some(input_worker) = workers.input {
            input_worker
                .send(InputWork {
                    task: Arc::clone(&task),
                    input: spawned.input.expect("input worker requires piped stdin"),
                    received: input_rx,
                    done_signal: writer_done,
                })
                .unwrap_or_else(|_| unreachable!("prepared input worker stopped"));
        }
        workers
            .waiter
            .send(WaitWork {
                shared: Arc::clone(self),
                task: Arc::clone(&task),
                child: spawned.child,
                script: script_path,
                exit_status,
                eof: eof_rx,
                ender: workers.ender,
            })
            .unwrap_or_else(|_| unreachable!("prepared waiter worker stopped"));
        Ok(((task, entry, reader), probe_warning))
    }

    pub(crate) fn start_script(
        self: &Arc<Self>,
        args: ScriptArgs,
        session: &Arc<Session>,
        key: String,
    ) -> Result<Started, String> {
        let (id, mut pending) = self.admit(true)?;
        let (script, worker) = codemode::prepare(self.handle.clone())?;
        let output = OutputWorkers::new()
            .map_err(|error| format!("Cannot prepare the script workers: {error}."))?;
        let (pipe, writer) =
            std::io::pipe().map_err(|error| format!("Cannot create the script pipe: {error}."))?;
        let log_path = self.dir.join(format!("{id}.log"));
        let log = private_file(&log_path)
            .map_err(|error| format!("Cannot create the script log: {error}."))?;
        let task = Arc::new(Task {
            id,
            command: format!("[codemode] {}", args.code),
            pty: false,
            log_path,
            encoding: encoding_rs::UTF_8,
            keep: false,
            owner: session.label.clone(),
            session_id: Some(session.id.clone()),
            started_by: args.call_id.map(|id| (session.id.clone(), id)),
            parent_task_id: None,
            started: Instant::now(),
            root: Root::Script(script),
            tree_done: watch::Sender::new(false),
            tree_ended_at: OnceLock::new(),
            kill_error: Mutex::new(None),
            kill_requested: AtomicBool::new(false),
            expired: AtomicBool::new(false),
            out: Mutex::default(),
            end: Mutex::new(None),
            done: watch::Sender::new(false),
            activity: watch::Sender::new(()),
            input: Mutex::new(None),
            writer_done: WriterDone::new(true),
            input_warning: Mutex::new(None),
            input_error: Mutex::new(None),
            queued: AtomicUsize::new(0),
            input_gate: tokio::sync::Mutex::new(()),
            terminal: None,
            transcript_file: Mutex::new(None),
            size: Mutex::new((0, 0)),
            pty_control: Mutex::new(None),
            size_error: Mutex::new(None),
        });
        let mut registry = lock(&self.registry);
        let entry = session.entry(&task);
        let reader = entry.start_reader(key);
        registry.tasks.push(Arc::clone(&task));
        pending.release(&mut registry);
        let stopping = registry.stopping;
        drop(registry);
        if stopping {
            let _ = task.kill();
        }
        self.arm_deadline(&task, args.kill_after_ms);
        let (eof, captured) = mpsc::channel();
        let (chunks, received) = mpsc::sync_channel(1);
        output
            .reader
            .send(ReadWork {
                output: Box::new(pipe),
                chunks,
                pty: false,
            })
            .unwrap_or_else(|_| unreachable!("prepared output worker stopped"));
        output
            .capture
            .send(CaptureWork {
                shared: Arc::clone(self),
                task: Arc::clone(&task),
                received,
                log,
                answer: None,
                eof,
            })
            .unwrap_or_else(|_| unreachable!("prepared capture worker stopped"));
        worker.start(codemode::Work {
            service: args.service,
            session: Arc::clone(session),
            launch: args.launch,
            transfer: args.transfer,
            call_id: args.call_id,
            code: args.code,
            writer,
            task: Arc::clone(&task),
            tasks: Arc::clone(self),
            captured,
            input_owner: args.input_owner,
        });
        Ok((task, entry, reader))
    }

    pub(crate) fn finish_script(
        &self,
        task: &Task,
        exit_code: Option<i32>,
        captured: mpsc::Receiver<()>,
    ) {
        let output_open = drain(task, &captured, false);
        let mut registry = lock(&self.registry);
        let killed = task.kill_requested.load(Ordering::SeqCst);
        let _ = task.tree_ended_at.set(Instant::now());
        task.tree_done.send_replace(true);
        *lock(&task.end) = Some(Final {
            exit_code: if killed { None } else { exit_code },
            killed,
            expired: task.expired.load(Ordering::SeqCst),
            forced_end: false,
            elapsed: task.started.elapsed(),
            output_open,
            leftovers: Vec::new(),
            kept: false,
        });
        self.retire_finished(&mut registry);
        drop(registry);
        task.done.send_replace(true);
        self.emit_task_exited(task);
    }

    /// Saves an attachment with a monotonic 1-based task-local index under the 4 GiB and 1024-entry caps.
    pub(crate) fn store_attachment(
        self: &Arc<Self>,
        task: &Task,
        kind: MediaKind,
        mime: &str,
        bytes: &[u8],
    ) -> Result<AttachmentMeta, String> {
        let index = {
            let mut out = lock(&task.out);
            if out.evicted {
                return Err("The task log and attachments have been evicted.".into());
            }
            if out.attachments.len() + out.pending_attachments >= MAX_TASK_ATTACHMENTS {
                return Err(format!(
                    "A task may store at most {MAX_TASK_ATTACHMENTS} attachments."
                ));
            }
            out.pending_attachments += 1;
            out.next_attachment_index += 1;
            out.next_attachment_index
        };
        let release_pending = || {
            let mut out = lock(&task.out);
            out.pending_attachments = out.pending_attachments.saturating_sub(1);
        };
        let size = bytes.len() as u64;
        if !self.reserve(size) {
            release_pending();
            return Err("Attachment exceeds the 4 GiB total task output limit.".into());
        }
        let path = task.attachment_path(index, mime);
        let mut file = match private_file(&path) {
            Ok(file) => file,
            Err(error) => {
                self.log_bytes.fetch_sub(size, Ordering::SeqCst);
                release_pending();
                return Err(format!("Cannot create the attachment: {error}."));
            }
        };
        if let Err(error) = file.write_all(bytes) {
            drop(file);
            release_pending();
            self.reclaim_file(path, size);
            return Err(format!("Cannot save the attachment: {error}."));
        }
        drop(file);
        let meta = AttachmentMeta {
            index,
            kind,
            mime: mime.to_owned(),
            path: path.clone(),
            size,
            base64_len: bytes.len().div_ceil(3) * 4,
        };
        let slot = Arc::new(AttachmentSlot {
            tasks: Arc::downgrade(self),
            meta: meta.clone(),
            state: Mutex::new(AttachmentSlotState {
                in_task: true,
                leases: 0,
                reclaimed: false,
            }),
        });
        let mut out = lock(&task.out);
        out.pending_attachments = out.pending_attachments.saturating_sub(1);
        if out.evicted {
            drop(out);
            self.reclaim_file(path, size);
            return Err("The task log and attachments have been evicted.".into());
        }
        let pos = out
            .attachments
            .partition_point(|existing| existing.meta.index < index);
        out.attachments.insert(pos, slot);
        drop(out);
        task.activity.send_replace(());
        Ok(meta)
    }

    fn unlink_file(&self, path: &Path) -> bool {
        #[cfg(test)]
        if let Some(hook) = lock(&self.remove_hook).clone() {
            return match hook(path) {
                Ok(()) => true,
                Err(error) => error.kind() == std::io::ErrorKind::NotFound,
            };
        }
        match std::fs::remove_file(path) {
            Ok(()) => true,
            Err(error) => error.kind() == std::io::ErrorKind::NotFound,
        }
    }

    /// Refunds `bytes` only when `path` is confirmed gone; otherwise keeps `bytes` charged for retry.
    fn reclaim_file(&self, path: PathBuf, bytes: u64) {
        if self.unlink_file(&path) {
            if bytes > 0 {
                self.log_bytes.fetch_sub(bytes, Ordering::SeqCst);
            }
        } else {
            lock(&self.pending_deletes).push(PendingDelete { path, bytes });
        }
    }

    /// Retries deferred unlinks, refunding each file's bytes at most once when deletion succeeds.
    pub(crate) fn retry_pending_deletes(&self) {
        let _gate = lock(&self.delete_retry_gate);
        let pending = std::mem::take(&mut *lock(&self.pending_deletes));
        if pending.is_empty() {
            return;
        }
        let mut still_pending = Vec::new();
        for item in pending {
            if self.unlink_file(&item.path) {
                if item.bytes > 0 {
                    self.log_bytes.fetch_sub(item.bytes, Ordering::SeqCst);
                }
            } else {
                still_pending.push(item);
            }
        }
        if !still_pending.is_empty() {
            lock(&self.pending_deletes).extend(still_pending);
        }
    }

    /// Keeps the [`Limits::max_finished`] most recently finished tasks and deletes the logs of
    /// older ones. Runs at each completion under the registry guard that shows its end, so the
    /// task that just finished always stays and no client sees the cap exceeded.
    fn retire_finished(&self, registry: &mut Registry) {
        // Root exit alone cannot retire a task whose ender has not confirmed completion.
        let mut finished: Vec<(Instant, String)> = registry
            .tasks
            .iter()
            .filter(|task| task.is_finished())
            .filter_map(|task| task.ended_at().map(|at| (at, task.id.clone())))
            .collect();
        let keep = self.limits.max_finished;
        if finished.len() <= keep {
            return;
        }
        finished.sort();
        let victims: Vec<String> = finished[..finished.len() - keep]
            .iter()
            .map(|(_, id)| id.clone())
            .collect();
        registry.tasks.retain(|task| {
            if !victims.contains(&task.id) {
                return true;
            }
            self.evict_log(task);
            false
        });
    }

    /// Removes every finished task that ended at least `finished_ttl` before `now`, deleting its
    /// log as `retire_finished` does. Pending or unconfirmed tree completion keeps the task.
    pub fn expire_finished(&self, now: Instant) {
        self.retry_pending_deletes();
        let Some(ttl) = self.limits.finished_ttl else {
            return;
        };
        // The victims leave the registry under the lock; their logs are deleted after it, so no
        // file work happens under the registry.
        let expired: Vec<Arc<Task>> = {
            let mut registry = lock(&self.registry);
            let mut expired = Vec::new();
            registry.tasks.retain(|task| {
                let gone = task.is_finished()
                    && task
                        .ended_at()
                        .is_some_and(|ended| now.saturating_duration_since(ended) >= ttl);
                if gone {
                    expired.push(Arc::clone(task));
                }
                !gone
            });
            expired
        };
        for task in &expired {
            self.evict_log(task);
        }
    }

    /// Whether the registry holds nothing at all: no task and no start in flight.
    pub fn idle(&self) -> bool {
        let registry = lock(&self.registry);
        registry.tasks.is_empty() && registry.pending == 0
    }

    /// How many starts are admitted but not yet inserted.
    pub fn pending(&self) -> usize {
        lock(&self.registry).pending
    }

    pub(crate) fn evict_log(&self, task: &Task) {
        let (written, transcript, has_transcript, attachments) = {
            let mut out = lock(&task.out);
            if out.evicted {
                return;
            }
            out.evicted = true;
            let written = out.written + out.log_tail;
            let transcript = out.transcript;
            let has_transcript = task.pty || transcript > 0;
            let attachments = std::mem::take(&mut out.attachments);
            (written, transcript, has_transcript, attachments)
        };
        // Drop our open handle outside `task.terminal` so eviction from `capture` or `resize`
        // cannot self-deadlock on the non-reentrant terminal mutex.
        let closed_transcript = lock(&task.transcript_file).take();
        drop(closed_transcript);

        self.reclaim_file(task.log_path.clone(), written);
        if has_transcript {
            self.reclaim_file(task.transcript_path(), transcript);
        }
        for slot in attachments {
            slot.detach_from_task();
        }
    }

    /// Reserves log space under the total limit, evicting the oldest finished logs if needed.
    /// A tree whose completion is pending or unconfirmed may still write to its log.
    fn reserve(&self, bytes: u64) -> bool {
        if self.log_bytes.fetch_add(bytes, Ordering::SeqCst) + bytes <= self.log_limit {
            return true;
        }
        self.retry_pending_deletes();
        if self.log_bytes.load(Ordering::SeqCst) <= self.log_limit {
            return true;
        }
        let mut candidates: Vec<(Instant, Arc<Task>)> = lock(&self.registry)
            .tasks
            .iter()
            .filter(|task| task.is_finished())
            .filter_map(|task| task.ended_at().map(|at| (at, Arc::clone(task))))
            .collect();
        // The log that ended earliest goes first, as retire_finished removes it: registry order
        // is start order, and a task that started first can end last.
        candidates
            .sort_by(|(a, first), (b, second)| a.cmp(b).then_with(|| first.id.cmp(&second.id)));
        for (_, task) in candidates {
            if self.log_bytes.load(Ordering::SeqCst) <= self.log_limit {
                break;
            }
            self.evict_log(&task);
        }
        if self.log_bytes.load(Ordering::SeqCst) <= self.log_limit {
            return true;
        }
        self.log_bytes.fetch_sub(bytes, Ordering::SeqCst);
        false
    }

    /// Counts `bytes` written to a task's transcript file; returns `false` without refunding when
    /// the task was concurrently evicted so the caller closes the handle and runs `reclaim_file`.
    fn hold(&self, task: &Task, bytes: u64) -> bool {
        let mut out = lock(&task.out);
        if out.evicted {
            false
        } else {
            out.transcript += bytes;
            true
        }
    }

    /// Gives back `bytes` a task's transcript file no longer holds.
    fn release(&self, task: &Task, bytes: u64) {
        let mut out = lock(&task.out);
        if !out.evicted {
            out.transcript -= bytes;
            self.log_bytes.fetch_sub(bytes, Ordering::SeqCst);
        }
    }

    /// Kills every tree, kept processes included, and removes the log directory.
    pub fn shutdown(&self) {
        let mut registry = lock(&self.registry);
        // Admission closes first: no start is admitted from here on, and one already admitted
        // still inserts its task and then ends its tree.
        registry.stopping = true;
        for task in registry.tasks.iter() {
            if task.is_running() {
                task.kill_requested.store(true, Ordering::SeqCst);
            }
            let _ = task.root.kill();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
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
                    snapshot.held = match &task.root {
                        Root::Process(tree) => members(tree)?.unwrap_or_default(),
                        Root::Script(_) => Vec::new(),
                    };
                    // The current PID vector fits the admitted member workspace; the
                    // previous reported vector may be longer and is only borrowed.
                    if snapshot.kill_error.is_none()
                        && entry.keeps_reported_eq(&pids(&snapshot.held))
                    {
                        continue;
                    }
                    (2, Notice::Keeps)
                }
                _ => continue,
            };
            entries.push((rank, notice, task, snapshot));
        }
        if entries.is_empty() && others == 0 {
            return Ok((None, claim));
        }
        entries.sort_by_key(|(rank, ..)| *rank);
        // Counts/labels are fixed-size metadata. Only three handles enter formatting;
        // process names were admitted while querying the relevant held trees.
        reserve(256)?;
        for (_, _, task, _) in entries.iter().take(3) {
            reserve(task.id.capacity().saturating_add(128))?;
        }
        let count = |rank: u8| entries.iter().filter(|entry| entry.0 == rank).count();
        let failed = count(0);
        let running = entries
            .iter()
            .filter(|entry| entry.1 == Notice::Running)
            .count();
        let keeping = count(2) - running;
        let finished = entries.len() - count(2);
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
                        (Notice::Keeps, _) => format!("{id} keeps {}", brief(&snapshot.held)),
                        (Notice::TreeEnded, _) => format!("{id} kept processes ended"),
                        (_, code) => {
                            let root = match code {
                                Some(code) if snapshot.state == "exited" => {
                                    format!("exited {code}")
                                }
                                _ if snapshot.expired => "killed by killAfterMs".into(),
                                _ => "killed".into(),
                            };
                            let tree = if snapshot.tree_ended {
                                " and its kept processes ended"
                            } else {
                                ""
                            };
                            format!("{id} {root}{tree}")
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

/// A task and the snapshot a result showed of it.
pub type Shown = (Arc<Task>, Snapshot);
pub type BackgroundNotice<'a> = (
    Option<(String, Vec<Shown>)>,
    crate::session::OutsideClaim<'a>,
);

/// The sorted PIDs of `held`.
pub(crate) fn pids(held: &[Member]) -> Vec<u32> {
    let mut pids: Vec<u32> = held.iter().map(|member| member.pid).collect();
    pids.sort_unstable();
    pids
}

/// The first held process and how many more: `adb.exe +2`.
fn brief(held: &[Member]) -> String {
    match held {
        [] => "processes".into(),
        [one] => one.name.clone(),
        [first, rest @ ..] => format!("{} +{}", first.name, rest.len()),
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

fn private_file(path: &Path) -> std::io::Result<File> {
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options.open(path)
}

/// Log directories are `<prefix><pid>-<random>`. On Unix the prefix carries the user ID, so servers of
/// different users sharing /tmp neither collide nor examine each other's directories.
fn dir_prefix() -> String {
    #[cfg(unix)]
    // SAFETY: getuid has no preconditions and cannot fail.
    return format!("fastexec-{}-", unsafe { libc::getuid() });
    #[cfg(windows)]
    return "fastexec-".to_string();
}

/// Resolve the parent once and require every ancestor to resist replacement by other users.
/// Linux sticky directories are safe only if the directory owner is trusted as well.
#[cfg(target_os = "linux")]
fn safe_sweep_parent(temp: &Path) -> Option<PathBuf> {
    use std::os::unix::fs::MetadataExt;
    let resolved = std::fs::canonicalize(temp).ok()?;
    // SAFETY: geteuid has no preconditions.
    let user = unsafe { libc::geteuid() };
    for ancestor in resolved.ancestors() {
        let meta = std::fs::symlink_metadata(ancestor).ok()?;
        if !meta.is_dir()
            || (meta.uid() != user && meta.uid() != 0)
            || (meta.mode() & 0o022 != 0 && meta.mode() & libc::S_ISVTX == 0)
        {
            return None;
        }
    }
    Some(resolved)
}

/// Remove only private log directories of dead servers when every parent protects the
/// checked path from replacement. Unverifiable entries are retained rather than deleted.
fn remove_stale_dirs(temp: &Path) {
    // On macOS ACLs and Windows DACLs can grant rights not visible in Unix mode bits.
    // Retain old logs there until deletion can be tied safely to a verified handle.
    #[cfg(not(target_os = "linux"))]
    let _ = temp;
    #[cfg(target_os = "linux")]
    let Some(temp) = safe_sweep_parent(temp) else {
        return;
    };
    #[cfg(target_os = "linux")]
    let Ok(entries) = std::fs::read_dir(temp) else {
        return;
    };
    #[cfg(target_os = "linux")]
    let prefix = dir_prefix();
    #[cfg(target_os = "linux")]
    for entry in entries.flatten() {
        let name = entry.file_name();
        let pid = name
            .to_str()
            .and_then(|name| name.strip_prefix(prefix.as_str()))
            .and_then(|tail| {
                let (pid, nonce) = tail.split_once('-')?;
                if nonce.len() != 12 || !nonce.bytes().all(|byte| ALPHABET.contains(&byte)) {
                    return None;
                }
                pid.parse().ok()
            });
        if pid.is_some_and(|pid| pid != std::process::id() && !process::is_alive(pid))
            && crate::platform::runtime::create_private_unix_check(&entry.path()).is_ok()
        {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}
