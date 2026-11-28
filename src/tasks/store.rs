//! Task registry store, eviction, retirement, attachment persistence, and event emission.

#[cfg(test)]
use super::quota::RemoveHook;
use super::quota::{Limits, LogEvictor, LogQuota, MAX_TASK_ATTACHMENTS, lock, private_file};
use super::state::{
    AttachmentMeta, AttachmentSlot, AttachmentSlotState, ReadState, Task, TaskError, Transcript,
};
use crate::id::{base32, random_chars};
use crate::lease::BytePool;
use crate::media::MediaKind;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::runtime::Handle;

pub struct Tasks {
    pub(super) dir: PathBuf,
    /// Arms `killAfterMs`: the deadline belongs to the Core, not to the starting request.
    pub(super) handle: Handle,
    /// This server's ID epoch: 40 bits from the OS random source as 8 Crockford base32
    /// characters. Every task ID starts with it, so an ID from another server is told apart.
    pub(super) epoch: String,
    /// The counter value of the next ID; IDs never repeat within an epoch.
    pub(super) next: AtomicU64,
    pub(super) registry: Mutex<Registry>,
    pub(super) limits: Limits,
    pub(super) quota: Arc<LogQuota>,
    pub(super) event_bus: Arc<crate::events::EventBus>,
}

#[derive(Default)]
pub(super) struct Registry {
    pub(super) tasks: Vec<Arc<Task>>,
    /// Starts admitted but not yet inserted; they count against [`Limits::max_running`].
    pub(super) pending: usize,
    pub(super) pending_scripts: usize,
    /// Set by [`Tasks::shutdown`]: no start is admitted from then on.
    pub(super) stopping: bool,
}

/// An epoch and a counter value as a task ID.
pub(super) fn task_id(epoch: &str, number: u64) -> String {
    format!("{epoch}-{}", base32(number as u128, 1))
}

/// A fresh 40-bit epoch from the OS random source: 8 Crockford base32 characters.
pub(super) fn new_epoch() -> std::io::Result<String> {
    random_chars(8)
}

/// The process-backed tests serialize their shared fixtures.
#[cfg(test)]
pub(crate) static ONE_TASKS: Mutex<()> = Mutex::new(());

impl LogEvictor for Tasks {
    fn evict_oldest_logs(&self) {
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
            if self.quota.log_bytes.load(Ordering::SeqCst) <= self.quota.log_limit {
                break;
            }
            self.evict_log(&task);
        }
    }
}

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
        let event_bus = Arc::new(crate::events::EventBus::new()?);
        crate::platform::runtime::create_private(&dir)?;
        let quota = LogQuota::new();
        Ok(Arc::new(Self {
            dir,
            handle,
            epoch,
            next: AtomicU64::new(1),
            registry: Mutex::default(),
            limits,
            quota,
            event_bus,
        }))
    }

    #[cfg(test)]
    pub(crate) fn set_log_limit(&mut self, limit: u64) {
        Arc::get_mut(&mut self.quota)
            .expect("set_log_limit requires unique LogQuota ownership")
            .set_log_limit(limit);
    }

    #[cfg(test)]
    pub(crate) fn set_remove_hook(&self, hook: Option<RemoveHook>) {
        self.quota.set_remove_hook(hook);
    }

    #[cfg(test)]
    pub(super) fn reserve(&self, bytes: u64) -> bool {
        self.quota.reserve(bytes, self)
    }

    #[cfg(test)]
    pub(super) fn reclaim_file(&self, path: PathBuf, bytes: u64) {
        self.quota.reclaim_file(path, bytes);
    }

    #[cfg(test)]
    pub(super) fn hold(&self, task: &Task, bytes: u64) -> bool {
        task.hold_transcript(bytes)
    }

    pub fn event_bus(&self) -> &Arc<crate::events::EventBus> {
        &self.event_bus
    }

    pub(super) fn emit_task_exited(&self, task: &Task) {
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
        let _ = self.event_bus.publish_task_exited(input);
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
    pub(super) fn next_id(&self) -> String {
        let number = self.next.fetch_add(1, Ordering::Relaxed);
        task_id(&self.epoch, number)
    }

    /// Every task, newest first.
    pub fn all(&self) -> Vec<Arc<Task>> {
        lock(&self.registry).tasks.iter().rev().cloned().collect()
    }

    pub(crate) fn runtime_handle(&self) -> &Handle {
        &self.handle
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
        if !self.quota.reserve(size, self.as_ref()) {
            release_pending();
            return Err("Attachment exceeds the 4 GiB total task output limit.".into());
        }
        let path = task.attachment_path(index, mime);
        let mut file = match private_file(&path) {
            Ok(file) => file,
            Err(error) => {
                self.quota.log_bytes.fetch_sub(size, Ordering::SeqCst);
                release_pending();
                return Err(format!("Cannot create the attachment: {error}."));
            }
        };
        if let Err(error) = file.write_all(bytes) {
            drop(file);
            release_pending();
            self.quota.reclaim_file(path, size);
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
            quota: Arc::downgrade(&self.quota),
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
            self.quota.reclaim_file(path, size);
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

    /// Retries deferred unlinks, refunding each file's bytes at most once when deletion succeeds.
    pub(crate) fn retry_pending_deletes(&self) {
        self.quota.retry_pending_deletes();
    }

    /// Keeps the [`Limits::max_finished`] most recently finished tasks and returns older ones so
    /// callers evict their logs after releasing the registry lock.
    pub(super) fn retire_finished(&self, registry: &mut Registry) -> Vec<Arc<Task>> {
        // Root exit alone cannot retire a task whose ender has not confirmed completion.
        let mut finished: Vec<(Instant, String)> = registry
            .tasks
            .iter()
            .filter(|task| task.is_finished())
            .filter_map(|task| task.ended_at().map(|at| (at, task.id.clone())))
            .collect();
        let keep = self.limits.max_finished;
        if finished.len() <= keep {
            return Vec::new();
        }
        finished.sort();
        let victims: Vec<String> = finished[..finished.len() - keep]
            .iter()
            .map(|(_, id)| id.clone())
            .collect();
        let mut retired = Vec::with_capacity(victims.len());
        registry.tasks.retain(|task| {
            if !victims.contains(&task.id) {
                return true;
            }
            retired.push(Arc::clone(task));
            false
        });
        retired
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

        self.quota.reclaim_file(task.log_path.clone(), written);
        if has_transcript {
            self.quota.reclaim_file(task.transcript_path(), transcript);
        }
        for slot in attachments {
            slot.detach_from_task();
        }
    }
}

impl Task {
    /// The terminal is `cols` by `rows` from now on. Delegates to [`Self::resize_with_quota`].
    pub fn resize(&self, shared: &Tasks, cols: u16, rows: u16) -> Result<(), String> {
        self.resize_with_quota(&shared.quota, shared, cols, rows)
    }

    /// Reads pending and unseen final transcript lines. Delegates to [`Self::transcript_with_quota`].
    pub fn transcript(
        &self,
        shared: &Tasks,
        state: &ReadState,
        memory: &Arc<BytePool>,
    ) -> Result<Transcript, TaskError> {
        self.transcript_with_quota(&shared.quota, shared, state, memory)
    }
}

/// Log directories are `<prefix><pid>-<random>`. On Unix the prefix carries the user ID, so servers of
/// different users sharing /tmp neither collide nor examine each other's directories.
pub(super) fn dir_prefix() -> String {
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
pub(super) fn remove_stale_dirs(temp: &Path) {
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
                if nonce.len() != 12
                    || !nonce
                        .bytes()
                        .all(|byte| crate::id::ALPHABET.contains(&byte))
                {
                    return None;
                }
                pid.parse().ok()
            });
        if pid.is_some_and(|pid| {
            pid != std::process::id() && !crate::platform::runtime::is_alive(pid)
        }) && crate::platform::runtime::create_private_unix_check(&entry.path()).is_ok()
        {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}
