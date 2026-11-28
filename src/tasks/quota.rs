//! Task admission limits, shared log byte quota, and deferred file reclamation.

use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

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

pub(crate) const MAX_TASK_ATTACHMENTS: usize = 1024;
const TOTAL_LOG_LIMIT: u64 = 4 << 30;

pub(super) struct PendingDelete {
    pub(super) path: PathBuf,
    pub(super) bytes: u64,
}

#[cfg(test)]
pub(crate) type RemoveHook = Arc<dyn Fn(&Path) -> std::io::Result<()> + Send + Sync>;
#[cfg(test)]
pub(super) type LogWriteHook = Arc<dyn Fn(&mut File, &[u8]) -> std::io::Result<()> + Send + Sync>;
#[cfg(test)]
pub(super) type LogTruncateHook = Arc<dyn Fn(&File, u64) -> std::io::Result<()> + Send + Sync>;

/// Narrow eviction callback invoked when `LogQuota::reserve` needs space under `log_limit`.
pub(super) trait LogEvictor: Send + Sync {
    fn evict_oldest_logs(&self);
}

pub(super) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(super) fn private_file(path: &Path) -> std::io::Result<File> {
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options.open(path)
}

/// Tracks total on-disk bytes across task logs, transcripts, and attachments.
pub(super) struct LogQuota {
    pub(super) log_bytes: AtomicU64,
    /// [`TOTAL_LOG_LIMIT`]; debug builds take a smaller one from
    /// `FASTEXEC_TEST_TOTAL_LOG_LIMIT`, so tests reach it without writing 4 GiB.
    pub(super) log_limit: u64,
    /// Files whose unlink failed keep their byte charge here until a later unlink succeeds.
    pub(super) pending_deletes: Mutex<Vec<PendingDelete>>,
    delete_retry_gate: Mutex<()>,
    #[cfg(test)]
    pub(super) remove_hook: Mutex<Option<RemoveHook>>,
    #[cfg(test)]
    pub(super) log_write_hook: Mutex<Option<LogWriteHook>>,
    #[cfg(test)]
    pub(super) log_truncate_hook: Mutex<Option<LogTruncateHook>>,
}

impl LogQuota {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            log_bytes: AtomicU64::new(0),
            log_limit: std::env::var("FASTEXEC_TEST_TOTAL_LOG_LIMIT")
                .ok()
                .and_then(|limit| limit.parse().ok())
                .filter(|limit| cfg!(debug_assertions) && (1..=TOTAL_LOG_LIMIT).contains(limit))
                .unwrap_or(TOTAL_LOG_LIMIT),
            pending_deletes: Mutex::default(),
            delete_retry_gate: Mutex::new(()),
            #[cfg(test)]
            remove_hook: Mutex::new(None),
            #[cfg(test)]
            log_write_hook: Mutex::new(None),
            #[cfg(test)]
            log_truncate_hook: Mutex::new(None),
        })
    }

    #[cfg(test)]
    pub(super) fn set_log_limit(&mut self, limit: u64) {
        self.log_limit = limit;
    }

    #[cfg(test)]
    pub(super) fn set_remove_hook(&self, hook: Option<RemoveHook>) {
        *lock(&self.remove_hook) = hook;
    }

    pub(super) fn write_log(&self, file: &mut File, bytes: &[u8]) -> std::io::Result<()> {
        #[cfg(test)]
        if let Some(hook) = lock(&self.log_write_hook).clone() {
            return hook(file, bytes);
        }
        file.write_all(bytes)
    }

    pub(super) fn truncate_log(&self, file: &File, bytes: u64) -> std::io::Result<()> {
        #[cfg(test)]
        if let Some(hook) = lock(&self.log_truncate_hook).clone() {
            return hook(file, bytes);
        }
        file.set_len(bytes)
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
    pub(super) fn reclaim_file(&self, path: PathBuf, bytes: u64) {
        if self.unlink_file(&path) {
            if bytes > 0 {
                self.log_bytes.fetch_sub(bytes, Ordering::SeqCst);
            }
        } else {
            lock(&self.pending_deletes).push(PendingDelete { path, bytes });
        }
    }

    /// Retries deferred unlinks, refunding each file's bytes at most once when deletion succeeds.
    pub(super) fn retry_pending_deletes(&self) {
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

    /// Reserves log space under the total limit, evicting the oldest finished logs if needed.
    /// A tree whose completion is pending or unconfirmed may still write to its log.
    pub(super) fn reserve(&self, bytes: u64, evictor: &dyn LogEvictor) -> bool {
        if self.log_bytes.fetch_add(bytes, Ordering::SeqCst) + bytes <= self.log_limit {
            return true;
        }
        self.retry_pending_deletes();
        if self.log_bytes.load(Ordering::SeqCst) <= self.log_limit {
            return true;
        }
        evictor.evict_oldest_logs();
        if self.log_bytes.load(Ordering::SeqCst) <= self.log_limit {
            return true;
        }
        self.log_bytes.fetch_sub(bytes, Ordering::SeqCst);
        false
    }
}
