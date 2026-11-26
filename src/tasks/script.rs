//! Task-level control state and final call ledger for script tasks.

use crate::id::parse_handle;
use crate::lease::{BytePool, Lease};
use serde::Serialize;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::Thread;
use tokio_util::sync::CancellationToken;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CallInfo {
    pub tool: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
}

pub(crate) struct ReportSnapshot {
    pub cancelled: Vec<CallInfo>,
    pub started: Vec<String>,
    // Drops last so cloned strings release before refunding the pool reservation.
    pub _lease: Lease,
}

#[derive(Default)]
struct ScriptLedger {
    started: Vec<String>,
    started_ids: HashSet<String>,
    cancelled: Vec<CallInfo>,
}

impl ScriptLedger {
    fn snapshot_bytes(&self) -> usize {
        let cancelled = self.cancelled.iter().fold(0usize, |bytes, call| {
            bytes
                .saturating_add(call.tool.capacity())
                .saturating_add(call.action.as_ref().map_or(0, String::capacity))
                .saturating_add(call.task_id.as_ref().map_or(0, String::capacity))
        });
        let strings = self.started.iter().fold(cancelled, |bytes, handle| {
            bytes.saturating_add(handle.capacity())
        });
        // Per-item field names and state strings, plus the outer report keys.
        let fields = self.cancelled.len().saturating_add(self.started.len());
        // Raw snapshots, JSON values, a candidate copy, and String growth coexist.
        strings
            .saturating_add(fields.saturating_mul(32))
            .saturating_add(64)
            .saturating_mul(6)
    }
}

pub struct ScriptControl {
    vm_thread: Thread,
    stopped: AtomicBool,
    cancel: CancellationToken,
    queued_starts: AtomicUsize,
    cancelled_starts: AtomicUsize,
    ledger: Mutex<ScriptLedger>,
}

impl ScriptControl {
    pub(crate) fn new(vm_thread: Thread) -> Self {
        Self {
            vm_thread,
            stopped: AtomicBool::new(false),
            cancel: CancellationToken::new(),
            queued_starts: AtomicUsize::new(0),
            cancelled_starts: AtomicUsize::new(0),
            ledger: Mutex::new(ScriptLedger::default()),
        }
    }

    pub(crate) fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.cancel.cancel();
        self.vm_thread.unpark();
    }

    pub(crate) fn unpark(&self) {
        self.vm_thread.unpark();
    }

    #[inline]
    pub(crate) fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    pub(crate) fn cancel_token(&self) -> &CancellationToken {
        &self.cancel
    }

    pub(crate) fn list_counts(&self) -> (usize, usize) {
        (
            self.queued_starts.load(Ordering::Relaxed),
            self.cancelled_starts.load(Ordering::Relaxed),
        )
    }

    pub(crate) fn record_started(&self, handle: String) {
        let mut ledger = lock(&self.ledger);
        if let Ok((id, _)) = parse_handle(&handle) {
            ledger.started_ids.insert(id.to_owned());
        }
        if !ledger.started.contains(&handle) {
            ledger.started.push(handle);
        }
    }

    pub(crate) fn owns(&self, task_id: &str) -> bool {
        lock(&self.ledger).started_ids.contains(task_id)
    }

    pub(crate) fn set_cancelled_calls(&self, cancelled: Vec<CallInfo>) {
        lock(&self.ledger).cancelled = cancelled;
    }

    pub(crate) fn report(
        &self,
        memory: &Arc<BytePool>,
        control: Option<&Arc<BytePool>>,
    ) -> Option<ReportSnapshot> {
        let ledger = lock(&self.ledger);
        let bytes = ledger.snapshot_bytes();
        let lease = memory
            .try_acquire(bytes)
            .or_else(|| control.and_then(|pool| pool.try_acquire(bytes)))?;
        Some(ReportSnapshot {
            cancelled: ledger.cancelled.clone(),
            started: ledger.started.clone(),
            _lease: lease,
        })
    }
}

/// Counts an admitted `exec start` from validation until it enters `registry.tasks`,
/// moving unspawned cancelled starts into `cancelled_starts` on drop.
pub struct QueuedStartGuard {
    control: Arc<ScriptControl>,
    armed: bool,
}

impl QueuedStartGuard {
    pub(crate) fn new(control: &Arc<ScriptControl>) -> Self {
        control.queued_starts.fetch_add(1, Ordering::Relaxed);
        Self {
            control: Arc::clone(control),
            armed: true,
        }
    }

    pub(crate) fn disarm(&mut self) {
        if self.armed {
            self.armed = false;
            self.control.queued_starts.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl Drop for QueuedStartGuard {
    fn drop(&mut self) {
        if self.armed {
            self.armed = false;
            self.control.queued_starts.fetch_sub(1, Ordering::Relaxed);
            if self.control.cancel.is_cancelled() {
                self.control
                    .cancelled_starts
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}
