//! Script request validation, lifecycle handles, and pending-call accounting.

use super::bridge::{BRIDGE_ERROR, BRIDGE_LIMIT};
use crate::lease::{BytePool, Lease};
use crate::service::schema as service_schema;
use crate::session::{LaunchContext, parse_handle};
use crate::term::output::Truncate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, mpsc};
use tokio_util::sync::CancellationToken;

pub(super) const MAX_CODE: usize = 256 * 1024;
const PENDING_CALLS: usize = 1024;

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Request {
    /// Body of an async JavaScript function, up to 256 KiB. Top-level await and return work.
    pub code: String,
    /// Working directory for nested exec (when it omits cwd) and relative paths in read, write, and edit. Omit for the session's default working directory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Wait up to this many milliseconds for the script (0-240000, default 30000). Use exec poll to continue waiting.
    #[schemars(range(min = 0, max = 240000))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_ms: Option<u64>,
    /// Return when script output matches a condition. Use 1-16 case-sensitive literals of 1-4096 bytes each, without line breaks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_when: Option<ReturnWhen>,
    /// Stop the script after this many milliseconds. Zero or omitted disables the deadline. Tasks the script already started keep running.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kill_after_ms: Option<u64>,
    /// Placement within the script output. Defaults to head_tail.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncate: Option<Truncate>,
    /// Script output body budget in UTF-8 bytes, 100-4194304 (default 16384). Status, footer, errors, and warnings are additional.
    #[schemars(range(min = 100, max = 4194304))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReturnWhen {
    /// Match a substring within one unseen line of script output.
    pub output_contains: Option<Vec<String>>,
    /// 10-60000 ms of silence since the wait began or the script last produced output, whichever is later.
    #[schemars(range(min = 10, max = 60000))]
    pub quiet_ms: Option<u64>,
}

impl Request {
    pub fn as_exec(&self) -> Result<service_schema::Request, String> {
        let mut arguments = serde_json::to_value(self).map_err(|error| error.to_string())?;
        let fields = arguments
            .as_object_mut()
            .expect("a request serializes to an object");
        fields.remove("code");
        fields.insert("action".into(), json!("start"));
        fields.insert("command".into(), json!("[codemode]"));
        serde_json::from_value(arguments).map_err(|error| error.to_string())
    }
}

pub fn validate(
    request: &Request,
    launch: &LaunchContext,
) -> Result<(service_schema::Request, Option<std::path::PathBuf>), String> {
    if request.code.trim().is_empty() {
        return Err("code must contain JavaScript.".into());
    }
    if request.code.len() > MAX_CODE {
        return Err("code exceeds the 256 KiB limit.".into());
    }
    if request.code.contains('\0') {
        return Err("code must not contain NUL bytes; use a JavaScript \\0 escape.".into());
    }
    let resolved_cwd = match &request.cwd {
        Some(dir) => Some(service_schema::validate_cwd(dir, launch)?),
        None => None,
    };
    let start = request.as_exec()?;
    service_schema::validate(&start, false)?;
    Ok((start, resolved_cwd))
}

pub(super) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[derive(Clone, Debug, Serialize)]
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
    pub _lease: Lease,
}

pub(super) struct PendingCall {
    pub(super) info: CallInfo,
    pub(super) bytes: usize,
    pub(super) input_owner: crate::lease::Owner,
}

#[derive(Default)]
pub(super) struct Report {
    #[cfg(test)]
    pub(super) limit: Option<usize>,
    pub(super) pending: BTreeMap<u64, PendingCall>,
    pub(super) bridge_bytes: usize,
    pub(super) cancelled: Vec<CallInfo>,
    pub(super) started: Vec<String>,
    pub(super) started_ids: HashSet<String>,
}

impl Report {
    pub(super) fn limit(&self) -> usize {
        #[cfg(test)]
        if let Some(limit) = self.limit {
            return limit;
        }
        BRIDGE_LIMIT
    }

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

    pub(super) fn admit(
        &mut self,
        id: u64,
        info: CallInfo,
        bytes: usize,
        input_owner: crate::lease::Owner,
    ) -> Result<(), String> {
        if self.pending.len() >= PENDING_CALLS {
            return Err("At most 1024 calls may be pending per script.".into());
        }
        if bytes > self.limit().saturating_sub(self.bridge_bytes) {
            return Err(BRIDGE_ERROR.into());
        }
        self.bridge_bytes += bytes;
        self.pending.insert(
            id,
            PendingCall {
                info,
                bytes,
                input_owner,
            },
        );
        Ok(())
    }

    pub(super) fn result(&mut self, id: u64, bytes: usize) -> bool {
        let limit = self.limit();
        let Some(pending) = self.pending.get_mut(&id) else {
            return false;
        };
        let other_bytes = self.bridge_bytes - pending.bytes;
        if bytes > limit.saturating_sub(other_bytes) {
            return false;
        }
        // Admission failure preserves the old reservation and total together.
        self.bridge_bytes = other_bytes + bytes;
        pending.bytes = bytes;
        true
    }

    pub(super) fn remove(&mut self, id: u64) {
        if let Some(pending) = self.pending.remove(&id) {
            self.bridge_bytes -= pending.bytes;
        }
    }
}

pub(super) enum Outcome {
    Value(crate::ipc::Owned<String>),
    Error(&'static str),
    Limit,
    Memory,
}

pub(super) enum Wake {
    Settled { id: u64, outcome: Outcome },
    Stop,
}

pub(super) struct Control {
    pub(super) stopped: AtomicBool,
    pub(super) exited: AtomicBool,
    pub(super) cancel: CancellationToken,
    pub(super) wake: mpsc::Sender<Wake>,
    pub(super) report: Mutex<Report>,
    pub(super) in_flight: (Mutex<usize>, Condvar),
    pub(super) held_attachments: Mutex<HashMap<u64, crate::tasks::state::AttachmentLease>>,
    pub(super) next_attachment_handle: std::sync::atomic::AtomicU64,
    pub(super) queued_starts: AtomicUsize,
    pub(super) cancelled_starts: AtomicUsize,
}

#[derive(Clone)]
pub struct ScriptHandle(pub(super) Arc<Control>);

/// Counts an admitted `exec start` from validation until it enters `registry.tasks`,
/// moving unspawned cancelled starts into `cancelled_starts` on drop.
pub struct QueuedStartGuard {
    control: Arc<Control>,
    armed: bool,
}

impl QueuedStartGuard {
    pub(super) fn new(script: &ScriptHandle) -> Self {
        script.0.queued_starts.fetch_add(1, Ordering::Relaxed);
        Self {
            control: Arc::clone(&script.0),
            armed: true,
        }
    }

    pub fn disarm(&mut self) {
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
                self.control.cancelled_starts.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct NestedCall<'a> {
    pub script: &'a ScriptHandle,
    pub pending_id: u64,
    pub parent_task_id: &'a str,
}

pub(super) struct InFlightGuard(ScriptHandle);

impl InFlightGuard {
    pub(super) fn enter(script: ScriptHandle) -> Self {
        *lock(&script.0.in_flight.0) += 1;
        Self(script)
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        let (count, idle) = &self.0.0.in_flight;
        let mut active = lock(count);
        *active -= 1;
        if *active == 0 {
            idle.notify_all();
        }
    }
}

impl ScriptHandle {
    pub fn list_counts(&self) -> (usize, usize) {
        (
            self.0.queued_starts.load(Ordering::Relaxed),
            self.0.cancelled_starts.load(Ordering::Relaxed),
        )
    }

    pub(crate) fn reserve_result(&self, id: u64, bytes: usize) -> bool {
        lock(&self.0.report).result(id, bytes)
    }

    #[cfg(test)]
    pub(crate) fn test_pending_result(&self, id: u64, bytes: usize, limit: usize) {
        let mut report = lock(&self.0.report);
        report.limit = Some(limit);
        report
            .admit(
                id,
                CallInfo {
                    tool: "exec".into(),
                    action: None,
                    task_id: None,
                },
                bytes,
                Default::default(),
            )
            .unwrap();
    }

    #[cfg(test)]
    pub(crate) fn test_bridge_bytes(&self) -> usize {
        lock(&self.0.report).bridge_bytes
    }

    #[cfg(test)]
    pub(crate) fn test_bridge_limit(&self, limit: usize) {
        lock(&self.0.report).limit = Some(limit);
    }

    pub fn kill(&self) {
        self.0.stopped.store(true, Ordering::Release);
        self.0.cancel.cancel();
        let _ = self.0.wake.send(Wake::Stop);
    }

    pub(super) fn interrupted(&self) -> bool {
        self.0.stopped.load(Ordering::Acquire) || self.0.exited.load(Ordering::Acquire)
    }

    pub fn report(
        &self,
        memory: &Arc<BytePool>,
        control: Option<&Arc<BytePool>>,
    ) -> Option<ReportSnapshot> {
        let report = lock(&self.0.report);
        let bytes = report.snapshot_bytes();
        let lease = memory
            .try_acquire(bytes)
            .or_else(|| control.and_then(|pool| pool.try_acquire(bytes)))?;
        Some(ReportSnapshot {
            cancelled: report.cancelled.clone(),
            started: report.started.clone(),
            _lease: lease,
        })
    }

    pub fn record_started(&self, handle: String) {
        let mut report = lock(&self.0.report);
        if let Ok((id, _)) = parse_handle(&handle) {
            report.started_ids.insert(id.to_owned());
        }
        if !report.started.contains(&handle) {
            report.started.push(handle);
        }
    }

    pub fn owns(&self, task_id: &str) -> bool {
        lock(&self.0.report).started_ids.contains(task_id)
    }

    pub(crate) fn register_attachment(&self, lease: crate::tasks::state::AttachmentLease) -> u64 {
        let id = self
            .0
            .next_attachment_handle
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        lock(&self.0.held_attachments).insert(id, lease);
        id
    }

    pub(crate) fn unregister_attachment(&self, id: u64) {
        let removed = lock(&self.0.held_attachments).remove(&id);
        drop(removed);
    }

    #[cfg(test)]
    pub(crate) fn test_attachment_count(&self) -> usize {
        lock(&self.0.held_attachments).len()
    }

    pub(super) fn get_attachment(&self, id: u64) -> Option<crate::tasks::state::AttachmentLease> {
        lock(&self.0.held_attachments).get(&id).cloned()
    }

    pub(super) fn cancel_and_drain(&self) {
        self.0.cancel.cancel();
        let (count, idle) = &self.0.in_flight;
        let active = lock(count);
        // A start can still be registering its task; only the caller's wait has a deadline.
        drop(idle.wait_while(active, |&mut active| active > 0));
    }

    pub(super) fn finish(&self) -> crate::ipc::Owned<Vec<CallInfo>> {
        let dropped = std::mem::take(&mut *lock(&self.0.held_attachments));
        drop(dropped);
        let mut report = lock(&self.0.report);
        report.cancelled = report
            .pending
            .values()
            .map(|pending| pending.info.clone())
            .collect();
        let owners: Vec<_> = report
            .pending
            .values()
            .map(|call| call.input_owner.clone())
            .collect();
        report.pending.clear();
        report.bridge_bytes = 0;
        self.0.cancel.cancel();
        crate::ipc::Owned::new(report.cancelled.clone(), crate::lease::Owner::new(owners))
    }
}
