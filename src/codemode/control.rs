//! Script request validation, bridge handles, and pending-call accounting.

use super::bridge::{BRIDGE_ERROR, BRIDGE_LIMIT};
use crate::env::LaunchContext;
use crate::lease::{BytePool, Lease};
use crate::service::schema as service_schema;
use crate::tasks::script::{CallInfo, ScriptControl};
use crate::tasks::state::AttachmentLease;
use crate::term::output::Truncate;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, mpsc};

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
    service_schema::validate(&start, service_schema::ExecPolicy::AGENT)?;
    Ok((start, resolved_cwd))
}

pub(super) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
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
}

impl Report {
    pub(super) fn limit(&self) -> usize {
        #[cfg(test)]
        if let Some(limit) = self.limit {
            return limit;
        }
        BRIDGE_LIMIT
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
        pending.input_owner = Default::default();
        true
    }

    pub(super) fn remove(&mut self, id: u64) {
        if let Some(pending) = self.pending.remove(&id) {
            self.bridge_bytes -= pending.bytes;
        }
    }
}

pub(crate) enum Outcome {
    Value(crate::lease::Owned<String>),
    Error(Cow<'static, str>),
    Cancelled(Cow<'static, str>),
    Limit,
    Memory(Option<(String, String)>),
}

pub(super) struct SettledCall {
    pub(super) id: u64,
    pub(super) outcome: Outcome,
}

struct HeldAttachment {
    attachment: AttachmentLease,
    _pin: Lease,
}

#[derive(Default)]
pub(super) struct AttachmentRegistry {
    entries: Vec<HeldAttachment>,
    capacity: Option<Lease>,
}

/// Keeps registrations private until bridge admission commits the batch.
pub(crate) struct NestedAttachments<'a> {
    registry: MutexGuard<'a, AttachmentRegistry>,
    original_len: usize,
    memory: &'a Arc<BytePool>,
    control: Option<&'a Arc<BytePool>>,
}

impl NestedAttachments<'_> {
    fn acquire(&self, bytes: usize) -> Option<Lease> {
        self.memory
            .try_acquire(bytes)
            .or_else(|| self.control.and_then(|pool| pool.try_acquire(bytes)))
    }

    pub(crate) fn register(&mut self, attachment: AttachmentLease) -> Option<u64> {
        if let Some(index) = self
            .registry
            .entries
            .iter()
            .position(|held| held.attachment.same_attachment(&attachment))
        {
            return Some(index as u64 + 1);
        }
        let pin = self.acquire(attachment.metadata_bytes())?;
        let held = HeldAttachment {
            attachment,
            _pin: pin,
        };
        if self.registry.entries.len() == self.registry.entries.capacity() {
            let capacity = self.registry.entries.capacity().checked_mul(2)?.max(1);
            let bytes = capacity.checked_mul(std::mem::size_of::<HeldAttachment>())?;
            // Keep the old buffer charged until its replacement is ready.
            let mut owner = self.acquire(bytes)?;
            let mut entries = Vec::new();
            entries.try_reserve_exact(capacity).ok()?;
            if !owner.try_resize(
                entries
                    .capacity()
                    .checked_mul(std::mem::size_of::<HeldAttachment>())?,
            ) {
                return None;
            }
            entries.append(&mut self.registry.entries);
            let old = std::mem::replace(&mut self.registry.entries, entries);
            drop(old);
            self.registry.capacity = Some(owner);
        }
        self.registry.entries.push(held);
        Some(self.registry.entries.len() as u64)
    }

    pub(crate) fn disarm(&mut self) {
        self.original_len = self.registry.entries.len();
    }
}

impl Drop for NestedAttachments<'_> {
    fn drop(&mut self) {
        if self.original_len == 0 {
            let dropped = std::mem::take(&mut *self.registry);
            drop(dropped);
        } else {
            self.registry.entries.truncate(self.original_len);
        }
    }
}

pub(super) struct BridgeState {
    pub(super) control: Arc<ScriptControl>,
    pub(super) exited: AtomicBool,
    pub(super) settled_tx: mpsc::Sender<SettledCall>,
    pub(super) report: Mutex<Report>,
    pub(super) in_flight: (Mutex<usize>, Condvar),
    pub(super) held_attachments: Mutex<AttachmentRegistry>,
}

#[derive(Clone)]
pub(crate) struct BridgeHandle(pub(super) Arc<BridgeState>);

/// Program-only delivery parameters for the nested `codemode` JS bridge.
pub(crate) struct ProgramDelivery<'a> {
    pub(crate) script: &'a BridgeHandle,
    pub(crate) pending_id: u64,
    pub(crate) is_control: bool,
}

pub(super) struct InFlightGuard(BridgeHandle);

impl InFlightGuard {
    pub(super) fn enter(script: BridgeHandle) -> Self {
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

impl BridgeHandle {
    pub(super) fn new(control: Arc<ScriptControl>, settled_tx: mpsc::Sender<SettledCall>) -> Self {
        Self(Arc::new(BridgeState {
            control,
            exited: AtomicBool::new(false),
            settled_tx,
            report: Mutex::new(Report::default()),
            in_flight: (Mutex::new(0), Condvar::new()),
            held_attachments: Mutex::default(),
        }))
    }

    pub(crate) fn control(&self) -> &ScriptControl {
        &self.0.control
    }

    pub(super) fn control_arc(&self) -> &Arc<ScriptControl> {
        &self.0.control
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

    #[inline]
    pub(super) fn interrupted(&self) -> bool {
        self.0.control.is_stopped() || self.0.exited.load(Ordering::Acquire)
    }

    pub(crate) fn nested_attachments<'a>(
        &'a self,
        memory: &'a Arc<BytePool>,
        control: Option<&'a Arc<BytePool>>,
    ) -> NestedAttachments<'a> {
        let registry = lock(&self.0.held_attachments);
        let original_len = registry.entries.len();
        NestedAttachments {
            registry,
            original_len,
            memory,
            control,
        }
    }

    #[cfg(test)]
    pub(crate) fn test_attachment_count(&self) -> usize {
        lock(&self.0.held_attachments).entries.len()
    }

    #[cfg(test)]
    pub(crate) fn test_attachment_bytes(&self) -> usize {
        let registry = lock(&self.0.held_attachments);
        registry
            .entries
            .iter()
            .map(|held| held._pin.bytes())
            .sum::<usize>()
            + registry.capacity.as_ref().map_or(0, Lease::bytes)
    }

    pub(super) fn get_attachment(&self, id: u64) -> Option<AttachmentLease> {
        let index = usize::try_from(id.checked_sub(1)?).ok()?;
        lock(&self.0.held_attachments)
            .entries
            .get(index)
            .map(|held| held.attachment.clone())
    }

    pub(super) fn cancel_and_drain(&self) {
        self.0.control.cancel_token().cancel();
        let (count, idle) = &self.0.in_flight;
        let active = lock(count);
        // A start can still be registering its task; only the caller's wait has a deadline.
        drop(idle.wait_while(active, |&mut active| active > 0));
    }

    pub(super) fn finish(&self) -> crate::lease::Owned<Vec<CallInfo>> {
        let dropped = std::mem::take(&mut *lock(&self.0.held_attachments));
        drop(dropped);
        let (cancelled, owners) = {
            let mut report = lock(&self.0.report);
            let cancelled: Vec<CallInfo> = report
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
            (cancelled, owners)
        };
        self.0.control.set_cancelled_calls(cancelled.clone());
        self.0.control.cancel_token().cancel();
        crate::lease::Owned::new(cancelled, crate::lease::Owner::new(owners))
    }
}
