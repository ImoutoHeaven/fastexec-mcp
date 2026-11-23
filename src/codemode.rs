//! JavaScript tasks and their asynchronous bridge to the four host tools.

use crate::files;
use crate::lease::{BytePool, Lease};
use crate::media::{self, CompressFailure, MediaKind};
use crate::service::{self, Call, OutputMode, Render, Service};
use crate::session::{LaunchContext, Session, parse_handle};
use crate::tasks::{TASK_LOG_LIMIT, Task, Tasks};
use crate::term::output::Truncate;
use base64::Engine;
use rquickjs::context::EvalOptions;
use rquickjs::{Context, Ctx, Exception, Function, Object, Persistent, Runtime, Value};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{PipeWriter, Write};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, mpsc};
use tokio::runtime::Handle;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

// QuickJS mixes column origins on its first physical line; user code starts on the next one.
const SCRIPT_PREFIX: &str = "(async () => {\n";
const SLEEP_CHUNK_MS: u64 = 1_u64 << 40;
const MAX_CODE: usize = 256 * 1024;
const MEMORY_LIMIT: usize = 256 * 1024 * 1024;
pub(crate) const BRIDGE_LIMIT: usize = 256 * 1024 * 1024;
const BRIDGE_ERROR: &str =
    "Script tool-call bridge exceeds 256 MiB of pending arguments and results.";
const DIAGNOSTIC_RESERVE: u64 = 4096;
// One bounded description and pending-call text coexist until diagnostics finish writing.
const DIAGNOSTIC_WORKSPACE: usize = 3 * DIAGNOSTIC_RESERVE as usize;
const BRIDGE_RESULT_ERROR: &str = "The tool call completed, but its result does not fit in the 256 MiB pending bridge budget; the operation was not undone. Do not retry start or resend input; find started task handles in the outer codemode report.";
const START_PERMITS: usize = 8;
const IO_PERMITS: usize = 8;
const PENDING_CALLS: usize = 1024;
const MAX_STARTS: usize = 4096;
const STARTS_ERROR: &str = "A script may accept at most 4096 exec start calls.";
const STALLED: &str =
    "The script is waiting on a promise that can never settle: no tool call or sleep is pending.";

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
    pub fn as_exec(&self) -> Result<service::Request, String> {
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
) -> Result<(service::Request, Option<std::path::PathBuf>), String> {
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
        Some(dir) => Some(service::validate_cwd(dir, launch)?),
        None => None,
    };
    let start = request.as_exec()?;
    service::validate(&start, false)?;
    Ok((start, resolved_cwd))
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
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

struct PendingCall {
    info: CallInfo,
    bytes: usize,
    input_owner: crate::lease::Owner,
}

#[derive(Default)]
struct Report {
    #[cfg(test)]
    limit: Option<usize>,
    pending: BTreeMap<u64, PendingCall>,
    bridge_bytes: usize,
    cancelled: Vec<CallInfo>,
    started: Vec<String>,
    started_ids: HashSet<String>,
}

impl Report {
    fn limit(&self) -> usize {
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

    fn admit(
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

    fn result(&mut self, id: u64, bytes: usize) -> bool {
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

    fn remove(&mut self, id: u64) {
        if let Some(pending) = self.pending.remove(&id) {
            self.bridge_bytes -= pending.bytes;
        }
    }
}

enum Outcome {
    Value(crate::ipc::Owned<String>),
    Error(&'static str),
    Limit,
    Memory,
}

const BRIDGE_MEMORY_RESULT_ERROR: &str = "The tool call completed, but its result does not fit in the process active memory budget; the operation was not undone. Do not retry start or resend input; use exec list to locate a started task.";

enum Wake {
    Settled { id: u64, outcome: Outcome },
    Stop,
}

struct Control {
    stopped: AtomicBool,
    exited: AtomicBool,
    cancel: CancellationToken,
    wake: mpsc::Sender<Wake>,
    report: Mutex<Report>,
    in_flight: (Mutex<usize>, Condvar),
    held_attachments: Mutex<HashMap<u64, crate::tasks::AttachmentLease>>,
    next_attachment_handle: std::sync::atomic::AtomicU64,
}

#[derive(Clone)]
pub struct ScriptHandle(Arc<Control>);

#[derive(Clone, Copy)]
pub(crate) struct NestedCall<'a> {
    pub script: &'a ScriptHandle,
    pub pending_id: u64,
}

struct InFlightGuard(ScriptHandle);

impl InFlightGuard {
    fn enter(script: ScriptHandle) -> Self {
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

    fn interrupted(&self) -> bool {
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

    pub(crate) fn register_attachment(&self, lease: crate::tasks::AttachmentLease) -> u64 {
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

    fn get_attachment(&self, id: u64) -> Option<crate::tasks::AttachmentLease> {
        lock(&self.0.held_attachments).get(&id).cloned()
    }

    fn cancel_and_drain(&self) {
        self.0.cancel.cancel();
        let (count, idle) = &self.0.in_flight;
        let active = lock(count);
        // A start can still be registering its task; only the caller's wait has a deadline.
        drop(idle.wait_while(active, |&mut active| active > 0));
    }

    fn finish(&self) -> crate::ipc::Owned<Vec<CallInfo>> {
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

pub struct Work {
    pub service: Arc<Service>,
    pub session: Arc<Session>,
    pub launch: Arc<LaunchContext>,
    pub transfer: bool,
    pub call_id: Option<u64>,
    pub code: String,
    pub writer: PipeWriter,
    pub task: Arc<Task>,
    pub tasks: Arc<Tasks>,
    pub captured: mpsc::Receiver<()>,
    pub input_owner: crate::lease::Owner,
}

pub struct ScriptWorker(mpsc::SyncSender<Work>);

impl ScriptWorker {
    pub fn start(self, work: Work) {
        self.0
            .send(work)
            .expect("the prepared script worker stopped");
    }
}

/// Creates a waiting VM thread. Dropping the worker abandons it.
pub fn prepare(runtime: Handle) -> Result<(ScriptHandle, ScriptWorker), String> {
    let (wake, receiver) = mpsc::channel();
    let script = ScriptHandle(Arc::new(Control {
        stopped: AtomicBool::new(false),
        exited: AtomicBool::new(false),
        cancel: CancellationToken::new(),
        wake,
        report: Mutex::new(Report::default()),
        in_flight: (Mutex::new(0), Condvar::new()),
        held_attachments: Mutex::new(HashMap::new()),
        next_attachment_handle: std::sync::atomic::AtomicU64::new(0),
    }));
    let (sender, job) = mpsc::sync_channel(1);
    let control = script.clone();
    std::thread::Builder::new()
        .name("fastexec-script".into())
        .stack_size(2 * 1024 * 1024)
        .spawn(move || {
            if let Ok(work) = job.recv() {
                run(work, control, receiver, runtime);
            }
        })
        .map_err(|error| format!("Cannot create the script worker: {error}"))?;
    Ok((script, ScriptWorker(sender)))
}

struct Output {
    writer: PipeWriter,
    written: u64,
}

impl Output {
    fn capacity(&self, bytes: u64) -> Result<(), String> {
        if self.written + bytes > TASK_LOG_LIMIT - DIAGNOSTIC_RESERVE {
            Err("Script output exceeds 64 MiB (4 KiB is reserved for diagnostics).".into())
        } else {
            Ok(())
        }
    }

    fn write(&mut self, text: &str) -> Result<(), String> {
        self.capacity(text.len() as u64 + 1)?;
        self.line(text)
            .map_err(|error| format!("Cannot write script output: {error}"))
    }

    fn line(&mut self, text: &str) -> std::io::Result<()> {
        self.writer.write_all(text.as_bytes())?;
        self.written += text.len() as u64;
        self.writer.write_all(b"\n")?;
        self.written += 1;
        Ok(())
    }

    fn diagnostic(&mut self, parts: &[&str]) {
        let left = TASK_LOG_LIMIT
            .saturating_sub(self.written)
            .min(DIAGNOSTIC_RESERVE);
        if left == 0 {
            return;
        }
        let room = left.saturating_sub(1) as usize;
        let total = parts
            .iter()
            .fold(0usize, |n, part| n.saturating_add(part.len()));
        let truncated = total > room && room >= 3;
        let mut remaining = room - usize::from(truncated) * 3;
        for part in parts {
            let mut end = part.len().min(remaining);
            while !part.is_char_boundary(end) {
                end -= 1;
            }
            if self.writer.write_all(&part.as_bytes()[..end]).is_err() {
                return;
            }
            self.written += end as u64;
            remaining -= end;
            if end < part.len() {
                break;
            }
        }
        if truncated {
            if self.writer.write_all(b"...").is_err() {
                return;
            }
            self.written += 3;
        }
        if self.writer.write_all(b"\n").is_ok() {
            self.written += 1;
        }
    }
}

struct Host {
    service: Arc<Service>,
    session: Arc<Session>,
    launch: Arc<LaunchContext>,
    transfer: bool,
    call_id: Option<u64>,
    runtime: Handle,
    script: ScriptHandle,
    start_permits: Arc<Semaphore>,
    io_permits: Arc<Semaphore>,
    next: u64,
    starts: usize,
}

enum ToolCall {
    Exec(Box<service::Request>),
    Read(files::ReadRequest),
    Write(files::WriteRequest),
    Edit(files::EditRequest),
    Sleep(f64),
}

impl ToolCall {
    fn parse(tool: &str, value: serde_json::Value) -> Result<Self, String> {
        if !value.is_object() {
            return Err("Tool arguments must be an object.".into());
        }
        let invalid = |error: serde_json::Error| format!("Invalid {tool} arguments: {error}");
        match tool {
            "exec" => serde_json::from_value::<service::Request>(value)
                .map(|mut request| {
                    service::normalize_request(&mut request);
                    Self::Exec(Box::new(request))
                })
                .map_err(invalid),
            "read" => serde_json::from_value(value)
                .map(Self::Read)
                .map_err(invalid),
            "write" => serde_json::from_value(value)
                .map(Self::Write)
                .map_err(invalid),
            "edit" => serde_json::from_value(value)
                .map(Self::Edit)
                .map_err(invalid),
            _ => Err("Available tools: exec, read, write, edit.".into()),
        }
    }
}

fn split_sleep(mut ms: f64, deferred: &mut Vec<f64>) -> f64 {
    // Above this threshold, subtracting one timer chunk can round away or remove two chunks.
    while ms > (1_u128 << 92) as f64 {
        ms /= 2.0;
        deferred.push(ms);
    }
    ms
}

async fn sleep(mut ms: f64) {
    if ms <= 0.0 {
        tokio::task::yield_now().await;
        return;
    }
    let mut deferred = Vec::new();
    loop {
        ms = split_sleep(ms, &mut deferred);
        // Keep each timer representable without shortening the requested delay.
        while ms > 0.0 {
            let chunk = ms.min(SLEEP_CHUNK_MS as f64);
            let duration = if chunk == SLEEP_CHUNK_MS as f64 {
                std::time::Duration::from_millis(SLEEP_CHUNK_MS)
            } else {
                std::time::Duration::from_secs_f64(chunk / 1000.0)
            };
            tokio::time::sleep(duration).await;
            ms -= chunk;
        }
        let Some(next) = deferred.pop() else {
            return;
        };
        ms = next;
    }
}

/// Register the wait on the VM thread, where calls arrive in script order. A spawned
/// future may run before an older sibling, so its first poll cannot establish queue order.
fn queue_permit(pool: Arc<Semaphore>) -> impl Future<Output = tokio::sync::OwnedSemaphorePermit> {
    use std::task::{Context, Poll, Waker};
    let mut waiting = Box::pin(pool.acquire_owned());
    let first = waiting
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    async move {
        match first {
            Poll::Ready(result) => result,
            Poll::Pending => waiting.await,
        }
        .expect("script permit pools stay open")
    }
}

impl Host {
    fn call(&mut self, tool: &str, arguments: &str) -> Result<u64, crate::ipc::Owned<String>> {
        if self.script.interrupted() || self.script.0.cancel.is_cancelled() {
            return Err(crate::ipc::Owned::new(
                "The script is ending; no new tool calls can start.".into(),
                Default::default(),
            ));
        }
        // QuickJS owns the borrowed UTF-8 input. Reserve before creating Rust
        // Value strings, escape scratch, typed arguments and worker copies.
        let bytes = arguments.len().saturating_add(tool.len()).saturating_mul(4);
        let (lease, control) = if let Some(lease) = self.launch.memory().try_acquire(bytes) {
            (lease, false)
        } else if arguments.len().saturating_add(tool.len()) <= crate::ipc::SHORT_INPUT {
            (
                self.launch
                    .control_memory()
                    .try_acquire(bytes)
                    .ok_or_else(|| {
                        crate::ipc::Owned::new(bridge_input_error(), Default::default())
                    })?,
                true,
            )
        } else {
            return Err(crate::ipc::Owned::new(
                bridge_input_error(),
                Default::default(),
            ));
        };
        let mut input_owner = crate::lease::Owner::new(lease);
        let id = self.next;
        self.next += 1;
        lock(&self.script.0.report)
            .admit(
                id,
                CallInfo {
                    tool: tool.to_owned(),
                    action: None,
                    task_id: None,
                },
                arguments.len(),
                input_owner.clone(),
            )
            .map_err(|error| crate::ipc::Owned::new(error, input_owner.clone()))?;
        let parsed = (|| {
            if tool == "sleep" {
                let ms = arguments
                    .parse::<f64>()
                    .ok()
                    .filter(|ms| ms.is_finite())
                    .ok_or_else(|| "sleep requires a finite number of milliseconds.".to_owned())?;
                return Ok(ToolCall::Sleep(ms.max(0.0)));
            }
            let value: serde_json::Value =
                serde_json::from_str(arguments).map_err(|error| error.to_string())?;
            let info = CallInfo {
                tool: tool.to_owned(),
                action: value
                    .get("action")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned),
                task_id: value
                    .get("taskId")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned),
            };
            let request = ToolCall::parse(tool, value)?;
            lock(&self.script.0.report)
                .pending
                .get_mut(&id)
                .expect("the call was just admitted")
                .info = info;
            Ok(request)
        })();
        let request = match parsed {
            Ok(request) => request,
            Err(error) => {
                lock(&self.script.0.report).remove(id);
                return Err(crate::ipc::Owned::new(error, input_owner));
            }
        };
        let is_control = matches!(&request,
            ToolCall::Exec(request) if matches!(request.action, service::Action::Kill | service::Action::List)
        );
        if control && !is_control {
            let Some(lease) = self.launch.memory().try_acquire(bytes) else {
                lock(&self.script.0.report).remove(id);
                return Err(crate::ipc::Owned::new(bridge_input_error(), input_owner));
            };
            input_owner = crate::lease::Owner::new(lease);
            lock(&self.script.0.report)
                .pending
                .get_mut(&id)
                .unwrap()
                .input_owner = input_owner.clone();
        }
        if let ToolCall::Exec(request) = &request
            && request.action == service::Action::Start
            && service::validate(request, true).is_ok()
        {
            if self.starts >= MAX_STARTS {
                lock(&self.script.0.report).remove(id);
                return Err(crate::ipc::Owned::new(STARTS_ERROR.into(), input_owner));
            }
            self.starts += 1;
        }
        let permit_pool = match &request {
            ToolCall::Exec(req) => match req.action {
                service::Action::Start => Some(Arc::clone(&self.start_permits)),
                service::Action::Transcript => Some(Arc::clone(&self.io_permits)),
                service::Action::Poll | service::Action::Kill | service::Action::List => None,
            },
            ToolCall::Read(_) | ToolCall::Write(_) | ToolCall::Edit(_) => {
                Some(Arc::clone(&self.io_permits))
            }
            ToolCall::Sleep(_) => None,
        };
        let permit_wait = permit_pool.map(queue_permit);
        let service = Arc::clone(&self.service);
        let session = Arc::clone(&self.session);
        let launch = Arc::clone(&self.launch);
        let script = self.script.clone();
        let in_flight = InFlightGuard::enter(script.clone());
        let transfer = self.transfer;
        let call_id = self.call_id;
        let cancel = script.0.cancel.child_token();
        let nested_render = Render {
            output_mode: OutputMode::Structured,
            structured_content: true,
        };
        let response_pool = Arc::clone(launch.memory());
        let response_control = is_control.then(|| Arc::clone(launch.control_memory()));
        self.runtime.spawn(input_owner.clone().hold_future(async move {
            let _in_flight = in_flight;
            let permit = match permit_wait {
                Some(wait) => Some(tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return,
                    permit = wait => permit,
                }),
                None => None,
            };
            let exec_result = matches!(&request, ToolCall::Exec(_));
            let mut result = match request {
                ToolCall::Sleep(ms) => {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return,
                        () = sleep(ms) => {}
                    }
                    let _ = lock(&script.0.report).result(id, 0);
                    let _ = script.0.wake.send(Wake::Settled {
                        id,
                        outcome: Outcome::Value(crate::ipc::Owned::new(String::new(), Default::default())),
                    });
                    return;
                }
                ToolCall::Exec(request) => {
                    let _permit = permit;
                    service
                        .call_tracked(
                            *request,
                            Call {
                                session: &session,
                                launch: Some(&launch),
                                render: nested_render,
                                cancel: cancel.clone(),
                                progress: None,
                                call_id,
                                nested: true,
                                started: Some(NestedCall { script: &script, pending_id: id }),
                                render_slot: None,
                                http_response: false,
                                rpc_id_bytes: 0,
                                input_owner: input_owner.clone(),
                            },
                        )
                        .await
                }
                ToolCall::Read(request) => {
                    let permit = permit.expect("read acquires an I/O permit");
                    let read_cancel = cancel.clone();
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return,
                        result = files::read_nested(request, launch, transfer, nested_render, permit, read_cancel, input_owner.clone()) => result,
                    }
                }
                ToolCall::Write(request) => {
                    let permit = permit.expect("write acquires an I/O permit");
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return,
                        result = files::write_nested(request, launch, nested_render, permit, input_owner.clone()) => result,
                    }
                }
                ToolCall::Edit(request) => {
                    let permit = permit.expect("edit acquires an I/O permit");
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return,
                        result = files::edit_nested(request, launch, nested_render, permit, input_owner.clone()) => result,
                    }
                }
            };
            // Only exec's reader/report commit prepaid its wire. File leases
            // still own the file payload and must stay with the envelope.
            let prepaid = exec_result.then(|| result.lease.take()).flatten();
            let mut outcome = encode_nested_result(result, &response_pool, response_control.as_ref(), prepaid);
            let bytes = match &outcome {
                Outcome::Value(value) => value.value.len(),
                Outcome::Error(error) => error.len(),
                Outcome::Limit | Outcome::Memory => 0,
            };
            if !lock(&script.0.report).result(id, bytes) {
                outcome = Outcome::Limit;
            }
            let _ = script.0.wake.send(Wake::Settled { id, outcome });
        }));
        Ok(id)
    }
}

fn bridge_input_error() -> String {
    "Server active memory budget is full while copying nested tool arguments; the call was not dispatched.".into()
}

fn nested_envelope(mut tracked: files::TrackedResult) -> crate::ipc::Owned<serde_json::Value> {
    let prior = std::mem::take(&mut tracked.payload_owner);
    let owner = crate::lease::Owner::new((prior, tracked.lease.take()));
    let mut result = crate::ipc::Owned::new(tracked.result, owner);
    let mut ok = !result.value.is_error.unwrap_or(false);
    let mut map = match result.value.structured_content.take() {
        Some(serde_json::Value::Object(map)) => map,
        _ => {
            let text = match std::mem::take(&mut result.value.content).into_iter().next() {
                Some(rmcp::model::ContentBlock::Text(text)) => text.text,
                _ => "Tool operation failed.".to_owned(),
            };
            let mut fallback = serde_json::Map::new();
            fallback.insert("error".into(), serde_json::Value::String(text));
            ok = false;
            fallback
        }
    };
    if map.remove("ok") == Some(serde_json::Value::Bool(false)) {
        ok = false;
    }
    let mut envelope = serde_json::Map::new();
    envelope.insert("ok".into(), serde_json::Value::Bool(ok));
    envelope.insert("value".into(), serde_json::Value::Object(map));
    crate::ipc::Owned::new(serde_json::Value::Object(envelope), result.owner)
}

fn encode_nested_result(
    result: files::TrackedResult,
    pool: &Arc<BytePool>,
    control: Option<&Arc<BytePool>>,
    prepaid: Option<Lease>,
) -> Outcome {
    // The service reserves exec's envelope workspace and wire before committing
    // its reader. Keep that full allowance until the old payload is gone.
    let precommitted = prepaid.is_some();
    let Some(mut wire) =
        prepaid.or_else(|| pool.try_acquire(128).or_else(|| control?.try_acquire(128)))
    else {
        return Outcome::Memory;
    };
    let envelope = nested_envelope(result);
    let Ok(length) = crate::ipc::json_len(&envelope.value) else {
        return Outcome::Error("Cannot measure the nested tool result.");
    };
    let peak = length.saturating_add(128);
    if wire.bytes() < peak {
        // A committed exec result must never need another admission here.
        if precommitted {
            return Outcome::Memory;
        }
        if !wire.try_grow(peak - wire.bytes()) {
            let Some(replacement) = control.and_then(|pool| pool.try_acquire(peak)) else {
                return Outcome::Memory;
            };
            wire = replacement;
        }
    }
    let mut bytes = Vec::with_capacity(length);
    if serde_json::to_writer(&mut bytes, &envelope.value).is_err() {
        return Outcome::Error("Cannot encode the nested tool result.");
    }
    let text = String::from_utf8(bytes).expect("JSON serialization emits UTF-8");
    drop(envelope);
    wire.shrink_to(text.capacity());
    Outcome::Value(crate::ipc::Owned::new(text, crate::lease::Owner::new(wire)))
}

fn pending_text(calls: &[CallInfo]) -> String {
    use std::fmt::Write;
    let limit = DIAGNOSTIC_RESERVE as usize / 2;
    let mut text = String::new();
    for (index, call) in calls.iter().enumerate() {
        if index != 0 {
            text.push_str(", ");
        }
        for (part_index, part) in [
            Some(call.tool.as_str()),
            call.action.as_deref(),
            call.task_id.as_deref(),
        ]
        .into_iter()
        .flatten()
        .enumerate()
        {
            if part_index != 0 {
                text.push(' ');
            }
            let room = limit.saturating_sub(text.len() + 64);
            if part.len() > room {
                let mut end = room;
                while !part.is_char_boundary(end) {
                    end -= 1;
                }
                text.push_str(&part[..end]);
                let _ = write!(
                    &mut text,
                    "... ({} call(s) omitted)",
                    calls.len() - index - 1
                );
                return text;
            }
            text.push_str(part);
        }
    }
    text
}

fn run(work: Work, script: ScriptHandle, receiver: mpsc::Receiver<Wake>, runtime: Handle) {
    let Work {
        service,
        session,
        launch,
        transfer,
        call_id,
        code,
        writer,
        task,
        tasks,
        captured,
        input_owner,
    } = work;
    let code = crate::ipc::Owned::new(code, input_owner);
    let output = Rc::new(RefCell::new(Output { writer, written: 0 }));
    let Some(diagnostic_lease) = launch.control_memory().try_acquire(DIAGNOSTIC_WORKSPACE) else {
        output.borrow_mut().diagnostic(&[
            "Script error: diagnostic memory budget is full; the script was not started.",
        ]);
        drop(output);
        tasks.finish_script(&task, Some(1), captured);
        return;
    };
    let host = Rc::new(RefCell::new(Host {
        service,
        session,
        launch,
        transfer,
        call_id,
        runtime,
        script: script.clone(),
        start_permits: Arc::new(Semaphore::new(START_PERMITS)),
        io_permits: Arc::new(Semaphore::new(IO_PERMITS)),
        next: 1,
        starts: 0,
    }));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_vm(
            &code.value,
            &script,
            receiver,
            Rc::clone(&host),
            Rc::clone(&output),
            &task,
            &tasks,
        )
    }))
    .unwrap_or_else(|_| Err("The JavaScript worker panicked.".into()));
    drop(host);
    script.cancel_and_drain();
    let pending = script.finish();
    let exit_code = if script.0.stopped.load(Ordering::Acquire) {
        let count = pending.value.len().to_string();
        let pending = pending_text(&pending.value);
        output.borrow_mut().diagnostic(&[
            "Script stopped: cancelled ",
            &count,
            " pending call(s): ",
            &pending,
        ]);
        None
    } else if script.0.exited.load(Ordering::Acquire) {
        Some(0)
    } else if let Err(message) = &result {
        let pending = pending_text(&pending.value);
        let prefix = if pending.is_empty() {
            ""
        } else {
            "\nPending calls: "
        };
        output
            .borrow_mut()
            .diagnostic(&["Script error: ", message, prefix, &pending]);
        Some(1)
    } else {
        Some(0)
    };
    drop(result);
    drop(output);
    drop(diagnostic_lease);
    tasks.finish_script(&task, exit_code, captured);
}

fn push_diagnostic(text: &mut String, part: &str) -> bool {
    let room = (DIAGNOSTIC_RESERVE as usize).saturating_sub(text.len());
    if part.len() <= room {
        text.push_str(part);
        return true;
    }
    let mut end = part.len().min(room);
    while !part.is_char_boundary(end) {
        end -= 1;
    }
    text.push_str(&part[..end]);
    let mut end = text.len().min(DIAGNOSTIC_RESERVE as usize - 3);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text.push_str("...");
    false
}

fn describe<'js>(ctx: &Ctx<'js>, value: Value<'js>) -> String {
    use rquickjs::{Coerced, FromJs};
    // CString remains owned by QuickJS; only the bounded description is copied to Rust.
    let prop = |object: &Object<'js>, key: &str| -> Option<rquickjs::CString<'js>> {
        let result = object
            .get::<_, Option<Coerced<rquickjs::String<'js>>>>(key)
            .and_then(|value| value.map(|coerced| coerced.0.to_cstring()).transpose());
        match result {
            Ok(value) => value,
            Err(error) => {
                if error.is_exception() {
                    let _ = ctx.catch();
                }
                None
            }
        }
    };
    let mut text = String::with_capacity(DIAGNOSTIC_RESERVE as usize);
    if let Some(exception) = value.as_exception() {
        let object = exception.as_object();
        if !push_diagnostic(
            &mut text,
            prop(object, "name").as_deref().unwrap_or("Error"),
        ) || !push_diagnostic(&mut text, ": ")
            || !push_diagnostic(&mut text, prop(object, "message").as_deref().unwrap_or(""))
        {
            return text;
        }
        if let Some(stack) = prop(object, "stack") {
            for line in stack
                .lines()
                .filter(|line| !line.contains("codemode-prelude.js"))
            {
                if !push_diagnostic(&mut text, "\n") || !push_diagnostic(&mut text, line) {
                    break;
                }
            }
        }
    } else {
        match Coerced::<rquickjs::String>::from_js(ctx, value)
            .and_then(|value| value.0.to_cstring())
        {
            Ok(value) => {
                push_diagnostic(&mut text, &value);
            }
            Err(error) => {
                if error.is_exception() {
                    let _ = ctx.catch();
                }
                push_diagnostic(
                    &mut text,
                    "The script threw a value that cannot be described.",
                );
            }
        }
    }
    text
}

fn is_js_whitespace(ch: char) -> bool {
    matches!(
        ch,
        '\u{0009}'..='\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200A}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202F}'
            | '\u{205F}'
            | '\u{3000}'
            | '\u{FEFF}'
    )
}

fn js_error(ctx: &Ctx<'_>, error: rquickjs::Error) -> String {
    if error.is_exception() {
        describe(ctx, ctx.catch())
    } else {
        error.to_string()
    }
}

fn run_vm(
    code: &str,
    script: &ScriptHandle,
    receiver: mpsc::Receiver<Wake>,
    host: Rc<RefCell<Host>>,
    output: Rc<RefCell<Output>>,
    task: &Arc<Task>,
    tasks: &Arc<Tasks>,
) -> Result<(), String> {
    let runtime = Runtime::new().map_err(|error| error.to_string())?;
    runtime.set_memory_limit(MEMORY_LIMIT);
    runtime.set_max_stack_size(512 * 1024);
    let interrupt = script.clone();
    runtime.set_interrupt_handler(Some(Box::new(move || interrupt.interrupted())));
    let rejections = Rc::new(RefCell::new(HashMap::<
        Persistent<Value<'static>>,
        Persistent<Value<'static>>,
    >::new()));
    let tracking = Rc::clone(&rejections);
    runtime.set_host_promise_rejection_tracker(Some(Box::new(
        move |ctx, promise, reason, handled| {
            let promise = Persistent::save(&ctx, promise);
            if handled {
                tracking.borrow_mut().remove(&promise);
            } else {
                tracking
                    .borrow_mut()
                    .insert(promise, Persistent::save(&ctx, reason));
            }
        },
    )));
    let context = Context::full(&runtime).map_err(|error| error.to_string())?;
    let result = (|| {
        let api = context.with(|ctx| -> Result<Persistent<Object<'static>>, String> {
            let mut options = EvalOptions::default();
            options.filename = Some("codemode-prelude.js".into());
            let factory: Function = ctx
                .eval_with_options(PRELUDE, options)
                .map_err(|error| js_error(&ctx, error))?;
            let writing = Rc::clone(&output);
            let control = script.clone();
            let write = Function::new(ctx.clone(), move |ctx: Ctx<'_>, text: rquickjs::CString<'_>| {
                if control.interrupted() {
                    return Err(Exception::throw_message(&ctx, "The script is ending."));
                }
                writing
                    .borrow_mut()
                    .write(&text)
                    .map_err(|error| Exception::throw_range(&ctx, &error))
            })
            .map_err(|error| js_error(&ctx, error))?;
            let launch_ctx = Arc::clone(&host.borrow().launch);
            let call = Function::new(
                ctx.clone(),
                move |ctx: Ctx<'_>, tool: rquickjs::CString<'_>, args: rquickjs::CString<'_>| {
                    host.borrow_mut().call(tool.as_str(), args.as_str()).map_err(|error| {
                        let message = &error.value;
                        if message.starts_with("At most")
                            || message == BRIDGE_ERROR
                            || message == STARTS_ERROR
                        {
                            Exception::throw_range(&ctx, message)
                        } else {
                            Exception::throw_type(&ctx, message)
                        }
                    })
                },
            )
            .map_err(|error| js_error(&ctx, error))?;
            let task = Arc::clone(task);
            let tasks = Arc::clone(tasks);
            let control = script.clone();
            let attach = Function::new(
                ctx.clone(),
                move |ctx: Ctx<'_>,
                      mode: u8,
                      payload: rquickjs::CString<'_>,
                      declared_mime: rquickjs::CString<'_>,
                      expected_kind: rquickjs::CString<'_>|
                      -> rquickjs::Result<String> {
                    if control.interrupted() {
                        return Err(Exception::throw_message(&ctx, "The script is ending."));
                    }
                    // The callback borrows VM strings. Path and MIME diagnostics
                    // can create Rust copies independently of the media bytes.
                    let copied = declared_mime.len().saturating_add(expected_kind.len());
                    let _argument_workspace = if mode == 1 {
                        files::with_path_workspace(
                            payload.as_str(),
                            &launch_ctx,
                            copied.saturating_mul(16),
                            |_| (),
                        )
                        .map_err(|error| Exception::throw_range(&ctx, &error))?
                        .owner
                    } else {
                        crate::lease::Owner::new(launch_ctx.memory()
                            .try_acquire(copied.saturating_mul(16).saturating_add(4096))
                            .ok_or_else(|| Exception::throw_range(
                                &ctx, "Server active memory budget is full."
                            ))?)
                    };
                    let max_bytes = launch_ctx
                        .max_media_bytes()
                        .map_err(|err| Exception::throw_type(&ctx, &err))?;
                    let compress = launch_ctx
                        .image_compress()
                        .map_err(|err| Exception::throw_type(&ctx, &err))?;

                    let (bytes, mut lease, kind, mime): (
                        Vec<u8>,
                        crate::lease::Lease,
                        MediaKind,
                        String,
                    ) = match mode {
                        0 => {
                            let hid: u64 = payload.parse().map_err(|_| {
                                Exception::throw_type(&ctx, "Invalid attachment handle.")
                            })?;
                            let Some(att_lease) = control.get_attachment(hid) else {
                                return Err(Exception::throw_type(
                                    &ctx,
                                    "Invalid or expired attachment handle.",
                                ));
                            };
                            let src_meta = att_lease.meta().clone();
                            let mut file = std::fs::File::open(&src_meta.path).map_err(|err| {
                                Exception::throw_type(
                                    &ctx,
                                    &format!("Cannot read source attachment: {err}."),
                                )
                            })?;
                            let (loaded, loaded_lease) = match files::read_media_from_zero(
                                &mut file,
                                src_meta.size,
                                files::MediaStart::Rewind,
                                max_bytes,
                                launch_ctx.memory(),
                                None,
                                Some(&control.0.cancel),
                            ) {
                                Ok(files::MediaBytes::Loaded { bytes, lease }) => (bytes, lease),
                                Ok(files::MediaBytes::Oversized { .. }) => {
                                    return Err(Exception::throw_range(
                                        &ctx,
                                        &format!(
                                            "Attachment exceeds FASTEXEC_MAX_MEDIA_BYTES ({max_bytes} bytes)."
                                        ),
                                    ));
                                }
                                Err(files::MediaReadError::Overload) => {
                                    return Err(Exception::throw_range(
                                        &ctx,
                                        "Server active memory budget is full.",
                                    ));
                                }
                                Err(files::MediaReadError::Cancelled) => {
                                    return Err(Exception::throw_message(
                                        &ctx,
                                        "The script is ending.",
                                    ));
                                }
                                Err(files::MediaReadError::Io(err)) => {
                                    return Err(Exception::throw_type(
                                        &ctx,
                                        &format!("Cannot read source attachment: {err}."),
                                    ));
                                }
                            };
                            drop(att_lease);
                            if loaded.len() as u64 != src_meta.size {
                                drop(loaded);
                                drop(loaded_lease);
                                return Err(Exception::throw_type(
                                    &ctx,
                                    "Source attachment size changed on disk.",
                                ));
                            }
                            (loaded, loaded_lease, src_meta.kind, src_meta.mime)
                        }
                        1 => {
                            let resolved = files::resolve(&payload, &launch_ctx)
                                .map_err(|err| Exception::throw_type(&ctx, &err))?;
                            let shown = files::display_path(&resolved);
                            let (mut file, meta) =
                                files::open_regular(&resolved).map_err(|err| {
                                    Exception::throw_type(
                                        &ctx,
                                        &format!("Cannot read {shown}: {err}."),
                                    )
                                })?;
                            let mut prefix = [0u8; media::SNIFF];
                            let mut sniffed = 0;
                            while sniffed < prefix.len() {
                                match std::io::Read::read(&mut file, &mut prefix[sniffed..]) {
                                    Ok(0) => break,
                                    Ok(n) => sniffed += n,
                                    Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {
                                        continue;
                                    }
                                    Err(err) => {
                                        return Err(Exception::throw_type(
                                            &ctx,
                                            &format!("Cannot read {shown}: {err}."),
                                        ));
                                    }
                                }
                            }
                            let Some(det) =
                                media::detect_path(&prefix[..sniffed], Some(&resolved))
                            else {
                                return Err(Exception::throw_type(
                                    &ctx,
                                    &format!(
                                        "Cannot attach {shown}: file is not a supported image, audio, PDF, or video format."
                                    ),
                                ));
                            };
                            match files::read_media_from_zero(
                                &mut file,
                                meta.len(),
                                files::MediaStart::Prefix(&prefix[..sniffed]),
                                max_bytes,
                                launch_ctx.memory(),
                                None,
                                Some(&control.0.cancel),
                            ) {
                                Ok(files::MediaBytes::Loaded { bytes, lease }) => {
                                    (bytes, lease, det.kind, det.mime.to_owned())
                                }
                                Ok(files::MediaBytes::Oversized { .. }) => {
                                    return Err(Exception::throw_range(
                                        &ctx,
                                        &format!(
                                            "Cannot attach {shown}: exceeds FASTEXEC_MAX_MEDIA_BYTES ({max_bytes} bytes)."
                                        ),
                                    ));
                                }
                                Err(files::MediaReadError::Overload) => {
                                    return Err(Exception::throw_range(
                                        &ctx,
                                        "Server active memory budget is full.",
                                    ));
                                }
                                Err(files::MediaReadError::Cancelled) => {
                                    return Err(Exception::throw_message(
                                        &ctx,
                                        "The script is ending.",
                                    ));
                                }
                                Err(files::MediaReadError::Io(err)) => {
                                    return Err(Exception::throw_type(
                                        &ctx,
                                        &format!("Cannot read {shown}: {err}."),
                                    ));
                                }
                            }
                        }
                        _ => {
                            let clean_len: usize = payload.chars()
                                .filter(|ch| !is_js_whitespace(*ch))
                                .map(char::len_utf8).sum();
                            let needs_cleaning = clean_len != payload.len();
                            let unpadded_len: usize = payload.chars().rev()
                                .skip_while(|ch| is_js_whitespace(*ch) || *ch == '=')
                                .filter(|ch| !is_js_whitespace(*ch))
                                .map(char::len_utf8).sum();
                            let min_decoded = (unpadded_len / 4)
                                .saturating_mul(3)
                                .saturating_add((unpadded_len % 4).saturating_sub(1));
                            if min_decoded as u64 > max_bytes {
                                return Err(Exception::throw_range(
                                    &ctx,
                                    &format!(
                                        "Inline media exceeds FASTEXEC_MAX_MEDIA_BYTES ({max_bytes} bytes)."
                                    ),
                                ));
                            }
                            let est_decoded =
                                (clean_len / 4).saturating_mul(3).saturating_add(3);
                            let Some(mut inline_lease) = (if needs_cleaning { clean_len } else { 0usize })
                                .checked_add(est_decoded)
                                .and_then(|peak| launch_ctx.memory().try_acquire(peak))
                            else {
                                return Err(Exception::throw_range(
                                    &ctx,
                                    "Server active memory budget is full.",
                                ));
                            };
                            let cleaned_owned = needs_cleaning.then(|| {
                                let mut cleaned = String::with_capacity(clean_len);
                                cleaned.extend(payload.chars().filter(|ch| !is_js_whitespace(*ch)));
                                cleaned
                            });
                            let b64_str = cleaned_owned.as_deref().unwrap_or(payload.as_str());
                            let raw = base64::engine::general_purpose::STANDARD
                                .decode(b64_str)
                                .map_err(|_| {
                                    Exception::throw_type(
                                        &ctx,
                                        "Inline media data is not valid base64.",
                                    )
                                })?;
                            drop(cleaned_owned);
                            inline_lease.shrink_to(raw.capacity());
                            if raw.len() as u64 > max_bytes {
                                return Err(Exception::throw_range(
                                    &ctx,
                                    &format!(
                                        "Inline media exceeds FASTEXEC_MAX_MEDIA_BYTES ({max_bytes} bytes)."
                                    ),
                                ));
                            }
                            let hint = (!declared_mime.is_empty()).then_some(declared_mime.as_str());
                            let det = media::detect_inline(&raw, hint)
                                .map_err(|err| Exception::throw_type(&ctx, &err))?;
                            if !expected_kind.is_empty() && det.kind.as_str() != expected_kind.as_str() {
                                return Err(Exception::throw_type(
                                    &ctx,
                                    &format!(
                                        "MCP {} block contains {} ({}) bytes.",
                                        expected_kind.as_str(),
                                        det.kind.as_str(),
                                        det.mime
                                    ),
                                ));
                            }
                            (raw, inline_lease, det.kind, det.mime.to_owned())
                        }
                    };

                    let (final_bytes, final_mime) = if compress
                        && kind == MediaKind::Image
                        && media::is_compressible_image(&mime, &bytes)
                    {
                        match media::compress_image(bytes, &mime, &mut lease) {
                            Ok(img) => (img.bytes, img.mime),
                            Err(CompressFailure::Overload) => {
                                return Err(Exception::throw_range(
                                    &ctx,
                                    "Server active memory budget is full.",
                                ));
                            }
                            Err(CompressFailure::Rejected(reason)) => {
                                return Err(Exception::throw_type(&ctx, &reason));
                            }
                        }
                    } else {
                        (bytes, mime)
                    };
                    let stored = tasks
                        .store_attachment(&task, kind, &final_mime, &final_bytes)
                        .map_err(|error| Exception::throw_range(&ctx, &error))?;
                    drop(final_bytes);
                    drop(lease);
                    Ok(json!({
                        "index": stored.index,
                        "kind": stored.kind.as_str(),
                        "mimeType": stored.mime,
                        "bytes": stored.size,
                    })
                    .to_string())
                },
            )
            .map_err(|error| js_error(&ctx, error))?;
            let control = script.clone();
            let exit = Function::new(ctx.clone(), move || {
                control.0.exited.store(true, Ordering::Release);
            })
            .map_err(|error| js_error(&ctx, error))?;
            let api: Object = factory
                .call((write, call, attach, exit))
                .map_err(|error| js_error(&ctx, error))?;
            let mut options = EvalOptions::default();
            options.filename = Some("codemode.js".into());
            let function: Function = ctx
                .eval_with_options(format!("{SCRIPT_PREFIX}{code}\n}})"), options)
                .map_err(|error| js_error(&ctx, error))?;
            let run: Function = api.get("run").map_err(|error| js_error(&ctx, error))?;
            run.call::<_, ()>((function,))
                .map_err(|error| js_error(&ctx, error))?;
            Ok(Persistent::save(&ctx, api))
        })?;
        loop {
            if script.interrupted() {
                return Ok(());
            }
            let mut jobs = 0;
            while runtime.is_job_pending() {
                runtime
                    .execute_pending_job()
                    .map_err(|error| error.0.with(|ctx| describe(&ctx, ctx.catch())))?;
                jobs += 1;
                if jobs % 50 == 0 && script.interrupted() {
                    return Ok(());
                }
            }
            if script.interrupted() {
                return Ok(());
            }
            let unhandled = rejections.borrow().values().next().cloned();
            if let Some(reason) = unhandled {
                return Err(context.with(|ctx| {
                    reason
                        .restore(&ctx)
                        .map(|value| describe(&ctx, value))
                        .unwrap_or_else(|error| error.to_string())
                }));
            }
            let finished = context.with(|ctx| -> Result<bool, String> {
                let api = api
                    .clone()
                    .restore(&ctx)
                    .map_err(|error| js_error(&ctx, error))?;
                let status: Function = api.get("status").map_err(|error| js_error(&ctx, error))?;
                let state: i32 = status.call(()).map_err(|error| js_error(&ctx, error))?;
                match state {
                    0 => Ok(false),
                    1 => Ok(true),
                    2 => {
                        let failure: Function =
                            api.get("failure").map_err(|error| js_error(&ctx, error))?;
                        let value: Value =
                            failure.call(()).map_err(|error| js_error(&ctx, error))?;
                        Err(describe(&ctx, value))
                    }
                    _ => Err(STALLED.into()),
                }
            })?;
            if finished {
                return Ok(());
            }
            match receiver.recv() {
                Ok(Wake::Settled { id, outcome }) => {
                    let settled = context.with(|ctx| -> Result<(), String> {
                        let api = api
                            .clone()
                            .restore(&ctx)
                            .map_err(|error| js_error(&ctx, error))?;
                        let settle: Function =
                            api.get("settle").map_err(|error| js_error(&ctx, error))?;
                        let (ok, range, payload) = match &outcome {
                            Outcome::Value(value) => (true, false, value.value.as_str()),
                            Outcome::Error(error) => (false, false, *error),
                            Outcome::Limit => (false, true, BRIDGE_RESULT_ERROR),
                            Outcome::Memory => (false, true, BRIDGE_MEMORY_RESULT_ERROR),
                        };
                        settle
                            .call::<_, ()>((id, ok, payload, range))
                            .map_err(|error| js_error(&ctx, error))
                    });
                    lock(&script.0.report).remove(id);
                    settled?;
                }
                Ok(Wake::Stop) | Err(_) => return Ok(()),
            }
        }
    })();
    runtime.set_host_promise_rejection_tracker(None);
    rejections.borrow_mut().clear();
    result
}

const PRELUDE: &str = r#"(function (__write, __call, __attach, __exit) {
    const parse = Function.prototype.call.bind(JSON.parse, JSON);
    const stringify = Function.prototype.call.bind(JSON.stringify, JSON);
    const get = Function.prototype.call.bind(Map.prototype.get);
    const set = Function.prototype.call.bind(Map.prototype.set);
    const remove = Function.prototype.call.bind(Map.prototype.delete);
    const then = Function.prototype.call.bind(Promise.prototype.then);
    const PromiseCtor = Promise;
    const ErrorCtor = Error;
    const TypeErrorCtor = TypeError;
    const RangeErrorCtor = RangeError;
    const StringCtor = String;
    const Uint8ArrayCtor = Uint8Array;
    const ArrayBufferCtor = ArrayBuffer;
    const DataViewCtor = DataView;
    const SharedArrayBufferCtor = typeof SharedArrayBuffer === "function" ? SharedArrayBuffer : undefined;
    const TypedArrayProto = Object.getPrototypeOf(Uint8ArrayCtor.prototype);
    const taTag = Function.prototype.call.bind(
        Object.getOwnPropertyDescriptor(TypedArrayProto, Symbol.toStringTag).get
    );
    const taBuffer = Function.prototype.call.bind(
        Object.getOwnPropertyDescriptor(TypedArrayProto, "buffer").get
    );
    const taByteOffset = Function.prototype.call.bind(
        Object.getOwnPropertyDescriptor(TypedArrayProto, "byteOffset").get
    );
    const taByteLength = Function.prototype.call.bind(
        Object.getOwnPropertyDescriptor(TypedArrayProto, "byteLength").get
    );
    const dvBuffer = Function.prototype.call.bind(
        Object.getOwnPropertyDescriptor(DataViewCtor.prototype, "buffer").get
    );
    const dvByteOffset = Function.prototype.call.bind(
        Object.getOwnPropertyDescriptor(DataViewCtor.prototype, "byteOffset").get
    );
    const dvByteLength = Function.prototype.call.bind(
        Object.getOwnPropertyDescriptor(DataViewCtor.prototype, "byteLength").get
    );
    const abByteLength = Function.prototype.call.bind(
        Object.getOwnPropertyDescriptor(ArrayBufferCtor.prototype, "byteLength").get
    );
    const abDetachedDesc = Object.getOwnPropertyDescriptor(ArrayBufferCtor.prototype, "detached");
    const abDetached = abDetachedDesc && abDetachedDesc.get
        ? Function.prototype.call.bind(abDetachedDesc.get)
        : undefined;
    const abImmutableDesc = Object.getOwnPropertyDescriptor(ArrayBufferCtor.prototype, "immutable");
    const abImmutable = abImmutableDesc && abImmutableDesc.get
        ? Function.prototype.call.bind(abImmutableDesc.get)
        : undefined;
    const sabByteLength = SharedArrayBufferCtor
        ? Function.prototype.call.bind(
            Object.getOwnPropertyDescriptor(SharedArrayBufferCtor.prototype, "byteLength").get
        )
        : undefined;
    const objectToString = Function.prototype.call.bind(Object.prototype.toString);
    const charCodeAt = Function.prototype.call.bind(String.prototype.charCodeAt);
    const stringIndexOf = Function.prototype.call.bind(String.prototype.indexOf);
    const regexpExec = Function.prototype.call.bind(RegExp.prototype.exec);
    const sourceFrame = /^(    at (?:.* \()?codemode\.js:)([0-9]+)(:[0-9]+\)?)$/;
    const stringSlice = Function.prototype.call.bind(String.prototype.slice);
    const stringToLowerCase = Function.prototype.call.bind(String.prototype.toLowerCase);
    const fromCharCode = String.fromCharCode;
    const reflectApply = Reflect.apply;
    const weakSetAdd = Function.prototype.call.bind(WeakSet.prototype.add);
    const weakSetHas = Function.prototype.call.bind(WeakSet.prototype.has);
    const weakMapSet = Function.prototype.call.bind(WeakMap.prototype.set);
    const weakMapGet = Function.prototype.call.bind(WeakMap.prototype.get);
    const defineProperty = Object.defineProperty;
    const objectFreeze = Object.freeze;
    const hasOwn = Function.prototype.call.bind(Object.prototype.hasOwnProperty);
    const numberIsFinite = Number.isFinite;
    const arrayJoin = Function.prototype.call.bind(Array.prototype.join);
    const toStringTag = Symbol.toStringTag;
    const isArray = Array.isArray;
    const errorStackDesc = Object.getOwnPropertyDescriptor(ErrorCtor.prototype, "stack");
    const getErrorStack = Function.prototype.call.bind(errorStackDesc.get);
    // The wrapper occupies one line; normalize native stacks at their shared getter.
    defineProperty(ErrorCtor.prototype, "stack", {
        __proto__: null,
        get() { return sourceStack(getErrorStack(this)); },
        set: errorStackDesc.set,
        enumerable: errorStackDesc.enumerable,
        configurable: errorStackDesc.configurable,
    });
    const pending = new Map();
    let waiting = 0;
    let state = 0;
    let failure;

    function sourceStack(raw) {
        if (typeof raw !== "string") return raw;
        let out = "";
        let pos = 0;
        while (pos < raw.length) {
            const nl = stringIndexOf(raw, "\n", pos);
            let line = nl === -1 ? stringSlice(raw, pos) : stringSlice(raw, pos, nl);
            const frame = regexpExec(sourceFrame, line);
            if (frame && +frame[2] > 1) line = frame[1] + (+frame[2] - 1) + frame[3];
            out += line;
            if (nl === -1) break;
            out += "\n";
            pos = nl + 1;
        }
        return out;
    }
    function stripPreludeFrames(raw) {
        if (typeof raw !== "string" || raw.length === 0) return "";
        let out = "";
        let pos = 0;
        while (pos < raw.length) {
            const nl = stringIndexOf(raw, "\n", pos);
            const line = nl === -1 ? stringSlice(raw, pos) : stringSlice(raw, pos, nl);
            if (line.length > 0 && stringIndexOf(line, "codemode-prelude.js") === -1) {
                out += out.length === 0 ? line : "\n" + line;
            }
            if (nl === -1) break;
            pos = nl + 1;
        }
        return out;
    }
    function readErrorStack(error) {
        if (error === null || typeof error !== "object") return "";
        try { return stripPreludeFrames(error.stack); }
        catch { return ""; }
    }
    function captureCallStack() {
        try { return readErrorStack(new ErrorCtor()); }
        catch { return ""; }
    }
    function withCallStack(error, stack) {
        if (error !== null && typeof error === "object") {
            const chosen = readErrorStack(error) || stack;
            if (chosen) {
                try {
                    defineProperty(error, "stack", {
                        __proto__: null,
                        value: chosen,
                        writable: true,
                        enumerable: false,
                        configurable: true,
                    });
                } catch {}
            }
        }
        return error;
    }

    function safeString(value) {
        try { return StringCtor(value); }
        catch {
            try { return objectToString(value); }
            catch { return "[unprintable]"; }
        }
    }
    function text(value) {
        const json = typeof value === "string" ? value : stringify(value);
        __write(json === undefined ? "undefined" : json);
    }
    function format(value) {
        let isErr = false;
        try { isErr = value instanceof ErrorCtor; } catch {}
        if (!isErr && value !== null && typeof value === "object") {
            try { isErr = objectToString(value) === "[object Error]"; } catch {}
        }
        if (isErr) {
            let name = "Error";
            let message = "";
            let stack = "";
            try { if (value.name !== undefined) name = safeString(value.name); } catch {}
            try { if (value.message !== undefined) message = safeString(value.message); } catch {}
            try { if (value.stack) stack = safeString(value.stack); } catch {}
            const head = name + ": " + message;
            return stack ? head + "\n" + stack : head;
        }
        if (typeof value === "string" || typeof value === "number") return StringCtor(value);
        try {
            const json = stringify(value);
            return json === undefined ? safeString(value) : json;
        } catch { return safeString(value); }
    }
    const console = {};
    for (const level of ["log", "info", "warn", "error", "debug"]) {
        console[level] = (...args) => {
            let line = "";
            for (let i = 0; i < args.length; i++) line += (i ? " " : "") + format(args[i]);
            __write(line);
        };
    }
    function toolErrorMessage(value) {
        if (value && typeof value === "object") {
            if (typeof value.error === "string" && value.error.length > 0) return value.error;
            const parts = [];
            if (typeof value.inputError === "string" && value.inputError.length > 0) {
                parts[parts.length] = value.inputError;
            }
            if (typeof value.captureError === "string" && value.captureError.length > 0) {
                parts[parts.length] = "output capture failed: " + value.captureError;
            }
            if (typeof value.logError === "string" && value.logError.length > 0) {
                parts[parts.length] = "log write failed: " + value.logError;
            }
            if (parts.length > 0) return arrayJoin(parts, "; ");
        }
        return "Tool operation failed.";
    }
    class ToolError extends ErrorCtor {
        constructor(tool, result) {
            super(toolErrorMessage(result));
            defineProperty(this, "name", {
                __proto__: null,
                value: "ToolError",
                writable: true,
                enumerable: false,
                configurable: true,
            });
            defineProperty(this, "tool", {
                __proto__: null,
                value: tool,
                writable: true,
                enumerable: true,
                configurable: true,
            });
            defineProperty(this, "result", {
                __proto__: null,
                value: result,
                writable: true,
                enumerable: true,
                configurable: true,
            });
        }
    }
    const members = Object.create(null);
    for (const name of ["exec", "read", "write", "edit"]) {
        members[name] = args => {
            const callStack = captureCallStack();
            return new PromiseCtor((resolve, reject) => {
                if (args === null || typeof args !== "object" || isArray(args)) {
                    reject(withCallStack(new TypeErrorCtor("Tool arguments must be an object."), callStack));
                    return;
                }
                try {
                    const id = __call(name, stringify(args));
                    set(pending, id, { resolve, reject, tool: name, stack: callStack });
                    waiting++;
                } catch (error) { reject(withCallStack(error, callStack)); }
            });
        };
    }
    function sleep(ms) {
        const callStack = captureCallStack();
        return new PromiseCtor((resolve, reject) => {
            if (typeof ms !== "number" || !numberIsFinite(ms)) {
                reject(withCallStack(new TypeErrorCtor("sleep requires a finite number of milliseconds."), callStack));
                return;
            }
            try {
                const id = __call("sleep", ms <= 0 ? "0" : StringCtor(ms));
                set(pending, id, { resolve, reject, tool: "sleep", stack: callStack });
                waiting++;
            } catch (error) { reject(withCallStack(error, callStack)); }
        });
    }
    const tools = new Proxy(members, {
        __proto__: null,
        get(object, name) {
            if (typeof name !== "string") return undefined;
            if (name === "exec" || name === "read" || name === "write" || name === "edit") return object[name];
            throw new TypeErrorCtor("tools." + name + " does not exist. Available: exec, read, write, edit.");
        }
    });
    const shHelpers = {
        quote(value) {
            let str;
            if (typeof value === "string") {
                str = value;
            } else if (typeof value === "number") {
                if (!numberIsFinite(value)) {
                    throw new TypeErrorCtor("sh.quote requires a string or finite number.");
                }
                str = StringCtor(value);
            } else {
                throw new TypeErrorCtor("sh.quote requires a string or finite number.");
            }
            if (stringIndexOf(str, "\0") !== -1) {
                throw new TypeErrorCtor("sh.quote string must not contain NUL (U+0000).");
            }
            let out = "'";
            let start = 0;
            while (true) {
                const at = stringIndexOf(str, "'", start);
                if (at === -1) {
                    out += start === 0 ? str : stringSlice(str, start);
                    break;
                }
                out += stringSlice(str, start, at) + "'\\''";
                start = at + 1;
            }
            return out + "'";
        },
        join(values) {
            if (!isArray(values)) {
                throw new TypeErrorCtor("sh.join requires an array.");
            }
            const len = values.length;
            if (typeof len !== "number" || (len >>> 0) !== len) {
                throw new TypeErrorCtor("sh.join array has an invalid length.");
            }
            let out = "";
            for (let i = 0; i < len; i++) {
                if (!hasOwn(values, i)) {
                    throw new TypeErrorCtor("sh.join array must not contain holes.");
                }
                const word = quote(values[i]);
                out += i === 0 ? word : " " + word;
            }
            return out;
        },
        sh(strings) {
            if (strings === null || typeof strings !== "object" || !hasOwn(strings, "raw")) {
                throw new TypeErrorCtor("sh must be called as a tagged template.");
            }
            const raw = strings.raw;
            if (!isArray(raw)) {
                throw new TypeErrorCtor("sh template strings.raw must be an array.");
            }
            const len = raw.length;
            if (typeof len !== "number" || (len >>> 0) !== len || len !== arguments.length) {
                throw new TypeErrorCtor("sh template strings.raw length does not match values.");
            }
            let out = "";
            for (let i = 0; i < len; i++) {
                if (!hasOwn(raw, i)) {
                    throw new TypeErrorCtor("sh template strings.raw must not contain holes.");
                }
                const seg = raw[i];
                if (typeof seg !== "string") {
                    throw new TypeErrorCtor("sh template segments must be strings.");
                }
                if (stringIndexOf(seg, "\0") !== -1) {
                    throw new TypeErrorCtor("sh template must not contain NUL (U+0000).");
                }
                out += seg;
                if (i + 1 < len) {
                    const val = arguments[i + 1];
                    out += isArray(val) ? join(val) : quote(val);
                }
            }
            return out;
        },
    };
    const quote = objectFreeze(shHelpers.quote);
    const join = objectFreeze(shHelpers.join);
    const sh = shHelpers.sh;
    defineProperty(sh, "quote", {
        __proto__: null,
        value: quote,
        writable: false,
        enumerable: true,
        configurable: false,
    });
    defineProperty(sh, "join", {
        __proto__: null,
        value: join,
        writable: false,
        enumerable: true,
        configurable: false,
    });
    objectFreeze(sh);
    const attachmentHandles = new WeakMap();
    function parseDataUrl(value) {
        if (value.length < 12) return undefined;
        if (stringToLowerCase(stringSlice(value, 0, 5)) !== "data:") return undefined;
        const comma = stringIndexOf(value, ",", 5);
        if (comma < 12) {
            throw new TypeErrorCtor("attach data URL must include ';base64,'.");
        }
        const header = stringSlice(value, 5, comma);
        if (stringToLowerCase(stringSlice(header, header.length - 7)) !== ";base64") {
            throw new TypeErrorCtor("attach data URL must use ;base64 encoding.");
        }
        return {
            mime: stringSlice(header, 0, header.length - 7),
            data: stringSlice(value, comma + 1),
        };
    }
    function attach(value) {
        if (value !== null && typeof value === "object" && !isArray(value)) {
            const hid = weakMapGet(attachmentHandles, value);
            if (hid !== undefined) {
                return parse(__attach(0, StringCtor(hid), "", ""));
            }
            if (value.attachable === false) {
                const note = typeof value.note === "string" && value.note.length > 0
                    ? value.note
                    : "The media file cannot be attached.";
                throw new TypeErrorCtor(note);
            }
            if (typeof value.path === "string" && value.type === undefined) {
                return parse(__attach(1, value.path, "", ""));
            }
            if ((value.type === "image" || value.type === "audio") && typeof value.data === "string") {
                if (value.mimeType !== undefined && typeof value.mimeType !== "string") {
                    throw new TypeErrorCtor("MCP " + value.type + " block mimeType must be a string.");
                }
                return parse(__attach(2, value.data, value.mimeType || "", value.type));
            }
            if (
                value.type === "resource" &&
                value.resource !== null &&
                typeof value.resource === "object" &&
                typeof value.resource.blob === "string"
            ) {
                const res = value.resource;
                if (res.mimeType !== undefined && typeof res.mimeType !== "string") {
                    throw new TypeErrorCtor("MCP resource block mimeType must be a string.");
                }
                if (res.uri !== undefined && typeof res.uri !== "string") {
                    throw new TypeErrorCtor("MCP resource block uri must be a string.");
                }
                return parse(__attach(2, res.blob, res.mimeType || "", ""));
            }
        }
        if (typeof value === "string") {
            const dataUrl = parseDataUrl(value);
            if (dataUrl !== undefined) {
                return parse(__attach(2, dataUrl.data, dataUrl.mime, ""));
            }
            return parse(__attach(1, value, "", ""));
        }
        throw new TypeErrorCtor(
            "attach expects a file path, a read media descriptor, an opaque task attachment handle, an MCP image/audio/resource(blob) block, or a base64 data URL."
        );
    }
    function sealNestedAttachments(val) {
        if (
            val !== null &&
            typeof val === "object" &&
            val.codemode !== null &&
            typeof val.codemode === "object" &&
            isArray(val.codemode.attachments)
        ) {
            const rawList = val.codemode.attachments;
            const out = [];
            for (let i = 0; i < rawList.length; i++) {
                const item = rawList[i];
                const hid = item.__hid;
                const handle = objectFreeze({
                    index: item.index,
                    kind: item.kind,
                    mimeType: item.mimeType,
                    bytes: item.bytes,
                });
                if (typeof hid === "number") {
                    weakMapSet(attachmentHandles, handle, hid);
                }
                out[i] = handle;
            }
            val.codemode.attachments = objectFreeze(out);
        }
        return val;
    }
    function exit() {
        __exit();
        // QuickJS's interrupt is uncatchable, including inside a script's try/catch.
        while (true) {}
    }

    const encoders = new WeakSet();
    class TextEncoder {
        constructor() {
            weakSetAdd(encoders, this);
        }
        get encoding() {
            if (!weakSetHas(encoders, this)) throw new TypeErrorCtor("Illegal invocation");
            return "utf-8";
        }
        encode(input = "") {
            if (!weakSetHas(encoders, this)) throw new TypeErrorCtor("Illegal invocation");
            const str = input === undefined ? "" : `${input}`;
            const len = str.length;
            let total = 0;
            for (let i = 0; i < len; i++) {
                const c = charCodeAt(str, i);
                if (c < 0x80) total += 1;
                else if (c < 0x800) total += 2;
                else if (c >= 0xD800 && c <= 0xDBFF && i + 1 < len) {
                    const d = charCodeAt(str, i + 1);
                    if (d >= 0xDC00 && d <= 0xDFFF) { total += 4; i++; }
                    else total += 3;
                } else total += 3;
            }
            const out = new Uint8ArrayCtor(total);
            let pos = 0;
            for (let i = 0; i < len; i++) {
                const c = charCodeAt(str, i);
                if (c < 0x80) {
                    out[pos++] = c;
                } else if (c < 0x800) {
                    out[pos++] = 0xC0 | (c >> 6);
                    out[pos++] = 0x80 | (c & 0x3F);
                } else if (c >= 0xD800 && c <= 0xDFFF) {
                    if (c <= 0xDBFF && i + 1 < len) {
                        const d = charCodeAt(str, i + 1);
                        if (d >= 0xDC00 && d <= 0xDFFF) {
                            const cp = 0x10000 + ((c - 0xD800) << 10) + (d - 0xDC00);
                            out[pos++] = 0xF0 | (cp >> 18);
                            out[pos++] = 0x80 | ((cp >> 12) & 0x3F);
                            out[pos++] = 0x80 | ((cp >> 6) & 0x3F);
                            out[pos++] = 0x80 | (cp & 0x3F);
                            i++;
                            continue;
                        }
                    }
                    out[pos++] = 0xEF;
                    out[pos++] = 0xBF;
                    out[pos++] = 0xBD;
                } else {
                    out[pos++] = 0xE0 | (c >> 12);
                    out[pos++] = 0x80 | ((c >> 6) & 0x3F);
                    out[pos++] = 0x80 | (c & 0x3F);
                }
            }
            return out;
        }
        encodeInto(source, destination) {
            if (!weakSetHas(encoders, this)) throw new TypeErrorCtor("Illegal invocation");
            if (arguments.length < 2) {
                throw new TypeErrorCtor("TextEncoder.encodeInto requires 2 arguments.");
            }
            const str = `${source}`;
            if (destination === null || typeof destination !== "object" || taTag(destination) !== "Uint8Array") {
                throw new TypeErrorCtor("TextEncoder.encodeInto destination must be a Uint8Array.");
            }
            if (abImmutable) {
                const buf = taBuffer(destination);
                let ro = false;
                try { ro = abImmutable(buf); } catch {}
                if (ro) throw new TypeErrorCtor("TextEncoder.encodeInto destination buffer is immutable.");
            }
            const cap = taByteLength(destination);
            const len = str.length;
            let read = 0;
            let written = 0;
            while (read < len) {
                const c = charCodeAt(str, read);
                if (c < 0x80) {
                    if (written + 1 > cap) break;
                    destination[written++] = c;
                    read++;
                } else if (c < 0x800) {
                    if (written + 2 > cap) break;
                    destination[written++] = 0xC0 | (c >> 6);
                    destination[written++] = 0x80 | (c & 0x3F);
                    read++;
                } else if (c >= 0xD800 && c <= 0xDFFF) {
                    if (c <= 0xDBFF && read + 1 < len) {
                        const d = charCodeAt(str, read + 1);
                        if (d >= 0xDC00 && d <= 0xDFFF) {
                            if (written + 4 > cap) break;
                            const cp = 0x10000 + ((c - 0xD800) << 10) + (d - 0xDC00);
                            destination[written++] = 0xF0 | (cp >> 18);
                            destination[written++] = 0x80 | ((cp >> 12) & 0x3F);
                            destination[written++] = 0x80 | ((cp >> 6) & 0x3F);
                            destination[written++] = 0x80 | (cp & 0x3F);
                            read += 2;
                            continue;
                        }
                    }
                    if (written + 3 > cap) break;
                    destination[written++] = 0xEF;
                    destination[written++] = 0xBF;
                    destination[written++] = 0xBD;
                    read++;
                } else {
                    if (written + 3 > cap) break;
                    destination[written++] = 0xE0 | (c >> 12);
                    destination[written++] = 0x80 | ((c >> 6) & 0x3F);
                    destination[written++] = 0x80 | (c & 0x3F);
                    read++;
                }
            }
            return { read, written };
        }
    }
    defineProperty(TextEncoder.prototype, toStringTag, {
        __proto__: null,
        value: "TextEncoder",
        configurable: true,
    });

    function asciiLabel(raw) {
        const str = `${raw}`;
        let start = 0;
        let end = str.length;
        while (start < end) {
            const c = charCodeAt(str, start);
            if (c === 0x09 || c === 0x0A || c === 0x0C || c === 0x0D || c === 0x20) start++;
            else break;
        }
        while (end > start) {
            const c = charCodeAt(str, end - 1);
            if (c === 0x09 || c === 0x0A || c === 0x0C || c === 0x0D || c === 0x20) end--;
            else break;
        }
        let out = "";
        for (let i = start; i < end; i++) {
            const c = charCodeAt(str, i);
            if (c > 0x7F) return "";
            out += c >= 0x41 && c <= 0x5A ? fromCharCode(c + 0x20) : fromCharCode(c);
        }
        return out;
    }

    function isUtf8Label(label) {
        return (
            label === "utf-8" ||
            label === "utf8" ||
            label === "unicode-1-1-utf-8" ||
            label === "unicode11utf8" ||
            label === "unicode20utf8" ||
            label === "x-unicode20utf8"
        );
    }

    function isDetached(buf) {
        if (!abDetached) return false;
        try { return abDetached(buf); } catch { return false; }
    }

    function isBufferSource(input) {
        if (input === undefined) return true;
        if (input === null || typeof input !== "object") return false;
        if (taTag(input) !== undefined) return true;
        try { dvBuffer(input); return true; } catch {}
        try { abByteLength(input); return true; } catch {}
        if (sabByteLength) {
            try { sabByteLength(input); return true; } catch {}
        }
        return false;
    }

    function asByteView(input) {
        if (input === undefined) return new Uint8ArrayCtor(0);
        if (taTag(input) !== undefined) {
            const buf = taBuffer(input);
            if (isDetached(buf)) return new Uint8ArrayCtor(0);
            const len = taByteLength(input);
            return len === 0 ? new Uint8ArrayCtor(0) : new Uint8ArrayCtor(buf, taByteOffset(input), len);
        }
        let dvBuf;
        let isDv = false;
        try { dvBuf = dvBuffer(input); isDv = true; } catch {}
        if (isDv) {
            if (isDetached(dvBuf)) return new Uint8ArrayCtor(0);
            let off = 0;
            let len = 0;
            try {
                off = dvByteOffset(input);
                len = dvByteLength(input);
            } catch {
                return new Uint8ArrayCtor(0);
            }
            return len === 0 ? new Uint8ArrayCtor(0) : new Uint8ArrayCtor(dvBuf, off, len);
        }
        let isAb = false;
        try { abByteLength(input); isAb = true; } catch {}
        if (isAb) {
            return isDetached(input) ? new Uint8ArrayCtor(0) : new Uint8ArrayCtor(input);
        }
        if (sabByteLength) {
            let isSab = false;
            try { sabByteLength(input); isSab = true; } catch {}
            if (isSab) return new Uint8ArrayCtor(input);
        }
        throw new TypeErrorCtor("TextDecoder.decode input must be an ArrayBuffer, SharedArrayBuffer, or ArrayBufferView.");
    }

    const decodeUnits = [];
    for (let i = 0; i < 1024; i++) decodeUnits[i] = 0;
    const decoders = new WeakMap();
    class TextDecoder {
        constructor(label = "utf-8", options = undefined) {
            const normalized = label === undefined ? "utf-8" : asciiLabel(label);
            if (options !== undefined && options !== null && typeof options !== "object" && typeof options !== "function") {
                throw new TypeErrorCtor("TextDecoder options must be an object.");
            }
            const fatal = Boolean(options && options.fatal);
            const ignoreBOM = Boolean(options && options.ignoreBOM);
            if (!isUtf8Label(normalized)) {
                throw new RangeErrorCtor("TextDecoder only supports utf-8 encoding.");
            }
            weakMapSet(decoders, this, {
                fatal,
                ignoreBOM,
                doNotFlush: false,
                bomSeen: false,
                pending: undefined,
                cp: 0,
                seen: 0,
                needed: 0,
                lower: 0x80,
                upper: 0xBF,
            });
        }
        get encoding() {
            if (!weakMapGet(decoders, this)) throw new TypeErrorCtor("Illegal invocation");
            return "utf-8";
        }
        get fatal() {
            const s = weakMapGet(decoders, this);
            if (!s) throw new TypeErrorCtor("Illegal invocation");
            return s.fatal;
        }
        get ignoreBOM() {
            const s = weakMapGet(decoders, this);
            if (!s) throw new TypeErrorCtor("Illegal invocation");
            return s.ignoreBOM;
        }
        decode(input = undefined, options = undefined) {
            const s = weakMapGet(decoders, this);
            if (!s) throw new TypeErrorCtor("Illegal invocation");
            if (!isBufferSource(input)) {
                throw new TypeErrorCtor("TextDecoder.decode input must be an ArrayBuffer, SharedArrayBuffer, or ArrayBufferView.");
            }
            if (options !== undefined && options !== null && typeof options !== "object" && typeof options !== "function") {
                throw new TypeErrorCtor("TextDecoder decode options must be an object.");
            }
            const stream = Boolean(options && options.stream);
            let bytes = asByteView(input);
            const reset = () => {
                s.cp = 0;
                s.seen = 0;
                s.needed = 0;
                s.lower = 0x80;
                s.upper = 0xBF;
            };
            if (!s.doNotFlush) {
                reset();
                s.bomSeen = false;
                s.pending = undefined;
            }
            s.doNotFlush = stream;
            if (s.pending !== undefined) {
                const head = s.pending;
                const headLen = taByteLength(head);
                const tailLen = taByteLength(bytes);
                const joined = new Uint8ArrayCtor(headLen + tailLen);
                for (let i = 0; i < headLen; i++) joined[i] = head[i];
                for (let i = 0; i < tailLen; i++) joined[headLen + i] = bytes[i];
                bytes = joined;
                s.pending = undefined;
            }
            let bomSeen = s.bomSeen;
            const len = taByteLength(bytes);
            const fail = start => {
                // Fatal streaming errors leave unread input queued for the next decode.
                if (stream && start < len) {
                    const pending = new Uint8ArrayCtor(len - start);
                    for (let i = start; i < len; i++) pending[i - start] = bytes[i];
                    s.pending = pending;
                }
                throw new TypeErrorCtor("The encoded data was not valid utf-8.");
            };
            let out = "";
            let unitCount = 0;
            const pushUnit = unit => {
                decodeUnits[unitCount++] = unit;
                if (unitCount === 1024) {
                    out += reflectApply(fromCharCode, null, decodeUnits);
                    unitCount = 0;
                }
            };
            const emit = scalar => {
                if (!bomSeen) {
                    bomSeen = true;
                    if (!s.ignoreBOM && scalar === 0xFEFF) return;
                }
                if (scalar <= 0xFFFF) {
                    pushUnit(scalar);
                } else {
                    const offset = scalar - 0x10000;
                    pushUnit(0xD800 + (offset >> 10));
                    pushUnit(0xDC00 + (offset & 0x3FF));
                }
            };
            for (let i = 0; i < len; i++) {
                const byte = bytes[i];
                if (s.needed === 0) {
                    if (byte <= 0x7F) {
                        emit(byte);
                    } else if (byte >= 0xC2 && byte <= 0xDF) {
                        s.needed = 1;
                        s.cp = byte & 0x1F;
                    } else if (byte >= 0xE0 && byte <= 0xEF) {
                        if (byte === 0xE0) s.lower = 0xA0;
                        else if (byte === 0xED) s.upper = 0x9F;
                        s.needed = 2;
                        s.cp = byte & 0x0F;
                    } else if (byte >= 0xF0 && byte <= 0xF4) {
                        if (byte === 0xF0) s.lower = 0x90;
                        else if (byte === 0xF4) s.upper = 0x8F;
                        s.needed = 3;
                        s.cp = byte & 0x07;
                    } else if (s.fatal) {
                        reset();
                        fail(i + 1);
                    } else {
                        emit(0xFFFD);
                    }
                } else if (byte < s.lower || byte > s.upper) {
                    reset();
                    if (s.fatal) fail(i);
                    emit(0xFFFD);
                    i--;
                } else {
                    s.lower = 0x80;
                    s.upper = 0xBF;
                    s.cp = (s.cp << 6) | (byte & 0x3F);
                    s.seen++;
                    if (s.seen === s.needed) {
                        const scalar = s.cp;
                        reset();
                        emit(scalar);
                    }
                }
            }
            if (!stream && s.needed !== 0) {
                reset();
                if (s.fatal) fail(len);
                emit(0xFFFD);
            }
            s.bomSeen = bomSeen;
            for (let j = 0; j < unitCount; j++) {
                out += fromCharCode(decodeUnits[j]);
            }
            return out;
        }
    }
    defineProperty(TextDecoder.prototype, toStringTag, {
        __proto__: null,
        value: "TextDecoder",
        configurable: true,
    });

    defineProperty(globalThis, "TextEncoder", {
        __proto__: null,
        value: TextEncoder,
        writable: true,
        enumerable: false,
        configurable: true,
    });
    defineProperty(globalThis, "TextDecoder", {
        __proto__: null,
        value: TextDecoder,
        writable: true,
        enumerable: false,
        configurable: true,
    });
    globalThis.tools = tools;
    globalThis.sh = sh;
    globalThis.console = console;
    globalThis.text = text;
    globalThis.attach = attach;
    globalThis.sleep = sleep;
    globalThis.exit = exit;
    return {
        run(fn) {
            then(fn(), value => {
                try {
                    if (value !== undefined) {
                        if (typeof value === "string") {
                            __write(value);
                        } else {
                            let json;
                            try {
                                json = stringify(value);
                            } catch {
                                throw new TypeErrorCtor("the return value is not JSON-serializable");
                            }
                            if (json === undefined) {
                                throw new TypeErrorCtor("the return value is not JSON-serializable");
                            }
                            __write(json);
                        }
                    }
                    state = 1;
                }
                catch (error) { failure = error; state = 2; }
            }, error => { failure = error; state = 2; });
        },
        settle(id, ok, payload, range) {
            const entry = get(pending, id);
            if (!entry) return;
            remove(pending, id);
            waiting--;
            if (!ok) {
                const error = range ? new RangeErrorCtor(payload) : new ErrorCtor(payload);
                entry.reject(withCallStack(error, entry.stack));
                return;
            }
            if (entry.tool === "sleep") {
                entry.resolve(undefined);
                return;
            }
            let envelope;
            try { envelope = parse(payload); } catch (error) { entry.reject(withCallStack(error, entry.stack)); return; }
            if (envelope.ok) {
                entry.resolve(sealNestedAttachments(envelope.value));
            } else {
                entry.reject(withCallStack(new ToolError(entry.tool, envelope.value), entry.stack));
            }
        },
        status() { return state || (waiting ? 0 : 3); },
        failure() { return failure; }
    };
})"#;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::LaunchContext;
    use crate::tasks::{self, Limits, ScriptArgs};
    use std::time::{Duration, Instant};

    struct Fixture {
        tasks: Arc<Tasks>,
        service: Arc<Service>,
        session: Arc<Session>,
        dir: std::path::PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "fastexec-script-test-{}",
                tasks::random_chars(12).unwrap()
            ));
            let tasks = Tasks::new_in(Limits::OFF, dir.clone()).unwrap();
            let service = Arc::new(Service::new(Arc::clone(&tasks), false));
            let session = Session::new(
                "script-tests".into(),
                "script-tests".into(),
                Arc::new(LaunchContext::new(
                    dir.clone(),
                    std::env::vars_os().collect(),
                )),
            );
            Self {
                tasks,
                service,
                session,
                dir,
            }
        }

        fn start(&self, code: &str, deadline: Option<u64>) -> Arc<Task> {
            self.tasks
                .start_script(
                    ScriptArgs {
                        code: code.into(),
                        launch: Arc::clone(&self.session.launch),
                        kill_after_ms: deadline,
                        transfer: false,
                        call_id: None,
                        service: Arc::clone(&self.service),
                        input_owner: Default::default(),
                    },
                    &self.session,
                    tasks::random_chars(4).unwrap(),
                )
                .unwrap()
                .0
        }

        async fn result(&self, code: &str) -> (crate::tasks::Snapshot, String) {
            let task = self.start(code, Some(5000));
            complete(&task).await;
            (
                task.snapshot(),
                std::fs::read_to_string(&task.log_path).unwrap(),
            )
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.tasks.shutdown();
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    async fn complete(task: &Task) {
        let mut done = task.done();
        tokio::time::timeout(Duration::from_secs(8), async {
            while !*done.borrow_and_update() {
                done.changed().await.unwrap();
            }
        })
        .await
        .expect("script did not finish");
    }

    #[test]
    fn nested_precommit_prepaid_wire_covers_envelope_and_needs_no_new_admission() {
        let mut structured = rmcp::model::CallToolResult::success(Vec::new());
        structured.structured_content =
            Some(json!({"ok":true,"text":"hello λ\n","nested":{"value":"quote\""}}));
        let cases = [
            (
                structured,
                json!({"ok":true,"value":{"text":"hello λ\n","nested":{"value":"quote\""}}}),
            ),
            (
                rmcp::model::CallToolResult::error(vec![rmcp::model::ContentBlock::text(
                    "ordinary error",
                )]),
                json!({"ok":false,"value":{"error":"ordinary error"}}),
            ),
            (
                rmcp::model::CallToolResult::success(Vec::new()),
                json!({"ok":false,"value":{"error":"Tool operation failed."}}),
            ),
        ];
        for (result, expected) in cases {
            let encoded = serde_json::to_string(&expected).unwrap();
            let wire = crate::ipc::json_len(&result).unwrap() + 256;
            assert!(128 + encoded.len() <= wire);
            let payload = service::result_payload_capacity(&result);
            let pool = BytePool::new(payload + wire);
            let tracked = files::TrackedResult::owned(
                result,
                crate::lease::Owner::new(pool.try_acquire(payload).unwrap()),
            );
            let prepaid = pool.try_acquire(wire).unwrap();
            assert!(pool.try_acquire(1).is_none());
            let outcome = encode_nested_result(tracked, &pool, None, Some(prepaid));
            let Outcome::Value(value) = outcome else {
                panic!("the prepaid result must encode");
            };
            assert_eq!(value.value, encoded);
            assert_eq!(pool.used(), value.value.capacity());
            let (sender, receiver) = mpsc::channel();
            sender.send(value).unwrap();
            drop(sender);
            assert_eq!(pool.used(), encoded.len());
            drop(receiver);
            assert_eq!(pool.used(), 0);
        }
    }

    #[test]
    fn nested_precommit_small_quota_replacement_is_atomic() {
        let info = || CallInfo {
            tool: "exec".into(),
            action: None,
            task_id: None,
        };
        let mut report = Report {
            limit: Some(20),
            ..Default::default()
        };
        report.admit(1, info(), 8, Default::default()).unwrap();
        report.admit(2, info(), 2, Default::default()).unwrap();
        assert!(report.result(1, 18));
        assert_eq!(report.bridge_bytes, 20);
        assert!(!report.result(1, 19));
        assert_eq!((report.bridge_bytes, report.pending[&1].bytes), (20, 18));
        assert!(report.result(1, 3));
        assert_eq!(report.bridge_bytes, 5);
        report.remove(1);
        report.remove(2);
        assert_eq!(report.bridge_bytes, 0);
    }

    #[test]
    fn text_buffer_nested_envelope_moves_existing_strings() {
        let pool = BytePool::new(512);
        let mut body = String::with_capacity(128);
        body.push_str("hello");
        let pointer = body.as_ptr();
        let mut fields = serde_json::Map::new();
        fields.insert("text".into(), serde_json::Value::String(body));
        let mut result = rmcp::model::CallToolResult::success(Vec::new());
        result.structured_content = Some(serde_json::Value::Object(fields));
        let tracked = files::TrackedResult {
            result,
            lease: pool.try_acquire(160),
            overload: false,
            task_meta: None,
            payload_owner: Default::default(),
        };
        let workspace = pool.try_acquire(128).unwrap();
        let envelope = nested_envelope(tracked);
        assert_eq!(
            envelope.value["value"]["text"].as_str().unwrap().as_ptr(),
            pointer
        );
        assert_eq!(pool.used(), 288);
        drop(envelope);
        assert_eq!(pool.used(), 128);
        drop(workspace);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn text_buffer_bridge_reserves_exact_encoding_and_keeps_queue_ownership() {
        let expected = serde_json::to_string(&json!({
            "ok": false, "value": { "error": "hello λ\n" }
        }))
        .unwrap();
        for (budget, fits) in [
            (128 + 128 + expected.len() - 1, false),
            (128 + 128 + expected.len(), true),
        ] {
            let pool = BytePool::new(budget);
            let mut body = String::with_capacity(128);
            body.push_str("hello λ\n");
            let tracked = files::TrackedResult {
                result: rmcp::model::CallToolResult::error(vec![rmcp::model::ContentBlock::text(
                    body,
                )]),
                lease: pool.try_acquire(128),
                overload: false,
                task_meta: None,
                payload_owner: Default::default(),
            };
            let outcome = encode_nested_result(tracked, &pool, None, None);
            if !fits {
                assert!(matches!(outcome, Outcome::Memory));
                assert_eq!(pool.used(), 0);
                continue;
            }
            let (sender, receiver) = mpsc::channel();
            assert!(sender.send(Wake::Settled { id: 1, outcome }).is_ok());
            drop(sender);
            assert_eq!(pool.used(), expected.len());
            let Wake::Settled {
                outcome: Outcome::Value(payload),
                ..
            } = receiver.recv().unwrap()
            else {
                panic!("a completed bridge result");
            };
            assert_eq!(payload.value, expected);
            let runtime = Runtime::new().unwrap();
            let context = Context::full(&runtime).unwrap();
            context.with(|ctx| {
                let parse: Function = ctx
                    .eval("(value) => JSON.parse(value).value.error === 'hello λ\\n'")
                    .unwrap();
                assert!(parse.call::<_, bool>((payload.value.as_str(),)).unwrap());
                assert_eq!(pool.used(), expected.len());
            });
            drop(payload);
            assert_eq!(pool.used(), 0);
        }
    }

    #[test]
    fn nested_control_wire_uses_bounded_reserve_and_keeps_queue_owner() {
        let expected = serde_json::to_string(&json!({"ok":true,"value":{"tasks":[]}})).unwrap();
        for main_limit in [0, 128] {
            for extra in [-1isize, 0, 1] {
                let mut result = rmcp::model::CallToolResult::success(Vec::new());
                result.structured_content = Some(json!({"ok":true,"tasks":[]}));
                let payload = service::result_payload_capacity(&result);
                let pool = BytePool::new(main_limit);
                let needed = payload + 128 + expected.len();
                let control = BytePool::new(needed.saturating_add_signed(extra));
                let result = files::TrackedResult::owned(
                    result,
                    crate::lease::Owner::new(control.try_acquire(payload).unwrap()),
                );
                let outcome = encode_nested_result(result, &pool, Some(&control), None);
                if extra < 0 {
                    assert!(matches!(outcome, Outcome::Memory));
                    assert_eq!((pool.used(), control.used()), (0, 0));
                    continue;
                }
                let (sender, receiver) = mpsc::channel();
                sender.send(Wake::Settled { id: 1, outcome }).unwrap();
                drop(sender);
                assert_eq!(pool.used(), 0);
                assert_eq!(control.used(), expected.len());
                let Wake::Settled {
                    outcome: Outcome::Value(value),
                    ..
                } = receiver.recv().unwrap()
                else {
                    panic!("a control response");
                };
                assert_eq!(value.value, expected);
                drop(value);
                assert_eq!(control.used(), 0);
            }
        }
    }

    #[tokio::test]
    async fn nested_control_replies_complete_with_main_full_and_sleep_stays_ordinary() {
        let mut fixture = Fixture::new();
        let main = BytePool::new(0);
        let control = BytePool::new(8 << 20);
        fixture.session = Session::new(
            "control".into(),
            "control".into(),
            Arc::new(
                fixture
                    .session
                    .launch
                    .for_session()
                    .with_memory(Arc::clone(&main))
                    .with_control_memory(Arc::clone(&control)),
            ),
        );
        let (snapshot, output) = fixture.result(
            "const listed = await tools.exec({action:'list',scope:'all'});\n\
             if(!Array.isArray(listed.tasks)) throw new Error('list did not return');\n\
             try { await tools.exec({action:'kill',taskId:'aaaaaaaa-1'}); throw new Error('expected unknown task'); }\n\
             catch(e) { if(!String(e).includes('Unknown taskId')) throw e; }\n\
             try { await sleep(0); throw new Error('sleep used control reserve'); }\n\
             catch(e) { if(!String(e).includes('not dispatched')) throw e; }\n\
             text('control-complete');"
        ).await;
        assert_eq!(snapshot.exit_code, Some(0), "{output}");
        assert_eq!(output, "control-complete\n");
        assert_eq!((main.used(), control.used()), (0, 0));
    }

    #[tokio::test]
    async fn text_buffer_nested_file_round_trip_releases_rust_staging() {
        let mut fixture = Fixture::new();
        let pool = BytePool::new(2 << 20);
        fixture.session = Session::new(
            "text-bridge".into(),
            "text-bridge".into(),
            Arc::new(
                fixture
                    .session
                    .launch
                    .for_session()
                    .with_memory(Arc::clone(&pool)),
            ),
        );
        let (snapshot, output) = fixture.result(
            "await tools.write({path:'small.txt',content:'alpha\\nbeta\\n'});\n\
             const whole = await tools.read({path:'small.txt'});\n\
             const slice = await tools.read({path:'small.txt',offset:2,limit:1});\n\
             if(whole.text !== 'alpha\\nbeta\\n' || slice.text !== 'beta\\n') throw new Error('text changed');\n\
             text('text-round-trip');"
        ).await;
        assert_eq!(snapshot.exit_code, Some(0), "{output}");
        assert_eq!(output, "text-round-trip\n");
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn text_buffer_bridge_control_classification_preserves_main_admission() {
        let fixture = Fixture::new();
        let memory = BytePool::new(0);
        let control = BytePool::new(4096);
        let launch = Arc::new(
            fixture
                .session
                .launch
                .for_session()
                .with_memory(Arc::clone(&memory))
                .with_control_memory(Arc::clone(&control)),
        );
        let (script, worker) = prepare(Handle::current()).unwrap();
        let mut host = Host {
            service: Arc::clone(&fixture.service),
            session: Arc::clone(&fixture.session),
            launch,
            transfer: false,
            call_id: None,
            runtime: Handle::current(),
            script: script.clone(),
            start_permits: Arc::new(Semaphore::new(0)),
            io_permits: Arc::new(Semaphore::new(0)),
            next: 1,
            starts: 0,
        };
        let error = host.call("read", r#"{"path":"small.txt"}"#).unwrap_err();
        assert!(error.value.contains("not dispatched"));
        assert!(lock(&script.0.report).pending.is_empty());
        assert!(
            control.used() > 0,
            "the synchronous error still owns its input workspace"
        );
        drop(error);
        assert_eq!(control.used(), 0);
        let sleep_error = host.call("sleep", "0").unwrap_err();
        assert!(sleep_error.value.contains("not dispatched"));
        assert!(lock(&script.0.report).pending.is_empty());
        drop(sleep_error);
        assert_eq!(control.used(), 0);
        host.call("exec", r#"{"action":"kill","taskId":"aaaaaaaa-1"}"#)
            .unwrap();
        assert!(control.used() > 0);
        script.kill();
        tokio::time::timeout(Duration::from_secs(5), async {
            while *lock(&script.0.in_flight.0) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let pending = script.finish();
        assert!(
            control.used() > 0,
            "temporary report strings still hold their input allowance"
        );
        drop(pending);
        drop(host);
        drop(worker);
        // The waiting VM thread drops its receiver after the worker channel closes;
        // a completed control reply keeps its wire owner until that happens.
        tokio::time::timeout(Duration::from_secs(5), async {
            while control.used() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(memory.used(), 0);
        assert_eq!(control.used(), 0);
    }

    #[tokio::test]
    async fn report_snapshot_reserves_spare_capacity_and_retains_its_owner() {
        let (script, _worker) = prepare(Handle::current()).unwrap();
        let mut tool = String::with_capacity(128);
        tool.push_str("read");
        let mut handle = String::with_capacity(256);
        handle.push_str("task-1.abcd");
        {
            let mut report = lock(&script.0.report);
            report.cancelled.push(CallInfo {
                tool,
                action: None,
                task_id: None,
            });
            report.started.push(handle);
        }
        let memory = BytePool::new(1024);
        let control = BytePool::new(4096);
        assert!(script.report(&memory, None).is_none());
        assert_eq!(memory.used(), 0);
        let snapshot = script.report(&memory, Some(&control)).unwrap();
        assert!(control.used() > memory.limit());
        assert_eq!(snapshot.cancelled[0].tool, "read");
        assert_eq!(snapshot.started, ["task-1.abcd"]);
        script.record_started("task-2.abcd".into());
        assert_eq!(snapshot.started, ["task-1.abcd"]);
        assert_eq!(lock(&script.0.report).started.len(), 2);
        let retained = control.used();
        assert!(retained > 0);
        assert!(script.report(&memory, Some(&control)).is_none());
        assert_eq!(control.used(), retained);
        drop(snapshot);
        assert_eq!(control.used(), 0);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn bridge_budget_replaces_arguments_with_results_and_releases_them() {
        let info = || CallInfo {
            tool: "read".into(),
            action: None,
            task_id: None,
        };
        let mut report = Report::default();
        report
            .admit(1, info(), BRIDGE_LIMIT - 4, Default::default())
            .unwrap();
        report.admit(2, info(), 4, Default::default()).unwrap();
        assert_eq!(
            report.admit(3, info(), 1, Default::default()).unwrap_err(),
            BRIDGE_ERROR
        );
        assert!(!report.result(1, BRIDGE_LIMIT));
        assert_eq!(report.bridge_bytes, BRIDGE_LIMIT);
        assert_eq!(report.pending[&1].bytes, BRIDGE_LIMIT - 4);
        report.remove(1);
        assert_eq!(report.bridge_bytes, 4);
        assert!(report.result(2, BRIDGE_LIMIT));
        assert_eq!(report.bridge_bytes, BRIDGE_LIMIT);
        report.remove(2);
        assert_eq!(report.bridge_bytes, 0);
        report.remove(1);
        assert!(report.pending.is_empty());
        report.admit(3, info(), 8, Default::default()).unwrap();
        assert!(report.result(3, 12));
        assert_eq!(report.bridge_bytes, 12);
        report.remove(3);
        assert_eq!(report.bridge_bytes, 0);
    }

    #[tokio::test]
    async fn queued_starts_keep_submission_order_when_workers_poll_in_reverse() {
        let pool = Arc::new(Semaphore::new(1));
        let occupied = pool.clone().acquire_owned().await.unwrap();
        let first = queue_permit(pool.clone());
        let second = queue_permit(pool.clone());
        let mut second = std::pin::pin!(second);
        drop(occupied);
        let first_permit = tokio::select! {
            biased;
            _ = &mut second => panic!("the second worker jumped the start queue"),
            permit = first => permit,
        };
        drop(first_permit);
        drop(second.await);
        assert_eq!(pool.available_permits(), 1);
    }

    #[tokio::test]
    async fn start_history_limit_counts_admitted_calls_without_spawning_processes() {
        let fixture = Fixture::new();
        let runtime = Handle::current();
        let (script, worker) = prepare(runtime.clone()).unwrap();
        drop(worker);
        let mut host = Host {
            service: Arc::clone(&fixture.service),
            session: Arc::clone(&fixture.session),
            launch: Arc::clone(&fixture.session.launch),
            transfer: false,
            call_id: None,
            runtime,
            script: script.clone(),
            start_permits: Arc::new(Semaphore::new(0)),
            io_permits: Arc::new(Semaphore::new(0)),
            next: 1,
            starts: MAX_STARTS - 1,
        };
        let args = json!({"action":"start","command":"true"}).to_string();
        host.call("exec", &args).unwrap();
        assert_eq!(host.starts, MAX_STARTS);
        assert_eq!(host.call("exec", &args).unwrap_err().value, STARTS_ERROR);
        assert_eq!(host.starts, MAX_STARTS);
        // Unvalidated start requests resolve as tool errors without consuming start slots.
        host.call("exec", &json!({"action":"start","command":""}).to_string())
            .unwrap();
        assert_eq!(host.starts, MAX_STARTS);
        assert_eq!(lock(&script.0.report).pending.len(), 2);
        host.call("read", &json!({"path":"missing"}).to_string())
            .unwrap();
        assert_eq!(host.starts, MAX_STARTS);
        script.kill();
        tokio::task::yield_now().await;
        assert!(fixture.tasks.all().is_empty());
    }

    #[test]
    fn pending_diagnostics_bound_long_identifiers_without_changing_the_report() {
        let calls = vec![
            CallInfo {
                tool: "exec".into(),
                action: Some("poll".into()),
                task_id: Some("任务".repeat(4096))
            };
            3
        ];
        let text = pending_text(&calls);
        assert!(text.len() <= DIAGNOSTIC_RESERVE as usize / 2);
        assert!(text.contains("2 call(s) omitted"));
        assert!(text.starts_with("exec poll 任务"));
        assert_eq!(calls[0].task_id.as_ref().unwrap().chars().count(), 8192);
    }

    #[test]
    fn validates_code_and_only_script_options() {
        let launch = LaunchContext::current().unwrap();
        for code in ["", " \n\t", "text('\0')"] {
            let request: Request = serde_json::from_value(json!({"code": code})).unwrap();
            assert!(validate(&request, &launch).is_err());
        }
        let request: Request =
            serde_json::from_value(json!({"code": "x".repeat(MAX_CODE + 1)})).unwrap();
        assert!(validate(&request, &launch).unwrap_err().contains("256 KiB"));
        for extra in [
            json!({"cwd":"definitely-missing-relative-dir-fastexec"}),
            json!({"pty":true}),
            json!({"raw":true}),
            json!({"waitMs":240001}),
            json!({"returnWhen":{"screenContains":["x"]}}),
        ] {
            let mut value = json!({"code":"return 1"});
            value
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            assert!(
                serde_json::from_value::<Request>(value)
                    .map_or(true, |r| validate(&r, &launch).is_err())
            );
        }
        let request: Request = serde_json::from_value(
            json!({"code":"return '\\0'", "cwd": ".", "waitMs":0, "killAfterMs":0}),
        )
        .unwrap();
        assert!(validate(&request, &launch).is_ok());
    }

    #[tokio::test]
    async fn text_console_return_and_exit() {
        let fixture = Fixture::new();
        let (state, output) = fixture.result("text('first'); await null; console.info('a', {b:2}); console.warn('warn'); console.error('error'); console.debug('debug'); return {done:true};").await;
        assert_eq!(state.exit_code, Some(0), "{output}");
        assert_eq!(
            output,
            "first\na {\"b\":2}\nwarn\nerror\ndebug\n{\"done\":true}\n"
        );
        for (code, expected) in [
            ("return 'last'", "last\n"),
            ("return undefined", ""),
            (
                r#"
                const value = {
                    value: 42,
                    get [Symbol.toStringTag]() { throw new Error("tag unavailable"); },
                };
                console.log(value);
                value.self = value;
                console.log(value);
                return "done";
                "#,
                "{\"value\":42}\n[unprintable]\ndone\n",
            ),
            (
                "text('before'); try { exit(); } catch(e) { text('caught'); } text('after');",
                "before\n",
            ),
            (
                r#"
                const tools = globalThis.tools;
                const console = globalThis.console;
                const cyc = Object.create(null);
                cyc.self = cyc;
                const big = Object.create(null);
                big.n = 1n;
                Object.defineProperty(Error, Symbol.hasInstance, {
                    value() { throw new Error('broken instanceof'); },
                    configurable: true,
                });
                const oddErr = new TypeError('bad');
                oddErr.name = Object.create(null);
                oddErr.stack = '';
                console.log(cyc, big, Symbol('tag'), oddErr);
                return typeof tools.read;
                "#,
                "[object Object] [object Object] Symbol(tag) [object Object]: bad\nfunction\n",
            ),
        ] {
            let (state, output) = fixture.result(code).await;
            assert_eq!(state.exit_code, Some(0), "{output}");
            assert_eq!(output, expected);
        }
    }

    #[tokio::test]
    async fn diagnostics_are_bounded_before_rust_copy_and_admitted_before_vm() {
        for fits in [false, true] {
            let mut fixture = Fixture::new();
            let control = BytePool::new(DIAGNOSTIC_WORKSPACE - usize::from(!fits));
            fixture.session = Session::new(
                "diagnostic".into(),
                "diagnostic".into(),
                Arc::new(
                    fixture
                        .session
                        .launch
                        .for_session()
                        .with_control_memory(Arc::clone(&control)),
                ),
            );
            for code in [
                "throw 'é'.repeat(2100);",
                "const e = new Error('é'.repeat(2100)); e.stack = 'stack'.repeat(1100); throw e;",
            ] {
                let (state, output) = fixture.result(code).await;
                assert_eq!(state.exit_code, Some(1), "{output}");
                assert!(output.len() <= DIAGNOSTIC_RESERVE as usize, "{output}");
                if fits {
                    assert!(output.starts_with("Script error: "));
                    assert!(output.contains('é'));
                    assert!(output.ends_with("...\n"));
                } else {
                    assert_eq!(
                        output,
                        "Script error: diagnostic memory budget is full; the script was not started.\n"
                    );
                }
                assert_eq!(control.used(), 0);
            }
        }
    }

    #[tokio::test]
    async fn diagnostic_workspace_stays_with_running_vm_until_final_output() {
        let mut fixture = Fixture::new();
        let control = BytePool::new(DIAGNOSTIC_WORKSPACE);
        fixture.session = Session::new(
            "diagnostic".into(),
            "diagnostic".into(),
            Arc::new(
                fixture
                    .session
                    .launch
                    .for_session()
                    .with_control_memory(Arc::clone(&control)),
            ),
        );
        let task = fixture.start("await sleep(60000);", Some(5000));
        tokio::time::timeout(Duration::from_secs(2), async {
            while control.used() != DIAGNOSTIC_WORKSPACE {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!*task.done().borrow());
        task.kill().unwrap();
        complete(&task).await;
        let output = std::fs::read_to_string(&task.log_path).unwrap();
        assert!(output.starts_with("Script stopped: "));
        assert_eq!(control.used(), 0);
    }

    #[tokio::test]
    async fn failures_keep_source_lines_and_unhandled_rejections() {
        let fixture = Fixture::new();
        for (code, message) in [
            ("text('one');\nthrow new Error('boom')", "boom"),
            ("\nconst = 1", "SyntaxError"),
            (
                "Promise.reject(new Error('unhandled')); return 'done'",
                "unhandled",
            ),
            ("await new Promise(() => {})", STALLED),
        ] {
            let (state, output) = fixture.result(code).await;
            assert_eq!(state.exit_code, Some(1), "{output}");
            assert!(output.contains("Script error:"), "{output}");
            assert!(output.contains(message), "{output}");
            if code.starts_with("text('one')") {
                assert!(output.contains("codemode.js:2"), "{output}");
            }
        }
        let (state, output) = fixture.result("const promise = Promise.reject(new Error('handled')); await null; promise.catch(() => {}); return 'ok';").await;
        assert_eq!(state.exit_code, Some(0), "{output}");
        assert_eq!(output, "ok\n");

        for (code, expected) in [
            ("throw Symbol('boom')", "cannot be described"),
            ("throw Object.create(null)", "cannot be described"),
            (
                "const e = new Error('msg'); Object.defineProperty(e, 'name', {get(){throw 1}}); Object.defineProperty(e, 'stack', {value: Symbol('s')}); throw e;",
                "Error: msg",
            ),
        ] {
            let (state, output) = fixture.result(code).await;
            assert_eq!(state.exit_code, Some(1), "{output}");
            assert!(output.contains(expected), "{output}");
        }
        let rt = Runtime::new().unwrap();
        let ctx = Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            let val: Value = ctx
                .eval("const e = new Error(); Object.defineProperty(e, 'name', {get(){throw 1}}); Object.defineProperty(e, 'message', {get(){throw 2}}); Object.defineProperty(e, 'stack', {get(){throw 3}}); e")
                .unwrap();
            assert_eq!(describe(&ctx, val), "Error: ");
            assert!(!ctx.has_exception());
            let sym: Value = ctx.eval("Symbol('x')").unwrap();
            assert!(describe(&ctx, sym).contains("cannot be described"));
            assert!(!ctx.has_exception());
        });
    }

    #[tokio::test]
    async fn file_tools_resolve_errors_reject_bad_arguments_and_queue() {
        let fixture = Fixture::new();
        let code = r#"
            const write = await tools.write({path:'fixture.txt',content:'old\n'});
            if ('ok' in write || 'structuredContent' in write || write.bytes !== 4 || !write.path.endsWith('/fixture.txt')) throw new Error('write shape');
            const edit = await tools.edit({path:'fixture.txt',edits:[{oldText:'old',newText:'new'}]});
            if ('ok' in edit || edit.replacements !== 1 || edit.edits[0].line !== 1) throw new Error('edit failed');
            let missingErr = null;
            try { await tools.read({path:'missing.txt'}); } catch (e) { missingErr = e; }
            if (!missingErr || missingErr.name !== 'ToolError' || missingErr.tool !== 'read' || typeof missingErr.result?.error !== 'string') throw new Error('missing file must reject with ToolError');
            let badExecErr = null;
            try { await tools.exec({action:'start',command:''}); } catch (e) { badExecErr = e; }
            if (!badExecErr || badExecErr.name !== 'ToolError' || badExecErr.tool !== 'exec' || badExecErr.result?.action !== 'start') throw new Error('empty exec must reject with ToolError');
            for (const args of [null, [], 'x', {path:'fixture.txt',surprise:true}]) {
                let rejected = false;
                try { await tools.read(args); } catch (e) { if (e instanceof TypeError) rejected = true; }
                if (!rejected) throw new Error('invalid arguments resolved');
            }
            for (const args of [null, {action:'bogus'}, {action:'list',surprise:true}]) {
                let rejected = false;
                try { await tools.exec(args); } catch (e) { if (e instanceof TypeError) rejected = true; }
                if (!rejected) throw new Error('invalid exec schema resolved');
            }
            try { tools.codemode({code:'return 1'}); throw new Error('unknown tool accepted'); }
            catch(e) { if (!(e instanceof TypeError)) throw e; }
            const results = await Promise.all(Array.from({length:20}, () => tools.read({path:'fixture.txt'})));
            if (results.some(r => 'ok' in r || r.text !== 'new\n' || r.truncated !== false || r.totalLines !== 1)) throw new Error('queued read failed');
            text(results.length);
        "#;
        let (state, output) = fixture.result(code).await;
        assert_eq!(state.exit_code, Some(0), "{output}");
        assert_eq!(output, "20\n");
        assert_eq!(
            std::fs::read_to_string(fixture.dir.join("fixture.txt")).unwrap(),
            "new\n"
        );
    }

    #[tokio::test]
    async fn bridge_survives_replaced_builtins() {
        let fixture = Fixture::new();
        std::fs::write(fixture.dir.join("fixture.txt"), "kept").unwrap();
        let (state, output) = fixture.result(r#"
            const promise = tools.read({path:'fixture.txt'});
            Map.prototype.get = Map.prototype.set = Map.prototype.delete = () => { throw Error('patched Map'); };
            JSON.parse = JSON.stringify = () => { throw Error('patched JSON'); };
            Promise.prototype.then = () => { throw Error('patched then'); };
            Function.prototype.call = Function.prototype.bind = () => { throw Error('patched function'); };
            const result = await promise;
            text({ok: result.text === 'kept' && !('ok' in result)});
            return 'done';
        "#).await;
        assert_eq!(state.exit_code, Some(0), "{output}");
        assert_eq!(output, "{\"ok\":true}\ndone\n");
    }

    #[tokio::test]
    async fn returning_cancels_calls_that_were_not_awaited() {
        let fixture = Fixture::new();
        let task = fixture.start(
            "for (let i=0;i<20;i++) tools.read({path:'missing'}); return 'done';",
            None,
        );
        complete(&task).await;
        assert_eq!(task.snapshot().exit_code, Some(0));
        let script = task.script().unwrap();
        assert!(script.0.cancel.is_cancelled());
        let report = script.report(crate::lease::process_pool(), None).unwrap();
        assert_eq!(report.cancelled.len(), 20);
        assert!(report.cancelled.iter().all(|call| call.tool == "read"));
        assert!(report.started.is_empty());
        assert_eq!(std::fs::read_to_string(&task.log_path).unwrap(), "done\n");
    }

    #[tokio::test]
    async fn finishing_waits_for_a_slow_inflight_start_to_be_reported() {
        let fixture = Fixture::new();
        let task = fixture.start("while (true) {}", None);
        let script = task.script().unwrap().clone();
        // Hold an admitted start before its task is registered, past the old five-second drain.
        let in_flight = InFlightGuard::enter(script.clone());
        lock(&script.0.report)
            .admit(
                1,
                CallInfo {
                    tool: "exec".into(),
                    action: Some("start".into()),
                    task_id: None,
                },
                0,
                Default::default(),
            )
            .unwrap();
        task.kill().unwrap();
        let still_running =
            tokio::time::timeout(Duration::from_secs(6), task.done().wait_for(|done| *done))
                .await
                .is_err();
        let handle = "late-start.abcd".to_owned();
        script.record_started(handle.clone());
        drop(in_flight);
        complete(&task).await;
        assert!(
            still_running,
            "script finished before the admitted start registered"
        );
        assert_eq!(
            script
                .report(crate::lease::process_pool(), None)
                .unwrap()
                .started,
            vec![handle]
        );
        assert_eq!(task.snapshot().state, "killed");
    }

    #[tokio::test]
    async fn interrupts_bytecode_regex_and_job_loops() {
        let fixture = Fixture::new();
        for code in [
            "while(true) {}",
            "while(true) await null",
            "new RegExp('(a+)+$').test('a'.repeat(100000)+'b')",
        ] {
            let task = fixture.start(code, None);
            tokio::time::sleep(Duration::from_millis(30)).await;
            let start = Instant::now();
            task.kill().unwrap();
            complete(&task).await;
            assert!(
                start.elapsed() < Duration::from_millis(200),
                "stop took {:?}",
                start.elapsed()
            );
            let state = task.snapshot();
            assert_eq!(state.state, "killed");
            assert_eq!(state.exit_code, None);
            assert!(!state.expired);
        }
        let task = fixture.start("while(true) {}", Some(100));
        complete(&task).await;
        let state = task.snapshot();
        assert_eq!(state.state, "killed");
        assert_eq!(state.exit_code, None);
        assert!(state.expired);
    }

    #[tokio::test]
    async fn memory_stack_output_and_call_limits_finish_as_errors() {
        let fixture = Fixture::new();
        for code in [
            "const a=[]; while(true) a.push(new Array(65536).fill(1));",
            "function recurse(){return recurse()} recurse();",
            "const chunk='x'.repeat(1024*1024); for(let i=0;i<65;i++) text(chunk);",
            "await Promise.all(Array.from({length:1033}, () => tools.read({path:'missing'})));",
        ] {
            let (state, output) = fixture.result(code).await;
            assert_eq!(
                state.exit_code,
                Some(1),
                "{}",
                &output[output.len().saturating_sub(2048)..]
            );
            assert!(output.contains("Script error:"));
            assert!(output.len() <= TASK_LOG_LIMIT as usize);
        }
    }

    #[tokio::test]
    async fn attach_path_admits_cwd_and_home_before_resolving() {
        for path in ["missing.png", "~/missing.png"] {
            for extra in [-1isize, 0, 1] {
                let mut fixture = Fixture::new();
                let home = fixture.dir.join("session-home");
                let launch = LaunchContext::new(
                    fixture.dir.clone(),
                    vec![
                        ("HOME".into(), home.clone().into_os_string()),
                        ("USERPROFILE".into(), home.into_os_string()),
                    ],
                );
                let needed = launch.path_source_bytes(path) * 32 + 4096;
                let pool = crate::lease::BytePool::new(needed.checked_add_signed(extra).unwrap());
                fixture.session = Session::new(
                    "attach-path-budget".into(),
                    "attach-path-budget".into(),
                    Arc::new(launch.with_memory(Arc::clone(&pool))),
                );
                let (state, output) = fixture.result(&format!("attach({path:?});")).await;
                assert_eq!(state.exit_code, Some(1), "{output}");
                if extra < 0 {
                    assert!(
                        output.contains("budget") && output.contains("preparing the file path"),
                        "{output}"
                    );
                    assert!(!output.contains("Cannot read"), "{output}");
                } else {
                    assert!(output.contains("Cannot read"), "{output}");
                    assert!(!output.contains("memory budget"), "{output}");
                }
                assert_eq!(pool.used(), 0);
            }
        }
    }

    #[tokio::test]
    async fn inline_whitespace_copy_is_admitted_before_allocation() {
        let mut fixture = Fixture::new();
        let pool = crate::lease::BytePool::new(4096);
        fixture.session = Session::new(
            "inline-budget".into(),
            "inline-budget".into(),
            Arc::new(
                LaunchContext::new(fixture.dir.clone(), Vec::new()).with_memory(Arc::clone(&pool)),
            ),
        );
        let mut bytes = Vec::new();
        image::DynamicImage::new_rgb8(1, 1)
            .write_to(
                &mut std::io::Cursor::new(&mut bytes),
                image::ImageFormat::Png,
            )
            .unwrap();
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        let code = format!("attach('data:image/png;base64,' + ' '.repeat(8192) + {encoded:?});");
        let (state, output) = fixture.result(&code).await;
        assert_eq!(state.exit_code, Some(1), "{output}");
        assert!(output.contains("active memory budget is full"), "{output}");
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn attach_covers_r4_4_inline_and_paths_opaque_handles_and_lease_cleanup() {
        let fixture = Fixture::new();
        let mut png_bytes = Vec::new();
        image::DynamicImage::new_rgb8(1, 1)
            .write_to(
                &mut std::io::Cursor::new(&mut png_bytes),
                image::ImageFormat::Png,
            )
            .unwrap();
        let png_b64 = base64::engine::general_purpose::STANDARD.encode(&png_bytes);

        // Minimal ISO BMFF `ftyp` MP4 and EBML WebM payloads for R4-4 parity checks.
        let mut mp4_bytes = Vec::new();
        mp4_bytes.extend_from_slice(&20u32.to_be_bytes());
        mp4_bytes.extend_from_slice(b"ftypisom");
        mp4_bytes.extend_from_slice(&0u32.to_be_bytes());
        mp4_bytes.extend_from_slice(b"mp42");
        std::fs::write(fixture.dir.join("clip.mp4"), &mp4_bytes).unwrap();
        let mp4_b64 = base64::engine::general_purpose::STANDARD.encode(&mp4_bytes);

        let webm_bytes: Vec<u8> = vec![
            0x1A, 0x45, 0xDF, 0xA3, 0x8B, 0x42, 0x82, 0x84, b'w', b'e', b'b', b'm', 0x42, 0x87,
            0x81, 0x02,
        ];
        std::fs::write(fixture.dir.join("clip.webm"), &webm_bytes).unwrap();
        let webm_b64 = base64::engine::general_purpose::STANDARD.encode(&webm_bytes);

        // 1. R4-4 parity: identical kind, MIME, and raw bytes across local path, MCP resource, and data URL;
        //    conflicting declared MIME (`video/mp4` on PNG bytes) and old `image()` global are rejected.
        let r4_code = format!(
            r#"
            RegExp.prototype.exec = String.prototype.replace = () => {{ throw new Error("patched"); }};
            if (typeof globalThis.image !== "undefined") throw new Error("legacy image() must be gone");

            const mp4Desc = await tools.read({{path: "clip.mp4"}});
            if (mp4Desc.attachable !== true || mp4Desc.kind !== "video" || mp4Desc.mimeType !== "video/mp4" || mp4Desc.bytes !== {mp4_len}) {{
                throw new Error("mp4Desc: " + JSON.stringify(mp4Desc));
            }}
            const a1 = attach("clip.mp4");
            const a2 = attach({{ type: "resource", resource: {{ blob: {mp4_b64:?}, mimeType: "video/mp4" }} }});
            const a3 = attach("data:video/mp4;base64," + {mp4_b64:?});
            for (const [idx, item] of [[1, a1], [2, a2], [3, a3]]) {{
                if (item.index !== idx || item.kind !== "video" || item.mimeType !== "video/mp4" || item.bytes !== {mp4_len}) {{
                    throw new Error("mp4 mismatch at " + idx + ": " + JSON.stringify(item));
                }}
            }}

            const w1 = attach(await tools.read({{path: "clip.webm"}}));
            const w2 = attach({{ type: "resource", resource: {{ blob: {webm_b64:?}, mimeType: "video/webm" }} }});
            const w3 = attach("DATA:VIDEO/WEBM;BASE64,\n  " + {webm_b64:?});
            for (const [idx, item] of [[4, w1], [5, w2], [6, w3]]) {{
                if (item.index !== idx || item.kind !== "video" || item.mimeType !== "video/webm" || item.bytes !== {webm_len}) {{
                    throw new Error("webm mismatch at " + idx + ": " + JSON.stringify(item));
                }}
            }}

            let conflictRejected = false;
            try {{
                attach("data:video/mp4;base64," + {png_b64:?});
            }} catch (e) {{
                if (e instanceof TypeError && e.message.includes("conflicts")) conflictRejected = true;
            }}
            if (!conflictRejected) throw new Error("conflicting MIME must reject");
            return "r4-4-ok";
            "#,
            mp4_len = mp4_bytes.len(),
            webm_len = webm_bytes.len(),
        );
        let r4_task = fixture.start(&r4_code, Some(5000));
        complete(&r4_task).await;
        let r4_out = std::fs::read_to_string(&r4_task.log_path).unwrap();
        assert_eq!(r4_task.snapshot().exit_code, Some(0), "{r4_out}");
        assert_eq!(r4_out, "r4-4-ok\n");
        for idx in 1..=3 {
            assert_eq!(
                std::fs::read(r4_task.attachment_path(idx, "video/mp4")).unwrap(),
                mp4_bytes
            );
        }
        for idx in 4..=6 {
            assert_eq!(
                std::fs::read(r4_task.attachment_path(idx, "video/webm")).unwrap(),
                webm_bytes
            );
        }

        // 2. Opaque task attachment handles from nested `poll` and `kill`: reusable without consumption,
        //    unforgeable against clones/spoofed `__hid`, and `AttachmentLease` pins source files across
        //    source-task eviction until the consumer script exits or is cancelled!
        for action in ["poll", "kill"] {
            let producer = fixture.start(
                &format!("attach('data:image/png;base64,{png_b64}'); attach('clip.mp4');"),
                Some(5000),
            );
            complete(&producer).await;
            let src_att1 = producer.attachment_path(1, "image/png");
            assert!(src_att1.is_file());

            let consumer = fixture.start(
                &format!(
                    r#"
                    const r = await tools.exec({{action: {action:?}, taskId: {pid:?}}});
                    const list = r.codemode.attachments;
                    if (!Array.isArray(list) || list.length !== 2) throw new Error("expected 2 handles");
                    const h = list[0];
                    if (h.index !== 1 || h.kind !== "image" || h.mimeType !== "image/png" || h.bytes !== {png_len}) {{
                        throw new Error("handle meta: " + JSON.stringify(h));
                    }}
                    if ("__hid" in h || !Object.isFrozen(h)) throw new Error("handle must be opaque & frozen");

                    // Attaching twice does NOT consume the handle.
                    const c1 = attach(h);
                    const c2 = attach(h);
                    if (c1.index !== 1 || c2.index !== 2 || c2.bytes !== {png_len}) {{
                        throw new Error("reuse failed");
                    }}

                    // Forged or cloned objects cannot spoof an attachment handle.
                    for (const fake of [{{ ...h }}, {{ index: 1, kind: "image", mimeType: "video/mp4", bytes: 1, __hid: 1 }}]) {{
                        let rejected = false;
                        try {{ attach(fake); }} catch (e) {{ if (e instanceof TypeError) rejected = true; }}
                        if (!rejected) throw new Error("forged handle accepted");
                    }}

                    const next = await tools.exec({{action: "poll", taskId: r.taskId, waitMs: 0}});
                    if (next.codemode?.attachments) throw new Error("handles delivered twice");

                    text("PINNED");
                    await sleep(30000);
                    "#,
                    pid = producer.id,
                    png_len = png_bytes.len(),
                ),
                Some(10000),
            );

            // Wait until `consumer` reaches `PINNED`.
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if std::fs::read_to_string(&consumer.log_path)
                    .unwrap_or_default()
                    .contains("PINNED")
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            assert!(
                std::fs::read_to_string(&consumer.log_path)
                    .unwrap()
                    .contains("PINNED")
            );

            // Evict `producer` while `consumer` is still running: `src_att1` stays pinned on disk!
            fixture.tasks.evict_log(&producer);
            assert!(
                src_att1.exists(),
                "AttachmentLease must pin source file while consumer script runs"
            );

            // Cancel/kill `consumer`: once `consumer` exits, its `AttachmentLease` drops and unlinks `src_att1`!
            consumer.kill().unwrap();
            complete(&consumer).await;
            assert!(
                !src_att1.exists(),
                "killing consumer script must release AttachmentLease and unlink evicted file"
            );
            assert_eq!(
                std::fs::read(consumer.attachment_path(1, "image/png")).unwrap(),
                png_bytes
            );
            assert_eq!(
                std::fs::read(consumer.attachment_path(2, "image/png")).unwrap(),
                png_bytes
            );
        }

        // 3. Per-task 1024 attachment cap (above old 64 cap) and disk-quota pre-write rejection with zero residue.
        let (many_state, many_out) = fixture
            .result(&format!(
                r#"
                for (let i = 1; i <= 1024; i++) {{
                    const r = attach("data:image/png;base64,{png_b64}");
                    if (r.index !== i) throw new Error("index " + r.index);
                }}
                let capHit = false;
                try {{ attach("data:image/png;base64,{png_b64}"); }}
                catch (e) {{ if (e instanceof RangeError && e.message.includes("1024")) capHit = true; }}
                if (!capHit) throw new Error("expected 1025th attach to throw RangeError");
                return "cap-1024-ok";
                "#
            ))
            .await;
        assert_eq!(many_state.exit_code, Some(0), "{many_out}");
        assert_eq!(many_out, "cap-1024-ok\n");

        // Tight disk quota rejects oversized attachment before writing any orphan file.
        let quota_dir = std::env::temp_dir().join(format!(
            "fastexec-script-quota-{}",
            tasks::random_chars(12).unwrap()
        ));
        let mut tight_tasks = Tasks::new_in(Limits::OFF, quota_dir.clone()).unwrap();
        Arc::get_mut(&mut tight_tasks).unwrap().set_log_limit(256);
        let tight_service = Arc::new(Service::new(Arc::clone(&tight_tasks), false));
        let tight_session = Session::new(
            "tight-quota".into(),
            "tight-quota".into(),
            Arc::new(LaunchContext::new(
                quota_dir.clone(),
                std::env::vars_os().collect(),
            )),
        );
        let payload_b64 = base64::engine::general_purpose::STANDARD.encode({
            let mut big_png = png_bytes.clone();
            big_png.resize(512, 0);
            big_png
        });
        let (tight_task, _, _) = tight_tasks
            .start_script(
                ScriptArgs {
                    code: format!(
                        r#"
                        let quotaRejected = false;
                        try {{ attach("data:image/png;base64,{payload_b64}"); }}
                        catch (e) {{ if (e instanceof RangeError && e.message.includes("4 GiB")) quotaRejected = true; }}
                        if (!quotaRejected) throw new Error("expected disk quota RangeError");
                        return "quota-ok";
                        "#
                    ),
                    launch: Arc::clone(&tight_session.launch),
                    kill_after_ms: Some(5000),
                    transfer: false,
                    call_id: None,
                    service: tight_service,
                    input_owner: Default::default(),
                },
                &tight_session,
                "q001".into(),
            )
            .unwrap();
        complete(&tight_task).await;
        let tight_out = std::fs::read_to_string(&tight_task.log_path).unwrap();
        assert_eq!(tight_task.snapshot().exit_code, Some(0), "{tight_out}");
        assert!(
            !tight_task.attachment_path(1, "image/png").exists(),
            "rejected attachment must leave no orphan file"
        );
        tight_tasks.shutdown();
        let _ = std::fs::remove_dir_all(&quota_dir);
    }

    #[tokio::test]
    async fn text_encoder_and_decoder_handle_utf8_streams_surrogates_bom_and_tampered_builtins() {
        let fixture = Fixture::new();
        let code = r#"
            const enc = new TextEncoder();
            if (enc.encoding !== "utf-8" || Object.prototype.toString.call(enc) !== "[object TextEncoder]") {
                throw new Error("encoder metadata");
            }
            if (enc.encode().length !== 0) throw new Error("empty encode");

            // Round-trip UTF-8 + base64 for CJK and Astral plane characters.
            const sample = "你好，fastexec 🚀 café";
            const encoded = enc.encode(sample);
            let binary = "";
            for (let i = 0; i < encoded.length; i++) binary += String.fromCharCode(encoded[i]);
            const b64 = btoa(binary);
            const raw = atob(b64);
            const restoredBytes = Uint8Array.from(raw, c => c.charCodeAt(0));
            const dec = new TextDecoder("  UTF-8\t\n");
            if (dec.encoding !== "utf-8" || dec.fatal !== false || dec.ignoreBOM !== false) {
                throw new Error("decoder metadata");
            }
            if (dec.decode(restoredBytes) !== sample) throw new Error("utf8 base64 roundtrip");

            // Lone high/low surrogates become U+FFFD without corrupting neighbors or pairs.
            for (const [bad, expected] of [
                ["\uD800", "\uFFFD"],
                ["A\uD800B", "A\uFFFDB"],
                ["\uDC00X", "\uFFFDX"],
                ["\uD800\uD83D\uDE80", "\uFFFD🚀"],
            ]) {
                if (dec.decode(enc.encode(bad)) !== expected) {
                    throw new Error("surrogate mismatch: " + expected);
                }
            }

            // encodeInto never writes partial multi-byte sequences when capacity runs short.
            const target = new Uint8Array(5);
            const step1 = enc.encodeInto("a🚀b", target);
            if (step1.read !== 3 || step1.written !== 5 || dec.decode(target) !== "a🚀") {
                throw new Error("encodeInto pair fit: " + JSON.stringify(step1));
            }
            const tight = new Uint8Array(4);
            const step2 = enc.encodeInto("a🚀", tight);
            if (step2.read !== 1 || step2.written !== 1 || tight[0] !== 0x61) {
                throw new Error("encodeInto stopped before 4-byte pair: " + JSON.stringify(step2));
            }

            // Subarray views, DataView, ArrayBuffer, BOM stripping, ignoreBOM, and streaming splits.
            const padded = new Uint8Array([0x99, 0xEF, 0xBB, 0xBF, 0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD, 0x88]);
            const sub = padded.subarray(1, 10);
            if (dec.decode(sub) !== "你好") throw new Error("default BOM strip on subarray");
            if (dec.decode(sub) !== "你好") throw new Error("second non-stream decode must also strip BOM");
            const keepBom = new TextDecoder("utf8", { ignoreBOM: true });
            if (keepBom.decode(new DataView(padded.buffer, 1, 9)) !== "\uFEFF你好") {
                throw new Error("ignoreBOM DataView");
            }

            // Split BOM and 4-byte emoji across 1-byte streaming chunks; keep mid-stream U+FEFF.
            const streamBytes = new Uint8Array([
                0xEF, 0xBB, 0xBF,
                0xF0, 0x9F, 0x9A, 0x80,
                0xEF, 0xBB, 0xBF
            ]);
            const streamDec = new TextDecoder();
            let streamed = "";
            streamed += streamDec.decode(new Uint8Array(0), { stream: true });
            for (let i = 0; i < streamBytes.length; i++) {
                streamed += streamDec.decode(streamBytes.subarray(i, i + 1), { stream: true });
            }
            streamed += streamDec.decode();
            if (streamed !== "🚀\uFEFF") throw new Error("streaming decode: " + JSON.stringify(streamed));

            // WHATWG maximal subpart replacement and fatal mode.
            if (dec.decode(new Uint8Array([0xE2, 0x82, 0x41])) !== "\uFFFDA") {
                throw new Error("maximal subpart truncated 3-byte");
            }
            if (dec.decode(new Uint8Array([0xF0, 0x80, 0x80, 0x41])) !== "\uFFFD\uFFFD\uFFFDA") {
                throw new Error("maximal subpart overlong 4-byte");
            }
            if (dec.decode(new Uint8Array([0xED, 0xA0, 0x80])) !== "\uFFFD\uFFFD\uFFFD") {
                throw new Error("CESU-8 surrogate rejected");
            }
            const fatalDec = new TextDecoder("utf-8", { fatal: true });
            for (const invalid of [
                new Uint8Array([0xFF]),
                new Uint8Array([0xE2, 0x82]),
                new Uint8Array([0xED, 0xA0, 0x80]),
            ]) {
                let threw = false;
                try { fatalDec.decode(invalid); } catch (e) { if (e instanceof TypeError) threw = true; }
                if (!threw) throw new Error("fatal decoder accepted invalid bytes");
            }

            // Streaming fatal error keeps doNotFlush and bomSeen per WHATWG Encoding § 7.2,
            // while an early fatal error before serializing does not prematurely mark bomSeen.
            const fatalStream = new TextDecoder("utf-8", { fatal: true });
            if (fatalStream.decode(new Uint8Array([0xEF, 0xBB, 0xBF, 0x41]), { stream: true }) !== "A") {
                throw new Error("initial BOM strip in fatal stream");
            }
            let midStreamThrew = false;
            try { fatalStream.decode(new Uint8Array([0xFF]), { stream: true }); }
            catch (e) { if (e instanceof TypeError) midStreamThrew = true; }
            if (!midStreamThrew) throw new Error("expected mid-stream TypeError");
            if (fatalStream.decode(new Uint8Array([0xEF, 0xBB, 0xBF, 0x42]), { stream: true }) !== "\uFEFFB") {
                throw new Error("mid-stream U+FEFF after fatal error must be preserved");
            }
            if (fatalStream.decode() !== "") throw new Error("stream flush");
            if (fatalStream.decode(new Uint8Array([0xEF, 0xBB, 0xBF, 0x43])) !== "C") {
                throw new Error("new stream after flush must strip leading BOM again");
            }

            const earlyFatal = new TextDecoder("utf-8", { fatal: true });
            let earlyThrew = false;
            try { earlyFatal.decode(new Uint8Array([0x41, 0xFF]), { stream: true }); }
            catch (e) { if (e instanceof TypeError) earlyThrew = true; }
            if (!earlyThrew) throw new Error("expected early TypeError");
            if (earlyFatal.decode(new Uint8Array([0xEF, 0xBB, 0xBF, 0x42]), { stream: true }) !== "B") {
                throw new Error("failed first chunk must not commit bomSeen");
            }

            // WebIDL ToString coercion (.toString() before .valueOf()) and argument validation order.
            const labelObj = { valueOf() { return 123; }, toString() { return "utf-8"; } };
            const strObj = { valueOf() { return 456; }, toString() { return "ok"; } };
            const coercedDec = new TextDecoder(labelObj);
            if (coercedDec.decode(enc.encode(strObj)) !== "ok") {
                throw new Error("ToString coercion order");
            }
            const intoBuf = new Uint8Array(4);
            if (enc.encodeInto(strObj, intoBuf).written !== 2 || dec.decode(intoBuf.subarray(0, 2)) !== "ok") {
                throw new Error("encodeInto ToString coercion");
            }
            let optsBeforeLabel = false;
            try { new TextDecoder("latin1", "bad-options"); }
            catch (e) { if (e instanceof TypeError) optsBeforeLabel = true; }
            if (!optsBeforeLabel) throw new Error("options TypeError must precede label RangeError");
            let inputBeforeOpts = false;
            try {
                dec.decode("not-bytes", { get stream() { throw new Error("options accessed too early"); } });
            } catch (e) {
                if (e instanceof TypeError) inputBeforeOpts = true;
            }
            if (!inputBeforeOpts) throw new Error("input TypeError must precede options getter");

            // Detached, mid-call detached, shrunk/grown Resizable ArrayBuffer, and immutable ArrayBuffer.
            const detAb = new Uint8Array([0x61, 0x62]).buffer;
            const detTa = new Uint8Array(detAb);
            const detDv = new DataView(detAb);
            detAb.transfer();
            if (dec.decode(detAb) !== "" || dec.decode(detTa) !== "" || dec.decode(detDv) !== "") {
                throw new Error("detached BufferSource must decode as empty");
            }
            const detInto = enc.encodeInto("abc", detTa);
            if (detInto.read !== 0 || detInto.written !== 0) {
                throw new Error("detached encodeInto must return 0/0");
            }
            const midAb = new Uint8Array([0x61, 0x62]).buffer;
            const midTa = new Uint8Array(midAb);
            if (dec.decode(midTa, { get stream() { midAb.transfer(); return false; } }) !== "") {
                throw new Error("mid-call detach must decode as empty");
            }
            const rab = new ArrayBuffer(8, { maxByteLength: 16 });
            const oobTa = new Uint8Array(rab, 4, 4);
            const oobDv = new DataView(rab, 4, 4);
            const trackTa = new Uint8Array(rab);
            rab.resize(2);
            if (dec.decode(oobTa) !== "" || dec.decode(oobDv) !== "") {
                throw new Error("OOB RAB views must decode as empty");
            }
            const oobInto = enc.encodeInto("abc", oobTa);
            if (oobInto.read !== 0 || oobInto.written !== 0) {
                throw new Error("OOB RAB encodeInto must return 0/0");
            }
            rab.resize(6);
            if (enc.encodeInto("hello!", trackTa).written !== 6 || dec.decode(trackTa) !== "hello!") {
                throw new Error("grown RAB tracking view failed");
            }
            if (typeof ArrayBuffer.prototype.transferToImmutable === "function") {
                const immAb = enc.encode("immutable-ok").buffer.transferToImmutable();
                const immTa = new Uint8Array(immAb);
                if (dec.decode(immAb) !== "immutable-ok" || dec.decode(immTa) !== "immutable-ok") {
                    throw new Error("immutable ArrayBuffer decode failed");
                }
                for (const s of ["", "x"]) {
                    let immRejected = false;
                    try { enc.encodeInto(s, immTa); } catch (e) { if (e instanceof TypeError) immRejected = true; }
                    if (!immRejected) throw new Error("immutable ArrayBuffer encodeInto must reject");
                }
            }

            // Unsupported labels and invalid arguments reject with RangeError / TypeError.
            let badLabel = false;
            try { new TextDecoder("latin1"); } catch (e) { if (e instanceof RangeError) badLabel = true; }
            if (!badLabel) throw new Error("latin1 should fail");
            for (const call of [
                () => TextEncoder(),
                () => TextDecoder(),
                () => enc.encode(Symbol("s")),
                () => enc.encodeInto(),
                () => enc.encodeInto("a"),
                () => enc.encodeInto(Symbol("s"), new Uint8Array(4)),
                () => enc.encodeInto("a", new Int8Array(4)),
                () => new TextDecoder(Symbol("s")),
                () => dec.decode("not-bytes"),
                () => TextEncoder.prototype.encode("a"),
                () => TextDecoder.prototype.decode(new Uint8Array(0)),
            ]) {
                let rejected = false;
                try { call(); } catch (e) { if (e instanceof TypeError) rejected = true; }
                if (!rejected) throw new Error("expected TypeError");
            }

            // Survives prototype mutation of Object (including get/set and [0]), TypedArray, Array, String, and WeakMap,
            // including surrogate pairs split across the 1024-unit internal chunk boundary and re-entrant options getters.
            const taProto = Object.getPrototypeOf(Uint8Array.prototype);
            const origTaLen = Object.getOwnPropertyDescriptor(taProto, "length");
            Object.defineProperty(taProto, "length", { __proto__: null, get() { throw new Error("patched TA length"); }, configurable: true });
            Object.defineProperty(Object.prototype, 0, { __proto__: null, set() { throw new Error("patched Object[0]"); }, configurable: true });
            Object.defineProperty(Object.prototype, "get", { __proto__: null, value() { throw new Error("patched desc get"); }, configurable: true });
            Object.defineProperty(Object.prototype, "set", { __proto__: null, value() { throw new Error("patched desc set"); }, configurable: true });
            Array.prototype.push = Array.prototype.join = () => { throw new Error("patched Array"); };
            String.prototype.charCodeAt = String.prototype.slice = () => { throw new Error("patched String"); };
            WeakMap.prototype.get = WeakSet.prototype.has = () => { throw new Error("patched Weak"); };
            const big = "中🚀ab".repeat(3000);
            const bigBytes = enc.encode(big);
            const nestedOpts = {
                get stream() {
                    if (new TextDecoder().decode(enc.encode("nested-🚀")) !== "nested-🚀") {
                        throw new Error("re-entrant decode failed");
                    }
                    return false;
                }
            };
            if (dec.decode(bigBytes, nestedOpts) !== big) {
                throw new Error("failed multi-chunk roundtrip after prototype mutation");
            }
            const scratch = new Uint8Array(16);
            if (enc.encodeInto("中🚀", scratch).written !== 7 || dec.decode(scratch.subarray(0, 7)) !== "中🚀") {
                throw new Error("encodeInto failed under prototype mutation");
            }
            delete Object.prototype[0];
            delete Object.prototype.get;
            delete Object.prototype.set;
            Object.defineProperty(taProto, "length", origTaLen);
            return "codec-ok";
        "#;
        let (state, output) = fixture.result(code).await;
        assert_eq!(state.exit_code, Some(0), "{output}");
        assert_eq!(output, "codec-ok\n");
    }

    #[tokio::test]
    async fn text_decoder_retains_unread_input_after_fatal_stream_errors() {
        let fixture = Fixture::new();
        let code = r#"
            const decoder = () => new TextDecoder("utf-8", { fatal: true });
            const rejects = (dec, bytes, stream = true) => {
                try { dec.decode(bytes, { stream }); }
                catch (error) { if (error instanceof TypeError) return; throw error; }
                throw new Error("expected invalid UTF-8");
            };
            const retained = decoder();
            const source = new Uint8Array([0xFF, 0x61]);
            rejects(retained, source);
            source[1] = 0x7A;
            if (retained.decode(new Uint8Array([0x62]), { stream: true }) !== "ab") {
                throw new Error("unread bytes must be copied and precede the next input");
            }
            const restored = decoder();
            rejects(restored, new Uint8Array([0xE2, 0x41, 0x42]));
            if (restored.decode() !== "AB") throw new Error("invalid continuation must be restored");
            const repeated = decoder();
            rejects(repeated, new Uint8Array([0xE2, 0xFF, 0x41]));
            rejects(repeated, new Uint8Array(0));
            if (repeated.decode(new Uint8Array([0x42])) !== "AB") {
                throw new Error("successive errors must consume only the offending sequence");
            }
            for (const prior of ["", "X"]) {
                const bom = decoder();
                if (prior) bom.decode(new Uint8Array([0x58]), { stream: true });
                rejects(bom, new Uint8Array([0x41, 0xFF, 0xEF, 0xBB, 0xBF, 0x42]));
                if (bom.decode() !== (prior ? "\uFEFFB" : "B")) {
                    throw new Error("failed output must not change BOM state");
                }
            }
            const reset = decoder();
            rejects(reset, new Uint8Array([0xFF, 0x61]), false);
            if (reset.decode(new Uint8Array([0x62])) !== "b") {
                throw new Error("non-streaming failure must reset the input queue");
            }
            rejects(reset, new Uint8Array([0xFF, 0xE2]));
            rejects(reset, new Uint8Array(0), false);
            if (reset.decode(new Uint8Array([0x41])) !== "A") {
                throw new Error("failed flush must reset the decoder");
            }
            return "queue-ok";
        "#;
        let (state, output) = fixture.result(code).await;
        assert_eq!(state.exit_code, Some(0), "{output}");
        assert_eq!(output, "queue-ok\n");
    }

    #[tokio::test]
    async fn nested_read_uses_the_one_mib_window_and_reads_long_lines() {
        let fixture = Fixture::new();
        let long_line = format!("{{\"payload\":\"{}\"}}\n", "x".repeat(120 * 1024));
        std::fs::write(fixture.dir.join("long.jsonl"), &long_line).unwrap();
        let many_lines: String = (1..=3500).map(|i| format!("row-{i}\n")).collect();
        std::fs::write(fixture.dir.join("rows.txt"), &many_lines).unwrap();
        let (state, output) = fixture
            .result(
                r#"
                const [longRes, rowsRes] = await Promise.all([
                    tools.read({path: "long.jsonl"}),
                    tools.read({path: "rows.txt"}),
                ]);
                if (longRes.truncated || longRes.lineTooLong) {
                    throw new Error("long line truncated");
                }
                const parsed = JSON.parse(longRes.text);
                if (parsed.payload.length !== 120 * 1024) throw new Error("payload length");
                if (rowsRes.truncated || rowsRes.shownLines !== 3500 || rowsRes.totalLines !== 3500) {
                    throw new Error("rows truncated: " + JSON.stringify(rowsRes));
                }
                return "nested-window-ok";
            "#,
            )
            .await;
        assert_eq!(state.exit_code, Some(0), "{output}");
        assert_eq!(output, "nested-window-ok\n");
    }

    #[tokio::test]
    async fn sh_quote_image_notes_queue_cap_and_integrity_tool_errors() {
        let fixture = Fixture::new();
        // Create a small PNG and a sparse >100 MiB PNG header for unattachable descriptor read.
        let img = image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(
            4,
            4,
            image::Rgb([1, 2, 3]),
        ));
        let mut png = Vec::new();
        img.write_to(&mut std::io::Cursor::new(&mut png), image::ImageFormat::Png)
            .unwrap();
        std::fs::write(fixture.dir.join("ok.png"), &png).unwrap();
        {
            use std::io::Write;
            let mut huge = std::fs::File::create(fixture.dir.join("huge.png")).unwrap();
            huge.write_all(&png[..33]).unwrap();
            huge.set_len(105 * 1024 * 1024).unwrap();
        }

        let (state, output) = fixture
            .result(
                r#"
                if (!Object.isFrozen(sh) || typeof sh.quote !== "function") {
                    throw new Error("sh not frozen");
                }
                const cases = [
                    ["", "''"],
                    ["hello world", "'hello world'"],
                    ["it's $HOME `id` \\ \n", "'it'\\''s $HOME `id` \\ \n'"],
                    [42, "'42'"],
                    [-3.5, "'-3.5'"],
                ];
                for (const [inp, exp] of cases) {
                    const got = sh.quote(inp);
                    if (got !== exp) throw new Error("sh.quote mismatch: " + got + " !== " + exp);
                }
                for (const bad of [null, undefined, true, {}, [], NaN, Infinity, -Infinity, "a\0b", Symbol("x")]) {
                    let threw = false;
                    try { sh.quote(bad); } catch (e) { if (e instanceof TypeError) threw = true; }
                    if (!threw) throw new Error("expected TypeError for " + String(bad));
                }
                const okImg = await tools.read({path: "ok.png"});
                if (okImg.attachable !== true || okImg.bytes === undefined || okImg.data !== undefined) {
                    throw new Error("okImg descriptor shape: " + JSON.stringify(okImg));
                }
                attach(okImg);
                const bigImg = await tools.read({path: "huge.png"});
                if (bigImg.attachable !== false || bigImg.bytes !== undefined || bigImg.sourceBytes !== 105 * 1024 * 1024 || typeof bigImg.note !== "string" || !bigImg.note.includes("exceeds FASTEXEC_MAX_MEDIA_BYTES")) {
                    throw new Error("bigImg shape: " + JSON.stringify(bigImg));
                }
                let noteThrown = false;
                try { attach(bigImg); } catch (e) {
                    if (e instanceof TypeError && e.message === bigImg.note) noteThrown = true;
                }
                if (!noteThrown) throw new Error("expected attach(bigImg) to throw bigImg.note");
                return "helpers-ok";
                "#,
            )
            .await;
        assert_eq!(state.exit_code, Some(0), "{output}");
        assert!(output.contains("helpers-ok"), "{output}");

        // End-to-end JS ToolError on injected captureError and logError: non-empty message
        // built from diagnostics and full report (output, exitCode, diagnostic) in err.result,
        // plus the 1025th pending call rejecting with RangeError.
        let seeded = fixture.start("text('partial-out'); throw new Error('boom');", None);
        complete(&seeded).await;
        seeded.inject_io_errors(Some("pty pipe lost"), Some("disk full"));
        let seeded_id = seeded.id.clone();
        let (io_state, io_out) = fixture
            .result(&format!(
                r#"
                let caught = null;
                try {{
                    await tools.exec({{action: "poll", taskId: {seeded_id:?}, waitMs: 0}});
                }} catch (e) {{
                    caught = e;
                }}
                if (!caught || caught.name !== "ToolError" || caught.tool !== "exec") {{
                    throw new Error("expected ToolError: " + String(caught));
                }}
                if (!caught.message.includes("output capture failed: pty pipe lost") ||
                    !caught.message.includes("log write failed: disk full")) {{
                    throw new Error("unexpected ToolError.message: " + caught.message);
                }}
                if (caught.result.exitCode !== 1 ||
                    !caught.result.output.startsWith("partial-out\n") ||
                    caught.result.captureError !== "pty pipe lost" ||
                    caught.result.logError !== "disk full") {{
                    throw new Error("unexpected ToolError.result: " + JSON.stringify(caught.result));
                }}

                const settled = await Promise.allSettled(
                    Array.from({{length: 1025}}, () => tools.read({{path: "ok.png"}}))
                );
                const rangeErrors = settled.filter(
                    s => s.status === "rejected" && s.reason instanceof RangeError
                );
                if (rangeErrors.length !== 1) {{
                    throw new Error("expected 1 RangeError at 1025th pending call, got " + rangeErrors.length);
                }}
                return "io-and-queue-ok";
                "#
            ))
            .await;
        assert_eq!(io_state.exit_code, Some(0), "{io_out}");
        assert!(io_out.contains("io-and-queue-ok"), "{io_out}");

        // Integrity failure without top-level `error` builds ToolError.message from inputError
        // and preserves output, exitCode, and diagnostics in ToolError.result.
        let mut synth =
            rmcp::model::CallToolResult::error(vec![rmcp::model::ContentBlock::text("status")]);
        synth.structured_content = Some(json!({
            "ok": false,
            "action": "poll",
            "taskId": "abcdefgh-1.2345",
            "state": "exited",
            "exitCode": 0,
            "output": "captured-before-err",
            "inputError": "INPUT_WRITE_FAILED: broken pipe",
        }));
        let parsed = nested_envelope(files::TrackedResult::plain(synth)).value;
        assert_eq!(parsed["ok"], false);
        assert!(parsed["value"].get("ok").is_none());
        assert_eq!(parsed["value"]["output"], "captured-before-err");
        assert_eq!(parsed["value"]["exitCode"], 0);
        assert_eq!(
            parsed["value"]["inputError"],
            "INPUT_WRITE_FAILED: broken pipe"
        );
    }

    #[tokio::test]
    async fn call_site_stacks_and_serializable_return_values() {
        let fixture = Fixture::new();
        let (st_uncaught, out_uncaught) = fixture
            .result("const x = 1;\nawait tools.read({path: 'no-such-file-xyz'});\nreturn x;")
            .await;
        assert_eq!(st_uncaught.exit_code, Some(1), "{out_uncaught}");
        assert!(out_uncaught.contains("codemode.js:2"), "{out_uncaught}");

        let (st_caught, out_caught) = fixture
            .result(
                r#"
                let toolStack = "";
                try { await tools.read({path: "missing-2"}); } catch (e) { toolStack = e.stack; }
                let typeStack = "";
                try { await tools.exec({action: "bogus"}); } catch (e) { typeStack = e.stack; }
                const holds = Array.from({length: 1024}, () => sleep(60000));
                let rangeStack = "";
                try { await sleep(60000); } catch (e) { if (e instanceof RangeError) rangeStack = e.stack; }
                if (!toolStack.includes("codemode.js:3")) throw new Error("toolStack: " + toolStack);
                if (!typeStack.includes("codemode.js:5")) throw new Error("typeStack: " + typeStack);
                if (!rangeStack.includes("codemode.js:8")) throw new Error("rangeStack: " + rangeStack);
                return "stacks-ok";
                "#,
            )
            .await;
        assert_eq!(st_caught.exit_code, Some(0), "{out_caught}");
        assert_eq!(out_caught, "stacks-ok\n");

        for bad in [
            "return Symbol('x')",
            "return () => 1",
            "return 1n",
            "const c = {}; c.self = c; return c;",
        ] {
            let (st, out) = fixture.result(bad).await;
            assert_eq!(st.exit_code, Some(1), "{bad}: {out}");
            assert!(
                out.contains("Script error: TypeError: the return value is not JSON-serializable"),
                "{bad}: {out}"
            );
        }

        let (st_null, out_null) = fixture.result("return null").await;
        assert_eq!((st_null.exit_code, out_null.as_str()), (Some(0), "null\n"));
        let (st_str, out_str) = fixture.result("return \"a\\nb\"").await;
        assert_eq!((st_str.exit_code, out_str.as_str()), (Some(0), "a\nb\n"));
    }

    #[test]
    fn very_large_sleep_splits_preserve_time_and_allow_exact_chunk_subtraction() {
        for ms in [
            (1_u128 << 92) as f64,
            (1_u128 << 93) as f64,
            ((1_u128 << 93) + (1_u128 << 41)) as f64,
            (1_u128 << 94) as f64,
            f64::MAX,
        ] {
            let mut deferred = Vec::new();
            let part = split_sleep(ms, &mut deferred);
            assert!(part <= (1_u128 << 92) as f64);
            let chunk = SLEEP_CHUNK_MS as f64;
            assert_eq!(part - (part - chunk), chunk);
            let restored = deferred.iter().rev().fold(part, |sum, half| sum + half);
            assert_eq!(restored, ms);
            assert!(deferred.len() <= 932);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn long_sleep_keeps_every_chunk_of_the_requested_delay() {
        let chunk = Duration::from_millis(SLEEP_CHUNK_MS);
        let sleeping = tokio::spawn(sleep((SLEEP_CHUNK_MS * 2 + 1) as f64));
        tokio::task::yield_now().await;
        for _ in 0..2 {
            tokio::time::advance(chunk).await;
            tokio::task::yield_now().await;
            assert!(
                !sleeping.is_finished(),
                "sleep discarded the remaining delay"
            );
        }
        tokio::time::advance(Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(sleeping.is_finished());
        sleeping.await.unwrap();
    }

    #[tokio::test]
    async fn first_line_stack_columns_match_later_lines_and_keep_custom_stacks() {
        fn columns(output: &str, line: u32) -> Vec<u32> {
            let location = format!("codemode.js:{line}:");
            output
                .lines()
                .filter(|line| line.starts_with("    at "))
                .filter_map(|frame| frame.rsplit_once(&location))
                .map(|(_, column)| column.trim_end_matches(')').parse().unwrap())
                .collect()
        }

        let fixture = Fixture::new();
        for code in [
            "throw new Error('codemode.js:1:99');",
            "const = 1;",
            "return new Error('direct').stack;",
            "console.error(new Error('console'));",
            "await tools.read({path: 'missing-column.txt'});",
            "await tools.exec({action: 'bogus'});",
            "try { await tools.read({path: 'missing-column.txt'}); } catch (e) { return e.stack; }",
            "try { await tools.exec({action: 'bogus'}); } catch (e) { return e.stack; }",
            "const holds = Array.from({length: 1024}, () => sleep(60000)); try { await sleep(60000); } catch (e) { return e.stack; }",
            "const bad = {toJSON() { throw new Error('inner'); }}; try { await tools.write({path: 'unused', content: bad}); } catch (e) { return e.stack; }",
            "let saved; try { await tools.read({path: 'missing-column.txt'}); } catch (e) { saved = e; } try { await tools.write({path: 'unused', content: {toJSON() { throw saved; }}}); } catch (e) { return e.stack; }",
        ] {
            let (first_state, first) = fixture.result(code).await;
            let (later_state, later) = fixture.result(&format!("\n{code}")).await;
            assert_eq!(
                first_state.exit_code, later_state.exit_code,
                "{first}\n{later}"
            );
            let first_columns = columns(&first, 1);
            let later_columns = columns(&later, 2);
            assert!(!first_columns.is_empty(), "{code}: {first}");
            assert_eq!(first_columns, later_columns, "{code}:\n{first}\n{later}");
            if code.starts_with("throw new Error") {
                assert!(first.contains("Error: codemode.js:1:99"), "{first}");
            }
        }

        let custom = "    at custom (codemode.js:1:73)\n    at caller (custom.js:2:3)";
        for setup in [
            format!(
                "const error = new Error('custom'); Object.defineProperty(error, 'stack', {{value: {custom:?}, configurable: true}});"
            ),
            format!("const error = new Error('custom'); error.stack = {custom:?};"),
            format!(
                "class Custom extends Error {{ get stack() {{ return {custom:?}; }} }} const error = new Custom('custom');"
            ),
        ] {
            for ending in [
                "throw error;",
                "try { await tools.write({path: 'unused', content: {toJSON() { throw error; }}}); } catch (e) { return e.stack; }",
            ] {
                let (_, output) = fixture.result(&format!("{setup} {ending}")).await;
                assert!(output.contains(custom), "{output}");
            }
        }
    }

    #[tokio::test]
    async fn sleep_primitive_race_kill_and_validation() {
        let fixture = Fixture::new();
        let child_script = fixture.start("await sleep(10000);", None);
        let child_id = child_script.id.clone();
        let start_t = Instant::now();
        let (st, out) = fixture
            .result(&format!(
                r#"
                await sleep(-3);
                await sleep(0);
                for (const bad of [NaN, Infinity, -Infinity, "1", null, undefined, {{}}]) {{
                    let ok = false;
                    try {{ await sleep(bad); }} catch (e) {{ if (e instanceof TypeError) ok = true; }}
                    if (!ok) throw new Error("accepted bad sleep: " + String(bad));
                }}
                const winner = await Promise.race([
                    tools.exec({{action: "poll", taskId: {child_id:?}}}),
                    sleep(100).then(() => "timeout"),
                ]);
                return winner;
                "#
            ))
            .await;
        assert_eq!(st.exit_code, Some(0), "{out}");
        assert_eq!(out, "timeout\n");
        assert!(start_t.elapsed() < Duration::from_secs(3));
        child_script.kill().unwrap();
        complete(&child_script).await;

        let sleeping = fixture.start(
            "const p = sleep(Number.MAX_VALUE); text('ready'); await p;",
            None,
        );
        let wait_deadline = Instant::now() + Duration::from_secs(10);
        while lock(&sleeping.script().unwrap().0.report)
            .pending
            .is_empty()
        {
            assert!(
                Instant::now() < wait_deadline,
                "sleeping script did not admit sleep(Number.MAX_VALUE) in time"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let kill_t = Instant::now();
        sleeping.kill().unwrap();
        complete(&sleeping).await;
        assert!(kill_t.elapsed() < Duration::from_secs(3));
        assert_eq!(sleeping.snapshot().state, "killed");
        let report = sleeping
            .script()
            .unwrap()
            .report(crate::lease::process_pool(), None)
            .unwrap();
        assert_eq!(report.cancelled.len(), 1);
        assert_eq!(report.cancelled[0].tool, "sleep");
    }

    #[tokio::test]
    async fn passive_waits_and_control_calls_need_no_start_or_io_permits() {
        let fixture = Fixture::new();
        std::fs::write(fixture.dir.join("probe.txt"), "fast-read\n").unwrap();

        // Start 2 background script tasks (using 2 of Limits::OFF's 4 script slots) so the
        // orchestrator script (slot 3) can hold 8 passive polls across distinct readers,
        // read files, list, and kill without starvation.
        let bg1 = fixture.start("text('ready'); await sleep(60000);", None);
        let bg2 = fixture.start("await sleep(60000);", None);
        let id1 = bg1.id.clone();
        let id2 = bg2.id.clone();

        let t0 = Instant::now();
        let (st, out) = fixture
            .result(&format!(
                r#"
                const polls1 = Array.from({{length: 8}}, (_, i) =>
                    tools.exec({{action: "poll", taskId: {id1:?} + ".a00" + i, waitMs: 2000}})
                        .then(() => "poll_done")
                );
                const polls2 = Array.from({{length: 8}}, (_, i) =>
                    tools.exec({{action: "poll", taskId: {id2:?} + ".b00" + i}})
                );
                await sleep(20);
                // 16 passive polls are in flight simultaneously; read, list, and kill still run immediately.
                const raceWinner = await Promise.race([
                    tools.read({{path: "probe.txt"}}).then(r => r.text.trim()),
                    Promise.all(polls1).then(() => "polls_won"),
                ]);
                if (raceWinner !== "fast-read") throw new Error("read delayed: " + raceWinner);
                const listed = await tools.exec({{action: "list", state: "active"}});
                if (listed.tasks.length < 2) throw new Error("list failed");
                const k2 = await tools.exec({{action: "kill", taskId: {id2:?}}});
                if (k2.state !== "killed") throw new Error("kill failed: " + k2.state);
                const settled2 = await Promise.all(polls2);
                if (!settled2.every(s => s.state === "killed")) throw new Error("polls2 state");
                const k1 = await tools.exec({{action: "kill", taskId: {id1:?}}});
                if (k1.state !== "killed") throw new Error("k1 failed");
                await Promise.allSettled(polls1);
                return "permits-ok";
                "#
            ))
            .await;
        assert_eq!(st.exit_code, Some(0), "{out}");
        assert_eq!(out, "permits-ok\n");
        assert!(
            t0.elapsed() < Duration::from_secs(10),
            "took {:?}",
            t0.elapsed()
        );
    }

    #[tokio::test]
    async fn sync_serialization_errors_keep_inner_frames_and_sleep_resolves_undefined() {
        let fixture = Fixture::new();
        let (st, out) = fixture
            .result(
                "const slept = await sleep(0);\n\
                 if (slept !== undefined) throw new Error('expected undefined');\n\
                 const bad = {\n\
                     toJSON() { throw new Error('inner-tojson'); }\n\
                 };\n\
                 try {\n\
                     await tools.write({ path: 'out.txt', content: bad });\n\
                 } catch (e) {\n\
                     return e.stack;\n\
                 }",
            )
            .await;
        assert_eq!(st.exit_code, Some(0), "{out}");
        assert!(
            out.contains("codemode.js:4") && out.contains("codemode.js:7"),
            "expected both inner toJSON frame and tools.write call frame: {out}"
        );
        assert!(!out.contains("codemode-prelude.js"), "{out}");
    }

    #[tokio::test]
    async fn sh_template_join_quote_contract_and_prototype_pollution_immunity() {
        let fixture = Fixture::new();
        let (st, out) = fixture
            .result(
                r#"
                function assertEq(got, exp, label) {
                    if (got !== exp) {
                        throw new Error(label + ": " + JSON.stringify(got) + " !== " + JSON.stringify(exp));
                    }
                }
                function assertTypeError(fn, label) {
                    let ok = false;
                    try { fn(); } catch (e) { if (e instanceof TypeError) ok = true; }
                    if (!ok) throw new Error("expected TypeError for " + label);
                }

                // 1. Frozen and non-constructible function objects without own .prototype.
                assertEq(typeof sh, "function", "typeof sh");
                assertEq(typeof sh.quote, "function", "typeof sh.quote");
                assertEq(typeof sh.join, "function", "typeof sh.join");
                assertEq(Object.isFrozen(sh), true, "sh frozen");
                assertEq(Object.isFrozen(sh.quote), true, "sh.quote frozen");
                assertEq(Object.isFrozen(sh.join), true, "sh.join frozen");
                for (const [name, fn] of [["sh", sh], ["sh.quote", sh.quote], ["sh.join", sh.join]]) {
                    assertEq(
                        Object.prototype.hasOwnProperty.call(fn, "prototype"),
                        false,
                        name + " has no own prototype"
                    );
                    assertTypeError(() => new fn(), "new " + name);
                }
                for (const prop of ["quote", "join"]) {
                    const desc = Object.getOwnPropertyDescriptor(sh, prop);
                    assertEq(desc.writable, false, "sh." + prop + " writable");
                    assertEq(desc.configurable, false, "sh." + prop + " configurable");
                }

                // 2. sh.quote valid and invalid inputs.
                assertEq(sh.quote(""), "''", "quote empty");
                assertEq(sh.quote("hello world"), "'hello world'", "quote spaces");
                assertEq(sh.quote("it's $HOME `id` \\ \n"), "'it'\\''s $HOME `id` \\ \n'", "quote special");
                assertEq(sh.quote(0), "'0'", "quote 0");
                assertEq(sh.quote(-3.5), "'-3.5'", "quote float");
                for (const bad of [null, undefined, true, false, {}, [], NaN, Infinity, -Infinity, "a\0b", Symbol("x"), 1n]) {
                    assertTypeError(() => sh.quote(bad), "sh.quote(" + typeof bad + ")");
                }

                // 3. sh.join valid and invalid inputs, sparse arrays, and Proxy length tampering.
                assertEq(sh.join([]), "", "join empty");
                assertEq(sh.join(["solo"]), "'solo'", "join single");
                assertEq(
                    sh.join(["a b", "it's", "line1\nline2", 42, -1.5, ""]),
                    "'a b' 'it'\\''s' 'line1\nline2' '42' '-1.5' ''",
                    "join multi"
                );
                for (const bad of [null, undefined, "abc", 123, { 0: "a", length: 1 }, true]) {
                    assertTypeError(() => sh.join(bad), "sh.join non-array");
                }
                const sparse1 = new Array(1);
                const sparse2 = ["a", , "b"];
                for (const badArr of [
                    [undefined],
                    [null],
                    [NaN],
                    [Infinity],
                    [-Infinity],
                    [true],
                    [{}],
                    [["nested"]],
                    ["a\0b"],
                    [Symbol("x")],
                    [1n],
                    sparse1,
                    sparse2,
                ]) {
                    assertTypeError(() => sh.join(badArr), "sh.join invalid element");
                }
                for (const badLen of [NaN, -1, 1.5, Infinity, -Infinity, "0", "1", null, undefined]) {
                    const proxied = new Proxy(["a"], {
                        get(target, prop, receiver) {
                            if (prop === "length") return badLen;
                            return Reflect.get(target, prop, receiver);
                        },
                    });
                    assertTypeError(() => sh.join(proxied), "sh.join Proxy length=" + String(badLen));
                }

                // 4. sh`...` tagged template: raw escapes, scalar+array, empty array, errors.
                assertEq(sh`printf '%s\n\t\s\\'`, "printf '%s\\n\\t\\s\\\\'", "raw escapes");
                assertEq(
                    sh`rg ${"-n"} ${["foo bar", "a'b"]} ${123}`,
                    "rg '-n' 'foo bar' 'a'\\''b' '123'",
                    "mixed scalar and array"
                );
                assertEq(sh`cmd ${[]}`, "cmd ", "empty array trailing");
                assertEq(sh`ls ${[]} /tmp`, "ls  /tmp", "empty array middle");
                assertTypeError(() => sh(), "sh()");
                assertTypeError(() => sh("ls"), "sh('ls')");
                assertTypeError(() => sh(null), "sh(null)");
                assertTypeError(() => sh({}), "sh({})");
                assertTypeError(() => sh({ raw: "not-array" }), "sh raw non-array");
                assertTypeError(() => sh({ raw: [] }), "sh raw empty");
                assertTypeError(() => sh({ raw: ["a"] }, "x"), "sh raw too short");
                assertTypeError(() => sh({ raw: ["a", "b"] }), "sh raw too long");
                assertTypeError(() => sh({ raw: ["a\0b"] }), "sh raw NUL");
                assertTypeError(() => sh({ raw: [123] }), "sh raw non-string");
                assertTypeError(() => sh({ raw: [null] }), "sh raw null seg");

                const sparseRaw = new Array(2);
                sparseRaw[0] = "echo ";
                assertTypeError(() => sh({ raw: sparseRaw }, "x"), "sh sparse raw");

                let rawReads = 0;
                const oneShotOk = {
                    get raw() {
                        rawReads++;
                        if (rawReads > 1) throw new Error("raw getter read more than once");
                        return ["echo ", " end"];
                    },
                };
                assertEq(sh(oneShotOk, "hi"), "echo 'hi' end", "sh raw getter single read ok");
                assertEq(rawReads, 1, "raw getter read count ok");

                let badRawReads = 0;
                const oneShotMismatch = {
                    get raw() {
                        badRawReads++;
                        return ["only-one"];
                    },
                };
                assertTypeError(() => sh(oneShotMismatch, "extra"), "sh raw getter mismatch");
                assertEq(badRawReads, 1, "raw getter read count mismatch");

                for (const badLen of [NaN, -1, 1.5, Infinity, "1"]) {
                    const proxiedRaw = new Proxy(["a"], {
                        get(target, prop, receiver) {
                            if (prop === "length") return badLen;
                            return Reflect.get(target, prop, receiver);
                        },
                    });
                    assertTypeError(() => sh({ raw: proxiedRaw }), "sh raw Proxy length=" + String(badLen));
                }

                for (const badVal of [undefined, null, true, {}, NaN, Infinity, -Infinity, [["nested"]], sparse1, "a\0b", Symbol("x"), 1n]) {
                    assertTypeError(() => sh`echo ${badVal}`, "sh bad interpolation");
                }

                // 5. Prototype pollution immunity.
                Array.prototype.join = () => "POLLUTED_JOIN";
                Array.prototype.map = () => ["POLLUTED_MAP"];
                Array.prototype[Symbol.iterator] = function* () { yield "POLLUTED_ITER"; };
                Array.prototype[0] = "POLLUTED_ARR0";
                Object.prototype[0] = "POLLUTED_OBJ0";
                Object.prototype.raw = ["POLLUTED_RAW"];

                assertEq(sh.quote("it's"), "'it'\\''s'", "polluted quote");
                assertEq(sh.join(["a b", 7]), "'a b' '7'", "polluted join");
                assertEq(sh.join([]), "", "polluted join empty");
                assertEq(sh`printf '%s\n' ${["a b", "c'd"]}`, "printf '%s\\n' 'a b' 'c'\\''d'", "polluted template");
                assertTypeError(() => sh({}), "polluted Object.prototype.raw rejected");
                assertTypeError(() => sh.join(new Array(1)), "polluted sparse join rejected");
                const pollutedSparseRaw = new Array(1);
                assertTypeError(() => sh({ raw: pollutedSparseRaw }), "polluted sparse raw rejected");

                return "sh-contract-ok";
                "#,
            )
            .await;
        assert_eq!(st.exit_code, Some(0), "{out}");
        assert_eq!(out, "sh-contract-ok\n");
    }
}
