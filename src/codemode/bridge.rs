//! Asynchronous host bridge, 256 MiB pending-call budget, permits, and script output writer.

use super::control::{
    CallInfo, InFlightGuard, NestedCall, Outcome, QueuedStartGuard, ScriptHandle, Wake, lock,
};
use crate::files;
use crate::lease::{BytePool, Lease};
use crate::service::render::{OutputMode, Render};
use crate::service::schema::{self as service_schema, Action};
use crate::service::{Call, Service};
use crate::session::{LaunchContext, Session};
use crate::tasks::TASK_LOG_LIMIT;
use std::io::{PipeWriter, Write};
use std::sync::Arc;
use tokio::runtime::Handle;
use tokio::sync::Semaphore;

pub(super) const SLEEP_CHUNK_MS: u64 = 1_u64 << 40;
pub(crate) const BRIDGE_LIMIT: usize = 256 * 1024 * 1024;
pub(super) const BRIDGE_ERROR: &str =
    "Script tool-call bridge exceeds 256 MiB of pending arguments and results.";
pub(super) const DIAGNOSTIC_RESERVE: u64 = 4096;
// One bounded description and pending-call text coexist until diagnostics finish writing.
pub(super) const DIAGNOSTIC_WORKSPACE: usize = 3 * DIAGNOSTIC_RESERVE as usize;
pub(super) const BRIDGE_RESULT_ERROR: &str = "The tool call completed, but its result does not fit in the 256 MiB pending bridge budget; the operation was not undone. Do not retry start or resend input; find started task handles in the outer codemode report.";
pub(super) const BRIDGE_MEMORY_RESULT_ERROR: &str = "The tool call completed, but its result does not fit in the process active memory budget; the operation was not undone. Do not retry start or resend input; use exec list to locate a started task.";
pub(super) const START_PERMITS: usize = 8;
pub(super) const IO_PERMITS: usize = 8;
pub(super) const MAX_STARTS: usize = 4096;
pub(super) const STARTS_ERROR: &str = "A script may accept at most 4096 exec start calls.";

pub(super) struct Output {
    pub(super) writer: PipeWriter,
    pub(super) written: u64,
}

impl Output {
    fn capacity(&self, bytes: u64) -> Result<(), String> {
        if self.written + bytes > TASK_LOG_LIMIT - DIAGNOSTIC_RESERVE {
            Err("Script output exceeds 64 MiB (4 KiB is reserved for diagnostics).".into())
        } else {
            Ok(())
        }
    }

    pub(super) fn write(&mut self, text: &str) -> Result<(), String> {
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

    pub(super) fn diagnostic(&mut self, parts: &[&str]) {
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

pub(super) struct Host {
    pub(super) service: Arc<Service>,
    pub(super) session: Arc<Session>,
    pub(super) launch: Arc<LaunchContext>,
    pub(super) transfer: bool,
    pub(super) call_id: Option<u64>,
    pub(super) parent_task_id: String,
    pub(super) runtime: Handle,
    pub(super) script: ScriptHandle,
    pub(super) start_permits: Arc<Semaphore>,
    pub(super) io_permits: Arc<Semaphore>,
    pub(super) next: u64,
    pub(super) starts: usize,
}

enum ToolCall {
    Exec(Box<service_schema::Request>),
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
            "exec" => serde_json::from_value::<service_schema::Request>(value)
                .map(|mut request| {
                    service_schema::normalize_request(&mut request);
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

pub(super) fn split_sleep(mut ms: f64, deferred: &mut Vec<f64>) -> f64 {
    // Above this threshold, subtracting one timer chunk can round away or remove two chunks.
    while ms > (1_u128 << 92) as f64 {
        ms /= 2.0;
        deferred.push(ms);
    }
    ms
}

pub(super) async fn sleep(mut ms: f64) {
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
pub(super) fn queue_permit(
    pool: Arc<Semaphore>,
) -> impl Future<Output = tokio::sync::OwnedSemaphorePermit> {
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
    pub(super) fn call(
        &mut self,
        tool: &str,
        arguments: &str,
    ) -> Result<u64, crate::ipc::Owned<String>> {
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
            ToolCall::Exec(request) if matches!(request.action, Action::Kill | Action::List)
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
        let queued_guard = if let ToolCall::Exec(request) = &request
            && request.action == Action::Start
            && service_schema::validate(request, service_schema::ExecPolicy::PROGRAM).is_ok()
        {
            if self.starts >= MAX_STARTS {
                lock(&self.script.0.report).remove(id);
                return Err(crate::ipc::Owned::new(STARTS_ERROR.into(), input_owner));
            }
            self.starts += 1;
            Some(QueuedStartGuard::new(&self.script))
        } else {
            None
        };
        let permit_pool = match &request {
            ToolCall::Exec(req) => match req.action {
                Action::Start => Some(Arc::clone(&self.start_permits)),
                Action::Transcript => Some(Arc::clone(&self.io_permits)),
                Action::Poll | Action::Kill | Action::List => None,
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
        let parent_task_id = self.parent_task_id.clone();
        let cancel = script.0.cancel.child_token();
        let nested_render = Render {
            output_mode: OutputMode::Structured,
            structured_content: true,
        };
        let response_pool = Arc::clone(launch.memory());
        let response_control = is_control.then(|| Arc::clone(launch.control_memory()));
        self.runtime.spawn(input_owner.clone().hold_future(async move {
            let _in_flight = in_flight;
            let queued_guard = queued_guard;
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
                                started: Some(NestedCall {
                                    script: &script,
                                    pending_id: id,
                                    parent_task_id: &parent_task_id,
                                }),
                                queued_guard,
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

pub(super) fn nested_envelope(
    mut tracked: files::TrackedResult,
) -> crate::ipc::Owned<serde_json::Value> {
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

pub(super) fn encode_nested_result(
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
