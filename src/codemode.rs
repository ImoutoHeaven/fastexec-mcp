//! JavaScript tasks and their asynchronous bridge to the four host tools.

use crate::files;
use crate::images;
use crate::output::Truncate;
use crate::service::{self, Call, OutputMode, Render, Service};
use crate::session::{LaunchContext, Session, parse_handle};
use crate::tasks::{TASK_LOG_LIMIT, Task, Tasks};
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

const MAX_CODE: usize = 256 * 1024;
const MEMORY_LIMIT: usize = 128 * 1024 * 1024;
const BRIDGE_LIMIT: usize = 128 * 1024 * 1024;
const BRIDGE_ERROR: &str =
    "Script tool-call bridge exceeds 128 MiB of pending arguments and results.";
const DIAGNOSTIC_RESERVE: u64 = 4096;
const MAX_IMAGES: usize = 64;
const BRIDGE_RESULT_ERROR: &str = "The tool call completed, but its result does not fit in the 128 MiB pending bridge budget; the operation was not undone.";
const ACTIVE_CALLS: usize = 8;
const QUEUED_CALLS: usize = 1024;
const MAX_STARTS: usize = 4096;
const STARTS_ERROR: &str = "A script may accept at most 4096 exec start calls.";
const STALLED: &str = "The script is waiting on a promise that can never settle: no tool call is pending, and timers do not exist here.";

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Request {
    /// Body of an async JavaScript function, up to 256 KiB. Top-level await and return work.
    pub code: String,
    /// Working directory for nested exec (when it omits cwd) and relative paths in read, write, and edit. Omit for the session's default working directory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// start, poll: wait up to 240000 ms per call. Poll again for longer. Ignored by kill.
    #[schemars(range(min = 0, max = 240000))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_ms: Option<u64>,
    /// start, poll: any match returns; the task continues. Text conditions take 1-16 case-sensitive literals, 1-4096 bytes each, without line breaks. waitMs defaults to 30000.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_when: Option<ReturnWhen>,
    /// start: kill the entire tree, including kept processes, this many ms after launch. Zero or omitted disables it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kill_after_ms: Option<u64>,
    /// Window placement for all actions. Default head_tail, or tail for transcript.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncate: Option<Truncate>,
    /// Result budget in bytes, 100-1048576 (default 16384). Errors and correctness warnings stay whole. Not with truncate "none".
    #[schemars(range(min = 100, max = 1048576))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReturnWhen {
    /// Match a substring within one unseen line, including text before carriage-return rewrites. Bash lines are cleaned. Codemode lines are unchanged.
    pub output_contains: Option<Vec<String>>,
    /// 10-60000 ms of silence, counted from wait start (after input and keys) or the latest output, whichever is later.
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

struct PendingCall {
    info: CallInfo,
    bytes: usize,
}

#[derive(Default)]
struct Report {
    pending: BTreeMap<u64, PendingCall>,
    bridge_bytes: usize,
    cancelled: Vec<CallInfo>,
    started: Vec<String>,
    started_ids: HashSet<String>,
}

impl Report {
    fn admit(&mut self, id: u64, info: CallInfo, bytes: usize) -> Result<(), String> {
        if self.pending.len() >= ACTIVE_CALLS + QUEUED_CALLS {
            return Err("At most 8 tool calls may run and 1024 may queue per script.".into());
        }
        if bytes > BRIDGE_LIMIT - self.bridge_bytes {
            return Err(BRIDGE_ERROR.into());
        }
        self.bridge_bytes += bytes;
        self.pending.insert(id, PendingCall { info, bytes });
        Ok(())
    }

    fn result(&mut self, id: u64, bytes: usize) -> bool {
        let Some(pending) = self.pending.get_mut(&id) else {
            return false;
        };
        self.bridge_bytes -= pending.bytes;
        pending.bytes = 0;
        if bytes > BRIDGE_LIMIT - self.bridge_bytes {
            return false;
        }
        self.bridge_bytes += bytes;
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
    Value(String),
    Error(String),
    Limit,
}

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
}

#[derive(Clone)]
pub struct ScriptHandle(Arc<Control>);

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
    pub fn kill(&self) {
        self.0.stopped.store(true, Ordering::Release);
        self.0.cancel.cancel();
        let _ = self.0.wake.send(Wake::Stop);
    }

    fn interrupted(&self) -> bool {
        self.0.stopped.load(Ordering::Acquire) || self.0.exited.load(Ordering::Acquire)
    }

    pub fn report(&self) -> (Vec<CallInfo>, Vec<String>) {
        let report = lock(&self.0.report);
        (report.cancelled.clone(), report.started.clone())
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

    fn cancel_and_drain(&self) {
        self.0.cancel.cancel();
        let (count, idle) = &self.0.in_flight;
        let active = lock(count);
        // A start can still be registering its task; only the caller's wait has a deadline.
        drop(idle.wait_while(active, |&mut active| active > 0));
    }

    fn finish(&self) -> Vec<CallInfo> {
        let mut report = lock(&self.0.report);
        report.cancelled = report
            .pending
            .values()
            .map(|pending| pending.info.clone())
            .collect();
        report.pending.clear();
        report.bridge_bytes = 0;
        self.0.cancel.cancel();
        report.cancelled.clone()
    }
}

pub struct Work {
    pub service: Arc<Service>,
    pub session: Arc<Session>,
    pub launch: Arc<LaunchContext>,
    pub http: bool,
    pub call_id: Option<u64>,
    pub code: String,
    pub writer: PipeWriter,
    pub task: Arc<Task>,
    pub tasks: Arc<Tasks>,
    pub captured: mpsc::Receiver<()>,
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
    image_bytes: u64,
    images: usize,
}

impl Output {
    fn capacity(&self, bytes: u64) -> Result<(), String> {
        if self.written + self.image_bytes + bytes > TASK_LOG_LIMIT - DIAGNOSTIC_RESERVE {
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

    fn diagnostic(&mut self, text: &str) {
        let left = TASK_LOG_LIMIT.saturating_sub(self.written + self.image_bytes);
        if left == 0 {
            return;
        }
        let mut end = text.len().min(left.saturating_sub(1) as usize);
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        let _ = self.line(&text[..end]);
    }
}

struct Host {
    service: Arc<Service>,
    session: Arc<Session>,
    launch: Arc<LaunchContext>,
    http: bool,
    call_id: Option<u64>,
    runtime: Handle,
    script: ScriptHandle,
    semaphore: Arc<Semaphore>,
    next: u64,
    starts: usize,
}

enum ToolCall {
    Exec(Box<service::Request>),
    Read(files::ReadRequest),
    Write(files::WriteRequest),
    Edit(files::EditRequest),
}

impl ToolCall {
    fn parse(tool: &str, value: serde_json::Value) -> Result<Self, String> {
        if !value.is_object() {
            return Err("Tool arguments must be an object.".into());
        }
        let invalid = |error: serde_json::Error| format!("Invalid {tool} arguments: {error}");
        match tool {
            "exec" => serde_json::from_value(value)
                .map(|request| Self::Exec(Box::new(request)))
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

impl Host {
    fn call(&mut self, tool: String, arguments: String) -> Result<u64, String> {
        if self.script.interrupted() || self.script.0.cancel.is_cancelled() {
            return Err("The script is ending; no new tool calls can start.".into());
        }
        let id = self.next;
        self.next += 1;
        lock(&self.script.0.report).admit(
            id,
            CallInfo {
                tool: tool.clone(),
                action: None,
                task_id: None,
            },
            arguments.len(),
        )?;
        let parsed = (|| {
            let value: serde_json::Value =
                serde_json::from_str(&arguments).map_err(|error| error.to_string())?;
            let info = CallInfo {
                tool: tool.clone(),
                action: value
                    .get("action")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned),
                task_id: value
                    .get("taskId")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned),
            };
            let request = ToolCall::parse(&tool, value)?;
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
                return Err(error);
            }
        };
        if let ToolCall::Exec(request) = &request
            && request.action == service::Action::Start
            && service::validate(request, true).is_ok()
        {
            if self.starts >= MAX_STARTS {
                lock(&self.script.0.report).remove(id);
                return Err(STARTS_ERROR.into());
            }
            self.starts += 1;
        }
        drop(arguments);
        let semaphore = Arc::clone(&self.semaphore);
        let service = Arc::clone(&self.service);
        let session = Arc::clone(&self.session);
        let launch = Arc::clone(&self.launch);
        let script = self.script.clone();
        let in_flight = InFlightGuard::enter(script.clone());
        let http = self.http;
        let call_id = self.call_id;
        let cancel = script.0.cancel.child_token();
        let nested_render = Render {
            output_mode: OutputMode::Structured,
            structured_content: true,
        };
        self.runtime.spawn(async move {
            let _in_flight = in_flight;
            let _permit = tokio::select! {
                biased;
                _ = cancel.cancelled() => return,
                permit = semaphore.acquire_owned() => match permit {
                    Ok(permit) => permit,
                    Err(_) => return,
                },
            };
            let result = match request {
                ToolCall::Exec(request) => {
                    service
                        .call(
                            *request,
                            Call {
                                session: &session,
                                launch: Some(&launch),
                                render: nested_render,
                                cancel: cancel.clone(),
                                progress: None,
                                call_id,
                                nested: true,
                                started: Some(&script),
                            },
                        )
                        .await
                }
                request => {
                    tokio::select! {
                        biased;
                        _ = cancel.cancelled() => return,
                        result = async move {
                            match request {
                                ToolCall::Read(request) => {
                                    files::read_nested(request, launch, http, nested_render).await
                                }
                                ToolCall::Write(request) => {
                                    files::write(request, launch, nested_render).await
                                }
                                ToolCall::Edit(request) => {
                                    files::edit(request, launch, nested_render).await
                                }
                                ToolCall::Exec(_) => unreachable!(),
                            }
                        } => result,
                    }
                }
            };
            let envelope = nested_envelope(result);
            let mut outcome = match serde_json::to_string(&envelope) {
                Ok(value) => Outcome::Value(value),
                Err(error) => Outcome::Error(error.to_string()),
            };
            let bytes = match &outcome {
                Outcome::Value(value) | Outcome::Error(value) => value.len(),
                Outcome::Limit => 0,
            };
            if !lock(&script.0.report).result(id, bytes) {
                outcome = Outcome::Limit;
            }
            let _ = script.0.wake.send(Wake::Settled { id, outcome });
        });
        Ok(id)
    }
}

fn nested_envelope(result: rmcp::model::CallToolResult) -> serde_json::Value {
    let mut ok = !result.is_error.unwrap_or(false);
    let mut map = match result.structured_content {
        Some(serde_json::Value::Object(map)) => map,
        _ => {
            let text = result
                .content
                .first()
                .and_then(|block| block.as_text())
                .map_or_else(|| "Tool operation failed.".to_owned(), |t| t.text.clone());
            let mut fallback = serde_json::Map::new();
            fallback.insert("error".into(), json!(text));
            ok = false;
            fallback
        }
    };
    if map.remove("ok") == Some(serde_json::Value::Bool(false)) {
        ok = false;
    }
    let mut images = result.content.iter().filter_map(|block| {
        let image = block.as_image()?;
        Some(json!({"type": "image", "data": image.data, "mimeType": image.mime_type}))
    });
    match map.get("kind").and_then(serde_json::Value::as_str) {
        Some("image") => {
            if let Some(image) = images.next() {
                map.insert("image".into(), image);
            } else if let Some(note) = result.content.first().and_then(|block| block.as_text()) {
                map.insert("note".into(), json!(note.text));
            }
        }
        Some("codemode") => {
            let blocks: Vec<_> = images.collect();
            if !blocks.is_empty() {
                map.entry("codemode").or_insert_with(|| json!({}))["images"] = blocks.into();
            }
        }
        _ => {}
    }
    json!({ "ok": ok, "value": map })
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
        http,
        call_id,
        code,
        writer,
        task,
        tasks,
        captured,
    } = work;
    let output = Rc::new(RefCell::new(Output {
        writer,
        written: 0,
        image_bytes: 0,
        images: 0,
    }));
    let host = Rc::new(RefCell::new(Host {
        service,
        session,
        launch,
        http,
        call_id,
        runtime,
        script: script.clone(),
        semaphore: Arc::new(Semaphore::new(ACTIVE_CALLS)),
        next: 1,
        starts: 0,
    }));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_vm(
            &code,
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
        output.borrow_mut().diagnostic(&format!(
            "Script stopped: cancelled {} pending call(s): {}",
            pending.len(),
            pending_text(&pending)
        ));
        None
    } else if script.0.exited.load(Ordering::Acquire) {
        Some(0)
    } else if let Err(message) = result {
        let suffix = if pending.is_empty() {
            String::new()
        } else {
            format!("\nPending calls: {}", pending_text(&pending))
        };
        output
            .borrow_mut()
            .diagnostic(&format!("Script error: {message}{suffix}"));
        Some(1)
    } else {
        Some(0)
    };
    drop(output);
    tasks.finish_script(&task, exit_code, captured);
}

fn describe<'js>(ctx: &Ctx<'js>, value: Value<'js>) -> String {
    use rquickjs::{Coerced, FromJs};
    let prop = |object: &Object<'js>, key: &str| -> Option<String> {
        match object.get::<_, Option<Coerced<String>>>(key) {
            Ok(value) => value.map(|coerced| coerced.0),
            Err(error) => {
                if error.is_exception() {
                    let _ = ctx.catch();
                }
                None
            }
        }
    };
    if let Some(exception) = value.as_exception() {
        let object = exception.as_object();
        let name = prop(object, "name").unwrap_or_else(|| "Error".into());
        let message = prop(object, "message").unwrap_or_default();
        let stack = prop(object, "stack").unwrap_or_default();
        let mut text = format!("{name}: {message}");
        for line in stack
            .lines()
            .filter(|line| !line.contains("codemode-prelude.js"))
        {
            text.push('\n');
            text.push_str(line);
        }
        text
    } else {
        Coerced::<String>::from_js(ctx, value).map_or_else(
            |error| {
                if error.is_exception() {
                    let _ = ctx.catch();
                }
                "The script threw a value that cannot be described.".into()
            },
            |value| value.0,
        )
    }
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
            let write = Function::new(ctx.clone(), move |ctx: Ctx<'_>, text: String| {
                if control.interrupted() {
                    return Err(Exception::throw_message(&ctx, "The script is ending."));
                }
                writing
                    .borrow_mut()
                    .write(&text)
                    .map_err(|error| Exception::throw_range(&ctx, &error))
            })
            .map_err(|error| js_error(&ctx, error))?;
            let call = Function::new(
                ctx.clone(),
                move |ctx: Ctx<'_>, tool: String, args: String| {
                    host.borrow_mut().call(tool, args).map_err(|error| {
                        if error.starts_with("At most")
                            || error == BRIDGE_ERROR
                            || error == STARTS_ERROR
                        {
                            Exception::throw_range(&ctx, &error)
                        } else {
                            Exception::throw_type(&ctx, &error)
                        }
                    })
                },
            )
            .map_err(|error| js_error(&ctx, error))?;
            let imaging = Rc::clone(&output);
            let task = Arc::clone(task);
            let tasks = Arc::clone(tasks);
            let control = script.clone();
            let image = Function::new(ctx.clone(), move |ctx: Ctx<'_>, data: String| {
                if control.interrupted() {
                    return Err(Exception::throw_message(&ctx, "The script is ending."));
                }
                let mut output = imaging.borrow_mut();
                if output.images >= MAX_IMAGES {
                    return Err(Exception::throw_range(
                        &ctx,
                        "A script may emit at most 64 images.",
                    ));
                }
                let cleaned: String = data.chars().filter(|ch| !is_js_whitespace(*ch)).collect();
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(cleaned)
                    .map_err(|_| {
                        Exception::throw_type(&ctx, "The image data is not valid base64.")
                    })?;
                let index = output.images + 1;
                let inline = match images::inline(bytes) {
                    Ok(image) => image,
                    Err(message) => {
                        output
                            .write(&format!("[image {index} omitted: {message}]"))
                            .map_err(|error| Exception::throw_range(&ctx, &error))?;
                        output.images = index;
                        return Ok(());
                    }
                };
                let path = task.image_path(index, &inline.mime);
                let mut marker = format!(
                    "[image {index}: {} ({}, {}x{}, {} bytes)]",
                    files::display_path(&path),
                    inline.mime,
                    inline.width,
                    inline.height,
                    inline.bytes.len()
                );
                for hint in &inline.hints {
                    marker.push(' ');
                    marker.push_str(hint);
                }
                let bytes = inline.bytes.len() as u64;
                output
                    .capacity(bytes + marker.len() as u64 + 1)
                    .map_err(|error| Exception::throw_range(&ctx, &error))?;
                let end_byte = output.written + marker.len() as u64 + 1;
                tasks
                    .store_image(&task, index, &inline.bytes, &inline.mime, end_byte)
                    .map_err(|error| Exception::throw_range(&ctx, &error))?;
                output.image_bytes += bytes;
                output.images = index;
                if let Err(error) = output.line(&marker) {
                    tasks.remove_image(&task, index);
                    output.image_bytes -= bytes;
                    return Err(Exception::throw_message(
                        &ctx,
                        &format!("Cannot write image marker: {error}"),
                    ));
                }
                Ok(())
            })
            .map_err(|error| js_error(&ctx, error))?;
            let control = script.clone();
            let exit = Function::new(ctx.clone(), move || {
                control.0.exited.store(true, Ordering::Release);
            })
            .map_err(|error| js_error(&ctx, error))?;
            let api: Object = factory
                .call((write, call, image, exit))
                .map_err(|error| js_error(&ctx, error))?;
            let mut options = EvalOptions::default();
            options.filename = Some("codemode.js".into());
            let function: Function = ctx
                .eval_with_options(format!("(async () => {{{code}\n}})"), options)
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
                        let (ok, range, payload) = match outcome {
                            Outcome::Value(value) => (true, false, value),
                            Outcome::Error(error) => (false, false, error),
                            Outcome::Limit => (false, true, BRIDGE_RESULT_ERROR.into()),
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

const PRELUDE: &str = r#"(function (__write, __call, __image, __exit) {
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
    const numberIsFinite = Number.isFinite;
    const arrayJoin = Function.prototype.call.bind(Array.prototype.join);
    const toStringTag = Symbol.toStringTag;
    const isArray = Array.isArray;
    const pending = new Map();
    let waiting = 0;
    let state = 0;
    let failure;

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
        members[name] = args => new PromiseCtor((resolve, reject) => {
            if (args === null || typeof args !== "object" || isArray(args)) {
                reject(new TypeErrorCtor("Tool arguments must be an object."));
                return;
            }
            try {
                const id = __call(name, stringify(args));
                set(pending, id, { resolve, reject, tool: name });
                waiting++;
            } catch (error) { reject(error); }
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
    function quote(value) {
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
    }
    const sh = objectFreeze({ quote });
    function extractDataUrlBase64(value) {
        if (value.length < 13) return undefined;
        if (stringToLowerCase(stringSlice(value, 0, 5)) !== "data:") return undefined;
        const comma = stringIndexOf(value, ",", 5);
        if (comma < 12) return undefined;
        if (stringToLowerCase(stringSlice(value, comma - 7, comma)) !== ";base64") return undefined;
        return stringSlice(value, comma + 1);
    }
    function image(value) {
        if (value && typeof value === "object" && value.kind === "image") {
            if (value.attached === false) {
                const note = typeof value.note === "string" && value.note.length > 0
                    ? value.note
                    : "The image could not be attached.";
                throw new TypeErrorCtor(note);
            }
            if (value.attached === true && value.image && typeof value.image === "object") {
                value = value.image;
            }
        }
        let data;
        if (value && typeof value === "object" && value.type === "image" && typeof value.data === "string") {
            data = value.data;
        } else if (typeof value === "string") {
            data = extractDataUrlBase64(value);
        }
        if (typeof data !== "string") {
            throw new TypeErrorCtor("image expects an attached image read result, an image block, or a base64 data URL.");
        }
        __image(data);
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
    globalThis.image = image;
    globalThis.exit = exit;
    return {
        run(fn) {
            then(fn(), value => {
                try { if (value !== undefined) text(value); state = 1; }
                catch (error) { failure = error; state = 2; }
            }, error => { failure = error; state = 2; });
        },
        settle(id, ok, payload, range) {
            const entry = get(pending, id);
            if (!entry) return;
            remove(pending, id);
            waiting--;
            if (!ok) { entry.reject(range ? new RangeErrorCtor(payload) : new ErrorCtor(payload)); return; }
            let envelope;
            try { envelope = parse(payload); } catch (error) { entry.reject(error); return; }
            if (envelope.ok) {
                entry.resolve(envelope.value);
            } else {
                entry.reject(new ToolError(entry.tool, envelope.value));
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
                        http: false,
                        call_id: None,
                        service: Arc::clone(&self.service),
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
    fn bridge_budget_replaces_arguments_with_results_and_releases_them() {
        let info = || CallInfo {
            tool: "read".into(),
            action: None,
            task_id: None,
        };
        let mut report = Report::default();
        report.admit(1, info(), BRIDGE_LIMIT - 4).unwrap();
        report.admit(2, info(), 4).unwrap();
        assert_eq!(report.admit(3, info(), 1).unwrap_err(), BRIDGE_ERROR);
        assert!(!report.result(1, BRIDGE_LIMIT));
        assert_eq!(report.bridge_bytes, 4);
        assert!(report.result(2, BRIDGE_LIMIT));
        assert_eq!(report.bridge_bytes, BRIDGE_LIMIT);
        report.remove(2);
        assert_eq!(report.bridge_bytes, 0);
        report.remove(1);
        assert!(report.pending.is_empty());
        report.admit(3, info(), 8).unwrap();
        assert!(report.result(3, 12));
        assert_eq!(report.bridge_bytes, 12);
        report.remove(3);
        assert_eq!(report.bridge_bytes, 0);
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
            http: false,
            call_id: None,
            runtime,
            script: script.clone(),
            semaphore: Arc::new(Semaphore::new(0)),
            next: 1,
            starts: MAX_STARTS - 1,
        };
        let args = json!({"action":"start","command":"true"}).to_string();
        host.call("exec".into(), args.clone()).unwrap();
        assert_eq!(host.starts, MAX_STARTS);
        assert_eq!(host.call("exec".into(), args).unwrap_err(), STARTS_ERROR);
        assert_eq!(host.starts, MAX_STARTS);
        // Unvalidated start requests resolve as tool errors without consuming start slots.
        host.call(
            "exec".into(),
            json!({"action":"start","command":""}).to_string(),
        )
        .unwrap();
        assert_eq!(host.starts, MAX_STARTS);
        assert_eq!(lock(&script.0.report).pending.len(), 2);
        host.call("read".into(), json!({"path":"missing"}).to_string())
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
        let (cancelled, started) = script.report();
        assert_eq!(cancelled.len(), 20);
        assert!(cancelled.iter().all(|call| call.tool == "read"));
        assert!(started.is_empty());
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
        assert_eq!(script.report().1, vec![handle]);
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
    async fn image_output_uses_detected_mime_and_enforces_count() {
        let fixture = Fixture::new();
        let mut bytes = std::io::Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(1, 1)
            .write_to(&mut bytes, image::ImageFormat::Png)
            .unwrap();
        let data = base64::engine::general_purpose::STANDARD.encode(bytes.into_inner());
        let spaced = format!(
            "DATA:IMAGE/JPEG;BASE64,\n  {}\r\n\t\u{feff}{}",
            &data[..8],
            &data[8..]
        );
        let task = fixture.start(
            &format!(
                "RegExp.prototype.exec = String.prototype.replace = () => {{ throw new Error('patched'); }}; image({});",
                json!(spaced)
            ),
            None,
        );
        complete(&task).await;
        assert_eq!(task.snapshot().exit_code, Some(0));
        let path = task.image_path(1, "image/png");
        assert!(path.is_file());
        let output = std::fs::read_to_string(&task.log_path).unwrap();
        assert!(output.contains("image/png, 1x1"), "{output}");
        for action in ["poll", "kill"] {
            let (status, log) = fixture.result(&format!(r#"
                const r = await tools.exec({{action: {action:?}, taskId: {id:?}}});
                if (r.codemode.images.length !== r.codemode.imagesAttached) throw Error("image count");
                const block = r.codemode.images[0];
                if (block.data !== {data:?} || block.mimeType !== "image/png") throw Error("image data");
                image(block);
                const next = await tools.exec({{action: "poll", taskId: r.taskId}});
                if (next.codemode?.images) throw Error("image delivered twice");
            "#, id = task.id)).await;
            assert_eq!(status.exit_code, Some(0), "{action}: {log}");
            assert!(log.contains("image/png, 1x1"), "{action}: {log}");
        }
        let (state, output) = fixture
            .result(&format!(
                "for(let i=0;i<65;i++) image({});",
                json!({"type":"image","data":data,"mimeType":"image/png"})
            ))
            .await;
        assert_eq!(state.exit_code, Some(1), "{output}");
        assert!(output.contains("RangeError"));
        assert!(output.contains("64 images"));
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
        // Create a small PNG and an oversized >10 MiB fake PNG header for note-only read.
        let img = image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(
            4,
            4,
            image::Rgb([1, 2, 3]),
        ));
        let mut png = Vec::new();
        img.write_to(&mut std::io::Cursor::new(&mut png), image::ImageFormat::Png)
            .unwrap();
        std::fs::write(fixture.dir.join("ok.png"), &png).unwrap();
        let mut huge = png[..32].to_vec();
        huge.resize(11 * 1024 * 1024, 0);
        std::fs::write(fixture.dir.join("huge.png"), &huge).unwrap();

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
                image(okImg);
                const bigImg = await tools.read({path: "huge.png"});
                if (bigImg.attached !== false || typeof bigImg.note !== "string" || !bigImg.note.includes("Image omitted")) {
                    throw new Error("bigImg shape: " + JSON.stringify(bigImg));
                }
                let noteThrown = false;
                try { image(bigImg); } catch (e) {
                    if (e instanceof TypeError && e.message === bigImg.note) noteThrown = true;
                }
                if (!noteThrown) throw new Error("expected image(bigImg) to throw bigImg.note");
                return "helpers-ok";
                "#,
            )
            .await;
        assert_eq!(state.exit_code, Some(0), "{output}");
        assert!(output.contains("helpers-ok"), "{output}");

        // End-to-end JS ToolError on injected captureError and logError: non-empty message
        // built from diagnostics and full report (output, exitCode, diagnostic) in err.result,
        // plus 1025th queued call (8 active + 1024 queued + 1 = 1033) rejecting with RangeError.
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
                    Array.from({{length: 1033}}, () => tools.read({{path: "ok.png"}}))
                );
                const rangeErrors = settled.filter(
                    s => s.status === "rejected" && s.reason instanceof RangeError
                );
                if (rangeErrors.length !== 1) {{
                    throw new Error("expected 1 RangeError at 1025th queued call, got " + rangeErrors.length);
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
        let parsed = nested_envelope(synth);
        assert_eq!(parsed["ok"], false);
        assert!(parsed["value"].get("ok").is_none());
        assert_eq!(parsed["value"]["output"], "captured-before-err");
        assert_eq!(parsed["value"]["exitCode"], 0);
        assert_eq!(
            parsed["value"]["inputError"],
            "INPUT_WRITE_FAILED: broken pipe"
        );
    }
}
