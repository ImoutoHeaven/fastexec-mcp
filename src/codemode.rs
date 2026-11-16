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
use std::time::Duration;
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
    /// Absolute working directory for nested exec (when it omits cwd) and relative paths in read, write, and edit. Omit for the session's default working directory.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    /// Wait up to this many ms for output or completion, 0-240000; default 30000.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wait_ms: Option<u64>,
    /// Return when output contains a marker or has been quiet for a while.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_when: Option<ReturnWhen>,
    /// Stop the script after this many ms; omitted or 0 means no deadline.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kill_after_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncate: Option<Truncate>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReturnWhen {
    pub output_contains: Option<Vec<String>>,
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

pub fn validate(request: &Request) -> Result<(), String> {
    if request.code.trim().is_empty() {
        return Err("code must contain JavaScript.".into());
    }
    if request.code.len() > MAX_CODE {
        return Err("code exceeds the 256 KiB limit.".into());
    }
    if request.code.contains('\0') {
        return Err("code must not contain NUL bytes; use a JavaScript \\0 escape.".into());
    }
    if let Some(dir) = &request.cwd {
        service::validate_cwd(dir)?;
    }
    service::validate(&request.as_exec()?).map(|_| ())
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
        let _ = idle.wait_timeout_while(active, Duration::from_secs(5), |&mut active| active > 0);
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
            && service::validate(request).is_ok()
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
            output_mode: OutputMode::Both,
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
                                    files::read(request, launch, http, nested_render).await
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
            let mut value =
                json!({ "content": result.content, "isError": result.is_error.unwrap_or(false) });
            if let Some(structured) = result.structured_content {
                value["structuredContent"] = structured;
            }
            let mut outcome = match serde_json::to_string(&value) {
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
    if let Some(exception) = value.as_exception() {
        let name = exception
            .as_object()
            .get::<_, String>("name")
            .unwrap_or_else(|_| "Error".into());
        let message = exception.message().unwrap_or_default();
        let stack = exception.stack().unwrap_or_default();
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
        use rquickjs::FromJs;
        rquickjs::Coerced::<String>::from_js(ctx, value)
            .map(|value| value.0)
            .unwrap_or_else(|_| "The script threw a value that cannot be described.".into())
    }
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
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(data)
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
                    path.display(),
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
                .eval_with_options(format!("(async (tools, console) => {{{code}\n}})"), options)
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
    const isArray = Array.isArray;
    const pending = new Map();
    let waiting = 0;
    let state = 0;
    let failure;

    function text(value) {
        const json = typeof value === "string" ? value : stringify(value);
        __write(json === undefined ? "undefined" : json);
    }
    function format(value) {
        if (value instanceof ErrorCtor) {
            const head = StringCtor(value.name) + ": " + StringCtor(value.message);
            return value.stack ? head + "\n" + StringCtor(value.stack) : head;
        }
        if (typeof value === "string" || typeof value === "number") return StringCtor(value);
        try {
            const json = stringify(value);
            return json === undefined ? StringCtor(value) : json;
        } catch { return StringCtor(value); }
    }
    const console = {};
    for (const level of ["log", "info", "warn", "error", "debug"]) {
        console[level] = (...args) => {
            let line = "";
            for (let i = 0; i < args.length; i++) line += (i ? " " : "") + format(args[i]);
            __write(line);
        };
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
                set(pending, id, { resolve, reject });
                waiting++;
            } catch (error) { reject(error); }
        });
    }
    const tools = new Proxy(members, {
        get(object, name) {
            if (typeof name !== "string") return undefined;
            if (name === "exec" || name === "read" || name === "write" || name === "edit") return object[name];
            throw new TypeErrorCtor("tools." + name + " does not exist. Available: exec, read, write, edit.");
        }
    });
    function image(value) {
        if (value && typeof value === "object" && isArray(value.content)) {
            let found;
            for (let i = 0; i < value.content.length; i++) {
                if (value.content[i] && value.content[i].type === "image") { found = value.content[i]; break; }
            }
            value = found;
        }
        let data;
        if (value && typeof value === "object" && value.type === "image" && typeof value.data === "string") {
            data = value.data;
        } else if (typeof value === "string") {
            const match = /^data:[^,]*;base64,([\s\S]*)$/i.exec(value);
            if (match) data = match[1];
        }
        if (typeof data !== "string") throw new TypeErrorCtor("image expects an image block, a base64 data URL, or a tool result containing an image block.");
        __image(data.replace(/\s+/g, ""));
    }
    function exit() {
        __exit();
        // QuickJS's interrupt is uncatchable, including inside a script's try/catch.
        while (true) {}
    }
    globalThis.tools = tools;
    globalThis.console = console;
    globalThis.text = text;
    globalThis.image = image;
    globalThis.exit = exit;
    return {
        run(fn) {
            then(fn(tools, console), value => {
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
            try { entry.resolve(parse(payload)); } catch (error) { entry.reject(error); }
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
        for code in ["", " \n\t", "text('\0')"] {
            let request: Request = serde_json::from_value(json!({"code": code})).unwrap();
            assert!(validate(&request).is_err());
        }
        let request: Request =
            serde_json::from_value(json!({"code": "x".repeat(MAX_CODE + 1)})).unwrap();
        assert!(validate(&request).unwrap_err().contains("256 KiB"));
        for extra in [
            json!({"cwd":"relative/dir"}),
            json!({"pty":true}),
            json!({"waitMs":240001}),
            json!({"returnWhen":{"screenContains":["x"]}}),
        ] {
            let mut value = json!({"code":"return 1"});
            value
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            assert!(
                serde_json::from_value::<Request>(value).map_or(true, |r| validate(&r).is_err())
            );
        }
        let dir = std::env::temp_dir();
        let request: Request = serde_json::from_value(
            json!({"code":"return '\\0'", "cwd": dir, "waitMs":0, "killAfterMs":0}),
        )
        .unwrap();
        assert!(validate(&request).is_ok());
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
                "text('before'); try { exit(); } catch(e) { text('caught'); } text('after');",
                "before\n",
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
    }

    #[tokio::test]
    async fn file_tools_resolve_errors_reject_bad_arguments_and_queue() {
        let fixture = Fixture::new();
        let code = r#"
            const write = await tools.write({path:'fixture.txt',content:'old\n'});
            if (write.isError !== false || write.structuredContent.ok !== true || write.structuredContent.bytes !== 4) throw new Error('write shape');
            const edit = await tools.edit({path:'fixture.txt',edits:[{oldText:'old',newText:'new'}]});
            if (edit.isError || edit.structuredContent.ok !== true || edit.structuredContent.edits[0].line !== 1) throw new Error('edit failed');
            const missing = await tools.read({path:'missing.txt'});
            if (missing.isError !== true || missing.structuredContent.ok !== false) throw new Error('missing file must resolve');
            const badExec = await tools.exec({action:'start',command:''});
            if (badExec.isError !== true || badExec.structuredContent.ok !== false || badExec.structuredContent.action !== 'start') throw new Error('empty exec must resolve as error');
            for (const args of [null, [], 'x', {path:'fixture.txt',surprise:true}]) {
                let rejected = false;
                try { await tools.read(args); } catch (e) { rejected = true; }
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
            if (results.some(r => r.isError || r.structuredContent.text !== 'new\n' || r.structuredContent.truncated !== false)) throw new Error('queued read failed');
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
            text({ok: !result.isError});
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
        let task = fixture.start(
            &format!(
                "image({});",
                json!(format!("data:image/jpeg;base64,{data}"))
            ),
            None,
        );
        complete(&task).await;
        assert_eq!(task.snapshot().exit_code, Some(0));
        let path = task.image_path(1, "image/png");
        assert!(path.is_file());
        let output = std::fs::read_to_string(&task.log_path).unwrap();
        assert!(output.contains("image/png, 1x1"), "{output}");
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
}
