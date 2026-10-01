//! fastexec: an MCP stdio server with one tool that runs bash commands as tasks.

mod bash;
mod output;
mod process;
mod tasks;

use output::{Truncate, Window};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CallToolResult, ContentBlock, Implementation, ProgressNotificationParam, ServerCapabilities,
    ServerInfo,
};
use rmcp::service::RequestContext;
use rmcp::{RoleServer, ServerHandler, ServiceExt, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tasks::{Snapshot, StartArgs, Task, Tasks};

const MAX_WAIT_MS: u64 = 240_000;
const PROGRESS_EVERY: Duration = Duration::from_secs(20);
const KILL_WAIT: Duration = Duration::from_secs(5);
const MAX_INPUT_BYTES: usize = 16 * 1024;
const DEFAULT_MAX_BYTES: u64 = 16 * 1024;
const MAX_BYTES_RANGE: std::ops::RangeInclusive<u64> = 1024..=1024 * 1024;
/// Room in `maxBytes` for the omission marker, the encoding note, and separators.
const NOTES_RESERVE: usize = 260;
/// Smallest window body, so a long status line still leaves room for some output.
const MIN_BODY_BUDGET: usize = 256;

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Action {
    Start,
    Poll,
    Kill,
    List,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Request {
    /// start: run a command. poll: send input and/or wait, then read unseen output. kill: terminate a task's process tree. list: this server's tasks.
    action: Action,
    /// start: the bash command line.
    command: Option<String>,
    /// start: absolute working directory. Omit for the server's working directory.
    cwd: Option<String>,
    /// start: run inside a pseudo-terminal (default false).
    pty: Option<bool>,
    /// start: login shell, bash -lc, so profile-managed tools resolve (default true). false runs bash --noprofile --norc -c.
    login_shell: Option<bool>,
    /// poll, kill: the task to act on.
    task_id: Option<String>,
    /// poll: text written to the task before waiting, at most 16 KiB.
    input: Option<String>,
    /// poll: close stdin after input (pipe mode only).
    eof: Option<bool>,
    /// start, poll: how long to wait, 0-240000 ms.
    #[schemars(range(min = 0, max = 240000))]
    wait_ms: Option<u64>,
    /// start, poll: output window mode (default head_tail).
    truncate: Option<Truncate>,
    /// start, poll: byte budget of the result, 1024-1048576 (default 16384).
    #[schemars(range(min = 1024, max = 1048576))]
    max_bytes: Option<u64>,
    /// start, poll: return output without cleaning (default false).
    raw: Option<bool>,
    /// start, poll: WHATWG label of the output encoding, e.g. "big5" or "gbk". On start it becomes the task default.
    encoding: Option<String>,
}

/// Output options shared by start and poll.
struct View {
    truncate: Truncate,
    max_bytes: usize,
    raw: bool,
    encoding: Option<&'static encoding_rs::Encoding>,
}

fn validate(request: &Request) -> Result<View, String> {
    let present = [
        ("command", request.command.is_some()),
        ("cwd", request.cwd.is_some()),
        ("pty", request.pty.is_some()),
        ("loginShell", request.login_shell.is_some()),
        ("taskId", request.task_id.is_some()),
        ("input", request.input.is_some()),
        ("eof", request.eof.is_some()),
        ("waitMs", request.wait_ms.is_some()),
        ("truncate", request.truncate.is_some()),
        ("maxBytes", request.max_bytes.is_some()),
        ("raw", request.raw.is_some()),
        ("encoding", request.encoding.is_some()),
    ];
    const VIEW: [&str; 5] = ["waitMs", "truncate", "maxBytes", "raw", "encoding"];
    let allowed: Vec<&str> = match request.action {
        Action::Start => ["command", "cwd", "pty", "loginShell"]
            .into_iter()
            .chain(VIEW)
            .collect(),
        Action::Poll => ["taskId", "input", "eof"].into_iter().chain(VIEW).collect(),
        Action::Kill => vec!["taskId"],
        Action::List => vec![],
    };
    for (name, set) in present {
        if set && !allowed.contains(&name) {
            return Err(format!(
                "`{name}` does not apply to action {:?}; remove it.",
                request.action
            ));
        }
    }
    match request.action {
        Action::Start
            if request
                .command
                .as_deref()
                .is_none_or(|c| c.trim().is_empty()) =>
        {
            return Err("start needs a non-empty `command`.".into());
        }
        Action::Poll | Action::Kill if request.task_id.is_none() => {
            return Err(format!(
                "{:?} needs `taskId`; use action list to find it.",
                request.action
            ));
        }
        _ => {}
    }
    if let Some(wait) = request.wait_ms.filter(|&wait| wait > MAX_WAIT_MS) {
        return Err(format!("waitMs {wait} is out of range 0-{MAX_WAIT_MS}."));
    }
    if let Some(bytes) = request.max_bytes {
        if !MAX_BYTES_RANGE.contains(&bytes) {
            return Err(format!("maxBytes {bytes} is out of range 1024-1048576."));
        }
        if request.truncate == Some(Truncate::None) {
            return Err(
                "maxBytes does not apply with truncate \"none\"; remove one of them.".into(),
            );
        }
    }
    if request
        .input
        .as_ref()
        .is_some_and(|input| input.len() > MAX_INPUT_BYTES)
    {
        return Err(
            "input exceeds 16 KiB; send it in smaller pieces or write it to a file.".into(),
        );
    }
    let encoding = match request.encoding.as_deref() {
        None => None,
        Some(label) => Some(encoding_rs::Encoding::for_label(label.trim().as_bytes())
            .filter(|encoding| encoding.is_ascii_compatible())
            .ok_or_else(|| {
            format!("Unknown or unsupported encoding {label:?}; use an ASCII-compatible WHATWG label such as \"utf-8\", \"big5\", or \"gbk\" (UTF-16 output is not supported).")
        })?),
    };
    Ok(View {
        truncate: request.truncate.unwrap_or_default(),
        max_bytes: request.max_bytes.unwrap_or(DEFAULT_MAX_BYTES) as usize,
        raw: request.raw.unwrap_or(false),
        encoding,
    })
}

#[derive(Clone)]
struct Server {
    tasks: Arc<Tasks>,
}

/// Server instructions say when to use the tool; hosts that show one line take the first.
const INSTRUCTIONS: &str = "Runs bash commands as tasks: long-running builds and servers, stdin-driven programs, and terminal (PTY) programs such as password prompts, with bounded output and process-tree kill.
Prefer it over one-shot shell tools when a command may outlive a single call or needs input; tasks end when the session ends.";

#[tool_handler]
impl ServerHandler for Server {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("fastexec", env!("CARGO_PKG_VERSION")))
            .with_instructions(INSTRUCTIONS)
    }
}

#[tool_router]
impl Server {
    #[doc = include_str!("description.md")]
    #[tool(
        name = "fastexec",
        annotations(
            title = "Run bash command",
            read_only_hint = false,
            destructive_hint = true,
            open_world_hint = true
        )
    )]
    async fn fastexec(
        &self,
        Parameters(request): Parameters<Request>,
        context: RequestContext<RoleServer>,
    ) -> CallToolResult {
        let action = request.action;
        match self.handle(request, &context).await {
            Ok(result) => result,
            Err(message) => error_result(action, &message),
        }
    }
}

impl Server {
    async fn handle(
        &self,
        request: Request,
        context: &RequestContext<RoleServer>,
    ) -> Result<CallToolResult, String> {
        let view = validate(&request)?;
        match request.action {
            Action::List => Ok(self.list()),
            Action::Start => {
                let cwd = match request.cwd {
                    None => std::env::current_dir()
                        .map_err(|error| format!("Cannot read the working directory: {error}."))?,
                    Some(dir) => {
                        let dir = PathBuf::from(dir);
                        if !dir.is_absolute() || !dir.is_dir() {
                            return Err(format!(
                                "cwd {} is not an existing absolute directory.",
                                dir.display()
                            ));
                        }
                        dir
                    }
                };
                let args = StartArgs {
                    command: request.command.unwrap_or_default(),
                    cwd,
                    pty: request.pty.unwrap_or(false),
                    login: request.login_shell.unwrap_or(true),
                    encoding: view.encoding.unwrap_or(encoding_rs::UTF_8),
                };
                let tasks = Arc::clone(&self.tasks);
                let task = tokio::task::spawn_blocking(move || tasks.start(args))
                    .await
                    .map_err(|error| format!("Internal failure while starting: {error}."))??;
                let wait = request.wait_ms.unwrap_or(30_000);
                wait_for(&task, wait, context).await?;
                window_result(Action::Start, task, view).await
            }
            Action::Poll => {
                let task = self.task(request.task_id.as_deref())?;
                task.send(request.input.as_deref(), request.eof.unwrap_or(false))?;
                let default_wait = if request.input.is_some() {
                    2_000
                } else {
                    30_000
                };
                wait_for(&task, request.wait_ms.unwrap_or(default_wait), context).await?;
                window_result(Action::Poll, task, view).await
            }
            Action::Kill => {
                let task = self.task(request.task_id.as_deref())?;
                task.kill();
                let mut done = task.done();
                let finished = async move {
                    let _ = done.wait_for(|done| *done).await;
                };
                let _ = tokio::time::timeout(KILL_WAIT, finished).await;
                let snapshot = task.snapshot();
                let mut text = status_line(&task, &snapshot);
                if snapshot.state == "running" {
                    text.push_str(" · the tree did not exit within 5 s; poll later to confirm");
                }
                Ok(success(text, task_json(Action::Kill, &task, &snapshot)))
            }
        }
    }

    fn task(&self, id: Option<&str>) -> Result<Arc<Task>, String> {
        let id = id.unwrap_or_default();
        self.tasks
            .find(id)
            .ok_or_else(|| format!("Unknown taskId {id:?}. Tasks live as long as this server; use action list to see them."))
    }

    fn list(&self) -> CallToolResult {
        let tasks = self.tasks.all();
        let mut lines = Vec::new();
        let mut entries = Vec::new();
        for task in &tasks {
            let snapshot = task.snapshot();
            let command: String = task.command.chars().take(120).collect();
            let code = snapshot
                .exit_code
                .map_or(String::new(), |code| format!(" {code}"));
            lines.push(format!(
                "{} [{}{code}] {:.1}s{} · {}",
                task.id,
                snapshot.state,
                snapshot.elapsed.as_secs_f64(),
                if task.pty { " pty" } else { "" },
                command.replace('\n', " ")
            ));
            entries.push(json!({
                "taskId": task.id, "state": snapshot.state, "exitCode": snapshot.exit_code, "pty": task.pty,
                "elapsedMs": snapshot.elapsed.as_millis() as u64, "command": command, "logPath": task.log_path,
            }));
        }
        let text = if lines.is_empty() {
            "No tasks.".to_string()
        } else {
            lines.join("\n")
        };
        success(
            text,
            json!({ "ok": true, "action": "list", "tasks": entries }),
        )
    }
}

/// Waits until the task ends, `wait_ms` elapses, or the call is cancelled, sending progress.
async fn wait_for(
    task: &Task,
    wait_ms: u64,
    context: &RequestContext<RoleServer>,
) -> Result<(), String> {
    let started = Instant::now();
    let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
    let token = context.meta.get_progress_token();
    let mut ticks =
        tokio::time::interval_at(tokio::time::Instant::now() + PROGRESS_EVERY, PROGRESS_EVERY);
    let mut done = task.done();
    let finished = async move {
        let _ = done.wait_for(|done| *done).await;
    };
    tokio::pin!(finished);
    loop {
        tokio::select! {
            _ = &mut finished => return Ok(()),
            _ = tokio::time::sleep_until(deadline) => return Ok(()),
            _ = context.ct.cancelled() => return Err("Cancelled; the task keeps running.".into()),
            _ = ticks.tick(), if token.is_some() => {
                let seconds = started.elapsed().as_secs_f64();
                let message = format!("{} running, waited {seconds:.0}s of {}s", task.id, wait_ms / 1000);
                let progress = ProgressNotificationParam::new(token.clone().expect("guarded"), seconds).with_message(message);
                let _ = context.peer.notify_progress(progress).await;
            }
        }
    }
}

async fn window_result(
    action: Action,
    task: Arc<Task>,
    view: View,
) -> Result<CallToolResult, String> {
    let reader = Arc::clone(&task);
    let encoding = view.encoding.unwrap_or(task.encoding);
    // The status line can only grow by its notes while the window is read.
    let reserved = status_line(&task, &task.snapshot()).len() + NOTES_RESERVE;
    let budget = match view.truncate {
        Truncate::None => usize::MAX,
        _ => view.max_bytes.saturating_sub(reserved).max(MIN_BODY_BUDGET),
    };
    let window = tokio::task::spawn_blocking(move || {
        reader.read_window(view.truncate, budget, view.raw, encoding)
    })
    .await
    .map_err(|error| format!("Internal failure while reading output: {error}."))?
    .map_err(|error| format!("Cannot read the task log: {error}."))?;
    let snapshot = task.snapshot();
    let mut status = status_line(&task, &snapshot);
    if window.bad_lines > 0 {
        status.push_str(&format!(
            " · {} lines had bytes invalid in {}; pass encoding (e.g. big5, gbk)",
            window.bad_lines,
            encoding.name()
        ));
    }
    let mut output = window.text.clone();
    if view.truncate != Truncate::None {
        // Hard guarantee: the whole text fits maxBytes. A status line longer than half the
        // budget is cut too; structuredContent.logPath keeps the full path.
        output::cut_to(&mut status, view.max_bytes / 2 + 1);
        output::cut_to(&mut output, view.max_bytes - status.len() - 2 + 1);
    }
    let body = if output.is_empty() {
        "(no new output)"
    } else {
        output.as_str()
    };
    let text = format!("{body}\n\n{status}");
    let mut structured = task_json(action, &task, &snapshot);
    add_window(&mut structured, &window);
    structured["output"] = json!(output);
    Ok(success(text, structured))
}

fn status_line(task: &Task, snapshot: &Snapshot) -> String {
    let state = match snapshot.exit_code {
        Some(code) if snapshot.state == "exited" => format!("exited {code}"),
        Some(code) => format!("{} (exit {code})", snapshot.state),
        None => snapshot.state.to_string(),
    };
    let mut line = format!(
        "[{state}] {} · {:.1}s · {} lines · log {}",
        task.id,
        snapshot.elapsed.as_secs_f64(),
        snapshot.lines,
        task.log_path.display()
    );
    if snapshot.state == "running" {
        line.push_str(" · poll to continue, kill to stop");
    }
    if snapshot.dropped > 0 {
        line.push_str(&format!(
            " · {} bytes past the log limit were not stored",
            snapshot.dropped
        ));
    }
    if snapshot.evicted {
        line.push_str(" · log evicted under the 1 GiB total limit");
    }
    if snapshot.output_open {
        line.push_str(" · output stayed open 2 s after exit; later output may follow");
    }
    line
}

fn task_json(action: Action, task: &Task, snapshot: &Snapshot) -> Value {
    json!({
        "ok": true,
        "action": action_name(action),
        "taskId": task.id,
        "state": snapshot.state,
        "exitCode": snapshot.exit_code,
        "pty": task.pty,
        "elapsedMs": snapshot.elapsed.as_millis() as u64,
        "logPath": task.log_path,
    })
}

fn add_window(value: &mut Value, window: &Window) {
    let omitted = window.omitted.map_or(0, |(first, last)| last - first + 1);
    value["output"] = json!(window.text);
    value["omittedLines"] = json!(omitted);
    value["omittedRange"] = json!(window.omitted.map(|(first, last)| [first, last]));
    value["encodingErrors"] = json!(window.bad_lines);
}

fn action_name(action: Action) -> &'static str {
    match action {
        Action::Start => "start",
        Action::Poll => "poll",
        Action::Kill => "kill",
        Action::List => "list",
    }
}

fn success(text: String, structured: Value) -> CallToolResult {
    let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
    result.structured_content = Some(structured);
    result
}

fn error_result(action: Action, message: &str) -> CallToolResult {
    let mut result = CallToolResult::error(vec![ContentBlock::text(message)]);
    result.structured_content =
        Some(json!({ "ok": false, "action": action_name(action), "error": message }));
    result
}

#[tokio::main]
async fn main() {
    let tasks = match Tasks::new() {
        Ok(tasks) => tasks,
        Err(error) => {
            eprintln!("fastexec: cannot create the log directory: {error}");
            std::process::exit(1);
        }
    };
    let server = Server {
        tasks: Arc::clone(&tasks),
    };
    let eof = Arc::new(tokio::sync::Notify::new());
    let stdin = EofSignal {
        inner: tokio::io::stdin(),
        eof: Arc::clone(&eof),
    };
    match server.serve((stdin, tokio::io::stdout())).await {
        Ok(service) => {
            // Stdin EOF ends the session at once; rmcp would first drain in-flight waits.
            tokio::select! {
                _ = service.waiting() => {}
                _ = eof.notified() => {}
                _ = shutdown_signal() => {}
            }
        }
        Err(error) => eprintln!("fastexec: MCP initialization failed: {error}"),
    }
    tasks.shutdown();
    std::process::exit(0);
}

/// Stdin wrapper that reports EOF: the host closing stdin is the MCP stdio shutdown signal.
struct EofSignal<R> {
    inner: R,
    eof: Arc<tokio::sync::Notify>,
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for EofSignal<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(result, std::task::Poll::Ready(Ok(()))) && buf.filled().len() == before {
            self.eof.notify_one();
        }
        result
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut hangup = signal(SignalKind::hangup()).expect("SIGHUP handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = hangup.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(windows)]
    let _ = tokio::signal::ctrl_c().await;
}
