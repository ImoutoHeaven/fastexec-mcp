//! fastexec: an MCP stdio server with one tool that runs bash commands as tasks.

mod bash;
mod conpty;
mod frames;
mod keys;
mod output;
mod process;
mod tasks;
mod terminal;
mod wait;

use output::{Cleaner, Truncate, Window, WindowBuilder, WindowSpec};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock, Implementation, ServerCapabilities, ServerInfo};
use rmcp::service::RequestContext;
use rmcp::{RoleServer, ServerHandler, ServiceExt, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tasks::{Snapshot, StartArgs, Task, Tasks};
use wait::WaitEnd;

const MAX_WAIT_MS: u64 = 240_000;
const KILL_WAIT: Duration = Duration::from_secs(5);
const MAX_INPUT_BYTES: usize = 16 * 1024;
const DEFAULT_KEY_DELAY_MS: u64 = 150;
const MAX_KEY_DELAY_MS: u64 = 2_000;
/// Longest pause the keys of one poll add up to before its wait begins.
const MAX_KEY_PAUSE_MS: u64 = 30_000;
/// Longest a key waits for the program's stdin to accept the input before it.
const INPUT_ACCEPT_LIMIT: Duration = Duration::from_secs(5);
const DEFAULT_MAX_BYTES: u64 = 16 * 1024;
const MAX_BYTES_RANGE: std::ops::RangeInclusive<u64> = 1024..=1024 * 1024;
/// Room in `maxBytes` for the omission marker, the encoding note, and separators.
const NOTES_RESERVE: usize = 260;
/// Smallest window body, so a long status line still leaves room for some output.
const MIN_BODY_BUDGET: usize = 256;
/// Largest background footer; bounded results give it at most a quarter of `maxBytes`.
const FOOTER_LIMIT: usize = 512;

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Action {
    Start,
    Poll,
    Kill,
    List,
    Transcript,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Request {
    /// start: run a command. poll: send input and/or wait, then read unseen output. kill: terminate a task's process tree. list: this server's tasks. transcript: a PTY task's whole output rendered as a terminal shows it, scrollback history then screen.
    action: Action,
    /// start: the bash command line.
    command: Option<String>,
    /// start: absolute working directory. Omit for the server's working directory.
    cwd: Option<String>,
    /// start: run inside a pseudo-terminal (default false).
    pty: Option<bool>,
    /// start: login shell, bash -lc, so profile-managed tools resolve (default true). false runs bash --noprofile --norc -c.
    login_shell: Option<bool>,
    /// start: kill the task's whole process tree this many ms after launch, whatever the waits and polls; 0 or omitted means no limit.
    kill_after_ms: Option<u64>,
    /// poll, kill, transcript: the task to act on.
    task_id: Option<String>,
    /// poll: text written to the task exactly as given, before `keys` and before waiting, at most 16 KiB.
    input: Option<String>,
    /// poll, PTY only: keys pressed after `input` is written, in array order, e.g. ["Enter"] or ["Escape", ":", "q", "Enter"]. One key per item, named as in tmux send-keys: Enter, Tab, BTab, Escape, Space, BSpace, Up, Down, Left, Right, Home, End, PageUp/PgUp/PPage, PageDown/PgDn/NPage, Insert/IC, Delete/DC, F1-F12, KP0-KP9, KP/, KP*, KP-, KP+, KP., KPEnter, [NUL]-[US] for C0 controls, 0xHH for a code point, or one character; prefixes C- (Ctrl), M- (Meta/Alt), S- (Shift) combine, as in C-c, M-x, C-M-a, S-Up, and ^c means C-c. Names ignore case. Arrow and keypad keys follow the program's cursor and keypad modes. An unknown name, or a modifier the key cannot carry (C-Enter, S-a) in the program's mode when the poll starts, is an error before anything is sent.
    keys: Option<Vec<String>>,
    /// poll, with keys: pause before each key, the first one after input included, 0-2000 ms (default 150); 0 writes input and keys at once. Programs that time keystrokes to detect pastes, such as Codex, read a key sent right after text as part of the paste; programs with an escape timeout read Escape and the next key as one Alt key unless the pause exceeds it (often 500).
    #[schemars(range(min = 0, max = 2000))]
    key_delay_ms: Option<u64>,
    /// poll: close stdin after input (pipe mode only).
    eof: Option<bool>,
    /// start, poll: how long to wait at most, 0-240000 ms.
    #[schemars(range(min = 0, max = 240000))]
    wait_ms: Option<u64>,
    /// start, poll: end the wait early when any condition holds; the task keeps running. Texts are literal and case-sensitive, 1-16 per condition, each 1-4096 bytes without line breaks. waitMs stays the longest wait and defaults to 30000 with returnWhen.
    return_when: Option<wait::ReturnWhen>,
    /// start, poll, transcript: output window mode (default head_tail; transcript defaults to tail).
    truncate: Option<Truncate>,
    /// start, poll, transcript: byte budget of the result, 1024-1048576 (default 16384).
    #[schemars(range(min = 1024, max = 1048576))]
    max_bytes: Option<u64>,
    /// start, poll: return output without cleaning (default false).
    raw: Option<bool>,
    /// start, poll: WHATWG label of the output encoding, e.g. "big5" or "gbk". On start it becomes the task default.
    encoding: Option<String>,
    /// start, poll, PTY only: return the rendered terminal screen (120x30) and cursor position instead of the output stream; for full-screen and TUI programs. Marks the stream read. Not with truncate, raw, or encoding.
    screen: Option<bool>,
}

/// Output options shared by start and poll.
struct View {
    truncate: Truncate,
    max_bytes: usize,
    raw: bool,
    encoding: Option<&'static encoding_rs::Encoding>,
    screen: bool,
    /// The call set `returnWhen`.
    conditions: bool,
}

fn validate(request: &Request) -> Result<View, String> {
    let present = [
        ("command", request.command.is_some()),
        ("cwd", request.cwd.is_some()),
        ("pty", request.pty.is_some()),
        ("loginShell", request.login_shell.is_some()),
        ("killAfterMs", request.kill_after_ms.is_some()),
        ("taskId", request.task_id.is_some()),
        ("input", request.input.is_some()),
        ("eof", request.eof.is_some()),
        ("waitMs", request.wait_ms.is_some()),
        ("truncate", request.truncate.is_some()),
        ("maxBytes", request.max_bytes.is_some()),
        ("raw", request.raw.is_some()),
        ("encoding", request.encoding.is_some()),
        ("keys", request.keys.is_some()),
        ("keyDelayMs", request.key_delay_ms.is_some()),
        ("screen", request.screen.is_some()),
        ("returnWhen", request.return_when.is_some()),
    ];
    const VIEW: [&str; 7] = [
        "waitMs",
        "returnWhen",
        "truncate",
        "maxBytes",
        "raw",
        "encoding",
        "screen",
    ];
    let allowed: Vec<&str> = match request.action {
        Action::Start => ["command", "cwd", "pty", "loginShell", "killAfterMs"]
            .into_iter()
            .chain(VIEW)
            .collect(),
        Action::Poll => ["taskId", "input", "eof", "keys", "keyDelayMs"]
            .into_iter()
            .chain(VIEW)
            .collect(),
        Action::Kill => vec!["taskId"],
        Action::List => vec![],
        Action::Transcript => vec!["taskId", "truncate", "maxBytes"],
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
        Action::Poll | Action::Kill | Action::Transcript if request.task_id.is_none() => {
            return Err(format!(
                "{:?} needs `taskId`; use action list to find it.",
                request.action
            ));
        }
        _ => {}
    }
    let screen = request.screen.unwrap_or(false);
    if screen {
        if let Some(name) = [
            ("truncate", request.truncate.is_some()),
            ("raw", request.raw.is_some()),
            ("encoding", request.encoding.is_some()),
        ]
        .into_iter()
        .find_map(|(name, set)| set.then_some(name))
        {
            return Err(format!(
                "`{name}` does not apply with screen; remove one of them."
            ));
        }
        if request.action == Action::Start && request.pty != Some(true) {
            return Err("screen needs a PTY task; start with pty: true.".into());
        }
    }
    if let Some(when) = &request.return_when {
        when.validate()?;
        if let Some(name) = when.screen_condition()
            && request.action == Action::Start
            && request.pty != Some(true)
        {
            return Err(format!("{name} needs a PTY task; start with pty: true."));
        }
    }
    if let Some(delay) = request.key_delay_ms {
        if request.keys.is_none() {
            return Err("keyDelayMs applies only with keys; remove it.".into());
        }
        if delay > MAX_KEY_DELAY_MS {
            return Err(format!(
                "keyDelayMs {delay} is out of range 0-{MAX_KEY_DELAY_MS}."
            ));
        }
    }
    if let Some(keys) = &request.keys {
        let delay = request.key_delay_ms.unwrap_or(DEFAULT_KEY_DELAY_MS);
        let pause = keys.len() as u64 * delay;
        if pause > MAX_KEY_PAUSE_MS {
            return Err(format!(
                "The keys would pause {pause} ms in all at keyDelayMs {delay}, over the {MAX_KEY_PAUSE_MS} ms limit; send fewer keys per poll or lower keyDelayMs."
            ));
        }
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
    if request.action == Action::Start
        && let Some(name) = request
            .return_when
            .as_ref()
            .and_then(|when| when.screen_condition())
        && encoding.is_some_and(|encoding| encoding != encoding_rs::UTF_8)
    {
        return Err(format!("{name} needs UTF-8 output; remove encoding."));
    }
    Ok(View {
        truncate: request
            .truncate
            .unwrap_or(if request.action == Action::Transcript {
                Truncate::Tail
            } else {
                Truncate::HeadTail
            }),
        max_bytes: request.max_bytes.unwrap_or(DEFAULT_MAX_BYTES) as usize,
        raw: request.raw.unwrap_or(false),
        encoding,
        screen,
        conditions: request.return_when.is_some(),
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OutputMode {
    Text,
    Both,
    Structured,
}

#[derive(Clone)]
struct Server {
    tasks: Arc<Tasks>,
    output_mode: OutputMode,
    structured_content: bool,
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
        let mut result = match self.handle(request, &context).await {
            Ok(result) => result,
            Err(message) => error_result(action, &message),
        };
        if !self.structured_content {
            result.structured_content = None;
        }
        result
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
                if let Some(limit) = request.kill_after_ms.filter(|&limit| limit > 0) {
                    // The deadline belongs to the task, not to this request: it survives the
                    // request's cancellation and every later wait.
                    let (expiring, mut done) = (Arc::clone(&task), task.done());
                    tokio::spawn(async move {
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_millis(limit)) => expiring.expire(),
                            _ = done.wait_for(|done| *done) => {}
                        }
                    });
                }
                let when = request.return_when.as_ref();
                let mark = wait::mark(&task, when)?;
                let encoding = view.encoding.unwrap_or(task.encoding);
                let wait = request.wait_ms.unwrap_or(30_000);
                let ended = wait::wait(&task, wait, when, mark, encoding, context).await?;
                self.window_result(Action::Start, task, view, ended).await
            }
            Action::Poll => {
                let task = self.task(request.task_id.as_deref())?;
                let when = request.return_when.as_ref();
                let watches_screen = when.and_then(|when| when.screen_condition());
                let needs_pty = [("screen", view.screen), ("keys", request.keys.is_some())]
                    .into_iter()
                    .find_map(|(name, set)| set.then_some(name))
                    .or(watches_screen);
                if let Some(name) = needs_pty.filter(|_| !task.pty) {
                    return Err(format!(
                        "{name} applies to PTY tasks only; in pipe mode, write text with input and end lines with a newline."
                    ));
                }
                if let Some(name) = view.screen.then_some("screen").or(watches_screen)
                    && task.encoding != encoding_rs::UTF_8
                {
                    return Err(format!(
                        "{name} needs UTF-8 output; this task was started with an encoding."
                    ));
                }
                let input = request.input.as_deref().unwrap_or_default().as_bytes();
                let names = request.keys.as_deref().unwrap_or_default();
                // Every key is checked against the current modes before anything is written.
                let mut parsed = Vec::with_capacity(names.len());
                let mut data = input.to_vec();
                if !names.is_empty() {
                    let modes = task.modes()?;
                    for name in names {
                        let key = keys::parse(name)?;
                        data.extend(keys::encode(name, key, modes)?);
                        parsed.push(key);
                    }
                }
                if data.len() > MAX_INPUT_BYTES {
                    return Err(
                        "input and keys together exceed 16 KiB; send them in smaller pieces."
                            .into(),
                    );
                }
                let mark = wait::mark(&task, when)?;
                let delay =
                    Duration::from_millis(request.key_delay_ms.unwrap_or(DEFAULT_KEY_DELAY_MS));
                if delay.is_zero() {
                    // Without a pause, input and keys go out as one write, like a typed burst.
                    task.send(Some(&data), request.eof.unwrap_or(false))?;
                } else {
                    task.send(Some(input), request.eof.unwrap_or(false))?;
                    press(&task, input, names, parsed, delay, context).await?;
                }
                let default_wait =
                    if (request.input.is_some() || request.keys.is_some()) && when.is_none() {
                        2_000
                    } else {
                        30_000
                    };
                let wait = request.wait_ms.unwrap_or(default_wait);
                let encoding = view.encoding.unwrap_or(task.encoding);
                let ended = wait::wait(&task, wait, when, mark, encoding, context).await?;
                self.window_result(Action::Poll, task, view, ended).await
            }
            Action::Kill => {
                let task = self.task(request.task_id.as_deref())?;
                task.kill().map_err(|error| {
                    format!(
                        "Cannot terminate the process tree of {}: {error}. Poll to see its state, or retry kill.",
                        task.id
                    )
                })?;
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
                let background = self.tasks.background(&task.id, FOOTER_LIMIT);
                if let Some((line, _)) = &background {
                    text.push('\n');
                    text.push_str(line);
                }
                let result = success(text, task_json(Action::Kill, &task, &snapshot));
                report(&task, &snapshot, background);
                Ok(result)
            }
            Action::Transcript => {
                let task = self.task(request.task_id.as_deref())?;
                if !task.pty {
                    return Err("transcript applies to PTY tasks only; poll returns a pipe task's output, and its log holds every byte.".into());
                }
                if task.encoding != encoding_rs::UTF_8 {
                    return Err(
                        "transcript needs UTF-8 output; this task was started with an encoding."
                            .into(),
                    );
                }
                self.transcript_result(task, view).await
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
        let mut shown_final = Vec::new();
        for task in &tasks {
            let snapshot = task.snapshot();
            if snapshot.state != "running" {
                shown_final.push(task);
            }
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
                "lifetimeExpired": snapshot.expired,
            }));
        }
        // The list shows these final states, which settles the background footer's notices.
        for task in shown_final {
            task.mark_reported();
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

/// Writes each key on its own after `input`, pausing `delay` after the program's stdin has
/// accepted what came before, and encodes it in the modes the program has set by then.
async fn press(
    task: &Task,
    input: &[u8],
    names: &[String],
    parsed: Vec<keys::Key>,
    delay: Duration,
    context: &RequestContext<RoleServer>,
) -> Result<(), String> {
    let mut size = input.len();
    for (index, (name, key)) in names.iter().zip(parsed).enumerate() {
        let sent = |error: String| format!("{error} The first {index} keys were sent.");
        // Input still queued, from this poll or an earlier one, goes first.
        let mut follows = index > 0 || !input.is_empty();
        let accepted = tokio::time::Instant::now() + INPUT_ACCEPT_LIMIT;
        // ponytail: polls the input queue; a delivery signal from the writer if 5 ms
        // granularity ever matters.
        while task.input_pending() {
            follows = true;
            if tokio::time::Instant::now() >= accepted {
                return Err(sent(
                    "The program has not accepted earlier input for 5 s; the task keeps running."
                        .into(),
                ));
            }
            tokio::select! {
                _ = context.ct.cancelled() => return Err(sent("Cancelled; the task keeps running.".into())),
                _ = tokio::time::sleep(Duration::from_millis(5)) => {}
            }
        }
        if follows {
            tokio::select! {
                _ = context.ct.cancelled() => return Err(sent("Cancelled; the task keeps running.".into())),
                _ = tokio::time::sleep(delay) => {}
            }
        }
        let bytes = task
            .modes()
            .and_then(|modes| keys::encode(name, key, modes))
            .map_err(sent)?;
        size += bytes.len();
        if size > MAX_INPUT_BYTES {
            return Err(sent(format!(
                "Key {name:?} in the program's current keyboard mode takes input and keys past 16 KiB; send fewer keys per poll."
            )));
        }
        task.send(Some(&bytes), false).map_err(sent)?;
    }
    Ok(())
}

/// Marks the footer's finished tasks, and the task itself when its shown snapshot was final,
/// reported once their result is built.
fn report(task: &Task, shown: &Snapshot, background: Option<(String, Vec<Arc<Task>>)>) {
    if shown.state != "running" {
        task.mark_reported();
    }
    for shown in background.map(|(_, shown)| shown).unwrap_or_default() {
        shown.mark_reported();
    }
}

impl Server {
    async fn window_result(
        &self,
        action: Action,
        task: Arc<Task>,
        view: View,
        ended: WaitEnd,
    ) -> Result<CallToolResult, String> {
        if view.screen {
            return self.screen_result(action, task, view, ended).await;
        }
        let reader = Arc::clone(&task);
        let encoding = view.encoding.unwrap_or(task.encoding);
        let footer_cap = match view.truncate {
            Truncate::None => FOOTER_LIMIT,
            _ => FOOTER_LIMIT.min(view.max_bytes / 4),
        };
        let background = self.tasks.background(&task.id, footer_cap);
        let footer = background
            .as_ref()
            .map_or(String::new(), |(line, _)| format!("\n{line}"));
        // The state is taken before the read: a finished state means output reached EOF (or
        // stayed open, which the status line notes), so the window below holds all of it. The
        // status line can only grow by its notes.
        let before = task.snapshot();
        let reserved = status_line(&task, &before).len()
            + ended.note(view.conditions).len()
            + NOTES_RESERVE
            + footer.len();
        let budget = match view.truncate {
            Truncate::None => usize::MAX,
            _ => view.max_bytes.saturating_sub(reserved).max(MIN_BODY_BUDGET),
        };
        let window = tokio::task::spawn_blocking(move || {
            reader.read_window(view.truncate, budget, view.raw, encoding, None)
        })
        .await
        .map_err(|error| format!("Internal failure while reading output: {error}."))?
        .map_err(|error| format!("Cannot read the task log: {error}."))?;
        // Log metadata, such as an eviction during the read, is taken after it.
        let after = task.snapshot();
        let snapshot = Snapshot {
            lines: after.lines,
            dropped: after.dropped,
            evicted: after.evicted,
            log_error: after.log_error,
            ..before
        };
        let mut status = status_line(&task, &snapshot);
        status.push_str(&ended.note(view.conditions));
        if window.bad_lines > 0 {
            status.push_str(&format!(
                " · {} lines had bytes invalid in {}; pass encoding (e.g. big5, gbk)",
                window.bad_lines,
                encoding.name()
            ));
        }
        let mut structured = task_json(action, &task, &snapshot);
        structured["waitEndedBy"] = ended.json();
        let text = self.layout(
            &window,
            status,
            &footer,
            &view,
            "(no new output)",
            &mut structured,
        );
        let result = success(text, structured);
        report(&task, &snapshot, background);
        Ok(result)
    }

    /// A PTY task's stored log rendered as a terminal shows it, written beside the log; the
    /// result shows its last lines by default, where a program's newest output is.
    async fn transcript_result(
        &self,
        task: Arc<Task>,
        view: View,
    ) -> Result<CallToolResult, String> {
        let snapshot = task.snapshot();
        let renderer = Arc::clone(&task);
        let transcript = tokio::task::spawn_blocking(move || renderer.transcript())
            .await
            .map_err(|error| format!("Internal failure while rendering: {error}."))??;
        let path = task.transcript_path();
        let footer_cap = match view.truncate {
            Truncate::None => FOOTER_LIMIT,
            _ => FOOTER_LIMIT.min(view.max_bytes / 4),
        };
        let background = self.tasks.background(&task.id, footer_cap);
        let footer = background
            .as_ref()
            .map_or(String::new(), |(line, _)| format!("\n{line}"));
        let mut status = status_line(&task, &snapshot);
        status.push_str(&format!(
            " · transcript {} lines · {}",
            transcript.lines,
            path.display()
        ));
        if transcript.history_full {
            status.push_str(&format!(
                " · history holds the last {} lines; older lines are not shown",
                terminal::HISTORY_LINES
            ));
        }
        if transcript.alternate_sessions > 0 {
            status.push_str(&format!(
                " · {} alternate-screen session(s), each after a {:?} line and merged from the frames it showed",
                transcript.alternate_sessions,
                terminal::ALTERNATE_MARKER
            ));
        }
        if let Some(warning) = conpty::load() {
            status.push_str(&format!(" · {warning}, so lines may be missing"));
        }
        let budget = match view.truncate {
            Truncate::None => usize::MAX,
            _ => view
                .max_bytes
                .saturating_sub(status.len() + NOTES_RESERVE + footer.len())
                .max(MIN_BODY_BUDGET),
        };
        let spec = WindowSpec {
            truncate: view.truncate,
            budget,
            raw: false,
            encoding: encoding_rs::UTF_8,
            source: "transcript",
        };
        let mut cleaner = Cleaner::default();
        let mut builder = WindowBuilder::new(spec, &mut cleaner, 1);
        builder.push(transcript.text.as_bytes());
        let window = builder.finish();
        let mut structured = task_json(Action::Transcript, &task, &snapshot);
        structured["transcriptPath"] = json!(path);
        structured["transcriptLines"] = json!(transcript.lines);
        structured["alternateSessions"] = json!(transcript.alternate_sessions);
        structured["historyFull"] = json!(transcript.history_full);
        let text = self.layout(
            &window,
            status,
            &footer,
            &view,
            "(blank terminal)",
            &mut structured,
        );
        let result = success(text, structured);
        report(&task, &snapshot, background);
        Ok(result)
    }

    /// Fits `window` with the status line and footer into `maxBytes`, adds the window metadata
    /// to `structured`, and returns the text content; `empty` stands in for an empty window.
    fn layout(
        &self,
        window: &Window,
        mut status: String,
        footer: &str,
        view: &View,
        empty: &str,
        structured: &mut Value,
    ) -> String {
        let room = if view.truncate == Truncate::None {
            usize::MAX
        } else {
            // Hard guarantee: the whole text fits maxBytes. A status line longer than half the
            // budget is cut too; structuredContent.logPath, when enabled, keeps the full path.
            output::cut_to(&mut status, view.max_bytes / 2 + 1);
            view.max_bytes - status.len() - 2 - footer.len()
        };
        let (output, cut_lines) = window.fit(room);
        let body = if output.is_empty() {
            empty
        } else {
            output.as_str()
        };
        let text = if self.output_mode == OutputMode::Structured {
            format!("{status}{footer}")
        } else {
            format!("{body}\n\n{status}{footer}")
        };
        add_window(structured, window);
        if self.output_mode != OutputMode::Text {
            structured["output"] = json!(output);
        }
        structured["cutLines"] = json!(cut_lines);
        text
    }
}

impl Server {
    /// The rendered screen of a PTY task, bottom rows first to stay within `maxBytes`.
    async fn screen_result(
        &self,
        action: Action,
        task: Arc<Task>,
        view: View,
        ended: WaitEnd,
    ) -> Result<CallToolResult, String> {
        let background = self
            .tasks
            .background(&task.id, FOOTER_LIMIT.min(view.max_bytes / 4));
        let footer = background
            .as_ref()
            .map_or(String::new(), |(line, _)| format!("\n{line}"));
        // A finished state, taken first, means the screen below holds all output.
        let snapshot = task.snapshot();
        let screen = task.screen()?;
        // The stream counts as read up to the output the screen shows.
        let (reader, until) = (Arc::clone(&task), screen.log_end);
        tokio::task::spawn_blocking(move || {
            let encoding = reader.encoding;
            reader.read_window(
                Truncate::Tail,
                MIN_BODY_BUDGET,
                false,
                encoding,
                Some(until),
            )
        })
        .await
        .map_err(|error| format!("Internal failure while reading output: {error}."))?
        .map_err(|error| format!("Cannot read the task log: {error}."))?;
        let (row, col) = (screen.cursor.0 + 1, screen.cursor.1 + 1);
        let mut status = status_line(&task, &snapshot);
        status.push_str(&ended.note(view.conditions));
        status.push_str(&format!(" · screen, cursor row {row} col {col}"));
        output::cut_to(&mut status, view.max_bytes / 2 + 1);
        let room = view.max_bytes - status.len() - 2 - footer.len();
        let mut omitted = 0;
        let mut body = screen.rows.join("\n");
        while body.len() > room && omitted < screen.rows.len() {
            omitted += 1;
            body = format!(
                "... [{omitted} top rows omitted] ...\n{}",
                screen.rows[omitted..].join("\n")
            );
        }
        // cut_to keeps budget - 1 bytes; the separators are already outside `room`.
        output::cut_to(&mut body, room + 1);
        let shown = if body.is_empty() {
            "(blank screen)"
        } else {
            &body
        };
        let text = if self.output_mode == OutputMode::Structured {
            format!("{status}{footer}")
        } else {
            format!("{shown}\n\n{status}{footer}")
        };
        let mut structured = task_json(action, &task, &snapshot);
        structured["waitEndedBy"] = ended.json();
        structured["cursor"] = json!([row, col]);
        structured["omittedRows"] = json!(omitted);
        if self.output_mode != OutputMode::Text {
            structured["output"] = json!(body);
        }
        let result = success(text, structured);
        report(&task, &snapshot, background);
        Ok(result)
    }
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
    if snapshot.expired {
        line.push_str(" · killed by killAfterMs");
    }
    if let Some(error) = &snapshot.log_error {
        line.push_str(&format!(
            " · log write failed ({error}); {} bytes were not stored",
            snapshot.dropped
        ));
    } else if snapshot.dropped > 0 {
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
        "lifetimeExpired": snapshot.expired,
        "logError": snapshot.log_error,
    })
}

fn add_window(value: &mut Value, window: &Window) {
    let omitted = window.omitted.map_or(0, |(first, last)| last - first + 1);
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
        Action::Transcript => "transcript",
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
    let output_mode = match std::env::var("FASTEXEC_OUTPUT_MODE").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("text") => OutputMode::Text,
        Ok("both") => OutputMode::Both,
        Ok("structured") => OutputMode::Structured,
        value => {
            eprintln!(
                "fastexec: invalid FASTEXEC_OUTPUT_MODE {value:?}; expected text, both, or structured"
            );
            std::process::exit(1);
        }
    };
    let structured_content = match std::env::var("FASTEXEC_STRUCTURED_CONTENT").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("true") => true,
        Ok("false") => false,
        value => {
            eprintln!(
                "fastexec: invalid FASTEXEC_STRUCTURED_CONTENT {value:?}; expected true or false"
            );
            std::process::exit(1);
        }
    };
    let tasks = match Tasks::new() {
        Ok(tasks) => tasks,
        Err(error) => {
            eprintln!("fastexec: cannot create the log directory: {error}");
            std::process::exit(1);
        }
    };
    let server = Server {
        tasks: Arc::clone(&tasks),
        output_mode: if structured_content {
            output_mode
        } else {
            OutputMode::Text
        },
        structured_content,
    };
    let eof = Arc::new(tokio::sync::Notify::new());
    let stdin = EofSignal {
        inner: tokio::io::stdin(),
        eof: Arc::clone(&eof),
    };
    // Signals are watched from the start, so an exit during initialization still cleans up.
    let signal = shutdown_signal();
    tokio::pin!(signal);
    let served = tokio::select! {
        served = server.serve((stdin, tokio::io::stdout())) => Some(served),
        _ = &mut signal => None,
    };
    match served {
        Some(Ok(service)) => {
            // Stdin EOF ends the session at once; rmcp would first drain in-flight waits.
            tokio::select! {
                _ = service.waiting() => {}
                _ = eof.notified() => {}
                _ = &mut signal => {}
            }
        }
        Some(Err(error)) => eprintln!("fastexec: MCP initialization failed: {error}"),
        None => {}
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
