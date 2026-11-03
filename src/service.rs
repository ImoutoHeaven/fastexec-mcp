//! The tool's work without MCP transport types: validate the arguments, run the operation on the
//! tasks, and render the result. The MCP adapter in main.rs calls it in-process; the daemon will
//! call it for IPC and HTTP callers.

use crate::conpty;
use crate::keys;
use crate::output::{self, Cleaner, Truncate, Window, WindowBuilder, WindowSpec};
use crate::process;
use crate::session::{ReadState, Session, TaskView};
use crate::tasks::{self, EVICTED_TRANSCRIPT, Snapshot, StartArgs, Task, Tasks};
use crate::wait::{self, WaitEnd};
use rmcp::model::{CallToolResult, ContentBlock};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

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
const MAX_BYTES_RANGE: std::ops::RangeInclusive<u64> = 100..=1024 * 1024;
/// Room in `maxBytes` for the omission marker, the encoding note, and separators.
const NOTES_RESERVE: usize = 260;
/// Smallest window body, so a long status line still leaves room for some output.
const MIN_BODY_BUDGET: usize = 256;
/// Largest background footer; bounded results give it at most a quarter of `maxBytes`.
const FOOTER_LIMIT: usize = 512;
/// `truncate: "none"` returns at most this much output; the rest is an omitted range.
const NONE_LIMIT: usize = 4 << 20;

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Start,
    Poll,
    Kill,
    List,
    Transcript,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Request {
    /// start: run a command. poll: send input and/or wait, then read unseen output. kill: terminate a task's process tree. list: this server's tasks. transcript: a PTY task's output as a terminal shows it, from the first line no earlier transcript returned, then the pending lines still on screen.
    pub action: Action,
    /// start: the bash command line.
    #[serde(skip_serializing_if = "Option::is_none")]
    command: Option<String>,
    /// start: absolute working directory. Omit for the working directory the session started in.
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    /// start: run inside a pseudo-terminal (default false).
    #[serde(skip_serializing_if = "Option::is_none")]
    pty: Option<bool>,
    /// start: login shell, bash -lc, so profile-managed tools resolve (default true). false runs bash --noprofile --norc -c.
    #[serde(skip_serializing_if = "Option::is_none")]
    login_shell: Option<bool>,
    /// start: kill the task's whole process tree this many ms after launch, whatever the waits and polls, kept processes included; 0 or omitted means no limit.
    #[serde(skip_serializing_if = "Option::is_none")]
    kill_after_ms: Option<u64>,
    /// start: keep the processes the command leaves running when its bash exits, such as a server a CLI starts for itself (adb, Gradle daemon), until kill on this task or the end of the tasks' owner (the session in the default mode, the daemon with FASTEXEC_DAEMON=on) (default false: they end with bash).
    #[serde(skip_serializing_if = "Option::is_none")]
    keep_tree: Option<bool>,
    /// poll, kill, transcript: the task to act on.
    #[serde(skip_serializing_if = "Option::is_none")]
    task_id: Option<String>,
    /// poll: text written to the task exactly as given, before `keys` and before waiting, at most 16 KiB.
    #[serde(skip_serializing_if = "Option::is_none")]
    input: Option<String>,
    /// poll, PTY only: keys pressed after `input` is written, in array order, e.g. ["Enter"] or ["Escape", ":", "q", "Enter"]. One key per item, named as in tmux send-keys: Enter, Tab, BTab, Escape, Space, BSpace, Up, Down, Left, Right, Home, End, PageUp/PgUp/PPage, PageDown/PgDn/NPage, Insert/IC, Delete/DC, F1-F12, KP0-KP9, KP/, KP*, KP-, KP+, KP., KPEnter, [NUL]-[US] for C0 controls, 0xHH for a code point, or one character; prefixes C- (Ctrl), M- (Meta/Alt), S- (Shift) combine, as in C-c, M-x, C-M-a, S-Up, and ^c means C-c. Names ignore case. Arrow and keypad keys follow the program's cursor and keypad modes. An unknown name, or a modifier the key cannot carry (C-Enter, S-a) in the program's mode when the poll starts, is an error before anything is sent.
    #[serde(skip_serializing_if = "Option::is_none")]
    keys: Option<Vec<String>>,
    /// poll, with keys: pause before each key, the first one after input included, 0-2000 ms (default 150); 0 writes input and keys at once. Programs that time keystrokes to detect pastes, such as Codex, read a key sent right after text as part of the paste; programs with an escape timeout read Escape and the next key as one Alt key unless the pause exceeds it (often 500).
    #[schemars(range(min = 0, max = 2000))]
    #[serde(skip_serializing_if = "Option::is_none")]
    key_delay_ms: Option<u64>,
    /// poll: close stdin after input (pipe mode only).
    #[serde(skip_serializing_if = "Option::is_none")]
    eof: Option<bool>,
    /// start, poll: how long to wait at most, 0-240000 ms; the cap returns each call within a 5-minute prompt-cache lifetime, so poll again to wait longer. kill ignores it.
    #[schemars(range(min = 0, max = 240000))]
    #[serde(skip_serializing_if = "Option::is_none")]
    wait_ms: Option<u64>,
    /// start, poll: end the wait early when any condition holds; the task keeps running. Texts are literal and case-sensitive, 1-16 per condition, each 1-4096 bytes without line breaks. waitMs stays the longest wait and defaults to 30000 with returnWhen.
    #[serde(skip_serializing_if = "Option::is_none")]
    return_when: Option<wait::ReturnWhen>,
    /// start, poll, list, transcript: output window mode (default head_tail; transcript defaults to tail).
    #[serde(skip_serializing_if = "Option::is_none")]
    truncate: Option<Truncate>,
    /// start, poll, kill, list, transcript: byte budget of the result, 100-1048576 (default 16384); error messages stay whole.
    #[schemars(range(min = 100, max = 1048576))]
    #[serde(skip_serializing_if = "Option::is_none")]
    max_bytes: Option<u64>,
    /// start, poll: return output without cleaning (default false).
    #[serde(skip_serializing_if = "Option::is_none")]
    raw: Option<bool>,
    /// start, poll: WHATWG label of the output encoding, e.g. "big5" or "gbk". On start it becomes the task default.
    #[serde(skip_serializing_if = "Option::is_none")]
    encoding: Option<String>,
    /// start, poll, PTY only: return the rendered terminal screen and cursor position instead of the output stream; for full-screen and TUI programs. Marks the stream read. Not with truncate, raw, or encoding.
    #[serde(skip_serializing_if = "Option::is_none")]
    screen: Option<bool>,
    /// start, poll, PTY only: terminal columns, 20-500. start defaults 120; poll keeps the current value when omitted.
    #[schemars(range(min = 20, max = 500))]
    #[serde(skip_serializing_if = "Option::is_none")]
    cols: Option<u16>,
    /// start, poll, PTY only: terminal screen rows, 5-200. start defaults 30; poll keeps the current value when omitted.
    #[schemars(range(min = 5, max = 200))]
    #[serde(skip_serializing_if = "Option::is_none")]
    rows: Option<u16>,
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

/// [`NONE_LIMIT`]; debug builds take a smaller one from `FASTEXEC_TEST_NONE_LIMIT`, so tests
/// reach the cap without writing 4 MiB.
fn none_limit() -> usize {
    std::env::var("FASTEXEC_TEST_NONE_LIMIT")
        .ok()
        .and_then(|limit| limit.parse().ok())
        .filter(|limit| cfg!(debug_assertions) && (1..=NONE_LIMIT).contains(limit))
        .unwrap_or(NONE_LIMIT)
}

fn validate(request: &Request) -> Result<View, String> {
    let present = [
        ("command", request.command.is_some()),
        ("cwd", request.cwd.is_some()),
        ("pty", request.pty.is_some()),
        ("loginShell", request.login_shell.is_some()),
        ("killAfterMs", request.kill_after_ms.is_some()),
        ("keepTree", request.keep_tree.is_some()),
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
        ("cols", request.cols.is_some()),
        ("rows", request.rows.is_some()),
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
        Action::Start => [
            "command",
            "cwd",
            "pty",
            "loginShell",
            "killAfterMs",
            "keepTree",
            "cols",
            "rows",
        ]
        .into_iter()
        .chain(VIEW)
        .collect(),
        Action::Poll => [
            "taskId",
            "input",
            "eof",
            "keys",
            "keyDelayMs",
            "cols",
            "rows",
        ]
        .into_iter()
        .chain(VIEW)
        .collect(),
        Action::Kill => vec!["taskId", "waitMs", "maxBytes"],
        Action::List => vec!["truncate", "maxBytes"],
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
    // A size belongs to a PTY task's terminal; a pipe task's output is a byte stream. `start`
    // reads an omitted `pty` as false, so a size there needs `pty: true`.
    if let Some(name) = [("cols", request.cols), ("rows", request.rows)]
        .into_iter()
        .find_map(|(name, set)| set.is_some().then_some(name))
    {
        let applies = match request.action {
            Action::Start => request.pty == Some(true),
            _ => true,
        };
        if !applies {
            return Err(format!(
                "`{name}` applies to PTY tasks only; start with pty: true or remove it."
            ));
        }
    }
    if let Some((name, value, range)) = [
        (
            "cols",
            request.cols,
            process::PTY_COLS_MIN..=process::PTY_COLS_MAX,
        ),
        (
            "rows",
            request.rows,
            process::PTY_ROWS_MIN..=process::PTY_ROWS_MAX,
        ),
    ]
    .into_iter()
    .find_map(|(name, set, range)| {
        set.filter(|value| !range.contains(value))
            .map(|value| (name, value, range))
    }) {
        return Err(format!(
            "{name} {value} is out of range {}-{}.",
            range.start(),
            range.end()
        ));
    }
    if let Some(bytes) = request.max_bytes {
        if !MAX_BYTES_RANGE.contains(&bytes) {
            return Err(format!("maxBytes {bytes} is out of range 100-1048576."));
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputMode {
    Text,
    Both,
    Structured,
}

#[cfg_attr(not(test), allow(dead_code))] // the socket block maps wire names to modes
impl OutputMode {
    /// The name the wire carries.
    pub fn name(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Both => "both",
            Self::Structured => "structured",
        }
    }

    /// The mode a wire name stands for. A name outside these three is refused, so a handshake
    /// that misspells one is a reject rather than a silent default.
    pub fn parse(name: &str) -> Result<Self, String> {
        match name {
            "text" => Ok(Self::Text),
            "both" => Ok(Self::Both),
            "structured" => Ok(Self::Structured),
            other => Err(format!("unknown output mode {other:?}")),
        }
    }
}

/// How a result is rendered, carried by each call: the daemon takes it from the caller's
/// handshake.
#[derive(Clone, Copy)]
pub struct Render {
    pub output_mode: OutputMode,
    pub structured_content: bool,
}

impl Render {
    /// Structured content off also means text only, so a caller that asks for structured output
    /// without structured content still gets the output body.
    fn effective(self) -> Self {
        Self {
            output_mode: if self.structured_content {
                self.output_mode
            } else {
                OutputMode::Text
            },
            structured_content: self.structured_content,
        }
    }
}

/// Strips structured content when the caller turned it off.
pub fn suppress(mut result: CallToolResult, render: Render) -> CallToolResult {
    if !render.structured_content {
        result.structured_content = None;
    }
    result
}

/// What a call carries besides its arguments: the session it reads for, how to render, and the
/// ways the caller can stop it or watch it.
pub struct Call<'a> {
    pub session: &'a Arc<Session>,
    pub render: Render,
    pub cancel: CancellationToken,
    /// The call replaces this with its latest progress message; a forwarder sends them.
    pub progress: Option<watch::Sender<Option<(f64, String)>>>,
    /// The daemon's call ID, kept on a task a start creates so `list` can name the call that
    /// started it. The stdio adapter has no call IDs, so it passes `None`.
    pub call_id: Option<u64>,
}

/// The tool's operations and renderers, independent of any transport.
pub struct Service {
    tasks: Arc<Tasks>,
    /// Whether the tasks live in a daemon: its unknown-task texts name the daemon and the tasks
    /// that outlive a session.
    daemon: bool,
}

impl Service {
    pub fn new(tasks: Arc<Tasks>, daemon: bool) -> Self {
        Self { tasks, daemon }
    }

    /// Runs one call and renders its result.
    pub async fn call(&self, request: Request, mut call: Call<'_>) -> CallToolResult {
        // Views of tasks that have retired are dropped here, so a call that never reaches the
        // background footer cannot keep them alive.
        self.tasks.prune_views(call.session);
        call.render = call.render.effective();
        let action = request.action;
        let result = match self.handle(request, &call).await {
            Ok(result) => result,
            Err(message) => error_result(action, &message),
        };
        suppress(result, call.render)
    }

    /// Runs one call from raw arguments, the way a daemon receives them, with the same
    /// validation and errors.
    #[allow(dead_code)] // The daemon dispatches raw arguments; the stdio adapter parses them.
    pub async fn call_json(&self, arguments: Value, call: Call<'_>) -> CallToolResult {
        match serde_json::from_value::<Request>(arguments) {
            Ok(request) => self.call(request, call).await,
            Err(error) => CallToolResult::error(vec![ContentBlock::text(format!(
                "failed to deserialize parameters: {error}"
            ))]),
        }
    }

    async fn handle(&self, request: Request, call: &Call<'_>) -> Result<CallToolResult, String> {
        let view = validate(&request)?;
        let session = Arc::clone(call.session);
        match request.action {
            Action::List => Ok(self.list(&session, &view)),
            Action::Start => {
                let cwd = match request.cwd {
                    None => session.launch.cwd.clone(),
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
                    cols: request.cols.unwrap_or(process::PTY_COLS),
                    rows: request.rows.unwrap_or(process::PTY_ROWS),
                    login: request.login_shell.unwrap_or(true),
                    keep: request.keep_tree.unwrap_or(false),
                    encoding: view.encoding.unwrap_or(encoding_rs::UTF_8),
                    kill_after_ms: request.kill_after_ms,
                    owner: session.label.clone(),
                    started_by: call.call_id.map(|id| (session.id.clone(), id)),
                    launch: Arc::clone(&session.launch),
                };
                let tasks = Arc::clone(&self.tasks);
                // The session takes the task even when this request is cancelled meanwhile, so
                // its own task shows in its background footer.
                let starting = Arc::clone(&session);
                let task = tokio::task::spawn_blocking(move || {
                    let task = tasks.start(args)?;
                    starting.view(&task);
                    Ok::<_, String>(task)
                })
                .await
                .map_err(|error| format!("Internal failure while starting: {error}."))??;
                let task_view = session.view(&task);
                let state = read_gate(&task_view, &call.cancel).await?;
                let when = request.return_when.as_ref();
                let mark = wait::mark(&task, &task_view, &state, when)?;
                let encoding = view.encoding.unwrap_or(task.encoding);
                let wait = request.wait_ms.unwrap_or(30_000);
                let ended = wait::wait(
                    &task,
                    wait,
                    when,
                    mark,
                    encoding,
                    &call.cancel,
                    call.progress.as_ref(),
                )
                .await?;
                self.window_result(
                    call.render,
                    &session,
                    Action::Start,
                    task,
                    task_view,
                    state,
                    view,
                    ended,
                )
                .await
            }
            Action::Poll => {
                let task = self.task(request.task_id.as_deref())?;
                let task_view = session.view(&task);
                let state = read_gate(&task_view, &call.cancel).await?;
                let when = request.return_when.as_ref();
                let watches_screen = when.and_then(|when| when.screen_condition());
                let needs_pty = [
                    ("screen", view.screen),
                    ("keys", request.keys.is_some()),
                    ("cols", request.cols.is_some() || request.rows.is_some()),
                ]
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
                // A PTY has no stdin half-close, so eof is refused before this poll changes
                // anything, a resize included.
                if request.eof == Some(true) && task.pty {
                    return Err(
                        "EOF_UNSUPPORTED_IN_PTY: a PTY has no stdin half-close. Press Ctrl-D with keys: [\"C-d\"], or kill the task."
                            .into(),
                    );
                }
                // Either axis may come alone: the axes this poll names are validated above, and
                // the omitted ones are resolved under the input gate, beside the resize, so a
                // concurrent poll cannot take this poll's axis from a stale pair.
                let axes = match (request.cols, request.rows) {
                    (None, None) => None,
                    (cols_axis, rows_axis) => Some((cols_axis, rows_axis)),
                };
                let input = request.input.as_deref().unwrap_or_default().as_bytes();
                let names = request.keys.as_deref().unwrap_or_default();
                // One poll's resize, input, keys, and eof go out as one sequence, and the wait's
                // starting mark is taken after the gate, so screenAppears compares against the
                // screen this caller's resize and input change.
                let sends = request.input.is_some()
                    || request.keys.is_some()
                    || request.eof.is_some()
                    || axes.is_some();
                let gate = if sends {
                    Some(tokio::select! {
                        biased;
                        _ = call.cancel.cancelled() => {
                            return Err("Cancelled before the input was sent; the task keeps running.".into());
                        }
                        gate = task.input_gate.lock() => gate,
                    })
                } else {
                    None
                };
                // Every key is checked against the current modes before anything is written, and
                // the limit holds for each burst: a mode declared during the call can encode the
                // same keys longer than the modes the first pass read.
                let encode = |task: &Task| -> Result<(Vec<u8>, Vec<keys::Key>), String> {
                    let mut data = input.to_vec();
                    let mut parsed = Vec::with_capacity(names.len());
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
                    Ok((data, parsed))
                };
                let (mut data, parsed) = encode(&task)?;
                let mark = wait::mark(&task, &task_view, &state, when)?;
                // The resize goes out after every validation and before the input and keys:
                // the emulator is at the new size before the program's redraw is parsed, the
                // native resize runs with no lock capture needs held, and its replies reach the
                // program through the same path as any other answer.
                if let Some((cols_axis, rows_axis)) = axes {
                    let (current_cols, current_rows) = task.size();
                    task.resize(
                        &self.tasks,
                        cols_axis.unwrap_or(current_cols),
                        rows_axis.unwrap_or(current_rows),
                    )?;
                }
                let delay =
                    Duration::from_millis(request.key_delay_ms.unwrap_or(DEFAULT_KEY_DELAY_MS));
                if delay.is_zero() {
                    // Without a pause, input and keys go out as one write, like a typed burst.
                    // The resize settles a synchronized update the program held, and the modes
                    // that held it declares are the terminal's from here on: the burst is
                    // encoded again rather than sent with the modes the validation pass read.
                    if axes.is_some() && !names.is_empty() {
                        data = encode(&task)?.0;
                    }
                    task.send(Some(&data), request.eof.unwrap_or(false))?;
                } else {
                    task.send(Some(input), request.eof.unwrap_or(false))?;
                    press(&task, input, names, parsed, delay, &call.cancel).await?;
                }
                drop(gate);
                let default_wait =
                    if (request.input.is_some() || request.keys.is_some() || axes.is_some())
                        && when.is_none()
                    {
                        2_000
                    } else {
                        30_000
                    };
                let wait = request.wait_ms.unwrap_or(default_wait);
                let encoding = view.encoding.unwrap_or(task.encoding);
                let ended = wait::wait(
                    &task,
                    wait,
                    when,
                    mark,
                    encoding,
                    &call.cancel,
                    call.progress.as_ref(),
                )
                .await?;
                self.window_result(
                    call.render,
                    &session,
                    Action::Poll,
                    task,
                    task_view,
                    state,
                    view,
                    ended,
                )
                .await
            }
            Action::Kill => {
                let task = self.task(request.task_id.as_deref())?;
                let task_view = session.view(&task);
                task.kill().map_err(|error| {
                    format!(
                        "Cannot terminate the process tree of {}: {error}. Poll to see its state, or retry kill.",
                        task.id
                    )
                })?;
                let (mut done, mut tree_done) = (task.done(), task.tree_done());
                let finished = async move {
                    let _ = done.wait_for(|done| *done).await;
                    let _ = tree_done.wait_for(|done| *done).await;
                };
                let _ = tokio::time::timeout(KILL_WAIT, finished).await;
                let snapshot = task.snapshot_held();
                let mut text = status_line(&task, &snapshot);
                if snapshot.state == "running" || snapshot.kept {
                    text.push_str(" · the tree did not exit within 5 s; poll later to confirm");
                }
                let budget = status_budget(&text, view.max_bytes, 0);
                output::cut_to(&mut text, budget);
                let background =
                    self.tasks
                        .background(&session, &task.id, FOOTER_LIMIT.min(view.max_bytes / 4));
                if let Some((line, _)) = &background {
                    text.push('\n');
                    text.push_str(line);
                }
                let result = success(text, task_json(Action::Kill, &task, &snapshot));
                report(&session, &task_view, &snapshot, background);
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
                let task_view = session.view(&task);
                let state = read_gate(&task_view, &call.cancel).await?;
                self.transcript_result(call.render, &session, task, task_view, state, view)
                    .await
            }
        }
    }

    fn task(&self, id: Option<&str>) -> Result<Arc<Task>, String> {
        let id = id.unwrap_or_default();
        if let Some(task) = self.tasks.find(id) {
            return Ok(task);
        }
        // An ID whose epoch differs came from another server, whose tasks this one never had.
        let epoch_differs = id
            .rsplit_once('-')
            .is_some_and(|(epoch, _)| epoch != self.tasks.epoch());
        Err(if self.daemon {
            if epoch_differs {
                format!(
                    "Unknown taskId {id:?}: it comes from an earlier fastexec daemon; tasks do not survive a daemon restart. Use action list to see this daemon's tasks."
                )
            } else {
                format!(
                    "Unknown taskId {id:?}. Finished tasks are kept for up to 24 h; use action list to see this daemon's tasks."
                )
            }
        } else if epoch_differs {
            format!(
                "Unknown taskId {id:?}: it comes from an earlier fastexec server; tasks do not survive a restart. Use action list to see this server's tasks."
            )
        } else {
            format!(
                "Unknown taskId {id:?}. Tasks live as long as this server; use action list to see them."
            )
        })
    }

    fn list(&self, session: &Session, view: &View) -> CallToolResult {
        let tasks = self.tasks.listed();
        let mut lines = Vec::new();
        let mut entries = Vec::new();
        let mut shown = Vec::new();
        for (task, snapshot) in &tasks {
            let command: String = task.command.chars().take(120).collect();
            let code = snapshot
                .exit_code
                .map_or(String::new(), |code| format!(" {code}"));
            // One read, so the line and the fields name the same size.
            let size = task.pty.then(|| task.size());
            lines.push(format!(
                "{} [{}{code}] {:.1}s{}{} · {} · {}",
                task.id,
                snapshot.state,
                snapshot.elapsed.as_secs_f64(),
                match size {
                    Some((cols, rows)) => format!(" pty {cols}x{rows}"),
                    None => String::new(),
                },
                if snapshot.kept {
                    format!(" · keeps {}", members(&snapshot.held))
                } else if snapshot.tree_ended {
                    " · kept processes ended".to_string()
                } else {
                    String::new()
                },
                match &task.started_by {
                    Some((_, call)) => format!("{} call {call}", task.owner),
                    None => task.owner.clone(),
                },
                command.replace('\n', " ")
            ));
            entries.push(json!({
                "taskId": task.id, "state": snapshot.state, "exitCode": snapshot.exit_code, "pty": task.pty,
                "cols": size.map(|(cols, _)| cols), "rows": size.map(|(_, rows)| rows),
                "elapsedMs": snapshot.elapsed.as_millis() as u64, "command": command, "logPath": task.log_path,
                "lifetimeExpired": snapshot.expired, "keepTree": task.keep, "keptRunning": snapshot.kept,
                "held": members_json(&snapshot.held),
                "leftovers": members_json(&snapshot.leftovers), "killError": snapshot.kill_error,
                "owner": task.owner,
                "session": task.started_by.as_ref().map(|(session, _)| session),
                "callId": task.started_by.as_ref().map(|(_, call)| *call),
            }));
            shown.push((task, snapshot));
        }
        let (kept, text) = fit_lines(&lines, view.truncate, view.max_bytes);
        // The list shows these states, which settles the background footer's notices; a task
        // the window leaves out stays due, and a task this session never addressed is not its
        // to mark.
        for index in kept {
            let (task, snapshot) = &shown[index];
            if let Some(task_view) = session.existing(task) {
                task_view.mark_reported(snapshot);
                task_view.mark_keeps_reported(snapshot);
            }
        }
        let text = if lines.is_empty() {
            "No tasks.".to_string()
        } else {
            text
        };
        success(
            text,
            json!({ "ok": true, "action": "list", "tasks": entries }),
        )
    }
}

/// Joins the whole `lines` that fit `budget` bytes, picked from the ends `truncate` names, with
/// one marker line where lines were left out. Returns the indexes of the lines shown and the
/// text.
fn fit_lines(lines: &[String], truncate: Truncate, budget: usize) -> (Vec<usize>, String) {
    let all = || ((0..lines.len()).collect(), lines.join("\n"));
    // `truncate: "none"` returns whole rows up to the none cap, and the marker line names the
    // rows left out, exactly as the bounded modes do.
    let budget = if truncate == Truncate::None {
        none_limit()
    } else {
        budget
    };
    if lines.iter().map(|line| line.len() + 1).sum::<usize>() <= budget + 1 {
        return all();
    }
    let marker =
        |omitted: usize| format!("... [{omitted} tasks omitted; raise maxBytes to see them] ...");
    let last = lines.len() - 1;
    let order: Vec<usize> = match truncate {
        Truncate::Head => (0..=last).collect(),
        Truncate::Tail => (0..=last).rev().collect(),
        _ => (0..=last / 2).flat_map(|i| [i, last - i]).collect(),
    };
    // Room for the marker at its longest, with the newlines around it.
    let mut room = budget.saturating_sub(marker(lines.len()).len() + 2);
    let mut kept = Vec::new();
    for index in order {
        if kept.contains(&index) {
            continue;
        }
        if lines[index].len() + 1 > room {
            break;
        }
        room -= lines[index].len() + 1;
        kept.push(index);
    }
    kept.sort_unstable();
    let mut text: Vec<String> = Vec::new();
    for index in 0..=last {
        if kept.contains(&index) {
            text.push(lines[index].clone());
        } else if index == 0 || kept.contains(&(index - 1)) {
            text.push(marker(lines.len() - kept.len()));
        }
    }
    (kept, text.join("\n"))
}

/// Writes each key on its own after `input`, pausing `delay` after the program's stdin has
/// accepted what came before, and encodes it in the modes the program has set by then.
async fn press(
    task: &Task,
    input: &[u8],
    names: &[String],
    parsed: Vec<keys::Key>,
    delay: Duration,
    cancel: &CancellationToken,
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
                _ = cancel.cancelled() => return Err(sent("Cancelled; the task keeps running.".into())),
                _ = tokio::time::sleep(Duration::from_millis(5)) => {}
            }
        }
        if follows {
            tokio::select! {
                _ = cancel.cancelled() => return Err(sent("Cancelled; the task keeps running.".into())),
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

/// The session's read gate for a task, taken with cancellation: one session's reads of a task
/// run one at a time, and another session's reads of it do not wait.
async fn read_gate(
    view: &TaskView,
    cancel: &CancellationToken,
) -> Result<tokio::sync::OwnedMutexGuard<ReadState>, String> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            Err("Cancelled before the output was read; the task keeps running.".into())
        }
        state = view.read_gate().lock_owned() => Ok(state),
    }
}

/// Marks what the result showed of the task and of the footer's tasks reported, once the
/// result is built.
fn report(
    session: &Session,
    view: &TaskView,
    shown: &Snapshot,
    background: Option<(String, Vec<tasks::Shown>)>,
) {
    view.mark_reported(shown);
    for (task, shown) in background.map(|(_, shown)| shown).unwrap_or_default() {
        let task_view = session.view(&task);
        task_view.mark_reported(&shown);
        task_view.mark_keeps_reported(&shown);
    }
}

impl Service {
    #[allow(clippy::too_many_arguments)]
    async fn window_result(
        &self,
        render: Render,
        session: &Session,
        action: Action,
        task: Arc<Task>,
        task_view: Arc<TaskView>,
        state: tokio::sync::OwnedMutexGuard<ReadState>,
        view: View,
        ended: WaitEnd,
    ) -> Result<CallToolResult, String> {
        if view.screen {
            return self
                .screen_result(render, session, action, task, task_view, state, view, ended)
                .await;
        }
        let reader = Arc::clone(&task);
        let encoding = view.encoding.unwrap_or(task.encoding);
        let footer_cap = match view.truncate {
            Truncate::None => FOOTER_LIMIT,
            _ => FOOTER_LIMIT.min(view.max_bytes / 4),
        };
        let background = self.tasks.background(session, &task.id, footer_cap);
        let footer = background
            .as_ref()
            .map_or(String::new(), |(line, _)| format!("\n{line}"));
        // The state is taken before the read: a finished state means output reached EOF (or
        // stayed open, which the status line notes), so the window below holds all of it. The
        // status line can only grow by its notes.
        let before = task.snapshot_held();
        let reserved = status_line(&task, &before).len()
            + ended.note(view.conditions).len()
            + NOTES_RESERVE
            + footer.len();
        let budget = match view.truncate {
            Truncate::None => none_limit(),
            _ => view.max_bytes.saturating_sub(reserved).max(MIN_BODY_BUDGET),
        };
        let (window, state) = tokio::task::spawn_blocking(move || {
            let mut state = state;
            let window =
                reader.read_window(&mut state, view.truncate, budget, view.raw, encoding, None);
            (window, state)
        })
        .await
        .map_err(|error| format!("Internal failure while reading output: {error}."))?;
        let window = window.map_err(|error| format!("Cannot read the task log: {error}."))?;
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
            render,
            &window,
            status,
            &footer,
            &view,
            "(no new output)",
            &mut structured,
        );
        let result = success(text, structured);
        report(session, &task_view, &snapshot, background);
        drop(state);
        Ok(result)
    }

    /// A PTY task's transcript from the first line no earlier read showed: its final lines,
    /// then the pending ones, which the file also holds after the final lines until output
    /// changes them. The result shows the last lines by default, where a program's newest
    /// output is.
    async fn transcript_result(
        &self,
        render: Render,
        session: &Session,
        task: Arc<Task>,
        task_view: Arc<TaskView>,
        state: tokio::sync::OwnedMutexGuard<ReadState>,
        view: View,
    ) -> Result<CallToolResult, String> {
        let snapshot = task.snapshot_held();
        let (reader, tasks) = (Arc::clone(&task), Arc::clone(&self.tasks));
        let (transcript, state) = tokio::task::spawn_blocking(move || {
            let mut state = state;
            let transcript = reader.transcript(&tasks, &mut state);
            (transcript, state)
        })
        .await
        .map_err(|error| format!("Internal failure while reading: {error}."))?;
        let transcript = transcript?;
        let pending_lines = transcript.pending.matches('\n').count() as u64;
        let total = transcript.lines + pending_lines;
        let footer_cap = match view.truncate {
            Truncate::None => FOOTER_LIMIT,
            _ => FOOTER_LIMIT.min(view.max_bytes / 4),
        };
        let background = self.tasks.background(session, &task.id, footer_cap);
        let footer = background
            .as_ref()
            .map_or(String::new(), |(line, _)| format!("\n{line}"));
        let mut status = status_line(&task, &snapshot);
        status.push_str(&format!(
            " · transcript {total} lines, final through {}; this read starts at line {} · {}",
            transcript.lines,
            transcript.first_line,
            transcript.path.display()
        ));
        if transcript.cut {
            status.push_str(" · pending lines past the transcript's 64 MiB limit are left out");
        }
        if let Some(reason) = &transcript.stopped {
            status.push_str(&format!(
                " · the transcript stops after line {}: {reason}",
                transcript.lines
            ));
        }
        if let Some(warning) = conpty::load() {
            status.push_str(&format!(" · {warning}, so lines may be missing"));
        }
        let budget = match view.truncate {
            Truncate::None => none_limit(),
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
        let mut structured = task_json(Action::Transcript, &task, &snapshot);
        structured["transcriptPath"] = json!(transcript.path);
        structured["transcriptLines"] = json!(total);
        structured["finalLines"] = json!(transcript.lines);
        structured["firstLine"] = json!(transcript.first_line);
        let logged = Arc::clone(&task);
        let window = tokio::task::spawn_blocking(move || {
            /// Reads the final lines into `builder`.
            fn read_into(
                path: &std::path::Path,
                start: u64,
                bytes: u64,
                builder: &mut WindowBuilder<'_>,
            ) -> std::io::Result<()> {
                // Final lines only grow, so the range read stays as it was.
                let mut file = File::open(path)?;
                file.seek(SeekFrom::Start(start))?;
                let mut remaining = bytes - start;
                let mut buffer = vec![0_u8; 64 * 1024];
                while remaining > 0 {
                    let want = remaining.min(buffer.len() as u64) as usize;
                    let read = file.read(&mut buffer[..want])?;
                    if read == 0 {
                        break;
                    }
                    builder.push(&buffer[..read]);
                    remaining -= read as u64;
                }
                Ok(())
            }
            let mut cleaner = Cleaner::default();
            let mut builder = WindowBuilder::new(spec, &mut cleaner, transcript.first_line);
            if let Err(error) = read_into(
                &transcript.path,
                transcript.start,
                transcript.bytes,
                &mut builder,
            ) {
                // Writing the pending lines can evict this very task's files; the result is then
                // the evicted one, as if the eviction had been seen up front.
                if logged.evicted() {
                    return Err(EVICTED_TRANSCRIPT.to_string());
                }
                return Err(format!("Cannot read the transcript file: {error}."));
            }
            builder.push(transcript.pending.as_bytes());
            Ok(builder.finish())
        })
        .await
        .map_err(|error| format!("Internal failure while reading: {error}."))??;
        let text = self.layout(
            render,
            &window,
            status,
            &footer,
            &view,
            "(no transcript lines since the last read)",
            &mut structured,
        );
        let result = success(text, structured);
        report(session, &task_view, &snapshot, background);
        drop(state);
        Ok(result)
    }

    /// Fits `window` with the status line and footer into `maxBytes`, adds the window metadata
    /// to `structured`, and returns the text content; `empty` stands in for an empty window.
    #[allow(clippy::too_many_arguments)]
    fn layout(
        &self,
        render: Render,
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
            let budget = status_budget(&status, view.max_bytes, footer.len());
            output::cut_to(&mut status, budget);
            view.max_bytes - status.len() - 2 - footer.len()
        };
        let (output, cut_lines, omitted) = window.fit(room);
        // A small maxBytes may leave no room for the placeholder.
        let body = if output.is_empty() {
            if empty.len() <= room { empty } else { "" }
        } else {
            output.as_str()
        };
        let text = if render.output_mode == OutputMode::Structured {
            format!("{status}{footer}")
        } else {
            format!("{body}\n\n{status}{footer}")
        };
        add_window(structured, window, omitted);
        if render.output_mode != OutputMode::Text {
            structured["output"] = json!(output);
        }
        structured["cutLines"] = json!(cut_lines);
        text
    }
}

impl Service {
    /// The rendered screen of a PTY task, bottom rows first to stay within `maxBytes`.
    #[allow(clippy::too_many_arguments)]
    async fn screen_result(
        &self,
        render: Render,
        session: &Session,
        action: Action,
        task: Arc<Task>,
        task_view: Arc<TaskView>,
        state: tokio::sync::OwnedMutexGuard<ReadState>,
        view: View,
        ended: WaitEnd,
    ) -> Result<CallToolResult, String> {
        let background =
            self.tasks
                .background(session, &task.id, FOOTER_LIMIT.min(view.max_bytes / 4));
        let footer = background
            .as_ref()
            .map_or(String::new(), |(line, _)| format!("\n{line}"));
        // A finished state, taken first, means the screen below holds all output.
        let snapshot = task.snapshot_held();
        let screen = task.screen()?;
        // The stream counts as read up to the output the screen shows.
        let (reader, until) = (Arc::clone(&task), screen.log_end);
        let (read, state) = tokio::task::spawn_blocking(move || {
            let mut state = state;
            let read = reader.read_window(
                &mut state,
                Truncate::Tail,
                MIN_BODY_BUDGET,
                false,
                reader.encoding,
                Some(until),
            );
            (read, state)
        })
        .await
        .map_err(|error| format!("Internal failure while reading output: {error}."))?;
        read.map_err(|error| format!("Cannot read the task log: {error}."))?;
        let (row, col) = (screen.cursor.0 + 1, screen.cursor.1 + 1);
        let mut status = status_line(&task, &snapshot);
        status.push_str(&ended.note(view.conditions));
        status.push_str(&format!(
            " · screen {}x{}, cursor row {row} col {col}",
            screen.cols, screen.rows_count
        ));
        let budget = status_budget(&status, view.max_bytes, footer.len());
        output::cut_to(&mut status, budget);
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
        let shown = if body.is_empty() && room >= "(blank screen)".len() {
            "(blank screen)"
        } else {
            &body
        };
        let text = if render.output_mode == OutputMode::Structured {
            format!("{status}{footer}")
        } else {
            format!("{shown}\n\n{status}{footer}")
        };
        let mut structured = task_json(action, &task, &snapshot);
        structured["waitEndedBy"] = ended.json();
        structured["cols"] = json!(screen.cols);
        structured["rows"] = json!(screen.rows_count);
        structured["cursor"] = json!([row, col]);
        structured["omittedRows"] = json!(omitted);
        if render.output_mode != OutputMode::Text {
            structured["output"] = json!(body);
        }
        let result = success(text, structured);
        report(session, &task_view, &snapshot, background);
        drop(state);
        Ok(result)
    }
}

/// The leading `[state] taskId` of a status line, in bytes. A text-only client reads the task ID
/// from there, so a cut never falls inside it.
fn status_prefix(status: &str) -> usize {
    status.find(" · ").unwrap_or(status.len())
}

/// The room a status line gets: half the budget, or enough for its `[state] taskId` prefix and
/// the cut marker, whichever is more, and never more than the text leaves after `reserved`.
fn status_budget(status: &str, max_bytes: usize, reserved: usize) -> usize {
    let needed = status_prefix(status) + output::MARK.len() + 1;
    (max_bytes / 2 + 1)
        .max(needed)
        .min(max_bytes.saturating_sub(2 + reserved))
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
    if snapshot.kept {
        let names = members(&snapshot.held);
        line.push_str(&match &snapshot.kill_error {
            Some(error) => format!(" · could not end {names}: {error}; kill retries"),
            None => format!(" · keeps {names} running; kill ends them"),
        });
    } else if snapshot.tree_ended {
        line.push_str(" · kept processes ended");
    } else if !snapshot.leftovers.is_empty() && !task.keep {
        line.push_str(&format!(
            " · ended with bash: {}; start with keepTree: true to keep them",
            members(&snapshot.leftovers)
        ));
    }
    if snapshot.forced_end {
        line.push_str(" · root forced after 5 s exit grace");
    }
    if snapshot.output_open {
        line.push_str(if snapshot.kept {
            " · processes it keeps hold the output open; later output may follow"
        } else {
            " · output stayed open after exit; later output may follow"
        });
    }
    line
}

/// Names processes as `adb.exe (4120), sleep (77) and 3 more`.
fn members(members: &[process::Member]) -> String {
    if members.is_empty() {
        return "processes".into();
    }
    let mut text = members
        .iter()
        .take(3)
        .map(|member| format!("{} ({})", member.name, member.pid))
        .collect::<Vec<_>>()
        .join(", ");
    if members.len() > 3 {
        text.push_str(&format!(" and {} more", members.len() - 3));
    }
    text
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
        "keepTree": task.keep,
        "forcedEnd": snapshot.forced_end,
        "leftovers": members_json(&snapshot.leftovers),
        "keptRunning": snapshot.kept,
        "held": members_json(&snapshot.held),
        "killError": snapshot.kill_error,
    })
}

fn members_json(members: &[process::Member]) -> Value {
    json!(
        members
            .iter()
            .map(|member| json!({"pid": member.pid, "name": member.name}))
            .collect::<Vec<_>>()
    )
}

fn add_window(value: &mut Value, window: &Window, omitted: Option<(u64, u64)>) {
    value["omittedLines"] = json!(omitted.map_or(0, |(first, last)| last - first + 1));
    value["omittedRange"] = json!(omitted.map(|(first, last)| [first, last]));
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

pub fn error_result(action: Action, message: &str) -> CallToolResult {
    let mut result = CallToolResult::error(vec![ContentBlock::text(message)]);
    result.structured_content =
        Some(json!({ "ok": false, "action": action_name(action), "error": message }));
    result
}
