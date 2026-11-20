//! The tool's work without MCP transport types: validate the arguments, run the operation on the
//! tasks, and render the result. The MCP adapter in main.rs calls it in-process; the daemon
//! calls it for IPC and HTTP callers.

use crate::keys;
use crate::output::{self, Cleaner, Truncate, Window, WindowBuilder, WindowSpec};
use crate::process;
use crate::session::{ProbeWarning, ReadState, Reader, Session, TaskEntry, parse_handle};
use crate::tasks::{self, EVICTED_TRANSCRIPT, ScriptArgs, Snapshot, StartArgs, Task, Tasks};
use crate::wait::{self, WaitEnd};
use crate::{codemode, conpty, files, images};
use base64::Engine;
use rmcp::model::{CallToolResult, ContentBlock};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore, watch};
use tokio_util::sync::CancellationToken;

const MAX_WAIT_MS: u64 = 240_000;
const KILL_WAIT: Duration = Duration::from_secs(5);
const MAX_INPUT_BYTES: usize = 16 * 1024;
const MAX_START_INPUT_BYTES: usize = 1 << 20;
const DEFAULT_KEY_DELAY_MS: u64 = 150;
const MAX_KEY_DELAY_MS: u64 = 2_000;
/// Longest pause the keys of one poll add up to before its wait begins.
const MAX_KEY_PAUSE_MS: u64 = 30_000;
const DEFAULT_MAX_BYTES: u64 = 16 * 1024;
const DEFAULT_NESTED_MAX_BYTES: u64 = 4 << 20;
const MAX_BYTES_RANGE: std::ops::RangeInclusive<u64> = 100..=4 << 20;
const DEFAULT_LIST_LIMIT: u64 = 20;
const LIST_LIMIT_RANGE: std::ops::RangeInclusive<u64> = 1..=500;
/// Screen reads only need stream decoding diagnostics and the updated cursor.
const MIN_BODY_BUDGET: usize = 256;
/// Largest background footer.
const FOOTER_LIMIT: usize = 512;
const NEW_READER_NOTE: &str = " · new reader";

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Start,
    Poll,
    Kill,
    List,
    Transcript,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ListScope {
    #[default]
    Session,
    All,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ListState {
    Active,
    Finished,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Request {
    /// start runs bash. poll accepts input, waits and returns unseen output. kill ends the tree and returns unseen output. list shows tasks. transcript returns new PTY scrollback and the current screen.
    pub action: Action,
    /// start: the bash command line. NUL bytes (U+0000) are rejected before execution.
    #[serde(skip_serializing_if = "Option::is_none")]
    command: Option<String>,
    /// start: working directory. Omit for the working directory the session started in.
    #[serde(skip_serializing_if = "Option::is_none")]
    cwd: Option<String>,
    /// start: run inside a pseudo-terminal (default false).
    #[serde(skip_serializing_if = "Option::is_none")]
    pty: Option<bool>,
    /// start: run bash -lc to source login profiles (default false: bash --noprofile --norc -c with the session's login-environment snapshot).
    #[serde(skip_serializing_if = "Option::is_none")]
    login_shell: Option<bool>,
    /// start: kill the entire tree, including kept processes, this many ms after launch. Zero or omitted disables it.
    #[serde(skip_serializing_if = "Option::is_none")]
    kill_after_ms: Option<u64>,
    /// start: keep descendants after bash exits until kill, killAfterMs or owner shutdown (session or daemon). By default they end with bash.
    #[serde(skip_serializing_if = "Option::is_none")]
    keep_tree: Option<bool>,
    /// poll, kill, transcript: a reader handle (<taskId>.<reader>) to continue, or a bare task ID to attach a new reader from the retained log's beginning.
    #[serde(skip_serializing_if = "Option::is_none")]
    task_id: Option<String>,
    /// start, pipe only: text written to stdin, up to 1 MiB; stdin closes after it unless eof is false. poll: at most 16 KiB written to stdin before keys and waiting. Write failures are reported; terminal line discipline can transform or discard PTY input, which is warned separately.
    #[serde(skip_serializing_if = "Option::is_none")]
    input: Option<String>,
    /// poll, PTY only: keys pressed after `input` is written, in array order, e.g. ["Enter"] or ["Escape", ":", "q", "Enter"]. One key per item, named as in tmux send-keys: Enter, Tab, BTab, Escape, Space, BSpace, Up, Down, Left, Right, Home, End, PageUp/PgUp/PPage, PageDown/PgDn/NPage, Insert/IC, Delete/DC, F1-F12, KP0-KP9, KP/, KP*, KP-, KP+, KP., KPEnter, [NUL]-[US] for C0 controls, 0xHH for a code point, or one character; prefixes C- (Ctrl), M- (Meta/Alt), S- (Shift) combine, as in C-c, M-x, C-M-a, S-Up, and ^c means C-c. Named keys ignore case; single characters retain their case. Arrow and keypad keys follow the program's cursor and keypad modes. An unknown name, or a modifier the key cannot carry (C-Enter, C-S-a) in the program's mode when the poll starts, is an error before anything is sent.
    #[serde(skip_serializing_if = "Option::is_none")]
    keys: Option<Vec<String>>,
    /// poll: delay before each key, including the first after input, 0-2000 ms (default 150). Zero sends input and keys together. To separate Escape from the next key, exceed the program's escape timeout (often 500 ms).
    #[schemars(range(min = 0, max = 2000))]
    #[serde(skip_serializing_if = "Option::is_none")]
    key_delay_ms: Option<u64>,
    /// start, pipe only: false keeps stdin open for poll input (default: stdin closes after input, and is empty without it). poll: true closes stdin after input.
    #[serde(skip_serializing_if = "Option::is_none")]
    eof: Option<bool>,
    /// start, poll: wait up to 240000 ms per call. Poll again for longer. Ignored by kill and transcript.
    #[schemars(range(min = 0, max = 240000))]
    #[serde(skip_serializing_if = "Option::is_none")]
    wait_ms: Option<u64>,
    /// start, poll: any match returns; the task continues. Text conditions take 1-16 case-sensitive literals, 1-4096 bytes each, without line breaks. waitMs defaults to 30000.
    #[serde(skip_serializing_if = "Option::is_none")]
    return_when: Option<wait::ReturnWhen>,
    /// Window placement for all actions. Default head_tail, tail for transcript, head for nested.
    #[serde(skip_serializing_if = "Option::is_none")]
    truncate: Option<Truncate>,
    /// Body budget in UTF-8 bytes, 100-4194304 (default 16384; nested 4194304). Status, footer, errors, and warnings stay outside it.
    #[schemars(range(min = 100, max = 4194304))]
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
    /// list: "session" for tasks this session started or addressed (default), or "all" for every task in this server.
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<ListScope>,
    /// list: "active" for tasks whose root runs or that hold kept processes, or "finished" for the rest. Omit for both.
    #[serde(skip_serializing_if = "Option::is_none")]
    state: Option<ListState>,
    /// list: maximum tasks to return, newest first, 1-500 (default 20).
    #[schemars(range(min = 1, max = 500))]
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<u64>,
}

/// Output options shared by start and poll.
pub(crate) struct View {
    nested: bool,
    owned_by_script: bool,
    truncate: Truncate,
    max_bytes: usize,
    raw: bool,
    encoding: Option<&'static encoding_rs::Encoding>,
    screen: bool,
    /// The call set `returnWhen`.
    conditions: bool,
}

/// Effective presence of an optional boolean flag in applicability and conflict checks:
/// `false` and `None` count as absent where the parameter is inapplicable.
fn flag(value: Option<bool>) -> bool {
    value == Some(true)
}

pub(crate) fn normalize_request(request: &mut Request) {
    if request.keys.as_ref().is_some_and(Vec::is_empty) {
        request.keys = None;
        request.key_delay_ms = None;
    }
}

pub(crate) fn validate(request: &Request, nested: bool) -> Result<View, String> {
    let present = [
        ("command", request.command.is_some()),
        ("cwd", request.cwd.is_some()),
        ("pty", flag(request.pty)),
        ("loginShell", flag(request.login_shell)),
        ("killAfterMs", request.kill_after_ms.is_some()),
        ("keepTree", flag(request.keep_tree)),
        ("taskId", request.task_id.is_some()),
        ("input", request.input.is_some()),
        ("eof", flag(request.eof)),
        ("waitMs", request.wait_ms.is_some()),
        ("truncate", request.truncate.is_some()),
        ("maxBytes", request.max_bytes.is_some()),
        ("raw", flag(request.raw)),
        ("encoding", request.encoding.is_some()),
        ("keys", request.keys.is_some()),
        ("keyDelayMs", request.key_delay_ms.is_some()),
        ("screen", flag(request.screen)),
        ("cols", request.cols.is_some()),
        ("rows", request.rows.is_some()),
        ("returnWhen", request.return_when.is_some()),
        ("scope", request.scope.is_some()),
        ("state", request.state.is_some()),
        ("limit", request.limit.is_some()),
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
            "input",
            "eof",
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
        Action::Kill => vec!["taskId", "waitMs", "truncate", "maxBytes"],
        Action::List => vec!["truncate", "maxBytes", "scope", "state", "limit"],
        Action::Transcript => vec!["taskId", "waitMs", "truncate", "maxBytes"],
    };
    for (name, set) in present {
        if set && !allowed.contains(&name) {
            return Err(format!(
                "`{name}` does not apply to action {}; remove it.",
                action_name(request.action)
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
        Action::Start
            if request
                .command
                .as_deref()
                .is_some_and(|command| command.contains('\0')) =>
        {
            return Err("`command` contains a NUL byte (U+0000); no command was started.".into());
        }
        Action::Poll | Action::Kill | Action::Transcript if request.task_id.is_none() => {
            return Err(format!(
                "{} needs `taskId`; use action list to find it.",
                action_name(request.action)
            ));
        }
        _ => {}
    }
    if request.action == Action::Start {
        if flag(request.pty) {
            if request.input.is_some() {
                return Err(
                    "`input` on start applies to pipe tasks only; for PTY tasks, send input with poll."
                        .into(),
                );
            }
            if flag(request.eof) {
                return Err(
                    "`eof` does not apply to PTY tasks; a PTY has no stdin half-close.".into(),
                );
            }
        } else if request
            .input
            .as_ref()
            .is_some_and(|input| input.len() > MAX_START_INPUT_BYTES)
        {
            return Err(
                "input on start exceeds 1 MiB; write larger data to a file and redirect stdin."
                    .into(),
            );
        }
    }
    let screen = flag(request.screen);
    if screen {
        if let Some(name) = [
            ("truncate", request.truncate.is_some()),
            ("raw", flag(request.raw)),
            ("encoding", request.encoding.is_some()),
        ]
        .into_iter()
        .find_map(|(name, set)| set.then_some(name))
        {
            return Err(format!(
                "`{name}` does not apply with screen; remove one of them."
            ));
        }
        if request.action == Action::Start && !flag(request.pty) {
            return Err("screen needs a PTY task; start with pty: true.".into());
        }
    }
    if let Some(when) = &request.return_when {
        when.validate()?;
        if let Some(name) = when.screen_condition()
            && request.action == Action::Start
            && !flag(request.pty)
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
    if !nested && let Some(wait) = request.wait_ms.filter(|&wait| wait > MAX_WAIT_MS) {
        return Err(format!("waitMs {wait} is out of range 0-{MAX_WAIT_MS}."));
    }
    // A size belongs to a PTY task's terminal; a pipe task's output is a byte stream. `start`
    // reads an omitted `pty` as false, so a size there needs `pty: true`.
    if let Some(name) = [("cols", request.cols), ("rows", request.rows)]
        .into_iter()
        .find_map(|(name, set)| set.is_some().then_some(name))
    {
        let applies = match request.action {
            Action::Start => flag(request.pty),
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
    if let Some(bytes) = request.max_bytes
        && !MAX_BYTES_RANGE.contains(&bytes)
    {
        return Err(format!("maxBytes {bytes} is out of range 100-4194304."));
    }
    if let Some(limit) = request.limit
        && !LIST_LIMIT_RANGE.contains(&limit)
    {
        return Err(format!("limit {limit} is out of range 1-500."));
    }
    if request.action == Action::Poll
        && request
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
    let default_truncate = if request.action == Action::Transcript {
        Truncate::Tail
    } else if nested {
        Truncate::Head
    } else {
        Truncate::HeadTail
    };
    let default_max_bytes = if nested {
        DEFAULT_NESTED_MAX_BYTES
    } else {
        DEFAULT_MAX_BYTES
    };
    Ok(View {
        nested,
        owned_by_script: false,
        truncate: request.truncate.unwrap_or(default_truncate),
        max_bytes: request.max_bytes.unwrap_or(default_max_bytes) as usize,
        raw: flag(request.raw),
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
    pub(crate) fn effective(self) -> Self {
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

/// A local daemon call obtains one render slot on demand and keeps it until encoding.
pub(crate) struct RenderSlot<'a> {
    pool: &'a Arc<Semaphore>,
    permit: OnceCell<Arc<OwnedSemaphorePermit>>,
}

impl<'a> RenderSlot<'a> {
    pub(crate) fn new(pool: &'a Arc<Semaphore>) -> Self {
        Self {
            pool,
            permit: OnceCell::new(),
        }
    }

    fn try_hold(&self) -> Option<Arc<OwnedSemaphorePermit>> {
        if let Some(permit) = self.permit.get() {
            return Some(Arc::clone(permit));
        }
        let permit = Arc::new(Arc::clone(self.pool).try_acquire_owned().ok()?);
        self.permit
            .set(Arc::clone(&permit))
            .expect("one renderer per call");
        Some(permit)
    }

    pub(crate) async fn hold(&self) -> Arc<OwnedSemaphorePermit> {
        Arc::clone(
            self.permit
                .get_or_init(|| async {
                    Arc::new(
                        Arc::clone(self.pool)
                            .acquire_owned()
                            .await
                            .expect("render pool never closes"),
                    )
                })
                .await,
        )
    }
}

/// What a call carries besides its arguments: the session it reads for, how to render, and the
/// ways the caller can stop it or watch it.
pub struct Call<'a> {
    pub session: &'a Arc<Session>,
    pub launch: Option<&'a Arc<crate::session::LaunchContext>>,
    pub render: Render,
    pub cancel: CancellationToken,
    /// The call replaces this with its latest progress message; a forwarder sends them.
    pub progress: Option<watch::Sender<Option<(f64, String)>>>,
    /// The daemon's call ID, kept on a task a start creates so `list` can name the call that
    /// started it. The stdio adapter has no call IDs, so it passes `None`.
    pub call_id: Option<u64>,
    pub nested: bool,
    /// Records a nested start before its caller's wait can be cancelled.
    pub started: Option<&'a codemode::ScriptHandle>,
    pub(crate) render_slot: Option<&'a RenderSlot<'a>>,
}

impl<'a> Call<'a> {
    async fn render_permit(&self) -> Result<Option<Arc<OwnedSemaphorePermit>>, String> {
        let Some(slot) = self.render_slot else {
            return Ok(None);
        };
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err("Cancelled while reading output; use poll to continue.".into()),
            permit = slot.hold() => Ok(Some(permit)),
        }
    }

    async fn task_render_permit(
        &self,
        action: Action,
        task: &Task,
        reader: &Reader,
    ) -> Result<Option<Arc<OwnedSemaphorePermit>>, String> {
        let Some(slot) = self.render_slot else {
            return Ok(None);
        };
        if action == Action::Kill {
            return self.render_permit().await;
        }
        // Do not wait on ordinary output while holding a reader that kill may need.
        slot.try_hold().map(Some).ok_or_else(|| {
            let recovery = match action {
                Action::Poll => "Poll the same task for output only. Do not resend input, keys, or resize.",
                Action::Transcript => "Repeat transcript for the same task or read its log.",
                _ => "Poll the task or read its log. Do not replay the original operation.",
            };
            format!(
                "The task exists and may still be running. Its output was not read because the daemon is rendering other results.\ntaskId: {}\nlogPath: {}\n{recovery}",
                reader.handle(&task.id),
                files::display_path(&task.log_path),
            )
        })
    }

    fn launch(&self) -> &'a Arc<crate::session::LaunchContext> {
        self.launch.unwrap_or(&self.session.launch)
    }
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
        // Entries of tasks that have retired are dropped here, so a call that never reaches the
        // background footer cannot keep them alive.
        self.tasks.prune_entries(call.session);
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
    pub async fn call_json(&self, arguments: Value, call: Call<'_>) -> CallToolResult {
        match serde_json::from_value::<Request>(arguments) {
            Ok(request) => self.call(request, call).await,
            Err(error) => CallToolResult::error(vec![ContentBlock::text(format!(
                "failed to deserialize parameters: {error}"
            ))]),
        }
    }

    pub async fn codemode(
        self: &Arc<Self>,
        request: codemode::Request,
        mut call: Call<'_>,
        transfer: bool,
    ) -> CallToolResult {
        self.tasks.prune_entries(call.session);
        call.render = call.render.effective();
        let result = match self.start_script(request, &call, transfer).await {
            Ok(result) => result,
            Err(message) => codemode_error(&message),
        };
        suppress(result, call.render)
    }

    async fn start_script(
        self: &Arc<Self>,
        request: codemode::Request,
        call: &Call<'_>,
        transfer: bool,
    ) -> Result<CallToolResult, String> {
        let base_launch = call.launch();
        let (start, resolved_cwd) = codemode::validate(&request, base_launch)?;
        let view = validate(&start, call.nested)?;
        let launch = match resolved_cwd {
            None => Arc::clone(base_launch),
            Some(dir) => Arc::new(base_launch.with_cwd(dir)),
        };
        let args = ScriptArgs {
            code: request.code,
            launch,
            kill_after_ms: start.kill_after_ms,
            transfer,
            call_id: call.call_id,
            service: Arc::clone(self),
        };
        let tasks = Arc::clone(&self.tasks);
        let session = Arc::clone(call.session);
        let key =
            tasks::random_chars(4).map_err(|error| format!("Cannot name the reader: {error}"))?;
        let started = tokio::task::spawn_blocking(move || tasks.start_script(args, &session, key))
            .await
            .map_err(|error| format!("Internal failure while starting: {error}."))??;
        self.wait_started(
            started,
            None,
            Some(Duration::from_millis(start.wait_ms.unwrap_or(30_000))),
            start.return_when.as_ref(),
            view,
            call,
        )
        .await
    }

    async fn wait_started(
        &self,
        started: tasks::Started,
        startup_warning: Option<ProbeWarning>,
        wait_duration: Option<Duration>,
        when: Option<&wait::ReturnWhen>,
        view: View,
        call: &Call<'_>,
    ) -> Result<CallToolResult, String> {
        let (task, entry, reader) = started;
        let state = read_gate(&reader, &call.cancel).await?;
        let mark = wait::mark(&task, &reader, &state, when)?;
        let encoding = view.encoding.unwrap_or(task.encoding);
        let ended = wait::wait(
            &task,
            wait_duration,
            when,
            mark,
            encoding,
            &call.cancel,
            call.progress.as_ref(),
        )
        .await?;
        read_result(
            &call.cancel,
            self.window_result(
                call,
                call.session,
                Action::Start,
                task,
                entry,
                reader,
                state,
                view,
                Some(ended),
                startup_warning,
            ),
        )
        .await
    }

    async fn handle(
        &self,
        mut request: Request,
        call: &Call<'_>,
    ) -> Result<CallToolResult, String> {
        normalize_request(&mut request);
        if request.action == Action::Poll
            && let Ok((task, _)) = self.task(request.task_id.as_deref())
            && task.is_script()
            && (request.input.is_some()
                || flag(request.eof)
                || request.keys.is_some()
                || request.key_delay_ms.is_some()
                || request.cols.is_some()
                || request.rows.is_some()
                || flag(request.screen)
                || flag(request.raw)
                || request.encoding.is_some()
                || request
                    .return_when
                    .as_ref()
                    .and_then(|when| when.screen_condition())
                    .is_some())
        {
            return Err("A codemode task takes no input, eof, keys, resize, screen, raw, or encoding; poll it with waitMs and returnWhen.".into());
        }
        let mut view = validate(&request, call.nested)?;
        let session = Arc::clone(call.session);
        match request.action {
            Action::List => {
                let _permit = call.render_permit().await?;
                Ok(self.list(
                    &session,
                    &view,
                    request.scope.unwrap_or_default(),
                    request.state,
                    request.limit.unwrap_or(DEFAULT_LIST_LIMIT) as usize,
                ))
            }
            Action::Start => {
                let base_launch = call.launch();
                let cwd = match request.cwd {
                    None => base_launch.cwd.clone(),
                    Some(dir) => validate_cwd(&dir, base_launch)?,
                };
                let args = StartArgs {
                    command: request.command.unwrap_or_default(),
                    cwd,
                    pty: flag(request.pty),
                    cols: request.cols.unwrap_or(process::PTY_COLS),
                    rows: request.rows.unwrap_or(process::PTY_ROWS),
                    login: request.login_shell.unwrap_or(false),
                    keep: flag(request.keep_tree),
                    input: request.input,
                    eof: request.eof,
                    encoding: view.encoding.unwrap_or(encoding_rs::UTF_8),
                    kill_after_ms: request.kill_after_ms,
                    owner: session.label.clone(),
                    started_by: call.call_id.map(|id| (session.id.clone(), id)),
                    launch: Arc::clone(base_launch),
                };
                let tasks = Arc::clone(&self.tasks);
                // The session takes the task even when this request is cancelled meanwhile, so
                // its own task shows in its background footer.
                let starting = Arc::clone(&session);
                let key = tasks::random_chars(4)
                    .map_err(|error| format!("Cannot name the reader: {error}"))?;
                if call.nested && call.cancel.is_cancelled() {
                    return Err("Cancelled before the command was started.".into());
                }
                let ((task, entry, task_reader), startup_warning) =
                    tokio::task::spawn_blocking(move || tasks.start(args, &starting, key))
                        .await
                        .map_err(|error| format!("Internal failure while starting: {error}."))??;
                if let Some(script) = call.started {
                    script.record_started(task_reader.handle(&task.id));
                    view.owned_by_script = true;
                }
                let wait = match request.wait_ms {
                    Some(ms) => Some(Duration::from_millis(ms)),
                    None if request.return_when.is_some() => Some(Duration::from_millis(30_000)),
                    None if call.nested => None,
                    None => Some(Duration::from_millis(30_000)),
                };
                self.wait_started(
                    (task, entry, task_reader),
                    startup_warning,
                    wait,
                    request.return_when.as_ref(),
                    view,
                    call,
                )
                .await
            }
            Action::Poll => {
                let (task, key) = self.task(request.task_id.as_deref())?;
                view.owned_by_script = call.started.is_some_and(|script| script.owns(&task.id));
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
                if flag(request.eof) && task.pty {
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
                let input = request.input.as_deref().map(str::as_bytes);
                let input_bytes = input.unwrap_or_default();
                let names = request.keys.as_deref().unwrap_or_default();
                let parsed = names
                    .iter()
                    .map(|name| keys::parse(name))
                    .collect::<Result<Vec<_>, _>>()?;
                let entry = session.entry(&task);
                let task_reader = entry.reader(key)?;
                let state = read_gate(&task_reader, &call.cancel).await?;
                // One poll's resize, input, keys, and eof go out as one sequence, and the wait's
                // starting mark is taken after the gate, so screenAppears compares against the
                // screen this caller's resize and input change.
                let sends = request.input.is_some()
                    || request.keys.is_some()
                    || flag(request.eof)
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
                let encode = |task: &Task| -> Result<Vec<u8>, String> {
                    let mut data = input_bytes.to_vec();
                    if !names.is_empty() {
                        let modes = task.modes()?;
                        for (name, key) in names.iter().zip(parsed.iter().copied()) {
                            data.extend(keys::encode(name, key, modes)?);
                        }
                    }
                    if data.len() > MAX_INPUT_BYTES {
                        return Err(
                            "input and keys together exceed 16 KiB; send them in smaller pieces."
                                .into(),
                        );
                    }
                    Ok(data)
                };
                let mut data = encode(&task)?;
                let mark = wait::mark(&task, &task_reader, &state, when)?;
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
                        data = encode(&task)?;
                    }
                    let burst = (input.is_some() || !data.is_empty()).then_some(data.as_slice());
                    task.send(burst, flag(request.eof), &call.cancel).await?;
                } else {
                    task.send(input, flag(request.eof), &call.cancel).await?;
                    press(&task, input_bytes, names, parsed, delay, &call.cancel).await?;
                }
                drop(gate);
                let interacts = request.input.is_some() || request.keys.is_some() || axes.is_some();
                let wait = match request.wait_ms {
                    Some(ms) => Some(Duration::from_millis(ms)),
                    None if when.is_some() => Some(Duration::from_millis(30_000)),
                    None if interacts => Some(Duration::from_millis(2_000)),
                    None if call.nested => None,
                    None => Some(Duration::from_millis(30_000)),
                };
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
                read_result(
                    &call.cancel,
                    self.window_result(
                        call,
                        &session,
                        Action::Poll,
                        task,
                        entry,
                        task_reader,
                        state,
                        view,
                        Some(ended),
                        None,
                    ),
                )
                .await
            }
            Action::Kill => {
                let (task, key) = self.task(request.task_id.as_deref())?;
                view.owned_by_script = call.started.is_some_and(|script| script.owns(&task.id));
                let entry = session.entry(&task);
                let task_reader = entry.reader(key)?;
                // Request termination before acquiring the reader gate so a long poll on the
                // same reader wakes immediately and cannot delay the kill.
                task.kill().map_err(|error| {
                    format!(
                        "Cannot terminate the process tree of {}: {error}. Poll to see its state, or retry kill.",
                        task.id
                    )
                })?;
                let (mut done, mut tree_done) = (task.done(), task.tree_done());
                let mut activity = task.activity();
                let finished = async {
                    let _ = done.wait_for(|done| *done).await;
                    let _ = tree_done.wait_for(|done| *done).await;
                    while !task.output_closed() {
                        let _ = activity.changed().await;
                    }
                };
                tokio::select! {
                    biased;
                    _ = call.cancel.cancelled() => {
                        return Err("Cancelled while ending the task; use poll to see its state.".into());
                    }
                    _ = tokio::time::timeout(KILL_WAIT, finished) => {}
                }
                let state = read_gate(&task_reader, &call.cancel).await?;
                read_result(
                    &call.cancel,
                    self.window_result(
                        call,
                        &session,
                        Action::Kill,
                        task,
                        entry,
                        task_reader,
                        state,
                        view,
                        None,
                        None,
                    ),
                )
                .await
            }
            Action::Transcript => {
                let (task, key) = self.task(request.task_id.as_deref())?;
                view.owned_by_script = call.started.is_some_and(|script| script.owns(&task.id));
                if task.is_script() {
                    return Err(
                        "A codemode task has no terminal or transcript; poll returns its output."
                            .into(),
                    );
                }
                if !task.pty {
                    return Err("transcript applies to PTY tasks only; poll returns a pipe task's output, and its log holds every byte.".into());
                }
                if task.encoding != encoding_rs::UTF_8 {
                    return Err(
                        "transcript needs UTF-8 output; this task was started with an encoding."
                            .into(),
                    );
                }
                let entry = session.entry(&task);
                let task_reader = entry.reader(key)?;
                let state = read_gate(&task_reader, &call.cancel).await?;
                read_result(
                    &call.cancel,
                    self.transcript_result(call, &session, task, entry, task_reader, state, view),
                )
                .await
            }
        }
    }

    fn task<'a>(&self, handle: Option<&'a str>) -> Result<(Arc<Task>, Option<&'a str>), String> {
        let (id, key) = parse_handle(handle.unwrap_or_default())?;
        if let Some(task) = self.tasks.find(id) {
            return Ok((task, key));
        }
        // A well-formed ID whose epoch differs came from an earlier server or daemon.
        let epoch_differs = tasks::is_valid_task_id(id)
            && id
                .split_once('-')
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

    fn background<'a>(
        &self,
        session: &'a Session,
        id: &str,
        nested: bool,
    ) -> (
        Option<(String, Vec<tasks::Shown>)>,
        crate::session::OutsideClaim<'a>,
    ) {
        if nested {
            (None, crate::session::OutsideClaim::none())
        } else {
            self.tasks.background(session, id, FOOTER_LIMIT)
        }
    }

    fn list(
        &self,
        session: &Session,
        view: &View,
        scope: ListScope,
        state: Option<ListState>,
        limit: usize,
    ) -> CallToolResult {
        let active_filter = state.map(|state| state == ListState::Active);
        let (total_count, selected) =
            self.tasks
                .listed(session, scope == ListScope::All, active_filter, limit);
        let mut lines = Vec::with_capacity(selected.len());
        let mut entries = Vec::with_capacity(selected.len());
        let mut warnings = Vec::new();
        for (task, snapshot) in &selected {
            warnings.extend(
                task_warnings(snapshot)
                    .into_iter()
                    .map(|warning| format!("{}: {warning}", task.id)),
            );
            let command: String = task.command.chars().take(120).collect();
            let code = snapshot
                .exit_code
                .map_or(String::new(), |code| format!(" {code}"));
            // One read, so the line and the fields name the same size.
            let size = task.pty.then(|| task.size());
            let log_path = files::display_path(&task.log_path);
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
                "kind": if task.is_script() { "codemode" } else { "bash" },
                "taskId": task.id, "state": snapshot.state, "exitCode": snapshot.exit_code, "pty": task.pty,
                "captureError": snapshot.capture_error, "logError": snapshot.log_error,
                "inputWarning": snapshot.input_warning, "inputError": snapshot.input_error,
                "droppedBytes": snapshot.dropped, "logEvicted": snapshot.evicted,
                "outputOpen": snapshot.output_open, "outputIncomplete": output_incomplete(snapshot),
                "warnings": task_warnings(snapshot),
                "cols": size.map(|(cols, _)| cols), "rows": size.map(|(_, rows)| rows),
                "elapsedMs": snapshot.elapsed.as_millis() as u64, "command": command, "logPath": log_path,
                "lifetimeExpired": snapshot.expired, "forcedEnd": snapshot.forced_end,
                "keepTree": task.keep, "keptRunning": snapshot.kept,
                "held": members_json(&snapshot.held),
                "leftovers": members_json(&snapshot.leftovers), "killError": snapshot.kill_error,
                "owner": task.owner,
                "session": task.started_by.as_ref().map(|(session, _)| session),
                "callId": task.started_by.as_ref().map(|(_, call)| *call),
            }));
        }
        let showing_note = (total_count > selected.len()).then(|| {
            format!(
                "Showing {} of {total_count} tasks; raise limit to see more.",
                selected.len()
            )
        });
        let structured = json!({
            "ok": true, "action": "list", "tasks": entries, "totalCount": total_count, "warnings": warnings,
        });
        let mut suffix = warning_text(&structured["warnings"]);
        if let Some(note) = showing_note {
            suffix.push('\n');
            suffix.push_str(&note);
        }
        let (kept, text) = if lines.is_empty() {
            (Vec::new(), "No tasks.".to_string())
        } else {
            fit_lines(&lines, view.truncate, view.max_bytes)
        };
        // Direct calls deliver the text window; nested calls deliver every selected task.
        let delivered = if view.nested {
            (0..selected.len()).collect()
        } else {
            kept
        };
        for index in delivered {
            let (task, snapshot) = &selected[index];
            if let Some(entry) = session.existing(task) {
                entry.mark_reported(snapshot);
                entry.mark_keeps_reported(snapshot);
            }
        }
        success(text, structured, &suffix)
    }
}

/// Joins the whole `lines` that fit `budget` bytes, picked from the ends `truncate` names, with
/// one marker line where lines were left out. Returns the indexes of the lines shown and the
/// text.
fn fit_lines(lines: &[String], truncate: Truncate, budget: usize) -> (Vec<usize>, String) {
    if lines.iter().map(|line| line.len() + 1).sum::<usize>() <= budget + 1 {
        return ((0..lines.len()).collect(), lines.join("\n"));
    }
    let marker =
        |omitted: usize| format!("... [{omitted} tasks omitted; raise maxBytes to see them] ...");
    let mut room = budget.saturating_sub(marker(lines.len()).len());
    let mut head = 0;
    let mut tail = lines.len();
    let mut want_head = truncate != Truncate::Tail;
    let mut want_tail = truncate != Truncate::Head;
    while head < tail && (want_head || want_tail) {
        if want_head {
            let cost = lines[head].len() + 1;
            if cost <= room {
                head += 1;
                room -= cost;
            } else {
                want_head = false;
            }
        }
        if want_tail && head < tail {
            let cost = lines[tail - 1].len() + 1;
            if cost <= room {
                tail -= 1;
                room -= cost;
            } else {
                want_tail = false;
            }
        }
    }
    let mut note = marker(tail - head);
    output::cut_to(&mut note, budget + 1);
    let text = lines[..head]
        .iter()
        .chain([&note])
        .chain(&lines[tail..])
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join("\n");
    ((0..head).chain(tail..lines.len()).collect(), text)
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
        if index > 0 || !input.is_empty() {
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
        task.send(Some(&bytes), false, cancel).await.map_err(sent)?;
    }
    Ok(())
}

/// A handle's read gate, held by queued waits and blocking reads even after cancellation.
async fn read_gate(
    reader: &Reader,
    cancel: &CancellationToken,
) -> Result<tokio::sync::OwnedMutexGuard<ReadState>, String> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            Err("Cancelled before the output was read; the task keeps running.".into())
        }
        state = reader.read_gate().lock_owned() => Ok(state),
    }
}

/// A blocking read owns the gate, but only a delivered result commits its working copy.
struct StagedRead {
    guard: tokio::sync::OwnedMutexGuard<ReadState>,
    state: ReadState,
}

impl StagedRead {
    fn new(guard: tokio::sync::OwnedMutexGuard<ReadState>) -> Self {
        let state = guard.clone();
        Self { guard, state }
    }

    fn commit(mut self) -> tokio::sync::OwnedMutexGuard<ReadState> {
        *self.guard = self.state;
        self.guard
    }
}

/// A cancelled render drops its staged state while its blocking read still owns the gate.
async fn read_result(
    cancel: &CancellationToken,
    result: impl std::future::Future<Output = Result<CallToolResult, String>>,
) -> Result<CallToolResult, String> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err("Cancelled while reading output; use poll to continue.".into()),
        result = result => result,
    }
}

/// Marks what the result showed of the task and of the footer's tasks reported, once the
/// result is built.
#[allow(clippy::too_many_arguments)]
fn report(
    session: &Session,
    entry: &TaskEntry,
    reader: &Reader,
    shown: &Snapshot,
    background: Option<(String, Vec<tasks::Shown>)>,
    claim: crate::session::OutsideClaim<'_>,
    nested: bool,
    owned_by_script: bool,
) {
    reader.mark_reported(shown);
    if nested {
        if owned_by_script {
            entry.mark_reported(shown);
        }
        return;
    }
    entry.mark_reported(shown);
    for (task, shown) in background.map(|(_, shown)| shown).unwrap_or_default() {
        if let Some(entry) = session.existing(&task) {
            entry.mark_reported(&shown);
            entry.mark_keeps_reported(&shown);
        }
    }
    claim.consume();
}

fn kill_pending_note(task: &Task, snapshot: &Snapshot) -> Option<&'static str> {
    if snapshot.state == "running" || snapshot.kept || !*task.tree_done().borrow() {
        Some(if task.is_script() {
            "the script did not end within 5 s; poll later to confirm"
        } else {
            "the tree did not exit within 5 s; poll later to confirm"
        })
    } else if snapshot.output_open {
        Some("output did not close within 5 s; poll later to confirm")
    } else {
        None
    }
}

fn needs_log_in_status(snapshot: &Snapshot, cut_lines: u64, omitted: Option<(u64, u64)>) -> bool {
    snapshot.state == "running"
        || snapshot.kept
        || snapshot.output_open
        || output_incomplete(snapshot)
        || cut_lines > 0
        || omitted.is_some()
}

impl Service {
    #[allow(clippy::too_many_arguments)]
    async fn window_result(
        &self,
        call: &Call<'_>,
        session: &Session,
        action: Action,
        task: Arc<Task>,
        entry: Arc<TaskEntry>,
        task_reader: Arc<Reader>,
        state: tokio::sync::OwnedMutexGuard<ReadState>,
        view: View,
        ended: Option<WaitEnd>,
        startup_warning: Option<ProbeWarning>,
    ) -> Result<CallToolResult, String> {
        let permit = call.task_render_permit(action, &task, &task_reader).await?;
        let render = call.render;
        if view.screen {
            return self
                .screen_result(
                    render,
                    session,
                    action,
                    task,
                    entry,
                    task_reader,
                    state,
                    view,
                    ended.expect("screen applies to start and poll"),
                    startup_warning,
                    permit,
                )
                .await;
        }
        let reader = Arc::clone(&task);
        let encoding = view.encoding.unwrap_or(task.encoding);
        // The state is taken before the read: a finished state means output reached EOF (or
        // stayed open, which the status line notes), so the window below holds all of it.
        let before = task.snapshot_held();
        let clean = !task.is_script();
        let keep_nul = view.nested;
        let budget = view.max_bytes;
        let (window, images, read, _permit) = tokio::task::spawn_blocking(move || {
            let mut read = StagedRead::new(state);
            let start = read.state.cursor;
            let window = reader.read_window(
                &mut read.state,
                view.truncate,
                budget,
                view.raw,
                clean,
                keep_nul,
                encoding,
                None,
            );
            let images = if window.is_ok() {
                read_images(&reader, start, read.state.cursor, read.state.images_seen)
            } else {
                Ok(ImageRead::default())
            };
            if let Ok(images) = &images {
                read.state.images_seen = images.seen;
            }
            (window, images, read, permit)
        })
        .await
        .map_err(|error| format!("Internal failure while reading output: {error}."))?;
        let window = window.map_err(|error| format!("Cannot read the task log: {error}."))?;
        let images = images?;
        // Log metadata, such as an eviction during the read, is taken after it.
        let after = task.snapshot();
        let snapshot = Snapshot {
            lines: after.lines,
            dropped: after.dropped,
            evicted: after.evicted,
            log_error: after.log_error,
            capture_error: after.capture_error,
            input_warning: after.input_warning,
            input_error: after.input_error,
            ..before
        };
        let script = script_report(&self.tasks, &task);
        let mut status_with_log =
            status_line(&task, &snapshot, &task_reader, true, script.as_ref());
        let mut status_no_log = status_line(&task, &snapshot, &task_reader, false, script.as_ref());
        let mut structured = task_json(action, &task, &snapshot, &task_reader);
        if let Some(ended) = &ended {
            let note = ended.note(view.conditions);
            status_with_log.push_str(&note);
            status_no_log.push_str(&note);
            structured["waitEndedBy"] = ended.json();
        } else if let Some(note) = kill_pending_note(&task, &snapshot) {
            structured["warnings"]
                .as_array_mut()
                .unwrap()
                .push(json!(note));
        }
        if let Some(report) = script {
            structured["codemode"] = report.value;
            structured["codemode"]["imagesAttached"] = json!(images.content.len());
            structured["codemode"]["imagesNotAttached"] = json!(images.omitted);
            structured["warnings"]
                .as_array_mut()
                .unwrap()
                .extend(images.warnings.into_iter().map(Value::String));
        }
        if let Some(warning) = startup_warning.and_then(ProbeWarning::deliver) {
            structured["warnings"]
                .as_array_mut()
                .unwrap()
                .push(Value::String(warning));
        }
        add_encoding_warning(&mut structured, &window);
        let warnings = warning_text(&structured["warnings"]);
        let (background, claim) = self.background(session, &task.id, view.nested);
        let footer = background
            .as_ref()
            .map_or(String::new(), |(line, _)| format!("\n{line}"));
        let text = self.layout(
            render,
            &window,
            status_with_log,
            status_no_log,
            &snapshot,
            task_reader.is_fresh(),
            &footer,
            view.max_bytes,
            "(no new output)",
            &mut structured,
        );
        let mut result = success(text, structured, &warnings);
        result.content.extend(images.content);
        let state = read.commit();
        task_reader.mark_delivered();
        report(
            session,
            &entry,
            &task_reader,
            &snapshot,
            background,
            claim,
            view.nested,
            view.owned_by_script,
        );
        drop(state);
        Ok(result)
    }

    /// A PTY task's transcript from the first line no earlier read showed: its final lines,
    /// then the pending ones, which the file also holds after the final lines until output
    /// changes them. The result shows the last lines by default, where a program's newest
    /// output is.
    #[allow(clippy::too_many_arguments)]
    async fn transcript_result(
        &self,
        call: &Call<'_>,
        session: &Session,
        task: Arc<Task>,
        entry: Arc<TaskEntry>,
        task_reader: Arc<Reader>,
        state: tokio::sync::OwnedMutexGuard<ReadState>,
        view: View,
    ) -> Result<CallToolResult, String> {
        let permit = call
            .task_render_permit(Action::Transcript, &task, &task_reader)
            .await?;
        let render = call.render;
        let snapshot = task.snapshot_held();
        let (reader, tasks) = (Arc::clone(&task), Arc::clone(&self.tasks));
        let first_permit = permit.clone();
        let (transcript, read, _first_permit) = tokio::task::spawn_blocking(move || {
            let read = StagedRead::new(state);
            let transcript = reader.transcript(&tasks, &read.state);
            (transcript, read, first_permit)
        })
        .await
        .map_err(|error| format!("Internal failure while reading: {error}."))?;
        let transcript = transcript?;
        let pending_lines = transcript.pending.matches('\n').count() as u64;
        let total = transcript.lines + pending_lines;
        let shown_transcript_path = files::display_path(&transcript.path);
        let transcript_note = format!(
            " · transcript {total} lines, final through {}; this read starts at line {} · {shown_transcript_path}",
            transcript.lines, transcript.first_line,
        );
        let mut status_with_log = status_line(&task, &snapshot, &task_reader, true, None);
        status_with_log.push_str(&transcript_note);
        let mut status_no_log = status_line(&task, &snapshot, &task_reader, false, None);
        status_no_log.push_str(&transcript_note);
        let spec = WindowSpec {
            truncate: view.truncate,
            budget: view.max_bytes,
            raw: false,
            clean: true,
            keep_nul: false,
            encoding: encoding_rs::UTF_8,
            source: "transcript",
        };
        let mut structured = task_json(Action::Transcript, &task, &snapshot, &task_reader);
        structured["transcriptPath"] = json!(shown_transcript_path);
        structured["transcriptLines"] = json!(total);
        structured["finalLines"] = json!(transcript.lines);
        structured["firstLine"] = json!(transcript.first_line);
        structured["transcriptIncomplete"] =
            json!(transcript.cut || transcript.stopped.is_some() || conpty::load().is_some());
        structured["transcriptError"] = json!(transcript.stopped);
        if transcript.cut {
            structured["warnings"].as_array_mut().unwrap().push(json!(
                "pending lines past the transcript's 64 MiB limit are left out"
            ));
        }
        if let Some(reason) = &transcript.stopped {
            structured["warnings"]
                .as_array_mut()
                .unwrap()
                .push(json!(format!(
                    "the transcript stops after line {}: {reason}",
                    transcript.lines
                )));
        }
        if let Some(warning) = conpty::load() {
            structured["warnings"]
                .as_array_mut()
                .unwrap()
                .push(json!(format!(
                    "{warning}, so transcript lines may be missing"
                )));
        }
        let logged = Arc::clone(&task);
        let (window, read, _permit) = tokio::task::spawn_blocking(move || {
            let mut read = read;
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
                    file.read_exact(&mut buffer[..want])?;
                    builder.push(&buffer[..want]);
                    remaining -= want as u64;
                }
                Ok(())
            }
            let mut cleaner = Cleaner::default();
            let mut builder = WindowBuilder::new(spec, &mut cleaner, transcript.first_line);
            let window = match read_into(
                &transcript.path,
                transcript.start,
                transcript.bytes,
                &mut builder,
            ) {
                Err(_) if logged.evicted() => Err(EVICTED_TRANSCRIPT.to_string()),
                Err(error) => Err(format!("Cannot read the transcript file: {error}.")),
                Ok(()) => {
                    builder.push(transcript.pending.as_bytes());
                    read.state.shown = (transcript.bytes, transcript.lines);
                    Ok(builder.finish())
                }
            };
            (window, read, permit)
        })
        .await
        .map_err(|error| format!("Internal failure while reading: {error}."))?;
        let window = window?;
        add_encoding_warning(&mut structured, &window);
        let warnings = warning_text(&structured["warnings"]);
        let (background, claim) = self.background(session, &task.id, view.nested);
        let footer = background
            .as_ref()
            .map_or(String::new(), |(line, _)| format!("\n{line}"));
        let text = self.layout(
            render,
            &window,
            status_with_log,
            status_no_log,
            &snapshot,
            task_reader.is_fresh(),
            &footer,
            view.max_bytes,
            "(no transcript lines since the last read)",
            &mut structured,
        );
        let result = success(text, structured, &warnings);
        let state = read.commit();
        task_reader.mark_delivered();
        report(
            session,
            &entry,
            &task_reader,
            &snapshot,
            background,
            claim,
            view.nested,
            view.owned_by_script,
        );
        drop(state);
        Ok(result)
    }

    /// Combines the already-budgeted `window` body with the status line and background footer,
    /// adds the window metadata to `structured`, and returns the text content.
    #[allow(clippy::too_many_arguments)]
    fn layout(
        &self,
        render: Render,
        window: &Window,
        status_with_log: String,
        status_no_log: String,
        snapshot: &Snapshot,
        fresh: bool,
        footer: &str,
        max_bytes: usize,
        empty: &str,
        structured: &mut Value,
    ) -> String {
        let cut_lines = window.cut_lines;
        let omitted = window.omitted;
        let mut status = if needs_log_in_status(snapshot, cut_lines, omitted) {
            status_with_log
        } else {
            status_no_log
        };
        if let Some((first, last)) = omitted {
            status.push_str(&format!(
                " · {} lines omitted: {} lines {first}-{last}",
                last - first + 1,
                window.source
            ));
        }
        if fresh {
            status.push_str(NEW_READER_NOTE);
        }
        let body = if window.is_empty() {
            if empty.len() <= max_bytes { empty } else { "" }
        } else {
            window.text.as_str()
        };
        let text = if render.output_mode == OutputMode::Structured {
            format!("{status}{footer}")
        } else {
            format!("{body}\n\n{status}{footer}")
        };
        structured["omittedLines"] = json!(omitted.map_or(0, |(first, last)| last - first + 1));
        structured["omittedRange"] = json!(omitted.map(|(first, last)| [first, last]));
        if render.output_mode != OutputMode::Text {
            structured["output"] = json!(&window.text);
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
        entry: Arc<TaskEntry>,
        task_reader: Arc<Reader>,
        state: tokio::sync::OwnedMutexGuard<ReadState>,
        view: View,
        ended: WaitEnd,
        startup_warning: Option<ProbeWarning>,
        permit: Option<Arc<OwnedSemaphorePermit>>,
    ) -> Result<CallToolResult, String> {
        // A finished state, taken first, means the screen below holds all output.
        let snapshot = task.snapshot_held();
        let screen = task.screen()?;
        // The stream counts as read up to the output the screen shows.
        let (reader, until) = (Arc::clone(&task), screen.log_end);
        let (window, read, _permit) = tokio::task::spawn_blocking(move || {
            let mut read = StagedRead::new(state);
            let window = reader.read_window(
                &mut read.state,
                Truncate::Tail,
                MIN_BODY_BUDGET,
                false,
                true,
                false,
                reader.encoding,
                Some(until),
            );
            (window, read, permit)
        })
        .await
        .map_err(|error| format!("Internal failure while reading output: {error}."))?;
        let window = window.map_err(|error| format!("Cannot read the task log: {error}."))?;
        let mut structured = task_json(action, &task, &snapshot, &task_reader);
        if let Some(warning) = startup_warning.and_then(ProbeWarning::deliver) {
            structured["warnings"]
                .as_array_mut()
                .unwrap()
                .push(Value::String(warning));
        }
        add_encoding_warning(&mut structured, &window);
        let warnings = warning_text(&structured["warnings"]);
        let (background, claim) = self.background(session, &task.id, view.nested);
        let footer = background
            .as_ref()
            .map_or(String::new(), |(line, _)| format!("\n{line}"));
        let (row, col) = (screen.cursor.0 + 1, screen.cursor.1 + 1);
        let screen_note = format!(
            "{} · screen {}x{}, cursor row {row} col {col}",
            ended.note(view.conditions),
            screen.cols,
            screen.rows_count
        );
        let room = view.max_bytes;
        let full_len = screen.rows.iter().map(String::len).sum::<usize>()
            + screen.rows.len().saturating_sub(1);
        let need_log = needs_log_in_status(&snapshot, 0, None) || full_len > room;
        let mut status = status_line(&task, &snapshot, &task_reader, need_log, None);
        status.push_str(&screen_note);
        if task_reader.is_fresh() {
            status.push_str(NEW_READER_NOTE);
        }
        let (body, omitted) = fit_screen_rows(&screen.rows, room);
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
        structured["waitEndedBy"] = ended.json();
        structured["cols"] = json!(screen.cols);
        structured["rows"] = json!(screen.rows_count);
        structured["cursor"] = json!([row, col]);
        structured["omittedRows"] = json!(omitted);
        if render.output_mode != OutputMode::Text {
            structured["output"] = json!(body);
        }
        let result = success(text, structured, &warnings);
        let state = read.commit();
        task_reader.mark_delivered();
        report(
            session,
            &entry,
            &task_reader,
            &snapshot,
            background,
            claim,
            view.nested,
            view.owned_by_script,
        );
        drop(state);
        Ok(result)
    }
}

fn fit_screen_rows(rows: &[String], room: usize) -> (String, usize) {
    let mut body = rows.join("\n");
    let mut omitted = 0;
    while body.len() > room && omitted + 1 < rows.len() {
        omitted += 1;
        body = format!(
            "... [{omitted} top rows omitted] ...\n{}",
            rows[omitted..].join("\n")
        );
    }
    output::cut_to(&mut body, room.saturating_add(1));
    (body, omitted)
}

fn status_line(
    task: &Task,
    snapshot: &Snapshot,
    reader: &Reader,
    include_log: bool,
    script: Option<&ScriptReport>,
) -> String {
    let state = match snapshot.exit_code {
        Some(code) if snapshot.state == "exited" => format!("exited {code}"),
        Some(code) => format!("{} (exit {code})", snapshot.state),
        None => snapshot.state.to_string(),
    };
    let mut line = format!(
        "[{state}] {} · {:.1}s · {} lines",
        reader.handle(&task.id),
        snapshot.elapsed.as_secs_f64(),
        snapshot.lines,
    );
    if include_log {
        line.push_str(&format!(" · log {}", files::display_path(&task.log_path)));
    }
    if snapshot.state == "running" {
        line.push_str(" · poll to continue, kill to stop");
    }
    if snapshot.expired {
        line.push_str(" · killed by killAfterMs");
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
    if let Some(report) = script {
        if snapshot.state == "killed" {
            line.push_str(&format!(
                " · cancelled {} pending call(s) only; ",
                report.cancelled
            ));
            if report.running.is_empty() {
                line.push_str("tasks started with exec keep running");
            } else {
                line.push_str(&format!(
                    "tasks it started keep running: {}; stop each with exec kill",
                    task_handles(&report.running)
                ));
            }
        } else if snapshot.state != "running" && !report.running.is_empty() {
            line.push_str(&format!(
                " · tasks it started still running: {}",
                task_handles(&report.running)
            ));
        }
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

#[derive(Default)]
struct ImageRead {
    content: Vec<ContentBlock>,
    omitted: Vec<Value>,
    warnings: Vec<String>,
    seen: usize,
}

fn read_images(task: &Task, start: u64, end: u64, seen: usize) -> Result<ImageRead, String> {
    let mut result = ImageRead {
        seen,
        ..ImageRead::default()
    };
    let mut bytes = 0;
    let mut unavailable = Vec::new();
    for (image, dropped) in task.images_between(start, end, seen) {
        result.seen = result.seen.max(image.index);
        let reason = if dropped {
            Some("log_dropped")
        } else if result.content.len() == 4 || bytes + image.base64_len > images::MAX_BASE64 {
            Some("budget")
        } else {
            None
        };
        let shown_image_path = files::display_path(&image.path);
        if let Some(reason) = reason {
            result
                .omitted
                .push(json!({"index": image.index, "path": shown_image_path, "reason": reason}));
            unavailable.push(format!("#{} {shown_image_path}", image.index));
            continue;
        }
        let read = || -> std::io::Result<String> {
            let mut data = Vec::new();
            File::open(&image.path)?
                .take(image.size + 1)
                .read_to_end(&mut data)?;
            if data.len() as u64 != image.size {
                return Err(std::io::Error::other(
                    "image size changed; read its path directly",
                ));
            }
            Ok(base64::engine::general_purpose::STANDARD.encode(data))
        };
        let encoded = match read() {
            Ok(encoded) => encoded,
            Err(_) if task.evicted() => {
                result
                    .omitted
                    .push(json!({"index": image.index, "reason": "evicted"}));
                continue;
            }
            Err(error) => {
                return Err(format!(
                    "Cannot read stored image {shown_image_path}: {error}.",
                ));
            }
        };
        bytes += encoded.len();
        result
            .content
            .push(ContentBlock::image(encoded, image.mime));
    }
    if !unavailable.is_empty() {
        result.warnings.push(format!(
            "{} image(s) not attached (4 images or {} base64 bytes per result, or image markers lost from the log): {}; read their paths.",
            unavailable.len(), images::MAX_BASE64, unavailable.join(", "),
        ));
    }
    Ok(result)
}

struct ScriptReport {
    value: Value,
    running: Vec<String>,
    cancelled: usize,
}

fn script_report(tasks: &Tasks, task: &Task) -> Option<ScriptReport> {
    let (cancelled, handles) = task.script()?.report();
    let mut running = Vec::new();
    let started = handles
        .into_iter()
        .map(|handle| {
            let task = parse_handle(&handle)
                .ok()
                .and_then(|(id, _)| tasks.find(id));
            let state = task.as_ref().map_or("unknown", |task| {
                let snapshot = task.snapshot();
                if snapshot.state == "running" || snapshot.kept {
                    running.push(handle.clone());
                }
                snapshot.state
            });
            json!({"taskId": handle, "state": state})
        })
        .collect::<Vec<_>>();
    Some(ScriptReport {
        cancelled: cancelled.len(),
        running,
        value: json!({
            "cancelledCalls": cancelled, "startedTasks": started,
            "imagesAttached": 0, "imagesNotAttached": [],
        }),
    })
}

fn task_handles(handles: &[String]) -> String {
    let mut text = handles
        .iter()
        .take(3)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    if handles.len() > 3 {
        text.push_str(&format!(" and {} more", handles.len() - 3));
    }
    text
}

fn task_json(action: Action, task: &Task, snapshot: &Snapshot, reader: &Reader) -> Value {
    json!({
        "ok": task_io_ok(snapshot),
        "action": if task.is_script() && action == Action::Start { "codemode" } else { action_name(action) },
        "kind": if task.is_script() { "codemode" } else { "bash" },
        "taskId": reader.handle(&task.id),
        "state": snapshot.state,
        "exitCode": snapshot.exit_code,
        "pty": task.pty,
        "elapsedMs": snapshot.elapsed.as_millis() as u64,
        "logPath": files::display_path(&task.log_path),
        "lifetimeExpired": snapshot.expired,
        "logError": snapshot.log_error,
        "captureError": snapshot.capture_error,
        "inputWarning": snapshot.input_warning,
        "inputError": snapshot.input_error,
        "droppedBytes": snapshot.dropped,
        "logEvicted": snapshot.evicted,
        "outputOpen": snapshot.output_open,
        "outputIncomplete": output_incomplete(snapshot),
        "warnings": task_warnings(snapshot),
        "keepTree": task.keep,
        "forcedEnd": snapshot.forced_end,
        "leftovers": members_json(&snapshot.leftovers),
        "keptRunning": snapshot.kept,
        "held": members_json(&snapshot.held),
        "killError": snapshot.kill_error,
    })
}

fn task_io_ok(snapshot: &Snapshot) -> bool {
    snapshot.capture_error.is_none()
        && snapshot.log_error.is_none()
        && snapshot.input_error.is_none()
}

fn output_incomplete(snapshot: &Snapshot) -> bool {
    snapshot.capture_error.is_some()
        || snapshot.log_error.is_some()
        || snapshot.dropped > 0
        || snapshot.evicted
}

fn task_warnings(snapshot: &Snapshot) -> Vec<String> {
    let mut warnings = Vec::new();
    if let Some(error) = &snapshot.capture_error {
        warnings.push(format!(
            "output capture failed ({error}); output is incomplete"
        ));
    }
    if let Some(error) = &snapshot.log_error {
        warnings.push(format!(
            "log write failed ({error}); {} bytes were not stored",
            snapshot.dropped
        ));
    } else if snapshot.dropped > 0 {
        warnings.push(format!(
            "{} bytes past the log limit were not stored",
            snapshot.dropped
        ));
    }
    if snapshot.evicted {
        warnings.push(
            "log evicted under the 1 GiB total limit; stored output and images are unavailable"
                .into(),
        );
    }
    if snapshot.output_open {
        warnings.push(if snapshot.kept {
            "processes it keeps hold the output open; later output may follow".into()
        } else {
            "output stayed open after exit; later output may follow".into()
        });
    }
    if let Some(error) = &snapshot.kill_error {
        warnings.push(format!(
            "could not end the task's process tree: {error}; kill retries"
        ));
    }
    if let Some(error) = &snapshot.input_error {
        warnings.push(error.clone());
    }
    if let Some(warning) = &snapshot.input_warning {
        warnings.push(warning.clone());
    }
    warnings
}

fn members_json(members: &[process::Member]) -> Value {
    json!(
        members
            .iter()
            .map(|member| json!({"pid": member.pid, "name": member.name}))
            .collect::<Vec<_>>()
    )
}

fn add_encoding_warning(value: &mut Value, window: &Window) {
    value["encodingErrors"] = json!(window.bad_lines);
    if window.bad_lines > 0 {
        value["warnings"]
            .as_array_mut()
            .unwrap()
            .push(json!(format!(
                "{} lines had bytes invalid in {}; pass encoding on a stream read (e.g. big5, gbk)",
                window.bad_lines,
                window.encoding.name()
            )));
    }
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

/// Omits default-valued diagnostic fields on task results and `list` entries at final serialization.
fn prune_task_diagnostics(value: &mut Value) {
    let Some(obj) = value.as_object_mut() else {
        return;
    };
    for key in [
        "captureError",
        "logError",
        "killError",
        "inputError",
        "inputWarning",
        "omittedRange",
        "transcriptError",
        "cols",
        "rows",
        "session",
        "callId",
    ] {
        if obj.get(key) == Some(&Value::Null) {
            obj.remove(key);
        }
    }
    for key in [
        "droppedBytes",
        "encodingErrors",
        "cutLines",
        "omittedLines",
        "omittedRows",
    ] {
        if obj.get(key).and_then(Value::as_u64) == Some(0) {
            obj.remove(key);
        }
    }
    for key in [
        "forcedEnd",
        "lifetimeExpired",
        "logEvicted",
        "outputIncomplete",
        "outputOpen",
        "keptRunning",
        "keepTree",
        "transcriptIncomplete",
    ] {
        if obj.get(key) == Some(&Value::Bool(false)) {
            obj.remove(key);
        }
    }
    for key in ["held", "leftovers", "warnings"] {
        if obj
            .get(key)
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
        {
            obj.remove(key);
        }
    }
    if let Some(Value::Object(codemode)) = obj.get_mut("codemode") {
        for key in ["cancelledCalls", "startedTasks", "imagesNotAttached"] {
            if codemode
                .get(key)
                .and_then(Value::as_array)
                .is_some_and(Vec::is_empty)
            {
                codemode.remove(key);
            }
        }
        if codemode.get("imagesAttached").and_then(Value::as_u64) == Some(0) {
            codemode.remove("imagesAttached");
        }
        if codemode.is_empty() {
            obj.remove("codemode");
        }
    }
    if let Some(Value::Array(tasks)) = obj.get_mut("tasks") {
        for entry in tasks {
            prune_task_diagnostics(entry);
        }
    }
}

/// Formats complete warnings outside the body budget.
fn warning_text(warnings: &Value) -> String {
    let mut text = String::new();
    if let Some(warnings) = warnings.as_array() {
        for warning in warnings.iter().filter_map(Value::as_str) {
            if text.is_empty() {
                text.push('\n');
            }
            text.push_str("\nWarning: ");
            text.push_str(warning);
        }
    }
    text
}

fn success(mut text: String, mut structured: Value, suffix: &str) -> CallToolResult {
    text.push_str(suffix);
    let is_err = structured["ok"] == false;
    prune_task_diagnostics(&mut structured);
    let content = vec![ContentBlock::text(text)];
    let mut result = if is_err {
        CallToolResult::error(content)
    } else {
        CallToolResult::success(content)
    };
    result.structured_content = Some(structured);
    result
}

pub(crate) fn validate_cwd(
    dir: &str,
    launch: &crate::session::LaunchContext,
) -> Result<PathBuf, String> {
    let resolved = files::resolve(dir, launch)?;
    if !resolved.is_dir() {
        return Err(format!(
            "cwd {} is not an existing directory.",
            files::display_path(&resolved)
        ));
    }
    Ok(resolved)
}

pub fn error_result(action: Action, message: &str) -> CallToolResult {
    error_named(action_name(action), message)
}

pub fn codemode_error(message: &str) -> CallToolResult {
    error_named("codemode", message)
}

fn error_named(action: &str, message: &str) -> CallToolResult {
    let mut result = CallToolResult::error(vec![ContentBlock::text(message)]);
    result.structured_content = Some(json!({ "ok": false, "action": action, "error": message }));
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn script_fixture() -> (Arc<Service>, Arc<Tasks>, Arc<Session>) {
        let tasks = Tasks::new(tasks::Limits::OFF).unwrap();
        let service = Arc::new(Service::new(Arc::clone(&tasks), false));
        let launch = Arc::new(crate::session::LaunchContext::new(
            std::env::temp_dir(),
            Vec::new(),
        ));
        let session = Session::new("script-test".into(), "script-test".into(), launch);
        (service, tasks, session)
    }

    fn script_call(session: &Arc<Session>, nested: bool) -> Call<'_> {
        Call {
            session,
            launch: None,
            render: Render {
                output_mode: OutputMode::Both,
                structured_content: true,
            },
            cancel: CancellationToken::new(),
            progress: None,
            call_id: None,
            nested,
            started: None,
            render_slot: None,
        }
    }

    fn pixel_url() -> String {
        let mut bytes = Vec::new();
        image::DynamicImage::new_rgb8(1, 1)
            .write_to(
                &mut std::io::Cursor::new(&mut bytes),
                image::ImageFormat::Png,
            )
            .unwrap();
        format!(
            "data:image/png;base64,{}",
            base64::engine::general_purpose::STANDARD.encode(bytes)
        )
    }

    fn image_count(result: &CallToolResult) -> usize {
        serde_json::to_value(&result.content)
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .filter(|block| block["type"] == "image")
            .count()
    }

    #[tokio::test]
    async fn codemode_images_follow_readers_despite_truncated_or_structured_text() {
        let (service, tasks, session) = script_fixture();
        let code = format!("for(let i=0;i<6;i++) image({});", json!(pixel_url()));
        for mode in [OutputMode::Both, OutputMode::Structured] {
            let mut call = script_call(&session, false);
            call.render.output_mode = mode;
            let result = service
                .codemode(
                    serde_json::from_value(json!({
                        "code": code, "maxBytes": 100, "truncate": "tail",
                    }))
                    .unwrap(),
                    call,
                    false,
                )
                .await;
            assert_eq!(result.is_error, Some(false), "{result:?}");
            assert_eq!(image_count(&result), 4, "{result:?}");
            let data = result.structured_content.unwrap();
            assert_eq!(data["codemode"]["imagesAttached"], 4);
            assert_eq!(
                data["codemode"]["imagesNotAttached"]
                    .as_array()
                    .unwrap()
                    .len(),
                2
            );
            let handle = data["taskId"].as_str().unwrap();
            let empty = service
                .call_json(
                    json!({"action":"poll","taskId":handle,"waitMs":0}),
                    script_call(&session, false),
                )
                .await;
            assert_eq!(image_count(&empty), 0);
            let bare = parse_handle(handle).unwrap().0;
            let reread = service
                .call_json(
                    json!({"action":"poll","taskId":bare,"waitMs":0}),
                    script_call(&session, false),
                )
                .await;
            assert_eq!(image_count(&reread), 4);
        }
        tasks.shutdown();
    }

    #[tokio::test]
    async fn codemode_images_obey_the_combined_base64_budget() {
        let (service, tasks, session) = script_fixture();
        let mut bytes = base64::engine::general_purpose::STANDARD
            .decode(pixel_url().split_once(',').unwrap().1)
            .unwrap();
        bytes.resize(2 << 20, 0);
        let encoded_size = bytes.len().div_ceil(3) * 4;
        assert!(encoded_size < images::MAX_BASE64 && 2 * encoded_size > images::MAX_BASE64);
        let path = std::env::temp_dir().join(format!(
            "fastexec-image-budget-{}.png",
            tasks::random_chars(8).unwrap()
        ));
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .unwrap();
            file.write_all(&bytes).unwrap();
        }
        let code = format!(
            "const pic = await tools.read({{path:{}}}); image(pic); image(pic);",
            json!(path.to_str().unwrap())
        );
        let result = service
            .codemode(
                serde_json::from_value(json!({"code":code})).unwrap(),
                script_call(&session, false),
                false,
            )
            .await;
        std::fs::remove_file(path).unwrap();
        assert_eq!(result.is_error, Some(false), "{result:?}");
        assert_eq!(image_count(&result), 1);
        let data = result.structured_content.unwrap();
        assert_eq!(data["exitCode"], 0);
        assert_eq!(data["codemode"]["imagesAttached"], 1);
        let omitted = data["codemode"]["imagesNotAttached"].as_array().unwrap();
        assert_eq!(omitted.len(), 1);
        assert_eq!(omitted[0]["index"], 2);
        assert_eq!(omitted[0]["reason"], "budget");
        let again = service
            .call_json(
                json!({
                    "action":"poll","taskId":data["taskId"],"waitMs":0,
                }),
                script_call(&session, false),
            )
            .await;
        assert_eq!(image_count(&again), 0);
        assert!(
            again
                .structured_content
                .unwrap()
                .get("codemode")
                .and_then(|cm| cm.get("imagesNotAttached"))
                .is_none()
        );
        let (id, _) = parse_handle(data["taskId"].as_str().unwrap()).unwrap();
        let image_path = tasks.find(id).unwrap().image_path(1, "image/png");
        let shorter = bytes.len() - 1;
        assert_eq!(shorter.div_ceil(3) * 4, encoded_size);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&image_path)
            .unwrap()
            .set_len(shorter as u64)
            .unwrap();
        let fresh = json!({"action":"poll","taskId":format!("{id}.5678"),"waitMs":0});
        let changed = service
            .call_json(fresh.clone(), script_call(&session, false))
            .await;
        assert_eq!(changed.is_error, Some(true));
        assert!(
            serde_json::to_string(&changed.content)
                .unwrap()
                .contains("image size changed")
        );
        std::fs::write(&image_path, &bytes).unwrap();
        let retried = service.call_json(fresh, script_call(&session, false)).await;
        assert_eq!(
            image_count(&retried),
            1,
            "a failed image read must leave its reader uncommitted"
        );
        tasks.shutdown();
    }

    #[tokio::test]
    async fn nested_task_reads_consume_reader_state_without_consuming_session_notices() {
        let (service, tasks, session) = script_fixture();
        let result = service
            .codemode(
                serde_json::from_value(json!({"code":"return 7"})).unwrap(),
                script_call(&session, true),
                false,
            )
            .await;
        let data = result.structured_content.unwrap();
        let handle = data["taskId"].as_str().unwrap();
        let (id, key) = parse_handle(handle).unwrap();
        let task = tasks.find(id).unwrap();
        let entry = session.entry(&task);
        assert!(entry.reader(key).unwrap().is_reported());
        assert!(!entry.is_reported());
        service
            .call_json(
                json!({"action":"poll","taskId":handle,"waitMs":0}),
                script_call(&session, true),
            )
            .await;
        service
            .call_json(
                json!({"action":"kill","taskId":handle}),
                script_call(&session, true),
            )
            .await;
        assert!(!entry.is_reported());
        assert!(tasks.background(&session, "another-task", 512).0.is_some());
        service
            .codemode(
                serde_json::from_value(json!({"code":"return 8"})).unwrap(),
                script_call(&session, false),
                false,
            )
            .await;
        assert!(entry.is_reported());
        tasks.shutdown();
    }

    #[tokio::test]
    async fn nested_list_marks_only_delivered_session_tasks() {
        let (service, tasks, session) = script_fixture();
        let mut entries = Vec::new();
        for code in ["return 7", "return 8"] {
            let result = service
                .codemode(
                    serde_json::from_value(json!({"code":code})).unwrap(),
                    script_call(&session, true),
                    false,
                )
                .await;
            let data = result.structured_content.unwrap();
            let (id, _) = parse_handle(data["taskId"].as_str().unwrap()).unwrap();
            let task = tasks.find(id).unwrap();
            entries.push(session.entry(&task));
        }
        let listed = service
            .call_json(
                json!({"action":"list","limit":1,"maxBytes":100}),
                script_call(&session, true),
            )
            .await;
        assert_eq!(
            listed.structured_content.unwrap()["tasks"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert!(!entries[0].is_reported(), "an unselected task stays due");
        assert!(entries[1].is_reported(), "the delivered task is reported");
        tasks.shutdown();
    }

    #[tokio::test]
    async fn script_poll_rejects_presence_even_when_false_or_empty() {
        let (service, tasks, session) = script_fixture();
        let result = service
            .codemode(
                serde_json::from_value(json!({"code":"return 1"})).unwrap(),
                script_call(&session, false),
                false,
            )
            .await;
        let data = result.structured_content.unwrap();
        for accepted in [
            json!({"eof":false,"screen":false,"raw":false}),
            json!({"input":null,"eof":null,"keys":null,"screen":null,"raw":null,"encoding":null}),
            json!({"keys":[]}),
            json!({"keys":[],"keyDelayMs":150}),
        ] {
            let mut request = json!({"action":"poll","taskId":data["taskId"],"waitMs":0});
            request
                .as_object_mut()
                .unwrap()
                .extend(accepted.as_object().unwrap().clone());
            let ok = service
                .call_json(request, script_call(&session, false))
                .await;
            assert_eq!(ok.is_error, Some(false), "{ok:?}");
        }
        for fields in [
            json!({"input":""}),
            json!({"eof":true}),
            json!({"keys":["Enter"]}),
            json!({"keyDelayMs":0}),
            json!({"cols":80}),
            json!({"rows":24}),
            json!({"screen":true}),
            json!({"raw":true}),
            json!({"encoding":"utf-8"}),
            json!({"returnWhen":{"screenContains":["x"]}}),
        ] {
            let mut request = json!({"action":"poll","taskId":data["taskId"],"waitMs":0});
            request
                .as_object_mut()
                .unwrap()
                .extend(fields.as_object().unwrap().clone());
            let result = service
                .call_json(request, script_call(&session, false))
                .await;
            assert_eq!(result.is_error, Some(true), "{result:?}");
            assert!(
                serde_json::to_string(&result.content)
                    .unwrap()
                    .contains("A codemode task takes no input")
            );
        }
        let result = service
            .call_json(
                json!({"action":"transcript","taskId":data["taskId"]}),
                script_call(&session, false),
            )
            .await;
        assert!(
            serde_json::to_string(&result.content)
                .unwrap()
                .contains("has no terminal")
        );
        tasks.shutdown();
    }

    #[tokio::test]
    async fn critical_warnings_survive_small_budgets_and_every_render_mode() {
        let service = Service::new(Tasks::new(tasks::Limits::OFF).unwrap(), false);
        let snapshot = Snapshot {
            state: "exited",
            exit_code: Some(0),
            forced_end: false,
            elapsed: Duration::ZERO,
            lines: 1,
            dropped: 128,
            evicted: true,
            output_open: true,
            expired: false,
            log_error: None,
            capture_error: Some("read failed".into()),
            input_warning: Some("canonical input may be discarded".into()),
            input_error: Some("stdin write failed".into()),
            leftovers: Vec::new(),
            kept: false,
            tree_ended: true,
            held: Vec::new(),
            kill_error: None,
        };
        assert!(output_incomplete(&snapshot));
        let warnings = task_warnings(&snapshot);
        assert_eq!(warnings.len(), 6);
        for mode in [OutputMode::Text, OutputMode::Both, OutputMode::Structured] {
            for structured_content in [false, true] {
                for raw in [false, true] {
                    let render = Render {
                        output_mode: mode,
                        structured_content,
                    }
                    .effective();
                    let view = View {
                        nested: false,
                        owned_by_script: false,
                        truncate: Truncate::HeadTail,
                        max_bytes: 100,
                        raw,
                        encoding: None,
                        screen: false,
                        conditions: false,
                    };
                    let mut cleaner = Cleaner::default();
                    let mut builder = WindowBuilder::new(
                        WindowSpec {
                            truncate: view.truncate,
                            budget: view.max_bytes,
                            raw,
                            clean: true,
                            keep_nul: false,
                            encoding: encoding_rs::UTF_8,
                            source: "log",
                        },
                        &mut cleaner,
                        1,
                    );
                    builder.push(b"captured prefix\n");
                    let window = builder.finish();
                    let mut structured = json!({ "ok": task_io_ok(&snapshot), "warnings": warnings,
                        "exitCode": snapshot.exit_code, "outputIncomplete": output_incomplete(&snapshot) });
                    let warnings_text = warning_text(&structured["warnings"]);
                    let text = service.layout(
                        render,
                        &window,
                        format!("[exited 0] task.reader · log {}", "x".repeat(200)),
                        "[exited 0] task.reader".to_owned(),
                        &snapshot,
                        false,
                        "",
                        view.max_bytes,
                        "(no new output)",
                        &mut structured,
                    );
                    let result = suppress(success(text, structured, &warnings_text), render);
                    let value = serde_json::to_value(result).unwrap();
                    assert_eq!(value["isError"], true);
                    let text = value["content"][0]["text"].as_str().unwrap();
                    assert!(text.find("[exited 0]").unwrap() < text.find("Warning:").unwrap());
                    for warning in &warnings {
                        assert!(text.contains(&format!("Warning: {warning}")), "{text}");
                    }
                    if structured_content {
                        assert_eq!(value["structuredContent"]["exitCode"], 0);
                        assert_eq!(value["structuredContent"]["outputIncomplete"], true);
                        assert_eq!(value["structuredContent"]["warnings"], json!(warnings));
                    } else {
                        assert!(value.get("structuredContent").is_none());
                    }
                }
            }
        }
        service.tasks.shutdown();
    }

    #[test]
    fn first_committed_render_gets_outside_count_while_an_earlier_read_is_pending() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let (service, tasks, owner) = script_fixture();
            service
                .codemode(
                    serde_json::from_value(json!({"code":"while (true) {}","waitMs":0})).unwrap(),
                    script_call(&owner, false),
                    false,
                )
                .await;
            let completed = service
                .codemode(
                    serde_json::from_value(json!({"code":"return 'ready'"})).unwrap(),
                    script_call(&owner, false),
                    false,
                )
                .await;
            let data = completed.structured_content.unwrap();
            let bare = parse_handle(data["taskId"].as_str().unwrap()).unwrap().0;
            let request = json!({"action":"poll","taskId":bare,"waitMs":0});

            for cancel_earlier in [false, true] {
                let session = Session::new(
                    format!("reader-{cancel_earlier}"),
                    "reader".into(),
                    Arc::clone(&owner.launch),
                );
                let (entered, ready) = tokio::sync::oneshot::channel();
                let (release, blocked) = std::sync::mpsc::channel();
                let blocker = tokio::task::spawn_blocking(move || {
                    entered.send(()).unwrap();
                    blocked.recv().unwrap();
                });
                ready.await.unwrap();
                let cancel = CancellationToken::new();
                let mut call = script_call(&session, false);
                call.cancel = cancel.clone();
                let mut earlier = Box::pin(service.call_json(request.clone(), call));
                let pending = std::future::poll_fn(|cx| {
                    std::task::Poll::Ready(earlier.as_mut().poll(cx).is_pending())
                })
                .await;
                release.send(()).unwrap();
                blocker.await.unwrap();
                assert!(
                    pending,
                    "the first render must stop at its queued blocking read"
                );

                // Its I/O may finish, but the earlier future cannot commit until we poll it again.
                let first = service
                    .call_json(request.clone(), script_call(&session, false))
                    .await;
                let text = serde_json::to_value(&first.content).unwrap()[0]["text"]
                    .as_str()
                    .unwrap()
                    .to_owned();
                assert!(
                    text.contains("1 task running outside this session"),
                    "the first committed render must carry the count: {text}"
                );
                if cancel_earlier {
                    cancel.cancel();
                }
                let later = earlier.await;
                let text = serde_json::to_value(&later.content).unwrap()[0]["text"]
                    .as_str()
                    .unwrap()
                    .to_owned();
                assert!(!text.contains("running outside this session"), "{text}");
                assert_eq!(later.is_error, Some(cancel_earlier), "{later:?}");
            }
            tasks.shutdown();
        });
    }

    #[tokio::test]
    async fn cancellation_during_blocking_render_keeps_reader_fresh_and_gate_pinned() {
        let entry = Arc::new(TaskEntry::default());
        let reader = entry.reader(None).unwrap();
        fn read_payload(state: &mut ReadState) -> Window {
            let bytes = b"prefix\n\x1b[";
            let mut builder = WindowBuilder::new(
                WindowSpec {
                    truncate: Truncate::Head,
                    budget: 1024,
                    raw: false,
                    clean: true,
                    keep_nul: false,
                    encoding: encoding_rs::UTF_8,
                    source: "log",
                },
                &mut state.cleaner,
                state.lines_before + 1,
            );
            builder.push(&bytes[state.cursor as usize..]);
            let window = builder.finish();
            state.cursor = bytes.len() as u64;
            state.lines_before += window.newlines;
            window
        }

        let rendering = Arc::clone(&reader);
        let reporting = Arc::clone(&entry);
        let mut state = reader.read_gate().lock_owned().await;
        state.shown = (7, 2);
        let initial_cleaner = format!("{:?}", state.cleaner);
        let cancel = CancellationToken::new();
        let cancelled = cancel.clone();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let call = tokio::spawn(async move {
            read_result(&cancelled, async move {
                let read = tokio::task::spawn_blocking(move || {
                    let mut read = StagedRead::new(state);
                    let window = read_payload(&mut read.state);
                    assert_eq!(window.text, "prefix");
                    read.state.shown = (77, 7);
                    read.state.images_seen = 9;
                    entered.send(()).unwrap();
                    blocked.recv().unwrap();
                    read
                })
                .await
                .unwrap();
                let state = read.commit();
                rendering.mark_delivered();
                let snapshot = Snapshot {
                    state: "exited",
                    exit_code: Some(0),
                    forced_end: false,
                    elapsed: Duration::ZERO,
                    lines: 0,
                    dropped: 0,
                    evicted: false,
                    output_open: false,
                    expired: false,
                    log_error: None,
                    capture_error: None,
                    input_warning: None,
                    input_error: None,
                    leftovers: Vec::new(),
                    kept: false,
                    tree_ended: true,
                    held: Vec::new(),
                    kill_error: None,
                };
                rendering.mark_reported(&snapshot);
                reporting.mark_reported(&snapshot);
                drop(state);
                Ok(success(String::new(), Value::Null, ""))
            })
            .await
        });
        started.await.unwrap();
        cancel.cancel();
        let result = tokio::time::timeout(Duration::from_secs(5), call)
            .await
            .unwrap()
            .unwrap();
        assert!(result.unwrap_err().contains("Cancelled"));
        assert!(reader.is_fresh());
        assert!(!reader.is_reported() && !entry.is_reported() && !entry.tree_end_reported());
        assert!(reader.read.try_lock().is_err());
        release.send(()).unwrap();
        let state = tokio::time::timeout(Duration::from_secs(5), reader.read_gate().lock_owned())
            .await
            .unwrap();
        assert_eq!(state.cursor, 0);
        assert_eq!(state.lines_before, 0);
        assert_eq!(state.shown, (7, 2));
        assert_eq!(state.images_seen, 0);
        assert_eq!(format!("{:?}", state.cleaner), initial_cleaner);
        assert!(reader.is_fresh());
        assert!(!reader.is_reported() && !entry.is_reported());
        let mut read = StagedRead::new(state);
        let output = read_payload(&mut read.state).text;
        assert_eq!(
            output, "prefix",
            "the retry must read and clean the whole original payload"
        );
        read_result(&CancellationToken::new(), async {
            let state = read.commit();
            reader.mark_delivered();
            drop(state);
            Ok(success(output, Value::Null, ""))
        })
        .await
        .unwrap();
        let state = reader.read_gate().lock_owned().await;
        assert_eq!(state.cursor, b"prefix\n\x1b[".len() as u64);
        assert_eq!(state.lines_before, 1);
        assert_eq!(state.shown, (7, 2));
        assert!(!reader.is_fresh());
    }

    #[tokio::test]
    async fn a_cancelled_token_wins_over_a_ready_result() {
        let reader = TaskEntry::default().reader(None).unwrap();
        let cancel = CancellationToken::new();
        cancel.cancel();
        let result = read_result(&cancel, async {
            reader.mark_delivered();
            Ok(success(String::new(), Value::Null, ""))
        })
        .await;
        assert!(result.is_err());
        assert!(reader.is_fresh());
    }

    #[test]
    fn a_large_list_row_does_not_hide_the_opposite_end() {
        for (rows, want, omitted_at_start) in [
            (vec!["x".repeat(200), "short".into()], 1, true),
            (vec!["short".into(), "x".repeat(200)], 0, false),
        ] {
            let (indexes, text) = fit_lines(&rows, Truncate::HeadTail, 100);
            assert_eq!(indexes, [want]);
            assert_eq!(text.starts_with("... [1 task"), omitted_at_start);
            assert!(text.contains("short"));
            assert!(text.len() <= 100);
        }
    }

    #[test]
    fn screen_row_omission_keeps_and_cuts_the_bottom_row_when_it_exceeds_budget() {
        let single = vec!["bottom-".repeat(30)];
        let (body, omitted) = fit_screen_rows(&single, 100);
        assert_eq!(omitted, 0);
        assert!(body.starts_with("bottom-bottom-"), "{body}");
        assert!(body.ends_with(" … [cut]"), "{body}");
        assert!(body.len() <= 100);

        let multi = vec!["top-1".into(), "top-2".into(), "bottom-".repeat(30)];
        let (body, omitted) = fit_screen_rows(&multi, 100);
        assert_eq!(omitted, 2);
        assert!(
            body.starts_with("... [2 top rows omitted] ...\nbottom-"),
            "{body}"
        );
        assert!(body.ends_with(" … [cut]"), "{body}");
        assert!(body.len() <= 100);
    }
}
