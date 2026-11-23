//! The tool's work without MCP transport types: validate the arguments, run the operation on the
//! tasks, and render the result. The MCP adapter calls it in-process; the daemon calls it for IPC
//! and HTTP callers.

pub(crate) mod render;
pub(crate) mod schema;
#[cfg(test)]
mod tests;

use self::render::{
    Background, Render, RenderSlot, RenderSnapshot, codemode_error, error_result,
    measure_outbound_len, members, members_json, metadata_members, metadata_workspace_bytes,
    nested_wire_bytes, output_incomplete, reserve_nested_bridge, resize_metadata_workspace,
    result_payload_capacity, success, suppress, task_warnings, warning_text, wire_result_len,
};
use self::schema::{
    Action, DEFAULT_KEY_DELAY_MS, ListScope, ListState, MAX_INPUT_BYTES, Request, View, flag,
    normalize_request, validate, validate_cwd,
};
use crate::lease::Lease;
use crate::platform::process;
use crate::session::{ProbeWarning, ReadState, Reader, Session, parse_handle};
use crate::tasks::state::Task;
use crate::tasks::{self, ScriptArgs, StartArgs, Tasks};
use crate::term::keys;
use crate::term::output::{self, Truncate};
use crate::wait;
use crate::{codemode, files};
use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, watch};
use tokio_util::sync::CancellationToken;

const KILL_WAIT: Duration = Duration::from_secs(5);
const DEFAULT_LIST_LIMIT: u64 = 20;
/// Largest background footer.
const FOOTER_LIMIT: usize = 512;

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
    /// The nested script and pending bridge ID; also records starts before cancellation.
    pub started: Option<codemode::control::NestedCall<'a>>,
    pub(crate) render_slot: Option<&'a RenderSlot<'a>>,
    pub http_response: bool,
    /// Encoded outer JSON-RPC ID length, known before committing output cursors.
    pub rpc_id_bytes: usize,
    /// Keeps decoded arguments alive through worker-owned copies after caller cancellation.
    pub input_owner: crate::lease::Owner,
}

impl<'a> Call<'a> {
    pub(super) async fn render_permit(&self) -> Result<Option<Arc<OwnedSemaphorePermit>>, String> {
        let Some(slot) = self.render_slot else {
            return Ok(None);
        };
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err("Cancelled while reading output; use poll to continue.".into()),
            permit = slot.hold() => Ok(Some(permit)),
        }
    }

    pub(super) async fn task_render_permit(
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
                files::path::display_path(&task.log_path),
            )
        })
    }

    pub(super) fn launch(&self) -> &'a Arc<crate::session::LaunchContext> {
        self.launch.unwrap_or(&self.session.launch)
    }
}

/// The tool's operations and renderers, independent of any transport.
pub struct Service {
    pub(super) tasks: Arc<Tasks>,
    pub(super) event_bus: Arc<crate::events::EventBus>,
    /// Whether the tasks live in a daemon: its unknown-task texts name the daemon and the tasks
    /// that outlive a session.
    pub(super) daemon: bool,
}

#[derive(Debug)]
pub(super) enum ServiceError {
    Plain(String),
    Overload(String, Option<files::TaskMeta>),
}

impl From<String> for ServiceError {
    fn from(message: String) -> Self {
        Self::Plain(message)
    }
}

impl From<&str> for ServiceError {
    fn from(message: &str) -> Self {
        Self::Plain(message.to_owned())
    }
}

impl std::ops::Deref for ServiceError {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Plain(s) | Self::Overload(s, _) => s,
        }
    }
}

pub(super) fn task_read_error(message: String, task: &Task) -> ServiceError {
    if message.starts_with("Server active memory budget (") {
        ServiceError::Overload(
            message,
            Some(files::TaskMeta {
                bare_task_id: task.id.clone(),
                log_path: files::path::display_path(&task.log_path),
            }),
        )
    } else {
        ServiceError::Plain(message)
    }
}

impl Service {
    pub fn new(tasks: Arc<Tasks>, daemon: bool) -> Self {
        let event_bus = Arc::new(crate::events::EventBus::new().unwrap_or_else(|_| {
            crate::events::EventBus::with_config(
                tasks.epoch().to_owned(),
                crate::events::DEFAULT_MAX_EVENTS,
                crate::events::DEFAULT_MAX_BYTES,
                1024,
                Arc::clone(crate::lease::process_pool()),
            )
        }));
        tasks.attach_event_bus(Arc::clone(&event_bus));
        Self {
            tasks,
            event_bus,
            daemon,
        }
    }

    pub fn event_bus(&self) -> &Arc<crate::events::EventBus> {
        &self.event_bus
    }

    pub async fn call_tracked(&self, request: Request, mut call: Call<'_>) -> files::TrackedResult {
        // Entries of tasks that have retired are dropped here, so a call that never reaches the
        // background footer cannot keep them alive.
        self.tasks.prune_entries(call.session);
        call.render = call.render.effective();
        let action = request.action;
        match self.handle(request, &call).await {
            Ok((result, lease, task_meta, payload_owner)) => files::TrackedResult {
                result: suppress(result, call.render),
                lease,
                overload: false,
                task_meta,
                payload_owner,
            },
            Err(ServiceError::Plain(message)) => {
                files::TrackedResult::plain(suppress(error_result(action, &message), call.render))
            }
            Err(ServiceError::Overload(message, task_meta)) => files::TrackedResult {
                result: suppress(error_result(action, &message), call.render),
                lease: None,
                overload: true,
                task_meta,
                payload_owner: Default::default(),
            },
        }
    }

    /// Runs one call from raw arguments, the way a daemon receives them, with the same
    /// validation and errors.
    #[cfg(test)]
    pub async fn call_json(&self, arguments: Value, call: Call<'_>) -> CallToolResult {
        let mut tracked = self.call_json_tracked(arguments, call).await;
        drop(tracked.lease.take());
        tracked.result
    }

    pub async fn call_json_tracked(
        &self,
        arguments: Value,
        call: Call<'_>,
    ) -> files::TrackedResult {
        match serde_json::from_value::<Request>(arguments) {
            Ok(request) => self.call_tracked(request, call).await,
            Err(error) => {
                files::TrackedResult::plain(CallToolResult::error(vec![ContentBlock::text(
                    format!("failed to deserialize parameters: {error}"),
                )]))
            }
        }
    }

    #[cfg(test)]
    pub async fn codemode(
        self: &Arc<Self>,
        request: codemode::control::Request,
        call: Call<'_>,
        transfer: bool,
    ) -> CallToolResult {
        let mut tracked = self.codemode_tracked(request, call, transfer).await;
        drop(tracked.lease.take());
        tracked.result
    }

    pub async fn codemode_tracked(
        self: &Arc<Self>,
        request: codemode::control::Request,
        mut call: Call<'_>,
        transfer: bool,
    ) -> files::TrackedResult {
        self.tasks.prune_entries(call.session);
        call.render = call.render.effective();
        match self.start_script(request, &call, transfer).await {
            Ok((result, lease, task_meta, payload_owner)) => files::TrackedResult {
                result: suppress(result, call.render),
                lease,
                overload: false,
                task_meta,
                payload_owner,
            },
            Err(ServiceError::Plain(message)) => {
                files::TrackedResult::plain(suppress(codemode_error(&message), call.render))
            }
            Err(ServiceError::Overload(message, task_meta)) => files::TrackedResult {
                result: suppress(codemode_error(&message), call.render),
                lease: None,
                overload: true,
                task_meta,
                payload_owner: Default::default(),
            },
        }
    }

    async fn start_script(
        self: &Arc<Self>,
        request: codemode::control::Request,
        call: &Call<'_>,
        transfer: bool,
    ) -> Result<
        (
            CallToolResult,
            Option<Lease>,
            Option<files::TaskMeta>,
            crate::lease::Owner,
        ),
        ServiceError,
    > {
        let base_launch = call.launch();
        let (start, resolved_cwd) = codemode::control::validate(&request, base_launch)?;
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
            input_owner: call.input_owner.clone(),
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
    ) -> Result<
        (
            CallToolResult,
            Option<Lease>,
            Option<files::TaskMeta>,
            crate::lease::Owner,
        ),
        ServiceError,
    > {
        let (task, entry, reader) = started;
        let state = read_gate(&reader, &call.cancel).await?;
        let mark = wait::mark(&task, &reader, &state, when, call.launch().memory())
            .map_err(|error| task_read_error(error, &task))?;
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
        .await
        .map_err(|error| task_read_error(error, &task))?;
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
    ) -> Result<
        (
            CallToolResult,
            Option<Lease>,
            Option<files::TaskMeta>,
            crate::lease::Owner,
        ),
        ServiceError,
    > {
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
                let (listed, lease, owner) = self.list(
                    call,
                    &view,
                    request.scope.unwrap_or_default(),
                    request.state,
                    request.limit.unwrap_or(DEFAULT_LIST_LIMIT) as usize,
                )?;
                Ok((listed, lease, None, owner))
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
                    input_owner: call.input_owner.clone(),
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
                if let Some(script) = call.started.map(|nested| nested.script) {
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
                view.owned_by_script = call
                    .started
                    .is_some_and(|nested| nested.script.owns(&task.id));
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
                    )
                    .into());
                }
                if let Some(name) = view.screen.then_some("screen").or(watches_screen)
                    && task.encoding != encoding_rs::UTF_8
                {
                    return Err(format!(
                        "{name} needs UTF-8 output; this task was started with an encoding."
                    )
                    .into());
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
                let mark = wait::mark(&task, &task_reader, &state, when, call.launch().memory())
                    .map_err(|error| task_read_error(error, &task))?;
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
                    task.send(
                        burst,
                        flag(request.eof),
                        &call.cancel,
                        call.input_owner.clone(),
                    )
                    .await?;
                } else {
                    task.send(
                        input,
                        flag(request.eof),
                        &call.cancel,
                        call.input_owner.clone(),
                    )
                    .await?;
                    press(
                        &task,
                        input_bytes,
                        names,
                        parsed,
                        delay,
                        &call.cancel,
                        &call.input_owner,
                    )
                    .await?;
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
                .await
                .map_err(|error| task_read_error(error, &task))?;
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
                view.owned_by_script = call
                    .started
                    .is_some_and(|nested| nested.script.owns(&task.id));
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
                view.owned_by_script = call
                    .started
                    .is_some_and(|nested| nested.script.owns(&task.id));
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
        call: &Call<'a>,
        task: &Task,
        nested: bool,
        action: Action,
    ) -> Result<Background<'a>, ServiceError> {
        let workspace = std::cell::RefCell::new(Lease::empty(call.launch().memory()));
        let (mut value, claim) = if nested {
            (None, crate::session::OutsideClaim::none())
        } else {
            self.tasks
                .background(
                    call.session,
                    &task.id,
                    FOOTER_LIMIT,
                    &mut |bytes| {
                        let mut held = workspace.borrow_mut();
                        let target = held.bytes().saturating_add(metadata_workspace_bytes(bytes));
                        resize_metadata_workspace(&mut held, target, call, action)
                    },
                    &mut |tree| metadata_members(tree, &mut workspace.borrow_mut(), call, action),
                )
                .map_err(|_| RenderSnapshot::overload(call, task))?
        };
        let external_cursor = if nested {
            None
        } else if let Some((ext_line, seq)) = self
            .event_bus
            .peek_external_background_summary(&call.session.id)
        {
            match &mut value {
                Some((line, _)) => {
                    line.push('\n');
                    line.push_str(&ext_line);
                }
                None => value = Some((ext_line, Vec::new())),
            }
            Some(seq)
        } else {
            None
        };
        Ok(Background {
            value,
            claim,
            external_cursor,
            event_bus: (!nested).then(|| Arc::clone(&self.event_bus)),
            _workspace: workspace.into_inner(),
        })
    }

    fn list(
        &self,
        call: &Call<'_>,
        view: &View,
        scope: ListScope,
        state: Option<ListState>,
        limit: usize,
    ) -> Result<(CallToolResult, Option<Lease>, crate::lease::Owner), ServiceError> {
        if call.cancel.is_cancelled() {
            return Err("Cancelled while listing tasks; retry list.".into());
        }
        let session = call.session;
        // Declared before every candidate: all text drops before its workspace on error.
        let workspace = std::cell::RefCell::new(Lease::empty(call.launch().control_memory()));
        resize_metadata_workspace(&mut workspace.borrow_mut(), 8192, call, Action::List)
            .map_err(|message| ServiceError::Overload(message, None))?;
        let active_filter = state.map(|state| state == ListState::Active);
        let (total_count, selected) = self
            .tasks
            .listed(
                session,
                scope == ListScope::All,
                active_filter,
                limit,
                &mut |bytes| {
                    let mut held = workspace.borrow_mut();
                    let target = held.bytes().saturating_add(metadata_workspace_bytes(bytes));
                    resize_metadata_workspace(&mut held, target, call, Action::List)
                },
                &mut |tree| metadata_members(tree, &mut workspace.borrow_mut(), call, Action::List),
            )
            .map_err(|message| ServiceError::Overload(message, None))?;
        let (listed, delivered) = {
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
                let log_path = files::path::display_path(&task.log_path);
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
            (
                suppress(success(text, structured, &suffix), call.render),
                delivered,
            )
        };
        let outbound = measure_outbound_len(call, &listed)?;
        if !call.nested && outbound > call.launch().outbound_limit() {
            return Err(ServiceError::Overload(
                format!(
                    "The list response was {outbound} bytes, over the frame limit; retry list with a smaller limit."
                ),
                None,
            ));
        }
        let payload = result_payload_capacity(&listed);
        let wire = if call.nested {
            nested_wire_bytes(&listed).map_err(|message| ServiceError::Overload(message, None))?
        } else if call.render_slot.is_some() {
            wire_result_len(call.call_id, &listed)?
        } else {
            outbound.saturating_mul(if call.http_response { 0 } else { 1 })
        };
        let retained = payload.saturating_add(wire);
        if retained > workspace.borrow().bytes() {
            resize_metadata_workspace(&mut workspace.borrow_mut(), retained, call, Action::List)
                .map_err(|message| ServiceError::Overload(message, None))?;
        }
        if call.cancel.is_cancelled() {
            return Err("Cancelled while listing tasks; retry list.".into());
        }
        if call.nested {
            reserve_nested_bridge(call, wire)
                .map_err(|message| ServiceError::Overload(message, None))?;
        }
        // Only an admitted candidate commits the entries it actually displays.
        for index in delivered {
            let (task, snapshot) = &selected[index];
            if let Some(entry) = session.existing(task) {
                entry.mark_reported(snapshot);
                entry.mark_keeps_reported(snapshot);
            }
        }
        drop(selected);
        let mut held = workspace.into_inner();
        held.shrink_to(retained);
        let owner = crate::lease::Owner::new(held.split_off(payload));
        Ok((listed, Some(held), owner))
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
    input_owner: &crate::lease::Owner,
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
        task.send(Some(&bytes), false, cancel, input_owner.clone())
            .await
            .map_err(sent)?;
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
pub(super) struct StagedRead {
    pub(super) guard: tokio::sync::OwnedMutexGuard<ReadState>,
    pub(super) state: ReadState,
}

impl StagedRead {
    pub(super) fn new(guard: tokio::sync::OwnedMutexGuard<ReadState>) -> Self {
        let state = guard.clone();
        Self { guard, state }
    }

    pub(super) fn into_guard(self) -> tokio::sync::OwnedMutexGuard<ReadState> {
        self.guard
    }

    pub(super) fn commit(mut self) -> tokio::sync::OwnedMutexGuard<ReadState> {
        *self.guard = self.state;
        self.guard
    }
}

/// A cancelled render drops its staged state while its blocking read still owns the gate.
async fn read_result<T>(
    cancel: &CancellationToken,
    result: impl std::future::Future<Output = Result<T, ServiceError>>,
) -> Result<T, ServiceError> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(ServiceError::Plain("Cancelled while reading output; use poll to continue.".into())),
        result = result => result,
    }
}
