//! Core execution engine for `exec` and `codemode` tasks, split into shared preparation
//! (`PreparedExec`) and sibling Agent (`render.rs`) and Program (`codemode/encode.rs`) frontends.

pub(crate) mod render;
pub(crate) mod schema;
#[cfg(test)]
mod tests;

use self::render::{
    RenderSlot, RenderSnapshot, ScriptReport, codemode_error, error_result, metadata_members,
    metadata_workspace_bytes, overload_task_message, resize_metadata_workspace, schema_error,
    script_report, text_workspace_bytes,
};
use self::schema::{
    Action, DEFAULT_KEY_DELAY_MS, ExecPolicy, ListScope, ListState, MAX_INPUT_BYTES, Request, View,
    flag, normalize_request, validate, validate_cwd,
};
use crate::lease::Lease;
use crate::platform::{conpty, process};
use crate::session::{
    LaunchContext, ProbeWarning, ReadState, Reader, Session, TaskEntry, parse_handle,
    parse_handle_for,
};
use crate::tasks::state::{AttachmentLease, Task};
use crate::tasks::{
    self, EVICTED_TRANSCRIPT, ListQuery, ListedResult, ListedTask, ScriptArgs, StartArgs, Tasks,
};
use crate::term::keys;
use crate::term::output::{Cleaner, Truncate, Window, WindowBuilder, WindowSpec};
use crate::wait::{self, WaitEnd};
use crate::{codemode, files};
use serde_json::Value;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, watch};
use tokio_util::sync::CancellationToken;

const KILL_WAIT: Duration = Duration::from_secs(5);
const DEFAULT_LIST_LIMIT: u64 = 20;
/// Screen reads only need stream decoding diagnostics and the updated cursor.
const MIN_BODY_BUDGET: usize = 256;

/// Shared core execution context for both Agent and Program calls.
pub struct ExecContext<'a> {
    pub session: &'a Arc<Session>,
    pub launch: Option<&'a Arc<LaunchContext>>,
    pub cancel: CancellationToken,
    /// The call replaces this with its latest progress message; a forwarder sends them.
    pub progress: Option<watch::Sender<Option<(f64, String)>>>,
    /// The daemon's call ID, kept on a task a start creates so `list` can name the call that
    /// started it. The stdio adapter has no call IDs, so it passes `None`.
    pub call_id: Option<u64>,
    pub parent_task_id: Option<String>,
    /// Tracks an admitted nested `exec start` until it enters `registry.tasks`.
    pub queued_guard: Option<codemode::control::QueuedStartGuard>,
    /// Nested script handle; records starts before cancellation and checks task ownership.
    pub on_started: Option<&'a codemode::control::ScriptHandle>,
    /// Keeps decoded arguments alive through worker-owned copies after caller cancellation.
    pub input_owner: crate::lease::Owner,
}

impl<'a> ExecContext<'a> {
    pub(crate) fn launch(&self) -> &'a Arc<LaunchContext> {
        self.launch.unwrap_or(&self.session.launch)
    }
}

/// Agent-only delivery parameters (MCP stdio / HTTP / Daemon IPC).
#[derive(Default)]
pub struct AgentDelivery<'a> {
    pub(crate) render_slot: Option<&'a RenderSlot<'a>>,
    pub http_response: bool,
    /// Encoded outer JSON-RPC ID length, known before committing output cursors.
    pub rpc_id_bytes: usize,
}

/// Program-only delivery parameters (nested `codemode` JS bridge).
pub(crate) struct ProgramDelivery<'a> {
    pub script: &'a codemode::control::ScriptHandle,
    pub pending_id: u64,
    pub is_control: bool,
}

#[derive(Debug)]
pub(crate) enum ServiceError {
    Validation(String),
    NotFound(String),
    Cancelled(String),
    Overload(String, Option<files::TaskMeta>),
    Plain(String),
}

impl ServiceError {
    pub(crate) fn message(&self) -> &str {
        match self {
            Self::Validation(s)
            | Self::NotFound(s)
            | Self::Cancelled(s)
            | Self::Overload(s, _)
            | Self::Plain(s) => s,
        }
    }
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
        self.message()
    }
}

pub(crate) fn task_read_error(message: String, task: &Task) -> ServiceError {
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

/// Two-phase execution output shared between Agent Presenter and Program Encoder.
pub(crate) enum PreparedExec {
    Window(Box<PreparedWindow>),
    Transcript(Box<PreparedTranscript>),
    Screen(Box<PreparedScreen>),
    List(PreparedList),
}

pub(crate) struct PreparedWindow {
    // Drop-order invariant: `workspace` and `permit` MUST be declared before `read` (`read_gate`).
    pub(crate) workspace: Lease,
    pub(crate) permit: Option<Arc<OwnedSemaphorePermit>>,
    pub(crate) metadata: RenderSnapshot,
    pub(crate) script: Option<ScriptReport>,
    pub(crate) unseen: Vec<AttachmentLease>,
    pub(crate) window: Window,
    pub(crate) had_new_text: bool,
    pub(crate) initial_seen: usize,
    pub(crate) action: Action,
    pub(crate) task: Arc<Task>,
    pub(crate) entry: Arc<TaskEntry>,
    pub(crate) reader: Arc<Reader>,
    pub(crate) view: View,
    pub(crate) requested_max_bytes: usize,
    pub(crate) ended: Option<WaitEnd>,
    pub(crate) startup_warning: Option<ProbeWarning>,
    pub(crate) mark_session_entry: bool,
    pub(crate) read: StagedRead,
}

pub(crate) struct PreparedTranscript {
    // Drop-order invariant: `workspace` and `permit` MUST be declared before `read`.
    pub(crate) workspace: Lease,
    pub(crate) permit: Option<Arc<OwnedSemaphorePermit>>,
    pub(crate) metadata: RenderSnapshot,
    pub(crate) window: Window,
    pub(crate) transcript_path: String,
    pub(crate) transcript_lines: u64,
    pub(crate) final_lines: u64,
    pub(crate) first_line: u64,
    pub(crate) transcript_incomplete: bool,
    pub(crate) transcript_error: Option<String>,
    pub(crate) transcript_warnings: Vec<String>,
    pub(crate) task: Arc<Task>,
    pub(crate) entry: Arc<TaskEntry>,
    pub(crate) reader: Arc<Reader>,
    pub(crate) view: View,
    pub(crate) mark_session_entry: bool,
    pub(crate) read: StagedRead,
}

pub(crate) struct PreparedScreen {
    // Drop-order invariant: `workspace` and `permit` MUST be declared before `read`.
    pub(crate) workspace: Lease,
    pub(crate) permit: Option<Arc<OwnedSemaphorePermit>>,
    pub(crate) metadata: RenderSnapshot,
    pub(crate) screen_rows: Vec<String>,
    pub(crate) cols: u16,
    pub(crate) rows_count: u16,
    pub(crate) cursor: (u16, u16),
    pub(crate) window: Window,
    pub(crate) action: Action,
    pub(crate) task: Arc<Task>,
    pub(crate) entry: Arc<TaskEntry>,
    pub(crate) reader: Arc<Reader>,
    pub(crate) view: View,
    pub(crate) ended: WaitEnd,
    pub(crate) startup_warning: Option<ProbeWarning>,
    pub(crate) mark_session_entry: bool,
    pub(crate) read: StagedRead,
}

pub(crate) struct PreparedList {
    pub(crate) workspace: Lease,
    pub(crate) permit: Option<Arc<OwnedSemaphorePermit>>,
    pub(crate) total_count: usize,
    pub(crate) items: Vec<ListedTask>,
    pub(crate) parent_filter: Option<String>,
    pub(crate) state_filter: Option<ListState>,
    pub(crate) view: View,
}

impl PreparedList {
    /// Marks every selected task, active child, and folded child reported in `session`.
    pub(crate) fn commit_all(self, session: &Session) {
        for item in &self.items {
            if let Some(entry) = session.existing(&item.task) {
                entry.mark_reported(&item.snapshot);
                entry.mark_keeps_reported(&item.snapshot);
            }
            if let Some(children) = &item.children {
                for (child, snapshot) in &children.active_tasks {
                    if let Some(entry) = session.existing(child) {
                        entry.mark_reported(snapshot);
                        entry.mark_keeps_reported(snapshot);
                    }
                }
                for (child, tree_ended) in &children.folded_finished {
                    if let Some(entry) = session.existing(child) {
                        entry.mark_finished_reported(*tree_ended);
                    }
                }
            }
        }
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

    /// Agent entrypoint for `exec`: returns pure-text/media `CallToolResult` with `structured_content: None`.
    pub async fn exec_agent(
        &self,
        request: Request,
        mut ctx: ExecContext<'_>,
        delivery: AgentDelivery<'_>,
    ) -> files::TrackedResult {
        self.tasks.prune_entries(ctx.session);
        let action = request.action;
        match self
            .exec_core(request, ExecPolicy::AGENT, &mut ctx, delivery.render_slot)
            .await
        {
            Ok(prepared) => match self.render_agent(prepared, &ctx, &delivery).await {
                Ok((result, lease, task_meta, payload_owner)) => files::TrackedResult {
                    result,
                    lease,
                    overload: false,
                    task_meta,
                    payload_owner,
                },
                Err(ServiceError::Overload(message, task_meta)) => files::TrackedResult {
                    result: error_result(action, &message),
                    lease: None,
                    overload: true,
                    task_meta,
                    payload_owner: Default::default(),
                },
                Err(err) => files::TrackedResult::plain(error_result(action, err.message())),
            },
            Err(ServiceError::Overload(message, task_meta)) => files::TrackedResult {
                result: error_result(action, &message),
                lease: None,
                overload: true,
                task_meta,
                payload_owner: Default::default(),
            },
            Err(err) => files::TrackedResult::plain(error_result(action, err.message())),
        }
    }

    /// Agent entrypoint for raw JSON `exec` arguments (daemon IPC / HTTP).
    pub async fn exec_agent_json(
        &self,
        arguments: Value,
        ctx: ExecContext<'_>,
        delivery: AgentDelivery<'_>,
    ) -> files::TrackedResult {
        match serde_json::from_value::<Request>(arguments) {
            Ok(request) => self.exec_agent(request, ctx, delivery).await,
            Err(error) => schema_error(format!("failed to deserialize parameters: {error}")),
        }
    }

    /// Agent entrypoint for `codemode`: starts a script task and renders its Agent output.
    pub async fn codemode_agent(
        self: &Arc<Self>,
        request: codemode::control::Request,
        ctx: ExecContext<'_>,
        delivery: AgentDelivery<'_>,
        transfer: bool,
    ) -> files::TrackedResult {
        self.tasks.prune_entries(ctx.session);
        match self
            .prepare_script(request, &ctx, delivery.render_slot, transfer)
            .await
        {
            Ok(prepared) => match self.render_agent(prepared, &ctx, &delivery).await {
                Ok((result, lease, task_meta, payload_owner)) => files::TrackedResult {
                    result,
                    lease,
                    overload: false,
                    task_meta,
                    payload_owner,
                },
                Err(ServiceError::Overload(message, task_meta)) => files::TrackedResult {
                    result: codemode_error(&message),
                    lease: None,
                    overload: true,
                    task_meta,
                    payload_owner: Default::default(),
                },
                Err(err) => files::TrackedResult::plain(codemode_error(err.message())),
            },
            Err(ServiceError::Overload(message, task_meta)) => files::TrackedResult {
                result: codemode_error(&message),
                lease: None,
                overload: true,
                task_meta,
                payload_owner: Default::default(),
            },
            Err(err) => files::TrackedResult::plain(codemode_error(err.message())),
        }
    }

    /// Program entrypoint for nested `tools.exec` inside `codemode`.
    pub(crate) async fn exec_program(
        &self,
        request: Request,
        mut ctx: ExecContext<'_>,
        delivery: ProgramDelivery<'_>,
    ) -> codemode::encode::ProgramOutcome {
        self.tasks.prune_entries(ctx.session);
        match self
            .exec_core(request, ExecPolicy::PROGRAM, &mut ctx, None)
            .await
        {
            Ok(prepared) => codemode::encode::encode_exec_program(prepared, &ctx, &delivery),
            Err(err) => codemode::encode::encode_service_error(err, &ctx, &delivery),
        }
    }

    async fn prepare_script(
        self: &Arc<Self>,
        request: codemode::control::Request,
        ctx: &ExecContext<'_>,
        render_slot: Option<&RenderSlot<'_>>,
        transfer: bool,
    ) -> Result<PreparedExec, ServiceError> {
        let base_launch = ctx.launch();
        let (start, resolved_cwd) =
            codemode::control::validate(&request, base_launch).map_err(ServiceError::Validation)?;
        let view = validate(&start, ExecPolicy::AGENT).map_err(ServiceError::Validation)?;
        let launch = match resolved_cwd {
            None => Arc::clone(base_launch),
            Some(dir) => Arc::new(base_launch.with_cwd(dir)),
        };
        let args = ScriptArgs {
            code: request.code,
            launch,
            kill_after_ms: start.kill_after_ms,
            transfer,
            call_id: ctx.call_id,
            service: Arc::clone(self),
            input_owner: ctx.input_owner.clone(),
        };
        let tasks = Arc::clone(&self.tasks);
        let session = Arc::clone(ctx.session);
        let key =
            tasks::random_chars(4).map_err(|error| format!("Cannot name the reader: {error}"))?;
        let started = tokio::task::spawn_blocking(move || tasks.start_script(args, &session, key))
            .await
            .map_err(|error| format!("Internal failure while starting: {error}."))??;
        self.wait_and_prepare_started(
            started,
            None,
            Some(Duration::from_millis(start.wait_ms.unwrap_or(30_000))),
            start.return_when.as_ref(),
            view,
            ctx,
            render_slot,
            true,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn wait_and_prepare_started(
        &self,
        started: tasks::Started,
        startup_warning: Option<ProbeWarning>,
        wait_duration: Option<Duration>,
        when: Option<&wait::ReturnWhen>,
        view: View,
        ctx: &ExecContext<'_>,
        render_slot: Option<&RenderSlot<'_>>,
        is_agent: bool,
    ) -> Result<PreparedExec, ServiceError> {
        let (task, entry, reader) = started;
        let state = read_gate(&reader, &ctx.cancel).await?;
        let mark = wait::mark(&task, &reader, &state, when, ctx.launch().memory())
            .map_err(|error| task_read_error(error, &task))?;
        let encoding = view.encoding.unwrap_or(task.encoding);
        let ended = wait::wait(
            &task,
            wait_duration,
            when,
            mark,
            encoding,
            &ctx.cancel,
            ctx.progress.as_ref(),
        )
        .await
        .map_err(|error| task_read_error(error, &task))?;
        read_result(
            &ctx.cancel,
            self.prepare_window_or_screen(
                ctx,
                render_slot,
                Action::Start,
                task,
                entry,
                reader,
                state,
                view,
                Some(ended),
                startup_warning,
                is_agent,
            ),
        )
        .await
    }

    async fn exec_core(
        &self,
        mut request: Request,
        policy: ExecPolicy,
        ctx: &mut ExecContext<'_>,
        render_slot: Option<&RenderSlot<'_>>,
    ) -> Result<PreparedExec, ServiceError> {
        let is_agent = policy.inline_markers;
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
            return Err(ServiceError::Validation(
                "A codemode task takes no input, eof, keys, resize, screen, raw, or encoding; poll it with waitMs and returnWhen."
                    .into(),
            ));
        }
        let mut view = validate(&request, policy).map_err(ServiceError::Validation)?;
        let session = Arc::clone(ctx.session);
        match request.action {
            Action::List => {
                let permit = render_permit(render_slot, &ctx.cancel).await?;
                self.prepare_list(
                    ctx,
                    view,
                    request.scope,
                    request.state,
                    request.parent_task_id.as_deref(),
                    request.limit.unwrap_or(DEFAULT_LIST_LIMIT) as usize,
                    permit,
                )
            }
            Action::Start => {
                let base_launch = ctx.launch();
                let cwd = match request.cwd {
                    None => base_launch.cwd.clone(),
                    Some(dir) => {
                        validate_cwd(&dir, base_launch).map_err(ServiceError::Validation)?
                    }
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
                    started_by: ctx.call_id.map(|id| (session.id.clone(), id)),
                    parent_task_id: ctx.parent_task_id.clone(),
                    queued_guard: ctx.queued_guard.take(),
                    launch: Arc::clone(base_launch),
                    input_owner: ctx.input_owner.clone(),
                };
                let tasks = Arc::clone(&self.tasks);
                let starting = Arc::clone(&session);
                let key = tasks::random_chars(4)
                    .map_err(|error| format!("Cannot name the reader: {error}"))?;
                if !is_agent && ctx.cancel.is_cancelled() {
                    return Err(ServiceError::Cancelled(
                        "Cancelled before the command was started.".into(),
                    ));
                }
                let ((task, entry, task_reader), startup_warning) =
                    tokio::task::spawn_blocking(move || tasks.start(args, &starting, key))
                        .await
                        .map_err(|error| format!("Internal failure while starting: {error}."))??;
                if let Some(script) = ctx.on_started {
                    script.record_started(task_reader.handle(&task.id));
                    view.owned_by_script = true;
                }
                let wait = policy
                    .resolve_wait_ms(request.wait_ms, request.return_when.is_some(), false)
                    .map(Duration::from_millis);
                self.wait_and_prepare_started(
                    (task, entry, task_reader),
                    startup_warning,
                    wait,
                    request.return_when.as_ref(),
                    view,
                    ctx,
                    render_slot,
                    is_agent,
                )
                .await
            }
            Action::Poll => {
                let (task, key) = self.task(request.task_id.as_deref())?;
                view.owned_by_script = ctx.on_started.is_some_and(|script| script.owns(&task.id));
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
                    return Err(ServiceError::Validation(format!(
                        "{name} applies to PTY tasks only; in pipe mode, write text with input and end lines with a newline."
                    )));
                }
                if let Some(name) = view.screen.then_some("screen").or(watches_screen)
                    && task.encoding != encoding_rs::UTF_8
                {
                    return Err(ServiceError::Validation(format!(
                        "{name} needs UTF-8 output; this task was started with an encoding."
                    )));
                }
                if flag(request.eof) && task.pty {
                    return Err(ServiceError::Validation(
                        "EOF_UNSUPPORTED_IN_PTY: a PTY has no stdin half-close. Press Ctrl-D with keys: [\"C-d\"], or kill the task."
                            .into(),
                    ));
                }
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
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(ServiceError::Validation)?;
                let entry = session.entry(&task);
                let task_reader = entry.reader(key).map_err(ServiceError::Validation)?;
                let state = read_gate(&task_reader, &ctx.cancel).await?;
                let sends = request.input.is_some()
                    || request.keys.is_some()
                    || flag(request.eof)
                    || axes.is_some();
                let gate = if sends {
                    Some(tokio::select! {
                        biased;
                        _ = ctx.cancel.cancelled() => {
                            return Err(ServiceError::Cancelled(
                                "Cancelled before the input was sent; the task keeps running.".into()
                            ));
                        }
                        gate = task.input_gate.lock() => gate,
                    })
                } else {
                    None
                };
                let encode = |task: &Task| -> Result<Vec<u8>, ServiceError> {
                    let mut data = input_bytes.to_vec();
                    if !names.is_empty() {
                        let modes = task.modes().map_err(ServiceError::Plain)?;
                        for (name, key) in names.iter().zip(parsed.iter().copied()) {
                            data.extend(
                                keys::encode(name, key, modes).map_err(ServiceError::Validation)?,
                            );
                        }
                    }
                    if data.len() > MAX_INPUT_BYTES {
                        return Err(ServiceError::Validation(
                            "input and keys together exceed 16 KiB; send them in smaller pieces."
                                .into(),
                        ));
                    }
                    Ok(data)
                };
                let mut data = encode(&task)?;
                let mark = wait::mark(&task, &task_reader, &state, when, ctx.launch().memory())
                    .map_err(|error| task_read_error(error, &task))?;
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
                    if axes.is_some() && !names.is_empty() {
                        data = encode(&task)?;
                    }
                    let burst = (input.is_some() || !data.is_empty()).then_some(data.as_slice());
                    task.send(
                        burst,
                        flag(request.eof),
                        &ctx.cancel,
                        ctx.input_owner.clone(),
                    )
                    .await?;
                } else {
                    task.send(
                        input,
                        flag(request.eof),
                        &ctx.cancel,
                        ctx.input_owner.clone(),
                    )
                    .await?;
                    press(
                        &task,
                        input_bytes,
                        names,
                        parsed,
                        delay,
                        &ctx.cancel,
                        &ctx.input_owner,
                    )
                    .await?;
                }
                drop(gate);
                let interacts = request.input.is_some() || request.keys.is_some() || axes.is_some();
                let wait = policy
                    .resolve_wait_ms(request.wait_ms, when.is_some(), interacts)
                    .map(Duration::from_millis);
                let encoding = view.encoding.unwrap_or(task.encoding);
                let ended = wait::wait(
                    &task,
                    wait,
                    when,
                    mark,
                    encoding,
                    &ctx.cancel,
                    ctx.progress.as_ref(),
                )
                .await
                .map_err(|error| task_read_error(error, &task))?;
                read_result(
                    &ctx.cancel,
                    self.prepare_window_or_screen(
                        ctx,
                        render_slot,
                        Action::Poll,
                        task,
                        entry,
                        task_reader,
                        state,
                        view,
                        Some(ended),
                        None,
                        is_agent,
                    ),
                )
                .await
            }
            Action::Kill => {
                let (task, key) = self.task(request.task_id.as_deref())?;
                view.owned_by_script = ctx.on_started.is_some_and(|script| script.owns(&task.id));
                let entry = session.entry(&task);
                let task_reader = entry.reader(key).map_err(ServiceError::Validation)?;
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
                    _ = ctx.cancel.cancelled() => {
                        return Err(ServiceError::Cancelled(
                            "Cancelled while ending the task; use poll to see its state.".into()
                        ));
                    }
                    _ = tokio::time::timeout(KILL_WAIT, finished) => {}
                }
                let state = read_gate(&task_reader, &ctx.cancel).await?;
                read_result(
                    &ctx.cancel,
                    self.prepare_window_or_screen(
                        ctx,
                        render_slot,
                        Action::Kill,
                        task,
                        entry,
                        task_reader,
                        state,
                        view,
                        None,
                        None,
                        is_agent,
                    ),
                )
                .await
            }
            Action::Transcript => {
                let (task, key) = self.task(request.task_id.as_deref())?;
                view.owned_by_script = ctx.on_started.is_some_and(|script| script.owns(&task.id));
                if task.is_script() {
                    return Err(ServiceError::Validation(
                        "A codemode task has no terminal or transcript; poll returns its output."
                            .into(),
                    ));
                }
                if !task.pty {
                    return Err(ServiceError::Validation(
                        "transcript applies to PTY tasks only; poll returns a pipe task's output, and its log holds every byte."
                            .into(),
                    ));
                }
                if task.encoding != encoding_rs::UTF_8 {
                    return Err(ServiceError::Validation(
                        "transcript needs UTF-8 output; this task was started with an encoding."
                            .into(),
                    ));
                }
                let entry = session.entry(&task);
                let task_reader = entry.reader(key).map_err(ServiceError::Validation)?;
                let state = read_gate(&task_reader, &ctx.cancel).await?;
                read_result(
                    &ctx.cancel,
                    self.prepare_transcript(
                        ctx,
                        render_slot,
                        task,
                        entry,
                        task_reader,
                        state,
                        view,
                        is_agent,
                    ),
                )
                .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_window_or_screen(
        &self,
        ctx: &ExecContext<'_>,
        render_slot: Option<&RenderSlot<'_>>,
        action: Action,
        task: Arc<Task>,
        entry: Arc<TaskEntry>,
        task_reader: Arc<Reader>,
        state: tokio::sync::OwnedMutexGuard<ReadState>,
        view: View,
        ended: Option<WaitEnd>,
        startup_warning: Option<ProbeWarning>,
        is_agent: bool,
    ) -> Result<PreparedExec, ServiceError> {
        let mark_session_entry = is_agent || view.owned_by_script;
        let permit =
            task_render_permit(render_slot, &ctx.cancel, action, &task, &task_reader).await?;
        if view.screen {
            return self
                .prepare_screen(
                    ctx,
                    action,
                    task,
                    entry,
                    task_reader,
                    state,
                    view,
                    ended.expect("screen applies to start and poll"),
                    startup_warning,
                    permit,
                    mark_session_entry,
                )
                .await;
        }
        let reader = Arc::clone(&task);
        let launch = ctx.launch();
        let pool = Arc::clone(launch.memory());
        let requested_max_bytes = view.max_bytes;
        let mut view = view;
        let workspace = pool
            .try_acquire(text_workspace_bytes(view.max_bytes))
            .or_else(|| {
                if action != Action::Kill {
                    return None;
                }
                view.max_bytes = view.max_bytes.min(64 << 10);
                launch
                    .control_memory()
                    .try_acquire(text_workspace_bytes(view.max_bytes))
            })
            .ok_or_else(|| {
                let meta = files::TaskMeta {
                    bare_task_id: task.id.clone(),
                    log_path: files::path::display_path(&task.log_path),
                };
                ServiceError::Overload(overload_task_message(pool.limit(), &meta), Some(meta))
            })?;
        let encoding = view.encoding.unwrap_or(task.encoding);
        let mut metadata = RenderSnapshot::new(&task, launch, action)?;
        let clean = !task.is_script();
        let keep_nul = view.keep_nul;
        let include_markers = view.inline_markers;
        let budget = view.max_bytes;
        let initial_seen = state.attachments_seen;
        let truncate = view.truncate;
        let raw = view.raw;
        // Tuple drops left-to-right: `workspace` and `permit` release before `read` (`read_gate`).
        let (window, unseen, had_new_text, workspace, permit, read) =
            tokio::task::spawn_blocking(move || {
                let workspace = workspace;
                let mut read = StagedRead::new(state);
                let start = read.state.cursor;
                let window = reader.read_window_with_markers(
                    &mut read.state,
                    truncate,
                    budget,
                    raw,
                    clean,
                    keep_nul,
                    include_markers,
                    encoding,
                    None,
                );
                let had_new_text = read.state.cursor > start;
                let unseen = if window.is_ok() {
                    reader.unseen_attachments(initial_seen)
                } else {
                    Vec::new()
                };
                (window, unseen, had_new_text, workspace, permit, read)
            })
            .await
            .map_err(|error| format!("Internal failure while reading output: {error}."))?;
        let window = window.map_err(|error| format!("Cannot read the task log: {error}."))?;
        metadata.refresh(&task, launch, action)?;
        let script = script_report(&self.tasks, &task, launch, action)?;
        Ok(PreparedExec::Window(Box::new(PreparedWindow {
            workspace,
            permit,
            metadata,
            script,
            unseen,
            window,
            had_new_text,
            initial_seen,
            action,
            task,
            entry,
            reader: task_reader,
            view,
            requested_max_bytes,
            ended,
            startup_warning,
            mark_session_entry,
            read,
        })))
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_screen(
        &self,
        ctx: &ExecContext<'_>,
        action: Action,
        task: Arc<Task>,
        entry: Arc<TaskEntry>,
        task_reader: Arc<Reader>,
        state: tokio::sync::OwnedMutexGuard<ReadState>,
        view: View,
        ended: WaitEnd,
        startup_warning: Option<ProbeWarning>,
        permit: Option<Arc<OwnedSemaphorePermit>>,
        mark_session_entry: bool,
    ) -> Result<PreparedExec, ServiceError> {
        let launch = ctx.launch();
        let metadata = RenderSnapshot::new(&task, launch, action)?;
        let screen = task
            .screen(launch.memory())
            .map_err(|error| task_read_error(error, &task))?;
        let (reader, until) = (Arc::clone(&task), screen.log_end);
        // Tuple drops left-to-right: `screen` (lease) and `permit` release before `read`.
        let (window, screen, permit, read) = tokio::task::spawn_blocking(move || {
            let owned_screen = screen;
            let mut read = StagedRead::new(state);
            let window = reader.read_window_with_markers(
                &mut read.state,
                Truncate::Tail,
                MIN_BODY_BUDGET,
                false,
                true,
                false,
                true,
                reader.encoding,
                Some(until),
            );
            (window, owned_screen, permit, read)
        })
        .await
        .map_err(|error| format!("Internal failure while reading output: {error}."))?;
        let window = window.map_err(|error| format!("Cannot read the task log: {error}."))?;
        Ok(PreparedExec::Screen(Box::new(PreparedScreen {
            workspace: screen._lease,
            permit,
            metadata,
            screen_rows: screen.rows,
            cols: screen.cols,
            rows_count: screen.rows_count,
            cursor: screen.cursor,
            window,
            action,
            task,
            entry,
            reader: task_reader,
            view,
            ended,
            startup_warning,
            mark_session_entry,
            read,
        })))
    }

    #[allow(clippy::too_many_arguments)]
    async fn prepare_transcript(
        &self,
        ctx: &ExecContext<'_>,
        render_slot: Option<&RenderSlot<'_>>,
        task: Arc<Task>,
        entry: Arc<TaskEntry>,
        task_reader: Arc<Reader>,
        state: tokio::sync::OwnedMutexGuard<ReadState>,
        view: View,
        is_agent: bool,
    ) -> Result<PreparedExec, ServiceError> {
        let mark_session_entry = is_agent || view.owned_by_script;
        let launch = ctx.launch();
        let permit = task_render_permit(
            render_slot,
            &ctx.cancel,
            Action::Transcript,
            &task,
            &task_reader,
        )
        .await?;
        let metadata = RenderSnapshot::new(&task, launch, Action::Transcript)?;
        let (reader, tasks) = (Arc::clone(&task), Arc::clone(&self.tasks));
        let first_permit = permit.clone();
        let memory = Arc::clone(launch.memory());
        let workspace = memory
            .try_acquire(text_workspace_bytes(view.max_bytes))
            .ok_or_else(|| {
                let meta = files::TaskMeta {
                    bare_task_id: task.id.clone(),
                    log_path: files::path::display_path(&task.log_path),
                };
                ServiceError::Overload(overload_task_message(memory.limit(), &meta), Some(meta))
            })?;
        let (transcript, workspace, _first_permit, read) = tokio::task::spawn_blocking(move || {
            let owned_workspace = workspace;
            let read = StagedRead::new(state);
            let transcript = reader.transcript(&tasks, &read.state, &memory);
            (transcript, owned_workspace, first_permit, read)
        })
        .await
        .map_err(|error| format!("Internal failure while reading: {error}."))?;
        let transcript = transcript.map_err(|error| task_read_error(error, &task))?;
        let pending_lines = transcript.pending.matches('\n').count() as u64;
        let total = transcript.lines + pending_lines;
        let transcript_path = files::path::display_path(&transcript.path);
        let final_lines = transcript.lines;
        let first_line = transcript.first_line;
        let transcript_incomplete =
            transcript.cut || transcript.stopped.is_some() || conpty::load().is_some();
        let transcript_error = transcript.stopped.clone();
        let mut transcript_warnings = Vec::new();
        if transcript.cut {
            transcript_warnings
                .push("pending lines past the transcript's 64 MiB limit are left out".into());
        }
        if let Some(reason) = &transcript.stopped {
            transcript_warnings.push(format!(
                "the transcript stops after line {}: {reason}",
                transcript.lines
            ));
        }
        if let Some(warning) = conpty::load() {
            transcript_warnings.push(format!("{warning}, so transcript lines may be missing"));
        }
        let spec = WindowSpec {
            truncate: view.truncate,
            budget: view.max_bytes,
            raw: false,
            clean: true,
            keep_nul: false,
            include_markers: view.inline_markers,
            encoding: encoding_rs::UTF_8,
            source: "transcript",
        };
        let logged = Arc::clone(&task);
        let (window, (snapshot_owner, workspace), permit, read) =
            tokio::task::spawn_blocking(move || {
                let held = (transcript._lease, workspace);
                let mut read = read;
                fn read_into(
                    path: &std::path::Path,
                    start: u64,
                    bytes: u64,
                    builder: &mut WindowBuilder<'_>,
                ) -> std::io::Result<()> {
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
                (window, held, permit, read)
            })
            .await
            .map_err(|error| format!("Internal failure while reading: {error}."))?;
        drop(snapshot_owner);
        let window = window?;
        Ok(PreparedExec::Transcript(Box::new(PreparedTranscript {
            workspace,
            permit,
            metadata,
            window,
            transcript_path,
            transcript_lines: total,
            final_lines,
            first_line,
            transcript_incomplete,
            transcript_error,
            transcript_warnings,
            task,
            entry,
            reader: task_reader,
            view,
            mark_session_entry,
            read,
        })))
    }

    #[allow(clippy::too_many_arguments)]
    fn prepare_list(
        &self,
        ctx: &ExecContext<'_>,
        view: View,
        scope: Option<ListScope>,
        state: Option<ListState>,
        parent_task_id: Option<&str>,
        limit: usize,
        permit: Option<Arc<OwnedSemaphorePermit>>,
    ) -> Result<PreparedExec, ServiceError> {
        if ctx.cancel.is_cancelled() {
            return Err(ServiceError::Cancelled(
                "Cancelled while listing tasks; retry list.".into(),
            ));
        }
        let parent_id = match parent_task_id {
            Some(raw) => Some(
                parse_handle_for(raw, "parentTaskId")
                    .map_err(ServiceError::Validation)?
                    .0,
            ),
            None => None,
        };
        let all_scope = match scope {
            Some(ListScope::All) => true,
            Some(ListScope::Session) => false,
            None => parent_id.is_some(),
        };
        let launch = ctx.launch();
        let workspace = std::cell::RefCell::new(Lease::empty(launch.control_memory()));
        resize_metadata_workspace(&mut workspace.borrow_mut(), 8192, launch, Action::List)
            .map_err(|message| ServiceError::Overload(message, None))?;
        let active_filter = state.map(|state| state == ListState::Active);
        let ListedResult { total_count, items } = match self
            .tasks
            .listed(
                ctx.session,
                ListQuery {
                    all_scope,
                    active_filter,
                    parent_task_id: parent_id,
                    limit,
                },
                &mut |bytes| {
                    let mut held = workspace.borrow_mut();
                    let target = held.bytes().saturating_add(metadata_workspace_bytes(bytes));
                    resize_metadata_workspace(&mut held, target, launch, Action::List)
                },
                &mut |tree| {
                    metadata_members(tree, &mut workspace.borrow_mut(), launch, Action::List)
                },
            )
            .map_err(|message| ServiceError::Overload(message, None))?
        {
            Some(result) => result,
            None => {
                return Err(ServiceError::NotFound(
                    self.unknown_task_error("parentTaskId", parent_id.unwrap_or_default()),
                ));
            }
        };
        Ok(PreparedExec::List(PreparedList {
            workspace: workspace.into_inner(),
            permit,
            total_count,
            items,
            parent_filter: parent_id.map(str::to_owned),
            state_filter: state,
            view,
        }))
    }

    fn unknown_task_error(&self, field: &str, id: &str) -> String {
        let epoch_differs = tasks::is_valid_task_id(id)
            && id
                .split_once('-')
                .is_some_and(|(epoch, _)| epoch != self.tasks.epoch());
        if self.daemon {
            if epoch_differs {
                format!(
                    "Unknown {field} {id:?}: it comes from an earlier fastexec daemon; tasks do not survive a daemon restart. Use action list to see this daemon's tasks."
                )
            } else {
                format!(
                    "Unknown {field} {id:?}. Finished tasks are kept for up to 24 h; use action list to see this daemon's tasks."
                )
            }
        } else if epoch_differs {
            format!(
                "Unknown {field} {id:?}: it comes from an earlier fastexec server; tasks do not survive a restart. Use action list to see this server's tasks."
            )
        } else {
            format!(
                "Unknown {field} {id:?}. Tasks live as long as this server; use action list to see them."
            )
        }
    }

    fn task<'a>(
        &self,
        handle: Option<&'a str>,
    ) -> Result<(Arc<Task>, Option<&'a str>), ServiceError> {
        let (id, key) =
            parse_handle(handle.unwrap_or_default()).map_err(ServiceError::Validation)?;
        if let Some(task) = self.tasks.find(id) {
            return Ok((task, key));
        }
        Err(ServiceError::NotFound(
            self.unknown_task_error("taskId", id),
        ))
    }
}

async fn render_permit(
    render_slot: Option<&RenderSlot<'_>>,
    cancel: &CancellationToken,
) -> Result<Option<Arc<OwnedSemaphorePermit>>, ServiceError> {
    let Some(slot) = render_slot else {
        return Ok(None);
    };
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(ServiceError::Cancelled(
            "Cancelled while reading output; use poll to continue.".into(),
        )),
        permit = slot.hold() => Ok(Some(permit)),
    }
}

async fn task_render_permit(
    render_slot: Option<&RenderSlot<'_>>,
    cancel: &CancellationToken,
    action: Action,
    task: &Task,
    reader: &Reader,
) -> Result<Option<Arc<OwnedSemaphorePermit>>, ServiceError> {
    let Some(slot) = render_slot else {
        return Ok(None);
    };
    if action == Action::Kill {
        return render_permit(render_slot, cancel).await;
    }
    // Do not wait on ordinary output while holding a reader that kill may need.
    slot.try_hold().map(Some).ok_or_else(|| {
        let recovery = match action {
            Action::Poll => {
                "Poll the same task for output only. Do not resend input, keys, or resize."
            }
            Action::Transcript => "Repeat transcript for the same task or read its log.",
            _ => "Poll the task or read its log. Do not replay the original operation.",
        };
        ServiceError::Plain(format!(
            "The task exists and may still be running. Its output was not read because the daemon is rendering other results.\ntaskId: {}\nlogPath: {}\n{recovery}",
            reader.handle(&task.id),
            files::path::display_path(&task.log_path),
        ))
    })
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
) -> Result<(), ServiceError> {
    let mut size = input.len();
    for (index, (name, key)) in names.iter().zip(parsed).enumerate() {
        let sent = |error: String| format!("{error} The first {index} keys were sent.");
        if index > 0 || !input.is_empty() {
            tokio::select! {
                _ = cancel.cancelled() => {
                    return Err(ServiceError::Cancelled(sent(
                        "Cancelled; the task keeps running.".into(),
                    )));
                }
                _ = tokio::time::sleep(delay) => {}
            }
        }
        let bytes = task
            .modes()
            .and_then(|modes| keys::encode(name, key, modes))
            .map_err(|error| ServiceError::Plain(sent(error)))?;
        size += bytes.len();
        if size > MAX_INPUT_BYTES {
            return Err(ServiceError::Validation(sent(format!(
                "Key {name:?} in the program's current keyboard mode takes input and keys past 16 KiB; send fewer keys per poll."
            ))));
        }
        task.send(Some(&bytes), false, cancel, input_owner.clone())
            .await
            .map_err(|error| ServiceError::Plain(sent(error)))?;
    }
    Ok(())
}

/// A handle's read gate, held by queued waits and blocking reads even after cancellation.
async fn read_gate(
    reader: &Reader,
    cancel: &CancellationToken,
) -> Result<tokio::sync::OwnedMutexGuard<ReadState>, ServiceError> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => {
            Err(ServiceError::Cancelled(
                "Cancelled before the output was read; the task keeps running.".into(),
            ))
        }
        state = reader.read_gate().lock_owned() => Ok(state),
    }
}

/// A blocking read owns the gate, but only a delivered result commits its working copy.
pub(crate) struct StagedRead {
    pub(crate) guard: tokio::sync::OwnedMutexGuard<ReadState>,
    pub(crate) state: ReadState,
}

impl StagedRead {
    pub(crate) fn new(guard: tokio::sync::OwnedMutexGuard<ReadState>) -> Self {
        let state = guard.clone();
        Self { guard, state }
    }

    pub(crate) fn into_guard(self) -> tokio::sync::OwnedMutexGuard<ReadState> {
        self.guard
    }

    pub(crate) fn commit(mut self) -> tokio::sync::OwnedMutexGuard<ReadState> {
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
        _ = cancel.cancelled() => Err(ServiceError::Cancelled(
            "Cancelled while reading output; use poll to continue.".into(),
        )),
        result = result => result,
    }
}
