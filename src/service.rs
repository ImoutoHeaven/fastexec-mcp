//! Core execution engine for `exec` and `codemode` tasks, producing shared `PreparedExec`
//! values for Agent (`crate::render`) and Program (`codemode::encode`) frontends.

pub mod core;
pub(crate) mod schema;
#[cfg(test)]
mod tests;

use self::core::{
    PreparedSnapshot, RenderSlot, ScriptReport, metadata_members, metadata_workspace_bytes,
    overload_task_message, resize_metadata_workspace, script_report, text_workspace_bytes,
};
pub use self::core::{ServiceError, TaskMeta};
use self::schema::{
    Action, DEFAULT_KEY_DELAY_MS, ExecPolicy, ListScope, ListState, MAX_INPUT_BYTES, Request, View,
    flag, normalize_request, validate, validate_cwd,
};
use crate::env::{LaunchContext, ProbeWarning};
use crate::files;
use crate::id;
use crate::lease::Lease;
use crate::platform::{conpty, process};
use crate::tasks::script::{QueuedStartGuard, ScriptControl};
use crate::tasks::session::{Reader, Session, TaskEntry};
use crate::tasks::state::{AttachmentLease, ReadState, Task};
use crate::tasks::{
    self, EVICTED_TRANSCRIPT, ListQuery, ListedResult, ListedTask, StartArgs, Tasks,
};
use crate::term::keys;
use crate::term::output::{Cleaner, Truncate, Window, WindowBuilder, WindowSpec};
use crate::wait::{self, WaitEnd};
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Consumer {
    Agent,
    Program,
}

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
    pub queued_guard: Option<QueuedStartGuard>,
    /// Nested script handle; records starts before cancellation and checks task ownership.
    pub on_started: Option<&'a ScriptControl>,
    /// Keeps decoded arguments alive through worker-owned copies after caller cancellation.
    pub input_owner: crate::lease::Owner,
}

impl<'a> ExecContext<'a> {
    pub(crate) fn launch(&self) -> &'a Arc<LaunchContext> {
        self.launch.unwrap_or(&self.session.launch)
    }
}

fn task_error(error: tasks::TaskError, task: &Task) -> ServiceError {
    match error {
        tasks::TaskError::Overload(message) => ServiceError::Overload(
            message,
            Some(TaskMeta {
                bare_task_id: task.id.clone(),
                log_path: files::path::display_path(&task.log_path),
            }),
        ),
        error => error.into(),
    }
}

/// Two-phase execution output shared between Agent Presenter and Program Encoder.
pub enum PreparedExec {
    Window(Box<PreparedWindow>),
    Transcript(Box<PreparedTranscript>),
    Screen(Box<PreparedScreen>),
    List(PreparedList),
}

pub struct PreparedWindow {
    pub(crate) metadata: PreparedSnapshot,
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
    // Drop data, its workspace and render permit before unlocking the reader.
    pub(crate) workspace: Lease,
    pub(crate) permit: Option<Arc<OwnedSemaphorePermit>>,
    pub(crate) read: StagedRead,
}

pub struct PreparedTranscript {
    pub(crate) metadata: PreparedSnapshot,
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
    // Drop data, its workspace and render permit before unlocking the reader.
    pub(crate) workspace: Lease,
    pub(crate) permit: Option<Arc<OwnedSemaphorePermit>>,
    pub(crate) read: StagedRead,
}

pub struct PreparedScreen {
    pub(crate) metadata: PreparedSnapshot,
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
    // Drop data, its workspace and render permit before unlocking the reader.
    pub(crate) workspace: Lease,
    pub(crate) permit: Option<Arc<OwnedSemaphorePermit>>,
    pub(crate) read: StagedRead,
}

pub struct PreparedList {
    pub(crate) total_count: usize,
    pub(crate) items: Vec<ListedTask>,
    pub(crate) parent_filter: Option<String>,
    pub(crate) state_filter: Option<ListState>,
    pub(crate) view: View,
    pub(crate) workspace: Lease,
    pub(crate) permit: Option<Arc<OwnedSemaphorePermit>>,
}

impl PreparedList {
    /// Marks only tasks started by the calling script (top-level, active children, and folded
    /// finished children) reported in `session`, without modifying outer-session entries or
    /// `keeps_reported`.
    pub(crate) fn commit_program(self, session: &Session, on_started: Option<&ScriptControl>) {
        let owns = |task: &Task| on_started.is_some_and(|script| script.owns(&task.id));
        for item in &self.items {
            if owns(&item.task)
                && let Some(entry) = session.existing(&item.task)
            {
                entry.mark_reported(&item.snapshot);
            }
            if let Some(children) = &item.children {
                for (child, snapshot) in &children.active_tasks {
                    if owns(child)
                        && let Some(entry) = session.existing(child)
                    {
                        entry.mark_reported(snapshot);
                    }
                }
                for (child, tree_ended) in &children.folded_finished {
                    if owns(child)
                        && let Some(entry) = session.existing(child)
                    {
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

struct StartWait<'a> {
    started: tasks::Started,
    startup_warning: Option<ProbeWarning>,
    wait_duration: Option<Duration>,
    when: Option<&'a wait::ReturnWhen>,
}

struct ReadTarget {
    task: Arc<Task>,
    entry: Arc<TaskEntry>,
    reader: Arc<Reader>,
    state: tokio::sync::OwnedMutexGuard<ReadState>,
}

struct OutputRead {
    action: Action,
    view: View,
    ended: Option<WaitEnd>,
    startup_warning: Option<ProbeWarning>,
}

struct ListFilter<'a> {
    scope: Option<ListScope>,
    state: Option<ListState>,
    parent_task_id: Option<&'a str>,
    limit: usize,
}

impl Service {
    pub fn new(tasks: Arc<Tasks>, daemon: bool) -> Self {
        let event_bus = Arc::clone(tasks.event_bus());
        Self {
            tasks,
            event_bus,
            daemon,
        }
    }

    pub fn event_bus(&self) -> &Arc<crate::events::EventBus> {
        &self.event_bus
    }

    pub(crate) fn tasks(&self) -> &Arc<Tasks> {
        &self.tasks
    }

    pub(crate) async fn wait_and_prepare_agent_start(
        &self,
        started: tasks::Started,
        wait_duration: Option<Duration>,
        when: Option<&wait::ReturnWhen>,
        view: View,
        ctx: &ExecContext<'_>,
        render_slot: Option<&RenderSlot<'_>>,
    ) -> Result<PreparedExec, ServiceError> {
        self.wait_and_prepare_started(
            StartWait {
                started,
                startup_warning: None,
                wait_duration,
                when,
            },
            view,
            ctx,
            render_slot,
            Consumer::Agent,
        )
        .await
    }

    async fn wait_and_prepare_started(
        &self,
        start: StartWait<'_>,
        view: View,
        ctx: &ExecContext<'_>,
        render_slot: Option<&RenderSlot<'_>>,
        consumer: Consumer,
    ) -> Result<PreparedExec, ServiceError> {
        let StartWait {
            started: (task, entry, reader),
            startup_warning,
            wait_duration,
            when,
        } = start;
        let state = read_gate(&reader, &ctx.cancel).await?;
        let mark = wait::mark(&task, &reader, &state, when, ctx.launch().memory())
            .map_err(|error| task_error(error, &task))?;
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
        .map_err(|error| task_error(error, &task))?;
        read_result(
            &ctx.cancel,
            self.prepare_window_or_screen(
                ctx,
                render_slot,
                ReadTarget {
                    task,
                    entry,
                    reader,
                    state,
                },
                OutputRead {
                    action: Action::Start,
                    view,
                    ended: Some(ended),
                    startup_warning,
                },
                consumer,
            ),
        )
        .await
    }

    pub(crate) async fn exec_core(
        &self,
        mut request: Request,
        policy: ExecPolicy,
        consumer: Consumer,
        ctx: &mut ExecContext<'_>,
        render_slot: Option<&RenderSlot<'_>>,
    ) -> Result<PreparedExec, ServiceError> {
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
        let view = validate(&request, policy).map_err(ServiceError::Validation)?;
        let session = Arc::clone(ctx.session);
        match request.action {
            Action::List => {
                let permit = render_permit(render_slot, &ctx.cancel).await?;
                self.prepare_list(
                    ctx,
                    view,
                    ListFilter {
                        scope: request.scope,
                        state: request.state,
                        parent_task_id: request.parent_task_id.as_deref(),
                        limit: request.limit.unwrap_or(DEFAULT_LIST_LIMIT) as usize,
                    },
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
                let key = id::random_chars(4)
                    .map_err(|error| format!("Cannot name the reader: {error}"))?;
                if consumer == Consumer::Program && ctx.cancel.is_cancelled() {
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
                }
                let wait = policy
                    .resolve_wait_ms(request.wait_ms, request.return_when.is_some(), false)
                    .map(Duration::from_millis);
                self.wait_and_prepare_started(
                    StartWait {
                        started: (task, entry, task_reader),
                        startup_warning,
                        wait_duration: wait,
                        when: request.return_when.as_ref(),
                    },
                    view,
                    ctx,
                    render_slot,
                    consumer,
                )
                .await
            }
            Action::Poll => {
                let (task, key) = self.task(request.task_id.as_deref())?;
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
                let task_reader = entry
                    .reader(key)
                    .map_err(|error| task_error(error, &task))?;
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
                        let modes = task.modes().map_err(ServiceError::Io)?;
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
                    .map_err(|error| task_error(error, &task))?;
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
                    .await
                    .map_err(|error| task_error(error, &task))?;
                } else {
                    task.send(
                        input,
                        flag(request.eof),
                        &ctx.cancel,
                        ctx.input_owner.clone(),
                    )
                    .await
                    .map_err(|error| task_error(error, &task))?;
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
                .map_err(|error| task_error(error, &task))?;
                read_result(
                    &ctx.cancel,
                    self.prepare_window_or_screen(
                        ctx,
                        render_slot,
                        ReadTarget {
                            task,
                            entry,
                            reader: task_reader,
                            state,
                        },
                        OutputRead {
                            action: Action::Poll,
                            view,
                            ended: Some(ended),
                            startup_warning: None,
                        },
                        consumer,
                    ),
                )
                .await
            }
            Action::Kill => {
                let (task, key) = self.task(request.task_id.as_deref())?;
                let entry = session.entry(&task);
                let task_reader = entry
                    .reader(key)
                    .map_err(|error| task_error(error, &task))?;
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
                        ReadTarget {
                            task,
                            entry,
                            reader: task_reader,
                            state,
                        },
                        OutputRead {
                            action: Action::Kill,
                            view,
                            ended: None,
                            startup_warning: None,
                        },
                        consumer,
                    ),
                )
                .await
            }
            Action::Transcript => {
                let (task, key) = self.task(request.task_id.as_deref())?;
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
                let task_reader = entry
                    .reader(key)
                    .map_err(|error| task_error(error, &task))?;
                let state = read_gate(&task_reader, &ctx.cancel).await?;
                read_result(
                    &ctx.cancel,
                    self.prepare_transcript(
                        ctx,
                        render_slot,
                        ReadTarget {
                            task,
                            entry,
                            reader: task_reader,
                            state,
                        },
                        view,
                        consumer,
                    ),
                )
                .await
            }
        }
    }

    async fn prepare_window_or_screen(
        &self,
        ctx: &ExecContext<'_>,
        render_slot: Option<&RenderSlot<'_>>,
        target: ReadTarget,
        output_read: OutputRead,
        consumer: Consumer,
    ) -> Result<PreparedExec, ServiceError> {
        let mark_session_entry = consumer == Consumer::Agent
            || ctx
                .on_started
                .is_some_and(|script| script.owns(&target.task.id));
        let permit = task_render_permit(
            render_slot,
            &ctx.cancel,
            output_read.action,
            &target.task,
            &target.reader,
        )
        .await?;
        if output_read.view.screen {
            return self
                .prepare_screen(ctx, target, output_read, permit, mark_session_entry)
                .await;
        }
        let ReadTarget {
            task,
            entry,
            reader: task_reader,
            state,
        } = target;
        let OutputRead {
            action,
            view,
            ended,
            startup_warning,
        } = output_read;
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
                let meta = TaskMeta {
                    bare_task_id: task.id.clone(),
                    log_path: files::path::display_path(&task.log_path),
                };
                ServiceError::Overload(overload_task_message(pool.limit(), &meta), Some(meta))
            })?;
        let encoding = view.encoding.unwrap_or(task.encoding);
        let initial_metadata = PreparedSnapshot::new(&task, launch, action)?;
        let spec = WindowSpec {
            truncate: view.truncate,
            budget: view.max_bytes,
            raw: view.raw,
            clean: !task.is_script(),
            keep_nul: view.keep_nul,
            include_markers: view.inline_markers,
            encoding,
            source: "log",
        };
        let initial_seen = state.attachments_seen;
        let staged = (workspace, permit, StagedRead::new(state));
        let pending = tokio::task::spawn_blocking(move || {
            let mut held = staged;
            let start = held.2.state.cursor;
            let window = reader.read_window_with_markers(&mut held.2.state, spec, None);
            let had_new_text = held.2.state.cursor > start;
            let unseen = if window.is_ok() {
                reader.unseen_attachments(initial_seen)
            } else {
                Vec::new()
            };
            (window, unseen, had_new_text, held)
        })
        .await
        .map_err(|error| format!("Internal failure while reading output: {error}."))?;
        // Preserve the owner tuple until all fallible preparation has finished.
        let mut metadata = initial_metadata;
        let window = pending
            .0
            .map_err(|error| format!("Cannot read the task log: {error}."))?;
        metadata.refresh(&task, launch, action)?;
        let script = script_report(&self.tasks, &task, launch, action)?;
        let (_, unseen, had_new_text, (workspace, permit, read)) = pending;
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

    async fn prepare_screen(
        &self,
        ctx: &ExecContext<'_>,
        target: ReadTarget,
        output_read: OutputRead,
        permit: Option<Arc<OwnedSemaphorePermit>>,
        mark_session_entry: bool,
    ) -> Result<PreparedExec, ServiceError> {
        let ReadTarget {
            task,
            entry,
            reader: task_reader,
            state,
        } = target;
        let OutputRead {
            action,
            view,
            ended,
            startup_warning,
        } = output_read;
        let ended = ended.expect("screen applies to start and poll");
        let launch = ctx.launch();
        let initial_metadata = PreparedSnapshot::new(&task, launch, action)?;
        let screen = task
            .screen(launch.memory())
            .map_err(|error| task_error(error, &task))?;
        let (reader, until) = (Arc::clone(&task), screen.log_end);
        let staged = (screen, permit, StagedRead::new(state));
        let pending = tokio::task::spawn_blocking(move || {
            let mut held = staged;
            let spec = WindowSpec {
                truncate: Truncate::Tail,
                budget: MIN_BODY_BUDGET,
                raw: false,
                clean: true,
                keep_nul: false,
                include_markers: true,
                encoding: reader.encoding,
                source: "log",
            };
            let window = reader.read_window_with_markers(&mut held.2.state, spec, Some(until));
            (window, held)
        })
        .await
        .map_err(|error| format!("Internal failure while reading output: {error}."))?;
        let metadata = initial_metadata;
        let window = pending
            .0
            .map_err(|error| format!("Cannot read the task log: {error}."))?;
        let (_, (screen, permit, read)) = pending;
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

    async fn prepare_transcript(
        &self,
        ctx: &ExecContext<'_>,
        render_slot: Option<&RenderSlot<'_>>,
        target: ReadTarget,
        view: View,
        consumer: Consumer,
    ) -> Result<PreparedExec, ServiceError> {
        let ReadTarget {
            task,
            entry,
            reader: task_reader,
            state,
        } = target;
        let mark_session_entry = consumer == Consumer::Agent
            || ctx.on_started.is_some_and(|script| script.owns(&task.id));
        let launch = ctx.launch();
        let permit = task_render_permit(
            render_slot,
            &ctx.cancel,
            Action::Transcript,
            &task,
            &task_reader,
        )
        .await?;
        let initial_metadata = PreparedSnapshot::new(&task, launch, Action::Transcript)?;
        let (reader, tasks) = (Arc::clone(&task), Arc::clone(&self.tasks));
        let memory = Arc::clone(launch.memory());
        let workspace = memory
            .try_acquire(text_workspace_bytes(view.max_bytes))
            .ok_or_else(|| {
                let meta = TaskMeta {
                    bare_task_id: task.id.clone(),
                    log_path: files::path::display_path(&task.log_path),
                };
                ServiceError::Overload(overload_task_message(memory.limit(), &meta), Some(meta))
            })?;
        let staged = (workspace, permit, StagedRead::new(state));
        let pending = tokio::task::spawn_blocking(move || {
            let held = staged;
            let transcript = reader.transcript(&tasks, &held.2.state, &memory);
            (transcript, held)
        })
        .await
        .map_err(|error| format!("Internal failure while reading: {error}."))?;
        let metadata = initial_metadata;
        let transcript = pending.0.map_err(|error| task_error(error, &task))?;
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
        let descriptions = (
            metadata,
            transcript_path,
            transcript_error,
            transcript_warnings,
        );
        let staged = (transcript, pending.1);
        let pending = tokio::task::spawn_blocking(move || {
            let mut held = staged;
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
            let mut builder = WindowBuilder::new(spec, &mut cleaner, held.0.first_line);
            let window = match read_into(&held.0.path, held.0.start, held.0.bytes, &mut builder) {
                Err(_) if logged.evicted() => Err(EVICTED_TRANSCRIPT.to_string()),
                Err(error) => Err(format!("Cannot read the transcript file: {error}.")),
                Ok(()) => {
                    builder.push(held.0.pending.as_bytes());
                    held.1.2.state.shown = (held.0.bytes, held.0.lines);
                    Ok(builder.finish())
                }
            };
            let (transcript, staged) = held;
            drop(transcript);
            (window, staged)
        })
        .await
        .map_err(|error| format!("Internal failure while reading: {error}."))?;
        let (metadata, transcript_path, transcript_error, transcript_warnings) = descriptions;
        let window = pending.0?;
        let (_, (workspace, permit, read)) = pending;
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

    fn prepare_list(
        &self,
        ctx: &ExecContext<'_>,
        view: View,
        filter: ListFilter<'_>,
        permit: Option<Arc<OwnedSemaphorePermit>>,
    ) -> Result<PreparedExec, ServiceError> {
        let ListFilter {
            scope,
            state,
            parent_task_id,
            limit,
        } = filter;
        if ctx.cancel.is_cancelled() {
            return Err(ServiceError::Cancelled(
                "Cancelled while listing tasks; retry list.".into(),
            ));
        }
        let parent_id = match parent_task_id {
            Some(raw) => Some(
                id::parse_handle_for(raw, "parentTaskId")
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
        let workspace = std::cell::RefCell::new(Lease::empty(launch.memory()));
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
        let epoch_differs = id::is_valid_task_id(id)
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
            id::parse_handle(handle.unwrap_or_default()).map_err(ServiceError::Validation)?;
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
        task_error(
            tasks::TaskError::Overload(format!(
                "The task exists and may still be running. Its output was not read because the daemon is rendering other results.\ntaskId: {}\nlogPath: {}\n{recovery}",
                reader.handle(&task.id),
                files::path::display_path(&task.log_path),
            )),
            task,
        )
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
        let modes = task
            .modes()
            .map_err(|error| ServiceError::Io(sent(error)))?;
        let bytes = keys::encode(name, key, modes)
            .map_err(|error| ServiceError::Validation(sent(error)))?;
        size += bytes.len();
        if size > MAX_INPUT_BYTES {
            return Err(ServiceError::Validation(sent(format!(
                "Key {name:?} in the program's current keyboard mode takes input and keys past 16 KiB; send fewer keys per poll."
            ))));
        }
        task.send(Some(&bytes), false, cancel, input_owner.clone())
            .await
            .map_err(|error| {
                let error = match error {
                    tasks::TaskError::Overload(message) => {
                        tasks::TaskError::Overload(sent(message))
                    }
                    tasks::TaskError::Cancelled(message) => {
                        tasks::TaskError::Cancelled(sent(message))
                    }
                    tasks::TaskError::Io(message) => tasks::TaskError::Io(sent(message)),
                };
                task_error(error, task)
            })?;
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
    pub(crate) state: ReadState,
    pub(crate) guard: tokio::sync::OwnedMutexGuard<ReadState>,
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
