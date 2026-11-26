//! Shared execution resources, snapshots, and diagnostics for Agent and Program delivery.

use super::schema::Action;
use crate::env::LaunchContext;
use crate::files;
use crate::id::parse_handle;
use crate::lease::Lease;
use crate::platform::members::{Member, MemberBudget};
use crate::platform::process::Tree;
use crate::tasks::script::{CallInfo, ReportSnapshot};
use crate::tasks::state::{Snapshot, Task};
use crate::tasks::{self, Tasks};
use crate::term::output::Window;
use std::sync::Arc;
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};

#[derive(Debug)]
pub struct TaskMeta {
    pub bare_task_id: String,
    pub log_path: String,
}

#[derive(Debug)]
pub enum ServiceError {
    Validation(String),
    NotFound(String),
    Cancelled(String),
    Overload(String, Option<TaskMeta>),
    Io(String),
}

impl ServiceError {
    pub(crate) fn message(&self) -> &str {
        match self {
            Self::Validation(s)
            | Self::NotFound(s)
            | Self::Cancelled(s)
            | Self::Overload(s, _)
            | Self::Io(s) => s,
        }
    }
}

impl From<String> for ServiceError {
    fn from(message: String) -> Self {
        Self::Io(message)
    }
}

impl From<&str> for ServiceError {
    fn from(message: &str) -> Self {
        Self::Io(message.to_owned())
    }
}

impl std::ops::Deref for ServiceError {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.message()
    }
}

impl From<tasks::TaskError> for ServiceError {
    fn from(error: tasks::TaskError) -> Self {
        match error {
            tasks::TaskError::Overload(message) => Self::Overload(message, None),
            tasks::TaskError::Cancelled(message) => Self::Cancelled(message),
            tasks::TaskError::Io(message) => Self::Io(message),
        }
    }
}

/// A local daemon call obtains one render slot on demand and keeps it until encoding.
pub struct RenderSlot<'a> {
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

    pub(crate) fn try_hold(&self) -> Option<Arc<OwnedSemaphorePermit>> {
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

pub(crate) fn text_workspace_bytes(bytes: usize) -> usize {
    bytes.saturating_mul(16).saturating_add(128 << 10)
}

pub(crate) fn overload_task_message(limit: usize, meta: &TaskMeta) -> String {
    format!(
        "Server active memory budget ({limit} bytes) is full while encoding the response; the operation ran (taskId: {}, logPath: {}). Do not retry start or resend input; poll taskId without input once in-flight transfers complete.",
        meta.bare_task_id, meta.log_path
    )
}

/// Snapshot strings can coexist with warning/status formatting, cloned fields,
/// the joined body and its growing suffix. Thirty-two times their source capacity,
/// including per-member/diagnostic keys, bounds those copies and geometric capacities.
pub(crate) fn metadata_workspace_bytes(bytes: usize) -> usize {
    bytes.saturating_mul(32)
}

pub(crate) fn resize_metadata_workspace(
    workspace: &mut Lease,
    target: usize,
    launch: &LaunchContext,
    action: Action,
) -> Result<(), String> {
    if workspace.try_resize(target) {
        return Ok(());
    }
    let replacement = launch
        .memory()
        .try_acquire(target)
        .or_else(|| {
            matches!(action, Action::List | Action::Kill)
                .then(|| launch.control_memory().try_acquire(target))
                .flatten()
        })
        .ok_or_else(|| {
            format!(
                "Server active memory budget ({} bytes) is full while preparing task metadata.",
                launch.memory().limit()
            )
        })?;
    // Acquire the replacement before releasing the old pool's charge.
    *workspace = replacement;
    Ok(())
}

pub(crate) fn metadata_members(
    tree: &Tree,
    workspace: &mut Lease,
    launch: &LaunchContext,
    action: Action,
) -> Result<Option<Vec<Member>>, String> {
    let retained = workspace.bytes();
    let (members, refused) = {
        let mut reserve = |bytes| {
            resize_metadata_workspace(workspace, retained.saturating_add(bytes), launch, action)
                .is_ok()
        };
        let mut budget = MemberBudget::new(&mut reserve);
        let members = tree.members_with_budget(&mut budget);
        (members, budget.refused())
    };
    if refused {
        drop(members);
        workspace.shrink_to(retained);
        return Err("Server active memory budget is full while reading task process names.".into());
    }
    let text = members.as_ref().map_or(0, |members| {
        members
            .iter()
            .map(|member| member.name.capacity().saturating_add(32))
            .fold(0, usize::saturating_add)
    });
    if let Err(error) = resize_metadata_workspace(
        workspace,
        retained.saturating_add(metadata_workspace_bytes(text)),
        launch,
        action,
    ) {
        drop(members);
        workspace.shrink_to(retained);
        return Err(error);
    }
    Ok(members)
}

// Snapshot text drops before the workspace, including on cancellation across an await.
pub(crate) struct PreparedSnapshot {
    pub(crate) value: Snapshot,
    pub(crate) workspace: Lease,
}

impl PreparedSnapshot {
    pub(crate) fn overload(launch: &LaunchContext, task: &Task) -> ServiceError {
        let meta = TaskMeta {
            bare_task_id: task.id.clone(),
            log_path: files::path::display_path(&task.log_path),
        };
        ServiceError::Overload(
            overload_task_message(launch.memory().limit(), &meta),
            Some(meta),
        )
    }

    pub(crate) fn new(
        task: &Task,
        launch: &LaunchContext,
        action: Action,
    ) -> Result<Self, ServiceError> {
        let workspace = std::cell::RefCell::new(Lease::empty(launch.memory()));
        let fixed = task
            .id
            .capacity()
            .saturating_add(task.log_path.capacity().saturating_mul(3))
            .saturating_add(512);
        resize_metadata_workspace(
            &mut workspace.borrow_mut(),
            metadata_workspace_bytes(fixed),
            launch,
            action,
        )
        .map_err(|_| Self::overload(launch, task))?;
        let value = task
            .snapshot_held_reserved(
                &mut |bytes| {
                    let mut held = workspace.borrow_mut();
                    let target = held.bytes().saturating_add(metadata_workspace_bytes(bytes));
                    resize_metadata_workspace(&mut held, target, launch, action)
                },
                &mut |tree| metadata_members(tree, &mut workspace.borrow_mut(), launch, action),
            )
            .map_err(|_| Self::overload(launch, task))?;
        Ok(Self {
            value,
            workspace: workspace.into_inner(),
        })
    }

    pub(crate) fn refresh(
        &mut self,
        task: &Task,
        launch: &LaunchContext,
        action: Action,
    ) -> Result<(), ServiceError> {
        let workspace = &mut self.workspace;
        task.refresh_snapshot_output(&mut self.value, &mut |bytes| {
            let target = workspace
                .bytes()
                .saturating_add(metadata_workspace_bytes(bytes));
            resize_metadata_workspace(workspace, target, launch, action)
        })
        .map_err(|_| Self::overload(launch, task))
    }
}

pub(crate) fn kill_pending_note(task: &Task, snapshot: &Snapshot) -> Option<&'static str> {
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

pub(crate) struct StartedTaskSummary {
    pub(crate) task_id: String,
    pub(crate) state: &'static str,
}

pub(crate) struct ScriptReport {
    pub(crate) cancelled_calls: Vec<CallInfo>,
    pub(crate) started_tasks: Vec<StartedTaskSummary>,
    pub(crate) running: Vec<String>,
    pub(crate) _lease: Lease,
}

pub(crate) fn script_report(
    tasks: &Tasks,
    task: &Task,
    launch: &LaunchContext,
    action: Action,
) -> Result<Option<ScriptReport>, ServiceError> {
    let Some(script) = task.script() else {
        return Ok(None);
    };
    let ReportSnapshot {
        cancelled,
        started: handles,
        _lease: lease,
    } = script
        .report(
            launch.memory(),
            (action == Action::Kill).then_some(launch.control_memory()),
        )
        .ok_or_else(|| {
            let meta = TaskMeta {
                bare_task_id: task.id.clone(),
                log_path: files::path::display_path(&task.log_path),
            };
            ServiceError::Overload(
                overload_task_message(launch.memory().limit(), &meta),
                Some(meta),
            )
        })?;
    let mut running = Vec::new();
    let started_tasks = handles
        .into_iter()
        .map(|handle| {
            let task = parse_handle(&handle)
                .ok()
                .and_then(|(id, _)| tasks.find(id));
            let state = task.as_ref().map_or("unknown", |task| {
                let (state, kept) = task.report_state();
                if state == "running" || kept {
                    running.push(handle.clone());
                }
                state
            });
            StartedTaskSummary {
                task_id: handle,
                state,
            }
        })
        .collect::<Vec<_>>();
    Ok(Some(ScriptReport {
        cancelled_calls: cancelled,
        started_tasks,
        running,
        _lease: lease,
    }))
}

pub(crate) fn task_io_ok(snapshot: &Snapshot) -> bool {
    snapshot.capture_error.is_none()
        && snapshot.log_error.is_none()
        && snapshot.input_error.is_none()
}

pub(crate) fn output_incomplete(snapshot: &Snapshot) -> bool {
    snapshot.capture_error.is_some()
        || snapshot.log_error.is_some()
        || snapshot.dropped > 0
        || snapshot.evicted
}

pub(crate) fn task_warnings(snapshot: &Snapshot) -> Vec<String> {
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
            "log evicted under the 4 GiB total limit; stored output and attachments are unavailable"
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

pub(crate) fn encoding_warning(window: &Window) -> Option<String> {
    (window.bad_lines > 0).then(|| {
        format!(
            "{} lines had bytes invalid in {}; pass encoding on a stream read (e.g. big5, gbk)",
            window.bad_lines,
            window.encoding.name()
        )
    })
}
