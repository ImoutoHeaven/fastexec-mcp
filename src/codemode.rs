//! JavaScript tasks and their asynchronous bridge to the four host tools.

pub(crate) mod bridge;
pub(crate) mod control;
pub(crate) mod encode;
#[cfg(test)]
mod tests;
mod vm;

use self::control::{BridgeHandle, Request};
use self::vm::Work;
use crate::env::LaunchContext;
use crate::id;
use crate::service::core::RenderSlot;
use crate::service::schema::{self as service_schema, ExecPolicy};
use crate::service::{ExecContext, PreparedExec, Service, ServiceError};
use crate::tasks::script::ScriptControl;
use crate::tasks::session::Session;
use crate::tasks::{ScriptIo, ScriptPublishArgs, Started, TaskError, Tasks};
use std::sync::{Arc, mpsc};
use std::time::Duration;

pub(crate) struct ScriptSpawnArgs {
    pub code: String,
    pub launch: Arc<LaunchContext>,
    pub kill_after_ms: Option<u64>,
    pub transfer: bool,
    pub call_id: Option<u64>,
    pub service: Arc<Service>,
    pub input_owner: crate::lease::Owner,
}

pub(super) type VmJob = (ScriptIo, (Work, BridgeHandle));

/// Spawns a waiting VM thread and captures its immutable [`std::thread::Thread`] handle.
pub(super) fn prepare_worker()
-> Result<(Arc<ScriptControl>, BridgeHandle, mpsc::SyncSender<VmJob>), String> {
    let (settled_tx, settled_rx) = mpsc::channel();
    let (job_tx, job_rx) = mpsc::sync_channel::<VmJob>(1);
    let join = std::thread::Builder::new()
        .name("fastexec-script".into())
        .stack_size(2 * 1024 * 1024)
        .spawn(move || {
            if let Ok((io, (work, bridge))) = job_rx.recv() {
                vm::run(io, work, bridge, settled_rx);
            }
        })
        .map_err(|error| format!("Cannot create the script worker: {error}"))?;
    let vm_thread = join.thread().clone();
    let control = Arc::new(ScriptControl::new(vm_thread));
    let bridge = BridgeHandle::new(Arc::clone(&control), settled_tx);
    Ok((control, bridge, job_tx))
}

pub(crate) fn spawn_script(
    tasks: &Arc<Tasks>,
    args: ScriptSpawnArgs,
    session: &Arc<Session>,
    key: String,
) -> Result<(Started, BridgeHandle), TaskError> {
    spawn_script_inner(
        tasks,
        args,
        session,
        key,
        #[cfg(test)]
        None,
    )
}

fn spawn_script_inner(
    tasks: &Arc<Tasks>,
    args: ScriptSpawnArgs,
    session: &Arc<Session>,
    key: String,
    #[cfg(test)] vm_memory_limit: Option<usize>,
) -> Result<(Started, BridgeHandle), TaskError> {
    let reserved = tasks.reserve_script_slot()?;
    let (control, bridge, job_tx) = prepare_worker()?;
    let publish_args = ScriptPublishArgs {
        command: format!("[codemode] {}", args.code),
        kill_after_ms: args.kill_after_ms,
        call_id: args.call_id,
        script: control,
        key,
    };
    let work = Work {
        service: args.service,
        session: Arc::clone(session),
        launch: args.launch,
        transfer: args.transfer,
        call_id: args.call_id,
        code: args.code,
        runtime: tasks.runtime_handle().clone(),
        input_owner: args.input_owner,
        #[cfg(test)]
        vm_memory_limit,
    };
    let started = tasks.publish_script(
        reserved,
        publish_args,
        session,
        job_tx,
        (work, bridge.clone()),
    )?;
    Ok((started, bridge))
}

pub(crate) async fn prepare_script(
    service: &Arc<Service>,
    request: Request,
    ctx: &mut ExecContext<'_>,
    render_slot: Option<&RenderSlot<'_>>,
    transfer: bool,
) -> Result<PreparedExec, ServiceError> {
    service.tasks().prune_entries(ctx.session);
    let base_launch = ctx.launch();
    let (start, resolved_cwd) =
        control::validate(&request, base_launch).map_err(ServiceError::Validation)?;
    let view =
        service_schema::validate(&start, ExecPolicy::AGENT).map_err(ServiceError::Validation)?;
    let launch = match resolved_cwd {
        None => Arc::clone(base_launch),
        Some(dir) => Arc::new(base_launch.with_cwd(dir)),
    };
    let args = ScriptSpawnArgs {
        code: request.code,
        launch,
        kill_after_ms: start.kill_after_ms,
        transfer,
        call_id: ctx.call_id,
        service: Arc::clone(service),
        input_owner: ctx.input_owner.clone(),
    };
    let tasks = Arc::clone(service.tasks());
    let session = Arc::clone(ctx.session);
    let key = id::random_chars(4).map_err(|error| format!("Cannot name the reader: {error}"))?;
    let (started, _bridge) =
        tokio::task::spawn_blocking(move || spawn_script(&tasks, args, &session, key))
            .await
            .map_err(|error| format!("Internal failure while starting: {error}."))??;
    service
        .wait_and_prepare_agent_start(
            started,
            Some(Duration::from_millis(start.wait_ms.unwrap_or(30_000))),
            start.return_when.as_ref(),
            view,
            ctx,
            render_slot,
        )
        .await
}
