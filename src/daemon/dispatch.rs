use super::state::{Daemon, RESERVED, lock};
use super::wire::{Outgoing, OutgoingPermit, error};
use crate::ipc::{self, ToEndpoint};
use crate::lease;
use crate::render;
use crate::render::files::{self as render_files, transfer_error};
use crate::render::types::{AgentDelivery, DaemonDelivery, TrackedResult};
use crate::service::ExecContext;
use crate::service::core::{RenderSlot, overload_task_message};
use crate::tasks::session::Session;
use rmcp::model::CallToolResult;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

pub(super) struct CallDispatch {
    pub(super) daemon: Arc<Daemon>,
    pub(super) session: Arc<Session>,
    pub(super) connection: CancellationToken,
    pub(super) token: CancellationToken,
    pub(super) id: u64,
    pub(super) tool: String,
    pub(super) arguments: serde_json::Value,
    pub(super) in_flight: Arc<Mutex<HashMap<u64, CancellationToken>>>,
    pub(super) outgoing: mpsc::UnboundedSender<Outgoing>,
    pub(super) rpc_id_bytes: usize,
    pub(super) input_owner: crate::lease::Owner,
}

/// Runs one call: its progress frames while it waits, then its result within the budget.
pub(super) async fn dispatch_call(call: CallDispatch) {
    let CallDispatch {
        daemon,
        session,
        connection,
        token,
        id,
        tool,
        arguments,
        in_flight,
        outgoing,
        rpc_id_bytes,
        input_owner,
    } = call;
    let render_pool = match (
        tool.as_str(),
        arguments.get("action").and_then(|action| action.as_str()),
    ) {
        ("exec", Some("kill")) => &daemon.render_kill,
        ("exec", Some("list")) => &daemon.render_list,
        _ => &daemon.render,
    };
    let management = !Arc::ptr_eq(render_pool, &daemon.render);
    let render_slot = RenderSlot::new(render_pool);
    let cancelled_before_dispatch = if tool != "exec" && tool != "codemode" {
        tokio::select! {
            biased;
            _ = connection.cancelled() => {
                lock(&in_flight).remove(&id);
                return;
            }
            _ = token.cancelled() => true,
            _ = render_slot.hold() => false,
        }
    } else {
        false
    };
    let (progress, latest) = watch::channel(None);
    let forwarder = Forwarder(tokio::spawn(forward_progress(id, latest, outgoing.clone())));
    let ctx = ExecContext {
        session: &session,
        launch: None,
        cancel: token.clone(),
        progress: Some(progress),
        call_id: Some(id),
        parent_task_id: None,
        queued_guard: None,
        on_started: None,
        input_owner: input_owner.clone(),
    };
    let delivery = AgentDelivery {
        daemon_ipc: Some(DaemonDelivery {
            slot: &render_slot,
            ipc_body_overhead: ipc::result_body_overhead(id),
        }),
        http_response: false,
        rpc_id_bytes,
    };
    let transfer = session.launch.var("FASTEXEC_TRANSFER") == Some(std::ffi::OsStr::new("true"));
    let mut tracked = if cancelled_before_dispatch {
        TrackedResult::plain(error(
            "Cancelled before the operation started; nothing was executed.".into(),
        ))
    } else if tool == "exec" {
        render::exec_agent_json(&daemon.service, arguments, ctx, delivery).await
    } else if tool == "codemode" {
        match serde_json::from_value::<crate::codemode::control::Request>(arguments) {
            Ok(request) => {
                let mut ctx = ctx;
                let outcome = crate::codemode::prepare_script(
                    &daemon.service,
                    request,
                    &mut ctx,
                    delivery.daemon_ipc.as_ref().map(|d| d.slot),
                    transfer,
                )
                .await;
                render::finish_exec_agent(&daemon.service, outcome, &ctx, &delivery).await
            }
            Err(problem) => TrackedResult::plain(error(problem.to_string())),
        }
    } else if tool == "transfer" {
        if !transfer {
            TrackedResult::plain(transfer_error(
                "transfer is disabled; set FASTEXEC_TRANSFER=true.".to_owned(),
            ))
        } else {
            match serde_json::from_value::<crate::transfer::Request>(arguments) {
                Ok(request) => {
                    daemon
                        .transfer(request, &session.launch, input_owner.clone())
                        .await
                }
                Err(problem) => TrackedResult::plain(transfer_error(format!(
                    "failed to deserialize parameters: {problem}"
                ))),
            }
        }
    } else {
        // File tools run in the daemon process using the calling session's launch context.
        let launch = Arc::clone(&session.launch);
        render_files::call_tracked(
            &tool,
            arguments,
            launch,
            transfer,
            Some(token.clone()),
            input_owner.clone(),
        )
        .await
    };
    // The result goes out last. Aborting the forwarder is not enough on its own: one already
    // inside an encode or a send could still queue a frame after the result, so this waits for
    // it to be gone.
    forwarder.stop().await;

    let task_meta = tracked.task_meta.take();
    let mut overload = tracked.overload;
    let mut lease = tracked.lease.take();
    let result = std::mem::replace(&mut tracked.result, CallToolResult::success(Vec::new()));
    let mut message = ToEndpoint::Result { id, result };
    let Some(mut length) = measure(&message) else {
        lock(&in_flight).remove(&id);
        return;
    };
    if length > lease::FRAME_LIMIT + 4 {
        let text = match &task_meta {
            Some(m) => format!(
                "The result was {length} bytes, over the 512 MiB frame limit; the operation ran (taskId: {}, logPath: {}). Do not retry start or resend input; poll taskId with a smaller maxBytes or read logPath.",
                m.bare_task_id, m.log_path
            ),
            None => format!(
                "The result was {length} bytes, over the 512 MiB frame limit; the call ran, and list shows its task and logPath."
            ),
        };
        message = ToEndpoint::Result {
            id,
            result: error(text),
        };
        drop(lease.take());
        overload = true;
        match measure(&message) {
            Some(smaller) => length = smaller,
            None => {
                lock(&in_flight).remove(&id);
                return;
            }
        }
    }

    let payload_bytes = match &message {
        ToEndpoint::Result { result, .. } => {
            render::unleased_result_payload_capacity(result, &tracked.payload_owner)
        }
        _ => 0,
    };
    let peak = payload_bytes.saturating_add(length);
    if (management || overload) && peak <= RESERVED {
        let want = u32::try_from(peak).unwrap_or(u32::MAX);
        let mut permit = tokio::select! {
            biased;
            _ = connection.cancelled() => {
                lock(&in_flight).remove(&id);
                return;
            }
            permit = Arc::clone(&daemon.reserved).acquire_many_owned(want) => match permit {
                Ok(permit) => permit,
                Err(_) => {
                    lock(&in_flight).remove(&id);
                    return;
                }
            },
        };
        let bytes = ipc::encode_measured(&message, length);
        drop(message);
        drop(lease);
        drop(render_slot);
        let outbound = permit
            .split(length)
            .expect("reserved peak includes the frame");
        drop(permit);
        if let Ok(bytes) = bytes {
            let _ = outgoing.send(Outgoing {
                bytes,
                _permit: Some(OutgoingPermit::Reserved(outbound)),
                written: None,
            });
        }
        lock(&in_flight).remove(&id);
        return;
    }

    let admitted = match lease.as_mut() {
        Some(held) => held.try_resize(peak),
        None => {
            lease = daemon.memory.try_acquire(peak);
            lease.is_some()
        }
    };
    if admitted {
        let mut staging_lease = lease.take().expect("admitted result has an owner");
        let frame_lease = staging_lease.split_off(length);
        let bytes = ipc::encode_measured(&message, length);
        drop(message);
        drop(staging_lease);
        drop(render_slot);
        if let Ok(bytes) = bytes {
            let _ = outgoing.send(Outgoing {
                bytes,
                _permit: Some(OutgoingPermit::Main(frame_lease)),
                written: None,
            });
        }
        lock(&in_flight).remove(&id);
        return;
    }

    // Active memory cannot fit the outbound frame without blocking; release the uncommitted result
    // and send a short overload receipt through the independent RESERVED control pool.
    let overload_text = match &task_meta {
        Some(meta) => overload_task_message(daemon.memory.limit(), meta),
        None => format!(
            "Server active memory budget ({} bytes) is full; retry once in-flight transfers complete.",
            daemon.memory.limit()
        ),
    };
    drop(message);
    drop(tracked);
    drop(lease);
    drop(render_slot);
    let fallback = ToEndpoint::Result {
        id,
        result: error(overload_text),
    };
    let Some(fallback_len) = measure(&fallback).filter(|&len| len <= RESERVED) else {
        lock(&in_flight).remove(&id);
        return;
    };
    let fallback_payload = match &fallback {
        ToEndpoint::Result { result, .. } => render::result_payload_capacity(result),
        _ => 0,
    };
    let fallback_peak = fallback_len.saturating_add(fallback_payload);
    if fallback_peak > RESERVED {
        lock(&in_flight).remove(&id);
        return;
    }
    let want = u32::try_from(fallback_peak).unwrap_or(u32::MAX);
    let mut permit = tokio::select! {
        biased;
        _ = connection.cancelled() => {
            lock(&in_flight).remove(&id);
            return;
        }
        permit = Arc::clone(&daemon.reserved).acquire_many_owned(want) => match permit {
            Ok(permit) => permit,
            Err(_) => {
                lock(&in_flight).remove(&id);
                return;
            }
        },
    };
    let bytes = ipc::encode_measured(&fallback, fallback_len);
    drop(fallback);
    let outbound = permit
        .split(fallback_len)
        .expect("reserved peak includes the frame");
    drop(permit);
    if let Ok(bytes) = bytes {
        let _ = outgoing.send(Outgoing {
            bytes,
            _permit: Some(OutgoingPermit::Reserved(outbound)),
            written: None,
        });
    }
    lock(&in_flight).remove(&id);
}

/// The bytes a result frame takes, counted without holding them.
fn measure(message: &ToEndpoint) -> Option<usize> {
    ipc::frame_len(message).ok()
}

/// Sends each latest progress message, one frame at a time: the next is queued only once the
/// last was written, so a slow connection sees the latest and skips the rest.
async fn forward_progress(
    id: u64,
    mut latest: watch::Receiver<Option<(f64, String)>>,
    outgoing: mpsc::UnboundedSender<Outgoing>,
) {
    while latest.changed().await.is_ok() {
        let next = latest.borrow_and_update().clone();
        let Some((seconds, message)) = next else {
            continue;
        };
        let Ok(bytes) = ipc::encode_frame(&ToEndpoint::Progress {
            id,
            seconds,
            message,
        }) else {
            return;
        };
        let (written, wait) = oneshot::channel();
        if outgoing
            .send(Outgoing {
                bytes,
                _permit: None,
                written: Some(written),
            })
            .is_err()
        {
            return;
        }
        if wait.await.is_err() {
            return;
        }
    }
}

/// Aborts a progress forwarder however the call returns, and ends it before the result is
/// queued.
struct Forwarder(tokio::task::JoinHandle<()>);

impl Forwarder {
    /// Stops the forwarder and waits until it can queue nothing more.
    async fn stop(mut self) {
        self.0.abort();
        let _ = (&mut self.0).await;
    }
}

impl Drop for Forwarder {
    fn drop(&mut self) {
        self.0.abort();
    }
}
