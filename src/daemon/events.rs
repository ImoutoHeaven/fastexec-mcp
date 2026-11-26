use super::wire::{Outgoing, OutgoingPermit};
use crate::ipc::{self, ToEndpoint};
use crate::lease::BytePool;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

pub(super) struct ActiveEventStream {
    pub(super) token: Arc<CancellationToken>,
    pub(super) _guard: crate::events::StreamGuard,
}

pub(super) type EventStreams = Arc<Mutex<HashMap<u64, ActiveEventStream>>>;

pub(super) struct EventStreamRegistration {
    pub(super) streams: EventStreams,
    pub(super) id: u64,
    pub(super) token: Arc<CancellationToken>,
}

impl Drop for EventStreamRegistration {
    fn drop(&mut self) {
        let mut streams = self
            .streams
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if streams
            .get(&self.id)
            .is_some_and(|stream| Arc::ptr_eq(&stream.token, &self.token))
        {
            streams.remove(&self.id);
        }
    }
}

pub(super) fn cancel_stream(event_streams: &EventStreams, id: u64) {
    if let Some(stream) = event_streams
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .remove(&id)
    {
        stream.token.cancel();
        // Release the quota before the next request can register a replacement.
        drop(stream);
    }
}

const MAX_EVENT_CONTROL_BYTES: usize = 4096;

pub(super) fn queue_event_frame(
    outgoing: &mpsc::UnboundedSender<Outgoing>,
    memory: &Arc<BytePool>,
    id: u64,
    frame: crate::lease::Owned<ipc::EventWireFrame>,
) -> bool {
    let crate::lease::Owned {
        value: frame,
        owner,
    } = frame;
    let message = ToEndpoint::EventFrame { id, frame };
    let Ok(length) = ipc::frame_len(&message) else {
        return false;
    };
    let Some(lease) = memory.try_acquire(length) else {
        return false;
    };
    let Ok(bytes) = ipc::encode_measured(&message, length) else {
        return false;
    };
    drop(message);
    drop(owner);
    outgoing
        .send(Outgoing {
            bytes,
            _permit: Some(OutgoingPermit::Main(lease)),
            written: None,
        })
        .is_ok()
}

pub(super) fn queue_event_control(
    outgoing: &mpsc::UnboundedSender<Outgoing>,
    memory: &Arc<BytePool>,
    reserved: &Arc<Semaphore>,
    closing: &CancellationToken,
    id: u64,
    frame: ipc::EventWireFrame,
) -> bool {
    let message = ToEndpoint::EventFrame { id, frame };
    let Ok(length) = ipc::frame_len(&message) else {
        return false;
    };
    let permit = if let Some(lease) = memory.try_acquire(length) {
        OutgoingPermit::Main(lease)
    } else if length <= MAX_EVENT_CONTROL_BYTES
        && let Ok(want) = u32::try_from(length)
        && let Ok(permit) = Arc::clone(reserved).try_acquire_many_owned(want)
    {
        OutgoingPermit::Reserved(permit)
    } else {
        closing.cancel();
        return false;
    };
    let Ok(bytes) = ipc::encode_measured(&message, length) else {
        return false;
    };
    drop(message);
    outgoing
        .send(Outgoing {
            bytes,
            _permit: Some(permit),
            written: None,
        })
        .is_ok()
}

pub(super) async fn run_event_stream(
    memory: Arc<BytePool>,
    reserved: Arc<Semaphore>,
    stopped: CancellationToken,
    registration: EventStreamRegistration,
    mut sub: crate::events::EventSubscription,
    outgoing: mpsc::UnboundedSender<Outgoing>,
    closing: CancellationToken,
) {
    let id = registration.id;
    let token = &registration.token;
    if token.is_cancelled() {
        return;
    }
    let queue = |frame| queue_event_frame(&outgoing, &memory, id, frame);
    let control = |frame| queue_event_control(&outgoing, &memory, &reserved, &closing, id, frame);
    let terminate = |error| {
        control(ipc::EventWireFrame::Terminated(error));
        control(ipc::EventWireFrame::Complete);
    };
    let terminate_budget = || {
        if !token.is_cancelled() && !closing.is_cancelled() {
            terminate(ipc::IpcEventError {
                code: -32025,
                message: "daemon active memory budget full during event stream".to_owned(),
                data: None,
            });
        }
    };
    let heartbeat = Duration::from_millis(sub.heartbeat_ms());
    let initial = match sub.initial_active() {
        Ok(active) => active,
        Err(error) => {
            control(ipc::EventWireFrame::Error(error.into()));
            return;
        }
    };
    if !queue(initial.map(ipc::EventWireFrame::Active)) {
        terminate_budget();
        return;
    }

    let sleep = tokio::time::sleep(heartbeat);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            biased;
            _ = token.cancelled() => {
                // Client cancelled or connection closed: exit without terminal response.
                break;
            }
            _ = stopped.cancelled() => {
                terminate(ipc::IpcEventError {
                    code: -32000,
                    message: "fastexec daemon shutting down".to_owned(),
                    data: None,
                });
                break;
            }
            item = sub.next() => {
                match item {
                    Ok(Some(item)) => {
                        if !queue(item.map(|item| match item {
                            crate::events::StreamItem::Active(active) => ipc::EventWireFrame::Active(active),
                            crate::events::StreamItem::Event(event) => ipc::EventWireFrame::Event(event),
                        })) {
                            terminate_budget();
                            break;
                        }
                        sleep.as_mut().reset(tokio::time::Instant::now() + heartbeat);
                    }
                    Ok(None) => {
                        terminate(ipc::IpcEventError {
                            code: -32000,
                            message: "event bus closed".to_owned(),
                            data: None,
                        });
                        break;
                    }
                    Err(error) => {
                        terminate(error.into());
                        break;
                    }
                }
            }
            _ = &mut sleep => {
                match sub.current_cursor() {
                    Ok(cursor) => {
                        if !queue(cursor.map(|cursor| ipc::EventWireFrame::Heartbeat { cursor })) {
                            terminate_budget();
                            break;
                        }
                    }
                    Err(error) => {
                        terminate(error.into());
                        break;
                    }
                }
                sleep.as_mut().reset(tokio::time::Instant::now() + heartbeat);
            }
        }
    }
}
