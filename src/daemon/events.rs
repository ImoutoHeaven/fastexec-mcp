use super::{Daemon, MAX_EVENT_STREAMS_PER_CONN, Outgoing, OutgoingPermit, lock};
use crate::ipc::{self, ToEndpoint};
use crate::lease::BytePool;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

pub(super) struct ActiveEventStream {
    token: Arc<CancellationToken>,
    _guard: crate::events::StreamGuard,
}

pub(super) type EventStreams = Arc<Mutex<HashMap<u64, ActiveEventStream>>>;

pub(super) struct EventStreamRegistration {
    streams: EventStreams,
    id: u64,
    token: Arc<CancellationToken>,
}

impl Drop for EventStreamRegistration {
    fn drop(&mut self) {
        let mut streams = lock(&self.streams);
        if streams
            .get(&self.id)
            .is_some_and(|stream| Arc::ptr_eq(&stream.token, &self.token))
        {
            streams.remove(&self.id);
        }
    }
}

pub(super) fn start_stream(
    daemon: &Arc<Daemon>,
    event_streams: &EventStreams,
    session_id: &str,
    closing: &CancellationToken,
    outgoing: &mpsc::UnboundedSender<Outgoing>,
    id: u64,
    request: ipc::Owned<crate::events::SubscribeRequest>,
) {
    let stream_token = {
        let mut streams = lock(event_streams);
        if streams.contains_key(&id) {
            Err(ipc::IpcEventError {
                code: -32602,
                message: format!("event stream {id} is already active"),
                data: None,
            })
        } else if streams.len() >= MAX_EVENT_STREAMS_PER_CONN {
            Err(ipc::IpcEventError {
                code: -32025,
                message: format!(
                    "active event streams on connection reached limit ({MAX_EVENT_STREAMS_PER_CONN})"
                ),
                data: Some(serde_json::json!({
                    "limit": "connection_event_streams",
                    "max": MAX_EVENT_STREAMS_PER_CONN
                })),
            })
        } else {
            match daemon.service.event_bus().try_acquire_stream() {
                Err(err) => Err(err.into()),
                Ok(guard) => match daemon
                    .service
                    .event_bus()
                    .subscribe(request.value, Some(session_id))
                {
                    Ok(sub) => {
                        let token = Arc::new(closing.child_token());
                        streams.insert(
                            id,
                            ActiveEventStream {
                                token: Arc::clone(&token),
                                _guard: guard,
                            },
                        );
                        let registration = EventStreamRegistration {
                            streams: Arc::clone(event_streams),
                            id,
                            token,
                        };
                        Ok((registration, sub))
                    }
                    Err(err) => Err(err.into()),
                },
            }
        }
    };
    match stream_token {
        Ok((registration, sub)) => {
            tokio::spawn(request.owner.hold_future(run_event_stream(
                Arc::clone(daemon),
                registration,
                sub,
                outgoing.clone(),
                closing.clone(),
            )));
        }
        Err(err) => {
            queue_event_control(
                outgoing,
                &daemon.memory,
                &daemon.reserved,
                closing,
                id,
                ipc::EventWireFrame::Error(err),
            );
        }
    }
}

pub(super) fn cancel_stream(event_streams: &EventStreams, id: u64) {
    if let Some(stream) = lock(event_streams).remove(&id) {
        stream.token.cancel();
        // Release the quota before the next request can register a replacement.
        drop(stream);
    }
}

pub(super) fn queue_event_frame(
    outgoing: &mpsc::UnboundedSender<Outgoing>,
    memory: &Arc<BytePool>,
    reserved: &Arc<Semaphore>,
    closing: &CancellationToken,
    id: u64,
    frame: ipc::Owned<ipc::EventWireFrame>,
) -> bool {
    let ipc::Owned {
        value: frame,
        owner,
    } = frame;
    let message = ToEndpoint::EventFrame { id, frame };
    let Ok(length) = ipc::frame_len(&message) else {
        return false;
    };
    let permit = if let Some(lease) = memory.try_acquire(length) {
        OutgoingPermit::Main(lease)
    } else if let Ok(want) = u32::try_from(length)
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
    drop(owner);
    outgoing
        .send(Outgoing {
            bytes,
            _permit: Some(permit),
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
    let Ok(length) = ipc::frame_len(&frame) else {
        return false;
    };
    let owner = if let Some(lease) = memory.try_acquire(length) {
        crate::lease::Owner::new(lease)
    } else if let Ok(want) = u32::try_from(length)
        && let Ok(permit) = Arc::clone(reserved).try_acquire_many_owned(want)
    {
        crate::lease::Owner::new(permit)
    } else {
        closing.cancel();
        return false;
    };
    queue_event_frame(
        outgoing,
        memory,
        reserved,
        closing,
        id,
        ipc::Owned::new(frame, owner),
    )
}

async fn run_event_stream(
    daemon: Arc<Daemon>,
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
    let queue = |frame| {
        queue_event_frame(
            &outgoing,
            &daemon.memory,
            &daemon.reserved,
            &closing,
            id,
            frame,
        )
    };
    let control = |frame| {
        queue_event_control(
            &outgoing,
            &daemon.memory,
            &daemon.reserved,
            &closing,
            id,
            frame,
        )
    };
    let terminate = |error| {
        control(ipc::EventWireFrame::Terminated(error));
        control(ipc::EventWireFrame::Complete);
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
            _ = daemon.stopped.cancelled() => {
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
