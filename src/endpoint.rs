//! The MCP endpoint in `on` mode: one link to the namespace's daemon, and the calls that travel
//! over it.
//!
//! The endpoint is the only thing here that knows the daemon is a process. It identifies this
//! build once, connects on the first call, and from then on multiplexes every call over one
//! connection: each call carries an ID that stays its name across a reconnect, so a caller's
//! `Cancel` still names the right call after the link was replaced.

use crate::env::LaunchContext;
use crate::ipc::{self, ToDaemon, ToEndpoint};
use crate::platform::runtime;
use crate::render::types::TrackedResult;
use crate::transport;
use crate::wait;
use rmcp::model::{CallToolResult, ContentBlock};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::io::{ReadHalf, WriteHalf};
use tokio::sync::{Mutex, OnceCell, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

/// Total deadline for queueing on the request writer and sending a call frame (not response read).
const WRITE_WAIT: std::time::Duration = std::time::Duration::from_secs(60);
/// How long `bye` may take, locks and write together.
const BYE_WAIT: std::time::Duration = std::time::Duration::from_secs(1);
/// How long a `Cancel` may take, the writer lock included.
const CANCEL_WAIT: std::time::Duration = std::time::Duration::from_secs(1);
/// Bounded capacity for one-shot event RPCs (`events/list`, `events/poll`).
const EVENT_RPC_CHANNEL_CAPACITY: usize = 4;
/// Bounded capacity for streaming event subscriptions (`events/stream`).
const EVENT_STREAM_CHANNEL_CAPACITY: usize = 128;

/// This build's identity and runtime directory, or why they could not be read.
type Identity = Result<(runtime::Identity, runtime::Paths), String>;

#[derive(Debug)]
pub(crate) enum SetupError {
    MemoryBudget,
    Other(String),
}

impl std::fmt::Display for SetupError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MemoryBudget => formatter.write_str(
                "Server active memory budget is full while preparing the daemon connection",
            ),
            Self::Other(message) => formatter.write_str(message),
        }
    }
}

impl From<String> for SetupError {
    fn from(message: String) -> Self {
        Self::Other(message)
    }
}

/// One endpoint's daemon link, and everything it knows before it has one.
pub struct Endpoint {
    /// Set once, by the task started in [`Endpoint::new`]: the hash reads the whole image.
    identity: Arc<OnceCell<Identity>>,
    /// Tells a waiting call that the identity is settled. The startup task's end closes it, so a
    /// wait never hangs on a task that died.
    ready: watch::Receiver<bool>,
    launch: Arc<LaunchContext>,
    /// This process's session, fixed for its life: a reconnect keeps the same session name.
    session: String,
    /// Call IDs are endpoint-wide, so one never names two calls, across reconnects included.
    next_call: AtomicU64,
    /// Connections are counted, so the daemon can tell a newer connection from an older one.
    generation: AtomicU64,
    link: Arc<Mutex<Option<Arc<Link>>>>,
}

#[cfg(not(test))]
type Writer = WriteHalf<transport::Client>;
#[cfg(test)]
type Writer = tests::ControlledWriter;

/// A live connection to the daemon, shared by every call in flight.
struct Link {
    epoch: String,
    writer: Mutex<Option<Writer>>,
    pending: std::sync::Mutex<HashMap<u64, Pending>>,
    event_pending: std::sync::Mutex<HashMap<u64, PendingEvent>>,
    #[cfg(test)]
    overflow_hook: std::sync::Mutex<Option<Arc<tests::OverflowHook>>>,
    /// Cancelled when the link ends: every call on it is lost, and the reader stops.
    closed: CancellationToken,
    memory: Arc<crate::lease::BytePool>,
    control_memory: Arc<crate::lease::BytePool>,
}

#[derive(Clone)]
struct PendingEvent {
    tx: mpsc::Sender<(ipc::EventWireFrame, crate::lease::Lease)>,
    overflowed: Option<Arc<AtomicBool>>,
}

/// Removes the event routing entry (and optionally cancels a stream) if dropped before `disarm`.
struct EventRegistration {
    link: Arc<Link>,
    id: u64,
    active: bool,
    cancel_on_drop: bool,
}

impl EventRegistration {
    fn new(link: Arc<Link>, id: u64) -> Self {
        Self {
            link,
            id,
            active: true,
            cancel_on_drop: false,
        }
    }

    fn arm_cancel(&mut self) {
        self.cancel_on_drop = true;
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for EventRegistration {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let removed = lock(&self.link.event_pending).remove(&self.id).is_some();
        if self.cancel_on_drop && removed && !self.link.closed.is_cancelled() {
            tokio::spawn(Arc::clone(&self.link).cancel_event(self.id));
        }
    }
}

/// Handle to a live daemon-backed `events/stream` subscription.
pub struct EndpointEventStream {
    link: Arc<Link>,
    id: u64,
    rx: mpsc::Receiver<(ipc::EventWireFrame, crate::lease::Lease)>,
    overflowed: Arc<AtomicBool>,
}

impl EndpointEventStream {
    pub async fn next(&mut self) -> Option<(ipc::EventWireFrame, crate::lease::Lease)> {
        self.rx.recv().await
    }

    pub(crate) fn link_closed(&self) -> bool {
        self.link.closed.is_cancelled()
    }

    pub(crate) fn overflowed(&self) -> bool {
        self.overflowed.load(Ordering::Acquire)
    }

    pub async fn cancel(&mut self) {
        let removed = lock(&self.link.event_pending).remove(&self.id).is_some();
        if removed {
            Arc::clone(&self.link).cancel_event(self.id).await;
        }
    }
}

impl Drop for EndpointEventStream {
    fn drop(&mut self) {
        let removed = lock(&self.link.event_pending).remove(&self.id).is_some();
        if removed && !self.link.closed.is_cancelled() {
            tokio::spawn(Arc::clone(&self.link).cancel_event(self.id));
        }
    }
}

/// A call waiting for its result.
struct Pending {
    result: oneshot::Sender<(CallToolResult, Option<crate::lease::Lease>)>,
    progress: Option<watch::Sender<Option<wait::Progress>>>,
    control: bool,
}

/// Removes the result routing entry even if the caller's future is dropped.
struct Registration<'a> {
    link: &'a Link,
    id: u64,
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        lock(&self.link.pending).remove(&self.id);
    }
}

impl Link {
    /// Sends a complete cancellation or closes the link, even if the future is dropped.
    fn cancel_event(
        self: Arc<Self>,
        id: u64,
    ) -> impl std::future::Future<Output = ()> + Send + 'static {
        let close_on_failure = self.closed.clone().drop_guard();
        async move {
            let mut attempted = false;
            if matches!(
                tokio::time::timeout(
                    CANCEL_WAIT,
                    self.send(&ToDaemon::EventsCancel { id }, &mut attempted),
                )
                .await,
                Ok(Ok(true))
            ) {
                close_on_failure.disarm();
            }
        }
    }

    /// A write that ends early may leave half a frame. The drop guard poisons the stream
    /// before the writer unlocks, including when the future itself is dropped.
    async fn send(&self, frame: &ToDaemon, attempted: &mut bool) -> std::io::Result<bool> {
        let frame = ipc::encode_frame_leased(
            frame,
            &self.memory,
            &self.control_memory,
            ipc::daemon_control(frame),
        )?;
        let mut writer = tokio::select! {
            biased;
            _ = self.closed.cancelled() => return Ok(false),
            writer = self.writer.lock() => writer,
        };
        let Some(writer) = writer.as_mut() else {
            return Ok(false);
        };
        Ok(tokio::select! {
            biased;
            _ = self.closed.cancelled() => false,
            sent = async {
                let poison = self.closed.drop_guard_ref();
                *attempted = true;
                if ipc::write_raw(&mut *writer, &frame.bytes).await.is_err() {
                    return false;
                }
                poison.disarm();
                true
            } => sent,
        })
    }
}

impl Endpoint {
    /// Creates the endpoint and starts identifying this build at once: the hash reads the whole
    /// image, so one task owns it from start to finish and callers only wait on the result.
    pub fn new(launch: Arc<LaunchContext>) -> std::io::Result<Self> {
        let session = crate::id::random_chars(26)?;
        let identity: Arc<OnceCell<Identity>> = Arc::new(OnceCell::new());
        let (announce, ready) = watch::channel(false);
        let stored = Arc::clone(&identity);
        let identifying = Arc::clone(&launch);
        tokio::spawn(async move {
            let outcome =
                match tokio::task::spawn_blocking(move || runtime::identity(&identifying)).await {
                    Ok(Ok(identity)) => match runtime::paths(&identity.namespace) {
                        Ok(paths) => Ok((identity, paths)),
                        Err(error) => Err(format!("cannot prepare the runtime directory: {error}")),
                    },
                    Ok(Err(error)) => Err(format!("cannot identify this fastexec build: {error}")),
                    Err(error) => Err(format!("cannot wait for the build identity: {error}")),
                };
            let _ = stored.set(outcome);
            // Then this task ends, dropping the sender: a later wait sees the channel close.
            let _ = announce.send(true);
        });
        Ok(Self {
            identity,
            ready,
            launch,
            session,
            next_call: AtomicU64::new(1),
            generation: AtomicU64::new(1),
            link: Arc::new(Mutex::new(None)),
        })
    }

    /// Waits for the identity the startup task settles, and answers with it. The wait holds no
    /// work, so a caller dropped mid-wait strands nothing and repeats nothing.
    async fn identity(&self) -> Result<&(runtime::Identity, runtime::Paths), String> {
        if let Some(outcome) = self.identity.get() {
            return settled(outcome);
        }
        let mut ready = self.ready.clone();
        let _ = ready.changed().await;
        match self.identity.get() {
            Some(outcome) => settled(outcome),
            None => Err("the build identity was not read".to_owned()),
        }
    }

    pub(crate) fn memory(&self) -> &Arc<crate::lease::BytePool> {
        self.launch.memory()
    }

    pub(crate) fn control_memory(&self) -> &Arc<crate::lease::BytePool> {
        self.launch.control_memory()
    }

    /// Runs one call on the daemon and answers with its result.
    #[cfg(test)]
    pub async fn call(
        &self,
        tool: &str,
        arguments: serde_json::Value,
        request_id: String,
        client_name: &str,
        cancel: CancellationToken,
        progress: Option<watch::Sender<Option<wait::Progress>>>,
    ) -> CallToolResult {
        let mut tracked = self
            .call_tracked(tool, arguments, request_id, client_name, cancel, progress)
            .await;
        drop(tracked.lease.take());
        tracked.result
    }

    pub async fn call_tracked(
        &self,
        tool: &str,
        arguments: serde_json::Value,
        request_id: String,
        client_name: &str,
        cancel: CancellationToken,
        progress: Option<watch::Sender<Option<wait::Progress>>>,
    ) -> TrackedResult {
        let plain_err = |msg: String| TrackedResult::plain(error(msg));
        let action = arguments
            .get("action")
            .and_then(|action| action.as_str())
            .unwrap_or("call")
            .to_owned();
        // A call this large is refused here: sending it would only make the daemon refuse it,
        // and the caller would have waited for a connection to hear that.
        match crate::lease::json_len(&arguments) {
            Ok(length) if length > ipc::CALL_LIMIT => {
                return plain_err(format!(
                    "The call is {} bytes; calls are limited to 3 MiB.",
                    length
                ));
            }
            Ok(_) => {}
            Err(problem) => return plain_err(format!("cannot serialize the call: {problem}")),
        }
        let id = self.next_call.fetch_add(1, Ordering::Relaxed);
        let deadline = std::time::Instant::now() + runtime::START_WAIT;
        let link = tokio::select! {
            biased;
            _ = cancel.cancelled() => return plain_err("Cancelled before the call was sent.".into()),
            linked = self.link_with_retry(client_name, deadline) => match linked {
                Ok(link) => link,
                Err(message) => {
                    return plain_err(format!("Cannot reach the fastexec daemon: {message}"));
                }
            }
        };
        let (sender, mut answer) = oneshot::channel();
        {
            let mut pending = lock(&link.pending);
            pending.insert(
                id,
                Pending {
                    result: sender,
                    progress,
                    control: tool == "exec" && matches!(action.as_str(), "list" | "kill"),
                },
            );
        }
        let _registration = Registration { link: &link, id };
        let frame = ToDaemon::Call {
            id,
            tool: tool.to_owned(),
            request_id: request_id.clone(),
            arguments,
        };
        let lost = || TrackedResult::plain(self.lost(&link, tool, &action, id, &request_id));
        let to_tracked =
            |(result, lease): (CallToolResult, Option<crate::lease::Lease>)| TrackedResult {
                result,
                overload: false,
                task_meta: None,
                lease,
                payload_owner: Default::default(),
            };
        let mut attempted = false;
        let sent = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err("Cancelled before the call was sent.".to_owned()),
            sent = tokio::time::timeout(WRITE_WAIT, link.send(&frame, &mut attempted)) => match sent {
                Ok(Ok(true)) => Ok(()),
                Ok(Ok(false)) => Err("The daemon connection closed before the call was sent.".to_owned()),
                Ok(Err(error)) => Err(error.to_string()),
                Err(_) => Err("Timed out waiting for the daemon writer before the call was sent.".to_owned()),
            },
        };
        if let Err(reason) = sent {
            if !attempted {
                return plain_err(reason);
            }
            // The write guard closed the link. A result received before EOF still wins.
            return answer.await.map(to_tracked).unwrap_or_else(|_| lost());
        }
        tokio::select! {
            biased;
            answer = &mut answer => answer.map(to_tracked).unwrap_or_else(|_| lost()),
            _ = cancel.cancelled() => {
                let mut attempted = false;
                let frame = ToDaemon::Cancel { id };
                // Finish a Cancel frame already in progress even if the call's result arrives
                // meanwhile. Interrupting that frame would sacrifice unrelated pending calls.
                let sent = tokio::time::timeout(CANCEL_WAIT, link.send(&frame, &mut attempted)).await;
                if let Ok(pair) = answer.try_recv() {
                    return to_tracked(pair);
                }
                match sent {
                    Ok(Ok(true)) => plain_err("Cancelled; the task keeps running.".into()),
                    Ok(Err(error)) if !attempted && !link.closed.is_cancelled() => {
                        plain_err(format!("The call was sent, but Cancel could not be sent ({error}). The call's result is unknown."))
                    }
                    Err(_) if !attempted && !link.closed.is_cancelled() => {
                        plain_err("The call was sent, but Cancel timed out before it was sent. The call's result is unknown.".into())
                    }
                    _ => lost(),
                }
            }
        }
    }

    /// Says Bye when there is a link and waits for the daemon to read it, so this session is gone
    /// before the process exits. A failed write is the daemon being gone already, which is the
    /// same outcome.
    pub async fn bye(&self) {
        let _ = tokio::time::timeout(BYE_WAIT, async {
            let link = self.link.lock().await.clone();
            let Some(link) = link else {
                return;
            };
            let sent = link.send(&ToDaemon::Bye, &mut false).await;
            // The reader drains what the daemon still sends until it reads the Bye and closes the
            // stream, which cancels `closed`.
            if matches!(sent, Ok(true)) {
                link.closed.cancelled().await;
            }
        })
        .await;
    }

    /// The live link, connecting first when there is none. One caller connects at a time; the
    /// others find the link it made.
    async fn link(
        &self,
        client_name: &str,
        deadline: std::time::Instant,
    ) -> Result<Arc<Link>, SetupError> {
        let mut slot = self.link.lock().await;
        if let Some(link) = slot.as_ref()
            && !link.closed.is_cancelled()
        {
            return Ok(Arc::clone(link));
        }
        *slot = None;
        let (identity, paths) = self.identity().await?;
        let (mut client, startup) =
            runtime::connect_or_start(identity, paths, deadline, &[]).await?;
        let hello = hello_owned(
            &self.launch,
            identity,
            &self.session,
            self.generation.fetch_add(1, Ordering::Relaxed),
            client_name,
        )?;
        let epoch = match tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            handshake_in(
                &mut client,
                hello,
                identity,
                self.memory(),
                self.control_memory(),
            ),
        )
        .await
        {
            Ok(answer) => answer?,
            Err(_) => {
                return Err("the daemon did not answer the Hello in time"
                    .to_owned()
                    .into());
            }
        };
        // The start is exclusive until the daemon has answered, and no longer.
        drop(startup);
        let (reader, writer) = tokio::io::split(client);
        #[cfg(test)]
        let writer = tests::ControlledWriter::new(writer);
        let link = Arc::new(Link {
            epoch,
            writer: Mutex::new(Some(writer)),
            pending: std::sync::Mutex::new(HashMap::new()),
            event_pending: std::sync::Mutex::new(HashMap::new()),
            #[cfg(test)]
            overflow_hook: std::sync::Mutex::new(None),
            closed: CancellationToken::new(),
            memory: Arc::clone(self.launch.memory()),
            control_memory: Arc::clone(self.launch.control_memory()),
        });
        tokio::spawn(read_loop(
            Arc::clone(&link),
            reader,
            Arc::clone(&self.link),
            Arc::clone(self.launch.memory()),
            Arc::clone(self.launch.control_memory()),
        ));
        *slot = Some(Arc::clone(&link));
        Ok(link)
    }

    /// Connects or reuses the current link, retrying once when a stale socket fails before a
    /// `Welcome` (without retrying local memory-budget refusals).
    async fn link_with_retry(
        &self,
        client_name: &str,
        deadline: std::time::Instant,
    ) -> Result<Arc<Link>, SetupError> {
        match self.link(client_name, deadline).await {
            Ok(link) => Ok(link),
            // No request was written before a Welcome, so one reconnect replays nothing.
            Err(SetupError::MemoryBudget) => Err(SetupError::MemoryBudget),
            Err(_) => self.link(client_name, deadline).await,
        }
    }

    fn event_setup_error(&self, error: SetupError) -> crate::lease::Owned<ipc::IpcEventError> {
        match error {
            SetupError::MemoryBudget => self.event_budget_error(
                "endpoint memory budget full while connecting to fastexec daemon",
            ),
            SetupError::Other(message) => self.event_link_error(message),
        }
    }

    fn event_link_error(
        &self,
        message: impl Into<std::borrow::Cow<'static, str>>,
    ) -> crate::lease::Owned<ipc::IpcEventError> {
        let message = message.into();
        let bytes = message.len().saturating_add(256);
        if let Some(lease) = self
            .memory()
            .try_acquire(bytes)
            .or_else(|| self.control_memory().try_acquire(bytes))
        {
            crate::lease::Owned::new(
                ipc::IpcEventError {
                    code: -32603,
                    message: message.into_owned(),
                    data: None,
                },
                crate::lease::Owner::new(lease),
            )
        } else {
            crate::lease::Owned::new(
                ipc::IpcEventError {
                    code: -32025,
                    message: String::new(),
                    data: None,
                },
                crate::lease::Owner::default(),
            )
        }
    }

    fn event_budget_error(&self, message: &'static str) -> crate::lease::Owned<ipc::IpcEventError> {
        ipc::IpcEventError::memory_full(self.memory(), self.control_memory(), message)
    }

    /// Lists event types from the daemon.
    pub async fn events_list(
        &self,
        client_name: &str,
        cancel: CancellationToken,
    ) -> Result<
        crate::lease::Owned<crate::events::EventsListResult>,
        crate::lease::Owned<ipc::IpcEventError>,
    > {
        let deadline = std::time::Instant::now() + runtime::START_WAIT;
        let link = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(self.event_link_error("events/list cancelled")),
            linked = self.link_with_retry(client_name, deadline) => {
                linked.map_err(|error| self.event_setup_error(error))?
            }
        };
        let id = self.next_call.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = mpsc::channel(EVENT_RPC_CHANNEL_CAPACITY);
        lock(&link.event_pending).insert(
            id,
            PendingEvent {
                tx,
                overflowed: None,
            },
        );
        let _reg = EventRegistration::new(Arc::clone(&link), id);
        let mut attempted = false;
        let frame = ToDaemon::EventsList { id };
        let sent = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return Err(self.event_link_error("events/list cancelled"));
            }
            sent = tokio::time::timeout(
                WRITE_WAIT,
                link.send(&frame, &mut attempted),
            ) => sent,
        };
        match sent {
            Ok(Ok(true)) => {}
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::OutOfMemory => {
                return Err(self
                    .event_budget_error("endpoint memory budget full while sending events/list"));
            }
            _ => {
                return Err(self.event_link_error("failed to send events/list to fastexec daemon"));
            }
        }
        let frame = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return Err(self.event_link_error("events/list cancelled"));
            }
            frame = rx.recv() => frame,
        };
        match frame {
            Some((ipc::EventWireFrame::ListResult(res), lease)) => Ok(crate::lease::Owned::new(
                res,
                crate::lease::Owner::new(lease),
            )),
            Some((ipc::EventWireFrame::Error(err), lease)) => Err(crate::lease::Owned::new(
                err,
                crate::lease::Owner::new(lease),
            )),
            Some((ipc::EventWireFrame::Terminated(_), lease)) => {
                drop(lease);
                Err(self.event_budget_error("endpoint memory budget full during events/list"))
            }
            None if link.closed.is_cancelled() => {
                Err(self.event_link_error("daemon connection closed during events/list"))
            }
            None => Err(self.event_budget_error("endpoint memory budget full during events/list")),
            Some(_) => Err(self.event_link_error("unexpected daemon response during events/list")),
        }
    }

    /// Polls events from the daemon.
    pub async fn events_poll(
        &self,
        client_name: &str,
        request: crate::events::PollRequest,
        cancel: CancellationToken,
    ) -> Result<
        crate::lease::Owned<crate::events::PollResponse>,
        crate::lease::Owned<ipc::IpcEventError>,
    > {
        let deadline = std::time::Instant::now() + runtime::START_WAIT;
        let link = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(self.event_link_error("events/poll cancelled")),
            linked = self.link_with_retry(client_name, deadline) => {
                linked.map_err(|error| self.event_setup_error(error))?
            }
        };
        let id = self.next_call.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = mpsc::channel(EVENT_RPC_CHANNEL_CAPACITY);
        lock(&link.event_pending).insert(
            id,
            PendingEvent {
                tx,
                overflowed: None,
            },
        );
        let _reg = EventRegistration::new(Arc::clone(&link), id);
        let mut attempted = false;
        let frame = ToDaemon::EventsPoll { id, request };
        let sent = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return Err(self.event_link_error("events/poll cancelled"));
            }
            sent = tokio::time::timeout(
                WRITE_WAIT,
                link.send(&frame, &mut attempted),
            ) => sent,
        };
        match sent {
            Ok(Ok(true)) => {}
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::OutOfMemory => {
                return Err(self
                    .event_budget_error("endpoint memory budget full while sending events/poll"));
            }
            _ => {
                return Err(self.event_link_error("failed to send events/poll to fastexec daemon"));
            }
        }
        let frame = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return Err(self.event_link_error("events/poll cancelled"));
            }
            frame = rx.recv() => frame,
        };
        match frame {
            Some((ipc::EventWireFrame::PollResult(res), lease)) => Ok(crate::lease::Owned::new(
                res,
                crate::lease::Owner::new(lease),
            )),
            Some((ipc::EventWireFrame::Error(err), lease)) => Err(crate::lease::Owned::new(
                err,
                crate::lease::Owner::new(lease),
            )),
            Some((ipc::EventWireFrame::Terminated(_), lease)) => {
                drop(lease);
                Err(self.event_budget_error("endpoint memory budget full during events/poll"))
            }
            None if link.closed.is_cancelled() => {
                Err(self.event_link_error("daemon connection closed during events/poll"))
            }
            None => Err(self.event_budget_error("endpoint memory budget full during events/poll")),
            Some(_) => Err(self.event_link_error("unexpected daemon response during events/poll")),
        }
    }

    /// Opens an `events/stream` subscription on the daemon. Returns the initial `ActiveNotice`
    /// and the live stream handle, or an `IpcEventError` if subscription setup failed.
    pub async fn events_stream(
        &self,
        client_name: &str,
        request: crate::events::SubscribeRequest,
        cancel: CancellationToken,
    ) -> Result<
        (
            crate::lease::Owned<crate::events::ActiveNotice>,
            EndpointEventStream,
        ),
        crate::lease::Owned<ipc::IpcEventError>,
    > {
        let deadline = std::time::Instant::now() + runtime::START_WAIT;
        let link = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(self.event_link_error("events/stream cancelled")),
            linked = self.link_with_retry(client_name, deadline) => {
                linked.map_err(|error| self.event_setup_error(error))?
            }
        };
        let id = self.next_call.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = mpsc::channel(EVENT_STREAM_CHANNEL_CAPACITY);
        let overflowed = Arc::new(AtomicBool::new(false));
        lock(&link.event_pending).insert(
            id,
            PendingEvent {
                tx,
                overflowed: Some(Arc::clone(&overflowed)),
            },
        );
        let mut reg = EventRegistration::new(Arc::clone(&link), id);
        let mut attempted = false;
        let frame = ToDaemon::EventsStream { id, request };
        let sent = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return Err(self.event_link_error("events/stream cancelled"));
            }
            sent = tokio::time::timeout(
                WRITE_WAIT,
                link.send(&frame, &mut attempted),
            ) => sent,
        };
        match sent {
            Ok(Ok(true)) => {
                reg.arm_cancel();
            }
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::OutOfMemory => {
                return Err(self.event_budget_error(
                    "endpoint memory budget full while sending events/stream",
                ));
            }
            _ => {
                return Err(
                    self.event_link_error("failed to send events/stream to fastexec daemon")
                );
            }
        }
        let first = tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                return Err(self.event_link_error("events/stream cancelled"));
            }
            frame = rx.recv() => frame,
        };
        match first {
            Some((ipc::EventWireFrame::Active(active), lease)) => {
                reg.disarm();
                Ok((
                    crate::lease::Owned::new(active, crate::lease::Owner::new(lease)),
                    EndpointEventStream {
                        link,
                        id,
                        rx,
                        overflowed,
                    },
                ))
            }
            Some((ipc::EventWireFrame::Error(err), lease)) => {
                // Daemon rejected setup; no server-side stream exists to cancel.
                reg.cancel_on_drop = false;
                Err(crate::lease::Owned::new(
                    err,
                    crate::lease::Owner::new(lease),
                ))
            }
            Some((ipc::EventWireFrame::Terminated(err), lease)) => {
                if err.message.starts_with("daemon ") {
                    reg.cancel_on_drop = false;
                    Err(crate::lease::Owned::new(
                        err,
                        crate::lease::Owner::new(lease),
                    ))
                } else {
                    drop(lease);
                    Err(self.event_budget_error(
                        "endpoint memory budget full before events/stream activated",
                    ))
                }
            }
            None => {
                if link.closed.is_cancelled() {
                    reg.cancel_on_drop = false;
                    Err(self.event_link_error(
                        "daemon connection closed before events/stream activated",
                    ))
                } else {
                    Err(self.event_budget_error(
                        "endpoint memory budget full before events/stream activated",
                    ))
                }
            }
            Some(_) => {
                Err(self
                    .event_link_error("unexpected daemon response before events/stream activated"))
            }
        }
    }

    /// The call may have run, so it is reported, never replayed.
    fn lost(
        &self,
        link: &Link,
        tool: &str,
        action: &str,
        id: u64,
        request_id: &str,
    ) -> CallToolResult {
        if tool == "exec" || tool == "codemode" {
            let action = if tool == "codemode" { tool } else { action };
            return error(format!(
                "The connection to the fastexec daemon (epoch {}) closed during {action} call {id} (request {request_id}, session {}); the operation may have run. Use action list to check.",
                link.epoch, self.session
            ));
        }
        error(format!(
            "The connection to the fastexec daemon (epoch {}) closed during {tool} call {id} (request {request_id}, session {}); the call may or may not have run.",
            link.epoch, self.session
        ))
    }
}

#[cfg(test)]
impl Endpoint {
    /// An endpoint whose identity is already settled and whose daemon is the listener at
    /// `paths.endpoint`. The unit tests own calls, links and cancellation, not identity
    /// creation, so nothing here hashes an image or starts a process.
    fn for_test(identity: runtime::Identity, paths: runtime::Paths) -> Self {
        let settled = OnceCell::new();
        let _ = settled.set(Ok((identity, paths)));
        let (announce, ready) = watch::channel(true);
        drop(announce);
        Self {
            identity: Arc::new(settled),
            ready,
            launch: Arc::new(LaunchContext::new(std::env::temp_dir(), Vec::new())),
            session: "s".repeat(26),
            next_call: AtomicU64::new(1),
            generation: AtomicU64::new(1),
            link: Arc::new(Mutex::new(None)),
        }
    }
}

/// Reads every frame the daemon sends: a call's progress, its result, and the end of the
/// connection, which loses every call still waiting.
async fn read_loop(
    link: Arc<Link>,
    mut reader: ReadHalf<transport::Client>,
    slot: Arc<Mutex<Option<Arc<Link>>>>,
    pool: Arc<crate::lease::BytePool>,
    control_pool: Arc<crate::lease::BytePool>,
) {
    loop {
        // Selecting on `closed` is what lets a closing link wake a reader that is waiting for
        // bytes: a flag alone would leave it parked in `read_endpoint_frame_leased`.
        let frame = tokio::select! {
            _ = link.closed.cancelled() => None,
            frame = ipc::read_endpoint_frame_leased(
                &mut reader,
                crate::lease::FRAME_LIMIT,
                &pool,
                &control_pool,
                |id| lock(&link.pending).get(&id).is_some_and(|entry| entry.control),
            ) => frame.ok(),
        };
        match frame {
            Some(ipc::LeasedEndpointFrame::Frame(
                ToEndpoint::Progress {
                    id,
                    seconds,
                    message,
                },
                _lease,
            )) => {
                let pending = lock(&link.pending);
                if let Some(progress) = pending.get(&id).and_then(|entry| entry.progress.as_ref()) {
                    progress.send_replace(Some((seconds, message)));
                }
            }
            Some(ipc::LeasedEndpointFrame::Frame(ToEndpoint::Result { id, result }, lease)) => {
                // A call that was cancelled is no longer pending, and its late result is not
                // anyone's answer.
                let entry = lock(&link.pending).remove(&id);
                if let Some(entry) = entry {
                    let _ = entry.result.send((result, Some(lease)));
                } else {
                    // The unclaimed payload must die before its allocation owner.
                    drop(result);
                    drop(lease);
                }
            }
            Some(ipc::LeasedEndpointFrame::Frame(ToEndpoint::EventFrame { id, frame }, lease)) => {
                let pending = lock(&link.event_pending).get(&id).cloned();
                if let Some(entry) = pending {
                    if let Err(err) = entry.tx.try_send((frame, lease)) {
                        #[cfg(test)]
                        let hook = lock(&link.overflow_hook).take();
                        #[cfg(test)]
                        if let Some(hook) = &hook {
                            hook.arrived.notify_one();
                            hook.resume.notified().await;
                        }
                        // The caller that removes the route owns cancellation.
                        let removed = lock(&link.event_pending).remove(&id);
                        let (frame, lease) = match err {
                            mpsc::error::TrySendError::Full(item) => {
                                if let Some(removed) = removed {
                                    if let Some(overflowed) = &removed.overflowed {
                                        overflowed.store(true, Ordering::Release);
                                    }
                                    tokio::spawn(Arc::clone(&link).cancel_event(id));
                                }
                                item
                            }
                            mpsc::error::TrySendError::Closed(item) => item,
                        };
                        drop(entry);
                        drop(frame);
                        drop(lease);
                        #[cfg(test)]
                        if let Some(hook) = hook {
                            hook.done.notify_one();
                        }
                    }
                } else {
                    drop(frame);
                    drop(lease);
                }
            }
            Some(ipc::LeasedEndpointFrame::OverloadProgress) => {}
            Some(ipc::LeasedEndpointFrame::OverloadResult { id: Some(id) }) => {
                let entry = lock(&link.pending).remove(&id);
                if let Some(entry) = entry {
                    let _ = entry.result.send((
                        error(format!(
                            "Endpoint active memory budget ({} bytes) is full while receiving daemon response; use action list or poll once in-flight transfers complete.",
                            pool.limit()
                        )),
                        None,
                    ));
                }
            }
            Some(ipc::LeasedEndpointFrame::OverloadEvent { id: Some(id) }) => {
                if let Some(entry) = lock(&link.event_pending).remove(&id) {
                    if entry.overflowed.is_some() {
                        tokio::spawn(Arc::clone(&link).cancel_event(id));
                    }
                    let frame = ipc::EventWireFrame::Terminated(ipc::IpcEventError {
                        code: -32025,
                        message: "endpoint active memory budget full during event stream".into(),
                        data: None,
                    });
                    if let Ok(bytes) = crate::lease::json_len(&frame)
                        && let Some(lease) = control_pool.try_acquire(bytes)
                    {
                        let _ = entry.tx.try_send((frame, lease));
                    }
                }
            }
            // Nothing else belongs on this connection after the Welcome.
            _ => break,
        }
    }
    link.closed.cancel();
    // Dropping the senders is what makes a waiting call see the link end.
    lock(&link.pending).clear();
    lock(&link.event_pending).clear();
    drop(reader);
    // Other stream handles may keep the Link alive after cancellation.
    drop(link.writer.lock().await.take());
    // A reconnect holds the slot while handshaking; do not keep the old writer alive behind it.
    let ended = Arc::downgrade(&link);
    drop(link);
    let mut slot = slot.lock().await;
    if slot
        .as_ref()
        .is_some_and(|current| ended.ptr_eq(&Arc::downgrade(current)))
    {
        *slot = None;
    }
}

/// Reserves the temporary environment and identity copies before building Hello.
pub(crate) fn hello_owned(
    launch: &LaunchContext,
    identity: &runtime::Identity,
    session: &str,
    connection: u64,
    client_name: &str,
) -> Result<crate::lease::Owned<ToDaemon>, SetupError> {
    let copied = ipc::launch_wire_capacity(launch)
        .saturating_add(identity.namespace.len())
        .saturating_add(identity.build.len())
        .saturating_add(session.len())
        .saturating_add(client_name.len());
    let lease = launch
        .memory()
        .try_acquire(copied)
        .or_else(|| {
            (copied <= 64 * 1024)
                .then(|| launch.control_memory().try_acquire(copied))
                .flatten()
        })
        .ok_or(SetupError::MemoryBudget)?;
    let (cwd, env) = ipc::launch_to_wire(launch);
    Ok(crate::lease::Owned::new(
        ToDaemon::Hello {
            protocol: ipc::PROTOCOL,
            namespace: identity.namespace.clone(),
            build: identity.build.clone(),
            session: session.to_owned(),
            connection,
            client_name: client_name.to_owned(),
            cwd,
            env,
        },
        crate::lease::Owner::new(lease),
    ))
}

/// Sends the Hello and returns the Welcome's epoch, or the reason the daemon refused it.
pub(crate) async fn handshake(
    client: &mut transport::Client,
    hello: crate::lease::Owned<ToDaemon>,
    identity: &runtime::Identity,
) -> Result<String, SetupError> {
    handshake_in(
        client,
        hello,
        identity,
        crate::lease::process_pool(),
        crate::lease::control_pool(),
    )
    .await
}

async fn handshake_in(
    client: &mut transport::Client,
    hello: crate::lease::Owned<ToDaemon>,
    identity: &runtime::Identity,
    main: &Arc<crate::lease::BytePool>,
    control: &Arc<crate::lease::BytePool>,
) -> Result<String, SetupError> {
    {
        let frame = ipc::encode_frame_leased(
            &hello.value,
            main,
            control,
            ipc::daemon_control(&hello.value),
        )
        .map_err(|error| {
            if error.kind() == std::io::ErrorKind::OutOfMemory {
                SetupError::MemoryBudget
            } else {
                SetupError::Other(format!("cannot send the Hello: {error}"))
            }
        })?;
        ipc::write_raw(client, &frame.bytes)
            .await
            .map_err(|error| SetupError::Other(format!("cannot send the Hello: {error}")))?;
    }
    match ipc::read_endpoint_frame_leased(client, crate::lease::FRAME_LIMIT, main, control, |_| {
        false
    })
    .await
    {
        Ok(ipc::LeasedEndpointFrame::Frame(
            ToEndpoint::Welcome {
                protocol,
                namespace,
                build,
                epoch,
            },
            _lease,
        )) => {
            // A Welcome is only a Welcome from the daemon this endpoint meant to reach.
            if protocol != ipc::PROTOCOL {
                return Err(format!(
                    "the daemon speaks protocol {protocol}, not {}",
                    ipc::PROTOCOL
                )
                .into());
            }
            if namespace != identity.namespace {
                return Err(format!(
                    "the daemon serves namespace {namespace}, not {}",
                    identity.namespace
                )
                .into());
            }
            if build != identity.build {
                return Err(
                    format!("the daemon runs build {build}, not {}", identity.build).into(),
                );
            }
            Ok(epoch)
        }
        Ok(ipc::LeasedEndpointFrame::Frame(ToEndpoint::Reject { message }, _lease)) => {
            Err(SetupError::Other(message))
        }
        Ok(
            ipc::LeasedEndpointFrame::OverloadResult { .. }
            | ipc::LeasedEndpointFrame::OverloadEvent { .. }
            | ipc::LeasedEndpointFrame::OverloadProgress,
        ) => Err(SetupError::MemoryBudget),
        Ok(other) => Err(format!("the daemon answered the Hello with {other:?}").into()),
        Err(error) => Err(format!("cannot read the Welcome: {error}").into()),
    }
}

fn settled(outcome: &Identity) -> Result<&(runtime::Identity, runtime::Paths), String> {
    outcome.as_ref().map_err(|message| message.clone())
}

fn error(text: impl Into<String>) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(text.into())])
}

/// A lock whose holder never awaits while it is held.
fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::OsText;
    use crate::transport::{Endpoint as TestEndpoint, Listener};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::AsyncWrite;
    use tokio::time::timeout;

    /// An opt-in write barrier for one link, independent of the transport's buffering.
    pub(super) struct ControlledWriter {
        inner: WriteHalf<transport::Client>,
        pause: Option<WritePause>,
    }

    struct WritePause {
        remaining: usize,
        blocked: Option<oneshot::Sender<()>>,
    }

    impl ControlledWriter {
        pub(super) fn new(inner: WriteHalf<transport::Client>) -> Self {
            Self { inner, pause: None }
        }
    }

    impl AsyncWrite for ControlledWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<std::io::Result<usize>> {
            let this = self.get_mut();
            if bytes.is_empty() {
                return Poll::Ready(Ok(0));
            }
            let length = if let Some(pause) = &mut this.pause {
                if pause.remaining == 0 {
                    if let Some(blocked) = pause.blocked.take() {
                        let _ = blocked.send(());
                    }
                    return Poll::Pending;
                }
                bytes.len().min(pause.remaining)
            } else {
                bytes.len()
            };
            let written = Pin::new(&mut this.inner).poll_write(cx, &bytes[..length]);
            if let Poll::Ready(Ok(count)) = &written
                && let Some(pause) = &mut this.pause
            {
                pause.remaining -= count;
            }
            written
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_flush(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
        }
    }

    /// Answers one Hello on a real endpoint with `welcome`, then holds the connection until the
    /// client lets go.
    async fn welcome_with(
        endpoint: &TestEndpoint,
        welcome: ToEndpoint,
    ) -> tokio::task::JoinHandle<()> {
        let mut listener = Listener::bind(endpoint.name()).unwrap();
        tokio::spawn(async move {
            let Ok(mut conn) = listener.accept().await else {
                return;
            };
            let _ = ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT).await;
            let _ = ipc::write_frame(&mut conn, &welcome).await;
            let mut buffer = [0u8; 1];
            let _ = tokio::io::AsyncReadExt::read(&mut conn, &mut buffer).await;
        })
    }

    fn identity() -> runtime::Identity {
        runtime::Identity {
            build: "b".repeat(64),
            namespace: "0123456789abcdef".to_owned(),
        }
    }

    fn hello(identity: &runtime::Identity) -> ToDaemon {
        ToDaemon::Hello {
            protocol: ipc::PROTOCOL,
            namespace: identity.namespace.clone(),
            build: identity.build.clone(),
            session: "s".repeat(26),
            connection: 1,
            client_name: "test".to_owned(),
            cwd: OsText::Text("/work".to_owned()),
            env: Vec::new(),
        }
    }

    #[test]
    fn ipc_ingress_hello_reserves_copies_before_construction() {
        let identity = identity();
        let launch = LaunchContext::new(
            std::env::temp_dir(),
            vec![("SAMPLE".into(), "hello λ".into())],
        );
        let copied = ipc::launch_wire_capacity(&launch)
            + identity.namespace.len()
            + identity.build.len()
            + 1
            + 4;
        for (limit, fits) in [(copied - 1, false), (copied, true)] {
            let pool = crate::lease::BytePool::new(limit);
            let launch = launch
                .for_session()
                .with_memory(Arc::clone(&pool))
                .with_control_memory(crate::lease::BytePool::new(0));
            let hello = hello_owned(&launch, &identity, "s", 1, "test");
            assert_eq!(hello.is_ok(), fits);
            assert_eq!(pool.used(), if fits { copied } else { 0 });
            drop(hello);
            assert_eq!(pool.used(), 0);
        }
        let control = crate::lease::BytePool::new(copied);
        let launch = launch
            .with_memory(crate::lease::BytePool::new(0))
            .with_control_memory(Arc::clone(&control));
        let hello = hello_owned(&launch, &identity, "s", 1, "test").unwrap();
        assert_eq!(control.used(), copied);
        drop(hello);
        assert_eq!(control.used(), 0);
    }

    const TEST_EPOCH: &str = "eeeeeeee";

    fn welcome(identity: &runtime::Identity) -> ToEndpoint {
        ToEndpoint::Welcome {
            protocol: ipc::PROTOCOL,
            namespace: identity.namespace.clone(),
            build: identity.build.clone(),
            epoch: TEST_EPOCH.to_owned(),
        }
    }

    /// A Welcome is only accepted from the daemon the endpoint meant to reach: the protocol, the
    /// namespace and the build all have to be this one's.
    #[tokio::test]
    async fn a_welcome_must_name_this_protocol_namespace_and_build() {
        let identity = identity();
        let mut wrong_protocol = welcome(&identity);
        if let ToEndpoint::Welcome { protocol, .. } = &mut wrong_protocol {
            *protocol = ipc::PROTOCOL + 1;
        }
        let mut legacy_protocol = welcome(&identity);
        if let ToEndpoint::Welcome { protocol, .. } = &mut legacy_protocol {
            *protocol = ipc::PROTOCOL - 1;
        }
        let mut wrong_namespace = welcome(&identity);
        if let ToEndpoint::Welcome { namespace, .. } = &mut wrong_namespace {
            *namespace = "ffffffffffffffff".to_owned();
        }
        let mut wrong_build = welcome(&identity);
        if let ToEndpoint::Welcome { build, .. } = &mut wrong_build {
            *build = "c".repeat(64);
        }
        let cases = [
            (welcome(&identity), None),
            (wrong_protocol, Some("speaks protocol")),
            (legacy_protocol, Some("speaks protocol")),
            (wrong_namespace, Some("serves namespace")),
            (wrong_build, Some("runs build")),
        ];
        for (index, (answer, expected)) in cases.into_iter().enumerate() {
            let endpoint = TestEndpoint::new(&format!("welcome{index}"));
            let server = welcome_with(&endpoint, answer).await;
            let mut client = transport::connect(endpoint.name()).await.unwrap();
            // The whole exchange is bounded, so a missing Hello or Welcome fails here instead of
            // hanging the harness.
            let answer = timeout(
                Duration::from_secs(30),
                handshake(
                    &mut client,
                    crate::lease::Owned::new(hello(&identity), Default::default()),
                    &identity,
                ),
            )
            .await
            .expect("the exchange finished");
            drop(client);
            let _ = timeout(Duration::from_secs(30), server).await;
            match expected {
                None => assert_eq!(answer.unwrap(), TEST_EPOCH),
                Some(text) => {
                    let message = answer.unwrap_err().to_string();
                    assert!(message.contains(text), "{message}");
                }
            }
        }
    }

    /// A daemon the test drives: a real listener at a path the endpoint is pointed at, so the
    /// frames the endpoint sends are the thing under test.
    fn test_daemon(tag: &str) -> (runtime::Identity, runtime::Paths, Listener, TestEndpoint) {
        let target = TestEndpoint::new(tag);
        let listener = Listener::bind(target.name()).unwrap();
        let paths = runtime::Paths {
            root: std::env::temp_dir().join(format!("fastexec-endpoint-{tag}")),
            ns: std::env::temp_dir().join(format!("fastexec-endpoint-{tag}-ns")),
            endpoint: target.name().to_owned(),
        };
        // The target stays with the caller: on Unix it removes its socket directory when it is
        // dropped, and the endpoint has to be able to connect first.
        (identity(), paths, listener, target)
    }

    /// The peer has finished, and said so: a peer that panicked or hung is a failed test, not a
    /// quiet one.
    async fn finish(peer: tokio::task::JoinHandle<()>) {
        timeout(Duration::from_secs(10), peer)
            .await
            .expect("the peer finished")
            .expect("the peer did not panic");
    }

    async fn failed_event_rpc(
        endpoint: &Endpoint,
        method: &str,
    ) -> crate::lease::Owned<ipc::IpcEventError> {
        let cancel = CancellationToken::new();
        match method {
            "list" => endpoint.events_list("test", cancel).await.err(),
            "poll" => endpoint
                .events_poll(
                    "test",
                    crate::events::PollRequest {
                        name: "external.event".into(),
                        ..Default::default()
                    },
                    cancel,
                )
                .await
                .err(),
            "stream" => endpoint
                .events_stream(
                    "test",
                    crate::events::SubscribeRequest {
                        name: "external.event".into(),
                        ..Default::default()
                    },
                    cancel,
                )
                .await
                .err(),
            _ => unreachable!(),
        }
        .expect("the event RPC must report the memory refusal")
    }

    #[tokio::test]
    async fn first_event_connection_budget_refusal_preserves_category_and_recovers() {
        for method in ["list", "poll", "stream"] {
            let (identity, paths, mut listener, _target) =
                test_daemon(&format!("evt-init-{method}"));
            let response = welcome(&identity);
            let peer = tokio::spawn(async move {
                let mut refused = listener.accept().await.unwrap();
                assert!(
                    ipc::read_frame::<_, ToDaemon>(&mut refused, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap()
                        .is_none(),
                    "budget refusal must not send a partial Hello"
                );
                drop(refused);
                let mut conn = listener.accept().await.unwrap();
                assert!(matches!(
                    ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap(),
                    Some(ToDaemon::Hello { .. })
                ));
                ipc::write_frame(&mut conn, &response).await.unwrap();
                let request = ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap()
                    .unwrap();
                let (id, frame) = match request {
                    ToDaemon::EventsList { id } => (
                        id,
                        ipc::EventWireFrame::ListResult(crate::events::EventsListResult {
                            result_type: "complete".into(),
                            events: Vec::new(),
                        }),
                    ),
                    ToDaemon::EventsPoll { id, .. } => (
                        id,
                        ipc::EventWireFrame::PollResult(crate::events::PollResponse {
                            result_type: "complete".into(),
                            events: Vec::new(),
                            cursor: "eeeeeeee:0".into(),
                            truncated: false,
                            has_more: false,
                            next_poll_ms: 2000,
                        }),
                    ),
                    ToDaemon::EventsStream { id, .. } => (
                        id,
                        ipc::EventWireFrame::Active(crate::events::ActiveNotice {
                            cursor: "eeeeeeee:0".into(),
                            truncated: false,
                            heartbeat_ms: 30_000,
                        }),
                    ),
                    other => panic!("unexpected event request after recovery: {other:?}"),
                };
                ipc::write_frame(&mut conn, &ToEndpoint::EventFrame { id, frame })
                    .await
                    .unwrap();
                loop {
                    match ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap()
                        .unwrap()
                    {
                        ToDaemon::EventsCancel { .. } => {}
                        ToDaemon::Bye => break,
                        other => panic!("unexpected frame after event response: {other:?}"),
                    }
                }
            });
            let pool = crate::lease::BytePool::new(16 << 10);
            let control = crate::lease::BytePool::new(512);
            let mut endpoint = Endpoint::for_test(identity, paths);
            endpoint.launch = Arc::new(
                LaunchContext::new(
                    std::env::temp_dir(),
                    vec![(
                        "SAMPLE".into(),
                        "ordinary environment value ".repeat(40).into(),
                    )],
                )
                .with_memory(Arc::clone(&pool))
                .with_control_memory(Arc::clone(&control)),
            );
            let full = pool.try_acquire(pool.limit()).unwrap();
            let error = timeout(Duration::from_secs(10), failed_event_rpc(&endpoint, method))
                .await
                .unwrap();
            assert_eq!(error.value.code, -32025, "{method}: {error:?}");
            assert_eq!(
                error.value.data,
                Some(serde_json::json!({"limit": "process_memory", "max": pool.limit()}))
            );
            assert!(error.owner.get::<crate::lease::Lease>().unwrap().bytes() > 0);
            assert!(control.used() > 0);
            assert!(endpoint.link.lock().await.is_none());
            drop(error);
            assert_eq!(control.used(), 0);
            drop(full);
            assert_eq!(pool.used(), 0);
            timeout(Duration::from_secs(10), async {
                match method {
                    "list" => {
                        let result = endpoint
                            .events_list("test", CancellationToken::new())
                            .await
                            .unwrap();
                        assert!(result.value.events.is_empty());
                    }
                    "poll" => {
                        let result = endpoint
                            .events_poll(
                                "test",
                                crate::events::PollRequest {
                                    name: "external.event".into(),
                                    ..Default::default()
                                },
                                CancellationToken::new(),
                            )
                            .await
                            .unwrap();
                        assert!(result.value.events.is_empty());
                    }
                    "stream" => {
                        let (active, mut stream) = endpoint
                            .events_stream(
                                "test",
                                crate::events::SubscribeRequest {
                                    name: "external.event".into(),
                                    ..Default::default()
                                },
                                CancellationToken::new(),
                            )
                            .await
                            .unwrap();
                        assert_eq!(active.value.cursor, "eeeeeeee:0");
                        stream.cancel().await;
                    }
                    _ => unreachable!(),
                }
            })
            .await
            .unwrap();
            endpoint.bye().await;
            finish(peer).await;
            assert_eq!(pool.used(), 0);
            assert_eq!(control.used(), 0);
        }
    }

    #[tokio::test]
    async fn event_cancel_failures_close_the_transport_with_link_owners_retained() {
        for (case, mode) in [
            "budget",
            "writer_timeout",
            "drop_future",
            "drop_stream",
            "drop_registration",
        ]
        .into_iter()
        .enumerate()
        {
            let (identity, paths, mut listener, _target) = test_daemon(&format!("evt-stop-{case}"));
            let response = welcome(&identity);
            let (ended, closed) = oneshot::channel();
            let peer = tokio::spawn(async move {
                let mut conn = listener.accept().await.unwrap();
                assert!(matches!(
                    ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap(),
                    Some(ToDaemon::Hello { .. })
                ));
                ipc::write_frame(&mut conn, &response).await.unwrap();
                let Some(ToDaemon::EventsStream { id, .. }) =
                    ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap()
                else {
                    panic!("expected subscription");
                };
                ipc::write_frame(
                    &mut conn,
                    &ToEndpoint::EventFrame {
                        id,
                        frame: ipc::EventWireFrame::Active(crate::events::ActiveNotice {
                            cursor: "eeeeeeee:0".into(),
                            truncated: false,
                            heartbeat_ms: 30_000,
                        }),
                    },
                )
                .await
                .unwrap();
                assert!(
                    ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap()
                        .is_none(),
                    "failed cancellation must close the actual transport"
                );
                ended.send(()).unwrap();
                drop(conn);
                let mut conn = listener.accept().await.unwrap();
                assert!(matches!(
                    ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap(),
                    Some(ToDaemon::Hello { .. })
                ));
                ipc::write_frame(&mut conn, &response).await.unwrap();
                let Some(ToDaemon::EventsList { id }) =
                    ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap()
                else {
                    panic!("expected reconnect list");
                };
                ipc::write_frame(
                    &mut conn,
                    &ToEndpoint::EventFrame {
                        id,
                        frame: ipc::EventWireFrame::ListResult(crate::events::EventsListResult {
                            result_type: "complete".into(),
                            events: Vec::new(),
                        }),
                    },
                )
                .await
                .unwrap();
                assert!(matches!(
                    ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap(),
                    Some(ToDaemon::Bye)
                ));
            });
            let pool = crate::lease::BytePool::new(16 << 10);
            let control = crate::lease::BytePool::new(4 << 10);
            let mut endpoint = Endpoint::for_test(identity, paths);
            endpoint.launch = Arc::new(
                LaunchContext::new(std::env::temp_dir(), Vec::new())
                    .with_memory(Arc::clone(&pool))
                    .with_control_memory(Arc::clone(&control)),
            );
            let (active, stream) = endpoint
                .events_stream(
                    "test",
                    crate::events::SubscribeRequest {
                        name: "external.event".into(),
                        ..Default::default()
                    },
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            drop(active);
            let link = Arc::clone(&stream.link);
            let mut stream = Some(stream);
            let held = if matches!(mode, "budget" | "drop_stream" | "drop_registration") {
                Some((
                    pool.try_acquire(pool.limit() - pool.used()).unwrap(),
                    control
                        .try_acquire(control.limit() - control.used())
                        .unwrap(),
                ))
            } else {
                None
            };
            match mode {
                "budget" => stream.as_mut().unwrap().cancel().await,
                "drop_stream" => drop(stream.take()),
                "drop_registration" => {
                    let stream = stream.take().unwrap();
                    let mut registration = EventRegistration::new(Arc::clone(&link), stream.id);
                    registration.arm_cancel();
                    drop(registration);
                    drop(stream);
                }
                "writer_timeout" | "drop_future" => {
                    let busy = link.writer.lock().await;
                    tokio::time::pause();
                    let mut cancelling = Box::pin(stream.as_mut().unwrap().cancel());
                    tokio::select! {
                        biased;
                        _ = &mut cancelling => panic!("cancel must wait for the writer"),
                        () = std::future::ready(()) => {}
                    }
                    if mode == "drop_future" {
                        drop(cancelling);
                    } else {
                        tokio::time::advance(CANCEL_WAIT + Duration::from_millis(1)).await;
                        cancelling.await;
                    }
                    tokio::time::resume();
                    assert!(link.closed.is_cancelled(), "{mode} must close the link");
                    drop(busy);
                }
                _ => unreachable!(),
            }
            if mode == "budget" {
                assert!(
                    link.closed.is_cancelled(),
                    "budget refusal must close the link"
                );
            }
            timeout(Duration::from_secs(5), closed)
                .await
                .expect("the peer must see EOF while Arc<Link> is still retained")
                .unwrap();
            assert!(link.closed.is_cancelled());
            drop(held);
            let list = timeout(
                Duration::from_secs(5),
                endpoint.events_list("test", CancellationToken::new()),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(list.value.events.is_empty());
            drop(list);
            endpoint.bye().await;
            finish(peer).await;
            drop(stream);
            assert_eq!(pool.used(), 0);
            assert_eq!(control.used(), 0);
            drop(link);
        }
    }

    #[tokio::test]
    async fn event_rpc_budget_errors_preserve_classification_owners_and_link() {
        for receive in [false, true] {
            let (identity, paths, mut listener, _target) =
                test_daemon(&format!("events-budget-{receive}"));
            let response = welcome(&identity);
            let (paused, mut pauses) = mpsc::channel(1);
            let peer = tokio::spawn(async move {
                let mut conn = listener.accept().await.unwrap();
                assert!(matches!(
                    ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap(),
                    Some(ToDaemon::Hello { .. })
                ));
                ipc::write_frame(&mut conn, &response).await.unwrap();
                let mut replies = 0;
                loop {
                    let request = ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap()
                        .unwrap();
                    let (id, frame) = match request {
                        ToDaemon::EventsList { id } => (
                            id,
                            ipc::EventWireFrame::ListResult(crate::events::EventsListResult {
                                result_type: "complete".into(),
                                events: Vec::new(),
                            }),
                        ),
                        ToDaemon::EventsPoll { id, .. } => (
                            id,
                            ipc::EventWireFrame::PollResult(crate::events::PollResponse {
                                result_type: "complete".into(),
                                events: Vec::new(),
                                cursor: "eeeeeeee:0".into(),
                                truncated: false,
                                has_more: false,
                                next_poll_ms: 2000,
                            }),
                        ),
                        ToDaemon::EventsStream { id, .. } => (
                            id,
                            ipc::EventWireFrame::Active(crate::events::ActiveNotice {
                                cursor: "eeeeeeee:0".into(),
                                truncated: false,
                                heartbeat_ms: 30_000,
                            }),
                        ),
                        ToDaemon::EventsCancel { .. } => continue,
                        ToDaemon::Bye => {
                            assert_eq!(replies, if receive { 4 } else { 1 });
                            break;
                        }
                        other => panic!("unexpected event request: {other:?}"),
                    };
                    if receive && replies < 3 {
                        let (resume, ready) = oneshot::channel();
                        paused.send(resume).await.unwrap();
                        ready.await.unwrap();
                    } else {
                        assert_eq!(id, 4, "only the recovery request is admitted");
                    }
                    ipc::write_frame(&mut conn, &ToEndpoint::EventFrame { id, frame })
                        .await
                        .unwrap();
                    replies += 1;
                }
            });
            let pool = crate::lease::BytePool::new(16 << 10);
            let control = crate::lease::BytePool::new(512);
            let mut endpoint = Endpoint::for_test(identity, paths);
            endpoint.launch = Arc::new(
                LaunchContext::new(std::env::temp_dir(), Vec::new())
                    .with_memory(Arc::clone(&pool))
                    .with_control_memory(Arc::clone(&control)),
            );
            let link = endpoint
                .link("test", std::time::Instant::now() + runtime::START_WAIT)
                .await
                .unwrap();
            for method in ["list", "poll", "stream"] {
                let calling = failed_event_rpc(&endpoint, method);
                tokio::pin!(calling);
                let resume = if receive {
                    Some(timeout(Duration::from_secs(5), async {
                        tokio::select! {
                            result = &mut calling => panic!("RPC ended before its response: {result:?}"),
                            resume = pauses.recv() => resume.unwrap(),
                        }
                    }).await.unwrap())
                } else {
                    None
                };
                let full = pool.try_acquire(pool.limit() - pool.used()).unwrap();
                if let Some(resume) = resume {
                    resume.send(()).unwrap();
                }
                let error = timeout(Duration::from_secs(5), &mut calling).await.unwrap();
                assert_eq!(
                    error.value.code, -32025,
                    "{method} receive={receive}: {error:?}"
                );
                assert_eq!(
                    error.value.data,
                    Some(serde_json::json!({"limit": "process_memory", "max": pool.limit()}))
                );
                assert!(error.owner.get::<crate::lease::Lease>().unwrap().bytes() > 0);
                assert!(control.used() > 0);
                assert!(!link.closed.is_cancelled());
                assert!(lock(&link.event_pending).is_empty());
                drop(error);
                drop(full);
            }
            let result = timeout(
                Duration::from_secs(5),
                endpoint.events_list("test", CancellationToken::new()),
            )
            .await
            .unwrap()
            .unwrap();
            assert!(result.value.events.is_empty());
            drop(result);
            endpoint.bye().await;
            finish(peer).await;
            assert_eq!(pool.used(), 0);
            assert_eq!(control.used(), 0);
        }
    }

    #[tokio::test]
    async fn event_rpc_disconnects_keep_internal_error() {
        for method in ["list", "poll", "stream"] {
            let (identity, paths, mut listener, _target) =
                test_daemon(&format!("events-disconnect-{method}"));
            let response = welcome(&identity);
            let peer = tokio::spawn(async move {
                let mut conn = listener.accept().await.unwrap();
                assert!(matches!(
                    ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap(),
                    Some(ToDaemon::Hello { .. })
                ));
                ipc::write_frame(&mut conn, &response).await.unwrap();
                assert!(matches!(
                    ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap(),
                    Some(
                        ToDaemon::EventsList { .. }
                            | ToDaemon::EventsPoll { .. }
                            | ToDaemon::EventsStream { .. }
                    )
                ));
            });
            let endpoint = Endpoint::for_test(identity, paths);
            let error = timeout(Duration::from_secs(5), failed_event_rpc(&endpoint, method))
                .await
                .unwrap();
            assert_eq!(error.value.code, -32603, "{method}: {error:?}");
            assert_eq!(error.value.data, None);
            assert!(error.value.message.contains("connection closed"));
            finish(peer).await;
        }
    }

    #[tokio::test]
    async fn event_rpc_outputs_keep_decoded_leases_after_return() {
        let (identity, paths, mut listener, _target) = test_daemon("events-output-owners");
        let welcome = welcome(&identity);
        let peer = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Hello { .. })
            ));
            ipc::write_frame(&mut conn, &welcome).await.unwrap();
            let mut replies = 0;
            loop {
                let request = ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap()
                    .unwrap();
                let (id, frame) = match request {
                    ToDaemon::EventsList { id } => (
                        id,
                        ipc::EventWireFrame::ListResult(crate::events::EventsListResult {
                            result_type: "complete".into(),
                            events: vec![crate::events::EventTypeDescriptor {
                                name: "external.event".into(),
                                description: "event documentation ".repeat(256).into(),
                                delivery: vec!["push".into(), "poll".into()],
                                input_schema: serde_json::json!({"type": "object"}),
                                payload_schema: serde_json::json!({"type": "object"}),
                            }],
                        }),
                    ),
                    ToDaemon::EventsPoll { id, .. } if replies == 1 => (
                        id,
                        ipc::EventWireFrame::PollResult(crate::events::PollResponse {
                            result_type: "complete".into(),
                            events: vec![crate::events::PolledEvent {
                                event_id: "eeeeeeee-1".to_owned(),
                                name: "external.event".to_owned(),
                                timestamp: "2026-11-22T00:00:00.000Z".to_owned(),
                                data: serde_json::Value::String("x".repeat(4096)),
                            }],
                            cursor: "eeeeeeee:1".to_owned(),
                            truncated: false,
                            has_more: false,
                            next_poll_ms: 2000,
                        }),
                    ),
                    ToDaemon::EventsStream { id, .. } => (
                        id,
                        ipc::EventWireFrame::Active(crate::events::ActiveNotice {
                            cursor: "eeeeeeee:1".to_owned(),
                            truncated: false,
                            heartbeat_ms: 30_000,
                        }),
                    ),
                    ToDaemon::EventsPoll { id, .. } => (
                        id,
                        ipc::EventWireFrame::Error(ipc::IpcEventError {
                            code: -32025,
                            message: "event output is temporarily unavailable".to_owned(),
                            data: Some(serde_json::Value::String("details ".repeat(512))),
                        }),
                    ),
                    ToDaemon::EventsCancel { .. } => continue,
                    ToDaemon::Bye => {
                        assert_eq!(replies, 4);
                        break;
                    }
                    other => panic!("unexpected event request: {other:?}"),
                };
                ipc::write_frame(&mut conn, &ToEndpoint::EventFrame { id, frame })
                    .await
                    .unwrap();
                replies += 1;
            }
        });
        let pool = crate::lease::BytePool::new(256 << 10);
        let mut endpoint = Endpoint::for_test(identity, paths);
        endpoint.launch = Arc::new(
            LaunchContext::new(std::env::temp_dir(), Vec::new())
                .with_memory(Arc::clone(&pool))
                .with_control_memory(crate::lease::BytePool::new(0)),
        );
        let list = endpoint
            .events_list("test", CancellationToken::new())
            .await
            .unwrap();
        let request = crate::events::PollRequest {
            name: "external.event".to_owned(),
            ..Default::default()
        };
        let poll = endpoint
            .events_poll("test", request.clone(), CancellationToken::new())
            .await
            .unwrap();
        let (active, mut stream) = endpoint
            .events_stream(
                "test",
                crate::events::SubscribeRequest {
                    name: "external.event".to_owned(),
                    ..Default::default()
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        stream.cancel().await;
        let error = endpoint
            .events_poll("test", request, CancellationToken::new())
            .await
            .unwrap_err();
        let data_ptr = error
            .value
            .data
            .as_ref()
            .unwrap()
            .as_str()
            .unwrap()
            .as_ptr();
        let outputs = [
            list.map(serde_json::Value::from),
            poll.map(serde_json::Value::from),
            active.map(serde_json::Value::from),
            error.map(crate::mcp::event_error_payload),
        ];
        assert_eq!(
            outputs[3].value["error"]["data"].as_str().unwrap().as_ptr(),
            data_ptr
        );
        let bytes: usize = outputs
            .iter()
            .map(|output| output.owner.get::<crate::lease::Lease>().unwrap().bytes())
            .sum();
        assert!(bytes > 8192);
        endpoint.bye().await;
        finish(peer).await;
        assert_eq!(pool.used(), bytes);
        drop(outputs);
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn stream_overload_terminates_without_reconnecting_and_preserves_link() {
        use rmcp::ServiceExt as _;
        use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};

        let (identity, paths, mut listener, _target) = test_daemon("events-overload-term");
        let response = welcome(&identity);
        let peer = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Hello { .. })
            ));
            ipc::write_frame(&mut conn, &response).await.unwrap();
            let Some(ToDaemon::EventsStream { id, .. }) =
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap()
            else {
                panic!("expected initial subscription");
            };
            ipc::write_frame(
                &mut conn,
                &ToEndpoint::EventFrame {
                    id,
                    frame: ipc::EventWireFrame::Active(crate::events::ActiveNotice {
                        cursor: "ep000001:0".into(),
                        truncated: false,
                        heartbeat_ms: 30_000,
                    }),
                },
            )
            .await
            .unwrap();
            // Send an event frame whose 5x parse peak exceeds the endpoint's 32 KiB main pool.
            ipc::write_frame(
                &mut conn,
                &ToEndpoint::EventFrame {
                    id,
                    frame: ipc::EventWireFrame::Event(crate::events::StreamEvent {
                        event_id: "ev-oversized".into(),
                        name: crate::events::EVENT_EXTERNAL.into(),
                        timestamp: "2026-11-25T00:00:00.000Z".into(),
                        data: serde_json::json!({"pad": "x".repeat(12 * 1024)}),
                        cursor: "ep000001:1".into(),
                    }),
                },
            )
            .await
            .unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT).await.unwrap(),
                Some(ToDaemon::EventsCancel { id: cancelled }) if cancelled == id
            ));
            // Subsequent RPC on the same IPC connection succeeds without reconnecting.
            let Some(ToDaemon::EventsList { id: list_id }) =
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap()
            else {
                panic!("expected EventsList on the preserved IPC connection");
            };
            ipc::write_frame(
                &mut conn,
                &ToEndpoint::EventFrame {
                    id: list_id,
                    frame: ipc::EventWireFrame::ListResult(crate::events::EventsListResult {
                        result_type: "complete".into(),
                        events: Vec::new(),
                    }),
                },
            )
            .await
            .unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Bye)
            ));
        });

        let pool = crate::lease::BytePool::new(32 * 1024);
        let mut ep = Endpoint::for_test(identity, paths);
        ep.launch = Arc::new(
            LaunchContext::new(std::env::temp_dir(), Vec::new())
                .with_memory(Arc::clone(&pool))
                .with_control_memory(crate::lease::BytePool::new(64 * 1024)),
        );
        let endpoint = Arc::new(ep);
        let leases = Arc::new(crate::mcp::RequestLeases::default());
        let server = crate::mcp::Server::new(
            crate::mcp::Backend::Daemon(Arc::clone(&endpoint)),
            &endpoint.launch,
            None,
            crate::config::Tools::default(),
            None,
            crate::config::InstructionsToolMode::Off,
        )
        .with_stdio_leases(Arc::clone(&leases));
        let (client, serving) = tokio::io::duplex(4096);
        let (read, write) = tokio::io::split(serving);
        let transport = crate::cli::stdio::StdioTransport::new(read, write, leases);
        let serving = tokio::spawn(async move {
            let service = server.serve(transport).await.unwrap();
            let _ = service.waiting().await;
        });
        let (read, mut input) = tokio::io::split(client);
        let mut lines = tokio::io::BufReader::new(read).lines();
        let meta = serde_json::json!({
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientInfo": {"name": "test", "version": "1"},
            "io.modelcontextprotocol/clientCapabilities": {},
        });
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "overload-stream",
            "method": "events/stream",
            "params": {
                "name": "external.event",
                "_meta": meta,
            }
        });
        input
            .write_all(format!("{request}\n").as_bytes())
            .await
            .unwrap();
        let active = timeout(Duration::from_secs(10), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&active).unwrap()["method"],
            "notifications/events/active"
        );

        let term_line = timeout(Duration::from_secs(10), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let term_msg: serde_json::Value = serde_json::from_str(&term_line).unwrap();
        assert_eq!(term_msg["method"], "notifications/events/terminated");
        assert_eq!(term_msg["params"]["error"]["code"], -32025);
        assert_eq!(
            term_msg["params"]["error"]["message"],
            "endpoint active memory budget full during event stream"
        );
        assert_eq!(
            term_msg["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"],
            "overload-stream"
        );

        let complete_line = timeout(Duration::from_secs(10), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let complete_msg: serde_json::Value = serde_json::from_str(&complete_line).unwrap();
        assert_eq!(complete_msg["id"], "overload-stream");
        assert_eq!(complete_msg["result"]["resultType"], "complete");

        let list_req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "after-overload",
            "method": "events/list",
            "params": { "_meta": meta }
        });
        input
            .write_all(format!("{list_req}\n").as_bytes())
            .await
            .unwrap();
        let list_line = timeout(Duration::from_secs(10), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let list_msg: serde_json::Value = serde_json::from_str(&list_line).unwrap();
        assert_eq!(list_msg["id"], "after-overload");
        assert_eq!(list_msg["result"]["resultType"], "complete");

        endpoint.bye().await;
        drop(input);
        drop(lines);
        finish(peer).await;
        finish(serving).await;
    }

    #[tokio::test]
    async fn event_rpcs_retry_once_when_stale_connection_fails_before_welcome() {
        for method in ["list", "poll", "stream"] {
            let (identity, paths, mut listener, _target) =
                test_daemon(&format!("evt-retry-{method}"));
            let response = welcome(&identity);
            let peer = tokio::spawn(async move {
                let mut stale = listener.accept().await.unwrap();
                assert!(matches!(
                    ipc::read_frame::<_, ToDaemon>(&mut stale, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap(),
                    Some(ToDaemon::Hello { connection: 1, .. })
                ));
                drop(stale);

                let mut fresh = listener.accept().await.unwrap();
                assert!(matches!(
                    ipc::read_frame::<_, ToDaemon>(&mut fresh, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap(),
                    Some(ToDaemon::Hello { connection: 2, .. })
                ));
                ipc::write_frame(&mut fresh, &response).await.unwrap();
                let request = ipc::read_frame::<_, ToDaemon>(&mut fresh, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap()
                    .unwrap();
                let (id, frame) = match request {
                    ToDaemon::EventsList { id } => (
                        id,
                        ipc::EventWireFrame::ListResult(crate::events::EventsListResult {
                            result_type: "complete".into(),
                            events: Vec::new(),
                        }),
                    ),
                    ToDaemon::EventsPoll { id, .. } => (
                        id,
                        ipc::EventWireFrame::PollResult(crate::events::PollResponse {
                            result_type: "complete".into(),
                            events: Vec::new(),
                            cursor: "ep000001:0".into(),
                            truncated: false,
                            has_more: false,
                            next_poll_ms: 2000,
                        }),
                    ),
                    ToDaemon::EventsStream { id, .. } => (
                        id,
                        ipc::EventWireFrame::Active(crate::events::ActiveNotice {
                            cursor: "ep000001:0".into(),
                            truncated: false,
                            heartbeat_ms: 30_000,
                        }),
                    ),
                    other => panic!("unexpected frame: {other:?}"),
                };
                ipc::write_frame(&mut fresh, &ToEndpoint::EventFrame { id, frame })
                    .await
                    .unwrap();
                loop {
                    match ipc::read_frame::<_, ToDaemon>(&mut fresh, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap()
                        .unwrap()
                    {
                        ToDaemon::EventsCancel { .. } => {}
                        ToDaemon::Bye => break,
                        other => panic!("unexpected trailing frame: {other:?}"),
                    }
                }
            });

            let endpoint = Endpoint::for_test(identity, paths);
            match method {
                "list" => {
                    let res = endpoint
                        .events_list("test", CancellationToken::new())
                        .await
                        .unwrap();
                    assert_eq!(res.value.result_type, "complete");
                }
                "poll" => {
                    let res = endpoint
                        .events_poll(
                            "test",
                            crate::events::PollRequest {
                                name: "external.event".into(),
                                ..Default::default()
                            },
                            CancellationToken::new(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(res.value.result_type, "complete");
                }
                "stream" => {
                    let (active, mut stream) = endpoint
                        .events_stream(
                            "test",
                            crate::events::SubscribeRequest {
                                name: "external.event".into(),
                                ..Default::default()
                            },
                            CancellationToken::new(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(active.value.cursor, "ep000001:0");
                    stream.cancel().await;
                }
                _ => unreachable!(),
            }
            endpoint.bye().await;
            finish(peer).await;
        }
    }

    #[tokio::test]
    async fn cancellation_during_a_handshake_never_dispatches_the_call() {
        let (identity, paths, mut listener, _target) = test_daemon("cancel-hello");
        let (announced, hello_seen) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Hello { .. })
            ));
            announced.send(()).unwrap();
            // No Welcome: cancellation must end the connection without sending a Call.
            assert!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap()
                    .is_none()
            );
        });
        let endpoint = Endpoint::for_test(identity, paths);
        let cancel = CancellationToken::new();
        let calling = endpoint.call(
            "exec",
            serde_json::json!({"action": "list"}),
            "1".into(),
            "test",
            cancel.clone(),
            None,
        );
        tokio::pin!(calling);
        timeout(Duration::from_secs(5), async {
            tokio::select! {
                result = &mut calling => panic!("call ended before the handshake: {result:?}"),
                seen = hello_seen => seen.unwrap(),
            }
            cancel.cancel();
            let result = (&mut calling).await;
            assert_eq!(
                result.content[0].as_text().unwrap().text,
                "Cancelled before the call was sent."
            );
        })
        .await
        .expect("cancellation does not wait for a Welcome");
        assert!(endpoint.link.lock().await.is_none());
        finish(peer).await;
    }

    #[tokio::test]
    async fn cancellation_while_queued_for_the_writer_preserves_the_link() {
        let (identity, paths, mut listener, _target) = test_daemon("cancel-queue");
        let welcome = welcome(&identity);
        let peer = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Hello { .. })
            ));
            ipc::write_frame(&mut conn, &welcome).await.unwrap();
            // The first call was cancelled before sending; only the second may arrive.
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Call { id: 2, .. })
            ));
            ipc::write_frame(
                &mut conn,
                &ToEndpoint::Result {
                    id: 2,
                    result: CallToolResult::success(vec![ContentBlock::text("ok")]),
                },
            )
            .await
            .unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Bye)
            ));
        });
        let endpoint = Arc::new(Endpoint::for_test(identity, paths));
        let link = endpoint
            .link("test", std::time::Instant::now() + runtime::START_WAIT)
            .await
            .unwrap();
        let busy = link.writer.lock().await;
        let cancel = CancellationToken::new();
        let calling = {
            let (endpoint, cancel) = (Arc::clone(&endpoint), cancel.clone());
            tokio::spawn(async move {
                endpoint
                    .call(
                        "exec",
                        serde_json::json!({"action": "list"}),
                        "1".into(),
                        "test",
                        cancel,
                        None,
                    )
                    .await
            })
        };
        timeout(Duration::from_secs(5), async {
            while !lock(&link.pending).contains_key(&1) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the call queued for the writer");
        cancel.cancel();
        let result = timeout(Duration::from_secs(5), calling)
            .await
            .expect("the queued cancellation ended")
            .unwrap();
        assert_eq!(
            result.content[0].as_text().unwrap().text,
            "Cancelled before the call was sent."
        );
        assert!(!link.closed.is_cancelled());
        assert!(lock(&link.pending).is_empty());
        drop(busy);
        let result = timeout(
            Duration::from_secs(5),
            endpoint.call(
                "exec",
                serde_json::json!({"action": "list"}),
                "2".into(),
                "test",
                CancellationToken::new(),
                None,
            ),
        )
        .await
        .expect("the existing link still works");
        assert_eq!(result.content[0].as_text().unwrap().text, "ok");
        endpoint.bye().await;
        drop(link);
        drop(endpoint);
        finish(peer).await;
    }

    #[tokio::test]
    async fn cancellation_during_a_frame_write_closes_then_reconnects() {
        interrupted_frame_reconnects(false).await;
    }

    #[tokio::test]
    async fn dropping_the_call_during_a_frame_write_closes_then_reconnects() {
        interrupted_frame_reconnects(true).await;
    }

    async fn interrupted_frame_reconnects(drop_call: bool) {
        use tokio::io::AsyncReadExt;

        const PARTIAL_BYTES: usize = 5;
        let (identity, paths, mut listener, _target) = test_daemon(if drop_call {
            "drop-frame"
        } else {
            "cancel-frame"
        });
        let welcome = welcome(&identity);
        let (announced, writing) = oneshot::channel();
        let (paused, blocked) = oneshot::channel();
        let (release, drain) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut first = listener.accept().await.unwrap();
            let _ = ipc::read_frame::<_, ToDaemon>(&mut first, ipc::REQUEST_LIMIT)
                .await
                .unwrap();
            ipc::write_frame(&mut first, &welcome).await.unwrap();
            let mut bytes = vec![0_u8; PARTIAL_BYTES];
            first.read_exact(&mut bytes).await.unwrap();
            announced.send(()).unwrap();
            drain.await.unwrap();
            first.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(bytes.len(), PARTIAL_BYTES);
            let declared = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
            assert!(
                bytes.len() < declared + 4,
                "the fixture must leave a partial frame"
            );
            drop(first);

            let mut second = listener.accept().await.unwrap();
            let _ = ipc::read_frame::<_, ToDaemon>(&mut second, ipc::REQUEST_LIMIT)
                .await
                .unwrap();
            ipc::write_frame(&mut second, &welcome).await.unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut second, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Call { id: 2, .. })
            ));
            ipc::write_frame(
                &mut second,
                &ToEndpoint::Result {
                    id: 2,
                    result: CallToolResult::success(vec![ContentBlock::text("ok")]),
                },
            )
            .await
            .unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut second, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Bye)
            ));
        });
        let endpoint = Endpoint::for_test(identity, paths);
        let link = endpoint
            .link("test", std::time::Instant::now() + runtime::START_WAIT)
            .await
            .unwrap();
        link.writer.lock().await.as_mut().unwrap().pause = Some(WritePause {
            remaining: PARTIAL_BYTES,
            blocked: Some(paused),
        });
        let cancel = CancellationToken::new();
        let mut calling = Box::pin(endpoint.call(
            "write",
            serde_json::json!({"path": "unused", "content": "partial"}),
            "1".into(),
            "test",
            cancel.clone(),
            None,
        ));
        timeout(Duration::from_secs(5), async {
            tokio::select! {
                result = &mut calling => panic!("call ended before writing: {result:?}"),
                () = async {
                    blocked.await.unwrap();
                    writing.await.unwrap();
                } => {},
            }
            if drop_call {
                drop(calling);
                return;
            }
            cancel.cancel();
            let result = calling.await;
            assert!(
                result.content[0]
                    .as_text()
                    .unwrap()
                    .text
                    .contains("the call may or may not have run")
            );
        })
        .await
        .expect("cancellation does not wait for a blocked frame write");
        assert!(link.closed.is_cancelled());
        assert!(lock(&link.pending).is_empty());
        drop(link);
        release.send(()).unwrap();
        let result = timeout(
            Duration::from_secs(5),
            endpoint.call(
                "exec",
                serde_json::json!({"action": "list"}),
                "2".into(),
                "test",
                CancellationToken::new(),
                None,
            ),
        )
        .await
        .expect("a later call reconnects");
        assert_eq!(result.content[0].as_text().unwrap().text, "ok");
        endpoint.bye().await;
        finish(peer).await;
    }

    #[tokio::test]
    async fn unsent_call_and_cancel_timeouts_leave_an_unrelated_call_on_the_link() {
        let (identity, paths, mut listener, _target) = test_daemon("writer-deadline");
        let welcome = welcome(&identity);
        let (announced, seen) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            let _ = ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                .await
                .unwrap();
            ipc::write_frame(&mut conn, &welcome).await.unwrap();
            for expected in [1, 2] {
                assert!(matches!(
                    ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT).await.unwrap(),
                    Some(ToDaemon::Call { id, .. }) if id == expected
                ));
            }
            announced.send(()).unwrap();
            wait.await.unwrap();
            ipc::write_frame(
                &mut conn,
                &ToEndpoint::Result {
                    id: 2,
                    result: CallToolResult::success(vec![ContentBlock::text("still connected")]),
                },
            )
            .await
            .unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Bye)
            ));
        });

        let endpoint = Endpoint::for_test(identity, paths);
        let link = endpoint
            .link("test", std::time::Instant::now() + runtime::START_WAIT)
            .await
            .unwrap();
        let make_call = |id: u64, cancel| {
            endpoint.call(
                "exec",
                serde_json::json!({"action":"list"}),
                id.to_string(),
                "test",
                cancel,
                None,
            )
        };
        let cancel = CancellationToken::new();
        let mut first = std::pin::pin!(make_call(1, cancel.clone()));
        let mut other = std::pin::pin!(make_call(2, CancellationToken::new()));
        timeout(Duration::from_secs(5), async {
            tokio::select! {
                biased;
                result = &mut first => panic!("first call ended early: {result:?}"),
                result = &mut other => panic!("second call ended early: {result:?}"),
                seen = seen => seen.unwrap(),
            }
        })
        .await
        .expect("both complete calls reached the peer");
        let busy = link.writer.lock().await;
        cancel.cancel();
        let mut queued = std::pin::pin!(make_call(3, CancellationToken::new()));
        tokio::time::pause();
        tokio::select! {
            biased;
            _ = &mut first => panic!("Cancel did not queue"),
            _ = &mut queued => panic!("Call did not queue"),
            () = std::future::ready(()) => {}
        }
        assert!(lock(&link.pending).contains_key(&3));
        tokio::time::advance(WRITE_WAIT + Duration::from_millis(1)).await;
        let cancelled = (&mut first).await;
        let unsent = (&mut queued).await;
        tokio::time::resume();
        assert_eq!(
            cancelled.content[0].as_text().unwrap().text,
            "The call was sent, but Cancel timed out before it was sent. The call's result is unknown."
        );
        assert_eq!(
            unsent.content[0].as_text().unwrap().text,
            "Timed out waiting for the daemon writer before the call was sent."
        );
        assert!(!link.closed.is_cancelled());
        assert_eq!(lock(&link.pending).keys().copied().collect::<Vec<_>>(), [2]);
        drop(busy);
        release.send(()).unwrap();
        let result = timeout(Duration::from_secs(5), other).await.unwrap();
        assert_eq!(result.content[0].as_text().unwrap().text, "still connected");
        endpoint.bye().await;
        finish(peer).await;
    }

    #[tokio::test]
    async fn cancel_budget_rejection_keeps_the_connection_available() {
        let (identity, paths, mut listener, _target) = test_daemon("cancel-budget");
        let welcome = welcome(&identity);
        let (announced, seen) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            let _ = ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                .await
                .unwrap();
            ipc::write_frame(&mut conn, &welcome).await.unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Call { id: 1, .. })
            ));
            announced.send(()).unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Call { id: 2, .. })
            ));
            ipc::write_frame(
                &mut conn,
                &ToEndpoint::Result {
                    id: 2,
                    result: CallToolResult::success(vec![ContentBlock::text("still connected")]),
                },
            )
            .await
            .unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Bye)
            ));
        });
        let pool = crate::lease::BytePool::new(16 * 1024);
        let control = crate::lease::BytePool::new(4 * 1024);
        let mut endpoint = Endpoint::for_test(identity, paths);
        endpoint.launch = Arc::new(
            LaunchContext::new(std::env::temp_dir(), Vec::new())
                .with_memory(Arc::clone(&pool))
                .with_control_memory(Arc::clone(&control)),
        );
        let cancel = CancellationToken::new();
        let mut first = std::pin::pin!(endpoint.call(
            "exec",
            serde_json::json!({"action": "list"}),
            "1".into(),
            "test",
            cancel.clone(),
            None,
        ));
        timeout(Duration::from_secs(5), async {
            tokio::select! {
                biased;
                result = &mut first => panic!("call ended before cancellation: {result:?}"),
                seen = seen => seen.unwrap(),
            }
        })
        .await
        .unwrap();
        let link = endpoint.link.lock().await.clone().unwrap();
        let full = pool.try_acquire(pool.limit() - pool.used()).unwrap();
        let full_control = control
            .try_acquire(control.limit() - control.used())
            .unwrap();
        cancel.cancel();
        let result = timeout(Duration::from_secs(5), &mut first).await.unwrap();
        let message = &result.content[0].as_text().unwrap().text;
        assert!(message.contains("Cancel could not be sent"), "{message}");
        assert!(message.contains("active memory budget"), "{message}");
        assert!(message.contains("result is unknown"), "{message}");
        assert!(!link.closed.is_cancelled());
        assert!(lock(&link.pending).is_empty());
        drop(full);
        drop(full_control);
        let result = timeout(
            Duration::from_secs(5),
            endpoint.call(
                "exec",
                serde_json::json!({"action": "list"}),
                "2".into(),
                "test",
                CancellationToken::new(),
                None,
            ),
        )
        .await
        .unwrap();
        assert_eq!(result.content[0].as_text().unwrap().text, "still connected");
        endpoint.bye().await;
        finish(peer).await;
    }

    /// The endpoint's own boundary: a cancelled call writes `Cancel` for its ID to the daemon,
    /// stops waiting, and leaves nothing pending.
    #[tokio::test]
    async fn a_cancel_reaches_the_daemon_and_ends_the_wait() {
        let (identity, paths, mut listener, _target) = test_daemon("cancel");
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = Arc::clone(&seen);
        let welcome = welcome(&identity);
        let peer = tokio::spawn(async move {
            let Ok(mut conn) = listener.accept().await else {
                return;
            };
            let Ok(Some(ToDaemon::Hello { .. })) =
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT).await
            else {
                return;
            };
            if ipc::write_frame(&mut conn, &welcome).await.is_err() {
                return;
            }
            while let Ok(Some(frame)) =
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT).await
            {
                let seen = match frame {
                    ToDaemon::Call { id, .. } => format!("call {id}"),
                    ToDaemon::Cancel { id } => format!("cancel {id}"),
                    ToDaemon::Bye => break,
                    _ => continue,
                };
                observed.lock().unwrap().push(seen);
            }
        });

        let endpoint = Arc::new(Endpoint::for_test(identity, paths));
        let cancel = CancellationToken::new();
        let calling = {
            let endpoint = Arc::clone(&endpoint);
            let cancel = cancel.clone();
            tokio::spawn(async move {
                endpoint
                    .call(
                        "exec",
                        serde_json::json!({"action": "list"}),
                        "1".to_owned(),
                        "test",
                        cancel,
                        None,
                    )
                    .await
            })
        };
        // The call reaches the daemon, which never answers it.
        let mut waited = 0;
        while seen.lock().unwrap().is_empty() && waited < 500 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            waited += 1;
        }
        assert_eq!(*seen.lock().unwrap(), vec!["call 1".to_owned()]);

        cancel.cancel();
        let result = timeout(Duration::from_secs(5), calling)
            .await
            .expect("the cancelled call ended")
            .unwrap();
        assert_eq!(
            result.content[0].as_text().map(|text| text.text.clone()),
            Some("Cancelled; the task keeps running.".to_owned())
        );

        let mut waited = 0;
        while seen.lock().unwrap().len() < 2 && waited < 500 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            waited += 1;
        }
        // The cancelled call left nothing behind: its ID no longer waits for an answer.
        let link = endpoint.link.lock().await.clone().expect("a live link");
        assert!(
            lock(&link.pending).is_empty(),
            "the cancelled call is still pending"
        );

        endpoint.bye().await;
        drop(endpoint);
        finish(peer).await;
        // Every frame is in: the call, then its Cancel, and nothing else.
        assert_eq!(
            *seen.lock().unwrap(),
            vec!["call 1".to_owned(), "cancel 1".to_owned()]
        );
    }

    /// A connection that ends or is refused before its Welcome is retried once, under the same
    /// deadline: the call is not replayed, and it keeps the ID it was given.
    #[tokio::test]
    async fn a_connection_that_ends_before_a_welcome_is_retried_once() {
        let (identity, paths, mut listener, _target) = test_daemon("retry");
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = Arc::clone(&seen);
        let welcome = welcome(&identity);
        let peer = tokio::spawn(async move {
            // The first connection is refused before any Welcome.
            let Ok(mut first) = listener.accept().await else {
                return;
            };
            if let Ok(Some(ToDaemon::Hello { connection, .. })) =
                ipc::read_frame::<_, ToDaemon>(&mut first, ipc::REQUEST_LIMIT).await
            {
                observed.lock().unwrap().push(format!("hello {connection}"));
            }
            let reject = ToEndpoint::Reject {
                message: "not yet".to_owned(),
            };
            let _ = ipc::write_frame(&mut first, &reject).await;
            drop(first);

            // The second one is answered, and the call arrives on it.
            let Ok(mut second) = listener.accept().await else {
                return;
            };
            if let Ok(Some(ToDaemon::Hello { connection, .. })) =
                ipc::read_frame::<_, ToDaemon>(&mut second, ipc::REQUEST_LIMIT).await
            {
                observed.lock().unwrap().push(format!("hello {connection}"));
            }
            if ipc::write_frame(&mut second, &welcome).await.is_err() {
                return;
            }
            while let Ok(Some(frame)) =
                ipc::read_frame::<_, ToDaemon>(&mut second, ipc::REQUEST_LIMIT).await
            {
                match frame {
                    ToDaemon::Call { id, .. } => {
                        observed.lock().unwrap().push(format!("call {id}"));
                        let answer = ToEndpoint::Result {
                            id,
                            result: CallToolResult::success(vec![ContentBlock::text("ok")]),
                        };
                        if ipc::write_frame(&mut second, &answer).await.is_err() {
                            return;
                        }
                    }
                    ToDaemon::Bye => break,
                    _ => {}
                }
            }
        });

        let endpoint = Arc::new(Endpoint::for_test(identity, paths));
        let result = timeout(
            Duration::from_secs(30),
            endpoint.call(
                "exec",
                serde_json::json!({"action": "list"}),
                "1".to_owned(),
                "test",
                CancellationToken::new(),
                None,
            ),
        )
        .await
        .expect("the call finished");
        assert_eq!(
            result.content[0].as_text().map(|text| text.text.clone()),
            Some("ok".to_owned()),
            "the retried connection did not answer the call"
        );
        endpoint.bye().await;
        drop(endpoint);
        finish(peer).await;
        // Two Hellos from one endpoint, the second on a newer connection, and one Call: the
        // retry sent nothing that was already sent.
        assert_eq!(
            *seen.lock().unwrap(),
            vec![
                "hello 1".to_owned(),
                "hello 2".to_owned(),
                "call 1".to_owned()
            ]
        );
    }

    #[tokio::test]
    async fn a_complete_result_survives_the_connection_ending() {
        let (identity, paths, mut listener, _target) = test_daemon("last-result");
        let welcome = welcome(&identity);
        let (seen, request_arrived) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            let hello = ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                .await
                .unwrap();
            assert!(matches!(hello, Some(ToDaemon::Hello { .. })));
            ipc::write_frame(&mut conn, &welcome).await.unwrap();
            let Some(ToDaemon::Call { id, .. }) = ipc::read_frame(&mut conn, ipc::REQUEST_LIMIT)
                .await
                .unwrap()
            else {
                panic!("expected a call");
            };
            seen.send(()).unwrap();
            let result = CallToolResult::success(vec![ContentBlock::text("confirmed")]);
            ipc::write_frame(&mut conn, &ToEndpoint::Result { id, result })
                .await
                .unwrap();
        });

        let endpoint = Endpoint::for_test(identity, paths);
        let link = endpoint
            .link("test", std::time::Instant::now() + runtime::START_WAIT)
            .await
            .unwrap();
        let call = endpoint.call(
            "exec",
            serde_json::json!({"action": "list"}),
            "1".to_owned(),
            "test",
            CancellationToken::new(),
            None,
        );
        tokio::pin!(call);
        // Only resume the caller after the reader has processed both the Result and EOF.
        timeout(Duration::from_secs(10), async {
            tokio::select! {
                biased;
                seen = request_arrived => seen.unwrap(),
                _ = &mut call => panic!("the call ended before the peer received it"),
            }
        })
        .await
        .expect("the daemon received the call");
        timeout(Duration::from_secs(10), link.closed.cancelled())
            .await
            .unwrap();
        let result = timeout(Duration::from_secs(10), call).await.unwrap();
        assert_eq!(result.content[0].as_text().unwrap().text, "confirmed");
        finish(peer).await;
    }

    /// A call queued behind a busy writer must let go of a link that has ended, rather than
    /// write a fresh frame onto a stream that may hold half of one.
    #[tokio::test]
    async fn a_queued_write_does_not_reach_a_link_that_has_ended() {
        let (identity, paths, mut listener, _target) = test_daemon("queued");
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = Arc::clone(&seen);
        let welcome = welcome(&identity);
        let peer = tokio::spawn(async move {
            let Ok(mut conn) = listener.accept().await else {
                return;
            };
            let Ok(Some(ToDaemon::Hello { .. })) =
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT).await
            else {
                return;
            };
            if ipc::write_frame(&mut conn, &welcome).await.is_err() {
                return;
            }
            while let Ok(Some(frame)) =
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT).await
            {
                match frame {
                    ToDaemon::Call { id, .. } => {
                        observed.lock().unwrap().push(format!("call {id}"))
                    }
                    ToDaemon::Bye => break,
                    _ => {}
                }
            }
        });

        let endpoint = Arc::new(Endpoint::for_test(identity, paths));
        let link = endpoint
            .link("test", std::time::Instant::now() + runtime::START_WAIT)
            .await
            .expect("a link");
        // The test holds the writer, so the call below queues behind it.
        let busy = link.writer.lock().await;
        let calling = {
            let endpoint = Arc::clone(&endpoint);
            tokio::spawn(async move {
                endpoint
                    .call(
                        "exec",
                        serde_json::json!({"action": "list"}),
                        "1".to_owned(),
                        "test",
                        CancellationToken::new(),
                        None,
                    )
                    .await
            })
        };
        // The call is on this link, queued behind the test's writer, before the link ends.
        let mut waited = 0;
        while lock(&link.pending).is_empty() && waited < 500 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            waited += 1;
        }
        assert_eq!(
            lock(&link.pending).keys().copied().collect::<Vec<u64>>(),
            vec![1],
            "the call is not queued on this link"
        );
        link.closed.cancel();
        let result = timeout(Duration::from_secs(5), calling)
            .await
            .expect("the queued call ended")
            .unwrap();
        assert_eq!(
            result.content[0].as_text().map(|text| text.text.clone()),
            Some("The daemon connection closed before the call was sent.".into())
        );
        drop(busy);
        // Nothing was written onto the ended link.
        assert!(
            seen.lock().unwrap().is_empty(),
            "{:?}",
            seen.lock().unwrap()
        );
        assert!(lock(&link.pending).is_empty(), "the call is still pending");

        // Dropping the last reference closes the connection, which ends the peer.
        drop(link);
        drop(endpoint);
        finish(peer).await;
    }

    /// A file tool's lost call names the tool and says only that it may or may not have run.
    #[tokio::test]
    async fn a_lost_file_tool_call_names_its_tool_and_no_action_list() {
        let (identity, paths, mut listener, _target) = test_daemon("lost-files");
        let welcome = welcome(&identity);
        let (announced, seen) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let Ok(mut conn) = listener.accept().await else {
                return;
            };
            let Ok(Some(ToDaemon::Hello { .. })) =
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT).await
            else {
                return;
            };
            if ipc::write_frame(&mut conn, &welcome).await.is_err() {
                return;
            }
            // The daemon reads the call and never answers it, so the link is lost under it.
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Call { id: 1, .. })
            ));
            announced.send(()).unwrap();
            while let Ok(Some(frame)) =
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT).await
            {
                if matches!(frame, ToDaemon::Bye) {
                    break;
                }
            }
        });

        let endpoint = Arc::new(Endpoint::for_test(identity, paths));
        let link = endpoint
            .link("test", std::time::Instant::now() + runtime::START_WAIT)
            .await
            .expect("a link");
        let calling = {
            let endpoint = Arc::clone(&endpoint);
            tokio::spawn(async move {
                endpoint
                    .call(
                        "read",
                        serde_json::json!({"path": "notes.txt"}),
                        "1".to_owned(),
                        "test",
                        CancellationToken::new(),
                        None,
                    )
                    .await
            })
        };
        timeout(Duration::from_secs(5), seen)
            .await
            .unwrap()
            .unwrap();
        link.closed.cancel();
        let result = timeout(Duration::from_secs(5), calling)
            .await
            .expect("the queued call ended")
            .unwrap();
        let text = result.content[0]
            .as_text()
            .map(|text| text.text.clone())
            .unwrap_or_default();
        assert_eq!(
            text,
            format!(
                "The connection to the fastexec daemon (epoch {}) closed during read call 1 (request 1, session {}); the call may or may not have run.",
                TEST_EPOCH, endpoint.session
            )
        );
        assert!(!text.contains("action list"), "{text}");

        // Dropping the last reference closes the connection, which ends the peer.
        drop(link);
        drop(endpoint);
        finish(peer).await;
    }

    #[tokio::test]
    async fn link_handshake_and_overload_event_stay_isolated_to_endpoint_pools() {
        // 1. `Endpoint::link` handshake must charge `self.memory()` / `self.control_memory()`,
        //    never falling back to global `process_pool()` / `control_pool()`.
        let (identity, paths, mut listener, _target) = test_daemon("isolated-handshake");
        let response = welcome(&identity);
        let (ready_for_oversized_tx, ready_for_oversized_rx) = oneshot::channel();
        let (cancel_notices_tx, mut cancel_notices_rx) = mpsc::channel(2);
        let peer = tokio::spawn(async move {
            let mut ready_for_oversized_rx = Some(ready_for_oversized_rx);
            // Attempt 1: `hello_owned` succeeds, but encoding `Hello` runs out of isolated pools.
            let mut first = listener.accept().await.unwrap();
            assert!(
                ipc::read_frame::<_, ToDaemon>(&mut first, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap()
                    .is_none(),
                "Hello encoding must fail against the endpoint pools before writing bytes"
            );
            drop(first);

            // Attempt 2: `Hello` encodes and arrives, but reading `Welcome` exceeds isolated pools.
            let mut second = listener.accept().await.unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut second, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Hello { .. })
            ));
            let mut large_welcome = response.clone();
            if let ToEndpoint::Welcome { epoch, .. } = &mut large_welcome {
                *epoch = "e".repeat(2048);
            }
            ipc::write_frame(&mut second, &large_welcome).await.unwrap();
            drop(second);

            // Attempt 3: `Link` connects cleanly; then an oversized `EventFrame` triggers
            // `OverloadEvent`. When the isolated `control` pool is full, it must NOT pierce into
            // `lease::control_pool()` to synthesize a `Terminated` frame; when `control` has room,
            // the `Terminated` lease comes strictly from `control`.
            let mut third = listener.accept().await.unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut third, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Hello { .. })
            ));
            ipc::write_frame(&mut third, &response).await.unwrap();
            for expected_streams in 0..2 {
                let Some(ToDaemon::EventsStream { id, .. }) =
                    ipc::read_frame::<_, ToDaemon>(&mut third, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap()
                else {
                    panic!("expected subscription {expected_streams}");
                };
                ipc::write_frame(
                    &mut third,
                    &ToEndpoint::EventFrame {
                        id,
                        frame: ipc::EventWireFrame::Active(crate::events::ActiveNotice {
                            cursor: "ep000001:0".into(),
                            truncated: false,
                            heartbeat_ms: 30_000,
                        }),
                    },
                )
                .await
                .unwrap();
                if let Some(rx) = ready_for_oversized_rx.take() {
                    rx.await.unwrap();
                }
                ipc::write_frame(
                    &mut third,
                    &ToEndpoint::EventFrame {
                        id,
                        frame: ipc::EventWireFrame::Event(crate::events::StreamEvent {
                            event_id: format!("ev-{expected_streams}"),
                            name: crate::events::EVENT_EXTERNAL.into(),
                            timestamp: "2026-11-26T00:00:00.000Z".into(),
                            data: serde_json::json!({"pad": "x".repeat(72 * 1024)}),
                            cursor: "ep000001:1".into(),
                        }),
                    },
                )
                .await
                .unwrap();
                assert!(matches!(
                    ipc::read_frame::<_, ToDaemon>(&mut third, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap(),
                    Some(ToDaemon::EventsCancel { id: cancelled }) if cancelled == id
                ));
                cancel_notices_tx.send(()).await.unwrap();
            }
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut third, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Bye)
            ));
        });

        let base_launch = LaunchContext::new(std::env::temp_dir(), Vec::new());
        let session = "s".repeat(26);
        let copied = ipc::launch_wire_capacity(&base_launch)
            + identity.namespace.len()
            + identity.build.len()
            + session.len()
            + "test".len();

        // Attempt 1: exact `copied` bytes fit `hello_owned`, leaving 0 bytes for `encode_frame_leased`.
        let mut endpoint = Endpoint::for_test(identity.clone(), paths.clone());
        let tight_encode_pool = crate::lease::BytePool::new(copied);
        endpoint.launch = Arc::new(
            base_launch
                .for_session()
                .with_memory(Arc::clone(&tight_encode_pool))
                .with_control_memory(crate::lease::BytePool::new(0)),
        );
        let err = endpoint
            .link("test", std::time::Instant::now() + runtime::START_WAIT)
            .await
            .err()
            .expect("Hello encode must fail when isolated pools have no room");
        assert!(matches!(err, SetupError::MemoryBudget), "{err:?}");
        assert_eq!(tight_encode_pool.used(), 0);

        // Attempt 2: enough bytes to construct and encode `Hello`, not enough to decode the 2 KiB `Welcome`.
        let sample_hello = hello_owned(&base_launch, &identity, &session, 1, "test").unwrap();
        let hello_wire = ipc::frame_len(&sample_hello.value).unwrap();
        drop(sample_hello);
        let tight_welcome_pool = crate::lease::BytePool::new(copied + hello_wire);
        endpoint.launch = Arc::new(
            base_launch
                .for_session()
                .with_memory(Arc::clone(&tight_welcome_pool))
                .with_control_memory(crate::lease::BytePool::new(0)),
        );
        let err = endpoint
            .link("test", std::time::Instant::now() + runtime::START_WAIT)
            .await
            .err()
            .expect("Welcome decode must fail when isolated pools cannot fit parse peak");
        assert!(matches!(err, SetupError::MemoryBudget), "{err:?}");
        assert_eq!(tight_welcome_pool.used(), 0);

        // Attempt 3: 32 KiB main pool + 4 KiB control pool.
        let main_pool = crate::lease::BytePool::new(32 * 1024);
        let control_pool = crate::lease::BytePool::new(4 * 1024);
        endpoint.launch = Arc::new(
            base_launch
                .with_memory(Arc::clone(&main_pool))
                .with_control_memory(Arc::clone(&control_pool)),
        );

        // 3a. Exhaust `control_pool` before the oversized (> 64 KiB) event frame arrives:
        //     `OverloadEvent` must drop `tx` cleanly (`stream.next() == None`) rather than
        //     acquiring from the global `lease::control_pool()`.
        let (active, mut stream1) = endpoint
            .events_stream(
                "test",
                crate::events::SubscribeRequest {
                    name: crate::events::EVENT_EXTERNAL.into(),
                    ..Default::default()
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        drop(active);
        let saturate_ctrl = control_pool
            .try_acquire(control_pool.limit() - control_pool.used())
            .unwrap();
        ready_for_oversized_tx.send(()).unwrap();
        timeout(Duration::from_secs(5), cancel_notices_rx.recv())
            .await
            .unwrap()
            .expect("OOM cancels upstream before the consumer reads");
        let next1 = timeout(Duration::from_secs(5), stream1.next())
            .await
            .unwrap();
        assert!(
            next1.is_none(),
            "OverloadEvent must not fall back to global control_pool when endpoint control_pool is full: {next1:?}"
        );
        assert!(!stream1.overflowed());
        assert!(!stream1.link_closed());
        drop(saturate_ctrl);
        stream1.cancel().await;

        // 3b. With `control_pool` available, `OverloadEvent`'s `Terminated` lease comes from `control_pool`.
        assert_eq!(control_pool.used(), 0);
        let (active2, mut stream2) = endpoint
            .events_stream(
                "test",
                crate::events::SubscribeRequest {
                    name: crate::events::EVENT_EXTERNAL.into(),
                    ..Default::default()
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        drop(active2);
        timeout(Duration::from_secs(5), cancel_notices_rx.recv())
            .await
            .unwrap()
            .expect("OOM cancels upstream before the consumer reads");
        let (term_frame, term_lease) = timeout(Duration::from_secs(5), stream2.next())
            .await
            .unwrap()
            .expect("Terminated frame fits in isolated control_pool");
        assert!(matches!(term_frame, ipc::EventWireFrame::Terminated(_)));
        assert_eq!(control_pool.used(), term_lease.bytes());
        drop(term_frame);
        drop(term_lease);
        assert_eq!(control_pool.used(), 0);
        assert!(
            timeout(Duration::from_secs(5), stream2.next())
                .await
                .unwrap()
                .is_none()
        );
        stream2.cancel().await;

        endpoint.bye().await;
        finish(peer).await;
    }

    #[derive(Default)]
    pub(super) struct OverflowHook {
        pub(super) arrived: tokio::sync::Notify,
        pub(super) resume: tokio::sync::Notify,
        pub(super) done: tokio::sync::Notify,
    }

    #[tokio::test]
    async fn cancelling_a_stream_owns_the_route_before_a_stale_full_send_finishes() {
        for explicit in [true, false] {
            let (identity, paths, mut listener, _target) = test_daemon(if explicit {
                "overflow-cancel"
            } else {
                "overflow-drop"
            });
            let response = welcome(&identity);
            let (start_tx, start_rx) = oneshot::channel();
            let (cancel_tx, cancel_rx) = oneshot::channel();
            let peer = tokio::spawn(async move {
                let mut conn = listener.accept().await.unwrap();
                assert!(matches!(
                    ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap(),
                    Some(ToDaemon::Hello { .. })
                ));
                ipc::write_frame(&mut conn, &response).await.unwrap();
                let Some(ToDaemon::EventsStream { id, .. }) =
                    ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap()
                else {
                    panic!("expected subscription")
                };
                ipc::write_frame(
                    &mut conn,
                    &ToEndpoint::EventFrame {
                        id,
                        frame: ipc::EventWireFrame::Active(crate::events::ActiveNotice {
                            cursor: "ep000001:0".into(),
                            truncated: false,
                            heartbeat_ms: 30_000,
                        }),
                    },
                )
                .await
                .unwrap();
                start_rx.await.unwrap();
                for _ in 0..=EVENT_STREAM_CHANNEL_CAPACITY {
                    ipc::write_frame(
                        &mut conn,
                        &ToEndpoint::EventFrame {
                            id,
                            frame: ipc::EventWireFrame::Heartbeat {
                                cursor: "ep000001:0".into(),
                            },
                        },
                    )
                    .await
                    .unwrap();
                }
                assert!(matches!(
                    ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT).await.unwrap(),
                    Some(ToDaemon::EventsCancel { id: cancelled }) if cancelled == id
                ));
                cancel_tx.send(()).unwrap();
                assert!(matches!(
                    ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap(),
                    Some(ToDaemon::Bye)
                ));
            });
            let endpoint = Endpoint::for_test(identity, paths);
            let (active, mut stream) = endpoint
                .events_stream(
                    "test",
                    crate::events::SubscribeRequest {
                        name: crate::events::EVENT_EXTERNAL.into(),
                        ..Default::default()
                    },
                    CancellationToken::new(),
                )
                .await
                .unwrap();
            drop(active);
            let link = Arc::clone(&stream.link);
            let overflowed = Arc::clone(&stream.overflowed);
            let hook = Arc::new(OverflowHook::default());
            *lock(&link.overflow_hook) = Some(Arc::clone(&hook));
            start_tx.send(()).unwrap();
            timeout(Duration::from_secs(5), hook.arrived.notified())
                .await
                .unwrap();
            if explicit {
                stream.cancel().await;
            }
            drop(stream);
            timeout(Duration::from_secs(5), cancel_rx)
                .await
                .unwrap()
                .unwrap();
            hook.resume.notify_one();
            timeout(Duration::from_secs(5), hook.done.notified())
                .await
                .unwrap();
            assert!(!overflowed.load(Ordering::Acquire));
            assert!(!link.closed.is_cancelled());
            endpoint.bye().await;
            finish(peer).await;
        }
    }

    #[tokio::test]
    async fn event_stream_channel_overflow_immediately_cancels_and_reports_slow_consumer() {
        use rmcp::ServiceExt as _;
        use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};

        // Part 1: Direct `Endpoint::events_stream` 128-slot saturation immediately sends `EventsCancel`
        //         and sets `stream.overflowed()` before the slow consumer drains `rx`.
        let (identity, paths, mut listener, _target) = test_daemon("evt-chan-overflow");
        let response = welcome(&identity);
        let (filled_128_tx, filled_128_rx) = oneshot::channel();
        let (proceed_129_tx, proceed_129_rx) = oneshot::channel();
        let (cancelled_tx, cancelled_rx) = oneshot::channel();
        let (mcp_active_read_tx, mcp_active_read_rx) = oneshot::channel();
        let (mcp_cancelled_tx, mcp_cancelled_rx) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut conn = listener.accept().await.unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Hello { .. })
            ));
            ipc::write_frame(&mut conn, &response).await.unwrap();
            let Some(ToDaemon::EventsStream { id, .. }) =
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap()
            else {
                panic!("expected first subscription");
            };
            ipc::write_frame(
                &mut conn,
                &ToEndpoint::EventFrame {
                    id,
                    frame: ipc::EventWireFrame::Active(crate::events::ActiveNotice {
                        cursor: "ep000001:0".into(),
                        truncated: false,
                        heartbeat_ms: 30_000,
                    }),
                },
            )
            .await
            .unwrap();
            for seq in 0..EVENT_STREAM_CHANNEL_CAPACITY {
                ipc::write_frame(
                    &mut conn,
                    &ToEndpoint::EventFrame {
                        id,
                        frame: ipc::EventWireFrame::Event(crate::events::StreamEvent {
                            event_id: format!("ev-{seq}"),
                            name: crate::events::EVENT_EXTERNAL.into(),
                            timestamp: "2026-11-26T00:00:00.000Z".into(),
                            data: serde_json::json!({"seq": seq}),
                            cursor: format!("ep000001:{}", seq + 1),
                        }),
                    },
                )
                .await
                .unwrap();
            }
            filled_128_tx.send(()).unwrap();
            proceed_129_rx.await.unwrap();
            // The 129th frame overflows `EVENT_STREAM_CHANNEL_CAPACITY` and must immediately trigger `EventsCancel`.
            ipc::write_frame(
                &mut conn,
                &ToEndpoint::EventFrame {
                    id,
                    frame: ipc::EventWireFrame::Event(crate::events::StreamEvent {
                        event_id: "ev-overflow".into(),
                        name: crate::events::EVENT_EXTERNAL.into(),
                        timestamp: "2026-11-26T00:00:00.000Z".into(),
                        data: serde_json::json!({"seq": EVENT_STREAM_CHANNEL_CAPACITY}),
                        cursor: "ep000001:129".into(),
                    }),
                },
            )
            .await
            .unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut conn, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::EventsCancel { id: cancelled }) if cancelled == id
            ));
            cancelled_tx.send(()).unwrap();

            // Part 2 on the same connection: MCP `events/stream` slow consumer receives distinct
            // slow-consumer queue-full message (not memory budget full).
            let (mut peer_read, mut peer_write) = tokio::io::split(conn);
            let Some(ToDaemon::EventsStream { id: mcp_id, .. }) =
                ipc::read_frame::<_, ToDaemon>(&mut peer_read, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap()
            else {
                panic!("expected MCP subscription on preserved IPC connection");
            };
            ipc::write_frame(
                &mut peer_write,
                &ToEndpoint::EventFrame {
                    id: mcp_id,
                    frame: ipc::EventWireFrame::Active(crate::events::ActiveNotice {
                        cursor: "ep000001:0".into(),
                        truncated: false,
                        heartbeat_ms: 30_000,
                    }),
                },
            )
            .await
            .unwrap();
            mcp_active_read_rx.await.unwrap();
            let stop_sending = AtomicBool::new(false);
            let writer = async {
                for seq in 0..(EVENT_STREAM_CHANNEL_CAPACITY * 3) {
                    if stop_sending.load(Ordering::Acquire) {
                        break;
                    }
                    ipc::write_frame(
                        &mut peer_write,
                        &ToEndpoint::EventFrame {
                            id: mcp_id,
                            frame: ipc::EventWireFrame::Event(crate::events::StreamEvent {
                                event_id: format!("mcp-ev-{seq}"),
                                name: crate::events::EVENT_EXTERNAL.into(),
                                timestamp: "2026-11-26T00:00:00.000Z".into(),
                                data: serde_json::json!({"seq": seq}),
                                cursor: format!("ep000001:{}", seq + 1),
                            }),
                        },
                    )
                    .await
                    .unwrap();
                    tokio::task::yield_now().await;
                }
            };
            let reader = async {
                assert!(matches!(
                    ipc::read_frame::<_, ToDaemon>(&mut peer_read, ipc::REQUEST_LIMIT)
                        .await
                        .unwrap(),
                    Some(ToDaemon::EventsCancel { id: cancelled }) if cancelled == mcp_id
                ));
                stop_sending.store(true, Ordering::Release);
            };
            tokio::join!(writer, reader);
            mcp_cancelled_tx.send(()).unwrap();
            assert!(matches!(
                ipc::read_frame::<_, ToDaemon>(&mut peer_read, ipc::REQUEST_LIMIT)
                    .await
                    .unwrap(),
                Some(ToDaemon::Bye)
            ));
        });

        let endpoint = Arc::new(Endpoint::for_test(identity, paths));
        let (active, mut stream) = endpoint
            .events_stream(
                "test",
                crate::events::SubscribeRequest {
                    name: crate::events::EVENT_EXTERNAL.into(),
                    ..Default::default()
                },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(active.value.cursor, "ep000001:0");
        drop(active);

        timeout(Duration::from_secs(5), filled_128_rx)
            .await
            .unwrap()
            .unwrap();
        // Wait briefly for `read_loop` to buffer all 128 frames without overflowing.
        timeout(Duration::from_secs(5), async {
            while stream.rx.len() < EVENT_STREAM_CHANNEL_CAPACITY {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(!stream.overflowed());
        proceed_129_tx.send(()).unwrap();

        // Daemon receives `EventsCancel` immediately while all 128 frames are still unread in `stream.rx`.
        timeout(Duration::from_secs(5), cancelled_rx)
            .await
            .expect("128-slot overflow must send EventsCancel before consumer reads")
            .unwrap();
        assert!(stream.overflowed());
        assert!(!stream.link_closed());

        for expected in 0..EVENT_STREAM_CHANNEL_CAPACITY {
            let Some((ipc::EventWireFrame::Event(ev), _lease)) =
                timeout(Duration::from_secs(5), stream.next())
                    .await
                    .unwrap()
            else {
                panic!("missing buffered event {expected}");
            };
            assert_eq!(ev.event_id, format!("ev-{expected}"));
        }
        assert!(
            timeout(Duration::from_secs(5), stream.next())
                .await
                .unwrap()
                .is_none()
        );
        assert!(stream.overflowed());
        stream.cancel().await;

        // Verify MCP `handle_events_stream` emits the slow-consumer queue-full message.
        let leases = Arc::new(crate::mcp::RequestLeases::default());
        let server = crate::mcp::Server::new(
            crate::mcp::Backend::Daemon(Arc::clone(&endpoint)),
            &endpoint.launch,
            None,
            crate::config::Tools::default(),
            None,
            crate::config::InstructionsToolMode::Off,
        )
        .with_stdio_leases(Arc::clone(&leases));
        // Tiny 64-byte duplex pipe stalls stdout writes as soon as the client stops reading.
        let (client, serving) = tokio::io::duplex(64);
        let (read, write) = tokio::io::split(serving);
        let transport = crate::cli::stdio::StdioTransport::new(read, write, leases);
        let serving = tokio::spawn(async move {
            let service = server.serve(transport).await.unwrap();
            let _ = service.waiting().await;
        });
        let (read, mut input) = tokio::io::split(client);
        let mut lines = tokio::io::BufReader::new(read).lines();
        let meta = serde_json::json!({
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientInfo": {"name": "test", "version": "1"},
            "io.modelcontextprotocol/clientCapabilities": {},
        });
        let req = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "slow-stream",
            "method": "events/stream",
            "params": {
                "name": "external.event",
                "_meta": meta,
            }
        });
        input
            .write_all(format!("{req}\n").as_bytes())
            .await
            .unwrap();
        let active_line = timeout(Duration::from_secs(5), lines.next_line())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&active_line).unwrap()["method"],
            "notifications/events/active"
        );
        mcp_active_read_tx.send(()).unwrap();
        timeout(Duration::from_secs(10), mcp_cancelled_rx)
            .await
            .expect("paused MCP client must stall stdout and trigger EventsCancel")
            .unwrap();

        let mut term_msg = None;
        loop {
            let line = timeout(Duration::from_secs(10), lines.next_line())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let msg: serde_json::Value = serde_json::from_str(&line).unwrap();
            if msg["id"] == "slow-stream" {
                assert_eq!(msg["result"]["resultType"], "complete");
                break;
            }
            if msg["method"] == "notifications/events/terminated" {
                term_msg = Some(msg);
            }
        }
        let term_msg = term_msg.expect("expected terminated notification on slow consumer");
        assert_eq!(term_msg["params"]["error"]["code"], -32025);
        assert_eq!(
            term_msg["params"]["error"]["message"],
            "endpoint event stream queue full due to slow consumer"
        );

        endpoint.bye().await;
        drop(input);
        drop(lines);
        finish(peer).await;
        finish(serving).await;
    }
}
