//! The daemon's side of a connection: the handshake, the session table, the writer task, and the
//! calls it runs over local byte streams and Streamable HTTP.

mod events;

use crate::http::{self, Http};
use crate::ipc::{self, ToDaemon, ToEndpoint};
use crate::lease::{self, BytePool, Lease};
use crate::platform::runtime;
use crate::service::core::{RenderSlot, overload_task_message};
use crate::service::render as service_render;
use crate::service::{AgentDelivery, ExecContext, Service};
use crate::session::{LaunchContext, Session};
use crate::tasks::{Limits, Tasks};
use crate::transfer::Transfers;
use crate::transport::Listener;
use rmcp::model::{CallToolResult, ContentBlock};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch};
use tokio::time::{MissedTickBehavior, interval, timeout};
use tokio_util::sync::CancellationToken;

/// How long a session with no live connection keeps its cursors before it is dropped.
pub const SESSION_TTL: Duration = Duration::from_secs(600);
/// How many sessions the table holds, attached or detached.
pub const MAX_SESSIONS: usize = 256;
/// How many calls one connection may have running.
pub const MAX_IN_FLIGHT: usize = 32;
/// How long a connection has to send its Hello.
pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a write may go without positive progress before its connection is closed.
pub const WRITE_STALL: Duration = Duration::from_secs(60);
/// How often the daemon looks at its own state: expired sessions, expired tasks, idleness.
pub const TICK: Duration = Duration::from_secs(60);
/// How long the daemon must be idle before it exits.
pub const IDLE_AFTER: Duration = Duration::from_secs(600);
/// How many connections the daemon serves at once, handshaking or live.
pub const MAX_CONNECTIONS: usize = 128;
/// Independent limit for lightweight `HelloEmit` connections so emitters do not starve normal
/// session connections.
pub const MAX_EMIT_CONNECTIONS: usize = 128;
/// Independent per-connection cap for active `events/stream` subscriptions (separate from
/// `MAX_IN_FLIGHT`).
pub const MAX_EVENT_STREAMS_PER_CONN: usize = 16;
/// How long a stopping daemon waits for admitted starts to end their tasks.
pub const STOP_WAIT: Duration = Duration::from_secs(5);
/// Independent control-plane reserve for `list`, short `kill` receipts, and overload receipts.
pub const RESERVED: usize = lease::CONTROL_LIMIT;
/// Normal renders can coexist, but neither they nor one management action consume the slots
/// reserved for the other management action. This bounds render concurrency, not daemon RSS.
const RENDER_SLOTS: usize = 2;

/// What a daemon is run with.
#[derive(Clone, Debug)]
pub struct Config {
    /// How long a write may go without positive progress before its connection is closed.
    pub write_stall: Duration,
    /// How often the daemon looks at its own state.
    pub tick: Duration,
    /// How long the daemon must be idle before it exits.
    pub idle_after: Duration,
    /// Whether it exits at all when idle.
    pub idle_exit: bool,
    /// Optional override for the shared active-byte pool (used in unit tests).
    pub memory: Option<Arc<BytePool>>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            write_stall: WRITE_STALL,
            tick: TICK,
            idle_after: IDLE_AFTER,
            idle_exit: false,
            memory: None,
        }
    }
}

/// A connection's byte stream, boxed so the halves have one type whatever carries them.
trait Stream: AsyncRead + AsyncWrite + Send {}

impl<T: AsyncRead + AsyncWrite + Send> Stream for T {}

/// A boxed stream, which is `Unpin` however the stream itself is.
type BoxedStream = std::pin::Pin<Box<dyn Stream>>;
type Reader = ReadHalf<BoxedStream>;
type Writer = WriteHalf<BoxedStream>;

/// The daemon: the session table and the result budget, shared by every connection.
pub struct Daemon {
    service: Arc<Service>,
    tasks: Arc<Tasks>,
    namespace: String,
    build: String,
    /// The address it serves Streamable HTTP on, when it serves it: `Status` reports it, and the
    /// listener it names is the one `run` was given.
    http: Option<String>,
    transfers: Arc<Transfers>,
    public_root: Option<Arc<str>>,
    transfer_listener: tokio::sync::OnceCell<std::net::SocketAddr>,
    sessions: Mutex<HashMap<String, Entry>>,
    /// Active byte pool shared by ordinary outbound frames, media reads, and codemode packing.
    memory: Arc<BytePool>,
    /// Independent 8 MiB control-plane byte pool shared with HTTP sessions.
    control_memory: Arc<BytePool>,
    /// Independent control-plane permits for `kill`, `list`, and overload error receipts.
    reserved: Arc<Semaphore>,
    render: Arc<Semaphore>,
    render_kill: Arc<Semaphore>,
    render_list: Arc<Semaphore>,
    config: Config,
    /// Who is admitted, and whether the daemon is stopping.
    gate: Mutex<Gate>,
    /// Cancelled when the daemon stops, so the accept loop ends.
    stopped: CancellationToken,
}

/// Admission and lifetime: how many connections are handshaking or live, whether the daemon is
/// stopping, and since when it has been idle.
#[derive(Default)]
struct Gate {
    handshaking: usize,
    live: usize,
    emit_live: usize,
    stopping: bool,
    idle_since: Option<Instant>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TicketKind {
    Handshaking,
    Live,
    Emit,
}

/// A connection's place in the gate. It gives the place back on drop, so no way out of serving a
/// connection leaves it counted.
pub struct Ticket {
    daemon: Arc<Daemon>,
    kind: TicketKind,
}

impl Ticket {
    /// Counts the connection as live, once its Welcome is written.
    pub fn promote(&mut self) {
        if self.kind != TicketKind::Handshaking {
            return;
        }
        let mut gate = lock(&self.daemon.gate);
        gate.handshaking -= 1;
        gate.live += 1;
        self.kind = TicketKind::Live;
    }

    /// Moves a handshaking ticket into the independent `HelloEmit` pool.
    fn promote_emit(&mut self) -> bool {
        if self.kind != TicketKind::Handshaking {
            return false;
        }
        let mut gate = lock(&self.daemon.gate);
        if gate.stopping || gate.emit_live >= MAX_EMIT_CONNECTIONS {
            return false;
        }
        gate.handshaking -= 1;
        gate.emit_live += 1;
        self.kind = TicketKind::Emit;
        true
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        let mut gate = lock(&self.daemon.gate);
        match self.kind {
            TicketKind::Handshaking => gate.handshaking -= 1,
            TicketKind::Live => gate.live -= 1,
            TicketKind::Emit => gate.emit_live -= 1,
        }
    }
}

enum Handshake {
    Session(Attachment),
    Emit,
}

/// One session in the table.
struct Entry {
    session: Arc<Session>,
    /// The generation of the connection that last attached; it only ever increases.
    connection: u64,
    /// That connection's token, while one is attached.
    live: Option<CancellationToken>,
    /// When the last connection let go, if none holds the session now.
    detached_at: Option<Instant>,
}

enum OutgoingPermit {
    Main(
        #[expect(
            dead_code,
            reason = "held for Drop until the outgoing frame is written or dropped"
        )]
        Lease,
    ),
    Reserved(
        #[expect(
            dead_code,
            reason = "held for Drop until the outgoing frame is written or dropped"
        )]
        OwnedSemaphorePermit,
    ),
}

/// A frame on its way out, with the lease or control permit its bytes hold until written.
struct Outgoing {
    bytes: Vec<u8>,
    _permit: Option<OutgoingPermit>,
    /// Signalled once the frame is written. A progress forwarder waits on it, so at most one
    /// progress frame per call is queued and the latest wins.
    written: Option<oneshot::Sender<()>>,
}

/// A connection's hold on its session. Dropping it closes the connection, so no way out of
/// serving one — including a panic or the task being aborted — leaves a session attached to
/// nothing.
struct Attachment {
    daemon: Arc<Daemon>,
    attached: Attached,
}

impl Drop for Attachment {
    fn drop(&mut self) {
        self.daemon.close(&self.attached);
    }
}

/// What a connection holds after its handshake.
struct Attached {
    session: Arc<Session>,
    session_id: String,
    connection: u64,
    token: CancellationToken,
}

impl Daemon {
    pub fn with_config(
        tasks: Arc<Tasks>,
        namespace: String,
        build: String,
        http: Option<String>,
        public_root: Option<Arc<str>>,
        mut config: Config,
    ) -> Arc<Self> {
        let isolated = config.memory.is_some();
        let memory = config
            .memory
            .take()
            .unwrap_or_else(|| Arc::clone(lease::process_pool()));
        let control_memory = if isolated {
            BytePool::new(RESERVED)
        } else {
            Arc::clone(lease::control_pool())
        };
        Arc::new(Self {
            service: Arc::new(Service::new(Arc::clone(&tasks), true)),
            tasks,
            namespace,
            build,
            http,
            transfers: Arc::new(Transfers::new()),
            public_root,
            transfer_listener: tokio::sync::OnceCell::new(),
            sessions: Mutex::new(HashMap::new()),
            memory,
            control_memory,
            reserved: Arc::new(Semaphore::new(RESERVED)),
            render: Arc::new(Semaphore::new(RENDER_SLOTS)),
            render_kill: Arc::new(Semaphore::new(1)),
            render_list: Arc::new(Semaphore::new(1)),
            config,
            gate: Mutex::default(),
            stopped: CancellationToken::new(),
        })
    }

    pub fn memory(&self) -> &Arc<BytePool> {
        &self.memory
    }

    pub fn control_memory(&self) -> &Arc<BytePool> {
        &self.control_memory
    }

    /// The service every connection's calls run on.
    pub fn service(&self) -> Arc<Service> {
        Arc::clone(&self.service)
    }

    /// The transfer capability table shared across HTTP sessions and stdio endpoints.
    pub fn transfers(&self) -> Arc<Transfers> {
        Arc::clone(&self.transfers)
    }

    fn attach_transfer_listener(
        &self,
        listener: tokio::net::TcpListener,
        bound: std::net::SocketAddr,
    ) {
        let _ = self.transfer_listener.set(bound);
        tokio::spawn(http::serve_transfers(
            listener,
            Arc::clone(&self.transfers),
            self.stopped.clone(),
        ));
    }

    async fn ensure_transfer_listener(
        &self,
        launch: &LaunchContext,
        has_public_root: bool,
    ) -> Result<std::net::SocketAddr, String> {
        let allow_remote = crate::config::remote_allowed()
            || launch.var(crate::config::HTTP_ALLOW_REMOTE) == Some(std::ffi::OsStr::new("1"));
        let requested = crate::config::transfer_addr_of(
            launch.var(crate::config::TRANSFER_ADDR),
            allow_remote,
        )?;
        if let Some(addr) = requested {
            crate::config::check_routable_addr(
                crate::config::TRANSFER_ADDR,
                addr,
                has_public_root,
            )?;
        }
        let bound = *self
            .transfer_listener
            .get_or_try_init(|| async {
                let addr =
                    requested.unwrap_or_else(|| std::net::SocketAddr::from(([127, 0, 0, 1], 0)));
                let listener = tokio::net::TcpListener::bind(addr)
                    .await
                    .map_err(|error| format!("cannot listen on {addr} for transfer: {error}"))?;
                let bound = listener
                    .local_addr()
                    .map_err(|error| format!("cannot read transfer listener address: {error}"))?;
                tokio::spawn(http::serve_transfers(
                    listener,
                    Arc::clone(&self.transfers),
                    self.stopped.clone(),
                ));
                Ok::<std::net::SocketAddr, String>(bound)
            })
            .await?;
        if let Some(addr) = requested
            && (bound.ip() != addr.ip() || (addr.port() != 0 && bound.port() != addr.port()))
        {
            return Err(format!(
                "the daemon already serves /t/ on {bound}, not {addr}; stop it first"
            ));
        }
        crate::config::check_routable_addr(crate::config::TRANSFER_ADDR, bound, has_public_root)?;
        Ok(bound)
    }

    async fn transfer(
        &self,
        request: crate::transfer::Request,
        launch: &Arc<LaunchContext>,
        input_owner: crate::lease::Owner,
    ) -> crate::files::TrackedResult {
        let session_root = match crate::config::root_of(launch.var(crate::config::PUBLIC_URL)) {
            Ok(root) => root,
            Err(message) => {
                return crate::files::TrackedResult::plain(crate::transfer::transfer_error(
                    message,
                ));
            }
        };
        let public_root = session_root.or_else(|| self.public_root.as_deref().map(str::to_owned));
        if let Some(http_addr) = &self.http {
            if launch.var(crate::config::TRANSFER_ADDR).is_some() {
                return crate::files::TrackedResult::plain(crate::transfer::transfer_error(
                    format!(
                        "cannot set {} with --http; --http already serves /t/",
                        crate::config::TRANSFER_ADDR
                    ),
                ));
            }
            if public_root.is_none()
                && let Ok(addr) = http_addr.parse::<std::net::SocketAddr>()
                && let Err(message) = crate::config::check_routable_addr("--http", addr, false)
            {
                return crate::files::TrackedResult::plain(crate::transfer::transfer_error(
                    message,
                ));
            }
        }
        let inspected = match crate::transfer::inspect_transfer_request(
            request,
            Arc::clone(launch),
            input_owner,
        )
        .await
        {
            lease::Owned {
                value: Ok(target),
                owner,
            } => lease::Owned::new(target, owner),
            lease::Owned {
                value: Err(message),
                owner,
            } => {
                return crate::files::TrackedResult::owned(
                    crate::transfer::transfer_error(message),
                    owner,
                );
            }
        };
        let base = match (public_root, &self.http) {
            (Some(root), Some(_)) => root,
            (None, Some(http_addr)) => format!("http://{http_addr}/"),
            (public_root, None) => {
                let bound = match self
                    .ensure_transfer_listener(launch, public_root.is_some())
                    .await
                {
                    Ok(addr) => addr,
                    Err(message) => {
                        return crate::files::TrackedResult::plain(
                            crate::transfer::transfer_error(message),
                        );
                    }
                };
                public_root.unwrap_or_else(|| format!("http://{bound}/"))
            }
        };
        let owner = inspected.owner.clone();
        let result = inspected.run(|(path, shown_path, bytes)| {
            crate::transfer::mint_transfer(path, shown_path, bytes, &self.transfers, &base)
        });
        crate::files::TrackedResult::owned(result, owner)
    }

    /// Takes a place for a connection, or refuses it: the daemon is stopping, or it already
    /// serves [`MAX_CONNECTIONS`].
    pub fn admit(self: &Arc<Self>) -> Option<Ticket> {
        let mut gate = lock(&self.gate);
        if gate.stopping || gate.handshaking + gate.live >= MAX_CONNECTIONS {
            return None;
        }
        gate.handshaking += 1;
        // A connection that comes and goes between ticks restarts the idle interval.
        gate.idle_since = None;
        Some(Ticket {
            daemon: Arc::clone(self),
            kind: TicketKind::Handshaking,
        })
    }

    /// Serves one connection to the end of its life: the handshake, then calls until the stream
    /// ends, the peer says `Bye`, or a newer connection takes the session. The ticket is the
    /// connection's place in the gate, and it goes back when this returns.
    pub async fn serve(
        self: Arc<Self>,
        stream: impl AsyncRead + AsyncWrite + Send + 'static,
        mut ticket: Ticket,
    ) {
        let stream: BoxedStream = Box::pin(stream);
        let (mut reader, mut writer) = tokio::io::split(stream);
        // A stopping daemon ends every connection, handshaking ones included, and the guard then
        // drops the ticket, the stream, and any attachment.
        let handshake = {
            let handshake = self.handshake(&mut reader, &mut writer, &mut ticket);
            tokio::pin!(handshake);
            tokio::select! {
                biased;
                _ = self.stopped.cancelled() => return,
                result = &mut handshake => result,
            }
        };
        let Some(handshake) = handshake else {
            return;
        };
        match handshake {
            Handshake::Emit => {
                self.emit_loop(&mut reader, &mut writer).await;
            }
            Handshake::Session(guard) => {
                // The Welcome is written, so the connection is live rather than handshaking.
                ticket.promote();
                let attached = &guard.attached;
                let (outgoing, outgoing_rx) = mpsc::unbounded_channel();
                let stall = self.config.write_stall;
                // The writer owns the write half, so a stalled write never holds up a read.
                let closing = attached.token.clone();
                tokio::spawn(async move {
                    write_loop(writer, outgoing_rx, closing, stall).await;
                });
                let in_flight: Arc<Mutex<HashMap<u64, CancellationToken>>> = Arc::default();
                let event_streams: events::EventStreams = Arc::default();
                self.read_loop(&mut reader, attached, &outgoing, &in_flight, &event_streams)
                    .await;
            }
        }
    }

    /// Runs until the daemon stops or exits on its own: accepts connections, and on every tick
    /// drops what has expired and checks whether the daemon is still needed.
    pub async fn run(self: Arc<Self>, mut listener: Listener, http: Option<Http>) {
        // The HTTP front door runs beside the pipe or socket, and stops with the daemon.
        if let Some(http) = http {
            let daemon = Arc::clone(&self);
            let stopped = self.stopped.clone();
            tokio::spawn(async move {
                http::serve(http, daemon, stopped).await;
            });
        }
        let mut tick = interval(self.config.tick);
        // A tick that came due while the loop was busy runs once, not once per missed period.
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // The first tick is due at once; the daemon has only just started, so it is skipped.
        tick.tick().await;
        loop {
            tokio::select! {
                biased;
                _ = self.stopped.cancelled() => break,
                _ = tick.tick() => {
                    let now = Instant::now();
                    self.sweep_sessions(now);
                    self.tasks.expire_finished(now);
                    self.transfers.sweep(now);
                    if self.idle_expired(now) {
                        break;
                    }
                }
                accepted = listener.accept() => match accepted {
                    // A refused connection is dropped unsaid: the gate is full or closing.
                    Ok(stream) => {
                        if let Some(ticket) = self.admit() {
                            tokio::spawn(Arc::clone(&self).serve(stream, ticket));
                        }
                    }
                    Err(error) => {
                        eprintln!("fastexec: cannot accept a connection: {error}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                },
            }
        }
        // The listener goes first, so nothing new arrives while the daemon is stopping.
        drop(listener);
        self.stop();
        self.stopped_wait().await;
    }

    /// Stops the daemon when it has been idle for `idle_after` and exiting on idleness is what it
    /// was asked to do. Called under the gate, which is where idleness is read.
    fn idle_expired(&self, now: Instant) -> bool {
        // Only `Tasks::idle`, `Transfers::is_idle`, and lock-free `active_streams()` are called
        // under the gate; none takes `gate` or `sessions`.
        let mut gate = lock(&self.gate);
        let idle = self.config.idle_exit
            && gate.handshaking == 0
            && gate.live == 0
            && gate.emit_live == 0
            && self.service.event_bus().active_streams() == 0
            && self.tasks.idle()
            && self.transfers.is_idle(now);
        if !idle {
            gate.idle_since = None;
            return false;
        }
        let since = *gate.idle_since.get_or_insert(now);
        if now.saturating_duration_since(since) < self.config.idle_after {
            return false;
        }
        gate.stopping = true;
        true
    }

    /// Stops the daemon: nothing is admitted, every live connection's waits end, and every task
    /// is killed.
    pub fn stop(&self) {
        // Lock order is gate, then the session table, here as in `attach`.
        lock(&self.gate).stopping = true;
        let tokens: Vec<CancellationToken> = lock(&self.sessions)
            .values()
            .filter_map(|entry| entry.live.clone())
            .collect();
        for token in tokens {
            token.cancel();
        }
        self.stopped.cancel();
        self.tasks.shutdown();
    }

    /// Waits for every admitted start to end its task, so a stop does not leave one behind.
    pub async fn stopped_wait(&self) {
        let deadline = Instant::now() + STOP_WAIT;
        while self.tasks.pending() > 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Drops every session that has been detached for `SESSION_TTL`, counted from `now`.
    pub fn sweep_sessions(&self, now: Instant) {
        let mut expired = Vec::new();
        {
            let mut sessions = lock(&self.sessions);
            sessions.retain(|id, entry| match entry.detached_at {
                Some(at) if now.saturating_duration_since(at) >= SESSION_TTL => {
                    expired.push(id.clone());
                    false
                }
                _ => true,
            });
        }
        for id in expired {
            self.service.event_bus().remove_session(&id);
        }
    }

    /// Reads the Hello or HelloEmit and answers it. `None` means the connection is done: it ended,
    /// or a `Reject` was written and nothing more is said.
    async fn handshake(
        self: &Arc<Self>,
        reader: &mut Reader,
        writer: &mut Writer,
        ticket: &mut Ticket,
    ) -> Option<Handshake> {
        let first = timeout(
            HANDSHAKE_TIMEOUT,
            ipc::read_daemon_frame_owned(
                reader,
                ipc::REQUEST_LIMIT,
                &self.memory,
                &self.control_memory,
            ),
        )
        .await;
        let message = match first {
            Ok(Ok(ipc::DaemonFrame::Message(message))) => message,
            _ => return None,
        };
        message
            .owner
            .hold_future(self.handshake_message(writer, message.value, ticket))
            .await
    }

    async fn handshake_message(
        self: &Arc<Self>,
        writer: &mut Writer,
        message: ToDaemon,
        ticket: &mut Ticket,
    ) -> Option<Handshake> {
        if let ToDaemon::HelloEmit { protocol, build } = message {
            if protocol != ipc::PROTOCOL {
                reject(
                    writer,
                    self.config.write_stall,
                    &self.stopped,
                    format!("protocol {protocol} is not {}", ipc::PROTOCOL),
                )
                .await;
                return None;
            }
            if build != self.build {
                reject(
                    writer,
                    self.config.write_stall,
                    &self.stopped,
                    format!("build {build:?} is not {:?}", self.build),
                )
                .await;
                return None;
            }
            if !ticket.promote_emit() {
                reject(
                    writer,
                    self.config.write_stall,
                    &self.stopped,
                    "emit connection limit reached or daemon stopping".to_owned(),
                )
                .await;
                return None;
            }
            let welcome = ToEndpoint::Welcome {
                protocol: ipc::PROTOCOL,
                namespace: self.namespace.clone(),
                build: self.build.clone(),
                epoch: self.tasks.epoch().to_owned(),
            };
            let stall = self.config.write_stall;
            let written = {
                let writing = async { ipc::write_frame_idle(writer, &welcome, stall).await };
                tokio::pin!(writing);
                tokio::select! {
                    biased;
                    _ = self.stopped.cancelled() => false,
                    result = &mut writing => result.is_ok(),
                }
            };
            return written.then_some(Handshake::Emit);
        }
        let ToDaemon::Hello {
            protocol,
            namespace,
            build,
            session,
            connection,
            client_name,
            cwd,
            env,
        } = message
        else {
            reject(
                writer,
                self.config.write_stall,
                &self.stopped,
                "expected Hello".to_owned(),
            )
            .await;
            return None;
        };
        if protocol != ipc::PROTOCOL {
            return reject(
                writer,
                self.config.write_stall,
                &self.stopped,
                format!("protocol {protocol} is not {}", ipc::PROTOCOL),
            )
            .await;
        }
        if namespace != self.namespace {
            return reject(
                writer,
                self.config.write_stall,
                &self.stopped,
                format!("namespace {namespace:?} is not {:?}", self.namespace),
            )
            .await;
        }
        if build != self.build {
            return reject(
                writer,
                self.config.write_stall,
                &self.stopped,
                format!("build {build:?} is not {:?}", self.build),
            )
            .await;
        }
        if !valid_session(&session) {
            return reject(
                writer,
                self.config.write_stall,
                &self.stopped,
                format!("session {session:?} is not 1-64 ASCII alphanumeric characters"),
            )
            .await;
        }
        let launch = match ipc::launch_from_wire(&cwd, &env) {
            Ok(launch) => launch,
            Err(message) => {
                return reject(
                    writer,
                    self.config.write_stall,
                    &self.stopped,
                    format!("launch: {message}"),
                )
                .await;
            }
        };
        let token = CancellationToken::new();
        let session_id = session;
        let session = match self.attach(&session_id, connection, &client_name, launch, &token) {
            Ok(session) => session,
            Err(message) => {
                return reject(writer, self.config.write_stall, &self.stopped, message).await;
            }
        };
        let guard = Attachment {
            daemon: Arc::clone(self),
            attached: Attached {
                session,
                session_id,
                connection,
                token,
            },
        };
        let welcome = ToEndpoint::Welcome {
            protocol: ipc::PROTOCOL,
            namespace: self.namespace.clone(),
            build: self.build.clone(),
            epoch: self.tasks.epoch().to_owned(),
        };
        // A peer that will not read its Welcome must not hold the session: the write is bounded
        // like every other, and it also gives way to a replacement or a close, so the session is
        // handed back instead of held for the whole stall.
        let written = {
            let stall = self.config.write_stall;
            let writing = async { ipc::write_frame_idle(writer, &welcome, stall).await };
            tokio::pin!(writing);
            tokio::select! {
                biased;
                _ = guard.attached.token.cancelled() => false,
                _ = self.stopped.cancelled() => false,
                result = &mut writing => result.is_ok(),
            }
        };
        // A Welcome that could not be written drops the guard, which hands the session back.
        written.then_some(Handshake::Session(guard))
    }

    /// Lightweight request/ack loop for `HelloEmit` connections. Only `EmitEvent` and `Bye` are
    /// accepted; frames are charged exclusively against `memory` (never `control_memory`).
    async fn emit_loop(&self, reader: &mut Reader, writer: &mut Writer) {
        let max_frame = crate::events::MAX_EXTERNAL_EVENT_BYTES * 2;
        loop {
            let frame = tokio::select! {
                biased;
                _ = self.stopped.cancelled() => return,
                frame = ipc::read_daemon_frame_main_only(reader, max_frame, &self.memory) => frame,
            };
            let ack = match frame {
                Ok(ipc::DaemonFrame::Eof) => return,
                Ok(ipc::DaemonFrame::Overload { .. } | ipc::DaemonFrame::OverloadEvent { .. }) => {
                    ToEndpoint::EmitAck {
                        ok: false,
                        event_id: None,
                        cursor: None,
                        deduplicated: false,
                        error: Some(ipc::IpcEventError {
                            code: -32025,
                            message: ipc::input_budget_error().to_string(),
                            data: None,
                        }),
                    }
                }
                Ok(ipc::DaemonFrame::Message(owned)) => match owned.value {
                    ToDaemon::EmitEvent { event } => {
                        let result = self.service.event_bus().publish_external(event);
                        drop(owned.owner);
                        match result {
                            Ok(published) => ToEndpoint::EmitAck {
                                ok: true,
                                event_id: Some(published.event_id),
                                cursor: Some(published.cursor),
                                deduplicated: published.deduplicated,
                                error: None,
                            },
                            Err(err) => ToEndpoint::EmitAck {
                                ok: false,
                                event_id: None,
                                cursor: None,
                                deduplicated: false,
                                error: Some(err.into()),
                            },
                        }
                    }
                    ToDaemon::Bye => return,
                    _ => return,
                },
                Err(_) => return,
            };
            let stall = self.config.write_stall;
            let written = {
                let writing = async { ipc::write_frame_idle(writer, &ack, stall).await };
                tokio::pin!(writing);
                tokio::select! {
                    biased;
                    _ = self.stopped.cancelled() => false,
                    result = &mut writing => result.is_ok(),
                }
            };
            if !written {
                return;
            }
        }
    }

    /// Puts this connection in the table, or says why it cannot be. The gate is held across the
    /// check and the insert, so a stop cannot slip between them; lock order is gate, then the
    /// session table. Neither is held across an await.
    fn attach(
        &self,
        session_id: &str,
        connection: u64,
        client_name: &str,
        launch: LaunchContext,
        token: &CancellationToken,
    ) -> Result<Arc<Session>, String> {
        let gate = lock(&self.gate);
        if gate.stopping {
            return Err("the daemon is stopping".to_owned());
        }
        let mut sessions = lock(&self.sessions);
        if let Some(entry) = sessions.get_mut(session_id) {
            // Generations only ever increase, so this generation is the connection's identity:
            // a Hello that does not advance it is not the connection that holds the session.
            if entry.connection >= connection {
                return Err(format!(
                    "connection {connection} is not newer than {} for session {session_id}",
                    entry.connection
                ));
            }
            // The older connection closes, and the session keeps its cursors and its first
            // launch context.
            if let Some(previous) = entry.live.take() {
                previous.cancel();
            }
            entry.connection = connection;
            entry.live = Some(token.clone());
            entry.detached_at = None;
            return Ok(Arc::clone(&entry.session));
        }
        if sessions.len() >= MAX_SESSIONS {
            let oldest = sessions
                .iter()
                .filter(|(_, entry)| entry.detached_at.is_some())
                .min_by_key(|(_, entry)| entry.detached_at)
                .map(|(id, _)| id.clone());
            match oldest {
                Some(id) => {
                    sessions.remove(&id);
                    self.service.event_bus().remove_session(&id);
                }
                None => return Err(format!("{MAX_SESSIONS} sessions are attached")),
            }
        }
        let label = format!("{client_name}-{}", short(session_id));
        let launch = launch
            .with_memory(Arc::clone(&self.memory))
            .with_control_memory(Arc::clone(&self.control_memory));
        let session = Session::new(session_id.to_owned(), label, Arc::new(launch));
        sessions.insert(
            session_id.to_owned(),
            Entry {
                session: Arc::clone(&session),
                connection,
                live: Some(token.clone()),
                detached_at: None,
            },
        );
        Ok(session)
    }

    /// Reads calls until the connection ends. The reader owns the stream: a read cancelled
    /// part-way through a frame ends that stream, and this loop with it.
    async fn read_loop(
        self: &Arc<Self>,
        reader: &mut Reader,
        attached: &Attached,
        outgoing: &mpsc::UnboundedSender<Outgoing>,
        in_flight: &Arc<Mutex<HashMap<u64, CancellationToken>>>,
        event_streams: &events::EventStreams,
    ) {
        loop {
            let message = tokio::select! {
                biased;
                _ = attached.token.cancelled() => return,
                // A stop ends the connection even when no entry holds its token any more.
                _ = self.stopped.cancelled() => return,
                frame = ipc::read_daemon_frame_owned(reader, ipc::REQUEST_LIMIT, &self.memory, &self.control_memory) => match frame {
                    Ok(ipc::DaemonFrame::Message(message)) => message,
                    Ok(ipc::DaemonFrame::Overload { id: Some(id) }) => {
                        queue_result(outgoing, &self.reserved, &attached.token, id,
                            error(ipc::input_budget_error().to_string()));
                        continue;
                    }
                    Ok(ipc::DaemonFrame::OverloadEvent { id: Some(id) }) => {
                        let err = ipc::IpcEventError::memory_full(
                            &self.memory,
                            &self.control_memory,
                            "Server active memory budget is full while reading the event request; the operation was not dispatched",
                        );
                        events::queue_event_control(
                            outgoing,
                            &self.memory,
                            &self.reserved,
                            &attached.token,
                            id,
                            ipc::EventWireFrame::Error(err.value),
                        );
                        drop(err.owner);
                        continue;
                    }
                    _ => return,
                },
            };
            let lease::Owned {
                value: message,
                owner,
            } = message;
            match message {
                ToDaemon::Call {
                    id,
                    tool,
                    arguments,
                    request_id,
                } => {
                    // The endpoint preserves string IDs here; numeric IDs are
                    // conservatively charged for their surrounding quotes too.
                    let rpc_id_bytes = lease::json_len(&request_id).unwrap_or(usize::MAX);
                    let token = {
                        let mut calls = lock(in_flight);
                        if calls.contains_key(&id) || calls.len() >= MAX_IN_FLIGHT {
                            None
                        } else {
                            let token = attached.token.child_token();
                            calls.insert(id, token.clone());
                            Some(token)
                        }
                    };
                    match token {
                        Some(token) => {
                            tokio::spawn(owner.clone().hold_future(run_call(
                                Arc::clone(self),
                                Arc::clone(&attached.session),
                                attached.token.clone(),
                                token,
                                id,
                                tool,
                                arguments,
                                Arc::clone(in_flight),
                                outgoing.clone(),
                                rpc_id_bytes,
                                owner,
                            )));
                        }
                        None => {
                            let message = if lock(in_flight).contains_key(&id) {
                                format!("Call {id} is already in flight.")
                            } else {
                                format!(
                                    "{MAX_IN_FLIGHT} calls are already in flight on this connection."
                                )
                            };
                            queue_result(
                                outgoing,
                                &self.reserved,
                                &attached.token,
                                id,
                                error(message),
                            );
                        }
                    }
                }
                ToDaemon::Cancel { id } => {
                    if let Some(token) = lock(in_flight).get(&id).cloned() {
                        token.cancel();
                    }
                }
                ToDaemon::EventsList { id } => match self.service.event_bus().list_event_types() {
                    Ok(result) => {
                        if !events::queue_event_frame(
                            outgoing,
                            &self.memory,
                            id,
                            result.map(ipc::EventWireFrame::ListResult),
                        ) {
                            events::queue_event_control(
                                outgoing,
                                &self.memory,
                                &self.reserved,
                                &attached.token,
                                id,
                                ipc::EventWireFrame::Error(ipc::IpcEventError {
                                    code: -32025,
                                    message: "daemon active memory budget full during events/list"
                                        .to_owned(),
                                    data: None,
                                }),
                            );
                        }
                    }
                    Err(error) => {
                        events::queue_event_control(
                            outgoing,
                            &self.memory,
                            &self.reserved,
                            &attached.token,
                            id,
                            ipc::EventWireFrame::Error(error.into()),
                        );
                    }
                },
                ToDaemon::EventsPoll { id, request } => {
                    match self
                        .service
                        .event_bus()
                        .poll(request, Some(&attached.session_id))
                    {
                        Ok(result) => {
                            if !events::queue_event_frame(
                                outgoing,
                                &self.memory,
                                id,
                                result.map(ipc::EventWireFrame::PollResult),
                            ) {
                                events::queue_event_control(
                                    outgoing,
                                    &self.memory,
                                    &self.reserved,
                                    &attached.token,
                                    id,
                                    ipc::EventWireFrame::Error(ipc::IpcEventError {
                                        code: -32025,
                                        message:
                                            "daemon active memory budget full during events/poll"
                                                .to_owned(),
                                        data: None,
                                    }),
                                );
                            }
                        }
                        Err(error) => {
                            events::queue_event_control(
                                outgoing,
                                &self.memory,
                                &self.reserved,
                                &attached.token,
                                id,
                                ipc::EventWireFrame::Error(error.into()),
                            );
                        }
                    }
                }
                ToDaemon::EventsStream { id, request } => {
                    events::start_stream(
                        self,
                        event_streams,
                        &attached.session_id,
                        &attached.token,
                        outgoing,
                        id,
                        lease::Owned::new(request, owner),
                    );
                }
                ToDaemon::EventsCancel { id } => {
                    events::cancel_stream(event_streams, id);
                }
                ToDaemon::Status => {
                    // The asking connection's own session is not counted, so a client that has
                    // just handshaken sees the sessions that were there before it.
                    let sessions = lock(&self.sessions)
                        .keys()
                        .filter(|id| **id != attached.session_id)
                        .count();
                    let status = ToEndpoint::Status {
                        epoch: self.tasks.epoch().to_owned(),
                        build: self.build.clone(),
                        pid: std::process::id(),
                        sessions,
                        tasks: self.tasks.all().len(),
                        http: self.http.clone(),
                    };
                    queue_frame(outgoing, &self.reserved, &attached.token, &status);
                }
                ToDaemon::Stop => {
                    // The daemon ends every connection and exits; this one goes with them.
                    self.stop();
                    attached.token.cancel();
                    return;
                }
                ToDaemon::Bye => {
                    self.bye(attached);
                    attached.token.cancel();
                    return;
                }
                // A second Hello, HelloEmit, or EmitEvent is not valid on a served session.
                ToDaemon::Hello { .. }
                | ToDaemon::HelloEmit { .. }
                | ToDaemon::EmitEvent { .. } => return,
            }
        }
    }

    /// Ends the session at once: its cursors go with it, so a later connection of the same ID
    /// starts a new session. Only the connection whose generation holds the entry may do this.
    fn bye(&self, attached: &Attached) {
        let removed = {
            let mut sessions = lock(&self.sessions);
            if sessions
                .get(&attached.session_id)
                .is_some_and(|entry| entry.connection == attached.connection)
            {
                sessions.remove(&attached.session_id).is_some()
            } else {
                false
            }
        };
        if removed {
            self.service
                .event_bus()
                .remove_session(&attached.session_id);
        }
    }

    /// Closes a connection: its waits end, and the session is detached unless a newer connection
    /// already holds it. Every way a connection can end comes here, including a write stall and a
    /// reader that ran out of reserved room, so the detach cannot depend on why it ended: the
    /// generation and the live token are what say whether the entry is still this connection's.
    fn close(&self, attached: &Attached) {
        attached.token.cancel();
        let mut sessions = lock(&self.sessions);
        if let Some(entry) = sessions.get_mut(&attached.session_id)
            && entry.connection == attached.connection
            && entry.live.is_some()
        {
            entry.live = None;
            entry.detached_at = Some(Instant::now());
        }
    }
}

/// Runs one call: its progress frames while it waits, then its result within the budget.
#[allow(clippy::too_many_arguments)]
async fn run_call(
    daemon: Arc<Daemon>,
    session: Arc<Session>,
    connection: CancellationToken,
    token: CancellationToken,
    id: u64,
    tool: String,
    arguments: serde_json::Value,
    in_flight: Arc<Mutex<HashMap<u64, CancellationToken>>>,
    outgoing: mpsc::UnboundedSender<Outgoing>,
    rpc_id_bytes: usize,
    input_owner: crate::lease::Owner,
) {
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
        render_slot: Some(&render_slot),
        http_response: false,
        rpc_id_bytes,
    };
    let transfer = session.launch.var("FASTEXEC_TRANSFER") == Some(std::ffi::OsStr::new("true"));
    let mut tracked = if cancelled_before_dispatch {
        crate::files::TrackedResult::plain(error(
            "Cancelled before the operation started; nothing was executed.".into(),
        ))
    } else if tool == "exec" {
        daemon
            .service
            .exec_agent_json(arguments, ctx, delivery)
            .await
    } else if tool == "codemode" {
        match serde_json::from_value::<crate::codemode::control::Request>(arguments) {
            Ok(request) => {
                daemon
                    .service
                    .codemode_agent(request, ctx, delivery, transfer)
                    .await
            }
            Err(problem) => crate::files::TrackedResult::plain(error(problem.to_string())),
        }
    } else if tool == "transfer" {
        if !transfer {
            crate::files::TrackedResult::plain(crate::transfer::transfer_error(
                "transfer is disabled; set FASTEXEC_TRANSFER=true.".to_owned(),
            ))
        } else {
            match serde_json::from_value::<crate::transfer::Request>(arguments) {
                Ok(request) => {
                    daemon
                        .transfer(request, &session.launch, input_owner.clone())
                        .await
                }
                Err(problem) => {
                    crate::files::TrackedResult::plain(crate::transfer::transfer_error(format!(
                        "failed to deserialize parameters: {problem}"
                    )))
                }
            }
        }
    } else {
        // File tools run in the daemon process using the calling session's launch context.
        let launch = Arc::clone(&session.launch);
        crate::files::call_tracked(
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
            service_render::unleased_result_payload_capacity(result, &tracked.payload_owner)
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
        ToEndpoint::Result { result, .. } => service_render::result_payload_capacity(result),
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

/// Writes queued frames one at a time. A write that stalls for `stall` closes the connection,
/// which cancels only this connection's waits.
async fn write_loop(
    mut writer: Writer,
    mut outgoing: mpsc::UnboundedReceiver<Outgoing>,
    closing: CancellationToken,
    stall: Duration,
) {
    loop {
        let mut item = tokio::select! {
            biased;
            _ = closing.cancelled() => return,
            item = outgoing.recv() => match item {
                Some(item) => item,
                None => return,
            },
        };
        // The write gives way to the connection token as well as to the stall: a replacement or
        // a close drops this half, the queued frames, and their permits at once.
        let sent = {
            let writing = async { ipc::write_raw_idle(&mut writer, &item.bytes, stall).await };
            tokio::pin!(writing);
            tokio::select! {
                biased;
                _ = closing.cancelled() => false,
                result = &mut writing => result.is_ok(),
            }
        };
        if !sent {
            closing.cancel();
            return;
        }
        if let Some(written) = item.written.take() {
            let _ = written.send(());
        }
    }
}

/// Queues a result the reader itself makes. The reader never awaits permits, so a frame that
/// does not fit the reserved share closes the connection instead of blocking Cancel and Bye.
fn queue_result(
    outgoing: &mpsc::UnboundedSender<Outgoing>,
    reserved: &Arc<Semaphore>,
    closing: &CancellationToken,
    id: u64,
    result: CallToolResult,
) {
    queue_frame(
        outgoing,
        reserved,
        closing,
        &ToEndpoint::Result { id, result },
    );
}

/// Queues one frame the reader makes, taking its bytes from the reserved share. A frame that does
/// not fit closes the connection instead of blocking Cancel, Bye, and Stop.
fn queue_frame(
    outgoing: &mpsc::UnboundedSender<Outgoing>,
    reserved: &Arc<Semaphore>,
    closing: &CancellationToken,
    message: &ToEndpoint,
) {
    let Ok(length) = ipc::frame_len(message) else {
        return;
    };
    let Ok(want) = u32::try_from(length) else {
        closing.cancel();
        return;
    };
    let Ok(permit) = Arc::clone(reserved).try_acquire_many_owned(want) else {
        closing.cancel();
        return;
    };
    if let Ok(bytes) = ipc::encode_frame(message) {
        let _ = outgoing.send(Outgoing {
            bytes,
            _permit: Some(OutgoingPermit::Reserved(permit)),
            written: None,
        });
    }
}

fn error(message: String) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(message)])
}

/// Writes a `Reject` under the write stall, or not at all once the daemon is stopping, and says
/// the connection is done.
async fn reject(
    writer: &mut Writer,
    stall: Duration,
    stopped: &CancellationToken,
    message: String,
) -> Option<Handshake> {
    let message = ToEndpoint::Reject { message };
    let writing = ipc::write_frame_idle(writer, &message, stall);
    tokio::select! {
        biased;
        _ = stopped.cancelled() => {}
        _ = writing => {}
    }
    None
}

/// A session ID is 1 to 64 ASCII alphanumeric characters.
fn valid_session(session: &str) -> bool {
    !session.is_empty()
        && session.len() <= 64
        && session
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
}

/// The first six characters of a session ID, which names it inside its owner label.
fn short(session: &str) -> String {
    session.chars().take(6).collect()
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

/// How long a daemon waits for the namespace's lock before it gives up.
const DAEMON_LOCK_WAIT: Duration = Duration::from_secs(5);

/// Runs the daemon for `namespace` in this process: the hidden `daemon serve` subcommand.
/// Returns the exit code.
pub fn serve_process(
    namespace: String,
    build: String,
    http: Option<String>,
    idle_exit: bool,
    foreground: bool,
) -> i32 {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("fastexec: cannot start the daemon runtime: {error}");
            return 1;
        }
    };
    runtime.block_on(serve_daemon(namespace, build, http, idle_exit, foreground))
}

async fn serve_daemon(
    namespace: String,
    build: String,
    http: Option<String>,
    idle_exit: bool,
    foreground: bool,
) -> i32 {
    let actual = match runtime::build_id() {
        Ok(actual) => actual,
        Err(error) => {
            eprintln!("fastexec: cannot read this image: {error}");
            return 1;
        }
    };
    if actual != build {
        eprintln!("fastexec: this image is {actual}, not build {build}");
        return 1;
    }
    // A question that cannot be answered is answered the safe way: the daemon is treated as
    // inside a Job rather than left to die with its starter. A foreground daemon is the
    // supervisor's own process, and the supervisor owning its lifetime is the point.
    if !foreground && !matches!(runtime::in_job(), Ok(false)) {
        eprintln!("fastexec: the daemon is inside a Job and would die with its starter");
        return 1;
    }
    let paths = match runtime::paths(&namespace) {
        Ok(paths) => paths,
        Err(error) => {
            eprintln!("fastexec: cannot use the runtime directory: {error}");
            return 1;
        }
    };
    // One daemon per namespace. The lock is taken before the log is truncated, so a second
    // daemon exits without disturbing the one that is serving, and it is held for the life of
    // the process.
    let Some(_daemon_lock) = daemon_lock(&paths.ns.join(runtime::DAEMON_LOCK)).await else {
        eprintln!("fastexec: another daemon owns namespace {namespace}");
        return 1;
    };
    // A foreground daemon keeps its logs where its supervisor put them: its own stderr.
    let log = paths.ns.join("daemon.log");
    if !foreground && let Err(error) = runtime::redirect_stderr(&log) {
        eprintln!("fastexec: cannot write {}: {error}", log.display());
        return 1;
    }
    // The HTTP front door is opened before the pipe or socket, so a daemon that cannot serve it
    // never becomes reachable the other way, and it never exits on idleness: a client that speaks
    // HTTP expects it to be there.
    let tools = match crate::config::Tools::from_env() {
        Ok(tools) => tools,
        Err(message) => {
            eprintln!("fastexec: {message}");
            return 1;
        }
    };
    // Parsed only when `--http` or daemon-level `FASTEXEC_TRANSFER=true` uses it; a plain
    // daemon with `transfer` off leaves `FASTEXEC_PUBLIC_URL` unread.
    let public_root = if http.is_some() || tools.transfer {
        match crate::config::public_root() {
            Ok(root) => root.map(Arc::from),
            Err(message) => {
                eprintln!("fastexec: {message}");
                return 1;
            }
        }
    } else {
        None
    };
    // The launch context is captured before the daemon leaves its starting directory, so HTTP
    // sessions keep that directory as the one a task starts in.
    let (http_addr, http, startup_transfer, config) = match http {
        Some(value) => {
            if std::env::var_os(crate::config::TRANSFER_ADDR).is_some() {
                eprintln!(
                    "fastexec: cannot set {} with --http; --http already serves /t/",
                    crate::config::TRANSFER_ADDR
                );
                return 1;
            }
            let addr = match crate::config::http_addr(&value) {
                Ok(addr) => addr,
                Err(message) => {
                    eprintln!("fastexec: {message}");
                    return 1;
                }
            };
            let digests = match crate::config::http_token() {
                Ok(tokens) => tokens
                    .iter()
                    .map(|token| Sha256::digest(token.as_bytes()).into())
                    .collect(),
                Err(message) => {
                    eprintln!("fastexec: {message}");
                    return 1;
                }
            };
            let launch = match LaunchContext::current() {
                Ok(launch) => Arc::new(launch.without("FASTEXEC_HTTP_TOKEN")),
                Err(error) => {
                    eprintln!("fastexec: cannot read the HTTP launch context: {error}");
                    return 1;
                }
            };
            let instructions_tool = match crate::config::instructions_tool_mode() {
                Ok(mode) => mode,
                Err(message) => {
                    eprintln!("fastexec: {message}");
                    return 1;
                }
            };
            let extra_instructions = match crate::config::instructions_file(&launch) {
                Ok(extra) => extra,
                Err(message) => {
                    eprintln!("fastexec: {message}");
                    return 1;
                }
            };
            let listener = match tokio::net::TcpListener::bind(addr).await {
                Ok(listener) => listener,
                Err(error) => {
                    eprintln!("fastexec: cannot listen on {addr} for HTTP: {error}");
                    return 1;
                }
            };
            let bound = listener
                .local_addr()
                .map_or_else(|_| addr.to_string(), |addr| addr.to_string());
            (
                Some(bound),
                Some(Http {
                    listener,
                    digests,
                    tools,
                    launch,
                    public_root: public_root.clone(),
                    extra_instructions,
                    instructions_tool,
                }),
                None,
                Config {
                    idle_exit: false,
                    ..Config::default()
                },
            )
        }
        None => {
            let startup_transfer = if tools.transfer {
                match crate::config::transfer_addr() {
                    Ok(Some(addr)) => {
                        if let Err(message) = crate::config::check_routable_addr(
                            crate::config::TRANSFER_ADDR,
                            addr,
                            public_root.is_some(),
                        ) {
                            eprintln!("fastexec: {message}");
                            return 1;
                        }
                        let listener = match tokio::net::TcpListener::bind(addr).await {
                            Ok(listener) => listener,
                            Err(error) => {
                                eprintln!(
                                    "fastexec: cannot listen on {addr} for transfer: {error}"
                                );
                                return 1;
                            }
                        };
                        let bound = listener.local_addr().unwrap_or(addr);
                        Some((listener, bound))
                    }
                    Ok(None) => None,
                    Err(message) => {
                        eprintln!("fastexec: {message}");
                        return 1;
                    }
                }
            } else {
                None
            };
            (
                None,
                None,
                startup_transfer,
                Config {
                    idle_exit: idle_exit && !foreground,
                    ..Config::default()
                },
            )
        }
    };
    // The daemon leaves the directory it was started from: a Windows process holds that directory
    // open for its whole life, and a starter's checkout must stay removable.
    if let Err(error) = std::env::set_current_dir(&paths.root) {
        eprintln!("fastexec: cannot leave the starting directory: {error}");
        return 1;
    }
    // A socket file left by a daemon that died would block the bind.
    #[cfg(unix)]
    let _ = std::fs::remove_file(&paths.endpoint);
    let listener = match Listener::bind(&paths.endpoint) {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!(
                "fastexec: cannot listen on {}: {error}",
                std::path::Path::new(&paths.endpoint).display()
            );
            return 1;
        }
    };
    // Whatever other namespaces left behind is swept, and this one starts with empty logs.
    runtime::sweep(&paths.root, &namespace);
    let logs = paths.ns.join("logs");
    let _ = std::fs::remove_dir_all(&logs);
    if let Err(error) = runtime::create_private(&logs) {
        eprintln!("fastexec: cannot create {}: {error}", logs.display());
        return 1;
    }
    let tasks = match Tasks::new_in(Limits::ON, logs) {
        Ok(tasks) => tasks,
        Err(error) => {
            eprintln!("fastexec: cannot create the task directory: {error}");
            return 1;
        }
    };
    // A foreground daemon's shutdown handlers are registered before it says it is ready, so a
    // signal that arrives after the ready line is never missed.
    let signal = if foreground {
        match foreground_signal() {
            Ok(signal) => Some(signal),
            Err(error) => {
                eprintln!("fastexec: cannot watch shutdown signals: {error}");
                return 1;
            }
        }
    } else {
        None
    };
    let shutdown = async move {
        match signal {
            Some(signal) => signal.await,
            None => std::future::pending::<()>().await,
        }
    };
    let daemon = Daemon::with_config(tasks, namespace, build, http_addr, public_root, config);
    if let Some((transfer_tcp, bound)) = startup_transfer {
        daemon.attach_transfer_listener(transfer_tcp, bound);
    }
    let mut run = std::pin::pin!(Arc::clone(&daemon).run(listener, http));
    let mut shutdown = std::pin::pin!(shutdown);
    if foreground {
        // Both listeners are bound and the handlers are in place, so this line is what a
        // supervisor waits for: the epoch, this process, the namespace, and the address actually
        // bound when the port was asked for as 0.
        match &daemon.http {
            Some(addr) => eprintln!(
                "fastexec daemon {} pid {} namespace {} http {addr}",
                daemon.tasks.epoch(),
                std::process::id(),
                daemon.namespace
            ),
            None => eprintln!(
                "fastexec daemon {} pid {} namespace {}",
                daemon.tasks.epoch(),
                std::process::id(),
                daemon.namespace
            ),
        }
        tokio::select! {
            () = &mut run => {}
            () = &mut shutdown => {
                // A signal takes the same path as `daemon stop`, and the run future is awaited
                // through its tail so the listener is dropped and the tasks are waited for.
                daemon.stop();
                run.await;
            }
        }
    } else {
        run.await;
    }
    // Exit here, with the namespace's lock still held: returning would drop it before the
    // process ends, and another daemon could take the namespace while this one is still alive.
    std::process::exit(0);
}

/// Watches the signals that end a foreground daemon. Registration happens here, synchronously,
/// so a signal that arrives after the caller prints its readiness is never missed: a future
/// whose body has not been polled has registered nothing.
#[cfg(unix)]
fn foreground_signal() -> std::io::Result<impl std::future::Future<Output = ()>> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    Ok(async move {
        tokio::select! {
            _ = terminate.recv() => {}
            _ = interrupt.recv() => {}
        }
    })
}

/// The Windows console events that end a foreground daemon. A logoff is not one of them: a
/// service receives it whenever any user logs off.
#[cfg(windows)]
fn foreground_signal() -> std::io::Result<impl std::future::Future<Output = ()>> {
    use tokio::signal::windows;

    let mut ctrl_c = windows::ctrl_c()?;
    let mut ctrl_break = windows::ctrl_break()?;
    let mut ctrl_close = windows::ctrl_close()?;
    let mut ctrl_shutdown = windows::ctrl_shutdown()?;
    Ok(async move {
        tokio::select! {
            _ = ctrl_c.recv() => {}
            _ = ctrl_break.recv() => {}
            _ = ctrl_close.recv() => {}
            _ = ctrl_shutdown.recv() => {}
        }
    })
}

/// Waits for the namespace's daemon lock, or returns `None` when another daemon holds it.
async fn daemon_lock(path: &std::path::Path) -> Option<runtime::Lock> {
    let deadline = Instant::now() + DAEMON_LOCK_WAIT;
    loop {
        match runtime::Lock::try_lock(path) {
            Ok(Some(lock)) => return Some(lock),
            Ok(None) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            _ => return None,
        }
    }
}

#[cfg(test)]
mod tests;
