//! The daemon's side of a connection: the handshake, the session table, the writer task, and the
//! calls it runs over local byte streams and Streamable HTTP.

use crate::http::{self, Http};
use crate::ipc::{self, ToDaemon, ToEndpoint};
use crate::runtime;
use crate::service::{Call, OutputMode, Render, RenderSlot, Service};
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
/// How long a write may stall before its connection is closed.
pub const WRITE_STALL: Duration = Duration::from_secs(30);
/// How often the daemon looks at its own state: expired sessions, expired tasks, idleness.
pub const TICK: Duration = Duration::from_secs(60);
/// How long the daemon must be idle before it exits.
pub const IDLE_AFTER: Duration = Duration::from_secs(600);
/// How many connections the daemon serves at once, handshaking or live.
pub const MAX_CONNECTIONS: usize = 128;
/// How long a stopping daemon waits for admitted starts to end their tasks.
pub const STOP_WAIT: Duration = Duration::from_secs(5);
/// How many bytes of queued, serialized frames the daemon holds across every connection.
pub const BUDGET: usize = 128 << 20;
/// The share of the budget `kill` and `list` results use, so a queue full of large results cannot
/// hold up the calls that clear it.
pub const RESERVED: usize = 8 << 20;
/// Normal renders can coexist, but neither they nor one management action consume the slots
/// reserved for the other management action. This bounds render concurrency, not daemon RSS.
const RENDER_SLOTS: usize = 2;

/// What a daemon is run with.
#[derive(Clone, Copy, Debug)]
pub struct Config {
    /// How long a write may stall before its connection is closed.
    pub write_stall: Duration,
    /// How often the daemon looks at its own state.
    pub tick: Duration,
    /// How long the daemon must be idle before it exits.
    pub idle_after: Duration,
    /// Whether it exits at all when idle.
    pub idle_exit: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            write_stall: WRITE_STALL,
            tick: TICK,
            idle_after: IDLE_AFTER,
            idle_exit: false,
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
    /// One permit per byte of queued result, `BUDGET - RESERVED` of them.
    budget: Arc<Semaphore>,
    /// The permits `kill` and `list` results use.
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
    stopping: bool,
    idle_since: Option<Instant>,
}

/// A connection's place in the gate. It gives the place back on drop, so no way out of serving a
/// connection leaves it counted.
pub struct Ticket {
    daemon: Arc<Daemon>,
    /// Whether the place counts as live rather than as handshaking.
    live: bool,
}

impl Ticket {
    /// Counts the connection as live, once its Welcome is written.
    pub fn promote(&mut self) {
        if self.live {
            return;
        }
        let mut gate = lock(&self.daemon.gate);
        gate.handshaking -= 1;
        gate.live += 1;
        self.live = true;
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        let mut gate = lock(&self.daemon.gate);
        if self.live {
            gate.live -= 1;
        } else {
            gate.handshaking -= 1;
        }
    }
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

/// A frame on its way out, with the permits its bytes hold until it is written.
struct Outgoing {
    bytes: Vec<u8>,
    _permit: Option<OwnedSemaphorePermit>,
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
    render: Render,
}

impl Daemon {
    pub fn with_config(
        tasks: Arc<Tasks>,
        namespace: String,
        build: String,
        http: Option<String>,
        public_root: Option<Arc<str>>,
        config: Config,
    ) -> Arc<Self> {
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
            budget: Arc::new(Semaphore::new(BUDGET - RESERVED)),
            reserved: Arc::new(Semaphore::new(RESERVED)),
            render: Arc::new(Semaphore::new(RENDER_SLOTS)),
            render_kill: Arc::new(Semaphore::new(1)),
            render_list: Arc::new(Semaphore::new(1)),
            config,
            gate: Mutex::default(),
            stopped: CancellationToken::new(),
        })
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
        let allow_remote = http::remote_allowed()
            || launch.var("FASTEXEC_HTTP_ALLOW_REMOTE") == Some(std::ffi::OsStr::new("1"));
        let requested = crate::transfer_addr_of(launch.var(crate::TRANSFER_ADDR), allow_remote)?;
        if let Some(addr) = requested {
            crate::check_routable_addr(crate::TRANSFER_ADDR, addr, has_public_root)?;
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
        crate::check_routable_addr(crate::TRANSFER_ADDR, bound, has_public_root)?;
        Ok(bound)
    }

    async fn transfer(
        &self,
        request: crate::transfer::Request,
        launch: &Arc<LaunchContext>,
        render: Render,
    ) -> CallToolResult {
        let session_root = match http::root_of(launch.var(http::PUBLIC_URL)) {
            Ok(root) => root,
            Err(message) => return crate::transfer_error(message, render),
        };
        let public_root = session_root.or_else(|| self.public_root.as_deref().map(str::to_owned));
        if let Some(http_addr) = &self.http {
            if launch.var(crate::TRANSFER_ADDR).is_some() {
                return crate::transfer_error(
                    format!(
                        "cannot set {} with --http; --http already serves /t/",
                        crate::TRANSFER_ADDR
                    ),
                    render,
                );
            }
            if public_root.is_none()
                && let Ok(addr) = http_addr.parse::<std::net::SocketAddr>()
                && let Err(message) = crate::check_routable_addr("--http", addr, false)
            {
                return crate::transfer_error(message, render);
            }
        }
        let (path, shown_path, bytes) =
            match crate::inspect_transfer_request(request, Arc::clone(launch)).await {
                Ok(inspected) => inspected,
                Err(message) => return crate::transfer_error(message, render),
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
                    Err(message) => return crate::transfer_error(message, render),
                };
                public_root.unwrap_or_else(|| format!("http://{bound}/"))
            }
        };
        crate::mint_transfer(path, shown_path, bytes, &self.transfers, &base, render)
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
            live: false,
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
        let guard = {
            let handshake = self.handshake(&mut reader, &mut writer);
            tokio::pin!(handshake);
            tokio::select! {
                biased;
                _ = self.stopped.cancelled() => return,
                guard = &mut handshake => guard,
            }
        };
        let Some(guard) = guard else {
            return;
        };
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
        self.read_loop(&mut reader, attached, &outgoing, &in_flight)
            .await;
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
        // Only `Tasks::idle` and `Transfers::is_idle` are called under the gate; neither takes
        // `gate` or `sessions`.
        let mut gate = lock(&self.gate);
        let idle = self.config.idle_exit
            && gate.handshaking == 0
            && gate.live == 0
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
        let mut sessions = lock(&self.sessions);
        sessions.retain(|_, entry| match entry.detached_at {
            Some(at) => now.saturating_duration_since(at) < SESSION_TTL,
            None => true,
        });
    }

    /// Reads the Hello and answers it. `None` means the connection is done: it ended, or a
    /// `Reject` was written and nothing more is said.
    async fn handshake(
        self: &Arc<Self>,
        reader: &mut Reader,
        writer: &mut Writer,
    ) -> Option<Attachment> {
        let first = timeout(
            HANDSHAKE_TIMEOUT,
            ipc::read_frame::<_, ToDaemon>(reader, ipc::REQUEST_LIMIT),
        )
        .await;
        let message = match first {
            Ok(Ok(Some(message))) => message,
            Ok(Ok(None)) | Ok(Err(_)) | Err(_) => return None,
        };
        let ToDaemon::Hello {
            protocol,
            namespace,
            build,
            session,
            connection,
            client_name,
            output_mode,
            structured_content,
            cwd,
            env,
        } = message
        else {
            return reject(
                writer,
                self.config.write_stall,
                &self.stopped,
                "expected Hello".to_owned(),
            )
            .await;
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
        let mode = match OutputMode::parse(&output_mode) {
            Ok(mode) => mode,
            Err(message) => {
                return reject(
                    writer,
                    self.config.write_stall,
                    &self.stopped,
                    format!("output_mode: {message}"),
                )
                .await;
            }
        };
        let launch = match LaunchContext::from_wire(&cwd, &env) {
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
                render: Render {
                    output_mode: mode,
                    structured_content,
                },
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
            let writing = async { ipc::write_frame(writer, &welcome).await };
            tokio::pin!(writing);
            tokio::select! {
                biased;
                _ = guard.attached.token.cancelled() => false,
                _ = self.stopped.cancelled() => false,
                result = timeout(self.config.write_stall, &mut writing) => matches!(result, Ok(Ok(()))),
            }
        };
        // A Welcome that could not be written drops the guard, which hands the session back.
        written.then_some(guard)
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
                }
                None => return Err(format!("{MAX_SESSIONS} sessions are attached")),
            }
        }
        let label = format!("{client_name}-{}", short(session_id));
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
    ) {
        loop {
            let message = tokio::select! {
                biased;
                _ = attached.token.cancelled() => return,
                // A stop ends the connection even when no entry holds its token any more.
                _ = self.stopped.cancelled() => return,
                frame = ipc::read_frame::<_, ToDaemon>(reader, ipc::REQUEST_LIMIT) => match frame {
                    Ok(Some(message)) => message,
                    Ok(None) | Err(_) => return,
                },
            };
            match message {
                ToDaemon::Call {
                    id,
                    tool,
                    arguments,
                    ..
                } => {
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
                            tokio::spawn(run_call(
                                Arc::clone(self),
                                Arc::clone(&attached.session),
                                attached.render,
                                attached.token.clone(),
                                token,
                                id,
                                tool,
                                arguments,
                                Arc::clone(in_flight),
                                outgoing.clone(),
                            ));
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
                // A second Hello is not a message a served connection can answer.
                ToDaemon::Hello { .. } => return,
            }
        }
    }

    /// Ends the session at once: its cursors go with it, so a later connection of the same ID
    /// starts a new session. Only the connection whose generation holds the entry may do this.
    fn bye(&self, attached: &Attached) {
        let mut sessions = lock(&self.sessions);
        if sessions
            .get(&attached.session_id)
            .is_some_and(|entry| entry.connection == attached.connection)
        {
            sessions.remove(&attached.session_id);
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
    render: Render,
    connection: CancellationToken,
    token: CancellationToken,
    id: u64,
    tool: String,
    arguments: serde_json::Value,
    in_flight: Arc<Mutex<HashMap<u64, CancellationToken>>>,
    outgoing: mpsc::UnboundedSender<Outgoing>,
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
    let call = Call {
        session: &session,
        launch: None,
        render,
        cancel: token.clone(),
        progress: Some(progress),
        call_id: Some(id),
        nested: false,
        started: None,
        render_slot: Some(&render_slot),
    };
    let transfer = session.launch.var("FASTEXEC_TRANSFER") == Some(std::ffi::OsStr::new("true"));
    let result = if cancelled_before_dispatch {
        error("Cancelled before the operation started; nothing was executed.".into())
    } else if tool == "exec" {
        daemon.service.call_json(arguments, call).await
    } else if tool == "codemode" {
        match serde_json::from_value::<crate::codemode::Request>(arguments) {
            Ok(request) => daemon.service.codemode(request, call, transfer).await,
            Err(problem) => error(problem.to_string()),
        }
    } else if tool == "transfer" {
        if !transfer {
            crate::transfer_error(
                "transfer is disabled; set FASTEXEC_TRANSFER=true.".to_owned(),
                render,
            )
        } else {
            match serde_json::from_value::<crate::transfer::Request>(arguments) {
                Ok(request) => daemon.transfer(request, &session.launch, render).await,
                Err(problem) => crate::transfer_error(
                    format!("failed to deserialize parameters: {problem}"),
                    render,
                ),
            }
        }
    } else {
        // File tools run in the daemon process using the calling session's launch context.
        let launch = Arc::clone(&session.launch);
        crate::files::call(&tool, arguments, launch, transfer, render).await
    };
    // The result goes out last. Aborting the forwarder is not enough on its own: one already
    // inside an encode or a send could still queue a frame after the result, so this waits for
    // it to be gone.
    forwarder.stop().await;

    let mut message = ToEndpoint::Result { id, result };
    let Some(mut length) = measure(&message) else {
        lock(&in_flight).remove(&id);
        return;
    };
    if length > ipc::RESPONSE_LIMIT + 4 {
        // The call ran: saying so, and where to look, is what a caller can act on.
        let text = format!(
            "The result was {length} bytes, over the 64 MiB frame limit; the call ran, and list shows its task and logPath."
        );
        message = ToEndpoint::Result {
            id,
            result: error(text),
        };
        match measure(&message) {
            Some(smaller) => length = smaller,
            None => {
                lock(&in_flight).remove(&id);
                return;
            }
        }
    }
    // A small `kill` or `list` result takes the reserved share; everything else, and any result
    // too large for that share, takes the main budget, which holds one whole frame.
    let (semaphore, capacity) = if management && length <= RESERVED {
        (&daemon.reserved, RESERVED)
    } else {
        (&daemon.budget, BUDGET - RESERVED)
    };
    let want = u32::try_from(length.min(capacity)).unwrap_or(u32::MAX);
    let permit = tokio::select! {
        biased;
        _ = connection.cancelled() => {
            // The connection is gone, so the result has nowhere to go. This is the connection's
            // token, not the call's: a cancelled call still has a result to send.
            lock(&in_flight).remove(&id);
            return;
        }
        permit = Arc::clone(semaphore).acquire_many_owned(want) => match permit {
            Ok(permit) => permit,
            Err(_) => {
                lock(&in_flight).remove(&id);
                return;
            }
        },
    };
    // The room for these bytes was reserved before they existed.
    let bytes = ipc::encode_measured(&message, length);
    drop(message);
    drop(render_slot);
    if let Ok(bytes) = bytes {
        let _ = outgoing.send(Outgoing {
            bytes,
            _permit: Some(permit),
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
        let item = tokio::select! {
            biased;
            _ = closing.cancelled() => return,
            item = outgoing.recv() => match item {
                Some(item) => item,
                None => return,
            },
        };
        let Outgoing {
            bytes,
            _permit,
            written,
        } = item;
        // The write gives way to the connection token as well as to the stall: a replacement or
        // a close drops this half, the queued frames, and their permits at once.
        let sent = {
            let writing = async { ipc::write_raw(&mut writer, &bytes).await };
            tokio::pin!(writing);
            tokio::select! {
                biased;
                _ = closing.cancelled() => false,
                result = timeout(stall, &mut writing) => matches!(result, Ok(Ok(()))),
            }
        };
        if !sent {
            closing.cancel();
            return;
        }
        if let Some(written) = written {
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
            _permit: Some(permit),
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
) -> Option<Attachment> {
    let message = ToEndpoint::Reject { message };
    let writing = ipc::write_frame(writer, &message);
    tokio::select! {
        biased;
        _ = stopped.cancelled() => {}
        _ = timeout(stall, writing) => {}
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
    let render = match crate::render() {
        Ok(render) => render,
        Err(message) => {
            eprintln!("fastexec: {message}");
            return 1;
        }
    };
    let tools = match crate::Tools::from_env() {
        Ok(tools) => tools,
        Err(message) => {
            eprintln!("fastexec: {message}");
            return 1;
        }
    };
    // Parsed only when `--http` or daemon-level `FASTEXEC_TRANSFER=true` uses it; a plain
    // daemon with `transfer` off leaves `FASTEXEC_PUBLIC_URL` unread.
    let public_root = if http.is_some() || tools.transfer {
        match http::public_root() {
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
            if std::env::var_os(crate::TRANSFER_ADDR).is_some() {
                eprintln!(
                    "fastexec: cannot set {} with --http; --http already serves /t/",
                    crate::TRANSFER_ADDR
                );
                return 1;
            }
            let addr = match crate::http_addr(&value) {
                Ok(addr) => addr,
                Err(message) => {
                    eprintln!("fastexec: {message}");
                    return 1;
                }
            };
            let digests = match crate::http_token() {
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
            let instructions_tool = match crate::instructions_tool_mode() {
                Ok(mode) => mode,
                Err(message) => {
                    eprintln!("fastexec: {message}");
                    return 1;
                }
            };
            let extra_instructions = match crate::instructions_file(&launch) {
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
                    render,
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
                match crate::transfer_addr() {
                    Ok(Some(addr)) => {
                        if let Err(message) = crate::check_routable_addr(
                            crate::TRANSFER_ADDR,
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
mod tests {
    // `Tasks::new` uses one directory per process, so these tests hold a std lock for their whole
    // length: they serialize the binary's task tests, and a current-thread runtime is what they
    // run on, so the guard across awaits is the point.
    #![allow(clippy::await_holding_lock)]
    // The task-starting scenarios run only where a task can start, so on Unix their helpers have
    // no caller.
    #![cfg_attr(not(windows), allow(dead_code))]

    use super::*;
    use crate::ipc::OsText;
    use crate::tasks::Limits;
    use crate::transport::Endpoint;
    use serde_json::{Value, json};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// `Tasks::new` uses one directory per process, so only one test in the binary builds one at
    /// a time; the daemon tests share that lock with the task tests.
    fn one_at_a_time() -> MutexGuard<'static, ()> {
        crate::tasks::ONE_TASKS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    /// A daemon for one test, holding the process-wide task lock and ending its tasks however
    /// the test ends.
    struct Fixture {
        daemon: Arc<Daemon>,
        _one_at_a_time: MutexGuard<'static, ()>,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.daemon.tasks.shutdown();
        }
    }

    fn fixture() -> Fixture {
        fixture_with_config(Config::default())
    }

    fn fixture_with_config(config: Config) -> Fixture {
        let lock = one_at_a_time();
        let daemon = Daemon::with_config(
            Tasks::new(Limits::OFF).unwrap(),
            "fastexec".to_owned(),
            env!("CARGO_PKG_VERSION").to_owned(),
            None,
            None,
            config,
        );
        Fixture {
            daemon,
            _one_at_a_time: lock,
        }
    }

    fn fixture_with_write_stall(write_stall: Duration) -> Fixture {
        fixture_with_config(Config {
            write_stall,
            ..Config::default()
        })
    }

    fn hello(session: &str, connection: u64) -> ToDaemon {
        let (cwd, env) = LaunchContext::current().unwrap().to_wire();
        ToDaemon::Hello {
            protocol: ipc::PROTOCOL,
            namespace: "fastexec".to_owned(),
            build: env!("CARGO_PKG_VERSION").to_owned(),
            session: session.to_owned(),
            connection,
            client_name: "client".to_owned(),
            output_mode: "both".to_owned(),
            structured_content: true,
            cwd,
            env,
        }
    }

    /// A Hello that also carries a variable only it knows, so a task proves where its
    /// environment came from.
    fn hello_with(session: &str, connection: u64, name: &str, value: &str) -> ToDaemon {
        let mut message = hello(session, connection);
        if let ToDaemon::Hello { env, .. } = &mut message {
            env.push((
                OsText::Text(name.to_owned()),
                OsText::Text(value.to_owned()),
            ));
        }
        message
    }

    fn call(id: u64, arguments: Value) -> ToDaemon {
        ToDaemon::Call {
            id,
            tool: "exec".to_owned(),
            request_id: id.to_string(),
            arguments,
        }
    }

    /// A task that waits far longer than a test runs, so a wait on it is still pending.
    fn long_wait(id: u64) -> ToDaemon {
        call(
            id,
            json!({
                "action": "start", "command": "sleep 30", "loginShell": false, "waitMs": 30000
            }),
        )
    }

    struct Client {
        reader: ReadHalf<BoxedStream>,
        writer: WriteHalf<BoxedStream>,
        /// The task serving a duplex connection; a connection over a listener is served by the
        /// daemon's own accept loop and has none.
        serving: Option<tokio::task::JoinHandle<()>>,
    }

    async fn connect_to(daemon: &Arc<Daemon>, capacity: usize) -> Client {
        let (client, server) = tokio::io::duplex(capacity);
        let ticket = daemon.admit().expect("the daemon admits a connection");
        let daemon = Arc::clone(daemon);
        let serving = tokio::spawn(async move { daemon.serve(server, ticket).await });
        let client: BoxedStream = Box::pin(client);
        let (reader, writer) = tokio::io::split(client);
        Client {
            reader,
            writer,
            serving: Some(serving),
        }
    }

    /// A client over a real listener, so a test exercises the transport as a client would.
    async fn connect_over(endpoint: &crate::transport::Endpoint) -> Client {
        let stream = crate::transport::connect(endpoint.name()).await.unwrap();
        let stream: BoxedStream = Box::pin(stream);
        let (reader, writer) = tokio::io::split(stream);
        Client {
            reader,
            writer,
            serving: None,
        }
    }

    async fn connect(daemon: &Arc<Daemon>) -> Client {
        connect_to(daemon, 1 << 20).await
    }

    impl Client {
        async fn send(&mut self, message: &ToDaemon) {
            ipc::write_frame(&mut self.writer, message).await.unwrap();
        }

        /// The next frame. The bound is a failure bound only: every test waits for what it
        /// needs, and a frame that never comes fails here instead of hanging Cargo.
        async fn next(&mut self) -> Option<ToEndpoint> {
            timeout(
                Duration::from_secs(60),
                ipc::read_frame(&mut self.reader, ipc::RESPONSE_LIMIT),
            )
            .await
            .expect("a frame arrived")
            .unwrap()
        }

        async fn welcome(&mut self, session: &str, connection: u64) -> ToEndpoint {
            self.send(&hello(session, connection)).await;
            match self.next().await {
                Some(message @ ToEndpoint::Welcome { .. }) => message,
                other => panic!("expected a Welcome, got {other:?}"),
            }
        }

        /// The result of `id`, whatever progress frames came before it.
        async fn result(&mut self, id: u64) -> CallToolResult {
            loop {
                match self.next().await {
                    Some(ToEndpoint::Result { id: got, result }) if got == id => return result,
                    Some(ToEndpoint::Progress { .. }) => continue,
                    other => panic!("expected the result of call {id}, got {other:?}"),
                }
            }
        }
    }

    fn text_of(result: &CallToolResult) -> String {
        result
            .content
            .iter()
            .filter_map(|block| block.as_text().map(|text| text.text.clone()))
            .collect()
    }

    fn json_of(result: &CallToolResult) -> Value {
        result.structured_content.clone().unwrap_or(Value::Null)
    }

    /// Starts `command` and returns the call's structured result.
    async fn start(client: &mut Client, id: u64, command: &str) -> Value {
        client
            .send(&call(
                id,
                json!({
                    "action": "start", "command": command, "loginShell": false, "waitMs": 20000
                }),
            ))
            .await;
        let result = client.result(id).await;
        assert_ne!(result.is_error, Some(true), "{}", text_of(&result));
        json_of(&result)
    }

    /// The structured result of a `poll` that does not wait.
    async fn poll(client: &mut Client, id: u64, task_id: &str) -> Value {
        client
            .send(&call(
                id,
                json!({"action": "poll", "taskId": task_id, "waitMs": 0}),
            ))
            .await;
        json_of(&client.result(id).await)
    }

    /// Waits until `test` holds, so a test never guesses at a delay.
    async fn until(test: impl Fn() -> bool) {
        timeout(Duration::from_secs(20), async {
            while !test() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the condition held");
    }

    #[tokio::test]
    async fn a_hello_is_answered_with_the_epoch_or_a_reject_naming_the_field() {
        let fixture = fixture();
        let daemon = &fixture.daemon;
        let mut client = connect(daemon).await;
        let mut wrong = hello("s1", 1);
        if let ToDaemon::Hello { build, .. } = &mut wrong {
            *build = "9.9.9".to_owned();
        }
        client.send(&wrong).await;
        match client.next().await {
            Some(ToEndpoint::Reject { message }) => {
                assert!(message.contains("9.9.9"), "{message}");
                assert!(message.contains(env!("CARGO_PKG_VERSION")), "{message}");
            }
            other => panic!("expected a Reject, got {other:?}"),
        }
        assert_eq!(client.next().await, None, "the daemon said nothing more");

        let mut client = connect(daemon).await;
        let ToEndpoint::Welcome { epoch, .. } = client.welcome("s2", 1).await else {
            unreachable!("welcome returned a Welcome")
        };
        assert_eq!(epoch, daemon.tasks.epoch());
    }

    // shortcut: on Unix a task runs under the fastexec binary as its task host, which a unit-test
    // binary is not, so this case runs only where a task can start here. Block 9's two-endpoint
    // tests cover start, poll, and kill across sessions against the real binary on Linux; no
    // test-host override is added for it.
    #[cfg(windows)]
    #[tokio::test]
    async fn a_start_runs_in_the_hellos_environment_and_list_names_its_session_and_call() {
        let fixture = fixture();
        let daemon = &fixture.daemon;
        let mut client = connect(daemon).await;
        client
            .send(&hello_with(
                "sessabc123",
                1,
                "FASTEXEC_IPC_PROBE",
                "ipc-value",
            ))
            .await;
        assert!(matches!(
            client.next().await,
            Some(ToEndpoint::Welcome { .. })
        ));

        // The variable exists only in this Hello, so only the Hello's launch context can supply
        // it: a task that sees it runs in what the handshake carried.
        let started = start(&mut client, 1, "printf '%s' \"$FASTEXEC_IPC_PROBE\"").await;
        assert_eq!(started["output"], json!("ipc-value"));
        let task_id = started["taskId"].as_str().expect("a task ID").to_owned();

        // A second session whose first six characters match, starting its own call 1.
        let mut other = connect(daemon).await;
        other.welcome("sessabc999", 1).await;
        let second = start(&mut other, 1, "printf other").await;
        let second_id = second["taskId"].as_str().expect("a task ID").to_owned();

        client
            .send(&call(2, json!({"action": "list", "scope": "all"})))
            .await;
        let list = client.result(2).await;
        let entries = json_of(&list)["tasks"].clone();
        let entries = entries.as_array().expect("a task list");
        let entry = |id: &str| {
            entries
                .iter()
                .find(|entry| entry["taskId"] == json!(id.split_once('.').unwrap().0))
                .expect("the task is listed")
                .clone()
        };
        assert_eq!(entry(&task_id)["session"], json!("sessabc123"));
        assert_eq!(entry(&second_id)["session"], json!("sessabc999"));
        assert_eq!(entry(&task_id)["callId"], json!(1));
        assert_eq!(entry(&second_id)["callId"], json!(1));
        assert_eq!(entry(&task_id)["owner"], json!("client-sessab"));
        assert!(text_of(&list).contains(" call 1"), "{}", text_of(&list));
    }

    // shortcut: on Unix a task runs under the fastexec binary as its task host, which a unit-test
    // binary is not, so this case runs only where a task can start here. Block 9's two-endpoint
    // tests cover start, poll, and kill across sessions against the real binary on Linux; no
    // test-host override is added for it.
    #[cfg(windows)]
    #[tokio::test]
    async fn a_second_session_reads_a_task_from_the_beginning() {
        let fixture = fixture();
        let daemon = &fixture.daemon;
        let mut first = connect(daemon).await;
        first.welcome("aaa111", 1).await;
        let started = start(&mut first, 1, "printf hi").await;
        let task_id = started["taskId"].as_str().expect("a task ID").to_owned();
        // The start already read the output, so this session's next read sees nothing new.
        assert_eq!(poll(&mut first, 2, &task_id).await["output"], json!(""));

        let mut second = connect(daemon).await;
        second.welcome("bbb222", 1).await;
        assert_eq!(
            poll(&mut second, 1, &task_id).await["output"],
            json!("hi"),
            "a session that never read this task starts at its beginning"
        );
        // And the first session's cursor is where it was.
        assert_eq!(poll(&mut first, 3, &task_id).await["output"], json!(""));
    }

    // shortcut: on Unix a task runs under the fastexec binary as its task host, which a unit-test
    // binary is not, so this case runs only where a task can start here. Block 9's two-endpoint
    // tests cover start, poll, and kill across sessions against the real binary on Linux; no
    // test-host override is added for it.
    #[cfg(windows)]
    #[tokio::test]
    async fn a_reconnect_keeps_the_cursors_and_an_old_generation_is_refused() {
        let fixture = fixture();
        let daemon = &fixture.daemon;
        let mut first = connect(daemon).await;
        first.welcome("sess11", 1).await;
        let started = start(&mut first, 1, "printf hi").await;
        let task_id = started["taskId"].as_str().expect("a task ID").to_owned();
        // The connection goes away without a Bye.
        drop(first);

        let mut second = connect(daemon).await;
        second.welcome("sess11", 2).await;
        assert_eq!(
            poll(&mut second, 1, &task_id).await["output"],
            json!(""),
            "the session kept its cursors"
        );

        // Neither the generation before nor the same one may take the session.
        for connection in [1, 2] {
            let mut older = connect(daemon).await;
            older.send(&hello("sess11", connection)).await;
            match older.next().await {
                Some(ToEndpoint::Reject { message }) => assert!(
                    message.contains("is not newer than"),
                    "connection {connection}: {message}"
                ),
                other => panic!("expected a Reject, got {other:?}"),
            }
        }
        // And the connection that does hold the session still works.
        assert_eq!(poll(&mut second, 2, &task_id).await["output"], json!(""));
    }

    #[tokio::test]
    async fn a_new_connection_takes_the_session_and_closes_the_old_one() {
        let fixture = fixture();
        let daemon = &fixture.daemon;
        let mut first = connect(daemon).await;
        first.welcome("sess22", 1).await;
        // A wait is pending on the first connection when the second one arrives.
        first.send(&long_wait(1)).await;

        let mut second = connect(daemon).await;
        second.welcome("sess22", 2).await;

        // What the old connection is promised is that it ends, not that nothing was queued on it:
        // a progress frame or a cancelled write can still be in the pipe.
        let mut drained = Vec::new();
        timeout(
            Duration::from_secs(20),
            first.reader.read_to_end(&mut drained),
        )
        .await
        .expect("the replaced connection ended")
        .expect("the drained bytes end");
        // And the connection that took the session still works.
        second.send(&call(2, json!({"action": "list"}))).await;
        assert_eq!(json_of(&second.result(2).await)["action"], json!("list"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_call_on_a_closed_connection_queues_nothing() {
        let fixture = fixture();
        let daemon = &fixture.daemon;
        let session = Arc::new(Session::new(
            "closed01".to_owned(),
            "closed-01".to_owned(),
            Arc::new(crate::session::LaunchContext::new(
                std::env::temp_dir(),
                std::env::vars_os().collect(),
            )),
        ));
        let (outgoing, mut queued) = mpsc::unbounded_channel();
        // A connection that is already gone, with the budget ready: only the token decides.
        for id in 1..=32 {
            let connection = CancellationToken::new();
            connection.cancel();
            let in_flight = Arc::new(Mutex::new(HashMap::new()));
            run_call(
                Arc::clone(daemon),
                Arc::clone(&session),
                Render {
                    output_mode: OutputMode::Text,
                    structured_content: true,
                },
                connection,
                CancellationToken::new(),
                id,
                "exec".to_owned(),
                json!({"action": "list"}),
                Arc::clone(&in_flight),
                outgoing.clone(),
            )
            .await;
            assert!(
                queued.try_recv().is_err(),
                "call {id} queued a result on a closed connection"
            );
        }
    }

    // shortcut: on Unix a task runs under the fastexec binary as its task host, which a unit-test
    // binary is not, so this case runs only where a task can start here. Block 9's two-endpoint
    // tests cover start, poll, and kill across sessions against the real binary on Linux; no
    // test-host override is added for it.
    #[cfg(windows)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_stop_ends_in_flight_waits_with_eof_and_no_result() {
        let fixture = fixture();
        let daemon = &fixture.daemon;
        let mut client = connect(daemon).await;
        client.welcome("sess55", 1).await;
        // One task whose waits stay pending, and eight polls of it in flight on this connection.
        client
            .send(&call(
                1,
                json!({
                    "action": "start", "command": "sleep 30", "loginShell": false, "waitMs": 0
                }),
            ))
            .await;
        let started = json_of(&client.result(1).await);
        let task_id = started["taskId"].as_str().expect("a task ID").to_owned();
        for id in 2..10 {
            client
                .send(&call(
                    id,
                    json!({"action": "poll", "taskId": task_id, "waitMs": 30000}),
                ))
                .await;
        }
        // The reader dispatches in order, so a list answered now proves all eight polls were
        // admitted before the stop rather than still sitting in the socket.
        client.send(&call(10, json!({"action": "list"}))).await;
        let _ = client.result(10).await;
        // A stop kills the task and ends every connection: each wait answers with nothing at all,
        // never with a result about a task the stop itself killed.
        daemon.stop();
        loop {
            match client.next().await {
                Some(ToEndpoint::Result { id, result }) => {
                    panic!(
                        "a stopped daemon answered call {id}: {:?}",
                        text_of(&result)
                    );
                }
                Some(ToEndpoint::Progress { .. }) => continue,
                Some(other) => panic!("unexpected frame from a stopped daemon: {other:?}"),
                None => break,
            }
        }
    }

    // shortcut: on Unix a task runs under the fastexec binary as its task host, which a unit-test
    // binary is not, so this case runs only where a task can start here. Block 9's two-endpoint
    // tests cover start, poll, and kill across sessions against the real binary on Linux; no
    // test-host override is added for it.
    #[cfg(windows)]
    #[tokio::test]
    async fn a_cancel_answers_the_wait_and_leaves_the_task_running() {
        let fixture = fixture();
        let daemon = &fixture.daemon;
        let mut client = connect(daemon).await;
        client.welcome("sess33", 1).await;
        client.send(&long_wait(1)).await;
        client.send(&ToDaemon::Cancel { id: 1 }).await;
        let result = client.result(1).await;
        // Whichever wait was running says it the same way: cancelled, and the task lives on.
        let cancelled = text_of(&result);
        assert!(cancelled.contains("Cancelled"), "{cancelled}");
        assert!(cancelled.contains("the task keeps running."), "{cancelled}");

        client.send(&call(2, json!({"action": "list"}))).await;
        let list = json_of(&client.result(2).await);
        assert_eq!(list["tasks"][0]["state"], json!("running"));
    }

    // shortcut: on Unix a task runs under the fastexec binary as its task host, which a unit-test
    // binary is not, so this case runs only where a task can start here. Block 9's two-endpoint
    // tests cover start, poll, and kill across sessions against the real binary on Linux; no
    // test-host override is added for it.
    #[cfg(windows)]
    #[tokio::test]
    async fn bye_ends_the_session_so_the_next_connection_starts_fresh() {
        let fixture = fixture();
        let daemon = &fixture.daemon;
        let mut first = connect(daemon).await;
        first.welcome("sess44", 1).await;
        let started = start(&mut first, 1, "printf hi").await;
        let task_id = started["taskId"].as_str().expect("a task ID").to_owned();
        first.send(&ToDaemon::Bye).await;
        // The daemon is done with this connection once its stream ends.
        assert_eq!(
            timeout(Duration::from_secs(20), first.next())
                .await
                .expect("the connection ended"),
            None
        );

        let mut second = connect(daemon).await;
        second.welcome("sess44", 2).await;
        assert_eq!(
            poll(&mut second, 1, &task_id).await["output"],
            json!("hi"),
            "a Bye left no session behind, so this one reads from the beginning"
        );
    }

    // shortcut: on Unix a task runs under the fastexec binary as its task host, which a unit-test
    // binary is not, so this case runs only where a task can start here. Block 9's two-endpoint
    // tests cover start, poll, and kill across sessions against the real binary on Linux; no
    // test-host override is added for it.
    #[cfg(windows)]
    #[tokio::test]
    async fn the_connection_runs_at_most_32_calls_and_refuses_a_duplicate_id() {
        let fixture = fixture();
        let daemon = &fixture.daemon;
        let mut client = connect(daemon).await;
        client.welcome("sess55", 1).await;
        // One task, and 32 waits on it: what is counted is the connection's calls. The start does
        // not wait, so the task is alive for the whole test.
        client
            .send(&call(
                1,
                json!({"action": "start", "command": "sleep 30", "loginShell": false, "waitMs": 0}),
            ))
            .await;
        let started = json_of(&client.result(1).await);
        let task_id = started["taskId"].as_str().expect("a task ID").to_owned();
        for id in 2..=MAX_IN_FLIGHT as u64 + 1 {
            client
                .send(&call(
                    id,
                    json!({"action": "poll", "taskId": task_id, "waitMs": 30000}),
                ))
                .await;
        }
        // One more call than the connection may run, then an ID already in flight.
        client.send(&call(100, json!({"action": "list"}))).await;
        client.send(&call(2, json!({"action": "list"}))).await;

        let mut refused = HashMap::new();
        while refused.len() < 2 {
            match client.next().await {
                Some(ToEndpoint::Result { id, result }) => {
                    refused.entry(id).or_insert_with(|| text_of(&result));
                }
                Some(ToEndpoint::Progress { .. }) => {}
                other => panic!("expected a refusal, got {other:?}"),
            }
        }
        assert_eq!(
            refused[&100],
            format!("{MAX_IN_FLIGHT} calls are already in flight on this connection.")
        );
        assert_eq!(refused[&2], "Call 2 is already in flight.");

        for id in 2..=MAX_IN_FLIGHT as u64 + 1 {
            client.send(&ToDaemon::Cancel { id }).await;
        }
        let mut cancelled = 0;
        while cancelled < MAX_IN_FLIGHT {
            match client.next().await {
                Some(ToEndpoint::Result { id, .. })
                    if (2..=MAX_IN_FLIGHT as u64 + 1).contains(&id) =>
                {
                    cancelled += 1;
                }
                Some(ToEndpoint::Result { .. }) | Some(ToEndpoint::Progress { .. }) => {}
                other => panic!("expected the cancelled waits, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn a_kill_or_list_result_uses_the_reserved_share_and_others_wait_for_the_budget() {
        let fixture = fixture();
        let daemon = &fixture.daemon;
        let mut client = connect(daemon).await;
        client.welcome("sess88", 1).await;
        // Hold the whole budget without allocating a byte of it.
        let held = Arc::clone(&daemon.budget)
            .acquire_many_owned((BUDGET - RESERVED) as u32)
            .await
            .expect("the budget is free");

        // An action that does not exist fails at once, and its error result takes the budget,
        // which is held, so it waits.
        client
            .send(&call(1, json!({"action": "no-such-action"})))
            .await;
        // A list result takes the reserved share, so it goes out while the other waits.
        client.send(&call(2, json!({"action": "list"}))).await;
        match client.next().await {
            Some(ToEndpoint::Result { id, .. }) => {
                assert_eq!(id, 2, "the reserved share carries list past a held budget")
            }
            other => panic!("expected the list result, got {other:?}"),
        }
        drop(held);
        match client.next().await {
            Some(ToEndpoint::Result { id, result }) => {
                assert_eq!(id, 1, "the held budget released its call");
                assert!(text_of(&result).contains("no-such-action"), "{result:?}");
            }
            other => panic!("expected the waiting result, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn held_render_slots_leave_a_receipt_and_do_not_block_kill_or_list() {
        let fixture = fixture();
        let daemon = &fixture.daemon;
        let mut client = connect(daemon).await;
        client.welcome("sessrender", 1).await;
        let held = Arc::clone(&daemon.render)
            .acquire_many_owned(RENDER_SLOTS as u32)
            .await
            .unwrap();

        let requests = [
            #[cfg(windows)]
            (
                "exec",
                json!({"action": "start", "command": "printf render-slot-test", "waitMs": 5000}),
            ),
            (
                "codemode",
                json!({"code": r#"const x = await tools.exec({action: "list"}); return Array.isArray(x.tasks) ? "render-slot-test" : "list failed";"#, "waitMs": 5000}),
            ),
        ];
        for (index, (tool, arguments)) in requests.into_iter().enumerate() {
            let call_id = (index as u64) * 4 + 1;
            client
                .send(&ToDaemon::Call {
                    id: call_id,
                    tool: tool.into(),
                    request_id: call_id.to_string(),
                    arguments,
                })
                .await;
            let receipt = client.result(call_id).await;
            assert_eq!(receipt.is_error, Some(true));
            let structured = json_of(&receipt);
            let receipt = text_of(&receipt);
            assert_eq!(structured["error"], receipt);
            assert_eq!(
                structured["action"],
                if tool == "exec" { "start" } else { tool }
            );
            let mut lines = receipt.lines();
            assert!(
                lines
                    .next()
                    .unwrap()
                    .contains("The task exists and may still be running"),
                "{tool}: {receipt}"
            );
            let handle = lines.next().unwrap().strip_prefix("taskId: ").unwrap();
            let path = lines.next().unwrap().strip_prefix("logPath: ").unwrap();
            assert!(receipt.contains("Its output was not read"));
            assert!(
                lines
                    .next()
                    .unwrap()
                    .contains("Do not replay the original operation")
            );

            client
                .send(&call(call_id + 1, json!({"action": "list"})))
                .await;
            let listed = json_of(&client.result(call_id + 1).await);
            let (task_id, _) = handle.split_once('.').unwrap();
            let task = listed["tasks"]
                .as_array()
                .unwrap()
                .iter()
                .find(|entry| entry["taskId"] == task_id)
                .expect("task is listed");
            assert_eq!(task["logPath"], path);

            client
                .send(&call(
                    call_id + 3,
                    json!({"action": "poll", "taskId": handle, "waitMs": 0}),
                ))
                .await;
            let poll_receipt = client.result(call_id + 3).await;
            assert_eq!(poll_receipt.is_error, Some(true));
            assert!(text_of(&poll_receipt).contains("Do not resend input, keys, or resize."));

            // Read the same handle through the separate kill lane. The cursor was not advanced,
            // and a codemode callback ran even though the ordinary render slots were held.
            client
                .send(&call(
                    call_id + 2,
                    json!({"action": "kill", "taskId": handle}),
                ))
                .await;
            let killed = json_of(&client.result(call_id + 2).await);
            assert_eq!(killed["action"], "kill");
            assert!(
                killed["output"]
                    .as_str()
                    .unwrap()
                    .contains("render-slot-test"),
                "{killed:?}"
            );
        }
        drop(held);
    }

    #[tokio::test]
    async fn cancelling_waiting_file_work_does_not_dispatch_it() {
        let fixture = fixture();
        let daemon = &fixture.daemon;
        let mut client = connect(daemon).await;
        client.welcome("sesscancelwrite", 1).await;
        let held = Arc::clone(&daemon.render)
            .acquire_many_owned(RENDER_SLOTS as u32)
            .await
            .unwrap();
        let path = std::env::temp_dir().join(format!(
            "fastexec-cancel-render-write-{}-{}.txt",
            std::process::id(),
            crate::tasks::random_chars(12).unwrap(),
        ));
        assert!(!path.exists());
        client
            .send(&ToDaemon::Call {
                id: 1,
                tool: "write".into(),
                request_id: "1".into(),
                arguments: json!({"path": path, "content": "not written"}),
            })
            .await;
        client.send(&ToDaemon::Cancel { id: 1 }).await;
        let result = client.result(1).await;
        assert_eq!(result.is_error, Some(true));
        assert_eq!(
            text_of(&result),
            "Cancelled before the operation started; nothing was executed."
        );
        assert!(!path.exists());
        drop(held);

        client.send(&call(2, json!({"action": "list"}))).await;
        assert_eq!(json_of(&client.result(2).await)["action"], "list");
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn the_materialization_slot_is_not_released_while_detached_work_can_keep_its_value() {
        let pool = Arc::new(Semaphore::new(1));
        let slot = RenderSlot::new(&pool);
        let permit = slot.hold().await;
        let (start, started) = oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let _ = start.send(());
            wait.recv_timeout(Duration::from_secs(10))
                .expect("the test releases the worker");
            (String::from("rendered output"), permit)
        });

        started.await.unwrap();
        drop(slot);
        assert!(pool.try_acquire().is_err());
        release.send(()).unwrap();
        let value = worker.await.unwrap();
        assert!(pool.try_acquire().is_err());
        drop(value);
        assert!(pool.try_acquire().is_ok());
    }

    #[tokio::test]
    async fn a_reader_refusal_with_no_reserved_room_closes_the_connection() {
        let fixture = fixture();
        let daemon = &fixture.daemon;
        let mut client = connect(daemon).await;
        client.welcome("sess99", 1).await;
        // Hold both shares, and keep one call waiting on the budget.
        let budget = Arc::clone(&daemon.budget)
            .acquire_many_owned((BUDGET - RESERVED) as u32)
            .await
            .expect("the budget is free");
        let reserved = Arc::clone(&daemon.reserved)
            .acquire_many_owned(RESERVED as u32)
            .await
            .expect("the reserved share is free");
        client
            .send(&call(1, json!({"action": "no-such-action"})))
            .await;
        // A duplicate ID is refused by the reader itself, which cannot wait for room.
        client.send(&call(1, json!({"action": "list"}))).await;
        assert_eq!(
            timeout(Duration::from_secs(20), client.next())
                .await
                .expect("the connection closed"),
            None,
            "a refusal with no room closes the connection"
        );
        drop((budget, reserved));
    }

    #[tokio::test]
    async fn a_stalled_write_closes_the_connection() {
        let fixture = fixture_with_write_stall(Duration::from_millis(200));
        let daemon = &fixture.daemon;
        let mut client = connect_to(daemon, 64).await;
        // The Welcome is read, so the handshake is not what stalls.
        client.welcome("sess66", 1).await;
        // From here the client does not read, so the daemon's writes back up behind 64 bytes.
        for id in 1..=4 {
            client.send(&call(id, json!({"action": "list"}))).await;
        }
        // The connection is closed before the client reads anything, which is what makes the
        // stall the cause: reading first could let the blocked write through.
        let serving = client
            .serving
            .take()
            .expect("a duplex connection is served");
        timeout(Duration::from_secs(20), serving)
            .await
            .expect("the daemon closed the stalled connection")
            .expect("the connection handler ended cleanly");

        // What the client can still read is whatever fit in the pipe; the rest is gone.
        let mut drained = Vec::new();
        client
            .reader
            .read_to_end(&mut drained)
            .await
            .expect("the drained bytes end");
    }

    #[tokio::test]
    async fn an_aborted_serve_task_detaches_the_session() {
        let fixture = fixture();
        let daemon = &fixture.daemon;
        // A pipe small enough that the Welcome cannot be written while nobody reads it, so the
        // serving task is still inside the handshake when it is aborted.
        let mut client = connect_to(daemon, 64).await;
        client.send(&hello("abort1", 1)).await;
        until(|| lock(&daemon.sessions).contains_key("abort1")).await;

        let serving = client
            .serving
            .take()
            .expect("a duplex connection is served");
        serving.abort();
        let _ = serving.await;

        // The guard closed the attachment: nothing is live, and the entry is sweepable.
        let sessions = lock(&daemon.sessions);
        let entry = sessions.get("abort1").expect("the entry is still there");
        assert!(entry.live.is_none(), "the session was left attached");
        assert!(
            entry.detached_at.is_some(),
            "the session was never detached"
        );
        drop(sessions);
        daemon.sweep_sessions(Instant::now() + SESSION_TTL);
        assert!(!lock(&daemon.sessions).contains_key("abort1"));
    }

    #[tokio::test]
    async fn a_frame_header_over_the_limit_closes_the_connection() {
        let fixture = fixture();
        let daemon = &fixture.daemon;
        let mut client = connect(daemon).await;
        client.welcome("sess77", 1).await;
        // A length the daemon will not read, and nothing behind it.
        let length = u32::try_from(ipc::REQUEST_LIMIT).unwrap() + 1;
        client
            .writer
            .write_all(&length.to_le_bytes())
            .await
            .unwrap();
        client.writer.flush().await.unwrap();
        assert_eq!(
            timeout(Duration::from_secs(20), client.next())
                .await
                .expect("the connection closed"),
            None
        );
    }

    #[tokio::test]
    async fn sweep_drops_a_session_detached_past_the_ttl_and_keeps_an_attached_one() {
        let fixture = fixture();
        let daemon = &fixture.daemon;
        let mut gone = connect(daemon).await;
        gone.welcome("gone11", 1).await;
        drop(gone);
        let mut kept = connect(daemon).await;
        kept.welcome("kept22", 1).await;
        until(|| {
            lock(&daemon.sessions)
                .get("gone11")
                .is_some_and(|entry| entry.detached_at.is_some())
        })
        .await;

        daemon.sweep_sessions(Instant::now());
        assert_eq!(lock(&daemon.sessions).len(), 2, "a fresh detach stays");
        daemon.sweep_sessions(Instant::now() + SESSION_TTL);
        let sessions = lock(&daemon.sessions);
        assert!(
            !sessions.contains_key("gone11"),
            "the detached session was dropped"
        );
        assert!(
            sessions.contains_key("kept22"),
            "the attached session stays"
        );
        drop(sessions);
    }
    /// Runs `daemon` on `endpoint`, so the test talks to it over the transport as a client does.
    fn run_on(daemon: &Arc<Daemon>, endpoint: &Endpoint) -> tokio::task::JoinHandle<()> {
        let listener = Listener::bind(endpoint.name()).unwrap();
        tokio::spawn(Arc::clone(daemon).run(listener, None))
    }

    #[tokio::test]
    async fn two_clients_handshake_over_the_listener() {
        let fixture = fixture();
        let daemon = Arc::clone(&fixture.daemon);
        let endpoint = Endpoint::new("two-clients");
        let running = run_on(&daemon, &endpoint);

        let mut first = connect_over(&endpoint).await;
        first.welcome("first1", 1).await;
        // The second connection proves the listener had its next instance ready.
        let mut second = connect_over(&endpoint).await;
        second.welcome("second", 1).await;
        for (client, id) in [(&mut first, 1), (&mut second, 2)] {
            client.send(&call(id, json!({"action": "list"}))).await;
            let result = client.result(id).await;
            assert_ne!(result.is_error, Some(true), "{}", text_of(&result));
        }

        daemon.stop();
        timeout(Duration::from_secs(10), running)
            .await
            .expect("the daemon stopped")
            .unwrap();
    }

    #[tokio::test]
    async fn the_daemon_admits_max_connections_and_refuses_the_next() {
        let fixture = fixture();
        let daemon = Arc::clone(&fixture.daemon);
        let endpoint = Endpoint::new("cap");
        let running = run_on(&daemon, &endpoint);

        let mut clients = Vec::new();
        for index in 0..MAX_CONNECTIONS {
            let mut client = connect_over(&endpoint).await;
            client.welcome(&format!("cap{index}"), 1).await;
            clients.push(client);
        }

        // The connection past the cap is accepted and dropped without a word.
        let mut refused = connect_over(&endpoint).await;
        assert_eq!(
            refused.next().await,
            None,
            "a connection past the cap was welcomed"
        );

        // One place comes free, and the next connection takes it.
        clients.pop();
        until(|| {
            let gate = lock(&daemon.gate);
            gate.live + gate.handshaking < MAX_CONNECTIONS
        })
        .await;
        let mut admitted = connect_over(&endpoint).await;
        admitted.welcome("after", 1).await;

        daemon.stop();
        timeout(Duration::from_secs(30), running)
            .await
            .expect("the daemon stopped")
            .unwrap();
    }

    #[tokio::test]
    async fn an_idle_daemon_exits_on_its_own() {
        let fixture = fixture_with_config(Config {
            tick: Duration::from_millis(50),
            idle_after: Duration::from_millis(200),
            idle_exit: true,
            ..Config::default()
        });
        let daemon = Arc::clone(&fixture.daemon);
        let endpoint = Endpoint::new("idle-exit");
        let running = run_on(&daemon, &endpoint);

        let mut client = connect_over(&endpoint).await;
        client.welcome("idle01", 1).await;
        // A connected client keeps the daemon up well past `idle_after`.
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(
            !running.is_finished(),
            "the daemon exited while a client was connected"
        );

        client.send(&ToDaemon::Bye).await;
        timeout(Duration::from_secs(5), running)
            .await
            .expect("the idle daemon exited")
            .unwrap();
        assert!(
            crate::transport::connect(endpoint.name()).await.is_err(),
            "the endpoint still accepts after the daemon exited"
        );
    }

    #[tokio::test]
    async fn a_live_transfer_keeps_an_idle_exit_daemon_running() {
        let fixture = fixture_with_config(Config {
            tick: Duration::from_millis(50),
            idle_after: Duration::from_millis(200),
            idle_exit: true,
            ..Config::default()
        });
        let daemon = Arc::clone(&fixture.daemon);
        daemon
            .transfers()
            .create(std::env::temp_dir().join("pinned.bin"))
            .unwrap();
        let created = Instant::now();
        assert!(!daemon.idle_expired(created));
        assert!(!daemon.idle_expired(created + Duration::from_secs(30)));
        let expired = created + crate::transfer::IDLE;
        // The first check after the capability expires starts the daemon's own idle window.
        assert!(!daemon.idle_expired(expired));
        assert!(daemon.idle_expired(expired + Duration::from_millis(200)));
    }

    #[tokio::test]
    async fn transfer_checks_disabled_ipc_target_before_bind_ip_conflicts_and_unspecified_addrs() {
        let fixture = fixture();
        let daemon = Arc::clone(&fixture.daemon);
        let dir = std::env::temp_dir().join(format!(
            "fastexec-daemon-transfer-unit-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let no_sc_render = Render {
            output_mode: OutputMode::Text,
            structured_content: false,
        };
        let plain_launch = Arc::new(LaunchContext::new(dir.clone(), Vec::new()));
        let plain_session = Session::new("u1".into(), "u1".into(), Arc::clone(&plain_launch));
        let (outgoing, mut queued) = mpsc::unbounded_channel();

        // Raw IPC `"transfer"` on a session without `FASTEXEC_TRANSFER=true` is refused via `transfer_error`.
        run_call(
            Arc::clone(&daemon),
            Arc::clone(&plain_session),
            no_sc_render,
            CancellationToken::new(),
            CancellationToken::new(),
            1,
            "transfer".into(),
            json!({"path": "file.bin"}),
            Arc::new(Mutex::new(HashMap::new())),
            outgoing.clone(),
        )
        .await;
        let disabled_frame = queued.recv().await.unwrap();
        let mut cursor = std::io::Cursor::new(disabled_frame.bytes);
        let Some(ToEndpoint::Result { result, .. }) =
            ipc::read_frame::<_, ToEndpoint>(&mut cursor, ipc::RESPONSE_LIMIT)
                .await
                .unwrap()
        else {
            panic!("expected Result frame");
        };
        assert_eq!(result.is_error, Some(true));
        assert!(
            result.structured_content.is_none(),
            "FASTEXEC_STRUCTURED_CONTENT=false suppresses structured_content"
        );
        assert_eq!(
            result.content,
            vec![ContentBlock::text(
                "transfer is disabled; set FASTEXEC_TRANSFER=true."
            )]
        );
        assert!(daemon.transfer_listener.get().is_none());

        // Malformed arguments on an enabled session also honor `render` without binding `/t/`.
        let enabled_launch = Arc::new(LaunchContext::new(
            dir.clone(),
            vec![("FASTEXEC_TRANSFER".into(), "true".into())],
        ));
        let enabled_session = Session::new("u2".into(), "u2".into(), Arc::clone(&enabled_launch));
        run_call(
            Arc::clone(&daemon),
            Arc::clone(&enabled_session),
            no_sc_render,
            CancellationToken::new(),
            CancellationToken::new(),
            2,
            "transfer".into(),
            json!({"unexpected": 1}),
            Arc::new(Mutex::new(HashMap::new())),
            outgoing,
        )
        .await;
        let bad_args_frame = queued.recv().await.unwrap();
        let mut cursor = std::io::Cursor::new(bad_args_frame.bytes);
        let Some(ToEndpoint::Result { result, .. }) =
            ipc::read_frame::<_, ToEndpoint>(&mut cursor, ipc::RESPONSE_LIMIT)
                .await
                .unwrap()
        else {
            panic!("expected Result frame");
        };
        assert_eq!(result.is_error, Some(true));
        assert!(result.structured_content.is_none());
        assert!(
            result.content[0]
                .as_text()
                .unwrap()
                .text
                .starts_with("failed to deserialize parameters:")
        );
        assert!(daemon.transfer_listener.get().is_none());

        let text_render = Render {
            output_mode: OutputMode::Text,
            structured_content: true,
        };
        // Passing a directory fails before `ensure_transfer_listener` binds any port.
        let dir_result = daemon
            .transfer(
                crate::transfer::Request { path: ".".into() },
                &enabled_launch,
                text_render,
            )
            .await;
        assert_eq!(dir_result.is_error, Some(true));
        assert!(
            daemon.transfer_listener.get().is_none(),
            "directory target must not bind /t/ listener"
        );

        // Unspecified `0.0.0.0:0` without `FASTEXEC_PUBLIC_URL` fails before binding.
        let unspec_no_root = Arc::new(LaunchContext::new(
            dir.clone(),
            vec![
                ("FASTEXEC_TRANSFER".into(), "true".into()),
                ("FASTEXEC_HTTP_ALLOW_REMOTE".into(), "1".into()),
                ("FASTEXEC_TRANSFER_ADDR".into(), "0.0.0.0:0".into()),
            ],
        ));
        let unspec_err = daemon
            .transfer(
                crate::transfer::Request {
                    path: "ok.bin".into(),
                },
                &unspec_no_root,
                text_render,
            )
            .await;
        assert_eq!(unspec_err.is_error, Some(true));
        assert!(
            unspec_err.structured_content.as_ref().unwrap()["error"]
                .as_str()
                .unwrap()
                .contains("is an unspecified address; set FASTEXEC_PUBLIC_URL")
        );
        assert!(daemon.transfer_listener.get().is_none());

        // Valid mint binds `127.0.0.1:<port>`, and subsequent `0.0.0.0:0` (even with port 0 and
        // `FASTEXEC_PUBLIC_URL`) is rejected due to IP mismatch.
        let minted = daemon
            .transfer(
                crate::transfer::Request {
                    path: "ok.bin".into(),
                },
                &enabled_launch,
                text_render,
            )
            .await;
        assert_eq!(minted.is_error, Some(false));
        let bound = *daemon.transfer_listener.get().expect("listener bound");
        assert_eq!(bound.ip(), std::net::Ipv4Addr::LOCALHOST);

        let conflict_launch = Arc::new(LaunchContext::new(
            dir.clone(),
            vec![
                ("FASTEXEC_TRANSFER".into(), "true".into()),
                ("FASTEXEC_HTTP_ALLOW_REMOTE".into(), "1".into()),
                ("FASTEXEC_TRANSFER_ADDR".into(), "0.0.0.0:0".into()),
                (
                    "FASTEXEC_PUBLIC_URL".into(),
                    "https://files.example/".into(),
                ),
            ],
        ));
        let conflict = daemon
            .transfer(
                crate::transfer::Request {
                    path: "other.bin".into(),
                },
                &conflict_launch,
                text_render,
            )
            .await;
        assert_eq!(conflict.is_error, Some(true));
        assert_eq!(
            conflict.structured_content.as_ref().unwrap()["error"],
            json!(format!(
                "the daemon already serves /t/ on {bound}, not 0.0.0.0:0; stop it first"
            ))
        );

        daemon.stop();

        // `--http 0.0.0.0:<port>` without `FASTEXEC_PUBLIC_URL` refuses stdio `Daemon::transfer`.
        let http_wildcard = Daemon::with_config(
            Arc::clone(&daemon.tasks),
            "fastexec".into(),
            env!("CARGO_PKG_VERSION").into(),
            Some("0.0.0.0:8765".into()),
            None,
            Config::default(),
        );
        let wildcard_err = http_wildcard
            .transfer(
                crate::transfer::Request {
                    path: "ok.bin".into(),
                },
                &enabled_launch,
                text_render,
            )
            .await;
        assert_eq!(wildcard_err.is_error, Some(true));
        assert_eq!(
            wildcard_err.structured_content.as_ref().unwrap()["error"],
            json!(
                "--http 0.0.0.0:8765 is an unspecified address; set FASTEXEC_PUBLIC_URL to the URL clients reach"
            )
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_default_daemon_runs_until_it_is_stopped() {
        let fixture = fixture_with_config(Config {
            tick: Duration::from_millis(50),
            idle_after: Duration::from_millis(100),
            ..Config::default()
        });
        let daemon = Arc::clone(&fixture.daemon);
        let endpoint = Endpoint::new("idle-off");
        let running = run_on(&daemon, &endpoint);

        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(
            !running.is_finished(),
            "the daemon exited although idleness is not a reason to"
        );

        daemon.stop();
        timeout(Duration::from_secs(5), running)
            .await
            .expect("the daemon stopped")
            .unwrap();
    }

    #[tokio::test]
    async fn stopping_ends_a_connected_client_and_one_still_handshaking() {
        let fixture = fixture();
        let daemon = Arc::clone(&fixture.daemon);
        let endpoint = Endpoint::new("stopping");
        let running = run_on(&daemon, &endpoint);

        let mut client = connect_over(&endpoint).await;
        client.welcome("stop01", 1).await;
        // A second connection says nothing, so it is still inside its handshake.
        let mut silent = connect_over(&endpoint).await;
        until(|| lock(&daemon.gate).handshaking == 1).await;

        daemon.stop();
        // Both end at once: the silent one well before its 10 s handshake timeout would have.
        timeout(Duration::from_secs(5), async {
            assert_eq!(
                client.next().await,
                None,
                "the stopped daemon ended the connection"
            );
            assert_eq!(silent.next().await, None, "the stop left a handshake open");
        })
        .await
        .expect("the stop ended both connections");

        timeout(Duration::from_secs(5), running)
            .await
            .expect("the daemon stopped")
            .unwrap();
        let gate = lock(&daemon.gate);
        assert_eq!(
            (gate.handshaking, gate.live),
            (0, 0),
            "the gate still counts a connection"
        );
    }

    #[tokio::test]
    async fn admitting_a_connection_restarts_the_idle_interval() {
        let fixture = fixture_with_config(Config {
            idle_after: Duration::from_millis(200),
            idle_exit: true,
            ..Config::default()
        });
        let daemon = &fixture.daemon;
        let start = Instant::now();
        // Idle from the first look, so the interval starts there.
        assert!(!daemon.idle_expired(start));
        // A connection that comes and goes between two ticks restarts the interval.
        let ticket = daemon.admit().expect("the daemon admits a connection");
        drop(ticket);
        let later = start + Duration::from_millis(200);
        assert!(
            !daemon.idle_expired(later),
            "the idle interval did not restart at the admission"
        );
        // A full `idle_after` after that admission, the daemon is idle long enough.
        assert!(daemon.idle_expired(later + Duration::from_millis(200)));
    }
}
