use super::events::{self, ActiveEventStream, EventStreamRegistration, EventStreams};
use super::wire::Outgoing;
use crate::env::LaunchContext;
use crate::ipc;
use crate::lease::{self, BytePool};
use crate::render::files::{mint_transfer, transfer_error};
use crate::render::types::TrackedResult;
use crate::service::Service;
use crate::tasks::Tasks;
use crate::tasks::session::Session;
use crate::transfer::Transfers;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::sync::{Semaphore, mpsc};
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
pub(super) const RENDER_SLOTS: usize = 2;

/// Streamable HTTP launch contract passed to the top-level CLI spawner.
pub struct HttpLaunch {
    pub listener: tokio::net::TcpListener,
    pub digests: Vec<[u8; 32]>,
    pub tools: crate::config::Tools,
    pub launch: Arc<LaunchContext>,
    pub public_root: Option<Arc<str>>,
    pub extra_instructions: Option<String>,
    pub instructions_tool: crate::config::InstructionsToolMode,
    pub skills: Option<Arc<crate::skills::SkillCatalog>>,
    pub service: Arc<Service>,
    pub transfers: Arc<Transfers>,
    pub memory: Arc<BytePool>,
    pub control_memory: Arc<BytePool>,
    pub stopped: CancellationToken,
}

/// Pre-bound HTTP front-door configuration collected before daemon state exists.
pub(crate) struct PendingHttp {
    pub listener: tokio::net::TcpListener,
    pub digests: Vec<[u8; 32]>,
    pub tools: crate::config::Tools,
    pub launch: Arc<LaunchContext>,
    pub public_root: Option<Arc<str>>,
    pub extra_instructions: Option<String>,
    pub instructions_tool: crate::config::InstructionsToolMode,
    pub skills: Option<Arc<crate::skills::SkillCatalog>>,
}

struct StopOnDrop(Arc<Daemon>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.stop();
    }
}

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

/// The daemon: the session table and the result budget, shared by every connection.
pub struct Daemon {
    pub(super) service: Arc<Service>,
    pub(super) tasks: Arc<Tasks>,
    pub(super) namespace: String,
    pub(super) build: String,
    /// The address it serves Streamable HTTP on, when it serves it: `Status` reports it, and the
    /// listener it names is the one `run` was given.
    pub(super) http: Option<String>,
    pub(super) transfers: Arc<Transfers>,
    pub(super) public_root: Option<Arc<str>>,
    pub(super) transfer_listener: tokio::sync::OnceCell<std::net::SocketAddr>,
    pub(super) sessions: Mutex<HashMap<String, Entry>>,
    /// Active byte pool shared by ordinary outbound frames, media reads, and codemode packing.
    pub(super) memory: Arc<BytePool>,
    /// Independent 8 MiB control-plane byte pool shared with HTTP sessions.
    pub(super) control_memory: Arc<BytePool>,
    /// Independent control-plane permits for `kill`, `list`, and overload error receipts.
    pub(super) reserved: Arc<Semaphore>,
    pub(super) render: Arc<Semaphore>,
    pub(super) render_kill: Arc<Semaphore>,
    pub(super) render_list: Arc<Semaphore>,
    pub(super) config: Config,
    /// Who is admitted, and whether the daemon is stopping.
    pub(super) gate: Mutex<Gate>,
    /// Cancelled when the daemon stops, so the accept loop ends.
    pub(super) stopped: CancellationToken,
}

/// Admission and lifetime: how many connections are handshaking or live, whether the daemon is
/// stopping, and since when it has been idle.
#[derive(Default)]
pub(super) struct Gate {
    pub(super) handshaking: usize,
    pub(super) live: usize,
    pub(super) emit_live: usize,
    pub(super) stopping: bool,
    pub(super) idle_since: Option<Instant>,
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
    pub(super) fn promote_emit(&mut self) -> bool {
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

/// One session in the table.
pub(super) struct Entry {
    pub(super) session: Arc<Session>,
    /// The generation of the connection that last attached; it only ever increases.
    pub(super) connection: u64,
    /// That connection's token, while one is attached.
    pub(super) live: Option<CancellationToken>,
    /// When the last connection let go, if none holds the session now.
    pub(super) detached_at: Option<Instant>,
}

/// A connection's hold on its session. Dropping it closes the connection, so no way out of
/// serving one — including a panic or the task being aborted — leaves a session attached to
/// nothing.
pub(super) struct Attachment {
    pub(super) daemon: Arc<Daemon>,
    pub(super) attached: Attached,
}

impl Drop for Attachment {
    fn drop(&mut self) {
        self.daemon.close(&self.attached);
    }
}

/// What a connection holds after its handshake.
pub(super) struct Attached {
    pub(super) session: Arc<Session>,
    pub(super) session_id: String,
    pub(super) connection: u64,
    pub(super) token: CancellationToken,
}

impl Daemon {
    pub(super) fn start_event_stream(
        self: &Arc<Self>,
        event_streams: &EventStreams,
        session_id: &str,
        closing: &CancellationToken,
        outgoing: &mpsc::UnboundedSender<Outgoing>,
        id: u64,
        request: crate::lease::Owned<crate::events::SubscribeRequest>,
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
                match self.service.event_bus().try_acquire_stream() {
                    Err(err) => Err(err.into()),
                    Ok(guard) => match self
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
                tokio::spawn(request.owner.hold_future(events::run_event_stream(
                    Arc::clone(&self.memory),
                    Arc::clone(&self.reserved),
                    self.stopped.clone(),
                    registration,
                    sub,
                    outgoing.clone(),
                    closing.clone(),
                )));
            }
            Err(err) => {
                events::queue_event_control(
                    outgoing,
                    &self.memory,
                    &self.reserved,
                    closing,
                    id,
                    ipc::EventWireFrame::Error(err),
                );
            }
        }
    }

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

    /// Spawns the Streamable HTTP task and propagates unexpected exits back to daemon shutdown.
    pub(crate) fn attach_http(
        self: &Arc<Self>,
        http: PendingHttp,
        spawn_http: fn(HttpLaunch) -> Pin<Box<dyn Future<Output = ()> + Send>>,
    ) -> tokio::task::JoinHandle<()> {
        let guard = StopOnDrop(Arc::clone(self));
        let fut = spawn_http(HttpLaunch {
            listener: http.listener,
            digests: http.digests,
            tools: http.tools,
            launch: http.launch,
            public_root: http.public_root,
            extra_instructions: http.extra_instructions,
            instructions_tool: http.instructions_tool,
            skills: http.skills,
            service: self.service(),
            transfers: self.transfers(),
            memory: Arc::clone(self.memory()),
            control_memory: Arc::clone(self.control_memory()),
            stopped: self.stopped.clone(),
        });
        tokio::spawn(async move {
            let _guard = guard;
            fut.await;
        })
    }

    pub(super) fn attach_transfer_listener(
        &self,
        listener: tokio::net::TcpListener,
        bound: std::net::SocketAddr,
    ) {
        let _ = self.transfer_listener.set(bound);
        tokio::spawn(crate::http_wire::serve_transfers(
            listener,
            Arc::clone(&self.transfers),
            Arc::clone(&self.memory),
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
                tokio::spawn(crate::http_wire::serve_transfers(
                    listener,
                    Arc::clone(&self.transfers),
                    Arc::clone(&self.memory),
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

    pub(super) async fn transfer(
        &self,
        request: crate::transfer::Request,
        launch: &Arc<LaunchContext>,
        input_owner: crate::lease::Owner,
    ) -> TrackedResult {
        let session_root = match crate::config::root_of(launch.var(crate::config::PUBLIC_URL)) {
            Ok(root) => root,
            Err(message) => {
                return TrackedResult::plain(transfer_error(message));
            }
        };
        let public_root = session_root.or_else(|| self.public_root.as_deref().map(str::to_owned));
        if let Some(http_addr) = &self.http {
            if launch.var(crate::config::TRANSFER_ADDR).is_some() {
                return TrackedResult::plain(transfer_error(format!(
                    "cannot set {} with --http; --http already serves /t/",
                    crate::config::TRANSFER_ADDR
                )));
            }
            if public_root.is_none()
                && let Ok(addr) = http_addr.parse::<std::net::SocketAddr>()
                && let Err(message) = crate::config::check_routable_addr("--http", addr, false)
            {
                return TrackedResult::plain(transfer_error(message));
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
                return TrackedResult::owned(transfer_error(message), owner);
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
                        return TrackedResult::plain(transfer_error(message));
                    }
                };
                public_root.unwrap_or_else(|| format!("http://{bound}/"))
            }
        };
        let owner = inspected.owner.clone();
        let result = inspected.run(|(path, shown_path, bytes)| {
            mint_transfer(path, shown_path, bytes, &self.transfers, &base)
        });
        TrackedResult::owned(result, owner)
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

    /// Stops the daemon when it has been idle for `idle_after` and exiting on idleness is what it
    /// was asked to do. Called under the gate, which is where idleness is read.
    pub(super) fn idle_expired(&self, now: Instant) -> bool {
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

    /// Puts this connection in the table, or says why it cannot be. The gate is held across the
    /// check and the insert, so a stop cannot slip between them; lock order is gate, then the
    /// session table. Neither is held across an await.
    pub(super) fn attach(
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

    /// Ends the session at once: its cursors go with it, so a later connection of the same ID
    /// starts a new session. Only the connection whose generation holds the entry may do this.
    pub(super) fn bye(&self, attached: &Attached) {
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
    pub(super) fn close(&self, attached: &Attached) {
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

/// The first six characters of a session ID, which names it inside its owner label.
fn short(session: &str) -> String {
    session.chars().take(6).collect()
}

pub(super) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}
