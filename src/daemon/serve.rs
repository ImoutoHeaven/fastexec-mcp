use super::dispatch::{CallDispatch, dispatch_call};
use super::events;
use super::state::{
    Attached, Attachment, Config, Daemon, HANDSHAKE_TIMEOUT, HttpLaunch, MAX_IN_FLIGHT,
    PendingHttp, Ticket, lock,
};
use super::wire::{
    BoxedStream, Outgoing, Reader, Writer, error, queue_frame, queue_result, reject, write_loop,
};
use crate::env::LaunchContext;
use crate::ipc::{self, ToDaemon, ToEndpoint};
use crate::lease;
use crate::platform::runtime;
use crate::tasks::{Limits, Tasks};
use crate::transport::Listener;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio::time::{MissedTickBehavior, interval, timeout};
use tokio_util::sync::CancellationToken;

enum Handshake {
    Session(Attachment),
    Emit,
}

impl Daemon {
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
    pub async fn run(self: Arc<Self>, mut listener: Listener) {
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
            reject(
                writer,
                self.config.write_stall,
                &self.stopped,
                format!("protocol {protocol} is not {}", ipc::PROTOCOL),
            )
            .await;
            return None;
        }
        if namespace != self.namespace {
            reject(
                writer,
                self.config.write_stall,
                &self.stopped,
                format!("namespace {namespace:?} is not {:?}", self.namespace),
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
        if !valid_session(&session) {
            reject(
                writer,
                self.config.write_stall,
                &self.stopped,
                format!("session {session:?} is not 1-64 ASCII alphanumeric characters"),
            )
            .await;
            return None;
        }
        let launch = match ipc::launch_from_wire(&cwd, &env) {
            Ok(launch) => launch,
            Err(message) => {
                reject(
                    writer,
                    self.config.write_stall,
                    &self.stopped,
                    format!("launch: {message}"),
                )
                .await;
                return None;
            }
        };
        let token = CancellationToken::new();
        let session_id = session;
        let session = match self.attach(&session_id, connection, &client_name, launch, &token) {
            Ok(session) => session,
            Err(message) => {
                reject(writer, self.config.write_stall, &self.stopped, message).await;
                return None;
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
                            let call = CallDispatch {
                                daemon: Arc::clone(self),
                                session: Arc::clone(&attached.session),
                                connection: attached.token.clone(),
                                token,
                                id,
                                tool,
                                arguments,
                                in_flight: Arc::clone(in_flight),
                                outgoing: outgoing.clone(),
                                rpc_id_bytes,
                                input_owner: owner.clone(),
                            };
                            tokio::spawn(owner.hold_future(dispatch_call(call)));
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
                    self.start_event_stream(
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
}

/// A session ID is 1 to 64 ASCII alphanumeric characters.
fn valid_session(session: &str) -> bool {
    !session.is_empty()
        && session.len() <= 64
        && session
            .chars()
            .all(|character| character.is_ascii_alphanumeric())
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
    spawn_http: fn(HttpLaunch) -> Pin<Box<dyn Future<Output = ()> + Send>>,
) -> i32 {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("fastexec: cannot start the daemon runtime: {error}");
            return 1;
        }
    };
    runtime.block_on(serve_daemon(
        namespace, build, http, idle_exit, foreground, spawn_http,
    ))
}

async fn serve_daemon(
    namespace: String,
    build: String,
    http: Option<String>,
    idle_exit: bool,
    foreground: bool,
    spawn_http: fn(HttpLaunch) -> Pin<Box<dyn Future<Output = ()> + Send>>,
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
    let (http_addr, http, startup_transfer, config, http_skills_dir) = match http {
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
            let skills_dir = match crate::config::skills_dir(&launch) {
                Ok(dir) => dir,
                Err(message) => {
                    eprintln!("fastexec: {message}");
                    return 1;
                }
            };
            if let Some(root) = &skills_dir
                && let Err(message) = crate::skills::preflight_check(root)
            {
                eprintln!("fastexec: {message}");
                return 1;
            }
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
                Some(PendingHttp {
                    listener,
                    digests,
                    tools,
                    launch,
                    public_root: public_root.clone(),
                    extra_instructions,
                    instructions_tool,
                    skills: None,
                }),
                None,
                Config {
                    idle_exit: false,
                    ..Config::default()
                },
                skills_dir,
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
                None,
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
    let http_task = if let Some(mut pending) = http {
        if let Some(root) = http_skills_dir {
            match crate::skills::load(&root, daemon.memory()) {
                Ok(catalog) => pending.skills = Some(Arc::new(catalog)),
                Err(message) => {
                    eprintln!("fastexec: {message}");
                    return 1;
                }
            }
        }
        Some(daemon.attach_http(pending, spawn_http))
    } else {
        None
    };
    let mut run = std::pin::pin!(Arc::clone(&daemon).run(listener));
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
    if let Some(http_task) = http_task
        && let Err(error) = http_task.await
    {
        eprintln!("fastexec: HTTP server task failed: {error}");
        std::process::exit(1);
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
