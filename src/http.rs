//! Streamable HTTP: the daemon's second front door, for clients that speak MCP over HTTP instead
//! of the stdio endpoint or the namespace's pipe.

use crate::config::{InstructionsToolMode, Tools};
use crate::daemon::Daemon;
use crate::env::LaunchContext;
use crate::mcp::{Backend, Server};
use crate::tasks::session::Session;
use crate::transfer::{self, Admitted, TransferEndpoint, Transfers};
use bytes::Bytes;
use http::header::HeaderName;
use http::{HeaderMap, Method, Request, Response, StatusCode, Version, header};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::service::TowerToHyperService;
use rmcp::transport::streamable_http_server::session::local::SessionConfig;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use sha2::{Digest, Sha256};
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, ReadBuf};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

mod sessions;
use sessions::Sessions;
/// The largest request body the daemon reads: a call carries its arguments, not a file.
pub(crate) const LIMIT: usize = 4 << 20;
/// HTTP has its own connection budget, so unauthenticated peers cannot consume the daemon
/// socket's management capacity or exhaust the process's file descriptors.
const MAX_CONNECTIONS: usize = 128;
/// Sub-quota for unauthenticated `/t/` file transfers on a shared HTTP listener, keeping 32
/// connection slots available for `/mcp` control traffic.
const MAX_TRANSFER_CONNECTIONS: usize = 96;
const MAX_REJECT_WRITERS: usize = 32;
const BUSY_503: &[u8] =
    b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";
/// Legacy HTTP sessions survive individual connections and have a separate count cap.
const MAX_HTTP_SESSIONS: usize = 256;
/// The deadline for a complete HTTP request header.
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);
/// Per-connection HTTP/2 stream cap.
const HTTP2_MAX_CONCURRENT_STREAMS: u32 = 64;
/// HTTP/2 PING interval for keeping idle multiplexed connections alive through proxies.
const HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);
/// An upload may continue indefinitely while it delivers data, but not hold a file idle.
const UPLOAD_IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// The daemon's HTTP front door: one listener and the token digests that admit requests.
pub struct Http {
    pub listener: TcpListener,
    /// The SHA-256 digests of tokens accepted as `Authorization: Bearer`.
    pub digests: Vec<[u8; 32]>,
    /// Which optional tools and annotation overrides HTTP sessions advertise.
    pub tools: Tools,
    /// What the sessions this listener serves inherit: captured before the daemon leaves the
    /// directory it was started in.
    pub launch: Arc<LaunchContext>,
    /// The base a `transfer` URL is minted from: the parsed [`crate::config::PUBLIC_URL`], read once at start.
    /// Its absence means each request's own Host and `X-Forwarded-Proto` name the address.
    pub public_root: Option<Arc<str>>,
    /// Normalized extra instructions loaded once at daemon startup.
    pub extra_instructions: Option<String>,
    /// Whether HTTP sessions also expose server instructions as an MCP tool.
    pub instructions_tool: InstructionsToolMode,
}

/// What a reply carries: rmcp's own response type, so a refusal and a served call have one shape.
/// A body can fail while it streams, which a transfer of a large file must be able to say.
type Reply = Response<BoxBody<Bytes, std::io::Error>>;

/// Serves MCP over Streamable HTTP on `http.listener` until the daemon stops.
pub async fn serve(http: Http, daemon: Arc<Daemon>, stopped: CancellationToken) {
    let Http {
        listener,
        digests,
        tools,
        launch,
        public_root,
        extra_instructions,
        instructions_tool,
    } = http;
    // The factory makes a server for each legacy session or inline-lifecycle request.
    let service = Arc::clone(&daemon.service());
    // Shared with the daemon so both HTTP sessions and stdio endpoints connected to it serve
    // capabilities through this `/t/` route.
    let table = daemon.transfers();
    let launch = Arc::new(
        launch
            .for_session()
            .with_memory(Arc::clone(daemon.memory()))
            .with_control_memory(Arc::clone(daemon.control_memory())),
    );
    let principals: Arc<[Arc<Session>]> = (0..digests.len())
        .map(|index| {
            let id = format!("t{}", index + 1);
            Session::new(
                id.clone(),
                format!("http-{id}"),
                Arc::new(launch.for_session()),
            )
        })
        .collect();
    let (instructions, tool_instructions) = crate::config::compose_instructions(
        true,
        tools.codemode,
        extra_instructions.as_deref(),
        instructions_tool,
    );
    let router = Arc::new(Server::build_router(
        &launch,
        tools,
        tool_instructions.as_deref(),
    ));
    let factory = {
        let table = Arc::clone(&table);
        let public_root = public_root.clone();
        move || {
            let backend = Backend::Local {
                service: Arc::clone(&service),
                session: Arc::new(OnceLock::new()),
                launch: Arc::clone(&launch),
                daemon: true,
            };
            let transfer = tools.transfer.then(|| TransferEndpoint::Http {
                table: Arc::clone(&table),
                public_root: public_root.clone(),
            });
            let mut server = Server::with_shared(
                backend,
                Arc::clone(&router),
                Arc::clone(&instructions),
                tool_instructions.clone(),
                transfer,
                tools,
            );
            server.principals = Some(Arc::clone(&principals));
            Ok(server)
        }
    };
    let bound = listener
        .local_addr()
        .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], 0)));
    let mut config = StreamableHttpServerConfig::default();
    // Legacy clients keep independent sessions; rmcp routes modern requests without one.
    config.legacy_session_mode = true;
    config.stateless_protocol_metadata_required = false;
    // Stateless responses return JSON unless the handler emits notifications first.
    config.json_response = true;
    config.sse_keep_alive = Some(Duration::from_secs(15));
    // Gemini rejects empty SSE priming events before reading the JSON-RPC answer.
    config.sse_retry = None;
    config.cancellation_token = stopped.child_token();
    config.allowed_hosts = allowed_hosts(bound, crate::config::remote_allowed());
    config.allowed_origins = allowed_origins(bound, &origins_from_env());
    let mut session_config = SessionConfig::default();
    session_config.channel_capacity = 16;
    session_config.keep_alive = Some(Duration::from_secs(3600));
    session_config.sse_retry = None;
    session_config.completed_cache_ttl = Duration::from_secs(60);
    session_config.init_timeout = Some(Duration::from_secs(60));
    // This independent count cap bounds HTTP session bookkeeping.
    let sessions = Sessions::new(session_config, MAX_HTTP_SESSIONS);
    let inner = StreamableHttpService::new(factory, Arc::new(sessions), config);
    let layer = Bearer {
        digests: digests.into(),
        table,
        transfer_slots: Arc::new(Semaphore::new(MAX_TRANSFER_CONNECTIONS)),
        stopped: stopped.clone(),
        inner,
    };
    serve_connections(
        listener,
        layer,
        stopped,
        Some(Arc::new(Semaphore::new(MAX_TRANSFER_CONNECTIONS))),
    )
    .await;
}

/// Serves `/t/` capabilities on `listener` until `stopped` is cancelled.
pub async fn serve_transfers(
    listener: TcpListener,
    table: Arc<Transfers>,
    stopped: CancellationToken,
) {
    let layer = TransferOnly {
        table,
        stopped: stopped.clone(),
    };
    serve_connections(listener, layer, stopped, None).await;
}

async fn serve_connections<S>(
    listener: TcpListener,
    service: S,
    stopped: CancellationToken,
    transfer_connections: Option<Arc<Semaphore>>,
) where
    S: tower_service::Service<Request<Incoming>, Response = Reply, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    let connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let reject_slots = Arc::new(Semaphore::new(MAX_REJECT_WRITERS));
    loop {
        tokio::select! {
            _ = stopped.cancelled() => break,
            accepted = listener.accept() => {
                let (mut stream, _peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        eprintln!("fastexec: cannot accept an HTTP connection: {error}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                let Ok(permit) = Arc::clone(&connections).try_acquire_owned() else {
                    if let Ok(reject_permit) = Arc::clone(&reject_slots).try_acquire_owned() {
                        tokio::spawn(async move {
                            let _permit = reject_permit;
                            let _ = tokio::time::timeout(Duration::from_millis(500), async {
                                let _ = stream.write_all(BUSY_503).await;
                                let _ = stream.shutdown().await;
                                let _ = tokio::time::timeout(Duration::from_millis(50), async {
                                    let mut sink = [0u8; 1024];
                                    while let Ok(n) = stream.read(&mut sink).await {
                                        if n == 0 {
                                            break;
                                        }
                                    }
                                })
                                .await;
                            })
                            .await;
                        });
                    } else {
                        let _ = stream.try_write(BUSY_503);
                    }
                    continue;
                };
                let service = service.clone();
                let stopped = stopped.clone();
                let transfer_connections = transfer_connections.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let close_requested = CancellationToken::new();
                    let request_close = close_requested.clone();
                    let transfer_permit = Arc::new(std::sync::Mutex::new(None));
                    let request_transfer_permit = Arc::clone(&transfer_permit);
                    let service = TowerToHyperService::new(service);
                    let service = hyper::service::service_fn(move |request: Request<Incoming>| {
                        let version = request.version();
                        let allowed = if request.uri().path().starts_with("/t/")
                            && let Some(slots) = &transfer_connections
                        {
                            // Retain one slot across requests until this connection closes.
                            let mut held = request_transfer_permit
                                .lock()
                                .unwrap_or_else(|p| p.into_inner());
                            if held.is_none() {
                                *held = Arc::clone(slots).try_acquire_owned().ok();
                            }
                            held.is_some()
                        } else {
                            true
                        };
                        let response = if allowed {
                            Some(hyper::service::Service::call(&service, request))
                        } else {
                            request_close.cancel();
                            None
                        };
                        async move {
                            match response {
                                Some(response) => response.await,
                                None => Ok(refusal(version, StatusCode::SERVICE_UNAVAILABLE, "")),
                            }
                        }
                    });
                    let mut builder = auto::Builder::new(TokioExecutor::new());
                    builder
                        .http1()
                        .timer(TokioTimer::new())
                        .header_read_timeout(HEADER_TIMEOUT);
                    builder
                        .http2()
                        .timer(TokioTimer::new())
                        .max_concurrent_streams(HTTP2_MAX_CONCURRENT_STREAMS)
                        .keep_alive_interval(HTTP2_KEEP_ALIVE_INTERVAL);
                    let connection = builder.serve_connection(TokioIo::new(stream), service);
                    tokio::pin!(connection);
                    let served = tokio::select! {
                        _ = stopped.cancelled() => return,
                        served = &mut connection => served,
                        _ = close_requested.cancelled() => {
                            connection.as_mut().graceful_shutdown();
                            tokio::select! {
                                _ = stopped.cancelled() => return,
                                served = &mut connection => served,
                            }
                        }
                    };
                    // Malformed or abandoned unauthenticated requests are ordinary client
                    // faults. Logging each one would let a peer fill the daemon's error log.
                    if let Err(error) = served
                        && !client_fault(error.as_ref())
                    {
                        eprintln!("fastexec: an HTTP connection failed: {error}");
                    }
                });
            }
        }
    }
}

#[derive(Clone)]
struct TransferOnly {
    table: Arc<Transfers>,
    stopped: CancellationToken,
}

impl tower_service::Service<Request<Incoming>> for TransferOnly {
    type Response = Reply;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Reply, Infallible>> + Send>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Incoming>) -> Self::Future {
        let table = Arc::clone(&self.table);
        let stopped = self.stopped.clone();
        Box::pin(async move {
            if request.uri().path().starts_with("/t/") {
                return Ok(serve_transfer(request, table, stopped, None).await);
            }
            Ok(refusal(request.version(), StatusCode::NOT_FOUND, ""))
        })
    }
}

/// The hosts a request may name: the loopback names rmcp allows, and the address this listener
/// actually bound, which can be any of `127.0.0.0/8`. A proxy in front of the daemon was allowed
/// to reach it outside loopback, so then every host is.
fn allowed_hosts(bound: SocketAddr, remote: bool) -> Vec<String> {
    if remote {
        return Vec::new();
    }
    let mut hosts = StreamableHttpServerConfig::default().allowed_hosts;
    if !bound.ip().is_unspecified() {
        let host = bound.ip().to_string();
        if !hosts.contains(&host) {
            hosts.push(host);
        }
    }
    hosts
}

/// The origins a browser request may come from: this listener's own loopback names and the
/// address it bound, and whatever `FASTEXEC_HTTP_ALLOWED_ORIGINS` adds.
fn allowed_origins(bound: SocketAddr, extra: &[String]) -> Vec<String> {
    let port = bound.port();
    let mut origins = vec![
        format!("http://127.0.0.1:{port}"),
        format!("http://localhost:{port}"),
        format!("http://[::1]:{port}"),
    ];
    if !bound.ip().is_unspecified() {
        origins.push(format!("http://{bound}"));
    }
    origins.extend(extra.iter().cloned());
    origins
}

/// The origins `FASTEXEC_HTTP_ALLOWED_ORIGINS` adds, comma-separated.
fn origins_from_env() -> Vec<String> {
    std::env::var("FASTEXEC_HTTP_ALLOWED_ORIGINS")
        .map(|extra| {
            extra
                .split(',')
                .map(str::trim)
                .filter(|origin| !origin.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// An invalid request, an idle peer or a disconnected client is not a server failure. Actual
/// transport/storage faults still reach the daemon log instead of disappearing with them.
fn client_fault(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(current) = source {
        if let Some(hyper_error) = current.downcast_ref::<hyper::Error>()
            && (hyper_error.is_parse()
                || hyper_error.is_incomplete_message()
                || hyper_error.is_timeout())
        {
            return true;
        }
        if let Some(io) = current.downcast_ref::<std::io::Error>() {
            return matches!(
                io.kind(),
                std::io::ErrorKind::ConnectionReset
                    | std::io::ErrorKind::ConnectionAborted
                    | std::io::ErrorKind::BrokenPipe
                    | std::io::ErrorKind::UnexpectedEof
            );
        }
        source = current.source();
    }
    false
}

/// The token position authenticated before rmcp receives a request.
#[derive(Clone, Copy)]
pub struct Principal(pub usize);

/// The token check and the body cap, in front of rmcp: a request without a token never reaches
/// a session, and one over the cap is refused before it is read.
#[derive(Clone)]
struct Bearer<H = Server> {
    /// Fixed-size digests shared by every request on this listener.
    digests: Arc<[[u8; 32]]>,
    /// The files this listener publishes under `/t/`.
    table: Arc<Transfers>,
    /// Sub-quota keeping `/t/` transfers from starving `/mcp` connections.
    transfer_slots: Arc<Semaphore>,
    stopped: CancellationToken,
    inner: StreamableHttpService<H, Sessions>,
}

impl<H> tower_service::Service<Request<Incoming>> for Bearer<H>
where
    H: rmcp::ServerHandler + Send + 'static,
{
    type Response = Reply;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Reply, Infallible>> + Send>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Incoming>) -> Self::Future {
        let mut inner = self.inner.clone();
        let digests = Arc::clone(&self.digests);
        let table = Arc::clone(&self.table);
        let transfer_slots = Arc::clone(&self.transfer_slots);
        let stopped = self.stopped.clone();
        Box::pin(async move {
            // A transfer is its own front door: no token, no body cap, and never a session.
            if request.uri().path().starts_with("/t/") {
                let Ok(permit) = transfer_slots.try_acquire_owned() else {
                    return Ok(refusal(
                        request.version(),
                        StatusCode::SERVICE_UNAVAILABLE,
                        "",
                    ));
                };
                return Ok(serve_transfer(request, table, stopped, Some(permit)).await);
            }
            let version = request.version();
            let Some(principal) = authorized(request.headers(), &digests) else {
                return Ok(refusal(version, StatusCode::UNAUTHORIZED, "Bearer"));
            };
            // Two routes, and no other: a proxy that forwards its prefix without stripping it
            // fails here, at `initialize`, rather than at a download an hour later.
            if request.uri().path() != "/mcp" {
                return Ok(refusal(version, StatusCode::NOT_FOUND, ""));
            }
            // A declared length over the cap is refused before a byte of it is read; a chunked
            // body over the cap fails at `Limited`, and rmcp answers that as an error response.
            if declared_length(request.headers()).is_some_and(|length| length > LIMIT as u64) {
                return Ok(refusal(version, StatusCode::PAYLOAD_TOO_LARGE, ""));
            }
            let (mut parts, body) = request.into_parts();
            parts.extensions.insert(Principal(principal));
            let request = Request::from_parts(parts, Limited::new(body, LIMIT));
            let answer = inner
                .call(request)
                .await
                .unwrap_or_else(|never| match never {});
            let (mut parts, body) = answer.into_parts();
            // A proxy that buffers would hold every event of a stream until the call ended.
            parts.headers.insert(
                HeaderName::from_static("x-accel-buffering"),
                "no".parse().expect("a header value"),
            );
            Ok(Response::from_parts(
                parts,
                body.map_err(|never: Infallible| match never {}).boxed(),
            ))
        })
    }
}

/// A refusal with no body, so nothing about why is said to a caller that was not admitted.
fn refusal(version: Version, status: StatusCode, challenge: &str) -> Reply {
    // The rejected request's body is deliberately unread. Closing prevents a peer from
    // keeping a connection occupied forever by declaring a body and never sending it.
    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_LENGTH, 0);
    if version < Version::HTTP_2 {
        builder = builder.header(header::CONNECTION, "close");
    }
    if !challenge.is_empty() {
        builder = builder.header(header::WWW_AUTHENTICATE, challenge);
    }
    builder
        .body(
            Full::new(Bytes::new())
                .map_err(|never: Infallible| match never {})
                .boxed(),
        )
        .expect("a response with no body")
}

/// The `Content-Length` a request declares, when it declares one that parses.
fn declared_length(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()
}

/// The token position, comparing every byte of every digest without a match-dependent branch.
fn authorized(headers: &HeaderMap, digests: &[[u8; 32]]) -> Option<usize> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() {
        return None;
    }
    // A credential is compared exactly as the header carries it, without trimming.
    let given: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    digests
        .iter()
        .enumerate()
        .fold(0usize, |matched, (index, digest)| {
            let difference = given
                .iter()
                .zip(digest)
                .fold(0u8, |difference, (a, b)| difference | (a ^ b));
            let mask = 0usize.wrapping_sub(usize::from(difference == 0));
            (matched & !mask) | ((index + 1) & mask)
        })
        .checked_sub(1)
}

/// The bytes a streamed file is read in.
const CHUNK: usize = 32 * 1024;

/// Answers one `/t/` request: a download, the size of a file, or an upload.
async fn serve_transfer(
    request: Request<Incoming>,
    table: Arc<Transfers>,
    stopped: CancellationToken,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Reply {
    let version = request.version();
    let id = request
        .uri()
        .path()
        .strip_prefix("/t/")
        .unwrap_or_default()
        .to_owned();
    if !transfer::valid_id(&id) {
        return transfer_empty(version, StatusCode::NOT_FOUND, &[]);
    }
    // An unknown or expired id is absent whatever the method is, and a request refreshes a live
    // one, so admission comes before the method is looked at. The guard then travels with the
    // request: a download holds it inside its body, an upload inside its writer task.
    let Some(admitted) = table.admit(&id) else {
        return transfer_empty(version, StatusCode::NOT_FOUND, &[]);
    };
    match *request.method() {
        Method::GET => transfer_get(version, request.headers(), admitted, permit).await,
        Method::HEAD => transfer_head(version, admitted).await,
        Method::PUT => transfer_put(request, admitted, stopped).await,
        _ => transfer_empty(
            version,
            StatusCode::METHOD_NOT_ALLOWED,
            &[(header::ALLOW.as_str(), "GET, HEAD, PUT")],
        ),
    }
}

/// What the table's entry has on disk.
enum Presence {
    File(u64),
    Missing,
    NotAFile,
    Failed,
}

/// Reserve Rust's native path argument buffers inside the worker that uses them.
async fn transfer_file<T: Send + 'static>(
    path: Arc<PathBuf>,
    pool: Arc<crate::lease::BytePool>,
    operation: impl FnOnce(&Path) -> std::io::Result<T> + Send + 'static,
) -> std::io::Result<T> {
    tokio::task::spawn_blocking(move || {
        let memory = || {
            std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "Server active memory budget is full while preparing a transfer file operation.",
            )
        };
        let bytes = crate::files::path::native_path_bytes(&path).ok_or_else(memory)?;
        let _lease = pool.try_acquire(bytes).ok_or_else(memory)?;
        operation(&path)
    })
    .await
    .map_err(std::io::Error::other)?
}

async fn presence(path: Arc<PathBuf>) -> Presence {
    match transfer_file(path, Arc::clone(crate::lease::process_pool()), |path| {
        std::fs::metadata(path)
    })
    .await
    {
        Ok(metadata) if metadata.is_file() => Presence::File(metadata.len()),
        Ok(_) => Presence::NotAFile,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Presence::Missing,
        Err(_) => Presence::Failed,
    }
}

/// Downloads a file, or the one range of it that was asked for.
async fn transfer_get(
    version: Version,
    headers: &HeaderMap,
    admitted: Admitted,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Reply {
    let path = admitted.path();
    let (file, metadata) = match transfer_file(
        path,
        Arc::clone(crate::lease::process_pool()),
        crate::files::path::open_regular,
    )
    .await
    {
        Ok(opened) => opened,
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::IsADirectory
                    | std::io::ErrorKind::Unsupported
            ) =>
        {
            return transfer_empty(version, StatusCode::NOT_FOUND, &[]);
        }
        _ => return transfer_empty(version, StatusCode::INTERNAL_SERVER_ERROR, &[]),
    };
    let mut file = tokio::fs::File::from_std(file);
    // Atomic saves may replace the path; the reported size and bytes share this handle.
    let size = metadata.len();
    let range = match ranges(headers) {
        Some(value) => transfer::parse_range(&value, size),
        None => transfer::Range::Whole,
    };
    let (start, length, status, content_range) = match range {
        transfer::Range::Unsatisfiable => {
            let header = format!("bytes */{size}");
            return transfer_empty(
                version,
                StatusCode::RANGE_NOT_SATISFIABLE,
                &[(header::CONTENT_RANGE.as_str(), &header)],
            );
        }
        transfer::Range::Part { start, end } => (
            start,
            end - start + 1,
            StatusCode::PARTIAL_CONTENT,
            Some(format!("bytes {start}-{end}/{size}")),
        ),
        transfer::Range::Whole => (0, size, StatusCode::OK, None),
    };
    if start > 0 && file.seek(std::io::SeekFrom::Start(start)).await.is_err() {
        return transfer_empty(version, StatusCode::INTERNAL_SERVER_ERROR, &[]);
    }
    let mut builder =
        transfer_response(status, Some(length)).header(header::ACCEPT_RANGES, "bytes");
    if let Some(content_range) = content_range {
        builder = builder.header(header::CONTENT_RANGE, content_range);
    }
    builder
        .body(
            FileBody {
                // The length is fixed, so a file that grows while it is sent cannot overrun it.
                file: Some(file.take(length)),
                memory: Arc::clone(crate::lease::process_pool()),
                _admitted: admitted,
                _permit: permit,
            }
            .boxed(),
        )
        .expect("a response with a body")
}

/// Reports the current size of a file, which an upload can resume from.
async fn transfer_head(version: Version, admitted: Admitted) -> Reply {
    let length = match presence(admitted.path()).await {
        Presence::File(size) => size,
        Presence::Missing => 0,
        Presence::NotAFile => return transfer_empty(version, StatusCode::NOT_FOUND, &[]),
        Presence::Failed => return transfer_empty(version, StatusCode::INTERNAL_SERVER_ERROR, &[]),
    };
    transfer_response(StatusCode::OK, Some(length))
        .header(header::ACCEPT_RANGES, "bytes")
        .body(
            Full::new(Bytes::new())
                .map_err(|never: Infallible| match never {})
                .boxed(),
        )
        .expect("a response with no body")
}

/// Uploads one part of a file at `offset`.
async fn transfer_put(
    request: Request<Incoming>,
    admitted: Admitted,
    stopped: CancellationToken,
) -> Reply {
    let version = request.version();
    let offset = match offset_of(request.uri().query()) {
        Ok(offset) => offset,
        Err(()) => return transfer_empty(version, StatusCode::BAD_REQUEST, &[]),
    };
    // A disconnected request waiting for another upload must release its file descriptor and
    // its place in the lock queue. Keep preparation in this handler, whose drop cancels it.
    let target = match prepare_upload(&admitted, offset).await {
        Ok(target) => target,
        Err(status) => return transfer_empty(version, status, &[]),
    };
    let body = request.into_body();
    // Only an actual writer is detached: a disconnect must not drop a write before its accepted
    // bytes are flushed, or release the lock while that flush is still in flight.
    let task = tokio::spawn(async move {
        write_upload(version, admitted, target, offset, body, stopped).await
    });
    match task.await {
        Ok(reply) => reply,
        Err(_) => transfer_empty(version, StatusCode::INTERNAL_SERVER_ERROR, &[]),
    }
}

/// An opened file and the upload turn that protects its contents until the final flush.
struct UploadFile {
    file: tokio::fs::File,
    turn: tokio::sync::OwnedMutexGuard<()>,
}

async fn prepare_upload(admitted: &Admitted, offset: u64) -> Result<UploadFile, StatusCode> {
    let path = admitted.path();
    match presence(Arc::clone(&path)).await {
        Presence::NotAFile => return Err(StatusCode::CONFLICT),
        Presence::Failed => return Err(StatusCode::INTERNAL_SERVER_ERROR),
        Presence::Missing if offset > 0 => return Err(StatusCode::CONFLICT),
        Presence::Missing => {
            let requested = Arc::clone(&path);
            tokio::task::spawn_blocking(move || {
                crate::files::write::create_target_parents(&requested)
            })
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        }
        Presence::File(_) => {}
    }
    // Opening without truncation lets aliases discover the same file identity, including a
    // file which did not exist when its URLs were minted. All mutations happen under its lock.
    let file = match transfer_file(
        path,
        Arc::clone(crate::lease::process_pool()),
        move |path| crate::files::path::open_regular_for_upload(path, offset == 0),
    )
    .await
    {
        Ok((file, _)) => tokio::fs::File::from_std(file),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound
                    | std::io::ErrorKind::IsADirectory
                    | std::io::ErrorKind::Unsupported
            ) =>
        {
            return Err(StatusCode::CONFLICT);
        }
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };
    let turn = admitted
        .upload(&file)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    match file.metadata().await {
        Ok(metadata) if metadata.is_file() && offset <= metadata.len() => {}
        Ok(_) => return Err(StatusCode::CONFLICT),
        Err(_) => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
    Ok(UploadFile { file, turn })
}

/// Writes one part into the entry's file, from `offset` on, and answers its new size.
async fn write_upload(
    version: Version,
    _admitted: Admitted,
    target: UploadFile,
    offset: u64,
    mut body: impl hyper::body::Body<Data = Bytes> + Unpin,
    stopped: CancellationToken,
) -> Reply {
    let UploadFile {
        mut file,
        turn: _turn,
    } = target;
    if stopped.is_cancelled() {
        return transfer_empty(version, StatusCode::SERVICE_UNAVAILABLE, &[]);
    }
    // The part replaces whatever followed `offset`, and nothing of the body is read before that.
    if file.set_len(offset).await.is_err()
        || file.seek(std::io::SeekFrom::Start(offset)).await.is_err()
    {
        return transfer_empty(version, StatusCode::INTERNAL_SERVER_ERROR, &[]);
    }
    // A write that fails is remembered to the end: a later flush can succeed on its own and must
    // not turn a rejected part into a success.
    let mut refused = false;
    let mut body_failure = None;
    let mut deadline = tokio::time::Instant::now() + UPLOAD_IDLE_TIMEOUT;
    loop {
        let next = tokio::select! {
            biased;
            _ = stopped.cancelled() => {
                body_failure = Some(StatusCode::SERVICE_UNAVAILABLE);
                break;
            }
            next = tokio::time::timeout_at(deadline, body.frame()) => next,
        };
        let frame = match next {
            Ok(Some(Ok(frame))) => frame,
            Ok(None) => break,
            Err(_) => {
                body_failure = Some(StatusCode::REQUEST_TIMEOUT);
                break;
            }
            // Keep the accepted prefix for HEAD/resume, but an invalid chunk or a premature
            // EOF is not a successful upload of the body the client declared.
            Ok(Some(Err(_))) => {
                body_failure = Some(StatusCode::BAD_REQUEST);
                break;
            }
        };
        if let Some(data) = frame.data_ref().filter(|data| !data.is_empty()) {
            // Each accepted part is flushed before the next frame is waited for, so a body that
            // fails later never leaves an earlier write in flight.
            if file.write_all(data).await.is_err() || file.flush().await.is_err() {
                refused = true;
                break;
            }
            deadline = tokio::time::Instant::now() + UPLOAD_IDLE_TIMEOUT;
        }
    }
    // Flushed before the answer and before the upload lock is released with this task.
    let flushed = file.flush().await;
    let size = file.metadata().await.map(|metadata| metadata.len());
    drop(file);
    match size {
        Ok(size) if !refused && flushed.is_ok() => match body_failure {
            Some(status) => transfer_empty(version, status, &[]),
            None => transfer_text(StatusCode::OK, size.to_string()),
        },
        // The disk refused part of the upload, or its size cannot be read: nothing to report.
        _ => transfer_empty(version, StatusCode::INTERNAL_SERVER_ERROR, &[]),
    }
}

/// The offset an upload resumes at: at most one `offset=` with a nonempty decimal value.
fn offset_of(query: Option<&str>) -> Result<u64, ()> {
    let mut offset = 0;
    let mut seen = false;
    for pair in query.unwrap_or_default().split('&') {
        if pair.is_empty() {
            continue;
        }
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        if name != "offset" {
            continue;
        }
        if seen {
            return Err(());
        }
        seen = true;
        offset = transfer::decimal(value)?;
    }
    Ok(offset)
}

/// The one `Range` a request asks for, or `None` when several were sent.
fn ranges(headers: &HeaderMap) -> Option<String> {
    let mut values = headers.get_all(header::RANGE).iter();
    let first = values.next()?;
    if values.next().is_some() {
        return None;
    }
    first.to_str().ok().map(str::to_owned)
}

/// A `/t/` response's headers: never cached, never buffered by a proxy, and the type and length
/// of a body when there is one.
fn transfer_response(status: StatusCode, length: Option<u64>) -> http::response::Builder {
    let builder = Response::builder()
        .status(status)
        .header(header::CACHE_CONTROL, "no-store")
        .header("x-accel-buffering", "no")
        // Hyper sends `Content-Length: 0` on a reply with no body, so every reply has a length
        // and therefore says what its bytes would be.
        .header(header::CONTENT_TYPE, "application/octet-stream");
    match length {
        Some(length) => builder.header(header::CONTENT_LENGTH, length),
        None => builder,
    }
}

fn transfer_empty(version: Version, status: StatusCode, extra: &[(&str, &str)]) -> Reply {
    let mut builder = transfer_response(status, None);
    if version < Version::HTTP_2 && (status.is_client_error() || status.is_server_error()) {
        // A refused capability or upload may also have a body the route did not consume.
        builder = builder.header(header::CONNECTION, "close");
    }
    for (name, value) in extra {
        builder = builder.header(*name, *value);
    }
    builder
        .body(
            Full::new(Bytes::new())
                .map_err(|never: Infallible| match never {})
                .boxed(),
        )
        .expect("a response with no body")
}

fn transfer_text(status: StatusCode, text: String) -> Reply {
    let length = text.len() as u64;
    transfer_response(status, Some(length))
        .body(
            Full::new(Bytes::from(text))
                .map_err(|never: Infallible| match never {})
                .boxed(),
        )
        .expect("a response with a body")
}

struct DownloadChunk {
    bytes: Vec<u8>,
    // Free the copied bytes before returning their capacity to the shared pool.
    _lease: crate::lease::Lease,
}

impl AsRef<[u8]> for DownloadChunk {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

fn copy_download_chunk(
    source: &[u8],
    memory: &Arc<crate::lease::BytePool>,
) -> std::io::Result<Bytes> {
    let refused = || {
        std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            "Server active memory budget is full while sending a transferred file.",
        )
    };
    let mut lease = memory.try_acquire(source.len()).ok_or_else(refused)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(source.len())
        .map_err(|_| refused())?;
    if !lease.try_resize(bytes.capacity()) {
        return Err(refused());
    }
    bytes.extend_from_slice(source);
    Ok(Bytes::from_owner(DownloadChunk {
        bytes,
        _lease: lease,
    }))
}

/// A file's bytes, read as the response is written, holding its entry open to the last byte.
struct FileBody {
    /// The bytes left to send, or `None` once the body has ended, so its end stays an end.
    file: Option<tokio::io::Take<tokio::fs::File>>,
    memory: Arc<crate::lease::BytePool>,
    _admitted: Admitted,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl hyper::body::Body for FileBody {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Bytes>, std::io::Error>>> {
        let this = self.get_mut();
        let Some(file) = this.file.as_mut() else {
            return Poll::Ready(None);
        };
        let mut buffer = [0u8; CHUNK];
        let mut read = ReadBuf::new(&mut buffer);
        match Pin::new(file).poll_read(context, &mut read) {
            Poll::Pending => Poll::Pending,
            // A read that fails ends the body with that failure rather than a quiet end of file.
            Poll::Ready(Err(error)) => {
                this.file = None;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Ok(())) if read.filled().is_empty() => {
                this.file = None;
                Poll::Ready(None)
            }
            Poll::Ready(Ok(())) => match copy_download_chunk(read.filled(), &this.memory) {
                Ok(bytes) => Poll::Ready(Some(Ok(hyper::body::Frame::data(bytes)))),
                Err(error) => {
                    this.file = None;
                    Poll::Ready(Some(Err(error)))
                }
            },
        }
    }

    fn is_end_stream(&self) -> bool {
        self.file.as_ref().is_none_or(|file| file.limit() == 0)
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        hyper::body::SizeHint::with_exact(self.file.as_ref().map_or(0, |file| file.limit()))
    }
}

#[cfg(test)]
mod tests;
