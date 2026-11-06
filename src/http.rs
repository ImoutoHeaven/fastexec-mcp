//! Streamable HTTP: the daemon's second front door, for clients that speak MCP over HTTP instead
//! of the stdio endpoint or the namespace's pipe.

use crate::daemon::Daemon;
use crate::service::Render;
use crate::session::{LaunchContext, Session};
use crate::transfer::{self, Admitted, Transfers};
use crate::{Backend, INSTRUCTIONS_DAEMON, Server};
use bytes::Bytes;
use http::header::HeaderName;
use http::{HeaderMap, Method, Request, Response, StatusCode, header};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use rmcp::transport::streamable_http_server::session::local::{LocalSessionManager, SessionConfig};
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use sha2::{Digest, Sha256};
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, ReadBuf};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
/// The largest request body the daemon reads: a call carries its arguments, not a file.
pub const LIMIT: usize = 4 << 20;
/// HTTP has its own connection budget, so unauthenticated peers cannot consume the daemon
/// socket's management capacity or exhaust the process's file descriptors.
const MAX_CONNECTIONS: usize = 128;
/// Only request headers are timed: streaming bodies, SSE and long tool calls may take longer.
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);

/// The value that names the address clients reach, when a proxy stands in front of the listener.
pub const PUBLIC_URL: &str = "FASTEXEC_PUBLIC_URL";

/// The daemon's HTTP front door: one listener, the token that admits a request, and the render
/// its sessions use.
pub struct Http {
    pub listener: TcpListener,
    /// The token a request must present as `Authorization: Bearer`.
    pub token: String,
    /// How results are rendered for the sessions this listener serves.
    pub render: Render,
    /// What the sessions this listener serves inherit: captured before the daemon leaves the
    /// directory it was started in.
    pub launch: Arc<LaunchContext>,
    /// The base a `transfer` URL is minted from: the parsed [`PUBLIC_URL`], read once at start.
    /// Its absence means each request's own Host and `X-Forwarded-Proto` name the address.
    pub public_root: Option<Arc<str>>,
}

/// The root this process was started with: `None` when the variable is unset, the normalized root
/// when it is valid, and the reason it cannot be used otherwise. Called once when an HTTP daemon
/// starts, so nothing rereads the environment while a URL is minted.
pub fn public_root() -> Result<Option<String>, String> {
    root_of(std::env::var_os(PUBLIC_URL).as_deref())
}

/// [`public_root`] over a value that is already in hand, so the unset, the valid, the empty and
/// the non-UTF-8 cases are all one function.
fn root_of(value: Option<&std::ffi::OsStr>) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(text) = value.to_str() else {
        return Err(format!("{PUBLIC_URL} is not UTF-8"));
    };
    valid_root(text).map(Some)
}

/// A public root, validated and normalized: a scheme, a host, an optional port, and a path prefix
/// that ends in one `/`. Every rejection names the variable and the reason.
///
/// The value crosses trust boundaries, so it is a small wrapper over [`http::Uri`] rather than a
/// URL normalizer: what it states is kept byte for byte, and only the scheme case and the one
/// trailing `/` are settled here.
pub fn valid_root(text: &str) -> Result<String, String> {
    // The value itself stays out of the message: a mistyped value can carry userinfo, and a
    // refusal is written to a terminal and to a daemon log.
    let refuse = |why: &str| Err(format!("{PUBLIC_URL} {why}"));
    if text
        .chars()
        .any(|character| character.is_whitespace() || character.is_control())
    {
        return refuse("contains whitespace or a control character");
    }
    if text.contains('?') || text.contains('#') {
        return refuse("carries a query or a fragment");
    }
    let Ok(uri) = text.parse::<::http::Uri>() else {
        return refuse(unparsed_form(text));
    };
    let Some(scheme) = uri.scheme_str() else {
        return refuse("must start with http:// or https://");
    };
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return refuse("must start with http:// or https://");
    }
    let scheme = scheme.to_ascii_lowercase();
    let Some(authority) = uri.authority() else {
        return refuse("names no host");
    };
    // The raw authority is what a port is judged on: `Authority::port()` answers `None` for a port
    // it refuses, which would read as "no port at all".
    let raw = authority.as_str();
    if raw.contains('@') {
        return refuse("carries userinfo");
    }
    if authority.host().is_empty() {
        return refuse("names no host");
    }
    let port = match authority_port(raw) {
        Ok(port) => port,
        Err(why) => return refuse(why),
    };
    if let Some(port) = port {
        if port.is_empty() {
            return refuse("has an empty port");
        }
        if !port.bytes().all(|byte| byte.is_ascii_digit()) {
            return refuse("has a port that is not a decimal number");
        }
        match port.parse::<u32>() {
            Ok(number) if (1..=65535).contains(&number) => {}
            _ => return refuse("has a port outside 1-65535"),
        }
    }
    let path = uri.path();
    // The one trailing `/` a value may state is not an interior segment; anything else empty, and
    // any literal dot segment, is refused rather than normalized away.
    let mut segments: Vec<&str> = path.split('/').skip(1).collect();
    if segments.last().is_some_and(|last| last.is_empty()) {
        segments.pop();
    }
    for segment in segments {
        if segment.is_empty() {
            return refuse("has an empty path segment");
        }
        if segment == "." || segment == ".." {
            return refuse("has a dot path segment");
        }
    }
    let trailing = if path.ends_with('/') { "" } else { "/" };
    Ok(format!("{scheme}://{raw}{path}{trailing}"))
}

/// Why a value that `http::Uri` refused is not a URL: a port that is not decimal digits is the
/// common mistake worth naming.
fn unparsed_form(text: &str) -> &'static str {
    let Some((_, rest)) = text.split_once("://") else {
        return "must start with http:// or https://";
    };
    let authority = rest.split('/').next().unwrap_or_default();
    if authority.is_empty() {
        return "names no host";
    }
    match authority_port(authority) {
        Err(why) => why,
        Ok(Some(port)) if !port.bytes().all(|byte| byte.is_ascii_digit()) => {
            "has a port that is not a decimal number"
        }
        _ => "is not a URL",
    }
}

/// The port a raw authority states, or why the part after its host is not one.
///
/// `http::Uri` reads a bracketed authority loosely: `[]` parses with a host of `[]`, and anything
/// behind the closing `]` is dropped from `host()` and answers `None` from `port()`. The raw text is
/// therefore read here, so a bracketed host is either a host with an optional port or a refusal.
fn authority_port(authority: &str) -> Result<Option<&str>, &'static str> {
    let Some(rest) = authority.strip_prefix('[') else {
        let mut parts = authority.rsplitn(2, ':');
        let last = parts.next().unwrap_or_default();
        return Ok(parts.next().map(|_| last));
    };
    let Some((host, after)) = rest.split_once(']') else {
        return Err("has an unclosed bracketed host");
    };
    if host.is_empty() {
        return Err("names no host");
    }
    match after.strip_prefix(':') {
        Some(port) => Ok(Some(port)),
        None if after.is_empty() => Ok(None),
        None => Err("has text after a bracketed host"),
    }
}

/// What a reply carries: rmcp's own response type, so a refusal and a served call have one shape.
/// A body can fail while it streams, which a transfer of a large file must be able to say.
type Reply = Response<BoxBody<Bytes, std::io::Error>>;

/// Serves MCP over Streamable HTTP on `listener` until the daemon stops.
pub async fn serve(
    listener: TcpListener,
    token: String,
    daemon: Arc<Daemon>,
    render: Render,
    launch: Arc<LaunchContext>,
    public_root: Option<Arc<str>>,
    stopped: CancellationToken,
) {
    // Every session gets a server of its own over the daemon's service, so two HTTP clients share
    // the daemon's tasks and keep their own cursors, exactly as two endpoints do.
    let service = Arc::clone(&daemon.service());
    // One table for this listener: the factory hands it to every session, so a URL a session
    // makes is served by the same route that carries the requests.
    let table = Arc::new(Transfers::new());
    let stateless_session = Session::new(
        "http-stateless".to_owned(),
        "http-stateless".to_owned(),
        Arc::clone(&launch),
    );
    let factory = {
        let table = Arc::clone(&table);
        let public_root = public_root.clone();
        move || {
            let backend = Backend::Local {
                service: Arc::clone(&service),
                render,
                session: Arc::new(OnceLock::new()),
                launch: Arc::clone(&launch),
                instructions: INSTRUCTIONS_DAEMON,
            };
            let mut server = Server::new(
                backend,
                &launch,
                Some(Arc::clone(&table)),
                public_root.clone(),
            );
            server.stateless_session = Some(Arc::clone(&stateless_session));
            Ok(server)
        }
    };
    let bound = listener
        .local_addr()
        .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], 0)));
    let mut config = StreamableHttpServerConfig::default();
    // Legacy clients keep independent sessions; rmcp routes modern requests without one.
    config.legacy_session_mode = true;
    // A tool result is one answer, and every client here speaks event streams.
    config.json_response = false;
    config.sse_keep_alive = Some(Duration::from_secs(15));
    config.cancellation_token = stopped.child_token();
    config.allowed_hosts = allowed_hosts(bound, remote_allowed());
    config.allowed_origins = allowed_origins(bound, &origins_from_env());
    let mut session_config = SessionConfig::default();
    session_config.channel_capacity = 16;
    session_config.keep_alive = Some(Duration::from_secs(600));
    session_config.sse_retry = Some(Duration::from_secs(3));
    session_config.completed_cache_ttl = Duration::from_secs(60);
    session_config.init_timeout = Some(Duration::from_secs(60));
    let mut sessions = LocalSessionManager::default();
    sessions.session_config = session_config;
    // shortcut: HTTP sessions and their bodies are not counted against the daemon's byte budget or
    // MAX_SESSIONS; rmcp holds the bodies and drops a session after `keep_alive` instead. Add shared
    // accounting for both before promising a namespace-wide HTTP quota. rmcp also treats a
    // configured origin that names no port as a wildcard for every port of that host, while an
    // origin that names a port is matched exactly; giving a port-less configured origin this
    // listener's own port is the upgrade, so no configured origin stays port-agnostic.
    let inner = StreamableHttpService::new(factory, Arc::new(sessions), config);
    let layer = Bearer {
        digest: Sha256::digest(token.as_bytes()).into(),
        table,
        inner,
    };
    let connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    loop {
        tokio::select! {
            _ = stopped.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, _peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        eprintln!("fastexec: cannot accept an HTTP connection: {error}");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                };
                // Never queue a task waiting for a slot: that would move the unbounded
                // allocation from sockets into futures. The accepted socket is dropped at once.
                let Ok(permit) = Arc::clone(&connections).try_acquire_owned() else {
                    continue;
                };
                let layer = layer.clone();
                let stopped = stopped.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let mut builder = http1::Builder::new();
                    builder.timer(TokioTimer::new()).header_read_timeout(HEADER_TIMEOUT);
                    let served = tokio::select! {
                        _ = stopped.cancelled() => return,
                        served = builder.serve_connection(
                            TokioIo::new(stream), TowerToHyperService::new(layer)
                        ) => served,
                    };
                    // Malformed or abandoned unauthenticated requests are ordinary client
                    // faults. Logging each one would let a peer fill the daemon's error log.
                    if let Err(error) = served
                        && !client_fault(&error)
                    {
                        eprintln!("fastexec: an HTTP connection failed: {error}");
                    }
                });
            }
        }
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

/// Whether a bind outside loopback was allowed: the daemon then trusts a TLS-terminating proxy in
/// front of it for both the host and the origin.
pub fn remote_allowed() -> bool {
    std::env::var("FASTEXEC_HTTP_ALLOW_REMOTE").is_ok_and(|value| value == "1")
}

/// An invalid request, an idle peer or a disconnected client is not a server failure. Actual
/// transport/storage faults still reach the daemon log instead of disappearing with them.
fn client_fault(error: &hyper::Error) -> bool {
    if error.is_parse() || error.is_incomplete_message() || error.is_timeout() {
        return true;
    }
    use std::error::Error;
    let mut source: Option<&(dyn Error + 'static)> = Some(error);
    while let Some(current) = source {
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

/// The token check and the body cap, in front of rmcp: a request without the token never reaches
/// a session, and one over the cap is refused before it is read.
#[derive(Clone)]
struct Bearer {
    /// The SHA-256 of the token, so the comparison is over two fixed-size values.
    digest: [u8; 32],
    /// The files this listener publishes under `/t/`.
    table: Arc<Transfers>,
    inner: StreamableHttpService<Server, LocalSessionManager>,
}

impl tower_service::Service<Request<Incoming>> for Bearer {
    type Response = Reply;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Reply, Infallible>> + Send>>;

    fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<Incoming>) -> Self::Future {
        let mut inner = self.inner.clone();
        let digest = self.digest;
        let table = Arc::clone(&self.table);
        Box::pin(async move {
            // A transfer is its own front door: no token, no body cap, and never a session.
            if request.uri().path().starts_with("/t/") {
                return Ok(serve_transfer(request, table).await);
            }
            if !authorized(request.headers(), &digest) {
                return Ok(refusal(StatusCode::UNAUTHORIZED, "Bearer"));
            }
            // Two routes, and no other: a proxy that forwards its prefix without stripping it
            // fails here, at `initialize`, rather than at a download an hour later.
            if request.uri().path() != "/mcp" {
                return Ok(refusal(StatusCode::NOT_FOUND, ""));
            }
            // A declared length over the cap is refused before a byte of it is read; a chunked
            // body over the cap fails at `Limited`, and rmcp answers that as an error response.
            if declared_length(request.headers()).is_some_and(|length| length > LIMIT as u64) {
                return Ok(refusal(StatusCode::PAYLOAD_TOO_LARGE, ""));
            }
            let (parts, body) = request.into_parts();
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
fn refusal(status: StatusCode, challenge: &str) -> Reply {
    // The rejected request's body is deliberately unread. Closing prevents a peer from
    // keeping a connection occupied forever by declaring a body and never sending it.
    let mut builder = Response::builder()
        .status(status)
        .header(header::CONNECTION, "close");
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

/// Whether the request carries this daemon's token. The digest comparison touches every byte of
/// both values, so the time it takes says nothing about how much of a guess was right.
fn authorized(headers: &HeaderMap, digest: &[u8; 32]) -> bool {
    let Some(value) = headers.get(header::AUTHORIZATION) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let Some((scheme, token)) = value.split_once(' ') else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("bearer") {
        return false;
    }
    // The token is compared as the header carries it: the daemon accepted only bytes a header
    // can hold, so nothing here is trimmed or reinterpreted.
    let given: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    given
        .iter()
        .zip(digest)
        .fold(0u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}

/// The bytes a streamed file is read in.
const CHUNK: usize = 32 * 1024;

/// Answers one `/t/` request: a download, the size of a file, or an upload.
async fn serve_transfer(request: Request<Incoming>, table: Arc<Transfers>) -> Reply {
    let id = request
        .uri()
        .path()
        .strip_prefix("/t/")
        .unwrap_or_default()
        .to_owned();
    if !transfer::valid_id(&id) {
        return transfer_empty(StatusCode::NOT_FOUND, &[]);
    }
    // An unknown or expired id is absent whatever the method is, and a request refreshes a live
    // one, so admission comes before the method is looked at. The guard then travels with the
    // request: a download holds it inside its body, an upload inside its writer task.
    let Some(admitted) = table.admit(&id) else {
        return transfer_empty(StatusCode::NOT_FOUND, &[]);
    };
    match *request.method() {
        Method::GET => transfer_get(request, admitted).await,
        Method::HEAD => transfer_head(admitted).await,
        Method::PUT => transfer_put(request, admitted).await,
        _ => transfer_empty(
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

async fn presence(path: &Path) -> Presence {
    match tokio::fs::metadata(path).await {
        Ok(metadata) if metadata.is_file() => Presence::File(metadata.len()),
        Ok(_) => Presence::NotAFile,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Presence::Missing,
        Err(_) => Presence::Failed,
    }
}

/// Downloads a file, or the one range of it that was asked for.
async fn transfer_get(request: Request<Incoming>, admitted: Admitted) -> Reply {
    let size = match presence(admitted.path()).await {
        Presence::File(size) => size,
        Presence::Failed => return transfer_empty(StatusCode::INTERNAL_SERVER_ERROR, &[]),
        Presence::Missing | Presence::NotAFile => {
            return transfer_empty(StatusCode::NOT_FOUND, &[]);
        }
    };
    let range = match ranges(request.headers()) {
        Some(value) => transfer::parse_range(&value, size),
        None => transfer::Range::Whole,
    };
    let (start, length, status, content_range) = match range {
        transfer::Range::Unsatisfiable => {
            let header = format!("bytes */{size}");
            return transfer_empty(
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
    let mut file = match tokio::fs::File::open(admitted.path()).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return transfer_empty(StatusCode::NOT_FOUND, &[]);
        }
        Err(_) => return transfer_empty(StatusCode::INTERNAL_SERVER_ERROR, &[]),
    };
    if start > 0 && file.seek(std::io::SeekFrom::Start(start)).await.is_err() {
        return transfer_empty(StatusCode::INTERNAL_SERVER_ERROR, &[]);
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
                _admitted: admitted,
            }
            .boxed(),
        )
        .expect("a response with a body")
}

/// Reports the current size of a file, which an upload can resume from.
async fn transfer_head(admitted: Admitted) -> Reply {
    let length = match presence(admitted.path()).await {
        Presence::File(size) => size,
        Presence::Missing => 0,
        Presence::NotAFile => return transfer_empty(StatusCode::NOT_FOUND, &[]),
        Presence::Failed => return transfer_empty(StatusCode::INTERNAL_SERVER_ERROR, &[]),
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
async fn transfer_put(request: Request<Incoming>, admitted: Admitted) -> Reply {
    let offset = match offset_of(request.uri().query()) {
        Ok(offset) => offset,
        Err(()) => return transfer_empty(StatusCode::BAD_REQUEST, &[]),
    };
    // A disconnected request waiting for another upload must release its file descriptor and
    // its place in the lock queue. Keep preparation in this handler, whose drop cancels it.
    let target = match prepare_upload(&admitted, offset).await {
        Ok(target) => target,
        Err(reply) => return reply,
    };
    let body = request.into_body();
    // Only an actual writer is detached: a disconnect must not drop a write before its accepted
    // bytes are flushed, or release the lock while that flush is still in flight.
    let task = tokio::spawn(async move { write_upload(admitted, target, offset, body).await });
    match task.await {
        Ok(reply) => reply,
        Err(_) => transfer_empty(StatusCode::INTERNAL_SERVER_ERROR, &[]),
    }
}

/// An opened file and the upload turn that protects its contents until the final flush.
struct UploadFile {
    file: tokio::fs::File,
    turn: tokio::sync::OwnedMutexGuard<()>,
}

async fn prepare_upload(admitted: &Admitted, offset: u64) -> Result<UploadFile, Reply> {
    let path = admitted.path().to_path_buf();
    match presence(&path).await {
        Presence::NotAFile => return Err(transfer_empty(StatusCode::CONFLICT, &[])),
        Presence::Failed => return Err(transfer_empty(StatusCode::INTERNAL_SERVER_ERROR, &[])),
        Presence::Missing if offset > 0 => return Err(transfer_empty(StatusCode::CONFLICT, &[])),
        Presence::Missing => {
            let parent = path.parent().map(Path::to_path_buf);
            if let Some(parent) = parent
                && !parent.as_os_str().is_empty()
                && tokio::fs::create_dir_all(&parent).await.is_err()
            {
                return Err(transfer_empty(StatusCode::INTERNAL_SERVER_ERROR, &[]));
            }
        }
        Presence::File(_) => {}
    }
    // Opening without truncation lets aliases discover the same file identity, including a
    // file which did not exist when its URLs were minted. All mutations happen under its lock.
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(offset == 0)
        .truncate(false)
        .open(&path)
        .await
        .map_err(|_| transfer_empty(StatusCode::INTERNAL_SERVER_ERROR, &[]))?;
    let turn = admitted
        .upload(&file)
        .await
        .map_err(|_| transfer_empty(StatusCode::INTERNAL_SERVER_ERROR, &[]))?;
    match file.metadata().await {
        Ok(metadata) if metadata.is_file() && offset <= metadata.len() => {}
        Ok(_) => return Err(transfer_empty(StatusCode::CONFLICT, &[])),
        Err(_) => return Err(transfer_empty(StatusCode::INTERNAL_SERVER_ERROR, &[])),
    }
    Ok(UploadFile { file, turn })
}

/// Writes one part into the entry's file, from `offset` on, and answers its new size.
async fn write_upload(
    _admitted: Admitted,
    target: UploadFile,
    offset: u64,
    mut body: Incoming,
) -> Reply {
    let UploadFile {
        mut file,
        turn: _turn,
    } = target;
    // The part replaces whatever followed `offset`, and nothing of the body is read before that.
    if file.set_len(offset).await.is_err()
        || file.seek(std::io::SeekFrom::Start(offset)).await.is_err()
    {
        return transfer_empty(StatusCode::INTERNAL_SERVER_ERROR, &[]);
    }
    // A write that fails is remembered to the end: a later flush can succeed on its own and must
    // not turn a rejected part into a success.
    let mut refused = false;
    let mut incomplete = false;
    while let Some(frame) = body.frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            // Keep the accepted prefix for HEAD/resume, but an invalid chunk or a premature
            // EOF is not a successful upload of the body the client declared.
            Err(_) => {
                incomplete = true;
                break;
            }
        };
        if let Some(data) = frame.data_ref() {
            // Each accepted part is flushed before the next frame is waited for, so a body that
            // fails later never leaves an earlier write in flight.
            if file.write_all(data).await.is_err() || file.flush().await.is_err() {
                refused = true;
                break;
            }
        }
    }
    // Flushed before the answer and before the upload lock is released with this task.
    let flushed = file.flush().await;
    let size = file.metadata().await.map(|metadata| metadata.len());
    drop(file);
    match size {
        Ok(_) if !refused && flushed.is_ok() && incomplete => {
            transfer_empty(StatusCode::BAD_REQUEST, &[])
        }
        Ok(size) if !refused && flushed.is_ok() => transfer_text(StatusCode::OK, size.to_string()),
        // The disk refused part of the upload, or its size cannot be read: nothing to report.
        _ => transfer_empty(StatusCode::INTERNAL_SERVER_ERROR, &[]),
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

fn transfer_empty(status: StatusCode, extra: &[(&str, &str)]) -> Reply {
    let mut builder = transfer_response(status, None);
    if status.is_client_error() || status.is_server_error() {
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

/// A file's bytes, read as the response is written, holding its entry open to the last byte.
struct FileBody {
    /// The bytes left to send, or `None` once the body has ended, so its end stays an end.
    file: Option<tokio::io::Take<tokio::fs::File>>,
    _admitted: Admitted,
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
            Poll::Ready(Ok(())) => Poll::Ready(Some(Ok(hyper::body::Frame::data(
                Bytes::copy_from_slice(read.filled()),
            )))),
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
mod tests {
    use super::*;

    fn headers(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, value.parse().unwrap());
        headers
    }

    fn digest(token: &str) -> [u8; 32] {
        Sha256::digest(token.as_bytes()).into()
    }

    #[test]
    fn the_token_admits_only_itself() {
        let token = "0123456789abcdef";
        assert!(authorized(
            &headers(&format!("Bearer {token}")),
            &digest(token)
        ));
        assert!(authorized(
            &headers(&format!("bearer {token}")),
            &digest(token)
        ));
        assert!(!authorized(
            &headers(&format!("Bearer {token}x")),
            &digest(token)
        ));
        assert!(!authorized(
            &headers(&format!("Bearer {}", &token[..15])),
            &digest(token)
        ));
        assert!(!authorized(
            &headers("Basic 0123456789abcdef"),
            &digest(token)
        ));
        assert!(!authorized(&headers(token), &digest(token)));
        assert!(!authorized(&HeaderMap::new(), &digest(token)));
    }

    #[test]
    fn the_origins_are_this_listener_and_what_the_configuration_adds() {
        let bound = SocketAddr::from(([127, 0, 0, 2], 8787));
        let origins = allowed_origins(bound, &["https://app.example".to_owned()]);
        assert!(origins.contains(&"http://127.0.0.1:8787".to_owned()));
        assert!(origins.contains(&"http://localhost:8787".to_owned()));
        assert!(origins.contains(&"http://[::1]:8787".to_owned()));
        // The address actually bound is one this listener's own client may name.
        assert!(origins.contains(&"http://127.0.0.2:8787".to_owned()));
        assert!(origins.contains(&"https://app.example".to_owned()));
        assert!(allowed_hosts(bound, false).contains(&"127.0.0.2".to_owned()));
        // A daemon reachable through a proxy trusts the proxy for both.
        assert!(allowed_hosts(bound, true).is_empty());
    }

    /// Every value that names an address clients reach, normalized to what a URL is minted from.
    #[test]
    fn a_public_root_keeps_what_it_states_and_gains_one_trailing_slash() {
        let valid = [
            ("http://h", "http://h/"),
            ("http://h/", "http://h/"),
            ("https://h.example/mac", "https://h.example/mac/"),
            ("https://h.example/mac/", "https://h.example/mac/"),
            ("HTTP://Host.Example/Mac", "http://Host.Example/Mac/"),
            (
                "https://HtTpS.example:8443/mac/",
                "https://HtTpS.example:8443/mac/",
            ),
            ("http://127.0.0.1:8765/mac/", "http://127.0.0.1:8765/mac/"),
            ("https://h:65535/", "https://h:65535/"),
            // A bracketed IPv6 host is an authority like any other, with or without a port.
            ("http://[::1]", "http://[::1]/"),
            ("http://[::1]:80/", "http://[::1]:80/"),
            ("http://[::1]:8080/mac", "http://[::1]:8080/mac/"),
            // Encoded bytes are what a proxy rewrites to, so they are kept as stated.
            ("https://h/a%20b/", "https://h/a%20b/"),
            ("https://h/%2E%2E/%2e/", "https://h/%2E%2E/%2e/"),
        ];
        for (value, expected) in valid {
            assert_eq!(valid_root(value).as_deref(), Ok(expected), "{value:?}");
        }
    }

    /// Each way a value cannot name an address, with the reason it is refused for.
    #[test]
    fn a_public_root_that_cannot_be_used_is_refused_with_its_reason() {
        let refused = [
            ("", "must start with http:// or https://"),
            (
                "http://h/ mac/",
                "contains whitespace or a control character",
            ),
            (
                "http://h/a\u{7}",
                "contains whitespace or a control character",
            ),
            ("https://h/a?b=c", "carries a query or a fragment"),
            ("https://h/a#b", "carries a query or a fragment"),
            ("ftp://h/mac/", "must start with http:// or https://"),
            ("h/mac/", "must start with http:// or https://"),
            ("http:///mac/", "names no host"),
            ("http://user@h/mac/", "carries userinfo"),
            ("http://user:secret@h/mac/", "carries userinfo"),
            ("http://h:/", "has an empty port"),
            ("http://h:/mac/", "has an empty port"),
            ("http://h:0/", "has a port outside 1-65535"),
            ("http://h:65536/", "has a port outside 1-65535"),
            ("http://h:abc/", "has a port that is not a decimal number"),
            ("http://h:8a80/", "has a port that is not a decimal number"),
            ("http://h:-1/", "has a port that is not a decimal number"),
            ("http://[::1]:/", "has an empty port"),
            ("http://[::1]:70000/", "has a port outside 1-65535"),
            // `http::Uri` reads a bracketed authority loosely, so these are judged on the raw text.
            ("http://[]/", "names no host"),
            ("http://[]:80/mac/", "names no host"),
            ("http://[::1]junk/mac/", "has text after a bracketed host"),
            (
                "http://[::1]junk:abc/mac/",
                "has text after a bracketed host",
            ),
            ("http://[::1:8080/mac/", "has an unclosed bracketed host"),
            ("http://h/./mac/", "has a dot path segment"),
            ("http://h/../mac/", "has a dot path segment"),
            ("http://h/mac/.", "has a dot path segment"),
            ("http://h//mac/", "has an empty path segment"),
            ("http://h/mac//deep/", "has an empty path segment"),
        ];
        for (value, reason) in refused {
            let message = valid_root(value).expect_err(value);
            // The reason and the variable, and never the value: a mistyped value can carry userinfo
            // and this message reaches a terminal and a daemon log.
            assert_eq!(
                message,
                format!("FASTEXEC_PUBLIC_URL {reason}"),
                "{value:?}"
            );
        }
    }

    /// The variable is unset or it is a value; a value that is not UTF-8 cannot be a URL.
    #[test]
    fn an_unset_public_root_is_no_root_and_an_unreadable_one_is_refused() {
        assert_eq!(root_of(None), Ok(None));
        assert_eq!(
            root_of(Some(std::ffi::OsStr::new("https://h/mac"))),
            Ok(Some("https://h/mac/".to_owned()))
        );
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            let message = root_of(Some(std::ffi::OsStr::from_bytes(b"https://h/\xff")))
                .expect_err("a value that is not UTF-8");
            assert!(
                message.starts_with("FASTEXEC_PUBLIC_URL ") && message.contains("UTF-8"),
                "{message}"
            );
        }
    }
}
