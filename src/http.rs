//! Streamable HTTP: the daemon's second front door, for clients that speak MCP over HTTP instead
//! of the stdio endpoint or the namespace's pipe.

use crate::daemon::Daemon;
use crate::service::Render;
use crate::session::LaunchContext;
use crate::transfer::{self, Admitted, Transfers};
use crate::{Backend, INSTRUCTIONS_DAEMON, Server};
use bytes::Bytes;
use http::header::HeaderName;
use http::{HeaderMap, Method, Request, Response, StatusCode, header};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
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
use tokio_util::sync::CancellationToken;
/// The largest request body the daemon reads: a call carries its arguments, not a file.
pub const LIMIT: usize = 4 << 20;

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
    stopped: CancellationToken,
) {
    // Every session gets a server of its own over the daemon's service, so two HTTP clients share
    // the daemon's tasks and keep their own cursors, exactly as two endpoints do.
    let service = Arc::clone(&daemon.service());
    // One table for this listener: the factory hands it to every session, so a URL a session
    // makes is served by the same route that carries the requests.
    let table = Arc::new(Transfers::new());
    let factory = {
        let table = Arc::clone(&table);
        move || {
            let backend = Backend::Local {
                service: Arc::clone(&service),
                render,
                session: Arc::new(OnceLock::new()),
                launch: Arc::clone(&launch),
                instructions: INSTRUCTIONS_DAEMON,
            };
            Ok(Server::new(backend, &launch, Some(Arc::clone(&table))))
        }
    };
    let bound = listener
        .local_addr()
        .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], 0)));
    let mut config = StreamableHttpServerConfig::default();
    // Sessions are the unit a client keeps its cursors in, so they are created and kept.
    config.stateful_mode = true;
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
                let layer = layer.clone();
                tokio::spawn(async move {
                    let served = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), TowerToHyperService::new(layer))
                        .await;
                    // A client that hangs up mid-request is ordinary; anything else is worth the
                    // log, which is the only place an HTTP fault shows.
                    if let Err(error) = served
                        && !disconnect(&error)
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

/// A connection error that is the client going away rather than the daemon failing.
fn disconnect(error: &hyper::Error) -> bool {
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
    let mut builder = Response::builder().status(status);
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
    let body = request.into_body();
    // The writer runs as a task of its own. A client that hangs up drops this handler before the
    // bytes it already sent are flushed, and the lock must be released only after they are.
    let task = tokio::spawn(async move { write_upload(admitted, offset, body).await });
    match task.await {
        Ok(reply) => reply,
        Err(_) => transfer_empty(StatusCode::INTERNAL_SERVER_ERROR, &[]),
    }
}

/// Writes one part into the entry's file, from `offset` on, and answers its new size.
async fn write_upload(admitted: Admitted, offset: u64, mut body: Incoming) -> Reply {
    let _turn = admitted.upload().await;
    let path = admitted.path().to_path_buf();
    match presence(&path).await {
        Presence::File(size) if offset > size => return transfer_empty(StatusCode::CONFLICT, &[]),
        Presence::NotAFile => return transfer_empty(StatusCode::CONFLICT, &[]),
        Presence::Failed => return transfer_empty(StatusCode::INTERNAL_SERVER_ERROR, &[]),
        Presence::Missing if offset > 0 => return transfer_empty(StatusCode::CONFLICT, &[]),
        Presence::Missing => {
            let parent = path.parent().map(Path::to_path_buf);
            if let Some(parent) = parent
                && !parent.as_os_str().is_empty()
                && tokio::fs::create_dir_all(&parent).await.is_err()
            {
                return transfer_empty(StatusCode::INTERNAL_SERVER_ERROR, &[]);
            }
        }
        Presence::File(_) => {}
    }
    let opened = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .await;
    let mut file = match opened {
        Ok(file) => file,
        Err(_) => return transfer_empty(StatusCode::INTERNAL_SERVER_ERROR, &[]),
    };
    // The part replaces whatever followed `offset`, and nothing of the body is read before that.
    if file.set_len(offset).await.is_err()
        || file.seek(std::io::SeekFrom::Start(offset)).await.is_err()
    {
        return transfer_empty(StatusCode::INTERNAL_SERVER_ERROR, &[]);
    }
    // A write that fails is remembered to the end: a later flush can succeed on its own and must
    // not turn a rejected part into a success.
    let mut refused = false;
    while let Some(frame) = body.frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            // A body that ends early keeps the bytes it did send: HEAD reports them.
            Err(_) => break,
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
}
