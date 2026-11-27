//! Streamable HTTP: the daemon's second front door, for clients that speak MCP over HTTP instead
//! of the stdio endpoint or the namespace's pipe.

use crate::config::{InstructionsToolMode, Tools};
use crate::env::LaunchContext;
use crate::http_wire::{self, Reply};
use crate::lease::BytePool;
use crate::mcp::{Backend, HttpResponseLease, Principal, Server, validate_skill_catalog};
use crate::service::Service;
use crate::skills::SkillCatalog;
use crate::tasks::session::Session;
use crate::transfer::{TransferEndpoint, Transfers};
use http::header::HeaderName;
use http::{HeaderMap, Request, Response, StatusCode, header};
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use rmcp::transport::streamable_http_server::session::local::SessionConfig;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use sha2::{Digest, Sha256};
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

mod sessions;
use sessions::Sessions;
/// The largest request body the daemon reads: a call carries its arguments, not a file.
pub(crate) const LIMIT: usize = 4 << 20;
/// Sub-quota for unauthenticated `/t/` file transfers on a shared HTTP listener, keeping 32
/// connection slots available for `/mcp` control traffic.
const MAX_TRANSFER_CONNECTIONS: usize = 96;
/// Legacy HTTP sessions survive individual connections and have a separate count cap.
const MAX_HTTP_SESSIONS: usize = 256;

tokio::task_local! {
    static HTTP_REQ_LEGACY: bool;
}

/// Arguments for the Streamable HTTP front door: listener, auth digests, and shared state.
pub struct HttpServeArgs {
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
    /// Optional read-only skills catalog loaded into `daemon.memory`.
    pub skills: Option<Arc<SkillCatalog>>,
    pub service: Arc<Service>,
    pub transfers: Arc<Transfers>,
    pub memory: Arc<BytePool>,
    pub control_memory: Arc<BytePool>,
    pub stopped: CancellationToken,
}

/// Serves MCP over Streamable HTTP on `args.listener` until `args.stopped` is cancelled.
pub async fn serve(args: HttpServeArgs) {
    let HttpServeArgs {
        listener,
        digests,
        tools,
        launch,
        public_root,
        extra_instructions,
        instructions_tool,
        skills,
        service,
        transfers: table,
        memory,
        control_memory,
        stopped,
    } = args;
    let launch = Arc::new(
        launch
            .for_session()
            .with_memory(memory)
            .with_control_memory(control_memory),
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
    if let Some(catalog) = &skills {
        validate_skill_catalog(catalog);
    }
    let memory = Arc::clone(launch.memory());
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
            let is_legacy = HTTP_REQ_LEGACY.try_with(|v| *v).unwrap_or(false);
            let mut server = Server::with_shared(
                backend,
                Arc::clone(&router),
                Arc::clone(&instructions),
                tool_instructions.clone(),
                transfer,
                tools,
            )
            .with_skills(skills.clone())
            .with_legacy_session(is_legacy);
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
        memory,
        transfer_slots: Arc::new(Semaphore::new(MAX_TRANSFER_CONNECTIONS)),
        stopped: stopped.clone(),
        inner,
    };
    http_wire::serve_connections(
        listener,
        layer,
        stopped,
        Some(Arc::new(Semaphore::new(MAX_TRANSFER_CONNECTIONS))),
    )
    .await;
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

/// The token check and the body cap, in front of rmcp: a request without a token never reaches
/// a session, and one over the cap is refused before it is read.
#[derive(Clone)]
struct Bearer<H = Server> {
    /// Fixed-size digests shared by every request on this listener.
    digests: Arc<[[u8; 32]]>,
    /// The files this listener publishes under `/t/`.
    table: Arc<Transfers>,
    /// Memory budget bounding `/t/` path workspaces and streamed chunks.
    memory: Arc<BytePool>,
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
        let memory = Arc::clone(&self.memory);
        let transfer_slots = Arc::clone(&self.transfer_slots);
        let stopped = self.stopped.clone();
        Box::pin(async move {
            // A transfer is its own front door: no token, no body cap, and never a session.
            if request.uri().path().starts_with("/t/") {
                let Ok(permit) = transfer_slots.try_acquire_owned() else {
                    return Ok(http_wire::refusal(
                        request.version(),
                        StatusCode::SERVICE_UNAVAILABLE,
                        "",
                    ));
                };
                return Ok(http_wire::serve_transfer(
                    request,
                    table,
                    memory,
                    stopped,
                    Some(permit),
                )
                .await);
            }
            let version = request.version();
            let Some(principal) = authorized(request.headers(), &digests) else {
                return Ok(http_wire::refusal(
                    version,
                    StatusCode::UNAUTHORIZED,
                    "Bearer",
                ));
            };
            // Two routes, and no other: a proxy that forwards its prefix without stripping it
            // fails here, at `initialize`, rather than at a download an hour later.
            if request.uri().path() != "/mcp" {
                return Ok(http_wire::refusal(version, StatusCode::NOT_FOUND, ""));
            }
            // A declared length over the cap is refused before a byte of it is read; a chunked
            // body over the cap fails at `Limited`, and rmcp answers that as an error response.
            if declared_length(request.headers()).is_some_and(|length| length > LIMIT as u64) {
                return Ok(http_wire::refusal(
                    version,
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "",
                ));
            }
            let (mut parts, body) = request.into_parts();
            let is_legacy = is_legacy_http_request(&parts.headers);
            let lease_slot = HttpResponseLease::default();
            parts.extensions.insert(Principal(principal));
            parts.extensions.insert(lease_slot.clone());
            let mut lease_guard = LeaseSlotGuard(Some(lease_slot));
            let request = Request::from_parts(parts, Limited::new(body, LIMIT));
            let answer = HTTP_REQ_LEGACY
                .scope(is_legacy, async move {
                    inner
                        .call(request)
                        .await
                        .unwrap_or_else(|never| match never {})
                })
                .await;
            let (mut parts, body) = answer.into_parts();
            // A proxy that buffers would hold every event of a stream until the call ended.
            parts.headers.insert(
                HeaderName::from_static("x-accel-buffering"),
                "no".parse().expect("a header value"),
            );
            let leased = LeaseBody {
                inner: body,
                pending: None,
                slot: lease_guard.0.take().expect("lease slot"),
            };
            Ok(Response::from_parts(
                parts,
                leased.map_err(|never: Infallible| match never {}).boxed(),
            ))
        })
    }
}

fn is_legacy_http_request(headers: &HeaderMap) -> bool {
    if headers.contains_key("mcp-session-id") {
        return false;
    }
    match headers
        .get("mcp-protocol-version")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
    {
        Some(version) => version < "2026-07-28",
        None => true,
    }
}

struct LeaseSlotGuard(Option<HttpResponseLease>);

impl Drop for LeaseSlotGuard {
    fn drop(&mut self) {
        if let Some(slot) = self.0.take() {
            slot.close();
        }
    }
}

// Slice large single-frame JSON responses into small chunks so Hyper cannot drain and drop
// `LeaseBody` before the client's transport window accepts the bytes.
const LEASE_BODY_CHUNK_BYTES: usize = 16 * 1024;

struct LeaseBody<B> {
    inner: B,
    pending: Option<bytes::Bytes>,
    slot: HttpResponseLease,
}

impl<B> LeaseBody<B> {
    fn take_pending_chunk(&mut self) -> Option<bytes::Bytes> {
        let pending = self.pending.as_mut()?;
        if pending.len() > LEASE_BODY_CHUNK_BYTES {
            Some(pending.split_to(LEASE_BODY_CHUNK_BYTES))
        } else {
            self.pending.take()
        }
    }
}

impl<B> Drop for LeaseBody<B> {
    fn drop(&mut self) {
        self.slot.close();
    }
}

impl<B> hyper::body::Body for LeaseBody<B>
where
    B: hyper::body::Body<Data = bytes::Bytes> + Unpin,
{
    type Data = bytes::Bytes;
    type Error = B::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<hyper::body::Frame<Self::Data>, Self::Error>>> {
        if let Some(chunk) = self.take_pending_chunk() {
            return Poll::Ready(Some(Ok(hyper::body::Frame::data(chunk))));
        }
        match Pin::new(&mut self.inner).poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(mut data) => {
                    if data.len() > LEASE_BODY_CHUNK_BYTES {
                        let first = data.split_to(LEASE_BODY_CHUNK_BYTES);
                        self.pending = Some(data);
                        Poll::Ready(Some(Ok(hyper::body::Frame::data(first))))
                    } else {
                        Poll::Ready(Some(Ok(hyper::body::Frame::data(data))))
                    }
                }
                Err(other) => Poll::Ready(Some(Ok(other))),
            },
            other => other,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.pending.is_none() && self.inner.is_end_stream()
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        let mut hint = self.inner.size_hint();
        if let Some(pending) = &self.pending {
            let extra = pending.len() as u64;
            if let Some(upper) = hint.upper() {
                hint.set_upper(upper.saturating_add(extra));
            }
            hint.set_lower(hint.lower().saturating_add(extra));
        }
        hint
    }
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

#[cfg(test)]
mod tests;
