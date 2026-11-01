//! Streamable HTTP: the daemon's second front door, for clients that speak MCP over HTTP instead
//! of the stdio endpoint or the namespace's pipe.

use crate::daemon::Daemon;
use crate::service::Render;
use crate::session::LaunchContext;
use crate::{Backend, INSTRUCTIONS_DAEMON, Server};
use bytes::Bytes;
use http::{HeaderMap, Request, Response, StatusCode, header};
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
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Duration;
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
type Reply = Response<BoxBody<Bytes, Infallible>>;

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
    let factory = move || {
        Ok(Server {
            backend: Backend::Local {
                service: Arc::clone(&service),
                render,
                session: Arc::new(OnceLock::new()),
                launch: Arc::clone(&launch),
                instructions: INSTRUCTIONS_DAEMON,
            },
        })
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
        Box::pin(async move {
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
            Ok(inner
                .call(request)
                .await
                .unwrap_or_else(|never| match never {}))
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
        .body(Full::new(Bytes::new()).boxed())
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
