//! MCP stdio transport, request-ID memory leases, and the stdio server entrypoint.

use crate::config::{
    TRANSFER_ADDR, Tools, check_routable_addr, instructions_file, instructions_tool_mode,
    transfer_addr,
};
use crate::endpoint;
use crate::http;
use crate::ingress;
use crate::ipc;
use crate::lease;
use crate::mcp::{Backend, RequestLeases, Server};
use crate::service::Service;
use crate::session::LaunchContext;
use crate::tasks::{Limits, Tasks};
use crate::transfer::{self, TransferEndpoint};
use rmcp::{RoleServer, ServiceExt};
use std::sync::{Arc, OnceLock};
use tokio_util::sync::CancellationToken;

const STDIO_MAX_LINE_BYTES: usize = 16 << 20;

enum StdioLine {
    Line,
    Oversized(Option<rmcp::model::RequestId>),
    Overloaded(Option<rmcp::model::RequestId>),
    End,
}

enum StdioReadStep {
    Line,
    More,
    EndDiscard,
    Oversized(Option<rmcp::model::RequestId>),
    OversizedNeedsDiscard(Option<rmcp::model::RequestId>),
    Overloaded(Option<rmcp::model::RequestId>),
    OverloadedNeedsDiscard(Option<rmcp::model::RequestId>),
}

/// Custom MCP stdio transport that pairs request-ID leases with exact-capacity `Vec<u8>` writes so
/// cancelled calls, interleaved progress notifications, and large responses never leak capacity.
pub(crate) struct StdioTransport<R, W> {
    read: tokio::io::BufReader<R>,
    pub(super) line_buf: ingress::Buffer,
    pub(super) max_line_length: usize,
    discarding: bool,
    ready: bool,
    write: Arc<tokio::sync::Mutex<Option<W>>>,
    leases: Arc<RequestLeases>,
}

fn sniff_prefix_id(
    prefix: &[u8],
    next: &[u8],
    has_newline: bool,
) -> Option<rmcp::model::RequestId> {
    let cap = ingress::SNIFF_HEAD_LIMIT + 1;
    let mut buf = [0u8; ingress::SNIFF_HEAD_LIMIT + 1];
    let first = prefix.len().min(cap);
    buf[..first].copy_from_slice(&prefix[..first]);
    let second = next.len().min(cap - first);
    buf[first..first + second].copy_from_slice(&next[..second]);
    let complete =
        has_newline && prefix.len().saturating_add(next.len()) <= ingress::SNIFF_HEAD_LIMIT;
    ingress::sniff_request_id(&buf[..first + second], complete)
}

impl<R, W> StdioTransport<R, W>
where
    R: tokio::io::AsyncRead + Unpin,
{
    pub(crate) fn new(read: R, write: W, leases: Arc<RequestLeases>) -> Self {
        Self {
            read: tokio::io::BufReader::new(read),
            line_buf: ingress::Buffer::default(),
            max_line_length: STDIO_MAX_LINE_BYTES,
            discarding: false,
            ready: false,
            write: Arc::new(tokio::sync::Mutex::new(Some(write))),
            leases,
        }
    }

    fn reset_line_buf(&mut self) {
        self.line_buf = ingress::Buffer::default();
    }

    async fn read_line(&mut self) -> StdioLine {
        use tokio::io::AsyncBufReadExt;
        loop {
            let (consumed, step) = {
                let Ok(available) = self.read.fill_buf().await else {
                    return StdioLine::End;
                };
                if available.is_empty() {
                    return StdioLine::End;
                }
                let newline = available.iter().position(|&b| b == b'\n');
                let take = newline.map_or(available.len(), |idx| idx + 1);
                if self.discarding {
                    (
                        take,
                        newline
                            .map(|_| StdioReadStep::EndDiscard)
                            .unwrap_or(StdioReadStep::More),
                    )
                } else if self.line_buf.len().saturating_add(take) > self.max_line_length {
                    let id = sniff_prefix_id(&self.line_buf, &available[..take], newline.is_some());
                    self.reset_line_buf();
                    (
                        take,
                        if newline.is_some() {
                            StdioReadStep::Oversized(id)
                        } else {
                            StdioReadStep::OversizedNeedsDiscard(id)
                        },
                    )
                } else if self
                    .line_buf
                    .append(&available[..take], self.max_line_length)
                    .is_err()
                {
                    let id = sniff_prefix_id(&self.line_buf, &available[..take], newline.is_some());
                    self.reset_line_buf();
                    (
                        take,
                        if newline.is_some() {
                            StdioReadStep::Overloaded(id)
                        } else {
                            StdioReadStep::OverloadedNeedsDiscard(id)
                        },
                    )
                } else {
                    (
                        take,
                        if newline.is_some() {
                            StdioReadStep::Line
                        } else {
                            StdioReadStep::More
                        },
                    )
                }
            };
            self.read.consume(consumed);
            match step {
                StdioReadStep::Line => return StdioLine::Line,
                StdioReadStep::More => {}
                StdioReadStep::EndDiscard => self.discarding = false,
                StdioReadStep::Oversized(id) => return StdioLine::Oversized(id),
                StdioReadStep::OversizedNeedsDiscard(id) => {
                    self.discarding = true;
                    return StdioLine::Oversized(id);
                }
                StdioReadStep::Overloaded(id) => return StdioLine::Overloaded(id),
                StdioReadStep::OverloadedNeedsDiscard(id) => {
                    self.discarding = true;
                    return StdioLine::Overloaded(id);
                }
            }
        }
    }
}

impl<R, W: tokio::io::AsyncWrite + Unpin> StdioTransport<R, W> {
    async fn send_local_error(
        &mut self,
        error: rmcp::model::ErrorData,
        id: Option<rmcp::model::RequestId>,
    ) -> bool {
        let reply = rmcp::service::TxJsonRpcMessage::<RoleServer>::error(error, id);
        let Ok(size) = ipc::json_len(&reply).map(|n| n.saturating_add(1)) else {
            return true;
        };
        let Some(mut lease) = lease::control_pool()
            .try_acquire(size)
            .or_else(|| lease::process_pool().try_acquire(size))
        else {
            return true;
        };
        let Ok(bytes) = encode_stdio_frame(&reply, Some(&mut lease)) else {
            return true;
        };
        write_stdio_frame(&self.write, &bytes).await.is_ok()
    }
}

impl<R, W> Drop for StdioTransport<R, W> {
    fn drop(&mut self) {
        self.leases.close();
    }
}

async fn write_stdio_frame<W: tokio::io::AsyncWrite + Unpin>(
    write: &tokio::sync::Mutex<Option<W>>,
    bytes: &[u8],
) -> std::io::Result<()> {
    let mut guard = write.lock().await;
    let Some(writer) = guard.as_mut() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "stdio transport is closed",
        ));
    };
    tokio::io::AsyncWriteExt::write_all(writer, bytes).await?;
    tokio::io::AsyncWriteExt::flush(writer).await
}

pub(super) fn encode_stdio_frame<T: serde::Serialize>(
    item: &T,
    outbound: Option<&mut lease::Lease>,
) -> std::io::Result<Vec<u8>> {
    let length = ipc::json_len(item)?.checked_add(1).ok_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::InvalidData, "stdio frame is too large")
    })?;
    if length > ipc::RESPONSE_LIMIT {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "stdio frame exceeds 512 MiB",
        ));
    }
    if let Some(lease) = outbound
        && !lease.try_resize(length)
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::OutOfMemory,
            "stdio frame exceeded active memory budget",
        ));
    }
    let mut bytes = Vec::with_capacity(length);
    serde_json::to_writer(&mut bytes, item).map_err(std::io::Error::other)?;
    bytes.push(b'\n');
    Ok(bytes)
}

struct StdioSendPayload {
    item: rmcp::service::TxJsonRpcMessage<RoleServer>,
    staging: Option<lease::Lease>,
    outbound: Option<lease::Lease>,
    payload_owner: lease::Owner,
}

struct StdioFrame {
    bytes: Vec<u8>,
    _lease: Option<lease::Lease>,
}

impl StdioSendPayload {
    fn encode(mut self) -> std::io::Result<StdioFrame> {
        if self.outbound.is_none() {
            let size = ipc::json_len(&self.item)?.saturating_add(1);
            self.outbound = lease::process_pool().try_acquire(size).or_else(|| {
                (size <= 64 * 1024)
                    .then(|| lease::control_pool().try_acquire(size))
                    .flatten()
            });
            if self.outbound.is_none() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    "stdio frame exceeded active memory budget",
                ));
            }
        }
        let bytes = encode_stdio_frame(&self.item, self.outbound.as_mut())?;
        drop(self.item);
        drop(self.staging);
        drop(self.payload_owner);
        Ok(StdioFrame {
            bytes,
            _lease: self.outbound,
        })
    }
}

impl<R, W> rmcp::transport::Transport<RoleServer> for StdioTransport<R, W>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
    W: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    type Error = std::io::Error;

    fn send(
        &mut self,
        item: rmcp::service::TxJsonRpcMessage<RoleServer>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let write = Arc::clone(&self.write);
        let leases = Arc::clone(&self.leases);
        let response_id = match &item {
            rmcp::model::JsonRpcMessage::Response(resp) => Some(&resp.id),
            rmcp::model::JsonRpcMessage::Error(err) => err.id.as_ref(),
            _ => None,
        };
        let (staging_lease, outbound_lease, payload_owner) =
            match response_id.and_then(|id| leases.take(id)) {
                Some((staging, outbound, owner)) => (Some(staging), Some(outbound), owner),
                None => (None, None, Default::default()),
            };
        let payload = StdioSendPayload {
            item,
            staging: staging_lease,
            outbound: outbound_lease,
            payload_owner,
        };
        async move {
            let frame = payload.encode()?;
            write_stdio_frame(&write, &frame.bytes).await
        }
    }

    async fn receive(&mut self) -> Option<rmcp::service::RxJsonRpcMessage<RoleServer>> {
        loop {
            match self.read_line().await {
                StdioLine::Line => {}
                StdioLine::Oversized(id) => {
                    if !self
                        .send_local_error(
                            rmcp::model::ErrorData::invalid_request(
                                "Request exceeds maximum stdio line size of 16 MiB.",
                                None,
                            ),
                            id,
                        )
                        .await
                    {
                        return None;
                    }
                    continue;
                }
                StdioLine::Overloaded(id) => {
                    if !self
                        .send_local_error(
                            rmcp::model::ErrorData::internal_error(
                                "Server active memory budget is full while reading the request.",
                                None,
                            ),
                            id,
                        )
                        .await
                    {
                        return None;
                    }
                    continue;
                }
                StdioLine::End => return None,
            }
            let parsed = std::mem::take(&mut self.line_buf).parse(true);
            match parsed {
                Ok(Some(parsed)) => {
                    let msg = parsed.into_message();
                    match &msg {
                        rmcp::model::JsonRpcMessage::Request(req) => {
                            if !matches!(
                                req.request,
                                rmcp::model::ClientRequest::DiscoverRequest(_)
                                    | rmcp::model::ClientRequest::PingRequest(_)
                            ) {
                                self.ready = true;
                            }
                            self.leases.on_request(&req.id);
                        }
                        rmcp::model::JsonRpcMessage::Notification(noti) => {
                            if !self.ready
                                || matches!(
                                    noti.notification,
                                    rmcp::model::ClientNotification::CustomNotification(_)
                                )
                            {
                                continue;
                            }
                            if let rmcp::model::ClientNotification::CancelledNotification(cancel) =
                                &noti.notification
                                && let Some(id) = &cancel.params.request_id
                                && !self.leases.on_cancel_owned(id, {
                                    use rmcp::model::GetExtensions;
                                    noti.notification
                                        .extensions()
                                        .get::<crate::lease::Owner>()
                                        .cloned()
                                        .unwrap_or_default()
                                })
                            {
                                continue;
                            }
                        }
                        _ if !self.ready => continue,
                        _ => {}
                    }
                    return Some(msg);
                }
                Ok(None) => continue,
                Err(ingress::Failure::Budget { id }) => {
                    if !self
                        .send_local_error(
                            rmcp::model::ErrorData::internal_error(
                                "Server active memory budget is full while reading the request.",
                                None,
                            ),
                            id,
                        )
                        .await
                    {
                        return None;
                    }
                }
                Err(ingress::Failure::Syntax) => {
                    if !self
                        .send_local_error(
                            rmcp::model::ErrorData::parse_error("Parse error", None),
                            None,
                        )
                        .await
                    {
                        return None;
                    }
                }
                Err(ingress::Failure::Limit) => {
                    if !self
                        .send_local_error(
                            rmcp::model::ErrorData::invalid_request(
                                "Request exceeds maximum stdio line size of 16 MiB.",
                                None,
                            ),
                            None,
                        )
                        .await
                    {
                        return None;
                    }
                }
                Err(ingress::Failure::InvalidRequest { id }) => {
                    if !self
                        .send_local_error(
                            rmcp::model::ErrorData::invalid_request("Invalid request", None),
                            id,
                        )
                        .await
                    {
                        return None;
                    }
                }
                Err(ingress::Failure::InvalidParams { id }) => {
                    if !self
                        .send_local_error(
                            rmcp::model::ErrorData::invalid_params("Invalid params", None),
                            Some(id),
                        )
                        .await
                    {
                        return None;
                    }
                }
            }
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.leases.close();
        let mut guard = self.write.lock().await;
        drop(guard.take());
        Ok(())
    }
}

#[tokio::main]
pub(crate) async fn serve() {
    let tools = match Tools::from_env() {
        Ok(tools) => tools,
        Err(message) => {
            eprintln!("fastexec: {message}");
            std::process::exit(1);
        }
    };
    let instructions_tool = match instructions_tool_mode() {
        Ok(mode) => mode,
        Err(message) => {
            eprintln!("fastexec: {message}");
            std::process::exit(1);
        }
    };
    let transfer_cfg = if tools.transfer {
        let root = match http::public_root() {
            Ok(root) => root,
            Err(message) => {
                eprintln!("fastexec: {message}");
                std::process::exit(1);
            }
        };
        let addr = match transfer_addr() {
            Ok(addr) => addr,
            Err(message) => {
                eprintln!("fastexec: {message}");
                std::process::exit(1);
            }
        };
        Some((root, addr))
    } else {
        None
    };
    // Captured once, at startup: the client's own environment is the server's own here.
    let launch = match LaunchContext::current() {
        Ok(launch) => Arc::new(launch),
        Err(error) => {
            eprintln!("fastexec: cannot read the launch context: {error}");
            std::process::exit(1);
        }
    };
    // `on` forwards every call to the namespace's daemon and keeps no tasks of its own.
    let daemon = match std::env::var("FASTEXEC_DAEMON") {
        Err(std::env::VarError::NotPresent) => false,
        Ok(value) if value == "off" => false,
        Ok(value) if value == "on" => true,
        Ok(value) => {
            eprintln!("fastexec: invalid FASTEXEC_DAEMON {value:?}; expected on or off");
            std::process::exit(1);
        }
        Err(error) => {
            eprintln!("fastexec: invalid FASTEXEC_DAEMON {error}; expected on or off");
            std::process::exit(1);
        }
    };
    let extra_instructions = match instructions_file(&launch) {
        Ok(extra) => extra,
        Err(message) => {
            eprintln!("fastexec: {message}");
            std::process::exit(1);
        }
    };
    let (backend, tasks, transfer_endpoint, transfer_stop) = if daemon {
        let endpoint = match endpoint::Endpoint::new(Arc::clone(&launch)) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                eprintln!("fastexec: cannot name this endpoint's session: {error}");
                std::process::exit(1);
            }
        };
        (Backend::Daemon(Arc::new(endpoint)), None, None, None)
    } else {
        let (transfer_endpoint, transfer_stop) = match transfer_cfg {
            Some((public_root, requested_addr)) => {
                if let Some(addr) = requested_addr
                    && let Err(message) =
                        check_routable_addr(TRANSFER_ADDR, addr, public_root.is_some())
                {
                    eprintln!("fastexec: {message}");
                    std::process::exit(1);
                }
                let addr = requested_addr
                    .unwrap_or_else(|| std::net::SocketAddr::from(([127, 0, 0, 1], 0)));
                let listener = match tokio::net::TcpListener::bind(addr).await {
                    Ok(listener) => listener,
                    Err(error) => {
                        eprintln!("fastexec: cannot listen on {addr} for transfer: {error}");
                        std::process::exit(1);
                    }
                };
                let bound = match listener.local_addr() {
                    Ok(bound) => bound,
                    Err(error) => {
                        eprintln!("fastexec: cannot read transfer listener address: {error}");
                        std::process::exit(1);
                    }
                };
                let table = Arc::new(transfer::Transfers::new());
                let stop = CancellationToken::new();
                tokio::spawn(http::serve_transfers(
                    listener,
                    Arc::clone(&table),
                    stop.clone(),
                ));
                let base: Arc<str> =
                    Arc::from(public_root.unwrap_or_else(|| format!("http://{bound}/")));
                (Some(TransferEndpoint::Fixed { table, base }), Some(stop))
            }
            None => (None, None),
        };
        let tasks = match Tasks::new(Limits::OFF) {
            Ok(tasks) => tasks,
            Err(error) => {
                eprintln!("fastexec: cannot create the log directory: {error}");
                std::process::exit(1);
            }
        };
        let backend = Backend::Local {
            service: Arc::new(Service::new(Arc::clone(&tasks), false)),
            session: Arc::new(OnceLock::new()),
            launch: Arc::clone(&launch),
            daemon: false,
        };
        (backend, Some(tasks), transfer_endpoint, transfer_stop)
    };
    let stdio_leases = Arc::new(RequestLeases::default());
    let server = Server::new(
        backend,
        &launch,
        transfer_endpoint,
        tools,
        extra_instructions.as_deref(),
        instructions_tool,
    )
    .with_stdio_leases(Arc::clone(&stdio_leases));
    let eof = Arc::new(tokio::sync::Notify::new());
    let stdin = EofSignal {
        inner: tokio::io::stdin(),
        eof: Arc::clone(&eof),
    };
    let transport = StdioTransport::new(stdin, tokio::io::stdout(), stdio_leases);
    // Signals are watched from the start, so an exit during initialization still cleans up.
    let signal = shutdown_signal();
    tokio::pin!(signal);
    // The backend outlives the serve call, which consumes the handler.
    let backend = server.backend.clone();
    let served = tokio::select! {
        served = server.serve(transport) => Some(served),
        _ = &mut signal => None,
    };
    let mut exit_code = 0;
    match served {
        Some(Ok(service)) => {
            // Stdin EOF ends the session at once; rmcp would first drain in-flight waits.
            tokio::select! {
                _ = service.waiting() => {}
                _ = eof.notified() => {}
                _ = &mut signal => {}
            }
        }
        Some(Err(error)) => {
            eprintln!("fastexec: MCP initialization failed: {error}");
            exit_code = 1;
        }
        None => {}
    }
    match &backend {
        // The endpoint never kills tasks: they belong to the daemon, and other sessions may be
        // using them.
        Backend::Daemon(endpoint) => endpoint.bye().await,
        Backend::Local { .. } => {
            if let Some(stop) = transfer_stop {
                stop.cancel();
            }
            if let Some(tasks) = tasks {
                tasks.shutdown();
            }
        }
    }
    std::process::exit(exit_code);
}

/// Stdin wrapper that reports EOF: the host closing stdin is the MCP stdio shutdown signal.
struct EofSignal<R> {
    inner: R,
    eof: Arc<tokio::sync::Notify>,
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for EofSignal<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let result = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
        if matches!(result, std::task::Poll::Ready(Ok(()))) && buf.filled().len() == before {
            self.eof.notify_one();
        }
        result
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        let mut hangup = signal(SignalKind::hangup()).expect("SIGHUP handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = hangup.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(windows)]
    let _ = tokio::signal::ctrl_c().await;
}
