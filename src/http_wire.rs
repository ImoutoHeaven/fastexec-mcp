//! HTTP/1.1 and HTTP/2 connection loop and unauthenticated `/t/` file transfers.

use crate::files::path::{native_path_bytes, open_regular, open_regular_for_upload};
use crate::files::write::create_target_parents_in;
use crate::lease::{BytePool, Lease};
use crate::transfer::{self, Admitted, Transfers};
use ::http::{HeaderMap, Method, Request, Response, StatusCode, Version, header};
use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use hyper_util::service::TowerToHyperService;
use std::convert::Infallible;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, ReadBuf};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

/// HTTP has its own connection budget, so unauthenticated peers cannot consume the daemon
/// socket's management capacity or exhaust the process's file descriptors.
const MAX_CONNECTIONS: usize = 128;
const MAX_REJECT_WRITERS: usize = 32;
const BUSY_503: &[u8] =
    b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";
/// The deadline for a complete HTTP request header.
const HEADER_TIMEOUT: Duration = Duration::from_secs(10);
/// Per-connection HTTP/2 stream cap.
const HTTP2_MAX_CONCURRENT_STREAMS: u32 = 64;
/// HTTP/2 PING interval for keeping idle multiplexed connections alive through proxies.
const HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(30);
/// An upload may continue indefinitely while it delivers data, but not hold a file idle.
const UPLOAD_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// The bytes a streamed file is read in.
const CHUNK: usize = 32 * 1024;

/// Boxed HTTP response whose body may fail mid-stream while reading a transferred file.
pub(crate) type Reply = Response<BoxBody<Bytes, std::io::Error>>;

/// Serves `/t/` capabilities on `listener` until `stopped` is cancelled.
pub async fn serve_transfers(
    listener: TcpListener,
    table: Arc<Transfers>,
    memory: Arc<BytePool>,
    stopped: CancellationToken,
) {
    let layer = TransferOnly {
        table,
        memory,
        stopped: stopped.clone(),
    };
    serve_connections(listener, layer, stopped, None).await;
}

pub(crate) async fn serve_connections<S>(
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
    memory: Arc<BytePool>,
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
        let memory = Arc::clone(&self.memory);
        let stopped = self.stopped.clone();
        Box::pin(async move {
            if request.uri().path().starts_with("/t/") {
                return Ok(serve_transfer(request, table, memory, stopped, None).await);
            }
            Ok(refusal(request.version(), StatusCode::NOT_FOUND, ""))
        })
    }
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

/// A refusal with no body, so nothing about why is said to a caller that was not admitted.
pub(crate) fn refusal(version: Version, status: StatusCode, challenge: &str) -> Reply {
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

/// Answers one `/t/` request: a download, the size of a file, or an upload.
pub(crate) async fn serve_transfer<B>(
    request: Request<B>,
    table: Arc<Transfers>,
    memory: Arc<BytePool>,
    stopped: CancellationToken,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Reply
where
    B: hyper::body::Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Send,
{
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
        Method::GET => transfer_get(version, request.headers(), admitted, memory, permit).await,
        Method::HEAD => transfer_head(version, admitted, &memory).await,
        Method::PUT => transfer_put(request, admitted, memory, stopped).await,
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
    pool: Arc<BytePool>,
    operation: impl FnOnce(&Path) -> std::io::Result<T> + Send + 'static,
) -> std::io::Result<T> {
    tokio::task::spawn_blocking(move || {
        let memory = || {
            std::io::Error::new(
                std::io::ErrorKind::OutOfMemory,
                "Server active memory budget is full while preparing a transfer file operation.",
            )
        };
        let bytes = native_path_bytes(&path).ok_or_else(memory)?;
        let _lease = pool.try_acquire(bytes).ok_or_else(memory)?;
        operation(&path)
    })
    .await
    .map_err(std::io::Error::other)?
}

async fn presence(path: Arc<PathBuf>, memory: &Arc<BytePool>) -> Presence {
    match transfer_file(path, Arc::clone(memory), |path| std::fs::metadata(path)).await {
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
    memory: Arc<BytePool>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) -> Reply {
    let path = admitted.path();
    let (file, metadata) = match transfer_file(path, Arc::clone(&memory), open_regular).await {
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
                memory,
                _admitted: admitted,
                _permit: permit,
            }
            .boxed(),
        )
        .expect("a response with a body")
}

/// Reports the current size of a file, which an upload can resume from.
async fn transfer_head(version: Version, admitted: Admitted, memory: &Arc<BytePool>) -> Reply {
    let length = match presence(admitted.path(), memory).await {
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
async fn transfer_put<B>(
    request: Request<B>,
    admitted: Admitted,
    memory: Arc<BytePool>,
    stopped: CancellationToken,
) -> Reply
where
    B: hyper::body::Body<Data = Bytes> + Send + Unpin + 'static,
    B::Error: Send,
{
    let version = request.version();
    let offset = match offset_of(request.uri().query()) {
        Ok(offset) => offset,
        Err(()) => return transfer_empty(version, StatusCode::BAD_REQUEST, &[]),
    };
    // A disconnected request waiting for another upload must release its file descriptor and
    // its place in the lock queue. Keep preparation in this handler, whose drop cancels it.
    let target = match prepare_upload(&admitted, offset, &memory).await {
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

async fn prepare_upload(
    admitted: &Admitted,
    offset: u64,
    memory: &Arc<BytePool>,
) -> Result<UploadFile, StatusCode> {
    let path = admitted.path();
    match presence(Arc::clone(&path), memory).await {
        Presence::NotAFile => return Err(StatusCode::CONFLICT),
        Presence::Failed => return Err(StatusCode::INTERNAL_SERVER_ERROR),
        Presence::Missing if offset > 0 => return Err(StatusCode::CONFLICT),
        Presence::Missing => {
            let requested = Arc::clone(&path);
            let pool = Arc::clone(memory);
            tokio::task::spawn_blocking(move || create_target_parents_in(&requested, &pool))
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        }
        Presence::File(_) => {}
    }
    // Opening without truncation lets aliases discover the same file identity, including a
    // file which did not exist when its URLs were minted. All mutations happen under its lock.
    let file = match transfer_file(path, Arc::clone(memory), move |path| {
        open_regular_for_upload(path, offset == 0)
    })
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
fn transfer_response(status: StatusCode, length: Option<u64>) -> ::http::response::Builder {
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

pub(crate) fn transfer_empty(
    version: Version,
    status: StatusCode,
    extra: &[(&str, &str)],
) -> Reply {
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
    _lease: Lease,
}

impl AsRef<[u8]> for DownloadChunk {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

fn copy_download_chunk(source: &[u8], memory: &Arc<BytePool>) -> std::io::Result<Bytes> {
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
    memory: Arc<BytePool>,
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
mod tests {
    use super::*;

    async fn download_fixture(
        content: &[u8],
        memory: Arc<BytePool>,
    ) -> (PathBuf, Arc<Transfers>, FileBody) {
        let directory = std::env::temp_dir().join(format!(
            "fastexec-download-budget-{}",
            crate::id::random_chars(12).unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("ordinary.txt");
        std::fs::write(&path, content).unwrap();
        let table = Arc::new(Transfers::new());
        let id = table.create(path.clone()).unwrap();
        let file = tokio::fs::File::open(&path).await.unwrap();
        let body = FileBody {
            file: Some(file.take(content.len() as u64)),
            memory,
            _admitted: table.admit(&id).unwrap(),
            _permit: None,
        };
        (directory, table, body)
    }

    #[tokio::test]
    async fn download_chunk_admission_ends_a_refused_body() {
        const CONTENT: &[u8] = b"ordinary";
        for cap in [CONTENT.len() - 1, CONTENT.len(), CONTENT.len() + 1] {
            let memory = BytePool::new(cap);
            let (directory, _table, mut body) =
                download_fixture(CONTENT, Arc::clone(&memory)).await;
            let frame = body.frame().await.unwrap();
            if cap < CONTENT.len() {
                assert_eq!(frame.unwrap_err().kind(), std::io::ErrorKind::OutOfMemory);
                assert!(hyper::body::Body::is_end_stream(&body));
                assert!(body.frame().await.is_none());
            } else {
                let bytes = frame.unwrap().into_data().unwrap();
                assert_eq!(&bytes[..], CONTENT);
                assert_eq!(memory.used(), CONTENT.len());
                drop(bytes);
            }
            drop(body);
            assert_eq!(memory.used(), 0);
            std::fs::remove_dir_all(directory).unwrap();
        }
    }

    #[tokio::test]
    async fn download_frame_slices_keep_their_lease_after_body_drop() {
        const CONTENT: &[u8] = b"ordinary";
        let memory = BytePool::new(CONTENT.len());
        let (directory, table, mut body) = download_fixture(CONTENT, Arc::clone(&memory)).await;
        let bytes = body.frame().await.unwrap().unwrap().into_data().unwrap();
        let retained = bytes.slice(1..);
        assert_eq!(memory.used(), CONTENT.len());
        drop(bytes);
        drop(body);
        assert!(table.is_idle(std::time::Instant::now() + transfer::IDLE));
        assert_eq!(memory.used(), CONTENT.len());
        assert_eq!(&retained[..], &CONTENT[1..]);
        drop(retained);
        assert_eq!(memory.used(), 0);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test]
    async fn native_file_worker_admits_before_io_and_releases_success_and_error() {
        let directory = std::env::temp_dir().join(format!(
            "fastexec-native-transfer-{}",
            crate::id::random_chars(12).unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = Arc::new(directory.join("ordinary.txt"));
        let required = native_path_bytes(&path).unwrap();
        for cap in [required - 1, required, required + 1] {
            let pool = BytePool::new(cap);
            let operation_pool = Arc::clone(&pool);
            let result = transfer_file(Arc::clone(&path), Arc::clone(&pool), move |path| {
                assert_eq!(operation_pool.used(), required);
                std::fs::File::create(path)
            })
            .await;
            if cap < required {
                assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::OutOfMemory);
                assert!(!path.exists(), "refusal must precede the native file call");
            } else {
                drop(result.unwrap());
                assert!(path.exists());
                std::fs::remove_file(path.as_path()).unwrap();
            }
            assert_eq!(pool.used(), 0);
        }
        let pool = BytePool::new(required);
        let operation_pool = Arc::clone(&pool);
        let error = transfer_file(path, Arc::clone(&pool), move |path| {
            assert_eq!(operation_pool.used(), required);
            std::fs::metadata(path)
        })
        .await
        .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        assert_eq!(pool.used(), 0);
        std::fs::remove_dir(directory).unwrap();
    }

    #[tokio::test]
    async fn native_file_worker_keeps_its_lease_after_waiter_cancellation() {
        let path = Arc::new(std::env::temp_dir());
        let required = native_path_bytes(&path).unwrap();
        let pool = BytePool::new(required);
        let worker_pool = Arc::clone(&pool);
        let operation_pool = Arc::clone(&pool);
        let (ready, observed) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let waiter = tokio::spawn(async move {
            transfer_file(path, worker_pool, move |path| {
                assert_eq!(operation_pool.used(), required);
                ready.send(()).unwrap();
                blocked.recv_timeout(Duration::from_secs(5)).unwrap();
                assert_eq!(operation_pool.used(), required);
                std::fs::metadata(path)
            })
            .await
        });
        observed.await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        assert_eq!(pool.used(), required);
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while pool.used() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn metadata_worker_keeps_the_shared_path_after_its_waiter_is_dropped() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let table = Arc::new(Transfers::new());
            let memory = BytePool::new(1 << 20);
            let id = table
                .create(PathBuf::from(format!(
                    "future-transfer-{}",
                    crate::id::random_chars(12).unwrap()
                )))
                .unwrap();
            let admitted = table.admit(&id).unwrap();
            let shared = admitted.path();
            let weak = Arc::downgrade(&shared);
            let (ready, observed) = tokio::sync::oneshot::channel();
            let (release, blocked) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                ready.send(()).unwrap();
                blocked.recv_timeout(Duration::from_secs(5)).unwrap();
            });
            observed.await.unwrap();
            let mut pending = Box::pin(presence(shared, &memory));
            std::future::poll_fn(|cx| {
                assert!(pending.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(pending);
            drop(admitted);
            assert!(table.is_idle(std::time::Instant::now() + transfer::IDLE));
            assert!(
                weak.upgrade().is_some(),
                "the queued worker still owns the path"
            );
            release.send(()).unwrap();
            blocker.await.unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while weak.upgrade().is_some() || memory.used() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        });
    }

    // Windows may refuse replacing a destination with an open handle.
    #[cfg(unix)]
    #[tokio::test]
    async fn downloads_size_the_same_open_handle_it_streams() {
        let directory = std::env::temp_dir().join(format!(
            "fastexec-transfer-replace-{}",
            crate::id::random_chars(12).unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("file.txt");
        let replacement = directory.join("replacement.txt");
        let table = Arc::new(Transfers::new());
        let memory = BytePool::new(1 << 20);
        let id = table.create(path.clone()).unwrap();
        for partial in [false, true] {
            std::fs::write(&path, b"original").unwrap();
            std::fs::write(&replacement, b"new").unwrap();
            let mut headers = HeaderMap::new();
            if partial {
                headers.insert(header::RANGE, "bytes=3-".parse().unwrap());
            }
            let response = transfer_get(
                Version::HTTP_11,
                &headers,
                table.admit(&id).unwrap(),
                Arc::clone(&memory),
                None,
            )
            .await;
            std::fs::rename(&replacement, &path).unwrap();
            let (status, length, body) = if partial {
                assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes 3-7/8");
                (StatusCode::PARTIAL_CONTENT, "5", &b"ginal"[..])
            } else {
                (StatusCode::OK, "8", &b"original"[..])
            };
            assert_eq!(response.status(), status);
            assert_eq!(response.headers()[header::CONTENT_LENGTH], length);
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                body
            );
            assert_eq!(memory.used(), 0);
        }
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn idle_upload_flushes_accepted_prefix_and_releases_turn() {
        use hyper::body::Frame;
        struct Stalled(Option<Bytes>);
        impl hyper::body::Body for Stalled {
            type Data = Bytes;
            type Error = Infallible;
            fn poll_frame(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
                match self.0.take() {
                    Some(bytes) => Poll::Ready(Some(Ok(Frame::data(bytes)))),
                    None => Poll::Pending,
                }
            }
        }

        let directory = std::env::temp_dir().join(format!(
            "fastexec-transfer-idle-{}",
            crate::id::random_chars(12).unwrap()
        ));
        let path = directory.join("file.txt");
        let table = Arc::new(Transfers::new());
        let memory = BytePool::new(1 << 20);
        let id = table.create(path.clone()).unwrap();
        let admitted = table.admit(&id).unwrap();
        let target = prepare_upload(&admitted, 0, &memory).await.unwrap();
        let input = Stalled(Some(Bytes::from_static(b"accepted")));
        let response = write_upload(
            Version::HTTP_11,
            admitted,
            target,
            0,
            input,
            CancellationToken::new(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
        assert_eq!(std::fs::read(&path).unwrap(), b"accepted");
        let admitted = table.admit(&id).unwrap();
        let resume = prepare_upload(&admitted, 8, &memory).await.unwrap();
        let response = write_upload(
            Version::HTTP_11,
            admitted,
            resume,
            8,
            Full::new(Bytes::from_static(b"!")),
            CancellationToken::new(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(std::fs::read(&path).unwrap(), b"accepted!");
        assert_eq!(memory.used(), 0);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
