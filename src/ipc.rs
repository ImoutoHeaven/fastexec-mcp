//! The wire between an endpoint and the daemon: length-prefixed JSON frames and the messages they
//! carry.

use crate::lease::Owner;
use crate::lease::{BytePool, Lease};
#[cfg(test)]
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::ffi::{OsStr, OsString};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// What a Hello and a Welcome must agree on.
pub const PROTOCOL: u32 = 4;

/// The largest frame the daemon accepts. A Hello carries the endpoint's whole environment, which
/// is the reason this is larger than a call.
pub const REQUEST_LIMIT: usize = 4 << 20;

/// The largest serialized arguments a call may carry. The endpoint refuses a larger call itself,
/// as a tool error, before it writes anything.
pub const CALL_LIMIT: usize = 3 << 20;

/// The largest frame the endpoint accepts. Results are sized to fit, so a larger one is a bug and
/// the endpoint closes the connection.
pub const RESPONSE_LIMIT: usize = 512 << 20;

const WRITE_CHUNK: usize = 64 * 1024;

/// The payload is destroyed before its last process-local byte owner.
#[derive(Debug)]
pub(crate) struct Owned<T> {
    pub value: T,
    pub owner: Owner,
}

impl<T> Owned<T> {
    pub fn new(value: T, owner: Owner) -> Self {
        Self { value, owner }
    }

    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Owned<U> {
        let Self { value, owner } = self;
        Owned::new(f(value), owner)
    }

    /// A derived value may reserve additional buffers while keeping its input owner.
    pub fn flat_map<U>(self, f: impl FnOnce(T) -> Owned<U>) -> Owned<U> {
        let Self { value, owner } = self;
        let result = f(value);
        Owned::new(result.value, Owner::new((owner, result.owner)))
    }

    /// Capturing this whole wrapper also covers a blocking job that has not started.
    pub fn run<U>(self, f: impl FnOnce(T) -> U) -> U {
        let Self { value, owner } = self;
        let result = f(value);
        drop(owner);
        result
    }
}

pub(crate) struct EncodedFrame {
    pub bytes: Vec<u8>,
    _lease: Lease,
}

pub(crate) const SHORT_INPUT: usize = 64 * 1024;

pub(crate) fn input_budget_error() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::OutOfMemory,
        "Server active memory budget is full while preparing the IPC request; the operation was not dispatched",
    )
}

fn input_lease(
    bytes: usize,
    main: &Arc<BytePool>,
    control: &Arc<BytePool>,
    short_control: bool,
) -> Option<(Lease, bool)> {
    main.try_acquire(bytes)
        .map(|lease| (lease, false))
        .or_else(|| {
            short_control
                .then(|| control.try_acquire(bytes).map(|lease| (lease, true)))
                .flatten()
        })
}

/// Measures and reserves the independent frame before its only encoding allocation.
pub(crate) fn encode_frame_leased<T: Serialize + ?Sized>(
    message: &T,
    main: &Arc<BytePool>,
    control: &Arc<BytePool>,
    allow_control: bool,
) -> std::io::Result<EncodedFrame> {
    let length = frame_len(message)?;
    let (mut lease, _) = input_lease(
        length,
        main,
        control,
        allow_control && length <= SHORT_INPUT,
    )
    .ok_or_else(input_budget_error)?;
    let bytes = encode_measured(message, length)?;
    // Vec::with_capacity uses exactly the requested element capacity here.
    if !lease.try_resize(bytes.capacity()) {
        return Err(input_budget_error());
    }
    Ok(EncodedFrame {
        bytes,
        _lease: lease,
    })
}

pub(crate) fn daemon_control(message: &ToDaemon) -> bool {
    match message {
        ToDaemon::Cancel { .. }
        | ToDaemon::EventsCancel { .. }
        | ToDaemon::Bye
        | ToDaemon::Status
        | ToDaemon::Stop
        | ToDaemon::Hello { .. }
        | ToDaemon::HelloEmit { .. } => true,
        ToDaemon::EmitEvent { .. }
        | ToDaemon::EventsList { .. }
        | ToDaemon::EventsPoll { .. }
        | ToDaemon::EventsStream { .. } => false,
        ToDaemon::Call {
            tool, arguments, ..
        } => {
            tool == "exec"
                && arguments
                    .get("action")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|action| matches!(action, "list" | "kill"))
        }
    }
}

pub(crate) async fn write_request_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    message: &ToDaemon,
    main: &Arc<BytePool>,
    control: &Arc<BytePool>,
) -> std::io::Result<()> {
    let frame = encode_frame_leased(message, main, control, daemon_control(message))?;
    write_raw(writer, &frame.bytes).await
}

pub(crate) enum DaemonFrame {
    Message(Owned<ToDaemon>),
    Overload { id: Option<u64> },
    OverloadEvent { id: Option<u64> },
    Eof,
}

fn sniff_daemon_overload(head: &[u8]) -> DaemonFrame {
    let text = std::str::from_utf8(head)
        .unwrap_or_else(|error| std::str::from_utf8(&head[..error.valid_up_to()]).unwrap_or(""));
    let parse_id = || {
        let digits = text.split_once("\"id\":")?.1.trim_start();
        let end = digits
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(digits.len());
        digits[..end].parse().ok()
    };
    if text.contains("\"type\":\"call\"") {
        return DaemonFrame::Overload { id: parse_id() };
    }
    if text.contains("\"type\":\"events_list\"")
        || text.contains("\"type\":\"events_poll\"")
        || text.contains("\"type\":\"events_stream\"")
    {
        return DaemonFrame::OverloadEvent { id: parse_id() };
    }
    DaemonFrame::Overload { id: None }
}

/// Charges raw bytes, parsing, and the owned arguments through actual dispatch.
/// Short control classification never leaves an ordinary call on the control pool.
pub(crate) async fn read_daemon_frame_owned<R: AsyncRead + Unpin>(
    reader: &mut R,
    max: usize,
    main: &Arc<BytePool>,
    control: &Arc<BytePool>,
) -> std::io::Result<DaemonFrame> {
    let mut header = [0u8; 4];
    let mut filled = 0;
    while filled < header.len() {
        match reader.read(&mut header[filled..]).await {
            Ok(0) if filled == 0 => return Ok(DaemonFrame::Eof),
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("the input ended after {filled} of 4 length bytes"),
                ));
            }
            Ok(n) => filled += n,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    let length = u32::from_le_bytes(header) as usize;
    if length > max {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("a frame of {length} bytes is over the {max} byte limit"),
        ));
    }
    // raw R + Value R + four R for borrowed-Value decoding and later typed
    // argument copies. The earlier Value parse (R + R + 2R scratch) fits too.
    let peak = length.checked_mul(6).ok_or_else(input_budget_error)?;
    let Some((lease, in_control)) = input_lease(peak, main, control, length <= SHORT_INPUT) else {
        let prefix_len = length.min(256);
        let mut head = [0u8; 256];
        read_exact_slice(reader, &mut head[..prefix_len]).await?;
        drain_exact_bytes(reader, length - prefix_len, length).await?;
        return Ok(sniff_daemon_overload(&head[..prefix_len]));
    };
    struct Raw {
        bytes: Vec<u8>,
        lease: Lease,
    }
    let mut raw = Raw {
        bytes: vec![0; length],
        lease,
    };
    read_exact_slice(reader, &mut raw.bytes).await?;
    let value: serde_json::Value = serde_json::from_slice(&raw.bytes)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let message = ToDaemon::deserialize(&value)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    drop(value);
    drop(std::mem::take(&mut raw.bytes));
    let retained = length * 4;
    raw.lease.shrink_to(retained);
    if in_control && !daemon_control(&message) {
        let Some(lease) = main.try_acquire(retained) else {
            let frame = match &message {
                ToDaemon::Call { id, .. } => DaemonFrame::Overload { id: Some(*id) },
                ToDaemon::EventsList { id }
                | ToDaemon::EventsPoll { id, .. }
                | ToDaemon::EventsStream { id, .. } => DaemonFrame::OverloadEvent { id: Some(*id) },
                _ => DaemonFrame::Overload { id: None },
            };
            drop(message);
            return Ok(frame);
        };
        raw.lease = lease;
    }
    Ok(DaemonFrame::Message(Owned::new(
        message,
        Owner::new(raw.lease),
    )))
}

/// Reads a daemon frame using only the main byte pool (never touches `control_pool`).
/// Used on `HelloEmit` connections so external event floods cannot contend with control traffic.
pub(crate) async fn read_daemon_frame_main_only<R: AsyncRead + Unpin>(
    reader: &mut R,
    max: usize,
    main: &Arc<BytePool>,
) -> std::io::Result<DaemonFrame> {
    let mut header = [0u8; 4];
    let mut filled = 0;
    while filled < header.len() {
        match reader.read(&mut header[filled..]).await {
            Ok(0) if filled == 0 => return Ok(DaemonFrame::Eof),
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("the input ended after {filled} of 4 length bytes"),
                ));
            }
            Ok(n) => filled += n,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    let length = u32::from_le_bytes(header) as usize;
    if length > max {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("a frame of {length} bytes is over the {max} byte limit"),
        ));
    }
    let peak = length.checked_mul(6).ok_or_else(input_budget_error)?;
    let Some(mut lease) = main.try_acquire(peak) else {
        drain_exact_bytes(reader, length, length).await?;
        return Ok(DaemonFrame::Overload { id: None });
    };
    let mut bytes = vec![0; length];
    read_exact_slice(reader, &mut bytes).await?;
    let message: ToDaemon = serde_json::from_slice(&bytes)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    drop(bytes);
    lease.shrink_to(length * 4);
    Ok(DaemonFrame::Message(Owned::new(message, Owner::new(lease))))
}

/// A message as the bytes a frame holds: the u32 little-endian length and its JSON. Encoding
/// stands apart from writing so a caller can size the bytes it will send before it queues them.
pub fn encode_frame<T>(message: &T) -> std::io::Result<Vec<u8>>
where
    T: Serialize + ?Sized,
{
    let body = serde_json::to_vec(message)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let length = u32::try_from(body.len()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("a frame of {} bytes does not fit its length", body.len()),
        )
    })?;
    let mut bytes = Vec::with_capacity(4 + body.len());
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(&body);
    Ok(bytes)
}

/// Encodes a measured response directly into its frame instead of keeping a second JSON buffer.
pub(crate) fn encode_measured<T>(message: &T, length: usize) -> std::io::Result<Vec<u8>>
where
    T: Serialize + ?Sized,
{
    let body_length = u32::try_from(length.saturating_sub(4))
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let mut bytes = Vec::with_capacity(length);
    bytes.extend_from_slice(&body_length.to_le_bytes());
    serde_json::to_writer(&mut bytes, message)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    if bytes.len() != length {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "frame length changed between measuring and encoding",
        ));
    }
    Ok(bytes)
}

/// Counts the serialized JSON bytes of `message` without allocating a buffer.
pub fn json_len<T>(message: &T) -> std::io::Result<usize>
where
    T: Serialize + ?Sized,
{
    struct Count(usize);

    impl std::io::Write for Count {
        fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
            self.0 += buffer.len();
            Ok(buffer.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let mut count = Count(0);
    serde_json::to_writer(&mut count, message)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    Ok(count.0)
}

/// The bytes a frame of `message` occupies, counted without holding them: the length prefix and
/// the JSON. A caller that must reserve room for a frame before it exists measures it here, so
/// no encoded buffer is ever held without the room for it.
pub fn frame_len<T>(message: &T) -> std::io::Result<usize>
where
    T: Serialize + ?Sized,
{
    Ok(4 + json_len(message)?)
}

/// Writes bytes that are already a frame, then flushes them: a frame a reader waits on is never
/// left in a buffer.
pub async fn write_raw<W>(writer: &mut W, bytes: &[u8]) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(bytes).await?;
    writer.flush().await
}

/// Writes `message` as one frame.
#[cfg(test)]
pub async fn write_frame<W, T>(writer: &mut W, message: &T) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize + ?Sized,
{
    write_raw(writer, &encode_frame(message)?).await
}

/// Writes a pre-encoded frame in chunks, resetting `idle` only when positive bytes are written.
pub async fn write_raw_idle<W>(
    writer: &mut W,
    bytes: &[u8],
    idle: std::time::Duration,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut written = 0;
    let mut deadline = tokio::time::Instant::now() + idle;
    while written < bytes.len() {
        if tokio::time::Instant::now() >= deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "write stalled",
            ));
        }
        let end = (written + WRITE_CHUNK).min(bytes.len());
        match tokio::time::timeout_at(deadline, writer.write(&bytes[written..end])).await {
            Ok(Ok(0)) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "write returned 0 bytes",
                ));
            }
            Ok(Ok(count)) => {
                written += count;
                deadline = tokio::time::Instant::now() + idle;
            }
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::Interrupted => {
                tokio::task::yield_now().await;
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "write stalled",
                ));
            }
        }
    }
    let flush_deadline = tokio::time::Instant::now() + idle;
    loop {
        if tokio::time::Instant::now() >= flush_deadline {
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "flush stalled",
            ));
        }
        match tokio::time::timeout_at(flush_deadline, writer.flush()).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::Interrupted => {
                tokio::task::yield_now().await;
            }
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "flush stalled",
                ));
            }
        }
    }
}

/// Encodes `message` as one frame and writes it under per-chunk idle timeout `idle`.
pub async fn write_frame_idle<W, T>(
    writer: &mut W,
    message: &T,
    idle: std::time::Duration,
) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize + ?Sized,
{
    write_raw_idle(writer, &encode_frame(message)?, idle).await
}

/// Reads one frame, or `None` when the reader ended cleanly before a length. A reader that ends
/// inside a frame is an error, and so is a length over `max`: the body of an oversized frame is
/// never allocated, because the length it claims is not trusted.
#[cfg(test)]
pub async fn read_frame<R, T>(reader: &mut R, max: usize) -> std::io::Result<Option<T>>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let mut header = [0u8; 4];
    let mut filled = 0;
    while filled < header.len() {
        let read = reader.read(&mut header[filled..]).await?;
        if read == 0 {
            if filled == 0 {
                return Ok(None);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("the input ended after {filled} of 4 length bytes"),
            ));
        }
        filled += read;
    }
    let length = u32::from_le_bytes(header) as usize;
    if length > max {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("a frame of {length} bytes is over the {max} byte limit"),
        ));
    }
    let mut body = vec![0u8; length];
    let mut filled = 0;
    while filled < length {
        let read = reader.read(&mut body[filled..]).await?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("the frame ended after {filled} of {length} bytes"),
            ));
        }
        filled += read;
    }
    serde_json::from_slice(&body)
        .map(Some)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

#[derive(Debug)]
pub enum LeasedEndpointFrame {
    Frame(ToEndpoint, crate::lease::Lease),
    OverloadResult { id: Option<u64> },
    OverloadEvent { id: Option<u64> },
    OverloadProgress,
    Eof,
}

async fn read_exact_slice<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
) -> std::io::Result<()> {
    let total = buf.len();
    let mut filled = 0;
    while filled < total {
        match reader.read(&mut buf[filled..]).await {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("the frame ended after {filled} of {total} bytes"),
                ));
            }
            Ok(n) => filled += n,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

async fn drain_exact_bytes<R: AsyncRead + Unpin>(
    reader: &mut R,
    mut remaining: usize,
    total: usize,
) -> std::io::Result<()> {
    let mut scratch = [0u8; 8192];
    while remaining > 0 {
        let want = remaining.min(scratch.len());
        match reader.read(&mut scratch[..want]).await {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("the frame ended with {remaining} of {total} bytes unread"),
                ));
            }
            Ok(n) => remaining -= n,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(())
}

// Exact raw bytes plus decoded strings, JSON escape scratch (up to 2R), and
// the extra owned copy required by untagged ResourceContents. Internal-tagged
// ToEndpoint/ContentBlock caches move their strings. The allowance is payload
// storage, not Rust object metadata or allocator RSS.
fn endpoint_parse_peak(length: usize) -> Option<usize> {
    length.checked_mul(5)?.checked_add(256)
}

/// Reads and deserializes one `ToEndpoint` frame under `main_pool` (falling back to `control_pool`
/// for `<= 8 MiB` `Progress`, short error receipts, and `list`/`kill` control calls), and drains
/// un-admitted frames in small chunks so an overload fails only that call without breaking IPC.
pub async fn read_endpoint_frame_leased<R, F>(
    reader: &mut R,
    max: usize,
    main_pool: &std::sync::Arc<crate::lease::BytePool>,
    control_pool: &std::sync::Arc<crate::lease::BytePool>,
    is_control_call: F,
) -> std::io::Result<LeasedEndpointFrame>
where
    R: AsyncRead + Unpin,
    F: Fn(u64) -> bool,
{
    let mut header = [0u8; 4];
    let mut filled = 0;
    while filled < header.len() {
        match reader.read(&mut header[filled..]).await {
            Ok(0) if filled == 0 => return Ok(LeasedEndpointFrame::Eof),
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("the input ended after {filled} of 4 length bytes"),
                ));
            }
            Ok(n) => filled += n,
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
    let length = u32::from_le_bytes(header) as usize;
    if length > max {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("a frame of {length} bytes is over the {max} byte limit"),
        ));
    }
    let parse_peak = endpoint_parse_peak(length);
    if let Some(mut lease) = parse_peak.and_then(|peak| main_pool.try_acquire(peak)) {
        let mut body = vec![0u8; length];
        read_exact_slice(reader, &mut body).await?;
        let decoded: ToEndpoint = serde_json::from_slice(&body)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        drop(body);
        lease.shrink_to(length);
        return Ok(LeasedEndpointFrame::Frame(decoded, lease));
    }
    if let Some(parse_peak) = parse_peak.filter(|&peak| peak <= crate::lease::CONTROL_LIMIT) {
        let prefix_len = length.min(256);
        let mut head = [0u8; 256];
        read_exact_slice(reader, &mut head[..prefix_len]).await?;
        let sniffed = sniff_endpoint_overload(&head[..prefix_len]);
        let skip_control_pool = length > 64 * 1024
            && match sniffed {
                LeasedEndpointFrame::OverloadResult { id } => !id.is_some_and(&is_control_call),
                LeasedEndpointFrame::OverloadEvent { .. } => true,
                _ => false,
            };
        if !skip_control_pool && let Some(mut clease) = control_pool.try_acquire(parse_peak) {
            let mut body = vec![0u8; length];
            body[..prefix_len].copy_from_slice(&head[..prefix_len]);
            read_exact_slice(reader, &mut body[prefix_len..]).await?;
            let decoded: ToEndpoint = serde_json::from_slice(&body)
                .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
            drop(body);
            match &decoded {
                ToEndpoint::Progress { .. } => {
                    clease.shrink_to(length);
                    return Ok(LeasedEndpointFrame::Frame(decoded, clease));
                }
                ToEndpoint::Result { id, result }
                    if result.is_error == Some(true) || is_control_call(*id) =>
                {
                    clease.shrink_to(length);
                    return Ok(LeasedEndpointFrame::Frame(decoded, clease));
                }
                ToEndpoint::Result { id, .. } => {
                    let id = *id;
                    drop(decoded);
                    drop(clease);
                    return Ok(LeasedEndpointFrame::OverloadResult { id: Some(id) });
                }
                ToEndpoint::EventFrame {
                    frame:
                        EventWireFrame::Error(_)
                        | EventWireFrame::Terminated(_)
                        | EventWireFrame::Complete
                        | EventWireFrame::Heartbeat { .. },
                    ..
                } => {
                    clease.shrink_to(length);
                    return Ok(LeasedEndpointFrame::Frame(decoded, clease));
                }
                ToEndpoint::EventFrame { id, .. } => {
                    let id = *id;
                    drop(decoded);
                    drop(clease);
                    return Ok(LeasedEndpointFrame::OverloadEvent { id: Some(id) });
                }
                _ => {
                    clease.shrink_to(length);
                    return Ok(LeasedEndpointFrame::Frame(decoded, clease));
                }
            }
        }
        drain_exact_bytes(reader, length - prefix_len, length).await?;
        return Ok(sniffed);
    }
    let prefix_len = length.min(256);
    let mut head = [0u8; 256];
    read_exact_slice(reader, &mut head[..prefix_len]).await?;
    drain_exact_bytes(reader, length - prefix_len, length).await?;
    Ok(sniff_endpoint_overload(&head[..prefix_len]))
}

fn sniff_endpoint_overload(head: &[u8]) -> LeasedEndpointFrame {
    // `head` may cut through a multibyte UTF-8 sequence at byte 256; keep the valid prefix so
    // ASCII JSON header fields (`"type"` and `"id"`) at the front of the frame still parse.
    let text = match std::str::from_utf8(head) {
        Ok(valid) => valid,
        Err(err) => std::str::from_utf8(&head[..err.valid_up_to()]).unwrap_or(""),
    };
    if text.contains("\"type\":\"progress\"") {
        return LeasedEndpointFrame::OverloadProgress;
    }
    let parse_id = || {
        text.split_once("\"id\":").and_then(|(_, rest)| {
            let digits: String = rest
                .trim_start()
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            digits.parse().ok()
        })
    };
    if text.contains("\"type\":\"event_frame\"") {
        return LeasedEndpointFrame::OverloadEvent { id: parse_id() };
    }
    if !text.contains("\"type\":\"result\"") {
        return LeasedEndpointFrame::OverloadResult { id: None };
    }
    LeasedEndpointFrame::OverloadResult { id: parse_id() }
}

/// An OS string as the wire carries it: a JSON string when it is valid Unicode, and its native
/// units otherwise, so an environment name or value survives exactly. A path is not text here;
/// a byte that is not UTF-8 is data to hand back to the OS unchanged.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum OsText {
    Text(String),
    Units { units: Vec<Unit> },
}

/// The native unit of an OS string: UTF-16 on Windows, a byte on Unix.
#[cfg(windows)]
pub type Unit = u16;
/// The native unit of an OS string: UTF-16 on Windows, a byte on Unix.
#[cfg(unix)]
pub type Unit = u8;

#[cfg(windows)]
fn units_of(value: &OsStr) -> Vec<Unit> {
    use std::os::windows::ffi::OsStrExt;
    let mut units = Vec::with_capacity(value.encode_wide().count());
    units.extend(value.encode_wide());
    units
}

#[cfg(unix)]
fn units_of(value: &OsStr) -> Vec<Unit> {
    use std::os::unix::ffi::OsStrExt;
    value.as_bytes().to_vec()
}

#[cfg(windows)]
fn from_units(units: &[Unit]) -> OsString {
    use std::os::windows::ffi::OsStringExt;
    OsString::from_wide(units)
}

#[cfg(unix)]
fn from_units(units: &[Unit]) -> OsString {
    use std::os::unix::ffi::OsStringExt;
    OsString::from_vec(units.to_vec())
}

impl From<&OsStr> for OsText {
    fn from(value: &OsStr) -> Self {
        match value.to_str() {
            Some(text) => Self::Text(text.to_owned()),
            None => Self::Units {
                units: units_of(value),
            },
        }
    }
}

impl OsText {
    /// The OS string this text stands for. Both spellings are read as they stand: units holding
    /// nothing are the empty OS string, exactly as `Text("")` is, because an empty environment
    /// value is legitimate and neither spelling is more correct than the other.
    pub fn to_os_string(&self) -> Result<OsString, String> {
        match self {
            Self::Text(text) => Ok(OsString::from(text)),
            Self::Units { units } => Ok(from_units(units)),
        }
    }
}

/// Structured event error carried over IPC.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IpcEventError {
    pub code: i32,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

impl From<crate::events::EventError> for IpcEventError {
    fn from(err: crate::events::EventError) -> Self {
        Self {
            code: err.code(),
            data: err.data(),
            message: match err {
                crate::events::EventError::InvalidParams(message)
                | crate::events::EventError::NotFound { message, .. }
                | crate::events::EventError::Forbidden(message)
                | crate::events::EventError::ResourceExhausted { message, .. } => message,
            },
        }
    }
}

impl From<IpcEventError> for serde_json::Value {
    fn from(error: IpcEventError) -> Self {
        let mut value = serde_json::Map::new();
        value.insert("code".to_owned(), error.code.into());
        value.insert(
            "message".to_owned(),
            serde_json::Value::String(error.message),
        );
        if let Some(data) = error.data {
            value.insert("data".to_owned(), data);
        }
        serde_json::Value::Object(value)
    }
}

/// Event RPC responses and streaming frames sent from daemon to endpoint.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EventWireFrame {
    ListResult(crate::events::EventsListResult),
    PollResult(crate::events::PollResponse),
    Active(crate::events::ActiveNotice),
    Event(crate::events::StreamEvent),
    Heartbeat { cursor: String },
    Error(IpcEventError),
    Terminated(IpcEventError),
    Complete,
}

/// What an endpoint sends the daemon.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToDaemon {
    /// The first frame on a connection: who this endpoint is, what it can render, and the
    /// directory and environment its tasks start with.
    Hello {
        protocol: u32,
        namespace: String,
        build: String,
        session: String,
        connection: u64,
        client_name: String,
        output_mode: String,
        structured_content: bool,
        cwd: OsText,
        env: Vec<(OsText, OsText)>,
    },
    /// Lightweight handshake for `fastexec daemon emit`: no cwd/env and no daemon session entry.
    HelloEmit { protocol: u32, build: String },
    /// External event injection from `fastexec daemon emit`. Only `external.event` is accepted,
    /// capped at 64 KiB, and never routed through `control_pool`.
    EmitEvent {
        event: crate::events::ExternalEventInput,
    },
    /// List available MCP event types.
    EventsList { id: u64 },
    /// Poll MCP events from a cursor.
    EventsPoll {
        id: u64,
        request: crate::events::PollRequest,
    },
    /// Open a streaming MCP event subscription.
    EventsStream {
        id: u64,
        request: crate::events::SubscribeRequest,
    },
    /// Cancel an active MCP event stream.
    EventsCancel { id: u64 },
    /// One call, of one tool. `id` comes from an endpoint-wide counter, so it keeps naming this
    /// call across a reconnect; `request_id` is the MCP JSON-RPC ID, kept as a label because
    /// clients reuse it.
    Call {
        id: u64,
        tool: String,
        request_id: String,
        arguments: serde_json::Value,
    },
    /// The request was cancelled: its wait ends and its task keeps running.
    Cancel { id: u64 },
    /// The endpoint's input ended, so the daemon drops the session at once.
    Bye,
    /// Report the daemon's state. Only a connection that has a `Welcome` may ask.
    Status,
    /// Stop the daemon. Only a connection that has a `Welcome` may ask.
    Stop,
}

/// What the daemon sends an endpoint.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToEndpoint {
    /// The handshake was accepted, and `epoch` names the daemon instance the session is bound to.
    Welcome {
        protocol: u32,
        namespace: String,
        build: String,
        epoch: String,
    },
    /// The handshake was refused, and `message` says why.
    Reject { message: String },
    /// Acknowledgement for `ToDaemon::EmitEvent`.
    EmitAck {
        ok: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<String>,
        #[serde(default)]
        deduplicated: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<IpcEventError>,
    },
    /// Response or streaming notification for an `EventsList`, `EventsPoll`, or `EventsStream`.
    EventFrame { id: u64, frame: EventWireFrame },
    /// A call is still running: `seconds` since it started, and what it is waiting on.
    Progress {
        id: u64,
        seconds: f64,
        message: String,
    },
    /// A call finished.
    Result {
        id: u64,
        result: rmcp::model::CallToolResult,
    },
    /// The answer to `Status`: which daemon this is, and what it holds.
    Status {
        epoch: String,
        build: String,
        pid: u32,
        sessions: usize,
        tasks: usize,
        /// The address it serves Streamable HTTP on, when it serves it.
        http: Option<String>,
    },
}

impl crate::session::LaunchContext {
    /// Payload capacity of the temporary wire copy, excluding the persistent environment.
    pub(crate) fn wire_capacity(&self) -> usize {
        fn copied(value: &OsStr) -> usize {
            if let Some(text) = value.to_str() {
                return text.len();
            }
            #[cfg(unix)]
            {
                use std::os::unix::ffi::OsStrExt;
                value.as_bytes().len()
            }
            #[cfg(windows)]
            {
                use std::os::windows::ffi::OsStrExt;
                value.encode_wide().count() * std::mem::size_of::<Unit>()
            }
        }
        self.env
            .iter()
            .fold(copied(self.cwd.as_os_str()), |sum, (name, value)| {
                sum.saturating_add(copied(name))
                    .saturating_add(copied(value))
            })
    }
    /// The context a Hello describes, or the field that could not be read.
    pub fn from_wire(cwd: &OsText, env: &[(OsText, OsText)]) -> Result<Self, String> {
        let cwd = cwd
            .to_os_string()
            .map_err(|problem| format!("cwd: {problem}"))?;
        let mut entries = Vec::with_capacity(env.len());
        for (index, (name, value)) in env.iter().enumerate() {
            let name = name
                .to_os_string()
                .map_err(|problem| format!("env[{index}] name: {problem}"))?;
            let value = value
                .to_os_string()
                .map_err(|problem| format!("env[{index}] value: {problem}"))?;
            entries.push((name, value));
        }
        Ok(Self::new(cwd.into(), entries))
    }

    /// This context as a Hello carries it.
    pub fn to_wire(&self) -> (OsText, Vec<(OsText, OsText)>) {
        let env = self
            .env
            .iter()
            .map(|(name, value)| {
                (
                    OsText::from(name.as_os_str()),
                    OsText::from(value.as_os_str()),
                )
            })
            .collect();
        (OsText::from(self.cwd.as_os_str()), env)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::OutputMode;
    use serde_json::json;
    use std::path::PathBuf;

    fn ingress_call() -> ToDaemon {
        ToDaemon::Call {
            id: 7,
            tool: "exec".into(),
            request_id: "request 7".into(),
            arguments: json!({"action":"start","command":"printf hello"}),
        }
    }

    #[tokio::test]
    async fn ipc_ingress_reader_owns_normal_arguments_and_exact_admission() {
        let message = ingress_call();
        let frame = encode_frame(&message).unwrap();
        let length = frame.len() - 4;
        for (limit, fits) in [(6 * length - 1, false), (6 * length, true)] {
            let pool = BytePool::new(limit);
            let control = BytePool::new(0);
            let mut reader = std::io::Cursor::new(&frame);
            match read_daemon_frame_owned(&mut reader, REQUEST_LIMIT, &pool, &control)
                .await
                .unwrap()
            {
                DaemonFrame::Message(owned) => {
                    assert!(fits);
                    assert_eq!(owned.value, message);
                    assert_eq!(pool.used(), 4 * length);
                    let kept = owned.owner.clone();
                    drop(owned);
                    assert_eq!(pool.used(), 4 * length);
                    drop(kept);
                }
                DaemonFrame::Overload { id } => {
                    assert!(!fits);
                    assert_eq!(id, Some(7));
                }
                DaemonFrame::OverloadEvent { .. } => panic!("expected call frame"),
                DaemonFrame::Eof => panic!("one complete frame"),
            }
            assert_eq!(pool.used(), 0);
        }
    }

    #[tokio::test]
    async fn ipc_ingress_control_classification_does_not_retain_regular_calls() {
        let pool = BytePool::new(0);
        let control = BytePool::new(4096);
        let mut messages = vec![
            ToDaemon::Cancel { id: 7 },
            ToDaemon::Bye,
            ToDaemon::Status,
            ToDaemon::Stop,
        ];
        for action in ["list", "kill"] {
            messages.push(ToDaemon::Call {
                id: 7,
                tool: "exec".into(),
                request_id: "7".into(),
                arguments: json!({"action":action,"taskId":"short-task"}),
            });
        }
        for message in messages {
            let frame = encode_frame(&message).unwrap();
            let mut reader = std::io::Cursor::new(frame);
            let DaemonFrame::Message(owned) =
                read_daemon_frame_owned(&mut reader, REQUEST_LIMIT, &pool, &control)
                    .await
                    .unwrap()
            else {
                panic!("small control request admitted");
            };
            assert_eq!(owned.value, message);
            assert!(control.used() > 0);
            drop(owned);
            assert_eq!(control.used(), 0);
        }
        let mut reader = std::io::Cursor::new(encode_frame(&ingress_call()).unwrap());
        assert!(matches!(
            read_daemon_frame_owned(&mut reader, REQUEST_LIMIT, &pool, &control)
                .await
                .unwrap(),
            DaemonFrame::Overload { id: Some(7) }
        ));
        assert_eq!(control.used(), 0);
    }

    #[tokio::test]
    async fn ipc_ingress_partial_read_and_stalled_write_release_on_cancel() {
        use std::future::Future;
        let message = ingress_call();
        let encoded = encode_frame(&message).unwrap();
        let length = encoded.len() - 4;
        let pool = BytePool::new(6 * length);
        let control = BytePool::new(0);
        let (mut writer, mut reader) = tokio::io::duplex(encoded.len());
        writer.write_all(&encoded[..9]).await.unwrap();
        let mut reading = Box::pin(read_daemon_frame_owned(
            &mut reader,
            REQUEST_LIMIT,
            &pool,
            &control,
        ));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                reading.as_mut().poll(cx).is_pending()
            ))
            .await
        );
        assert_eq!(pool.used(), 6 * length);
        drop(reading);
        assert_eq!(pool.used(), 0);
        let (mut writer, _reader) = tokio::io::duplex(8);
        let mut writing = Box::pin(write_request_frame(&mut writer, &message, &pool, &control));
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                writing.as_mut().poll(cx).is_pending()
            ))
            .await
        );
        assert_eq!(pool.used(), encoded.len());
        drop(writing);
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn ipc_ingress_native_units_keep_owner_through_hello_conversion() {
        let message = ToDaemon::Hello {
            protocol: PROTOCOL,
            namespace: "ns".into(),
            build: "build".into(),
            session: "s".into(),
            connection: 1,
            client_name: "test".into(),
            output_mode: "text".into(),
            structured_content: true,
            cwd: OsText::Units {
                units: vec![47, 119],
            },
            env: vec![(
                OsText::Units { units: vec![65] },
                OsText::Units { units: vec![66] },
            )],
        };
        let frame = encode_frame(&message).unwrap();
        let length = frame.len() - 4;
        let pool = BytePool::new(6 * length);
        let mut reader = std::io::Cursor::new(frame);
        let DaemonFrame::Message(owned) =
            read_daemon_frame_owned(&mut reader, REQUEST_LIMIT, &pool, &BytePool::new(0))
                .await
                .unwrap()
        else {
            panic!("normal native units admitted");
        };
        let ToDaemon::Hello { cwd, env, .. } = &owned.value else {
            panic!("Hello");
        };
        let installed = crate::session::LaunchContext::from_wire(cwd, env).unwrap();
        assert_eq!(installed.cwd, std::path::PathBuf::from("/w"));
        assert_eq!(
            installed.env,
            vec![(std::ffi::OsString::from("A"), std::ffi::OsString::from("B"))]
        );
        assert_eq!(pool.used(), 4 * length);
        drop(owned);
        assert_eq!(
            pool.used(),
            0,
            "installed launch state has its own lifetime"
        );
    }

    #[test]
    fn ipc_ingress_encoder_reserves_the_only_independent_frame() {
        let message = ingress_call();
        let length = frame_len(&message).unwrap();
        let short = BytePool::new(length - 1);
        assert!(encode_frame_leased(&message, &short, &BytePool::new(0), false).is_err());
        assert_eq!(short.used(), 0);
        let pool = BytePool::new(length);
        let frame = encode_frame_leased(&message, &pool, &BytePool::new(0), false).unwrap();
        assert_eq!(pool.used(), frame.bytes.capacity());
        assert_eq!(frame.bytes, encode_frame(&message).unwrap());
        drop(frame);
        assert_eq!(pool.used(), 0);
    }

    /// The bytes a frame occupies, as a reader would find them.
    async fn frame_bytes<T: Serialize>(message: &T) -> Vec<u8> {
        let bytes = encode_frame(message).unwrap();
        // What the encoder produced is what the writer sends, unchanged. The writer buffers, so
        // the flush inside `write_raw` is what gets the frame out.
        let (writer, mut reader) = tokio::io::duplex(1 << 16);
        let mut writer = tokio::io::BufWriter::new(writer);
        let reading = async {
            let mut sent = Vec::new();
            reader.read_to_end(&mut sent).await.unwrap();
            sent
        };
        let writing = async {
            write_raw(&mut writer, &bytes).await.unwrap();
            drop(writer);
        };
        assert_eq!(tokio::join!(reading, writing).0, bytes);
        bytes
    }

    async fn round_trip<T>(message: &T, max: usize) -> T
    where
        T: Serialize + DeserializeOwned,
    {
        let bytes = frame_bytes(message).await;
        let mut reader = std::io::Cursor::new(bytes);
        read_frame(&mut reader, max)
            .await
            .unwrap()
            .expect("a frame")
    }

    fn hello() -> ToDaemon {
        ToDaemon::Hello {
            protocol: PROTOCOL,
            namespace: "fastexec".to_owned(),
            build: "0.1.0".to_owned(),
            session: "3f2a".to_owned(),
            connection: 2,
            client_name: "probe".to_owned(),
            output_mode: "both".to_owned(),
            structured_content: true,
            cwd: OsText::Text("/work".to_owned()),
            env: vec![(
                OsText::Text("PATH".to_owned()),
                OsText::Text("/usr/bin".to_owned()),
            )],
        }
    }

    #[tokio::test]
    async fn a_frame_is_a_length_and_its_json() {
        let body = br#"{"type":"bye"}"#;
        let mut expected = u32::try_from(body.len()).unwrap().to_le_bytes().to_vec();
        expected.extend_from_slice(body);
        assert_eq!(frame_bytes(&ToDaemon::Bye).await, expected);
    }

    #[tokio::test]
    async fn every_message_survives_the_wire() {
        let mut result =
            rmcp::model::CallToolResult::success(vec![rmcp::model::ContentBlock::Text(
                rmcp::model::TextContent::new("done"),
            )]);
        result.structured_content = Some(json!({"state": "exited"}));
        let sent = [
            hello(),
            ToDaemon::Call {
                id: 7,
                tool: "exec".to_owned(),
                request_id: "3".to_owned(),
                arguments: json!({"action": "start", "command": "echo hi"}),
            },
            ToDaemon::Cancel { id: 7 },
            ToDaemon::Bye,
        ];
        for message in &sent {
            assert_eq!(&round_trip(message, REQUEST_LIMIT).await, message);
        }
        let received = [
            ToEndpoint::Welcome {
                protocol: PROTOCOL,
                namespace: "fastexec".to_owned(),
                build: "0.1.0".to_owned(),
                epoch: "e1".to_owned(),
            },
            ToEndpoint::Reject {
                message: "protocol 2".to_owned(),
            },
            ToEndpoint::Progress {
                id: 7,
                seconds: 1.5,
                message: "still running".to_owned(),
            },
            ToEndpoint::Result {
                id: 7,
                result: result.clone(),
            },
        ];
        for message in &received {
            assert_eq!(&round_trip(message, RESPONSE_LIMIT).await, message);
        }
    }

    #[tokio::test]
    async fn arguments_at_the_call_limit_still_fit_a_frame_the_daemon_reads() {
        // The endpoint's limit is on the serialized arguments, so a call at that limit carries an
        // envelope over it and is still a frame the daemon reads: the daemon's limit is larger.
        let arguments = json!({"command": "x".repeat(CALL_LIMIT - 14)});
        assert_eq!(serde_json::to_vec(&arguments).unwrap().len(), CALL_LIMIT);
        let message = ToDaemon::Call {
            id: 5,
            tool: "exec".to_owned(),
            request_id: "5".to_owned(),
            arguments,
        };
        let bytes = frame_bytes(&message).await;
        let body = bytes.len() - 4;
        assert!(body > CALL_LIMIT, "{body} is not over {CALL_LIMIT}");
        assert!(body <= REQUEST_LIMIT, "{body} is over {REQUEST_LIMIT}");
        let mut reader = std::io::Cursor::new(bytes);
        let read: Option<ToDaemon> = read_frame(&mut reader, REQUEST_LIMIT).await.unwrap();
        assert_eq!(read, Some(message));
    }

    #[tokio::test]
    async fn a_measured_frame_is_the_frame_that_is_written() {
        for message in [hello(), ToDaemon::Bye, ToDaemon::Cancel { id: 3 }] {
            assert_eq!(
                frame_len(&message).unwrap(),
                encode_frame(&message).unwrap().len(),
                "{message:?}"
            );
        }
    }

    #[test]
    fn measured_encoding_matches_regular_encoding() {
        let frame = ToEndpoint::Progress {
            id: 9,
            seconds: 1.25,
            message: "line one\n次の行".into(),
        };
        let length = frame_len(&frame).unwrap();
        assert_eq!(
            encode_measured(&frame, length).unwrap(),
            encode_frame(&frame).unwrap()
        );
        assert!(encode_measured(&frame, length + 1).is_err());
    }

    #[tokio::test]
    async fn a_written_frame_is_the_encoded_frame() {
        let message = ToDaemon::Cancel { id: 9 };
        let (mut writer, mut reader) = tokio::io::duplex(1 << 16);
        let reading = async {
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            bytes
        };
        let writing = async {
            write_frame(&mut writer, &message).await.unwrap();
            drop(writer);
        };
        let (sent, ()) = tokio::join!(reading, writing);
        assert_eq!(sent, encode_frame(&message).unwrap());
        let read: Option<ToDaemon> = read_frame(&mut std::io::Cursor::new(sent), REQUEST_LIMIT)
            .await
            .unwrap();
        assert_eq!(read, Some(message));
    }

    #[tokio::test]
    async fn frames_one_after_another_read_one_after_another() {
        let first = ToDaemon::Call {
            id: 1,
            tool: "exec".to_owned(),
            request_id: "1".to_owned(),
            arguments: json!({"action": "list"}),
        };
        let second = ToDaemon::Cancel { id: 1 };
        let mut bytes = frame_bytes(&first).await;
        bytes.extend_from_slice(&frame_bytes(&second).await);
        let mut reader = std::io::Cursor::new(bytes);
        let read: Option<ToDaemon> = read_frame(&mut reader, REQUEST_LIMIT).await.unwrap();
        assert_eq!(read, Some(first));
        let read: Option<ToDaemon> = read_frame(&mut reader, REQUEST_LIMIT).await.unwrap();
        assert_eq!(read, Some(second));
        // Then the clean end: the frames stopped where the bytes did.
        let read: Option<ToDaemon> = read_frame(&mut reader, REQUEST_LIMIT).await.unwrap();
        assert_eq!(read, None);
    }

    #[tokio::test]
    async fn a_frame_read_one_byte_at_a_time_is_the_same_frame() {
        let message = hello();
        // A one-byte buffer hands the reader one byte per read, as a socket may.
        let (mut writer, mut reader) = tokio::io::duplex(1);
        let reading = async {
            let read: Option<ToDaemon> = read_frame(&mut reader, REQUEST_LIMIT).await.unwrap();
            read
        };
        let writing = async {
            write_frame(&mut writer, &message).await.unwrap();
            drop(writer);
        };
        let (read, ()) = tokio::join!(reading, writing);
        assert_eq!(read, Some(message));
        let read: Option<ToDaemon> = read_frame(&mut reader, REQUEST_LIMIT).await.unwrap();
        assert_eq!(read, None);
    }

    #[tokio::test]
    async fn a_body_exactly_at_the_limit_reads() {
        let message = ToDaemon::Call {
            id: 4,
            tool: "exec".to_owned(),
            request_id: "4".to_owned(),
            arguments: json!({"action": "list"}),
        };
        let bytes = frame_bytes(&message).await;
        let at_limit = bytes.len() - 4;
        let mut reader = std::io::Cursor::new(bytes);
        let read: Option<ToDaemon> = read_frame(&mut reader, at_limit).await.unwrap();
        assert_eq!(read, Some(message));
    }

    #[tokio::test]
    async fn a_clean_end_of_input_is_not_a_frame() {
        let mut reader = std::io::Cursor::new(Vec::new());
        let frame: Option<ToDaemon> = read_frame(&mut reader, REQUEST_LIMIT).await.unwrap();
        assert_eq!(frame, None);
    }

    #[tokio::test]
    async fn a_frame_that_ends_early_is_an_error() {
        // A length that was cut off.
        let mut reader = std::io::Cursor::new(vec![1, 0]);
        let error = read_frame::<_, ToDaemon>(&mut reader, REQUEST_LIMIT)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        assert_eq!(
            error.to_string(),
            "the input ended after 2 of 4 length bytes"
        );

        // A body that was.
        let mut bytes = 8u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(br#"{"type""#);
        let mut reader = std::io::Cursor::new(bytes);
        let error = read_frame::<_, ToDaemon>(&mut reader, REQUEST_LIMIT)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
        assert_eq!(error.to_string(), "the frame ended after 7 of 8 bytes");
    }

    #[tokio::test]
    async fn a_length_over_the_limit_is_refused_without_reading_the_body() {
        // A gigabyte claimed, and nothing behind it: the length is refused, not trusted.
        let mut reader = std::io::Cursor::new((1u32 << 30).to_le_bytes().to_vec());
        let error = read_frame::<_, ToDaemon>(&mut reader, REQUEST_LIMIT)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "a frame of 1073741824 bytes is over the 4194304 byte limit"
        );
    }

    #[tokio::test]
    async fn a_body_that_is_not_json_is_an_error() {
        let mut bytes = 4u32.to_le_bytes().to_vec();
        bytes.extend_from_slice(b"nope");
        let mut reader = std::io::Cursor::new(bytes);
        let error = read_frame::<_, ToDaemon>(&mut reader, REQUEST_LIMIT)
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_text_name_is_a_string_on_the_wire() {
        let text = OsText::from(OsStr::new("PATH"));
        assert_eq!(serde_json::to_value(&text).unwrap(), json!("PATH"));
        assert_eq!(text.to_os_string().unwrap(), OsString::from("PATH"),);
    }

    #[cfg(windows)]
    #[test]
    fn a_name_that_is_not_unicode_keeps_its_units() {
        use std::os::windows::ffi::OsStringExt;
        // A lone high surrogate: no String holds it, and the environment can.
        let value = OsString::from_wide(&[b'a' as u16, 0xD800, b'b' as u16]);
        let text = OsText::from(value.as_os_str());
        let json = serde_json::to_value(&text).unwrap();
        assert_eq!(json, json!({"units": [97, 55296, 98]}));
        // Decoding is the half that can lose the units, so they come back through serde.
        let decoded: OsText = serde_json::from_value(json).unwrap();
        assert_eq!(decoded, text);
        assert_eq!(decoded.to_os_string().unwrap(), value);
    }

    #[cfg(unix)]
    #[test]
    fn a_name_that_is_not_unicode_keeps_its_units() {
        use std::os::unix::ffi::{OsStrExt, OsStringExt};
        let value = OsString::from_vec(vec![b'a', 0xFF, b'b']);
        let text = OsText::from(value.as_os_str());
        let json = serde_json::to_value(&text).unwrap();
        assert_eq!(json, json!({"units": [97, 255, 98]}));
        // Decoding is the half that can lose the bytes, so the units come back through serde.
        let decoded: OsText = serde_json::from_value(json).unwrap();
        assert_eq!(decoded, text);
        assert_eq!(decoded.to_os_string().unwrap(), value);
        assert_eq!(value.as_bytes(), &[b'a', 0xFF, b'b']);
    }

    #[test]
    fn an_empty_name_reads_either_way_it_is_spelled() {
        for text in [
            OsText::Text(String::new()),
            OsText::Units { units: Vec::new() },
        ] {
            assert_eq!(text.to_os_string().unwrap(), OsString::new());
        }
    }

    #[test]
    fn a_launch_context_survives_the_wire() {
        let context = crate::session::LaunchContext::new(
            PathBuf::from("/work"),
            vec![
                (OsString::from("PATH"), OsString::from("/usr/bin")),
                (OsString::from("LANG"), OsString::from("C.UTF-8")),
            ],
        );
        let (cwd, env) = context.to_wire();
        let back = crate::session::LaunchContext::from_wire(&cwd, &env).unwrap();
        assert_eq!(back.cwd, context.cwd);
        assert_eq!(back.env, context.env);

        // An empty value is a value: the wire carries it and the context keeps it.
        let empty = [(
            OsText::Text("TERM".to_owned()),
            OsText::Units { units: Vec::new() },
        )];
        let back = crate::session::LaunchContext::from_wire(&cwd, &empty).unwrap();
        assert_eq!(back.env, [(OsString::from("TERM"), OsString::new())]);
    }

    #[test]
    fn an_output_mode_round_trips_by_name() {
        for mode in [OutputMode::Text, OutputMode::Both, OutputMode::Structured] {
            assert_eq!(OutputMode::parse(mode.name()).unwrap(), mode);
        }
        assert_eq!(OutputMode::Text.name(), "text");
        assert_eq!(OutputMode::Both.name(), "both");
        assert_eq!(OutputMode::Structured.name(), "structured");
        assert_eq!(
            OutputMode::parse("Text").unwrap_err(),
            "unknown output mode \"Text\""
        );
    }

    #[tokio::test]
    async fn idle_writes_reset_on_forward_progress_and_fail_on_stall() {
        assert_eq!(PROTOCOL, 4);
        assert_eq!(RESPONSE_LIMIT, 512 << 20);

        let payload = vec![b'x'; 256];
        let idle = std::time::Duration::from_millis(80);
        let (mut writer, mut reader) = tokio::io::duplex(32);
        let reading = async {
            let mut received = Vec::with_capacity(payload.len());
            let mut chunk = [0_u8; 32];
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
                let n = reader.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                received.extend_from_slice(&chunk[..n]);
            }
            received
        };
        let started = std::time::Instant::now();
        let writing = async {
            write_raw_idle(&mut writer, &payload, idle).await.unwrap();
            drop(writer);
        };
        let (received, ()) = tokio::join!(reading, writing);
        assert!(started.elapsed() > idle * 2);
        assert_eq!(received, payload);

        let (mut stalled_writer, _unreading_peer) = tokio::io::duplex(16);
        let err = write_raw_idle(
            &mut stalled_writer,
            &payload,
            std::time::Duration::from_millis(40),
        )
        .await
        .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);

        struct InterruptingWriter {
            written: Vec<u8>,
            interrupts_left: usize,
            always_interrupt: bool,
        }
        impl AsyncWrite for InterruptingWriter {
            fn poll_write(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
                buf: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                if self.always_interrupt || self.interrupts_left > 0 {
                    if !self.always_interrupt {
                        self.interrupts_left -= 1;
                    }
                    cx.waker().wake_by_ref();
                    return std::task::Poll::Ready(Err(std::io::Error::from(
                        std::io::ErrorKind::Interrupted,
                    )));
                }
                let take = buf.len().min(32);
                self.written.extend_from_slice(&buf[..take]);
                std::task::Poll::Ready(Ok(take))
            }
            fn poll_flush(
                mut self: std::pin::Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                if self.interrupts_left > 0 {
                    self.interrupts_left -= 1;
                    cx.waker().wake_by_ref();
                    return std::task::Poll::Ready(Err(std::io::Error::from(
                        std::io::ErrorKind::Interrupted,
                    )));
                }
                std::task::Poll::Ready(Ok(()))
            }
            fn poll_shutdown(
                self: std::pin::Pin<&mut Self>,
                _cx: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Ready(Ok(()))
            }
        }

        let mut transient = InterruptingWriter {
            written: Vec::new(),
            interrupts_left: 5,
            always_interrupt: false,
        };
        write_raw_idle(
            &mut transient,
            &payload,
            std::time::Duration::from_millis(60),
        )
        .await
        .unwrap();
        assert_eq!(transient.written, payload);

        let mut endless_eintr = InterruptingWriter {
            written: Vec::new(),
            interrupts_left: 0,
            always_interrupt: true,
        };
        let err = write_raw_idle(
            &mut endless_eintr,
            &payload,
            std::time::Duration::from_millis(35),
        )
        .await
        .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn endpoint_parse_reserves_scratch_before_reading_and_releases_it_after_decode() {
        let message = ToEndpoint::Result {
            id: 7,
            result: rmcp::model::CallToolResult::success(vec![rmcp::model::ContentBlock::text(
                "line one\nline two 世界".repeat(16),
            )]),
        };
        let wire = encode_frame(&message).unwrap();
        let length = wire.len() - 4;
        let peak = endpoint_parse_peak(length).unwrap();
        let pool = crate::lease::BytePool::new(peak);
        let control = crate::lease::BytePool::new(0);
        let occupied = pool.try_acquire(1).unwrap();
        let mut input = std::io::Cursor::new(wire.clone());
        assert!(matches!(
            read_endpoint_frame_leased(&mut input, RESPONSE_LIMIT, &pool, &control, |_| false)
                .await
                .unwrap(),
            LeasedEndpointFrame::OverloadResult { id: Some(7) }
        ));
        assert_eq!(
            input.position() as usize,
            wire.len(),
            "unadmitted frames are drained"
        );
        assert_eq!(pool.used(), 1);
        drop(occupied);
        let mut input = std::io::Cursor::new(wire);
        let LeasedEndpointFrame::Frame(decoded, lease) =
            read_endpoint_frame_leased(&mut input, RESPONSE_LIMIT, &pool, &control, |_| false)
                .await
                .unwrap()
        else {
            panic!("exact peak must fit")
        };
        assert_eq!(decoded, message);
        assert_eq!(
            pool.used(),
            length,
            "the parser scratch and raw frame are gone"
        );
        drop(decoded);
        drop(lease);
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn read_endpoint_frame_leased_routes_control_and_handles_split_utf8_or_progress() {
        let main_pool = crate::lease::BytePool::new(1024);
        let control_pool = crate::lease::BytePool::new(2048);

        // 1. Ordinary non-error result when `main_pool` cannot fit the complete parse peak must NOT steal `control_pool`.
        let big = ToEndpoint::Result {
            id: 77,
            result: rmcp::model::CallToolResult::success(vec![rmcp::model::ContentBlock::text(
                "x".repeat(260),
            )]),
        };
        // 2. Control result (`is_control_call(78) == true`) succeeds via `control_pool` even with `main_pool` full.
        let held_main = main_pool.try_acquire(1024).unwrap();
        let control_res = ToEndpoint::Result {
            id: 78,
            result: rmcp::model::CallToolResult::success(vec![rmcp::model::ContentBlock::text(
                "list output",
            )]),
        };
        // 3. Error receipt (`is_error == Some(true)`) succeeds via `control_pool` while `main_pool` is full.
        let err_res = ToEndpoint::Result {
            id: 79,
            result: rmcp::model::CallToolResult::error(vec![rmcp::model::ContentBlock::text(
                "taskId: t-1234",
            )]),
        };

        let mut wire = encode_frame(&big).unwrap();
        let control_wire = encode_frame(&control_res).unwrap();
        let err_wire = encode_frame(&err_res).unwrap();
        wire.extend_from_slice(&control_wire);
        wire.extend_from_slice(&err_wire);
        let mut cursor = std::io::Cursor::new(wire);

        match read_endpoint_frame_leased(
            &mut cursor,
            RESPONSE_LIMIT,
            &main_pool,
            &control_pool,
            |id| id == 78,
        )
        .await
        .unwrap()
        {
            LeasedEndpointFrame::OverloadResult { id } => assert_eq!(id, Some(77)),
            other => panic!("expected OverloadResult(77), got {other:?}"),
        }
        assert_eq!(control_pool.used(), 0);

        match read_endpoint_frame_leased(
            &mut cursor,
            RESPONSE_LIMIT,
            &main_pool,
            &control_pool,
            |id| id == 78,
        )
        .await
        .unwrap()
        {
            LeasedEndpointFrame::Frame(ToEndpoint::Result { id, .. }, lease) => {
                assert_eq!(id, 78);
                assert_eq!(lease.bytes(), control_wire.len() - 4);
                assert_eq!(control_pool.used(), lease.bytes());
                drop(lease);
                assert_eq!(control_pool.used(), 0);
            }
            other => panic!("expected control Frame(78), got {other:?}"),
        }

        match read_endpoint_frame_leased(
            &mut cursor,
            RESPONSE_LIMIT,
            &main_pool,
            &control_pool,
            |_| false,
        )
        .await
        .unwrap()
        {
            LeasedEndpointFrame::Frame(ToEndpoint::Result { id, result }, lease) => {
                assert_eq!(id, 79);
                assert_eq!(result.is_error, Some(true));
                drop(lease);
            }
            other => panic!("expected error Frame(79), got {other:?}"),
        }

        // 4. When both pools are full, draining a `Progress` frame yields `OverloadProgress` and
        // draining a `Result` frame whose multibyte UTF-8 text is cut at byte 256 still extracts `id`.
        let _held_ctrl = control_pool.try_acquire(2048).unwrap();
        let progress = ToEndpoint::Progress {
            id: 98,
            seconds: 1.5,
            message: "中".repeat(200),
        };
        let utf8_result = ToEndpoint::Result {
            id: 99,
            result: rmcp::model::CallToolResult::success(vec![rmcp::model::ContentBlock::text(
                "汉".repeat(300),
            )]),
        };
        let utf8_wire = encode_frame(&utf8_result).unwrap();
        // Verify byte 256 of the body actually lands mid-codepoint (`std::str::from_utf8` fails).
        assert!(std::str::from_utf8(&utf8_wire[4..4 + 256]).is_err());

        let mut drain_wire = encode_frame(&progress).unwrap();
        drain_wire.extend_from_slice(&utf8_wire);
        drop(held_main);
        let tail_ok = ToEndpoint::Result {
            id: 100,
            result: rmcp::model::CallToolResult::success(vec![rmcp::model::ContentBlock::text(
                "ok",
            )]),
        };
        drain_wire.extend_from_slice(&encode_frame(&tail_ok).unwrap());

        let rem_main = main_pool.try_acquire(1024).unwrap();
        let mut drain_cursor = std::io::Cursor::new(drain_wire);
        assert!(matches!(
            read_endpoint_frame_leased(
                &mut drain_cursor,
                RESPONSE_LIMIT,
                &main_pool,
                &control_pool,
                |_| false,
            )
            .await
            .unwrap(),
            LeasedEndpointFrame::OverloadProgress
        ));
        match read_endpoint_frame_leased(
            &mut drain_cursor,
            RESPONSE_LIMIT,
            &main_pool,
            &control_pool,
            |_| false,
        )
        .await
        .unwrap()
        {
            LeasedEndpointFrame::OverloadResult { id } => assert_eq!(id, Some(99)),
            other => panic!("expected OverloadResult(99), got {other:?}"),
        }
        drop(rem_main);
        match read_endpoint_frame_leased(
            &mut drain_cursor,
            RESPONSE_LIMIT,
            &main_pool,
            &control_pool,
            |_| false,
        )
        .await
        .unwrap()
        {
            LeasedEndpointFrame::Frame(ToEndpoint::Result { id, .. }, _) => assert_eq!(id, 100),
            other => panic!("expected Frame(100), got {other:?}"),
        }

        // 5. Non-control `Result` frames larger than 64 KiB under `main_pool` overload drain on the
        // stack without borrowing `control_pool` or parsing the oversized body.
        let full_main = main_pool.try_acquire(1024).unwrap();
        let roomy_control = crate::lease::BytePool::new(crate::lease::CONTROL_LIMIT);
        let mut over_64k_body = br#"{"type":"result","id":101,"result":"#.to_vec();
        over_64k_body.resize(70 * 1024, b'!');
        let mut over_64k_wire = (over_64k_body.len() as u32).to_le_bytes().to_vec();
        over_64k_wire.extend_from_slice(&over_64k_body);
        let mut over_64k_cursor = std::io::Cursor::new(over_64k_wire);
        match read_endpoint_frame_leased(
            &mut over_64k_cursor,
            RESPONSE_LIMIT,
            &main_pool,
            &roomy_control,
            |_| false,
        )
        .await
        .unwrap()
        {
            LeasedEndpointFrame::OverloadResult { id } => assert_eq!(id, Some(101)),
            other => panic!("expected OverloadResult(101), got {other:?}"),
        }
        assert_eq!(roomy_control.used(), 0);
        drop(full_main);
    }

    #[tokio::test]
    async fn daemon_event_requests_return_overload_event_when_main_pool_is_full() {
        let requests = [
            (11, ToDaemon::EventsList { id: 11 }),
            (
                12,
                ToDaemon::EventsPoll {
                    id: 12,
                    request: crate::events::PollRequest::default(),
                },
            ),
            (
                13,
                ToDaemon::EventsStream {
                    id: 13,
                    request: crate::events::SubscribeRequest::default(),
                },
            ),
        ];

        // 1. Short frames decoded in `control` when `main` is full yield `OverloadEvent { id }`.
        let empty_main = BytePool::new(0);
        let roomy_control = BytePool::new(16 * 1024);
        for (expected_id, message) in &requests {
            let wire = encode_frame(message).unwrap();
            let mut cursor = std::io::Cursor::new(wire);
            match read_daemon_frame_owned(&mut cursor, REQUEST_LIMIT, &empty_main, &roomy_control)
                .await
                .unwrap()
            {
                DaemonFrame::OverloadEvent { id } => assert_eq!(id, Some(*expected_id)),
                _ => panic!("expected OverloadEvent({expected_id})"),
            }
            assert_eq!(roomy_control.used(), 0);
        }

        // 2. When both `main` and `control` are full, header sniffing still yields `OverloadEvent`
        //    and drains the frame cleanly so a trailing frame parses normally.
        let empty_control = BytePool::new(0);
        for (expected_id, message) in &requests {
            let mut wire = encode_frame(message).unwrap();
            wire.extend_from_slice(&encode_frame(&ToDaemon::Status).unwrap());
            let mut cursor = std::io::Cursor::new(wire);
            match read_daemon_frame_owned(&mut cursor, REQUEST_LIMIT, &empty_main, &empty_control)
                .await
                .unwrap()
            {
                DaemonFrame::OverloadEvent { id } => assert_eq!(id, Some(*expected_id)),
                _ => panic!("expected sniffed OverloadEvent({expected_id})"),
            }
            let status_pool = BytePool::new(4096);
            match read_daemon_frame_owned(&mut cursor, REQUEST_LIMIT, &status_pool, &empty_control)
                .await
                .unwrap()
            {
                DaemonFrame::Message(owned) => assert_eq!(owned.value, ToDaemon::Status),
                _ => panic!("expected trailing Status after drained OverloadEvent"),
            }
        }
    }

    #[tokio::test]
    async fn endpoint_event_frames_isolate_control_pool_and_return_overload_event() {
        let main_pool = crate::lease::BytePool::new(1024);
        let control_pool = crate::lease::BytePool::new(crate::lease::CONTROL_LIMIT);
        let _full_main = main_pool.try_acquire(1024).unwrap();

        // 1. Oversized (> 64 KiB) EventFrame skips `control_pool` and returns `OverloadEvent`.
        let mut big_body =
            br#"{"type":"event_frame","id":201,"frame":{"kind":"event","data":"#.to_vec();
        big_body.resize(70 * 1024, b'x');
        let mut big_wire = (big_body.len() as u32).to_le_bytes().to_vec();
        big_wire.extend_from_slice(&big_body);
        let mut big_cursor = std::io::Cursor::new(big_wire);
        match read_endpoint_frame_leased(
            &mut big_cursor,
            RESPONSE_LIMIT,
            &main_pool,
            &control_pool,
            |_| false,
        )
        .await
        .unwrap()
        {
            LeasedEndpointFrame::OverloadEvent { id } => assert_eq!(id, Some(201)),
            other => panic!("expected OverloadEvent(201), got {other:?}"),
        }
        assert_eq!(control_pool.used(), 0);

        // 2. Data EventFrames (<= 64 KiB) do not remain on `control_pool` and return `OverloadEvent`.
        let data_frames = [
            (
                202,
                EventWireFrame::ListResult(crate::events::EventsListResult {
                    result_type: std::borrow::Cow::Borrowed("events.list"),
                    events: vec![],
                }),
            ),
            (
                203,
                EventWireFrame::PollResult(crate::events::PollResponse {
                    result_type: std::borrow::Cow::Borrowed("events.poll"),
                    events: vec![],
                    cursor: "0:0".to_owned(),
                    has_more: false,
                    truncated: false,
                    next_poll_ms: 1000,
                }),
            ),
            (
                204,
                EventWireFrame::Active(crate::events::ActiveNotice {
                    cursor: "0:0".to_owned(),
                    truncated: false,
                    heartbeat_ms: 30_000,
                }),
            ),
        ];
        for (expected_id, frame) in data_frames {
            let wire = encode_frame(&ToEndpoint::EventFrame {
                id: expected_id,
                frame,
            })
            .unwrap();
            let mut cursor = std::io::Cursor::new(wire);
            match read_endpoint_frame_leased(
                &mut cursor,
                RESPONSE_LIMIT,
                &main_pool,
                &control_pool,
                |_| false,
            )
            .await
            .unwrap()
            {
                LeasedEndpointFrame::OverloadEvent { id } => assert_eq!(id, Some(expected_id)),
                other => panic!("expected OverloadEvent({expected_id}), got {other:?}"),
            }
            assert_eq!(control_pool.used(), 0);
        }

        // 3. Control EventFrames still decode on `control_pool`.
        let control_frames = [
            (
                210,
                EventWireFrame::Error(IpcEventError {
                    code: -32025,
                    message: "busy".to_owned(),
                    data: None,
                }),
            ),
            (
                211,
                EventWireFrame::Terminated(IpcEventError {
                    code: -32025,
                    message: "terminated".to_owned(),
                    data: None,
                }),
            ),
            (212, EventWireFrame::Complete),
            (
                213,
                EventWireFrame::Heartbeat {
                    cursor: "0:1".to_owned(),
                },
            ),
        ];
        for (expected_id, frame) in control_frames {
            let wire = encode_frame(&ToEndpoint::EventFrame {
                id: expected_id,
                frame: frame.clone(),
            })
            .unwrap();
            let mut cursor = std::io::Cursor::new(wire);
            match read_endpoint_frame_leased(
                &mut cursor,
                RESPONSE_LIMIT,
                &main_pool,
                &control_pool,
                |_| false,
            )
            .await
            .unwrap()
            {
                LeasedEndpointFrame::Frame(ToEndpoint::EventFrame { id, frame: got }, lease) => {
                    assert_eq!(id, expected_id);
                    assert_eq!(got, frame);
                    assert!(control_pool.used() > 0);
                    drop(lease);
                }
                other => panic!("expected control EventFrame({expected_id}), got {other:?}"),
            }
            assert_eq!(control_pool.used(), 0);
        }
    }
}
