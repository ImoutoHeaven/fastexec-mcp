use crate::ipc::{self, ToEndpoint};
use crate::lease::Lease;
use rmcp::model::{CallToolResult, ContentBlock};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// A connection's byte stream, boxed so the halves have one type whatever carries them.
pub(super) trait Stream: AsyncRead + AsyncWrite + Send {}

impl<T: AsyncRead + AsyncWrite + Send> Stream for T {}

/// A boxed stream, which is `Unpin` however the stream itself is.
pub(super) type BoxedStream = std::pin::Pin<Box<dyn Stream>>;
pub(super) type Reader = ReadHalf<BoxedStream>;
pub(super) type Writer = WriteHalf<BoxedStream>;

pub(super) enum OutgoingPermit {
    Main(
        #[expect(
            dead_code,
            reason = "held for Drop until the outgoing frame is written or dropped"
        )]
        Lease,
    ),
    Reserved(
        #[expect(
            dead_code,
            reason = "held for Drop until the outgoing frame is written or dropped"
        )]
        OwnedSemaphorePermit,
    ),
}

/// A frame on its way out, with the lease or control permit its bytes hold until written.
pub(super) struct Outgoing {
    pub(super) bytes: Vec<u8>,
    pub(super) _permit: Option<OutgoingPermit>,
    /// Signalled once the frame is written. A progress forwarder waits on it, so at most one
    /// progress frame per call is queued and the latest wins.
    pub(super) written: Option<oneshot::Sender<()>>,
}

/// Writes queued frames one at a time. A write that stalls for `stall` closes the connection,
/// which cancels only this connection's waits.
pub(super) async fn write_loop(
    mut writer: Writer,
    mut outgoing: mpsc::UnboundedReceiver<Outgoing>,
    closing: CancellationToken,
    stall: Duration,
) {
    loop {
        let mut item = tokio::select! {
            biased;
            _ = closing.cancelled() => return,
            item = outgoing.recv() => match item {
                Some(item) => item,
                None => return,
            },
        };
        // The write gives way to the connection token as well as to the stall: a replacement or
        // a close drops this half, the queued frames, and their permits at once.
        let sent = {
            let writing = async { ipc::write_raw_idle(&mut writer, &item.bytes, stall).await };
            tokio::pin!(writing);
            tokio::select! {
                biased;
                _ = closing.cancelled() => false,
                result = &mut writing => result.is_ok(),
            }
        };
        if !sent {
            closing.cancel();
            return;
        }
        if let Some(written) = item.written.take() {
            let _ = written.send(());
        }
    }
}

/// Queues a result the reader itself makes. The reader never awaits permits, so a frame that
/// does not fit the reserved share closes the connection instead of blocking Cancel and Bye.
pub(super) fn queue_result(
    outgoing: &mpsc::UnboundedSender<Outgoing>,
    reserved: &Arc<Semaphore>,
    closing: &CancellationToken,
    id: u64,
    result: CallToolResult,
) {
    queue_frame(
        outgoing,
        reserved,
        closing,
        &ToEndpoint::Result { id, result },
    );
}

/// Queues one frame the reader makes, taking its bytes from the reserved share. A frame that does
/// not fit closes the connection instead of blocking Cancel, Bye, and Stop.
pub(super) fn queue_frame(
    outgoing: &mpsc::UnboundedSender<Outgoing>,
    reserved: &Arc<Semaphore>,
    closing: &CancellationToken,
    message: &ToEndpoint,
) {
    let Ok(length) = ipc::frame_len(message) else {
        return;
    };
    let Ok(want) = u32::try_from(length) else {
        closing.cancel();
        return;
    };
    let Ok(permit) = Arc::clone(reserved).try_acquire_many_owned(want) else {
        closing.cancel();
        return;
    };
    if let Ok(bytes) = ipc::encode_frame(message) {
        let _ = outgoing.send(Outgoing {
            bytes,
            _permit: Some(OutgoingPermit::Reserved(permit)),
            written: None,
        });
    }
}

pub(super) fn error(message: String) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(message)])
}

/// Writes a `Reject` under the write stall, or not at all once the daemon is stopping.
pub(super) async fn reject(
    writer: &mut Writer,
    stall: Duration,
    stopped: &CancellationToken,
    message: String,
) {
    let message = ToEndpoint::Reject { message };
    let writing = ipc::write_frame_idle(writer, &message, stall);
    tokio::select! {
        biased;
        _ = stopped.cancelled() => {}
        _ = writing => {}
    }
}
