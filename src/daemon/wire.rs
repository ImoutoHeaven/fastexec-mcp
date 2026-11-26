use crate::lease::Lease;
use tokio::sync::{OwnedSemaphorePermit, oneshot};

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
