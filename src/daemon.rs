//! The daemon's side of a connection: the handshake, the session table, the writer task, and the
//! calls it runs over local byte streams and Streamable HTTP.

mod dispatch;
mod events;
mod serve;
mod state;
mod wire;

pub use self::serve::serve_process;
pub use self::state::{HttpLaunch, WRITE_STALL};

#[cfg(test)]
pub(crate) use self::state::{Config, Daemon, PendingHttp};

#[cfg(test)]
use self::dispatch::{CallDispatch, dispatch_call};
#[cfg(all(test, windows))]
use self::state::MAX_IN_FLIGHT;
#[cfg(test)]
use self::state::{MAX_CONNECTIONS, RENDER_SLOTS, RESERVED, SESSION_TTL, lock};
#[cfg(test)]
use self::wire::{BoxedStream, Outgoing};
#[cfg(test)]
use crate::env::LaunchContext;
#[cfg(test)]
use crate::ipc::{self, ToDaemon, ToEndpoint};
#[cfg(test)]
use crate::lease::{self, BytePool};
#[cfg(test)]
use crate::service::core::RenderSlot;
#[cfg(test)]
use crate::tasks::Tasks;
#[cfg(test)]
use crate::tasks::session::Session;
#[cfg(test)]
use crate::transport::Listener;
#[cfg(test)]
use rmcp::model::{CallToolResult, ContentBlock};
#[cfg(test)]
use std::collections::HashMap;
#[cfg(test)]
use std::pin::Pin;
#[cfg(test)]
use std::sync::{Arc, Mutex, MutexGuard};
#[cfg(test)]
use std::time::{Duration, Instant};
#[cfg(test)]
use tokio::io::{ReadHalf, WriteHalf};
#[cfg(test)]
use tokio::sync::{Semaphore, mpsc, oneshot};
#[cfg(test)]
use tokio::time::timeout;
#[cfg(test)]
use tokio_util::sync::CancellationToken;

#[cfg(test)]
#[expect(
    clippy::too_many_arguments,
    reason = "preserves the test call signature used by daemon/tests.rs"
)]
async fn run_call(
    daemon: Arc<Daemon>,
    session: Arc<Session>,
    connection: CancellationToken,
    token: CancellationToken,
    id: u64,
    tool: String,
    arguments: serde_json::Value,
    in_flight: Arc<Mutex<HashMap<u64, CancellationToken>>>,
    outgoing: mpsc::UnboundedSender<Outgoing>,
    rpc_id_bytes: usize,
    input_owner: crate::lease::Owner,
) {
    dispatch_call(CallDispatch {
        daemon,
        session,
        connection,
        token,
        id,
        tool,
        arguments,
        in_flight,
        outgoing,
        rpc_id_bytes,
        input_owner,
    })
    .await;
}

#[cfg(test)]
mod tests;
