//! Task registry: spawn, log capture, unseen-output cursors, input queues, and shutdown.

mod lifecycle;
mod query;
mod quota;
pub(crate) mod script;
pub(crate) mod session;
pub mod state;
mod store;
#[cfg(test)]
mod tests;
mod workers;

pub(crate) use self::lifecycle::{ScriptIo, ScriptPublishArgs};
pub use self::lifecycle::{StartArgs, Started};
pub use self::query::{ChildSummary, ListQuery, ListedResult, ListedTask, Shown};
pub use self::quota::Limits;
pub(crate) use self::state::TASK_LOG_LIMIT;
pub use self::state::{DRAIN_CAP, EVICTED_TRANSCRIPT, TaskError};
#[cfg(test)]
pub(crate) use self::store::ONE_TASKS;
pub use self::store::Tasks;
