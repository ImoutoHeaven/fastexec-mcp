//! Agent presentation: pure-text/media MCP `CallToolResult` formatting, payload budgeting,
//! attachment packing, and background footers.

mod budget;
mod exec;
pub mod files;
mod list;
#[cfg(test)]
mod tests;
pub mod types;

pub(crate) use self::budget::{result_payload_capacity, unleased_result_payload_capacity};
pub use self::exec::{exec_agent, exec_agent_json, finish_exec_agent};
#[cfg(test)]
pub(crate) use self::list::render_list_items_for_test;

#[cfg(test)]
use self::budget::wait_note;
#[cfg(test)]
use self::exec::background;
#[cfg(test)]
use self::types::{AgentDelivery, DaemonDelivery, TrackedResult};
#[cfg(test)]
use crate::lease::Lease;
#[cfg(test)]
use crate::service::schema::{Action, ExecPolicy, Request};
#[cfg(test)]
use crate::service::{Consumer, PreparedExec, PreparedList};
#[cfg(test)]
use crate::tasks::ChildSummary;
#[cfg(test)]
use crate::term::output::Truncate;
#[cfg(test)]
use crate::wait::WaitEnd;
