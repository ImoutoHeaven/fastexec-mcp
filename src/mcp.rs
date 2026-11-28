//! MCP server handler, tool routing, schema rewriting, and progress/event forwarding.

mod events;
mod handler;
pub(crate) mod ingress;
mod leases;
mod schema;
mod server;
mod skills;

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) use events::{decode_mcp_header_value, event_error_payload};
pub(crate) use handler::McpService;
pub use leases::Principal;
pub(crate) use leases::{HttpResponseLease, RequestLeases};
#[cfg(test)]
pub(crate) use schema::host_line;
pub(crate) use server::{Backend, Server};
pub(crate) use skills::validate_skill_catalog;
