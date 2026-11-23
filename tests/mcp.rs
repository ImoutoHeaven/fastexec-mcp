//! Contract tests through the real MCP stdio boundary: each test drives the built binary.

mod support;
#[path = "support/transfer_http.rs"]
mod transfer_http;

#[path = "mcp/codemode.rs"]
mod codemode;
#[path = "mcp/common.rs"]
mod common;
#[path = "mcp/exec.rs"]
mod exec;
#[path = "mcp/files.rs"]
mod files;
#[path = "mcp/protocol.rs"]
mod protocol;
#[path = "mcp/pty.rs"]
mod pty;
