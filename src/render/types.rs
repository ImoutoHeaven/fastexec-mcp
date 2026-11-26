//! Agent delivery options, tracked result ownership, and MCP content block helpers.

use crate::lease::Lease;
use crate::media;
use crate::service::core::{RenderSlot, TaskMeta};
use rmcp::model::{CallToolResult, ContentBlock, ResourceContents};
use std::path::Path;

pub struct TrackedResult {
    pub result: CallToolResult,
    pub overload: bool,
    pub task_meta: Option<TaskMeta>,
    pub lease: Option<Lease>,
    pub payload_owner: crate::lease::Owner,
}

impl TrackedResult {
    pub fn plain(result: CallToolResult) -> Self {
        Self {
            result,
            overload: false,
            task_meta: None,
            lease: None,
            payload_owner: Default::default(),
        }
    }

    pub fn owned(result: CallToolResult, payload_owner: crate::lease::Owner) -> Self {
        Self {
            payload_owner,
            ..Self::plain(result)
        }
    }
}

pub struct DaemonDelivery<'a> {
    pub slot: &'a RenderSlot<'a>,
    pub ipc_body_overhead: usize,
}

#[derive(Default)]
pub struct AgentDelivery<'a> {
    pub daemon_ipc: Option<DaemonDelivery<'a>>,
    pub http_response: bool,
    /// Encoded outer JSON-RPC ID length, known before committing output cursors.
    pub rpc_id_bytes: usize,
}

pub fn error_result(message: &str) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(message)])
}

pub fn schema_error(message: String) -> TrackedResult {
    TrackedResult::plain(CallToolResult::error(vec![ContentBlock::text(message)]))
}

pub(crate) fn media_content_block(
    kind: media::MediaKind,
    mime: &str,
    encoded_base64: String,
    path: &Path,
) -> ContentBlock {
    match kind {
        media::MediaKind::Image => ContentBlock::image(encoded_base64, mime.to_owned()),
        media::MediaKind::Audio => ContentBlock::audio(encoded_base64, mime.to_owned()),
        media::MediaKind::Document | media::MediaKind::Video => ContentBlock::resource(
            ResourceContents::blob(encoded_base64, media::file_uri(path))
                .with_mime_type(mime.to_owned()),
        ),
    }
}
