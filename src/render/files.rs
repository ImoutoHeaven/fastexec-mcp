//! Agent `TrackedResult` and `CallToolResult` wrappers for `read`, `write`, `edit`, and `transfer`.

use super::types::{TrackedResult, media_content_block};
use crate::env::LaunchContext;
use crate::files::edit::{EditError, edit_core};
use crate::files::read::{
    FileReadData, MediaPayload, ReadPolicy, TextSlice, format_size, read_core,
};
use crate::files::write::{FileWriteData, write_core};
use crate::files::{EditRequest, FileError, ReadRequest, WriteRequest};
use crate::transfer::{self, Transfers, inspect_transfer_request};
use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::Value;
use std::sync::Arc;

/// Runs one file tool from raw arguments, the way a daemon receives them, with the same
/// validation and errors as a local call.
pub async fn call_tracked(
    tool: &str,
    arguments: Value,
    launch: Arc<LaunchContext>,
    transfer: bool,
    cancel: Option<tokio_util::sync::CancellationToken>,
    input_owner: crate::lease::Owner,
) -> TrackedResult {
    match tool {
        "read" => match serde_json::from_value::<ReadRequest>(arguments) {
            Ok(request) => read_tracked(request, launch, transfer, cancel, input_owner).await,
            Err(error) => {
                TrackedResult::plain(failed(format!("failed to deserialize parameters: {error}")))
            }
        },
        "write" => match serde_json::from_value::<WriteRequest>(arguments) {
            Ok(request) => write_tracked(request, launch, input_owner).await,
            Err(error) => {
                TrackedResult::plain(failed(format!("failed to deserialize parameters: {error}")))
            }
        },
        "edit" => match serde_json::from_value::<EditRequest>(arguments) {
            Ok(request) => edit_tracked(request, launch, input_owner).await,
            Err(error) => {
                TrackedResult::plain(failed(format!("failed to deserialize parameters: {error}")))
            }
        },
        other => TrackedResult::plain(failed(format!("unknown tool {other}"))),
    }
}

/// Reads one file for an Agent caller: pure text or media blocks with `structured_content: None`.
pub async fn read_tracked(
    request: ReadRequest,
    launch: Arc<LaunchContext>,
    transfer: bool,
    cancel: Option<tokio_util::sync::CancellationToken>,
    input_owner: crate::lease::Owner,
) -> TrackedResult {
    let outcome = read_core(
        request,
        launch,
        transfer,
        ReadPolicy::AGENT,
        None,
        cancel,
        input_owner,
    )
    .await;
    let owner = outcome.owner.clone();
    let mut tracked = outcome.run(|read| match read {
        Ok(data) => render_read_agent(data, owner.clone()),
        Err(FileError::Overload(message)) => TrackedResult {
            result: failed(message),
            overload: true,
            task_meta: None,
            lease: None,
            payload_owner: Default::default(),
        },
        Err(error) => TrackedResult::plain(failed(error.into_message())),
    });
    tracked.payload_owner = owner;
    tracked
}

pub(crate) fn render_read_agent(data: FileReadData, owner: crate::lease::Owner) -> TrackedResult {
    match data {
        FileReadData::Text(slice) => {
            let text = format_text_slice(&slice);
            TrackedResult {
                result: CallToolResult::success(vec![ContentBlock::text(text)]),
                overload: false,
                task_meta: None,
                lease: Some(slice.lease),
                payload_owner: owner,
            }
        }
        FileReadData::Media(MediaPayload::Inline {
            kind,
            resolved_path,
            mime_type,
            encoded_base64,
            summary_line,
            lease,
            ..
        }) => {
            let block = media_content_block(kind, &mime_type, encoded_base64, &resolved_path);
            TrackedResult {
                result: CallToolResult::success(vec![ContentBlock::text(summary_line), block]),
                overload: false,
                task_meta: None,
                lease: Some(lease),
                payload_owner: owner,
            }
        }
        FileReadData::Media(MediaPayload::Descriptor {
            kind,
            mime_type,
            bytes,
            note,
            ..
        }) => {
            let summary = note.unwrap_or_else(|| {
                format!(
                    "Read {} file [{}] ({} B)",
                    kind.as_str(),
                    mime_type,
                    bytes.unwrap_or(0)
                )
            });
            TrackedResult {
                result: CallToolResult::success(vec![ContentBlock::text(summary)]),
                overload: false,
                task_meta: None,
                lease: None,
                payload_owner: owner,
            }
        }
    }
}

pub(crate) fn format_text_slice(slice: &TextSlice) -> String {
    let mut notices = Vec::new();
    if slice.line_too_long {
        let shell_path = slice.path.replace('\'', "'\\''");
        notices.push(format!(
            "[Line {} is {}, exceeds {} limit. Use exec: sed -n '{}p' '{}' | head -c {}]",
            slice.start_line,
            format_size(slice.first_line_bytes),
            format_size(slice.max_bytes),
            slice.start_line,
            shell_path,
            slice.max_bytes
        ));
    } else if slice.budget_truncated {
        let end_display = slice.start_line + slice.shown_lines - 1;
        let next = end_display + 1;
        if slice.stopped_on_bytes {
            notices.push(format!(
                "[Showing lines {}-{end_display} of {} ({} limit). Use offset={next} to continue.]",
                slice.start_line,
                slice.total_lines,
                format_size(slice.max_bytes)
            ));
        } else {
            notices.push(format!(
                "[Showing lines {}-{end_display} of {}. Use offset={next} to continue.]",
                slice.start_line, slice.total_lines
            ));
        }
    } else if slice.truncated
        && let Some(next) = slice.next_offset
    {
        let remaining = slice.total_lines - (next - 1);
        notices.push(format!(
            "[{remaining} more lines in file. Use offset={next} to continue.]"
        ));
    }
    if slice.lossy {
        notices.push("[Invalid UTF-8 was replaced with U+FFFD.]".to_owned());
    }
    let notice_block = notices.join("\n\n");
    if slice.line_too_long || slice.text.is_empty() {
        notice_block
    } else if notice_block.is_empty() {
        slice.text.clone()
    } else if slice.text.ends_with('\n') {
        format!("{}\n{notice_block}", slice.text)
    } else {
        format!("{}\n\n{notice_block}", slice.text)
    }
}

/// Writes one file, creating its parent directories.
pub async fn write_tracked(
    request: WriteRequest,
    launch: Arc<LaunchContext>,
    input_owner: crate::lease::Owner,
) -> TrackedResult {
    let outcome = write_core(request, launch, None, input_owner).await;
    let owner = outcome.owner.clone();
    let (result, overload) = outcome.run(|res| match res {
        Ok(FileWriteData { path, .. }) => {
            let text = format!("Successfully wrote to {path}");
            (
                CallToolResult::success(vec![ContentBlock::text(text)]),
                false,
            )
        }
        Err(FileError::Overload(message)) => (failed(message), true),
        Err(err) => (failed(err.into_message()), false),
    });
    TrackedResult {
        overload,
        ..TrackedResult::owned(result, owner)
    }
}

/// Applies targeted replacements to one file.
pub async fn edit_tracked(
    request: EditRequest,
    launch: Arc<LaunchContext>,
    input_owner: crate::lease::Owner,
) -> TrackedResult {
    let outcome = edit_core(request, launch, None, input_owner).await;
    let owner = outcome.owner.clone();
    let (result, overload) = outcome.run(|res| match res {
        Ok(data) => {
            let mut text = format!(
                "Successfully replaced {} block(s) in {}.",
                data.replacements, data.path
            );
            for (index, item) in data.edits.iter().enumerate() {
                text.push_str(&format!("\nedits[{index}]: line {}", item.line));
                if item.fuzzy {
                    text.push_str(" (fuzzy match)");
                }
            }
            (
                CallToolResult::success(vec![ContentBlock::text(text)]),
                false,
            )
        }
        Err(EditError {
            error: FileError::Overload(message),
            ..
        }) => (failed(message), true),
        Err(err) => (failed(err.error.into_message()), false),
    });
    TrackedResult {
        overload,
        ..TrackedResult::owned(result, owner)
    }
}

pub(crate) fn transfer_error(message: String) -> CallToolResult {
    failed(message)
}

pub(crate) fn mint_transfer(
    path: std::path::PathBuf,
    shown_path: String,
    bytes: Option<u64>,
    table: &Arc<Transfers>,
    base: &str,
) -> CallToolResult {
    let id = match table.create(path) {
        Ok(id) => id,
        Err(message) => return transfer_error(message),
    };
    let url = format!("{base}t/{id}");
    let text = match bytes {
        Some(size) => format!("{url}\n{shown_path} · {size} bytes"),
        None => format!("{url}\n{shown_path} · does not exist yet; a PUT creates it"),
    };
    CallToolResult::success(vec![ContentBlock::text(text)])
}

pub(crate) fn publish_transfer<'a>(
    request: transfer::Request,
    launch: Arc<LaunchContext>,
    table: &'a Arc<Transfers>,
    base: &'a str,
    input_owner: crate::lease::Owner,
) -> impl std::future::Future<Output = TrackedResult> + 'a {
    let inspection = inspect_transfer_request(request, launch, input_owner);
    async move {
        let inspected = inspection.await;
        let owner = inspected.owner.clone();
        let result = inspected.run(|inspected| match inspected {
            Ok((path, shown_path, bytes)) => mint_transfer(path, shown_path, bytes, table, base),
            Err(message) => transfer_error(message),
        });
        TrackedResult::owned(result, owner)
    }
}

fn failed(message: String) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(message)])
}
