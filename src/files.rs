//! The text file tools: `read`, `write` and `edit`, and the path resolution they share.
//!
//! Every function here blocks: the callers run them on `spawn_blocking`, so one large file never
//! holds a runtime thread. Read streams a file and keeps only the window it may show; write and
//! edit hold the whole file, because they rewrite it.

pub(crate) mod edit;
pub(crate) mod path;
pub(crate) mod read;
pub(crate) mod write;

#[cfg(test)]
mod tests;

pub use self::edit::{EditError, EditFailure, EditMatch, FileEditData, edit_core};
use self::read::format_size;
pub use self::read::{FileReadData, MediaPayload, ReadPolicy, TextSlice, read_core};
pub use self::write::{FileWriteData, write_core};
use crate::lease::Lease;
use crate::media;
use crate::session::LaunchContext;
use rmcp::model::{CallToolResult, ContentBlock};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileError {
    Validation(String),
    NotFound(String),
    Limit(String),
    Overload(String),
    Cancelled(String),
    Io(String),
}

impl FileError {
    pub fn message(&self) -> &str {
        match self {
            Self::Validation(msg)
            | Self::NotFound(msg)
            | Self::Limit(msg)
            | Self::Overload(msg)
            | Self::Cancelled(msg)
            | Self::Io(msg) => msg,
        }
    }

    pub fn into_message(self) -> String {
        match self {
            Self::Validation(msg)
            | Self::NotFound(msg)
            | Self::Limit(msg)
            | Self::Overload(msg)
            | Self::Cancelled(msg)
            | Self::Io(msg) => msg,
        }
    }
}

impl fmt::Display for FileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message())
    }
}

/// The one lock that serializes write and edit, so a read-modify-write cannot interleave with
/// another one. It is held for the whole blocking operation, never across an await.
static MUTATION: Mutex<()> = Mutex::new(());

pub(super) fn lock() -> MutexGuard<'static, ()> {
    MUTATION
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ReadRequest {
    /// Path to the file to read (relative or absolute)
    pub path: String,
    /// Line number to start reading from (1-indexed; negative counts from the end, -1 is the last line)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<i64>,
    /// Maximum number of lines to read (at least 1)
    #[schemars(range(min = 1))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct WriteRequest {
    /// Path to the file to write (relative or absolute)
    pub path: String,
    /// Content to write to the file
    pub content: String,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EditRequest {
    /// Path to the file to edit (relative or absolute)
    pub path: String,
    /// One or more targeted replacements. Each edit is matched against the original file, not incrementally. Do not include overlapping or nested edits. If two changes touch the same block or nearby lines, merge them into one edit instead.
    pub edits: Vec<Edit>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Edit {
    /// Exact text for one targeted replacement. It must be unique in the original file (or within startLine..=endLine) and must not overlap with any other edits[].oldText in the same call.
    pub old_text: String,
    /// Replacement text for this targeted edit.
    pub new_text: String,
    /// Optional 1-based line range the match must start in (inclusive first line); use it to pick one of several identical matches.
    #[schemars(range(min = 1))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_line: Option<u64>,
    /// Optional 1-based inclusive last line the match must start in.
    #[schemars(range(min = 1))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_line: Option<u64>,
}

#[derive(Debug)]
pub struct TaskMeta {
    pub bare_task_id: String,
    pub log_path: String,
}

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
            lease: None,
            overload: false,
            task_meta: None,
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
            lease: None,
            overload: true,
            task_meta: None,
            payload_owner: Default::default(),
        },
        Err(error) => TrackedResult::plain(failed(error.into_message())),
    });
    tracked.payload_owner = owner;
    tracked
}

fn render_read_agent(data: FileReadData, owner: crate::lease::Owner) -> TrackedResult {
    match data {
        FileReadData::Text(slice) => {
            let text = format_text_slice(&slice);
            TrackedResult {
                result: CallToolResult::success(vec![ContentBlock::text(text)]),
                lease: Some(slice.lease),
                overload: false,
                task_meta: None,
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
            let block = media::content_block(kind, &mime_type, encoded_base64, &resolved_path);
            TrackedResult {
                result: CallToolResult::success(vec![ContentBlock::text(summary_line), block]),
                lease: Some(lease),
                overload: false,
                task_meta: None,
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
                lease: None,
                overload: false,
                task_meta: None,
                payload_owner: owner,
            }
        }
    }
}

fn format_text_slice(slice: &TextSlice) -> String {
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

fn failed(message: String) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(message)])
}
