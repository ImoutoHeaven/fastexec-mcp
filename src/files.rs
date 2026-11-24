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

use self::edit::{EditError, FileEditData};
pub use self::edit::edit_core;
use self::read::{FileReadData, MediaPayload, MediaReadMode, TextSlice, format_size};
pub use self::read::{ReadPolicy, read_core};
use self::write::FileWriteData;
pub use self::write::write_core;
use crate::lease::Lease;
use crate::media;
use crate::service::render::{self as service_render, OutputMode, Render};
use crate::session::LaunchContext;
use rmcp::model::{CallToolResult, ContentBlock};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
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
    render: Render,
    cancel: Option<tokio_util::sync::CancellationToken>,
    input_owner: crate::lease::Owner,
) -> TrackedResult {
    match tool {
        "read" => match serde_json::from_value::<ReadRequest>(arguments) {
            Ok(request) => {
                read_tracked(request, launch, transfer, render, cancel, input_owner).await
            }
            Err(error) => TrackedResult::plain(schema_failed(format!(
                "failed to deserialize parameters: {error}"
            ))),
        },
        "write" => match serde_json::from_value::<WriteRequest>(arguments) {
            Ok(request) => write_tracked(request, launch, render, input_owner).await,
            Err(error) => TrackedResult::plain(schema_failed(format!(
                "failed to deserialize parameters: {error}"
            ))),
        },
        "edit" => match serde_json::from_value::<EditRequest>(arguments) {
            Ok(request) => edit_tracked(request, launch, render, input_owner).await,
            Err(error) => TrackedResult::plain(schema_failed(format!(
                "failed to deserialize parameters: {error}"
            ))),
        },
        other => TrackedResult::plain(schema_failed(format!("unknown tool {other}"))),
    }
}

/// Reads one file: its text, media attachment, or the message saying why it is not shown.
pub async fn read_tracked(
    request: ReadRequest,
    launch: Arc<LaunchContext>,
    transfer: bool,
    render: Render,
    cancel: Option<tokio_util::sync::CancellationToken>,
    input_owner: crate::lease::Owner,
) -> TrackedResult {
    read_with_policy_and_permit(
        request,
        launch,
        transfer,
        render,
        ReadPolicy::AGENT,
        None,
        cancel,
        input_owner,
    )
    .await
}

/// Reads one file for an in-process codemode script: whole-file when neither `offset` nor
/// `limit` is given, or a 4 MiB slice otherwise. `permit` stays inside the blocking worker
/// until disk I/O finishes, even if the awaiting task is cancelled.
pub(crate) async fn read_nested(
    request: ReadRequest,
    launch: Arc<LaunchContext>,
    transfer: bool,
    render: Render,
    permit: tokio::sync::OwnedSemaphorePermit,
    cancel: tokio_util::sync::CancellationToken,
    input_owner: crate::lease::Owner,
) -> TrackedResult {
    read_with_policy_and_permit(
        request,
        launch,
        transfer,
        render,
        ReadPolicy::PROGRAM,
        Some(permit),
        Some(cancel),
        input_owner,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn read_with_policy_and_permit(
    request: ReadRequest,
    launch: Arc<LaunchContext>,
    transfer: bool,
    render: Render,
    policy: ReadPolicy,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    cancel: Option<tokio_util::sync::CancellationToken>,
    input_owner: crate::lease::Owner,
) -> TrackedResult {
    let effective = render.effective();
    let outcome =
        read_core(request, launch, transfer, policy, permit, cancel, input_owner).await;
    let owner = outcome.owner.clone();
    let mut tracked = outcome.run(|read| match read {
        Ok(data) => read_data_to_outcome(data, policy, owner.clone()).into_tracked(effective),
        Err(FileError::Overload(message)) => TrackedResult {
            result: service_render::suppress(failed(message), effective),
            lease: None,
            overload: true,
            task_meta: None,
            payload_owner: Default::default(),
        },
        Err(error) => TrackedResult::plain(service_render::suppress(
            failed(error.into_message()),
            effective,
        )),
    });
    tracked.payload_owner = owner;
    tracked
}

pub(super) fn read_data_to_outcome(
    data: FileReadData,
    policy: ReadPolicy,
    owner: crate::lease::Owner,
) -> Outcome {
    match data {
        FileReadData::Text(slice) => text_slice_to_outcome(slice, owner),
        FileReadData::Media(MediaPayload::Inline {
            kind,
            path,
            resolved_path,
            mime_type,
            bytes,
            encoded_base64,
            summary_line,
            image_transform,
            lease,
        }) => {
            let block = media::content_block(kind, &mime_type, encoded_base64, &resolved_path);
            let mut map = serde_json::Map::new();
            map.insert("ok".into(), json!(true));
            map.insert("kind".into(), json!(kind.as_str()));
            map.insert("path".into(), json!(path));
            map.insert("mimeType".into(), json!(mime_type));
            map.insert("bytes".into(), json!(bytes));
            map.insert("attached".into(), json!(true));
            if let Some(transform) = image_transform {
                map.insert("originalBytes".into(), json!(transform.original_bytes));
                map.insert("originalWidth".into(), json!(transform.original_width));
                map.insert("originalHeight".into(), json!(transform.original_height));
                map.insert("width".into(), json!(transform.width));
                map.insert("height".into(), json!(transform.height));
            }
            Outcome {
                owner,
                notice: summary_line.clone(),
                text: summary_line,
                body: None,
                attachment: Some(block),
                lease: Some(lease),
                structured: Value::Object(map),
            }
        }
        FileReadData::Media(MediaPayload::Descriptor {
            kind,
            path,
            mime_type,
            bytes,
            source_bytes,
            attachable,
            note,
        }) => {
            let descriptor_only = policy.media_mode == MediaReadMode::DescriptorOnly;
            let flag_key = if descriptor_only {
                "attachable"
            } else {
                "attached"
            };
            let mut map = serde_json::Map::new();
            map.insert("ok".into(), json!(true));
            map.insert("kind".into(), json!(kind.as_str()));
            map.insert("path".into(), json!(path));
            map.insert("mimeType".into(), json!(mime_type));
            if let Some(size) = bytes {
                map.insert("bytes".into(), json!(size));
            }
            map.insert(flag_key.into(), json!(attachable));
            if let Some(size) = source_bytes {
                map.insert("sourceBytes".into(), json!(size));
            }
            let summary = match note {
                Some(note) => {
                    map.insert("note".into(), json!(note.clone()));
                    note
                }
                None => format!(
                    "Read {} file [{}] ({} B)",
                    kind.as_str(),
                    mime_type,
                    bytes.unwrap_or(0)
                ),
            };
            Outcome {
                owner,
                notice: summary.clone(),
                text: summary,
                body: None,
                attachment: None,
                lease: None,
                structured: Value::Object(map),
            }
        }
    }
}

pub(super) fn text_slice_to_outcome(slice: TextSlice, owner: crate::lease::Owner) -> Outcome {
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
    let text = if slice.line_too_long || slice.text.is_empty() {
        notice_block.clone()
    } else if notice_block.is_empty() {
        slice.text.clone()
    } else if slice.text.ends_with('\n') {
        format!("{}\n{notice_block}", slice.text)
    } else {
        format!("{}\n\n{notice_block}", slice.text)
    };
    let notice = if notice_block.is_empty() {
        format!("[read] {} · {} lines", slice.path, slice.shown_lines)
    } else {
        notice_block
    };
    let structured = json!({
        "ok": true,
        "kind": "text",
        "path": slice.path,
        "startLine": slice.start_line,
        "endLine": slice.end_line,
        "shownLines": slice.shown_lines,
        "totalLines": slice.total_lines,
        "truncated": slice.truncated,
        "nextOffset": slice.next_offset,
        "lineTooLong": slice.line_too_long,
        "lossy": slice.lossy,
    });
    Outcome {
        owner,
        text,
        notice,
        body: Some(slice.text),
        attachment: None,
        lease: Some(slice.lease),
        structured,
    }
}

/// What a read answers with: its text, and its media block when a media file is attached.
pub struct Outcome {
    pub text: String,
    pub notice: String,
    pub body: Option<String>,
    pub attachment: Option<ContentBlock>,
    pub structured: Value,
    pub lease: Option<Lease>,
    pub(super) owner: crate::lease::Owner,
}

impl Outcome {
    fn into_tracked(self, render: Render) -> TrackedResult {
        let mut structured = self.structured;
        if render.output_mode != OutputMode::Text
            && let Some(body) = self.body
        {
            structured["text"] = Value::String(body);
        }
        let shown = if render.output_mode == OutputMode::Structured && self.attachment.is_none() {
            self.notice
        } else {
            self.text
        };
        let mut blocks = vec![ContentBlock::text(shown)];
        if let Some(attachment) = self.attachment {
            blocks.push(attachment);
        }
        let mut result = CallToolResult::success(blocks);
        result.structured_content = Some(structured);
        TrackedResult {
            result: service_render::suppress(result, render),
            lease: self.lease,
            overload: false,
            task_meta: None,
            payload_owner: self.owner,
        }
    }
}

/// Writes one file, creating its parent directories.
pub async fn write_tracked(
    request: WriteRequest,
    launch: Arc<LaunchContext>,
    render: Render,
    input_owner: crate::lease::Owner,
) -> TrackedResult {
    write_with_permit(request, launch, render, None, input_owner).await
}

pub(crate) async fn write_nested(
    request: WriteRequest,
    launch: Arc<LaunchContext>,
    render: Render,
    permit: tokio::sync::OwnedSemaphorePermit,
    input_owner: crate::lease::Owner,
) -> TrackedResult {
    write_with_permit(request, launch, render, Some(permit), input_owner).await
}

async fn write_with_permit(
    request: WriteRequest,
    launch: Arc<LaunchContext>,
    render: Render,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    input_owner: crate::lease::Owner,
) -> TrackedResult {
    let effective = render.effective();
    let outcome = write_core(request, launch, permit, input_owner).await;
    let owner = outcome.owner.clone();
    let (result, overload) = outcome.run(|res| match res {
        Ok(FileWriteData { path, bytes }) => {
            let text = format!("Successfully wrote to {path}");
            let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
            result.structured_content = Some(json!({ "ok": true, "path": path, "bytes": bytes }));
            (result, false)
        }
        Err(FileError::Overload(message)) => (failed(message), true),
        Err(err) => (failed(err.into_message()), false),
    });
    TrackedResult {
        overload,
        ..TrackedResult::owned(service_render::suppress(result, effective), owner)
    }
}

/// Applies targeted replacements to one file.
pub async fn edit_tracked(
    request: EditRequest,
    launch: Arc<LaunchContext>,
    render: Render,
    input_owner: crate::lease::Owner,
) -> TrackedResult {
    edit_with_permit(request, launch, render, None, input_owner).await
}

pub(crate) async fn edit_nested(
    request: EditRequest,
    launch: Arc<LaunchContext>,
    render: Render,
    permit: tokio::sync::OwnedSemaphorePermit,
    input_owner: crate::lease::Owner,
) -> TrackedResult {
    edit_with_permit(request, launch, render, Some(permit), input_owner).await
}

async fn edit_with_permit(
    request: EditRequest,
    launch: Arc<LaunchContext>,
    render: Render,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    input_owner: crate::lease::Owner,
) -> TrackedResult {
    let effective = render.effective();
    let outcome = edit_core(request, launch, permit, input_owner).await;
    let owner = outcome.owner.clone();
    let (result, overload) = outcome.run(|res| match res {
        Ok(data) => {
            let (text, structured) = edit_data_to_agent(data);
            let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
            result.structured_content = Some(structured);
            (result, false)
        }
        Err(EditError {
            error: FileError::Overload(message),
            ..
        }) => (failed(message), true),
        Err(err) => (edit_error_to_agent(err), false),
    });
    TrackedResult {
        overload,
        ..TrackedResult::owned(service_render::suppress(result, effective), owner)
    }
}

pub(super) fn edit_data_to_agent(data: FileEditData) -> (String, Value) {
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
    let structured = json!({
        "ok": true,
        "path": data.path,
        "replacements": data.replacements,
        "edits": data.edits,
    });
    (text, structured)
}

fn edit_error_to_agent(err: EditError) -> CallToolResult {
    match err.failures {
        Some(failures) => {
            let message = err.error.into_message();
            let structured = json!({
                "ok": false,
                "error": message,
                "path": err.path.unwrap_or_default(),
                "failures": failures,
            });
            let mut result = CallToolResult::error(vec![ContentBlock::text(message)]);
            result.structured_content = Some(structured);
            result
        }
        None => failed(err.error.into_message()),
    }
}

#[cfg(test)]
pub(super) type BlockingOutcome = Result<(String, Value), (String, Option<Value>)>;

#[cfg(test)]
pub(super) async fn write(
    request: WriteRequest,
    launch: Arc<LaunchContext>,
    render: Render,
    input_owner: crate::lease::Owner,
) -> CallToolResult {
    write_tracked(request, launch, render, input_owner)
        .await
        .result
}

pub(super) fn failed(message: String) -> CallToolResult {
    let mut result = CallToolResult::error(vec![ContentBlock::text(message.clone())]);
    result.structured_content = Some(json!({ "ok": false, "error": message }));
    result
}

fn schema_failed(message: String) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(message)])
}
