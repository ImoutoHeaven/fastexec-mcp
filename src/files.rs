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

use self::edit::edit_owned_outcome;
use self::path::with_path_workspace;
use self::read::{ReadFailure, ReadLimits, read_cancellable};
use self::write::write_text_reserved;
use crate::lease::Lease;
use crate::service::render::{self as service_render, OutputMode, Render};
use crate::session::LaunchContext;
use rmcp::model::{CallToolResult, ContentBlock};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex, MutexGuard};

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
    read_with_limits_and_permit(
        crate::ipc::Owned::new(request, input_owner),
        launch,
        transfer,
        render,
        ReadLimits::DIRECT,
        None,
        cancel,
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
    read_with_limits_and_permit(
        crate::ipc::Owned::new(request, input_owner),
        launch,
        transfer,
        render,
        ReadLimits::NESTED,
        Some(permit),
        Some(cancel),
    )
    .await
}

async fn read_with_limits_and_permit(
    input: crate::ipc::Owned<ReadRequest>,
    launch: Arc<LaunchContext>,
    transfer: bool,
    render: Render,
    limits: ReadLimits,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    cancel: Option<tokio_util::sync::CancellationToken>,
) -> TrackedResult {
    let effective = render.effective();
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        input.flat_map(|request| {
            read_cancellable(&request, &launch, transfer, limits, cancel.as_ref())
        })
    })
    .await
    {
        Ok(outcome) => {
            let owner = outcome.owner.clone();
            let mut tracked = outcome.run(|outcome| match outcome {
                Ok(outcome) => outcome.into_tracked(effective),
                Err(ReadFailure::Overload(message)) => TrackedResult {
                    result: service_render::suppress(failed(message), effective),
                    lease: None,
                    overload: true,
                    task_meta: None,
                    payload_owner: Default::default(),
                },
                Err(ReadFailure::Error(message)) => {
                    TrackedResult::plain(service_render::suppress(failed(message), effective))
                }
            });
            tracked.payload_owner = owner;
            tracked
        }
        Err(error) => TrackedResult::plain(service_render::suppress(
            failed(format!("the file operation did not finish: {error}")),
            effective,
        )),
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
    let bytes = request.content.len();
    let input = crate::ipc::Owned::new((request, permit), input_owner);
    let outcome = tokio::task::spawn_blocking(move || {
        input.flat_map(|(request, _permit)| {
            with_path_workspace(&request.path, &launch, 0, |lease| {
                write_text_reserved(&request, &launch, lease)
                    .map(|(text, path)| (text, json!({ "ok": true, "path": path, "bytes": bytes })))
                    .map_err(|msg| (msg, None))
            })
            .unwrap_or_else(|message| {
                crate::ipc::Owned::new(Err((message, None)), Default::default())
            })
        })
    })
    .await;
    let mut tracked = finished_owned(outcome);
    tracked.result = service_render::suppress(tracked.result, effective);
    tracked
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
    let input = crate::ipc::Owned::new((request, permit), input_owner);
    let mut tracked = finished_owned(
        tokio::task::spawn_blocking(move || {
            input.flat_map(|(request, _permit)| edit_owned_outcome(&request, &launch))
        })
        .await,
    );
    tracked.result = service_render::suppress(tracked.result, effective);
    tracked
}

pub(super) type BlockingOutcome = Result<(String, Value), (String, Option<Value>)>;

fn finished_owned(
    outcome: Result<crate::ipc::Owned<BlockingOutcome>, tokio::task::JoinError>,
) -> TrackedResult {
    match outcome {
        Ok(outcome) => {
            let owner = outcome.owner.clone();
            let result = outcome.run(|outcome| finished(Ok(outcome)));
            TrackedResult::owned(result, owner)
        }
        Err(error) => TrackedResult::plain(finished(Err(error))),
    }
}

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

/// A blocking operation's answer: its text and structured metadata, or its message as an error.
pub(super) fn finished(outcome: Result<BlockingOutcome, tokio::task::JoinError>) -> CallToolResult {
    match outcome {
        Ok(Ok((text, structured))) => {
            let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
            result.structured_content = Some(structured);
            result
        }
        Ok(Err((message, Some(structured)))) => {
            let mut result = CallToolResult::error(vec![ContentBlock::text(message)]);
            result.structured_content = Some(structured);
            result
        }
        Ok(Err((message, None))) => failed(message),
        Err(error) => failed(format!("the file operation did not finish: {error}")),
    }
}

pub(super) fn failed(message: String) -> CallToolResult {
    let mut result = CallToolResult::error(vec![ContentBlock::text(message.clone())]);
    result.structured_content = Some(json!({ "ok": false, "error": message }));
    result
}

fn schema_failed(message: String) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(message)])
}
