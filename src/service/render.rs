//! Result rendering, payload budgeting, attachment packing, and error formatting.

use super::schema::{Action, View, action_name};
use super::{Call, Service, ServiceError, StagedRead, task_read_error};
use crate::lease::Lease;
use crate::platform::{conpty, process};
use crate::session::{ProbeWarning, ReadState, Reader, Session, TaskEntry, parse_handle};
use crate::tasks::state::{Snapshot, Task};
use crate::tasks::{self, EVICTED_TRANSCRIPT, Tasks};
use crate::term::output::{self, Cleaner, Truncate, Window, WindowBuilder, WindowSpec};
use crate::wait::WaitEnd;
use crate::{codemode, files, media};
use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::{Value, json};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};

/// Screen reads only need stream decoding diagnostics and the updated cursor.
const MIN_BODY_BUDGET: usize = 256;
const NEW_READER_NOTE: &str = " · new reader";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputMode {
    Text,
    Both,
    Structured,
}

impl OutputMode {
    /// The name the wire carries.
    pub fn name(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Both => "both",
            Self::Structured => "structured",
        }
    }

    /// The mode a wire name stands for. Unknown names return an error on handshake.
    pub fn parse(name: &str) -> Result<Self, String> {
        match name {
            "text" => Ok(Self::Text),
            "both" => Ok(Self::Both),
            "structured" => Ok(Self::Structured),
            other => Err(format!("unknown output mode {other:?}")),
        }
    }
}

/// How a result is rendered, carried by each call: the daemon takes it from the caller's
/// handshake.
#[derive(Clone, Copy)]
pub struct Render {
    pub output_mode: OutputMode,
    pub structured_content: bool,
}

impl Render {
    /// Structured content off also means text only, so a caller that asks for structured output
    /// without structured content still gets the output body.
    pub(crate) fn effective(self) -> Self {
        Self {
            output_mode: if self.structured_content {
                self.output_mode
            } else {
                OutputMode::Text
            },
            structured_content: self.structured_content,
        }
    }
}

/// Strips structured content when the caller turned it off.
pub fn suppress(mut result: CallToolResult, render: Render) -> CallToolResult {
    if !render.structured_content {
        result.structured_content = None;
    }
    result
}

/// A local daemon call obtains one render slot on demand and keeps it until encoding.
pub(crate) struct RenderSlot<'a> {
    pool: &'a Arc<Semaphore>,
    permit: OnceCell<Arc<OwnedSemaphorePermit>>,
}

impl<'a> RenderSlot<'a> {
    pub(crate) fn new(pool: &'a Arc<Semaphore>) -> Self {
        Self {
            pool,
            permit: OnceCell::new(),
        }
    }

    pub(super) fn try_hold(&self) -> Option<Arc<OwnedSemaphorePermit>> {
        if let Some(permit) = self.permit.get() {
            return Some(Arc::clone(permit));
        }
        let permit = Arc::new(Arc::clone(self.pool).try_acquire_owned().ok()?);
        self.permit
            .set(Arc::clone(&permit))
            .expect("one renderer per call");
        Some(permit)
    }

    pub(crate) async fn hold(&self) -> Arc<OwnedSemaphorePermit> {
        Arc::clone(
            self.permit
                .get_or_init(|| async {
                    Arc::new(
                        Arc::clone(self.pool)
                            .acquire_owned()
                            .await
                            .expect("render pool never closes"),
                    )
                })
                .await,
        )
    }
}

pub(super) fn text_workspace_bytes(bytes: usize) -> usize {
    bytes.saturating_mul(16).saturating_add(128 << 10)
}

fn value_payload_capacity(value: &Value) -> usize {
    match value {
        Value::String(text) => text.capacity(),
        Value::Array(items) => items
            .iter()
            .map(value_payload_capacity)
            .fold(0, usize::saturating_add),
        Value::Object(fields) => fields
            .iter()
            .map(|(key, value)| key.capacity().saturating_add(value_payload_capacity(value)))
            .fold(0, usize::saturating_add),
        _ => 0,
    }
}

fn meta_payload_capacity(meta: &Option<rmcp::model::MetaObject>) -> usize {
    meta.as_ref().map_or(0, |meta| {
        meta.0
            .iter()
            .map(|(key, value)| key.capacity().saturating_add(value_payload_capacity(value)))
            .fold(0, usize::saturating_add)
    })
}

fn annotation_payload_capacity(annotations: &Option<rmcp::model::Annotations>) -> usize {
    annotations
        .as_ref()
        .and_then(|annotations| annotations.last_modified.as_ref())
        .map_or(0, String::capacity)
}

pub(crate) fn result_payload_capacity(result: &CallToolResult) -> usize {
    let content = result
        .content
        .iter()
        .map(|block| {
            let (payload, meta, annotations) = match block {
                ContentBlock::Text(text) => (text.text.capacity(), &text.meta, &text.annotations),
                ContentBlock::Image(img) => (
                    img.data.capacity().saturating_add(img.mime_type.capacity()),
                    &img.meta,
                    &img.annotations,
                ),
                ContentBlock::Audio(aud) => (
                    aud.data.capacity().saturating_add(aud.mime_type.capacity()),
                    &aud.meta,
                    &aud.annotations,
                ),
                ContentBlock::Resource(res) => {
                    let payload = match &res.resource {
                        rmcp::model::ResourceContents::BlobResourceContents {
                            blob: body,
                            uri,
                            mime_type,
                            meta,
                        }
                        | rmcp::model::ResourceContents::TextResourceContents {
                            text: body,
                            uri,
                            mime_type,
                            meta,
                        } => body
                            .capacity()
                            .saturating_add(uri.capacity())
                            .saturating_add(mime_type.as_ref().map_or(0, String::capacity))
                            .saturating_add(meta_payload_capacity(meta)),
                        _ => 0,
                    };
                    (payload, &res.meta, &res.annotations)
                }
                ContentBlock::ResourceLink(res) => {
                    let icons = res.icons.as_ref().map_or(0, |icons| {
                        icons
                            .iter()
                            .map(|icon| {
                                icon.src
                                    .capacity()
                                    .saturating_add(
                                        icon.mime_type.as_ref().map_or(0, String::capacity),
                                    )
                                    .saturating_add(icon.sizes.as_ref().map_or(0, |sizes| {
                                        sizes
                                            .iter()
                                            .map(String::capacity)
                                            .fold(0, usize::saturating_add)
                                    }))
                            })
                            .fold(0, usize::saturating_add)
                    });
                    let payload = res
                        .uri
                        .capacity()
                        .saturating_add(res.name.capacity())
                        .saturating_add(res.title.as_ref().map_or(0, String::capacity))
                        .saturating_add(res.description.as_ref().map_or(0, String::capacity))
                        .saturating_add(res.mime_type.as_ref().map_or(0, String::capacity))
                        .saturating_add(icons);
                    (payload, &res.meta, &res.annotations)
                }
                _ => return 0,
            };
            payload
                .saturating_add(meta_payload_capacity(meta))
                .saturating_add(annotation_payload_capacity(annotations))
        })
        .fold(0, usize::saturating_add);
    content
        .saturating_add(
            result
                .structured_content
                .as_ref()
                .map_or(0, value_payload_capacity),
        )
        .saturating_add(meta_payload_capacity(&result.meta))
}

pub(crate) fn unleased_result_payload_capacity(
    result: &CallToolResult,
    owner: &crate::lease::Owner,
) -> usize {
    result_payload_capacity(result).saturating_sub(owner.get::<Lease>().map_or(0, Lease::bytes))
}

fn retain_text_result(
    result: CallToolResult,
    mut workspace: Lease,
    outgoing: &mut Option<Lease>,
    call: &Call<'_>,
    task: &Task,
) -> Result<(CallToolResult, crate::lease::Owner), ServiceError> {
    let text_bytes = result
        .content
        .iter()
        .map(|block| block.as_text().map_or(0, |text| text.text.capacity()))
        .fold(0, usize::saturating_add)
        .saturating_add(
            result
                .structured_content
                .as_ref()
                .map_or(0, value_payload_capacity),
        );
    let owner = if let Some(outgoing) = outgoing.as_mut().filter(|held| held.bytes() >= text_bytes)
    {
        outgoing.split_off(text_bytes)
    } else if workspace.try_resize(text_bytes) {
        workspace
    } else {
        drop(result);
        drop(workspace);
        let meta = files::TaskMeta {
            bare_task_id: task.id.clone(),
            log_path: files::path::display_path(&task.log_path),
        };
        return Err(ServiceError::Overload(
            overload_task_message(call.launch().memory().limit(), &meta),
            Some(meta),
        ));
    };
    Ok((result, crate::lease::Owner::new(owner)))
}

#[derive(serde::Serialize)]
struct WireResultRef<'a> {
    r#type: &'static str,
    id: u64,
    result: &'a CallToolResult,
}

pub(super) fn wire_result_len(
    call_id: Option<u64>,
    result: &CallToolResult,
) -> Result<usize, String> {
    crate::ipc::frame_len(&WireResultRef {
        r#type: "result",
        id: call_id.unwrap_or(0),
        result,
    })
    .map_err(|err| format!("Cannot measure result frame: {err}."))
}

pub(crate) fn overload_task_message(limit: usize, meta: &files::TaskMeta) -> String {
    format!(
        "Server active memory budget ({limit} bytes) is full while encoding the response; the operation ran (taskId: {}, logPath: {}). Do not retry start or resend input; poll taskId without input once in-flight transfers complete.",
        meta.bare_task_id, meta.log_path
    )
}

pub(super) fn nested_wire_bytes(result: &CallToolResult) -> Result<usize, String> {
    // The moved envelope adds at most 64 encoded bytes, even for the fixed
    // missing-content fallback. 128 bytes also cover its new keys/text. Keep
    // one conservative bound for both that workspace and the encoded String.
    let bytes = crate::ipc::json_len(result)
        .ok()
        .and_then(|bytes| bytes.checked_add(256))
        .ok_or_else(|| "Cannot measure the nested result.".to_owned())?;
    if bytes > codemode::bridge::BRIDGE_LIMIT {
        return Err("The nested result exceeds the 256 MiB bridge limit.".to_owned());
    }
    Ok(bytes)
}

pub(super) fn reserve_nested_bridge(call: &Call<'_>, bytes: usize) -> Result<(), String> {
    if call.cancel.is_cancelled() {
        return Err("Cancelled before nested output was committed.".to_owned());
    }
    if let Some(nested) = call.started
        && !nested.script.reserve_result(nested.pending_id, bytes)
    {
        return Err(
            "The nested result does not fit in the script's remaining bridge quota.".to_owned(),
        );
    }
    Ok(())
}

pub(super) fn admit_nested_result(
    call: &Call<'_>,
    action: Action,
    result: &CallToolResult,
    held: &mut Option<Lease>,
) -> Result<Lease, String> {
    let wire = nested_wire_bytes(result)?;
    let peak = result_payload_capacity(result)
        .checked_add(wire)
        .ok_or_else(|| "The nested result is too large.".to_owned())?;
    let old_bytes = held.as_ref().map_or(0, Lease::bytes);
    let reused = held
        .as_mut()
        .is_some_and(|lease| lease.try_grow(peak.saturating_sub(lease.bytes())));
    let replacement = if reused {
        None
    } else {
        Some(
            call.launch()
                .memory()
                .try_acquire(peak)
                .or_else(|| {
                    (matches!(action, Action::List | Action::Kill)
                        && peak <= crate::lease::CONTROL_LIMIT)
                        .then(|| call.launch().control_memory().try_acquire(peak))
                        .flatten()
                })
                .ok_or_else(|| {
                    "Server active memory budget is full while reserving the nested result."
                        .to_owned()
                })?,
        )
    };
    if let Err(error) = reserve_nested_bridge(call, wire) {
        if reused {
            held.as_mut().unwrap().shrink_to(old_bytes);
        }
        return Err(error);
    }
    if reused {
        Ok(held.take().unwrap())
    } else {
        // The replacement and script quota are admitted before releasing old storage.
        drop(held.take());
        Ok(replacement.unwrap())
    }
}

pub(super) fn admit_outbound(
    call: &Call<'_>,
    action: Action,
    task: &Task,
    suppressed: &CallToolResult,
    held_lease: &mut Option<Lease>,
) -> Result<Option<Lease>, ServiceError> {
    let meta = || files::TaskMeta {
        bare_task_id: task.id.clone(),
        log_path: files::path::display_path(&task.log_path),
    };
    if call.nested {
        return admit_nested_result(call, action, suppressed, held_lease)
            .map(Some)
            .map_err(|reason| {
                let meta = meta();
                ServiceError::Overload(format!(
                    "{reason} Output was not committed; the operation may have run (taskId: {}, logPath: {}). Do not retry start or resend input; poll taskId without input.",
                    meta.bare_task_id, meta.log_path
                ), Some(meta))
            });
    }
    let pool = call.launch().memory();
    let outbound_limit = call.launch().outbound_limit();
    let frame_len = measure_outbound_len(call, suppressed)?;
    if frame_len > outbound_limit {
        let m = meta();
        return Err(ServiceError::Overload(
            format!(
                "The result was {frame_len} bytes, over the 512 MiB frame limit; the operation ran (taskId: {}, logPath: {}). Do not retry start or resend input; poll taskId with a smaller maxBytes or read logPath.",
                m.bare_task_id, m.log_path
            ),
            Some(m),
        ));
    }
    let b64_bytes = result_payload_capacity(suppressed);
    if call.render_slot.is_some() {
        let wire_len = wire_result_len(call.call_id, suppressed)?;
        if action == Action::Kill || action == Action::List {
            return Ok(held_lease.take());
        }
        let Some(peak) = b64_bytes.checked_add(wire_len) else {
            let m = meta();
            return Err(ServiceError::Overload(
                overload_task_message(pool.limit(), &m),
                Some(m),
            ));
        };
        let admitted = match held_lease.as_mut() {
            Some(lease) => lease.try_resize(peak).then(|| held_lease.take()).flatten(),
            None => pool.try_acquire(peak),
        };
        match admitted {
            Some(lease) => Ok(Some(lease)),
            None => {
                let m = meta();
                Err(ServiceError::Overload(
                    overload_task_message(pool.limit(), &m),
                    Some(m),
                ))
            }
        }
    } else {
        let outbound = frame_len;
        let Some(peak) = outbound
            .checked_mul(if call.http_response { 0 } else { 1 })
            .and_then(|wire| b64_bytes.checked_add(wire))
        else {
            let m = meta();
            return Err(ServiceError::Overload(
                overload_task_message(pool.limit(), &m),
                Some(m),
            ));
        };
        let admitted = match held_lease.as_mut() {
            Some(lease) => lease.try_resize(peak).then(|| held_lease.take()).flatten(),
            None => pool.try_acquire(peak).or_else(|| {
                (action == Action::Kill && peak <= crate::lease::CONTROL_LIMIT)
                    .then(|| call.launch().control_memory().try_acquire(peak))
                    .flatten()
            }),
        };
        match admitted {
            Some(lease) => Ok(Some(lease)),
            None => {
                let m = meta();
                Err(ServiceError::Overload(
                    overload_task_message(pool.limit(), &m),
                    Some(m),
                ))
            }
        }
    }
}

/// Snapshot strings can coexist with warning/status formatting, cloned JSON fields,
/// the joined body and its growing suffix. Thirty-two times their source capacity,
/// including per-member/diagnostic keys, bounds those copies and geometric capacities.
pub(super) fn metadata_workspace_bytes(bytes: usize) -> usize {
    bytes.saturating_mul(32)
}

pub(super) fn resize_metadata_workspace(
    workspace: &mut Lease,
    target: usize,
    call: &Call<'_>,
    action: Action,
) -> Result<(), String> {
    if workspace.try_resize(target) {
        return Ok(());
    }
    let launch = call.launch();
    let replacement = launch
        .memory()
        .try_acquire(target)
        .or_else(|| {
            matches!(action, Action::List | Action::Kill)
                .then(|| launch.control_memory().try_acquire(target))
                .flatten()
        })
        .ok_or_else(|| {
            format!(
                "Server active memory budget ({} bytes) is full while preparing task metadata.",
                launch.memory().limit()
            )
        })?;
    // Acquire the replacement before releasing the old pool's charge.
    *workspace = replacement;
    Ok(())
}

pub(super) fn metadata_members(
    tree: &process::Tree,
    workspace: &mut Lease,
    call: &Call<'_>,
    action: Action,
) -> Result<Option<Vec<process::Member>>, String> {
    let retained = workspace.bytes();
    let (members, refused) = {
        let mut reserve = |bytes| {
            resize_metadata_workspace(workspace, retained.saturating_add(bytes), call, action)
                .is_ok()
        };
        let mut budget = process::MemberBudget::new(&mut reserve);
        let members = tree.members_with_budget(&mut budget);
        (members, budget.refused())
    };
    if refused {
        drop(members);
        workspace.shrink_to(retained);
        return Err("Server active memory budget is full while reading task process names.".into());
    }
    let text = members.as_ref().map_or(0, |members| {
        members
            .iter()
            .map(|member| member.name.capacity().saturating_add(32))
            .fold(0, usize::saturating_add)
    });
    // The native query's temporary scan/conversion strings have gone. Keep only
    // the selected names and their later status/JSON rendering workspace.
    if let Err(error) = resize_metadata_workspace(
        workspace,
        retained.saturating_add(metadata_workspace_bytes(text)),
        call,
        action,
    ) {
        drop(members);
        workspace.shrink_to(retained);
        return Err(error);
    }
    Ok(members)
}

// Footer text and shown snapshots stay owned until their report marks are committed.
pub(super) struct Background<'a> {
    pub(super) value: Option<(String, Vec<tasks::Shown>)>,
    pub(super) claim: crate::session::OutsideClaim<'a>,
    pub(super) external_cursor: Option<u64>,
    pub(super) event_bus: Option<Arc<crate::events::EventBus>>,
    pub(super) _workspace: Lease,
}

// Snapshot text drops before the workspace, including on cancellation across an await.
pub(super) struct RenderSnapshot {
    pub(super) value: Snapshot,
    pub(super) workspace: Lease,
}

impl RenderSnapshot {
    pub(super) fn overload(call: &Call<'_>, task: &Task) -> ServiceError {
        let meta = files::TaskMeta {
            bare_task_id: task.id.clone(),
            log_path: files::path::display_path(&task.log_path),
        };
        ServiceError::Overload(
            overload_task_message(call.launch().memory().limit(), &meta),
            Some(meta),
        )
    }

    pub(super) fn new(task: &Task, call: &Call<'_>, action: Action) -> Result<Self, ServiceError> {
        let workspace = std::cell::RefCell::new(Lease::empty(call.launch().memory()));
        let fixed = task
            .id
            .capacity()
            .saturating_add(task.log_path.capacity().saturating_mul(3))
            .saturating_add(512);
        resize_metadata_workspace(
            &mut workspace.borrow_mut(),
            metadata_workspace_bytes(fixed),
            call,
            action,
        )
        .map_err(|_| Self::overload(call, task))?;
        let value = task
            .snapshot_held_reserved(
                &mut |bytes| {
                    let mut held = workspace.borrow_mut();
                    let target = held.bytes().saturating_add(metadata_workspace_bytes(bytes));
                    resize_metadata_workspace(&mut held, target, call, action)
                },
                &mut |tree| metadata_members(tree, &mut workspace.borrow_mut(), call, action),
            )
            .map_err(|_| Self::overload(call, task))?;
        Ok(Self {
            value,
            workspace: workspace.into_inner(),
        })
    }

    pub(super) fn refresh(
        &mut self,
        task: &Task,
        call: &Call<'_>,
        action: Action,
    ) -> Result<(), ServiceError> {
        let workspace = &mut self.workspace;
        task.refresh_snapshot_output(&mut self.value, &mut |bytes| {
            let target = workspace
                .bytes()
                .saturating_add(metadata_workspace_bytes(bytes));
            resize_metadata_workspace(workspace, target, call, action)
        })
        .map_err(|_| Self::overload(call, task))
    }
}

/// Marks what the result showed of the task and of the footer's tasks reported, once the
/// result is built.
#[allow(clippy::too_many_arguments)]
pub(super) fn report(
    session: &Session,
    entry: &TaskEntry,
    reader: &Reader,
    shown: &Snapshot,
    mut background: Background<'_>,
    nested: bool,
    owned_by_script: bool,
) {
    reader.mark_reported(shown);
    if nested {
        if owned_by_script {
            entry.mark_reported(shown);
        }
        return;
    }
    entry.mark_reported(shown);
    for (task, shown) in background
        .value
        .take()
        .map(|(_, shown)| shown)
        .unwrap_or_default()
    {
        if let Some(entry) = session.existing(&task) {
            entry.mark_reported(&shown);
            entry.mark_keeps_reported(&shown);
        }
    }
    background.claim.consume();
    if let (Some(bus), Some(seq)) = (background.event_bus, background.external_cursor) {
        bus.commit_external_background_cursor(&session.id, seq);
    }
}

fn kill_pending_note(task: &Task, snapshot: &Snapshot) -> Option<&'static str> {
    if snapshot.state == "running" || snapshot.kept || !*task.tree_done().borrow() {
        Some(if task.is_script() {
            "the script did not end within 5 s; poll later to confirm"
        } else {
            "the tree did not exit within 5 s; poll later to confirm"
        })
    } else if snapshot.output_open {
        Some("output did not close within 5 s; poll later to confirm")
    } else {
        None
    }
}

fn needs_log_in_status(snapshot: &Snapshot, cut_lines: u64, omitted: Option<(u64, u64)>) -> bool {
    snapshot.state == "running"
        || snapshot.kept
        || snapshot.output_open
        || output_incomplete(snapshot)
        || cut_lines > 0
        || omitted.is_some()
}

impl Service {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn window_result(
        &self,
        call: &Call<'_>,
        session: &Session,
        action: Action,
        task: Arc<Task>,
        entry: Arc<TaskEntry>,
        task_reader: Arc<Reader>,
        state: tokio::sync::OwnedMutexGuard<ReadState>,
        view: View,
        ended: Option<WaitEnd>,
        startup_warning: Option<ProbeWarning>,
    ) -> Result<
        (
            CallToolResult,
            Option<Lease>,
            Option<files::TaskMeta>,
            crate::lease::Owner,
        ),
        ServiceError,
    > {
        let permit = call.task_render_permit(action, &task, &task_reader).await?;
        let render = call.render;
        if view.screen {
            return self
                .screen_result(
                    call,
                    render,
                    session,
                    action,
                    task,
                    entry,
                    task_reader,
                    state,
                    view,
                    ended.expect("screen applies to start and poll"),
                    startup_warning,
                    permit,
                )
                .await;
        }
        let reader = Arc::clone(&task);
        let pool = Arc::clone(call.launch().memory());
        let outbound_limit = call.launch().outbound_limit();
        let requested_max_bytes = view.max_bytes;
        let mut view = view;
        let workspace = pool
            .try_acquire(text_workspace_bytes(view.max_bytes))
            .or_else(|| {
                if action != Action::Kill {
                    return None;
                }
                view.max_bytes = view.max_bytes.min(64 << 10);
                call.launch()
                    .control_memory()
                    .try_acquire(text_workspace_bytes(view.max_bytes))
            })
            .ok_or_else(|| {
                let meta = files::TaskMeta {
                    bare_task_id: task.id.clone(),
                    log_path: files::path::display_path(&task.log_path),
                };
                ServiceError::Overload(overload_task_message(pool.limit(), &meta), Some(meta))
            })?;
        let encoding = view.encoding.unwrap_or(task.encoding);
        // The state is taken before the read: a finished state means output reached EOF (or
        // stayed open, which the status line notes), so the window below holds all of it.
        let mut metadata = RenderSnapshot::new(&task, call, action)?;
        let clean = !task.is_script();
        let keep_nul = view.nested;
        let budget = view.max_bytes;
        let initial_seen = state.attachments_seen;
        let (window, unseen, had_new_text, mut read, mut permit, mut workspace) =
            tokio::task::spawn_blocking(move || {
                let workspace = workspace;
                let mut read = StagedRead::new(state);
                let start = read.state.cursor;
                let window = reader.read_window(
                    &mut read.state,
                    view.truncate,
                    budget,
                    view.raw,
                    clean,
                    keep_nul,
                    encoding,
                    None,
                );
                let had_new_text = read.state.cursor > start;
                let unseen = if window.is_ok() {
                    reader.unseen_attachments(initial_seen)
                } else {
                    Vec::new()
                };
                (window, unseen, had_new_text, read, permit, workspace)
            })
            .await
            .map_err(|error| format!("Internal failure while reading output: {error}."))?;
        let mut window_val =
            window.map_err(|error| format!("Cannot read the task log: {error}."))?;
        // Log metadata, such as an eviction during the read, is taken after it.
        metadata.refresh(&task, call, action)?;
        let snapshot = &metadata.value;
        let delivered_warning = startup_warning.and_then(ProbeWarning::deliver);
        let mut extra_kill_warnings: Vec<String> = Vec::new();
        if view.max_bytes < requested_max_bytes {
            extra_kill_warnings.push(
                "kill output window narrowed to fit the 8 MiB control reserve; poll taskId or read logPath for remaining output".into(),
            );
        }
        let script = script_report(&self.tasks, &task, call, action)?;
        let build_candidate = |window_ref: &Window,
                               bundle_ref: &AttachmentBundle,
                               extra_warns: &[String],
                               footer: &str,
                               max_bytes: usize,
                               content_blocks: Vec<ContentBlock>|
         -> CallToolResult {
            let mut status_with_log =
                status_line(&task, snapshot, &task_reader, true, script.as_ref());
            let mut status_no_log =
                status_line(&task, snapshot, &task_reader, false, script.as_ref());
            let mut structured = task_json(action, &task, snapshot, &task_reader);
            if let Some(ended) = &ended {
                let note = ended.note(view.conditions);
                status_with_log.push_str(&note);
                status_no_log.push_str(&note);
                structured["waitEndedBy"] = ended.json();
            } else if let Some(note) = kill_pending_note(&task, snapshot) {
                structured["warnings"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!(note));
            }
            let mut delivered_json = Vec::new();
            let mut attached_summary = Vec::new();
            let mut step_warnings = Vec::new();
            for step in &bundle_ref.steps {
                match step {
                    PackStep::Delivered {
                        index,
                        kind,
                        mime,
                        bytes,
                        ..
                    } => {
                        delivered_json.push(json!({
                            "index": index,
                            "kind": kind.as_str(),
                            "mimeType": mime,
                            "bytes": bytes,
                        }));
                        attached_summary.push(format!("#{index} ({mime}, {bytes} B)"));
                    }
                    PackStep::Degraded { warning, .. } => {
                        step_warnings.push(warning.clone());
                    }
                }
            }
            if let Some(report) = &script {
                structured["codemode"] = report.value.clone();
                if !bundle_ref.nested_handles.is_empty() {
                    structured["codemode"]["attachments"] =
                        Value::Array(bundle_ref.nested_handles.clone());
                }
                if !delivered_json.is_empty() {
                    structured["codemode"]["attachmentsDelivered"] = Value::Array(delivered_json);
                }
            }
            structured["warnings"]
                .as_array_mut()
                .unwrap()
                .extend(step_warnings.into_iter().map(Value::String));
            structured["warnings"]
                .as_array_mut()
                .unwrap()
                .extend(bundle_ref.tail_warnings.iter().cloned().map(Value::String));
            structured["warnings"]
                .as_array_mut()
                .unwrap()
                .extend(extra_warns.iter().cloned().map(Value::String));
            if let Some(warning) = &delivered_warning {
                structured["warnings"]
                    .as_array_mut()
                    .unwrap()
                    .push(Value::String(warning.clone()));
            }
            add_encoding_warning(&mut structured, window_ref);
            let warnings = warning_text(&structured["warnings"]);
            let mut suffix = String::new();
            if !render.structured_content && !attached_summary.is_empty() {
                suffix.push_str("\nAttached: ");
                suffix.push_str(&attached_summary.join(", "));
            }
            suffix.push_str(&warnings);
            let text = self.layout(
                render,
                window_ref,
                status_with_log,
                status_no_log,
                snapshot,
                task_reader.is_fresh(),
                footer,
                max_bytes,
                "(no new output)",
                &mut structured,
            );
            let mut res = success(text, structured, &suffix);
            res.content.extend(content_blocks);
            suppress(res, render)
        };

        let mut active_max_bytes = view.max_bytes;

        if call.nested {
            let background = self.background(call, &task, view.nested, action)?;
            let footer = background
                .value
                .as_ref()
                .map_or(String::new(), |(line, _)| format!("\n{line}"));
            let mut bundle = AttachmentBundle::default();
            let mut final_seen = initial_seen;
            let mut registrations = NestedAttachments {
                script: call.started.map(|nested| nested.script),
                ids: Vec::new(),
            };
            if let Some(script) = call.started.map(|nested| nested.script) {
                for att in unseen {
                    let meta = att.meta().clone();
                    let hid = script.register_attachment(att);
                    registrations.ids.push(hid);
                    bundle.nested_handles.push(json!({
                        "index": meta.index,
                        "kind": meta.kind.as_str(),
                        "mimeType": meta.mime,
                        "bytes": meta.size,
                        "__hid": hid,
                    }));
                    final_seen = meta.index;
                }
            }
            let suppressed = build_candidate(
                &window_val,
                &bundle,
                &extra_kill_warnings,
                &footer,
                active_max_bytes,
                Vec::new(),
            );
            drop(window_val);
            let mut lease = admit_outbound(call, action, &task, &suppressed, &mut None)?;
            let (suppressed, payload_owner) =
                retain_text_result(suppressed, workspace, &mut lease, call, &task)?;
            read.state.attachments_seen = final_seen;
            let state = read.commit();
            registrations.ids.clear();
            task_reader.mark_delivered();
            report(
                session,
                &entry,
                &task_reader,
                snapshot,
                background,
                view.nested,
                view.owned_by_script,
            );
            drop(state);
            let meta = files::TaskMeta {
                bare_task_id: task.id.clone(),
                log_path: files::path::display_path(&task.log_path),
            };
            return Ok((suppressed, lease, Some(meta), payload_owner));
        }

        let effective_cap = if action == Action::Kill {
            (crate::daemon::RESERVED - 4).min(outbound_limit)
        } else {
            outbound_limit
        };
        let mut bundle = AttachmentBundle::default();
        let pre_check = build_candidate(
            &window_val,
            &bundle,
            &extra_kill_warnings,
            "",
            active_max_bytes,
            Vec::new(),
        );

        let pre_check_len = measure_outbound_len(call, &pre_check)?;
        drop(pre_check);
        if action == Action::Kill && pre_check_len.saturating_add(516) > effective_cap {
            let state_guard = read.into_guard();
            let reader_narrow = Arc::clone(&task);
            let narrow_budget = view.max_bytes.min(64 * 1024);
            let (narrow_win, narrow_read, narrow_permit, returned_workspace) =
                tokio::task::spawn_blocking(move || {
                    let mut staged = StagedRead::new(state_guard);
                    let win = reader_narrow.read_window(
                        &mut staged.state,
                        view.truncate,
                        narrow_budget,
                        view.raw,
                        clean,
                        keep_nul,
                        encoding,
                        None,
                    );
                    (win, staged, permit, workspace)
                })
                .await
                .map_err(|error| {
                    format!("Internal failure while narrowing kill output: {error}.")
                })?;
            workspace = returned_workspace;
            window_val =
                narrow_win.map_err(|error| format!("Cannot read the task log: {error}."))?;
            read = narrow_read;
            permit = narrow_permit;
            extra_kill_warnings.push(
                "kill output window narrowed to fit the 8 MiB control reserve; poll taskId or read logPath for remaining output".into(),
            );
            active_max_bytes = narrow_budget;
        }

        let mut packed_blocks = Vec::new();
        let total_unseen = unseen.len();
        if total_unseen > 0 {
            let base_candidate = build_candidate(
                &window_val,
                &bundle,
                &extra_kill_warnings,
                "",
                active_max_bytes,
                Vec::new(),
            );
            let base_payload = result_payload_capacity(&base_candidate);
            let raw_base_wire = measure_outbound_len(call, &base_candidate)?;
            let local_base_wire = local_wire_len(call, &base_candidate)?;
            drop(base_candidate);
            let min_envelope_wire = if had_new_text {
                512usize.saturating_add(call.rpc_id_bytes)
            } else {
                raw_base_wire
            };
            let base_wire = raw_base_wire.saturating_add(516);
            let mut running_wire = base_wire;
            let mut total_b64 = 0usize;
            let mut max_raw = 0usize;
            let mut pack_lease = Lease::empty(&pool);
            let mut planned: Vec<PlannedAttachment> = Vec::new();
            let mut stopped_on_memory = false;

            for att in unseen {
                let meta = att.meta();
                let shown_path = files::path::display_path(&meta.path);
                let item_wire_delta = meta
                    .base64_len
                    .saturating_add(shown_path.len().saturating_mul(3))
                    .saturating_add(256);
                let solo_wire = min_envelope_wire.saturating_add(item_wire_delta);

                let at_queue_head = planned
                    .iter()
                    .all(|p| matches!(p, PlannedAttachment::Degraded { .. }));
                if action != Action::Kill && at_queue_head && solo_wire > outbound_limit {
                    let msg = format!(
                        "attachment #{} [{}] ({} B, {} base64 B) exceeds the {} B outbound frame limit and was skipped; inspect or transfer {shown_path}",
                        meta.index, meta.mime, meta.size, meta.base64_len, outbound_limit
                    );
                    let warn_delta = msg.len().saturating_add(64);
                    if running_wire.saturating_add(warn_delta) > effective_cap {
                        break;
                    }
                    running_wire += warn_delta;
                    planned.push(PlannedAttachment::Degraded {
                        index: meta.index,
                        warning: msg,
                    });
                    continue;
                }

                if running_wire.saturating_add(item_wire_delta) > effective_cap {
                    break;
                }

                let raw_len = usize::try_from(meta.size).unwrap_or(usize::MAX);
                let next_b64 = total_b64.checked_add(meta.base64_len);
                let next_max_raw = max_raw.max(raw_len);
                let next_wire = running_wire.saturating_add(item_wire_delta);
                let needed_peak = next_b64.and_then(|b64| {
                    let read_peak = b64.checked_add(next_max_raw)?;
                    // The outer ID constrains packing, but a daemon reserves only
                    // its actual IPC encoding. The endpoint owns its later RPC frame.
                    let local_wire =
                        local_base_wire.checked_add(next_wire.saturating_sub(raw_base_wire))?;
                    let wire_peak =
                        local_wire.checked_mul(if call.http_response { 0 } else { 1 })?;
                    let out_peak = base_payload
                        .checked_add(b64)?
                        .checked_add(wire_peak)?
                        .checked_add(1024)?;
                    Some(read_peak.max(out_peak))
                });
                if needed_peak.is_some_and(|target| pack_lease.try_resize(target)) {
                    total_b64 = next_b64.unwrap();
                    max_raw = next_max_raw;
                    running_wire = next_wire;
                    planned.push(PlannedAttachment::Select(att));
                } else {
                    stopped_on_memory = true;
                    break;
                }
            }

            if planned.is_empty() && stopped_on_memory && action != Action::Kill && !had_new_text {
                let m = files::TaskMeta {
                    bare_task_id: task.id.clone(),
                    log_path: files::path::display_path(&task.log_path),
                };
                return Err(ServiceError::Overload(
                    overload_task_message(pool.limit(), &m),
                    Some(m),
                ));
            }

            if !planned.is_empty() {
                let task_for_pack = Arc::clone(&task);
                let (encoded_steps, blocks, evicted_mid_pack, held) =
                    tokio::task::spawn_blocking(move || {
                        // The blocking worker owns every guard until its buffers are gone,
                        // even if the awaiting request is cancelled. Keep `read` last so
                        // memory leases and permits release before `read_gate` unlocks.
                        let held = (pack_lease, workspace, permit, read);
                        let mut out = Vec::with_capacity(planned.len());
                        let mut blocks = Vec::with_capacity(planned.len());
                        let mut saw_eviction = false;
                        for entry in planned {
                            match entry {
                                PlannedAttachment::Degraded { index, warning } => {
                                    out.push(PackStep::Degraded { index, warning });
                                }
                                PlannedAttachment::Select(att) => {
                                    let meta = att.meta();
                                    let shown = files::path::display_path(&meta.path);
                                    let mut data = vec![0; meta.size as usize];
                                    let read_res = File::open(&meta.path).and_then(|mut file| {
                                        file.read_exact(&mut data)?;
                                        file.read(&mut [0; 1])
                                    });
                                    match read_res {
                                        Ok(0) => {
                                            let encoded = media::encode_base64(&data);
                                            drop(data);
                                            let block = media::content_block(
                                                meta.kind, &meta.mime, encoded, &meta.path,
                                            );
                                            out.push(PackStep::Delivered {
                                                index: meta.index,
                                                kind: meta.kind,
                                                mime: meta.mime.clone(),
                                                bytes: meta.size,
                                                base64_len: meta.base64_len,
                                                shown_path: shown,
                                            });
                                            blocks.push(block);
                                        }
                                        Ok(_) => {
                                            return Err(format!(
                                                "Cannot read stored attachment {shown}: attachment size changed; read its path directly."
                                            ));
                                        }
                                        Err(_) if task_for_pack.evicted() => {
                                            saw_eviction = true;
                                            break;
                                        }
                                        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                                            return Err(format!(
                                                "Cannot read stored attachment {shown}: attachment size changed; read its path directly."
                                            ));
                                        }
                                        Err(error) => {
                                            return Err(format!(
                                                "Cannot read stored attachment {shown}: {error}."
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                        Ok::<_, String>((out, blocks, saw_eviction, held))
                    })
                    .await
                    .map_err(|error| {
                        format!("Internal failure while encoding attachments: {error}.")
                    })??;
                let (pack_lease, packed_workspace, packed_permit, packed_read) = held;
                read = packed_read;
                permit = packed_permit;
                workspace = packed_workspace;
                bundle.steps = encoded_steps;
                packed_blocks = blocks;
                if evicted_mid_pack {
                    bundle.tail_warnings.push(
                        "log evicted under the 4 GiB total limit; remaining stored attachments are unavailable".into(),
                    );
                }
                if pack_lease.bytes() > 0 {
                    bundle.lease = Some(pack_lease);
                }
            }
        }

        let background = self.background(call, &task, view.nested, action)?;
        let footer = background
            .value
            .as_ref()
            .map_or(String::new(), |(line, _)| format!("\n{line}"));
        let mut suppressed = build_candidate(
            &window_val,
            &bundle,
            &extra_kill_warnings,
            &footer,
            active_max_bytes,
            packed_blocks,
        );

        if total_unseen > 0 {
            let update_queued_warning = |steps: &[PackStep],
                                         tail: &mut Vec<String>,
                                         evicted: bool| {
                tail.retain(|w| w.contains("evicted under the 4 GiB"));
                if evicted {
                    return;
                }
                let remaining = total_unseen.saturating_sub(steps.len());
                if remaining > 0 {
                    if action == Action::Kill {
                        tail.push(
                                "unseen attachment(s) deferred to fit the 8 MiB control reserve or active memory budget; poll taskId without input to retrieve them".into(),
                            );
                    } else {
                        let seen_now = steps.last().map_or(initial_seen, PackStep::index);
                        if seen_now > initial_seen {
                            tail.push(format!(
                                    "{remaining} attachment(s) remain queued after #{seen_now}; poll taskId without input to retrieve the next batch"
                                ));
                        } else {
                            tail.push(format!(
                                    "{remaining} attachment(s) remain queued; poll taskId without input to retrieve them"
                                ));
                        }
                    }
                }
            };

            let evicted_now = snapshot.evicted || task.evicted();
            update_queued_warning(&bundle.steps, &mut bundle.tail_warnings, evicted_now);
            let blocks = suppressed.content.split_off(1);
            drop(suppressed);
            suppressed = build_candidate(
                &window_val,
                &bundle,
                &extra_kill_warnings,
                &footer,
                active_max_bytes,
                blocks,
            );
            while !bundle.steps.is_empty()
                && measure_outbound_len(call, &suppressed)? > effective_cap
            {
                if bundle.steps.len() == 1
                    && !had_new_text
                    && action != Action::Kill
                    && let PackStep::Delivered {
                        index,
                        mime,
                        bytes,
                        base64_len,
                        shown_path,
                        ..
                    } = &bundle.steps[0]
                {
                    let msg = format!(
                        "attachment #{index} [{mime}] ({bytes} B, {base64_len} base64 B) exceeds the {outbound_limit} B outbound frame limit and was skipped; inspect or transfer {shown_path}"
                    );
                    bundle.steps[0] = PackStep::Degraded {
                        index: *index,
                        warning: msg,
                    };
                    suppressed.content.truncate(1);
                    bundle.lease = None;
                } else {
                    if matches!(bundle.steps.pop(), Some(PackStep::Delivered { .. })) {
                        suppressed.content.pop();
                    }
                    if bundle.steps.is_empty() {
                        bundle.lease = None;
                    }
                }
                update_queued_warning(&bundle.steps, &mut bundle.tail_warnings, evicted_now);
                let blocks = suppressed.content.split_off(1);
                drop(suppressed);
                suppressed = build_candidate(
                    &window_val,
                    &bundle,
                    &extra_kill_warnings,
                    &footer,
                    active_max_bytes,
                    blocks,
                );
                if matches!(bundle.steps.as_slice(), [PackStep::Degraded { .. }]) {
                    break;
                }
            }
        }

        let mut lease = match admit_outbound(call, action, &task, &suppressed, &mut bundle.lease) {
            Ok(lease) => lease,
            Err(ServiceError::Overload(..))
                if action == Action::Kill && !bundle.steps.is_empty() =>
            {
                suppressed.content.truncate(1);
                bundle.steps.clear();
                bundle.lease = None;
                bundle.tail_warnings = vec![
                    "unseen attachment(s) deferred because active media memory is full; poll taskId without input to retrieve them".into(),
                ];
                let blocks = suppressed.content.split_off(1);
                drop(suppressed);
                suppressed = build_candidate(
                    &window_val,
                    &bundle,
                    &extra_kill_warnings,
                    &footer,
                    active_max_bytes,
                    blocks,
                );
                admit_outbound(call, action, &task, &suppressed, &mut None)?
            }
            Err(err) => return Err(err),
        };
        drop(window_val);
        let (suppressed, payload_owner) =
            retain_text_result(suppressed, workspace, &mut lease, call, &task)?;
        let _permit = permit;
        read.state.attachments_seen = bundle.steps.last().map_or(initial_seen, PackStep::index);
        let state = read.commit();
        task_reader.mark_delivered();
        report(
            session,
            &entry,
            &task_reader,
            snapshot,
            background,
            view.nested,
            view.owned_by_script,
        );
        drop(state);
        let meta = files::TaskMeta {
            bare_task_id: task.id.clone(),
            log_path: files::path::display_path(&task.log_path),
        };
        Ok((suppressed, lease, Some(meta), payload_owner))
    }

    /// A PTY task's transcript from the first line no earlier read showed: its final lines,
    /// then the pending ones, which the file also holds after the final lines until output
    /// changes them. The result shows the last lines by default, where a program's newest
    /// output is.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn transcript_result(
        &self,
        call: &Call<'_>,
        session: &Session,
        task: Arc<Task>,
        entry: Arc<TaskEntry>,
        task_reader: Arc<Reader>,
        state: tokio::sync::OwnedMutexGuard<ReadState>,
        view: View,
    ) -> Result<
        (
            CallToolResult,
            Option<Lease>,
            Option<files::TaskMeta>,
            crate::lease::Owner,
        ),
        ServiceError,
    > {
        let permit = call
            .task_render_permit(Action::Transcript, &task, &task_reader)
            .await?;
        let render = call.render;
        let metadata = RenderSnapshot::new(&task, call, Action::Transcript)?;
        let snapshot = &metadata.value;
        let (reader, tasks) = (Arc::clone(&task), Arc::clone(&self.tasks));
        let first_permit = permit.clone();
        let memory = Arc::clone(call.launch().memory());
        let workspace = memory
            .try_acquire(text_workspace_bytes(view.max_bytes))
            .ok_or_else(|| {
                let meta = files::TaskMeta {
                    bare_task_id: task.id.clone(),
                    log_path: files::path::display_path(&task.log_path),
                };
                ServiceError::Overload(overload_task_message(memory.limit(), &meta), Some(meta))
            })?;
        let (transcript, read, _first_permit, workspace) = tokio::task::spawn_blocking(move || {
            let owned_workspace = workspace;
            let read = StagedRead::new(state);
            let transcript = reader.transcript(&tasks, &read.state, &memory);
            (transcript, read, first_permit, owned_workspace)
        })
        .await
        .map_err(|error| format!("Internal failure while reading: {error}."))?;
        let transcript = transcript.map_err(|error| task_read_error(error, &task))?;
        let pending_lines = transcript.pending.matches('\n').count() as u64;
        let total = transcript.lines + pending_lines;
        let shown_transcript_path = files::path::display_path(&transcript.path);
        let transcript_note = format!(
            " · transcript {total} lines, final through {}; this read starts at line {} · {shown_transcript_path}",
            transcript.lines, transcript.first_line,
        );
        let mut status_with_log = status_line(&task, snapshot, &task_reader, true, None);
        status_with_log.push_str(&transcript_note);
        let mut status_no_log = status_line(&task, snapshot, &task_reader, false, None);
        status_no_log.push_str(&transcript_note);
        let spec = WindowSpec {
            truncate: view.truncate,
            budget: view.max_bytes,
            raw: false,
            clean: true,
            keep_nul: false,
            encoding: encoding_rs::UTF_8,
            source: "transcript",
        };
        let mut structured = task_json(Action::Transcript, &task, snapshot, &task_reader);
        structured["transcriptPath"] = json!(shown_transcript_path);
        structured["transcriptLines"] = json!(total);
        structured["finalLines"] = json!(transcript.lines);
        structured["firstLine"] = json!(transcript.first_line);
        structured["transcriptIncomplete"] =
            json!(transcript.cut || transcript.stopped.is_some() || conpty::load().is_some());
        structured["transcriptError"] = json!(transcript.stopped);
        if transcript.cut {
            structured["warnings"].as_array_mut().unwrap().push(json!(
                "pending lines past the transcript's 64 MiB limit are left out"
            ));
        }
        if let Some(reason) = &transcript.stopped {
            structured["warnings"]
                .as_array_mut()
                .unwrap()
                .push(json!(format!(
                    "the transcript stops after line {}: {reason}",
                    transcript.lines
                )));
        }
        if let Some(warning) = conpty::load() {
            structured["warnings"]
                .as_array_mut()
                .unwrap()
                .push(json!(format!(
                    "{warning}, so transcript lines may be missing"
                )));
        }
        let logged = Arc::clone(&task);
        let (window, read, _permit, (snapshot_owner, workspace)) =
            tokio::task::spawn_blocking(move || {
                // Capture the lease explicitly: field-only closure capture must not leave it
                // outside the blocking worker while that worker still owns pending text.
                let held = (transcript._lease, workspace);
                let mut read = read;
                /// Reads the final lines into `builder`.
                fn read_into(
                    path: &std::path::Path,
                    start: u64,
                    bytes: u64,
                    builder: &mut WindowBuilder<'_>,
                ) -> std::io::Result<()> {
                    // Final lines only grow, so the range read stays as it was.
                    let mut file = File::open(path)?;
                    file.seek(SeekFrom::Start(start))?;
                    let mut remaining = bytes - start;
                    let mut buffer = vec![0_u8; 64 * 1024];
                    while remaining > 0 {
                        let want = remaining.min(buffer.len() as u64) as usize;
                        file.read_exact(&mut buffer[..want])?;
                        builder.push(&buffer[..want]);
                        remaining -= want as u64;
                    }
                    Ok(())
                }
                let mut cleaner = Cleaner::default();
                let mut builder = WindowBuilder::new(spec, &mut cleaner, transcript.first_line);
                let window = match read_into(
                    &transcript.path,
                    transcript.start,
                    transcript.bytes,
                    &mut builder,
                ) {
                    Err(_) if logged.evicted() => Err(EVICTED_TRANSCRIPT.to_string()),
                    Err(error) => Err(format!("Cannot read the transcript file: {error}.")),
                    Ok(()) => {
                        builder.push(transcript.pending.as_bytes());
                        read.state.shown = (transcript.bytes, transcript.lines);
                        Ok(builder.finish())
                    }
                };
                (window, read, permit, held)
            })
            .await
            .map_err(|error| format!("Internal failure while reading: {error}."))?;
        drop(snapshot_owner);
        let window = window?;
        add_encoding_warning(&mut structured, &window);
        let warnings = warning_text(&structured["warnings"]);
        let background = self.background(call, &task, view.nested, Action::Transcript)?;
        let footer = background
            .value
            .as_ref()
            .map_or(String::new(), |(line, _)| format!("\n{line}"));
        let text = self.layout(
            render,
            &window,
            status_with_log,
            status_no_log,
            snapshot,
            task_reader.is_fresh(),
            &footer,
            view.max_bytes,
            "(no transcript lines since the last read)",
            &mut structured,
        );
        let suppressed = suppress(success(text, structured, &warnings), render);
        let mut lease = admit_outbound(call, Action::Transcript, &task, &suppressed, &mut None)?;
        drop(window);
        let (suppressed, payload_owner) =
            retain_text_result(suppressed, workspace, &mut lease, call, &task)?;
        let state = read.commit();
        task_reader.mark_delivered();
        report(
            session,
            &entry,
            &task_reader,
            snapshot,
            background,
            view.nested,
            view.owned_by_script,
        );
        drop(state);
        let meta = files::TaskMeta {
            bare_task_id: task.id.clone(),
            log_path: files::path::display_path(&task.log_path),
        };
        Ok((suppressed, lease, Some(meta), payload_owner))
    }

    /// Combines the already-budgeted `window` body with the status line and background footer,
    /// adds the window metadata to `structured`, and returns the text content.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn layout(
        &self,
        render: Render,
        window: &Window,
        status_with_log: String,
        status_no_log: String,
        snapshot: &Snapshot,
        fresh: bool,
        footer: &str,
        max_bytes: usize,
        empty: &str,
        structured: &mut Value,
    ) -> String {
        let cut_lines = window.cut_lines;
        let omitted = window.omitted;
        let mut status = if needs_log_in_status(snapshot, cut_lines, omitted) {
            status_with_log
        } else {
            status_no_log
        };
        if let Some((first, last)) = omitted {
            status.push_str(&format!(
                " · {} lines omitted: {} lines {first}-{last}",
                last - first + 1,
                window.source
            ));
        }
        if fresh {
            status.push_str(NEW_READER_NOTE);
        }
        let body = if window.is_empty() {
            if empty.len() <= max_bytes { empty } else { "" }
        } else {
            window.text.as_str()
        };
        let text = if render.output_mode == OutputMode::Structured {
            format!("{status}{footer}")
        } else {
            format!("{body}\n\n{status}{footer}")
        };
        structured["omittedLines"] = json!(omitted.map_or(0, |(first, last)| last - first + 1));
        structured["omittedRange"] = json!(omitted.map(|(first, last)| [first, last]));
        if render.output_mode != OutputMode::Text {
            structured["output"] = json!(&window.text);
        }
        structured["cutLines"] = json!(cut_lines);
        text
    }
}

impl Service {
    /// The rendered screen of a PTY task, bottom rows first to stay within `maxBytes`.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn screen_result(
        &self,
        call: &Call<'_>,
        render: Render,
        session: &Session,
        action: Action,
        task: Arc<Task>,
        entry: Arc<TaskEntry>,
        task_reader: Arc<Reader>,
        state: tokio::sync::OwnedMutexGuard<ReadState>,
        view: View,
        ended: WaitEnd,
        startup_warning: Option<ProbeWarning>,
        permit: Option<Arc<OwnedSemaphorePermit>>,
    ) -> Result<
        (
            CallToolResult,
            Option<Lease>,
            Option<files::TaskMeta>,
            crate::lease::Owner,
        ),
        ServiceError,
    > {
        // A finished state, taken first, means the screen below holds all output.
        let metadata = RenderSnapshot::new(&task, call, action)?;
        let snapshot = &metadata.value;
        let screen = task
            .screen(call.launch().memory())
            .map_err(|error| task_read_error(error, &task))?;
        // The stream counts as read up to the output the screen shows.
        let (reader, until) = (Arc::clone(&task), screen.log_end);
        let (window, read, _permit, screen) = tokio::task::spawn_blocking(move || {
            let owned_screen = screen;
            let mut read = StagedRead::new(state);
            let window = reader.read_window(
                &mut read.state,
                Truncate::Tail,
                MIN_BODY_BUDGET,
                false,
                true,
                false,
                reader.encoding,
                Some(until),
            );
            (window, read, permit, owned_screen)
        })
        .await
        .map_err(|error| format!("Internal failure while reading output: {error}."))?;
        let window = window.map_err(|error| format!("Cannot read the task log: {error}."))?;
        let mut structured = task_json(action, &task, snapshot, &task_reader);
        if let Some(warning) = startup_warning.and_then(ProbeWarning::deliver) {
            structured["warnings"]
                .as_array_mut()
                .unwrap()
                .push(Value::String(warning));
        }
        add_encoding_warning(&mut structured, &window);
        let warnings = warning_text(&structured["warnings"]);
        let background = self.background(call, &task, view.nested, action)?;
        let footer = background
            .value
            .as_ref()
            .map_or(String::new(), |(line, _)| format!("\n{line}"));
        let (row, col) = (screen.cursor.0 + 1, screen.cursor.1 + 1);
        let screen_note = format!(
            "{} · screen {}x{}, cursor row {row} col {col}",
            ended.note(view.conditions),
            screen.cols,
            screen.rows_count
        );
        let room = view.max_bytes;
        let full_len = screen.rows.iter().map(String::len).sum::<usize>()
            + screen.rows.len().saturating_sub(1);
        let need_log = needs_log_in_status(snapshot, 0, None) || full_len > room;
        let mut status = status_line(&task, snapshot, &task_reader, need_log, None);
        status.push_str(&screen_note);
        if task_reader.is_fresh() {
            status.push_str(NEW_READER_NOTE);
        }
        let (body, omitted) = fit_screen_rows(&screen.rows, room);
        let shown = if body.is_empty() && room >= "(blank screen)".len() {
            "(blank screen)"
        } else {
            &body
        };
        let text = if render.output_mode == OutputMode::Structured {
            format!("{status}{footer}")
        } else {
            format!("{shown}\n\n{status}{footer}")
        };
        structured["waitEndedBy"] = ended.json();
        structured["cols"] = json!(screen.cols);
        structured["rows"] = json!(screen.rows_count);
        structured["cursor"] = json!([row, col]);
        structured["omittedRows"] = json!(omitted);
        if render.output_mode != OutputMode::Text {
            structured["output"] = json!(body);
        }
        let suppressed = suppress(success(text, structured, &warnings), render);
        let mut lease = admit_outbound(call, action, &task, &suppressed, &mut None)?;
        drop(window);
        drop(body);
        drop(screen.rows);
        let (suppressed, payload_owner) =
            retain_text_result(suppressed, screen._lease, &mut lease, call, &task)?;
        let state = read.commit();
        task_reader.mark_delivered();
        report(
            session,
            &entry,
            &task_reader,
            snapshot,
            background,
            view.nested,
            view.owned_by_script,
        );
        drop(state);
        let meta = files::TaskMeta {
            bare_task_id: task.id.clone(),
            log_path: files::path::display_path(&task.log_path),
        };
        Ok((suppressed, lease, Some(meta), payload_owner))
    }
}

pub(super) fn fit_screen_rows(rows: &[String], room: usize) -> (String, usize) {
    let mut body = rows.join("\n");
    let mut omitted = 0;
    while body.len() > room && omitted + 1 < rows.len() {
        omitted += 1;
        body = format!(
            "... [{omitted} top rows omitted] ...\n{}",
            rows[omitted..].join("\n")
        );
    }
    output::cut_to(&mut body, room.saturating_add(1));
    (body, omitted)
}

fn status_line(
    task: &Task,
    snapshot: &Snapshot,
    reader: &Reader,
    include_log: bool,
    script: Option<&ScriptReport>,
) -> String {
    let state = match snapshot.exit_code {
        Some(code) if snapshot.state == "exited" => format!("exited {code}"),
        Some(code) => format!("{} (exit {code})", snapshot.state),
        None => snapshot.state.to_string(),
    };
    let mut line = format!(
        "[{state}] {} · {:.1}s · {} lines",
        reader.handle(&task.id),
        snapshot.elapsed.as_secs_f64(),
        snapshot.lines,
    );
    if include_log {
        line.push_str(&format!(
            " · log {}",
            files::path::display_path(&task.log_path)
        ));
    }
    if snapshot.state == "running" {
        line.push_str(" · poll to continue, kill to stop");
    }
    if snapshot.expired {
        line.push_str(" · killed by killAfterMs");
    }
    if snapshot.kept {
        let names = members(&snapshot.held);
        line.push_str(&match &snapshot.kill_error {
            Some(error) => format!(" · could not end {names}: {error}; kill retries"),
            None => format!(" · keeps {names} running; kill ends them"),
        });
    } else if snapshot.tree_ended {
        line.push_str(" · kept processes ended");
    } else if !snapshot.leftovers.is_empty() && !task.keep {
        line.push_str(&format!(
            " · ended with bash: {}; start with keepTree: true to keep them",
            members(&snapshot.leftovers)
        ));
    }
    if snapshot.forced_end {
        line.push_str(" · root forced after 5 s exit grace");
    }
    if let Some(report) = script {
        if snapshot.state == "killed" {
            line.push_str(&format!(
                " · cancelled {} pending call(s) only; ",
                report.cancelled
            ));
            if report.running.is_empty() {
                line.push_str("tasks started with exec keep running");
            } else {
                line.push_str(&format!(
                    "tasks it started keep running: {}; stop each with exec kill",
                    task_handles(&report.running)
                ));
            }
        } else if snapshot.state != "running" && !report.running.is_empty() {
            line.push_str(&format!(
                " · tasks it started still running: {}",
                task_handles(&report.running)
            ));
        }
    }
    line
}

/// Names processes as `adb.exe (4120), sleep (77) and 3 more`.
pub(super) fn members(members: &[process::Member]) -> String {
    if members.is_empty() {
        return "processes".into();
    }
    let mut text = members
        .iter()
        .take(3)
        .map(|member| format!("{} ({})", member.name, member.pid))
        .collect::<Vec<_>>()
        .join(", ");
    if members.len() > 3 {
        text.push_str(&format!(" and {} more", members.len() - 3));
    }
    text
}

fn rpc_result_len(call: &Call<'_>, result: &CallToolResult) -> Result<usize, String> {
    crate::ipc::json_len(result)
        .map(|n| n.saturating_add(call.rpc_id_bytes).saturating_add(256))
        .map_err(|err| format!("Cannot measure result JSON: {err}."))
}

fn local_wire_len(call: &Call<'_>, result: &CallToolResult) -> Result<usize, String> {
    if call.render_slot.is_some() {
        wire_result_len(call.call_id, result)
    } else {
        rpc_result_len(call, result)
    }
}

pub(super) fn measure_outbound_len(
    call: &Call<'_>,
    result: &CallToolResult,
) -> Result<usize, String> {
    let rpc = rpc_result_len(call, result)?;
    if call.render_slot.is_some() {
        wire_result_len(call.call_id, result).map(|n| n.saturating_sub(4).max(rpc))
    } else {
        Ok(rpc)
    }
}

enum PlannedAttachment {
    Select(tasks::state::AttachmentLease),
    Degraded { index: usize, warning: String },
}

enum PackStep {
    Delivered {
        index: usize,
        kind: media::MediaKind,
        mime: String,
        bytes: u64,
        base64_len: usize,
        shown_path: String,
    },
    Degraded {
        index: usize,
        warning: String,
    },
}

impl PackStep {
    fn index(&self) -> usize {
        match self {
            Self::Delivered { index, .. } | Self::Degraded { index, .. } => *index,
        }
    }
}

// A candidate can register opaque handles before its final size is known.
// Refusal/cancellation removes only those new handles; committed handles stay.
struct NestedAttachments<'a> {
    script: Option<&'a codemode::control::ScriptHandle>,
    ids: Vec<u64>,
}

impl Drop for NestedAttachments<'_> {
    fn drop(&mut self) {
        if let Some(script) = self.script {
            for id in self.ids.drain(..) {
                script.unregister_attachment(id);
            }
        }
    }
}

#[derive(Default)]
struct AttachmentBundle {
    steps: Vec<PackStep>,
    nested_handles: Vec<Value>,
    tail_warnings: Vec<String>,
    lease: Option<Lease>,
}

pub(super) struct ScriptReport {
    pub(super) value: Value,
    pub(super) running: Vec<String>,
    pub(super) cancelled: usize,
    _lease: Lease,
}

pub(super) fn script_report(
    tasks: &Tasks,
    task: &Task,
    call: &Call<'_>,
    action: Action,
) -> Result<Option<ScriptReport>, ServiceError> {
    let Some(script) = task.script() else {
        return Ok(None);
    };
    let codemode::control::ReportSnapshot {
        cancelled,
        started: handles,
        _lease: lease,
    } = script
        .report(
            call.launch().memory(),
            (action == Action::Kill).then_some(call.launch().control_memory()),
        )
        .ok_or_else(|| {
            let meta = files::TaskMeta {
                bare_task_id: task.id.clone(),
                log_path: files::path::display_path(&task.log_path),
            };
            ServiceError::Overload(
                overload_task_message(call.launch().memory().limit(), &meta),
                Some(meta),
            )
        })?;
    let mut running = Vec::new();
    let started = handles
        .into_iter()
        .map(|handle| {
            let task = parse_handle(&handle)
                .ok()
                .and_then(|(id, _)| tasks.find(id));
            let state = task.as_ref().map_or("unknown", |task| {
                let (state, kept) = task.report_state();
                if state == "running" || kept {
                    running.push(handle.clone());
                }
                state
            });
            json!({"taskId": handle, "state": state})
        })
        .collect::<Vec<_>>();
    Ok(Some(ScriptReport {
        cancelled: cancelled.len(),
        running,
        value: json!({
            "cancelledCalls": cancelled,
            "startedTasks": started,
        }),
        _lease: lease,
    }))
}

fn task_handles(handles: &[String]) -> String {
    let mut text = handles
        .iter()
        .take(3)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    if handles.len() > 3 {
        text.push_str(&format!(" and {} more", handles.len() - 3));
    }
    text
}

fn task_json(action: Action, task: &Task, snapshot: &Snapshot, reader: &Reader) -> Value {
    json!({
        "ok": task_io_ok(snapshot),
        "action": if task.is_script() && action == Action::Start { "codemode" } else { action_name(action) },
        "kind": if task.is_script() { "codemode" } else { "bash" },
        "taskId": reader.handle(&task.id),
        "state": snapshot.state,
        "exitCode": snapshot.exit_code,
        "pty": task.pty,
        "elapsedMs": snapshot.elapsed.as_millis() as u64,
        "logPath": files::path::display_path(&task.log_path),
        "lifetimeExpired": snapshot.expired,
        "logError": snapshot.log_error,
        "captureError": snapshot.capture_error,
        "inputWarning": snapshot.input_warning,
        "inputError": snapshot.input_error,
        "droppedBytes": snapshot.dropped,
        "logEvicted": snapshot.evicted,
        "outputOpen": snapshot.output_open,
        "outputIncomplete": output_incomplete(snapshot),
        "warnings": task_warnings(snapshot),
        "keepTree": task.keep,
        "forcedEnd": snapshot.forced_end,
        "leftovers": members_json(&snapshot.leftovers),
        "keptRunning": snapshot.kept,
        "held": members_json(&snapshot.held),
        "killError": snapshot.kill_error,
    })
}

pub(super) fn task_io_ok(snapshot: &Snapshot) -> bool {
    snapshot.capture_error.is_none()
        && snapshot.log_error.is_none()
        && snapshot.input_error.is_none()
}

pub(super) fn output_incomplete(snapshot: &Snapshot) -> bool {
    snapshot.capture_error.is_some()
        || snapshot.log_error.is_some()
        || snapshot.dropped > 0
        || snapshot.evicted
}

pub(super) fn task_warnings(snapshot: &Snapshot) -> Vec<String> {
    let mut warnings = Vec::new();
    if let Some(error) = &snapshot.capture_error {
        warnings.push(format!(
            "output capture failed ({error}); output is incomplete"
        ));
    }
    if let Some(error) = &snapshot.log_error {
        warnings.push(format!(
            "log write failed ({error}); {} bytes were not stored",
            snapshot.dropped
        ));
    } else if snapshot.dropped > 0 {
        warnings.push(format!(
            "{} bytes past the log limit were not stored",
            snapshot.dropped
        ));
    }
    if snapshot.evicted {
        warnings.push(
            "log evicted under the 4 GiB total limit; stored output and attachments are unavailable"
                .into(),
        );
    }
    if snapshot.output_open {
        warnings.push(if snapshot.kept {
            "processes it keeps hold the output open; later output may follow".into()
        } else {
            "output stayed open after exit; later output may follow".into()
        });
    }
    if let Some(error) = &snapshot.kill_error {
        warnings.push(format!(
            "could not end the task's process tree: {error}; kill retries"
        ));
    }
    if let Some(error) = &snapshot.input_error {
        warnings.push(error.clone());
    }
    if let Some(warning) = &snapshot.input_warning {
        warnings.push(warning.clone());
    }
    warnings
}

pub(super) fn members_json(members: &[process::Member]) -> Value {
    json!(
        members
            .iter()
            .map(|member| json!({"pid": member.pid, "name": member.name}))
            .collect::<Vec<_>>()
    )
}

fn add_encoding_warning(value: &mut Value, window: &Window) {
    value["encodingErrors"] = json!(window.bad_lines);
    if window.bad_lines > 0 {
        value["warnings"]
            .as_array_mut()
            .unwrap()
            .push(json!(format!(
                "{} lines had bytes invalid in {}; pass encoding on a stream read (e.g. big5, gbk)",
                window.bad_lines,
                window.encoding.name()
            )));
    }
}

/// Omits default-valued diagnostic fields on task results and `list` entries at final serialization.
fn prune_task_diagnostics(value: &mut Value) {
    let Some(obj) = value.as_object_mut() else {
        return;
    };
    for key in [
        "captureError",
        "logError",
        "killError",
        "inputError",
        "inputWarning",
        "omittedRange",
        "transcriptError",
        "cols",
        "rows",
        "session",
        "callId",
    ] {
        if obj.get(key) == Some(&Value::Null) {
            obj.remove(key);
        }
    }
    for key in [
        "droppedBytes",
        "encodingErrors",
        "cutLines",
        "omittedLines",
        "omittedRows",
    ] {
        if obj.get(key).and_then(Value::as_u64) == Some(0) {
            obj.remove(key);
        }
    }
    for key in [
        "forcedEnd",
        "lifetimeExpired",
        "logEvicted",
        "outputIncomplete",
        "outputOpen",
        "keptRunning",
        "keepTree",
        "transcriptIncomplete",
    ] {
        if obj.get(key) == Some(&Value::Bool(false)) {
            obj.remove(key);
        }
    }
    for key in ["held", "leftovers", "warnings"] {
        if obj
            .get(key)
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
        {
            obj.remove(key);
        }
    }
    if let Some(Value::Object(codemode)) = obj.get_mut("codemode") {
        for key in [
            "cancelledCalls",
            "startedTasks",
            "attachmentsDelivered",
            "attachments",
        ] {
            if codemode
                .get(key)
                .and_then(Value::as_array)
                .is_some_and(Vec::is_empty)
            {
                codemode.remove(key);
            }
        }
        if codemode.is_empty() {
            obj.remove("codemode");
        }
    }
    if let Some(Value::Array(tasks)) = obj.get_mut("tasks") {
        for entry in tasks {
            prune_task_diagnostics(entry);
        }
    }
}

/// Formats complete warnings outside the body budget.
pub(super) fn warning_text(warnings: &Value) -> String {
    let mut text = String::new();
    if let Some(warnings) = warnings.as_array() {
        for warning in warnings.iter().filter_map(Value::as_str) {
            if text.is_empty() {
                text.push('\n');
            }
            text.push_str("\nWarning: ");
            text.push_str(warning);
        }
    }
    text
}

pub(super) fn success(mut text: String, mut structured: Value, suffix: &str) -> CallToolResult {
    text.push_str(suffix);
    let is_err = structured["ok"] == false;
    prune_task_diagnostics(&mut structured);
    let content = vec![ContentBlock::text(text)];
    let mut result = if is_err {
        CallToolResult::error(content)
    } else {
        CallToolResult::success(content)
    };
    result.structured_content = Some(structured);
    result
}

pub fn error_result(action: Action, message: &str) -> CallToolResult {
    error_named(action_name(action), message)
}

pub fn codemode_error(message: &str) -> CallToolResult {
    error_named("codemode", message)
}

fn error_named(action: &str, message: &str) -> CallToolResult {
    let mut result = CallToolResult::error(vec![ContentBlock::text(message)]);
    result.structured_content = Some(json!({ "ok": false, "action": action, "error": message }));
    result
}
