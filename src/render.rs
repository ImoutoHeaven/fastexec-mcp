//! Agent presentation: pure-text/media MCP `CallToolResult` formatting, payload budgeting,
//! attachment packing, and background footers.

pub mod files;
#[cfg(test)]
mod tests;
pub mod types;

use self::types::media_content_block;
use self::types::{AgentDelivery, DaemonDelivery, TrackedResult, error_result, schema_error};
use crate::env::{LaunchContext, ProbeWarning};
use crate::lease::Lease;
use crate::media;
use crate::platform::members::Member;
use crate::platform::process::clean_member_name;
use crate::service::core::{
    PreparedSnapshot, ScriptReport, ServiceError, TaskMeta, encoding_warning, kill_pending_note,
    metadata_members, metadata_workspace_bytes, output_incomplete, overload_task_message,
    resize_metadata_workspace, task_io_ok, task_warnings,
};
use crate::service::schema::{Action, ExecPolicy, ListState, Request};
use crate::service::{
    Consumer, ExecContext, PreparedExec, PreparedList, PreparedScreen, PreparedTranscript,
    PreparedWindow, Service, StagedRead,
};
use crate::tasks::session::{OutsideClaim, Reader, Session, TaskEntry};
use crate::tasks::state::{Snapshot, Task};
use crate::tasks::{self, ChildSummary};
use crate::term::output::{self, Truncate, Window, WindowSpec};
use crate::wait::WaitEnd;
use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::Value;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;

const NEW_READER_NOTE: &str = " · new reader";
/// Largest background footer.
const FOOTER_LIMIT: usize = 512;
const NOTE_CHARS: usize = 60;

/// Agent entrypoint for `exec`: returns pure-text/media `CallToolResult` with `structured_content: None`.
pub async fn exec_agent(
    service: &Service,
    request: Request,
    mut ctx: ExecContext<'_>,
    delivery: AgentDelivery<'_>,
) -> TrackedResult {
    service.tasks().prune_entries(ctx.session);
    let render_slot = delivery.daemon_ipc.as_ref().map(|d| d.slot);
    let outcome = service
        .exec_core(
            request,
            ExecPolicy::AGENT,
            Consumer::Agent,
            &mut ctx,
            render_slot,
        )
        .await;
    finish_exec_agent(service, outcome, &ctx, &delivery).await
}

/// Agent entrypoint for raw JSON `exec` arguments (daemon IPC / HTTP).
pub async fn exec_agent_json(
    service: &Service,
    arguments: Value,
    ctx: ExecContext<'_>,
    delivery: AgentDelivery<'_>,
) -> TrackedResult {
    match serde_json::from_value::<Request>(arguments) {
        Ok(request) => exec_agent(service, request, ctx, delivery).await,
        Err(error) => schema_error(format!("failed to deserialize parameters: {error}")),
    }
}

/// Renders a prepared execution outcome (or formats a pre-render `ServiceError`) into a `TrackedResult`.
pub async fn finish_exec_agent(
    service: &Service,
    outcome: Result<PreparedExec, ServiceError>,
    ctx: &ExecContext<'_>,
    delivery: &AgentDelivery<'_>,
) -> TrackedResult {
    match outcome {
        Ok(prepared) => match render_agent(service, prepared, ctx, delivery).await {
            Ok((result, lease, task_meta, payload_owner)) => TrackedResult {
                result,
                overload: false,
                task_meta,
                lease,
                payload_owner,
            },
            Err(ServiceError::Overload(message, task_meta)) => TrackedResult {
                result: error_result(&message),
                overload: true,
                task_meta,
                lease: None,
                payload_owner: Default::default(),
            },
            Err(err) => TrackedResult::plain(error_result(err.message())),
        },
        Err(ServiceError::Overload(message, task_meta)) => TrackedResult {
            result: error_result(&message),
            overload: true,
            task_meta,
            lease: None,
            payload_owner: Default::default(),
        },
        Err(err) => TrackedResult::plain(error_result(err.message())),
    }
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
    content.saturating_add(meta_payload_capacity(&result.meta))
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
    launch: &LaunchContext,
    task: &Task,
) -> Result<(CallToolResult, crate::lease::Owner), ServiceError> {
    let text_bytes = result
        .content
        .iter()
        .map(|block| block.as_text().map_or(0, |text| text.text.capacity()))
        .fold(0, usize::saturating_add);
    let owner = if let Some(outgoing) = outgoing.as_mut().filter(|held| held.bytes() >= text_bytes)
    {
        outgoing.split_off(text_bytes)
    } else if workspace.try_resize(text_bytes) {
        workspace
    } else {
        drop(result);
        drop(workspace);
        let meta = TaskMeta {
            bare_task_id: task.id.clone(),
            log_path: crate::files::path::display_path(&task.log_path),
        };
        return Err(ServiceError::Overload(
            overload_task_message(launch.memory().limit(), &meta),
            Some(meta),
        ));
    };
    Ok((result, crate::lease::Owner::new(owner)))
}

fn admit_mcp_frame(
    ctx: &ExecContext<'_>,
    delivery: &AgentDelivery<'_>,
    action: Action,
    task: &Task,
    result: &CallToolResult,
    held_lease: &mut Option<Lease>,
) -> Result<Option<Lease>, ServiceError> {
    let meta = || TaskMeta {
        bare_task_id: task.id.clone(),
        log_path: crate::files::path::display_path(&task.log_path),
    };
    let launch = ctx.launch();
    let pool = launch.memory();
    let outbound_limit = launch.outbound_limit();
    let frame_len = measure_outbound_len(delivery, result)?;
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
    let b64_bytes = result_payload_capacity(result);
    if let Some(daemon_ipc) = &delivery.daemon_ipc {
        let wire_len = ipc_wire_len(daemon_ipc, result)?;
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
            .checked_mul(if delivery.http_response { 0 } else { 1 })
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
                    .then(|| launch.control_memory().try_acquire(peak))
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

// Footer text and shown snapshots stay owned until their report marks are committed.
// `claim` drops last so rollback releases `_workspace` before unlocking `outside_consumed`.
struct Background<'a> {
    value: Option<(String, Vec<tasks::Shown>)>,
    external_cursor: Option<u64>,
    event_bus: Option<Arc<crate::events::EventBus>>,
    _workspace: Lease,
    claim: OutsideClaim<'a>,
}

fn commit_agent_report(
    session: &Session,
    entry: &TaskEntry,
    reader: &Reader,
    shown: &Snapshot,
    mut background: Background<'_>,
) {
    reader.mark_reported(shown);
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
    drop(background._workspace);
    background.claim.consume();
    if let (Some(bus), Some(seq)) = (background.event_bus, background.external_cursor) {
        bus.commit_external_background_cursor(&session.id, seq);
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

async fn render_agent(
    service: &Service,
    prepared: PreparedExec,
    ctx: &ExecContext<'_>,
    delivery: &AgentDelivery<'_>,
) -> Result<
    (
        CallToolResult,
        Option<Lease>,
        Option<TaskMeta>,
        crate::lease::Owner,
    ),
    ServiceError,
> {
    match prepared {
        PreparedExec::Window(prepared) => {
            render_window_agent(service, *prepared, ctx, delivery).await
        }
        PreparedExec::Transcript(prepared) => {
            render_transcript_agent(service, *prepared, ctx, delivery).await
        }
        PreparedExec::Screen(prepared) => {
            render_screen_agent(service, *prepared, ctx, delivery).await
        }
        PreparedExec::List(prepared) => render_list_agent(prepared, ctx, delivery),
    }
}

fn background<'a>(
    service: &Service,
    ctx: &ExecContext<'a>,
    task: &Task,
    action: Action,
) -> Result<Background<'a>, ServiceError> {
    let launch = ctx.launch();
    let workspace = std::cell::RefCell::new(Lease::empty(launch.memory()));
    let (mut value, claim) = service
        .tasks()
        .background(
            ctx.session,
            &task.id,
            FOOTER_LIMIT,
            &mut |bytes| {
                let mut held = workspace.borrow_mut();
                let target = held.bytes().saturating_add(metadata_workspace_bytes(bytes));
                resize_metadata_workspace(&mut held, target, launch, action)
            },
            &mut |tree| metadata_members(tree, &mut workspace.borrow_mut(), launch, action),
        )
        .map_err(|_| PreparedSnapshot::overload(launch, task))?;
    let external_room =
        FOOTER_LIMIT.saturating_sub(value.as_ref().map_or(0, |(line, _)| line.len() + 1));
    let external_cursor = if let Some((ext_line, seq)) = service
        .event_bus()
        .peek_external_background_summary(&ctx.session.id, external_room)
    {
        match &mut value {
            Some((line, _)) => {
                line.push('\n');
                line.push_str(&ext_line);
            }
            None => value = Some((ext_line, Vec::new())),
        }
        Some(seq)
    } else {
        None
    };
    Ok(Background {
        value,
        external_cursor,
        event_bus: Some(Arc::clone(service.event_bus())),
        _workspace: workspace.into_inner(),
        claim,
    })
}

async fn render_window_agent(
    service: &Service,
    prepared: PreparedWindow,
    ctx: &ExecContext<'_>,
    delivery: &AgentDelivery<'_>,
) -> Result<
    (
        CallToolResult,
        Option<Lease>,
        Option<TaskMeta>,
        crate::lease::Owner,
    ),
    ServiceError,
> {
    // Declare `read` first so reverse-order local drops always release `workspace` and `permit`
    // before unlocking `read_gate`.
    let mut read;
    let mut permit;
    let mut workspace;
    let PreparedWindow {
        workspace: w,
        permit: p,
        metadata,
        script,
        unseen,
        window: mut window_val,
        had_new_text,
        initial_seen,
        action,
        task,
        entry,
        reader: task_reader,
        view,
        requested_max_bytes,
        ended,
        mut startup_warning,
        mark_session_entry: _,
        read: r,
    } = prepared;
    read = r;
    permit = p;
    workspace = w;

    let launch = ctx.launch();
    let pool = Arc::clone(launch.memory());
    let outbound_limit = launch.outbound_limit();
    let snapshot = &metadata.value;
    let delivered_warning = startup_warning.as_mut().and_then(ProbeWarning::message);
    let mut extra_kill_warnings: Vec<String> = Vec::new();
    if view.max_bytes < requested_max_bytes {
        extra_kill_warnings.push(
            "kill output window narrowed to fit the 8 MiB control reserve; poll taskId or read logPath for remaining output".into(),
        );
    }
    let io_ok = task_io_ok(snapshot);
    let build_candidate = |window_ref: &Window,
                           bundle_ref: &AttachmentBundle,
                           extra_warns: &[String],
                           footer: &str,
                           max_bytes: usize,
                           content_blocks: Vec<ContentBlock>|
     -> CallToolResult {
        let mut status_with_log = status_line(&task, snapshot, &task_reader, true, script.as_ref());
        let mut status_no_log = status_line(&task, snapshot, &task_reader, false, script.as_ref());
        let mut warnings_list = task_warnings(snapshot);
        if let Some(ended) = &ended {
            let note = wait_note(ended, view.conditions);
            status_with_log.push_str(&note);
            status_no_log.push_str(&note);
        } else if let Some(note) = kill_pending_note(&task, snapshot) {
            warnings_list.push(note.to_owned());
        }
        let mut attached_summary = Vec::new();
        for step in &bundle_ref.steps {
            match step {
                PackStep::Delivered {
                    index, mime, bytes, ..
                } => {
                    attached_summary.push(format!("#{index} ({mime}, {bytes} B)"));
                }
                PackStep::Degraded { warning, .. } => {
                    warnings_list.push(warning.clone());
                }
            }
        }
        warnings_list.extend(bundle_ref.tail_warnings.iter().cloned());
        warnings_list.extend(extra_warns.iter().cloned());
        if let Some(warning) = delivered_warning {
            warnings_list.push(warning.to_owned());
        }
        if let Some(enc_warn) = encoding_warning(window_ref) {
            warnings_list.push(enc_warn);
        }
        let mut suffix = String::new();
        if !attached_summary.is_empty() {
            suffix.push_str("\nAttached: ");
            suffix.push_str(&attached_summary.join(", "));
        }
        suffix.push_str(&warning_text(&warnings_list));
        let text = layout(
            window_ref,
            StatusLines {
                with_log: status_with_log,
                without_log: status_no_log,
                snapshot,
                fresh: task_reader.is_fresh(),
            },
            footer,
            max_bytes,
            "(no new output)",
        );
        let mut res = success(text, io_ok, &suffix);
        res.content.extend(content_blocks);
        res
    };

    let mut active_max_bytes = view.max_bytes;
    let effective_cap = if action == Action::Kill {
        (crate::lease::CONTROL_LIMIT - 4).min(outbound_limit)
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
    let pre_check_len = measure_outbound_len(delivery, &pre_check)?;
    drop(pre_check);

    if action == Action::Kill && pre_check_len.saturating_add(516) > effective_cap {
        let state_guard = read.into_guard();
        let reader_narrow = Arc::clone(&task);
        let narrow_budget = view.max_bytes.min(64 * 1024);
        let spec = WindowSpec {
            truncate: view.truncate,
            budget: narrow_budget,
            raw: view.raw,
            clean: !task.is_script(),
            keep_nul: view.keep_nul,
            include_markers: view.inline_markers,
            encoding: view.encoding.unwrap_or(task.encoding),
            source: "log",
        };
        let narrow_win;
        (narrow_win, workspace, permit, read) = tokio::task::spawn_blocking(move || {
            let mut staged = StagedRead::new(state_guard);
            let win = reader_narrow.read_window_with_markers(&mut staged.state, spec, None);
            (win, workspace, permit, staged)
        })
        .await
        .map_err(|error| format!("Internal failure while narrowing kill output: {error}."))?;
        window_val = narrow_win.map_err(|error| format!("Cannot read the task log: {error}."))?;
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
        let raw_base_wire = measure_outbound_len(delivery, &base_candidate)?;
        let local_base_wire = local_wire_len(delivery, &base_candidate)?;
        drop(base_candidate);
        let min_envelope_wire = if had_new_text {
            512usize.saturating_add(delivery.rpc_id_bytes)
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
            let shown_path = crate::files::path::display_path(&meta.path);
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
                let local_wire =
                    local_base_wire.checked_add(next_wire.saturating_sub(raw_base_wire))?;
                let wire_peak =
                    local_wire.checked_mul(if delivery.http_response { 0 } else { 1 })?;
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
            let m = TaskMeta {
                bare_task_id: task.id.clone(),
                log_path: crate::files::path::display_path(&task.log_path),
            };
            return Err(ServiceError::Overload(
                overload_task_message(pool.limit(), &m),
                Some(m),
            ));
        }

        if !planned.is_empty() {
            let task_for_pack = Arc::clone(&task);
            let (encoded_steps, blocks, evicted_mid_pack, held) = tokio::select! {
                biased;
                _ = ctx.cancel.cancelled() => {
                    return Err(ServiceError::Cancelled(
                        "Cancelled before output was committed; the task keeps running.".into(),
                    ));
                }
                packed = tokio::task::spawn_blocking(move || {
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
                                let shown = crate::files::path::display_path(&meta.path);
                                let mut data = vec![0; meta.size as usize];
                                let read_res = File::open(&meta.path).and_then(|mut file| {
                                    file.read_exact(&mut data)?;
                                    file.read(&mut [0; 1])
                                });
                                match read_res {
                                    Ok(0) => {
                                        let encoded = media::encode_base64(&data);
                                        drop(data);
                                        let block = media_content_block(
                                            meta.kind, &meta.mime, encoded, &meta.path,
                                        );
                                        out.push(PackStep::Delivered {
                                            index: meta.index,
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
                                    Err(error)
                                        if error.kind() == std::io::ErrorKind::UnexpectedEof =>
                                    {
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
                }) => packed.map_err(|error| {
                    format!("Internal failure while encoding attachments: {error}.")
                })??,
            };
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

    let background = background(service, ctx, &task, action)?;
    let footer = background
        .value
        .as_ref()
        .map_or(String::new(), |(line, _)| format!("\n{line}"));
    let mut candidate = build_candidate(
        &window_val,
        &bundle,
        &extra_kill_warnings,
        &footer,
        active_max_bytes,
        packed_blocks,
    );

    if total_unseen > 0 {
        let update_queued_warning = |steps: &[PackStep], tail: &mut Vec<String>, evicted: bool| {
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
        let blocks = candidate.content.split_off(1);
        drop(candidate);
        candidate = build_candidate(
            &window_val,
            &bundle,
            &extra_kill_warnings,
            &footer,
            active_max_bytes,
            blocks,
        );
        while !bundle.steps.is_empty()
            && measure_outbound_len(delivery, &candidate)? > effective_cap
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
                } = &bundle.steps[0]
            {
                let msg = format!(
                    "attachment #{index} [{mime}] ({bytes} B, {base64_len} base64 B) exceeds the {outbound_limit} B outbound frame limit and was skipped; inspect or transfer {shown_path}"
                );
                bundle.steps[0] = PackStep::Degraded {
                    index: *index,
                    warning: msg,
                };
                candidate.content.truncate(1);
                bundle.lease = None;
            } else {
                if matches!(bundle.steps.pop(), Some(PackStep::Delivered { .. })) {
                    candidate.content.pop();
                }
                if bundle.steps.is_empty() {
                    bundle.lease = None;
                }
            }
            update_queued_warning(&bundle.steps, &mut bundle.tail_warnings, evicted_now);
            let blocks = candidate.content.split_off(1);
            drop(candidate);
            candidate = build_candidate(
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

    let mut lease = match admit_mcp_frame(
        ctx,
        delivery,
        action,
        &task,
        &candidate,
        &mut bundle.lease,
    ) {
        Ok(lease) => lease,
        Err(ServiceError::Overload(..)) if action == Action::Kill && !bundle.steps.is_empty() => {
            candidate.content.truncate(1);
            bundle.steps.clear();
            bundle.lease = None;
            bundle.tail_warnings = vec![
                "unseen attachment(s) deferred because active media memory is full; poll taskId without input to retrieve them".into(),
            ];
            let blocks = candidate.content.split_off(1);
            drop(candidate);
            candidate = build_candidate(
                &window_val,
                &bundle,
                &extra_kill_warnings,
                &footer,
                active_max_bytes,
                blocks,
            );
            admit_mcp_frame(ctx, delivery, action, &task, &candidate, &mut None)?
        }
        Err(err) => return Err(err),
    };
    drop(window_val);
    let (candidate, payload_owner) =
        retain_text_result(candidate, workspace, &mut lease, launch, &task)?;
    if let Some(warning) = startup_warning {
        warning.commit();
    }
    let _permit = permit;
    read.state.attachments_seen = bundle.steps.last().map_or(initial_seen, PackStep::index);
    let state = read.commit();
    task_reader.mark_delivered();
    commit_agent_report(ctx.session, &entry, &task_reader, snapshot, background);
    drop(state);
    let meta = TaskMeta {
        bare_task_id: task.id.clone(),
        log_path: crate::files::path::display_path(&task.log_path),
    };
    Ok((candidate, lease, Some(meta), payload_owner))
}

async fn render_transcript_agent(
    service: &Service,
    prepared: PreparedTranscript,
    ctx: &ExecContext<'_>,
    delivery: &AgentDelivery<'_>,
) -> Result<
    (
        CallToolResult,
        Option<Lease>,
        Option<TaskMeta>,
        crate::lease::Owner,
    ),
    ServiceError,
> {
    let read;
    let _permit;
    let workspace;
    let PreparedTranscript {
        workspace: w,
        permit: p,
        metadata,
        window,
        transcript_path,
        transcript_lines,
        final_lines,
        first_line,
        transcript_incomplete: _,
        transcript_error: _,
        transcript_warnings,
        task,
        entry,
        reader: task_reader,
        view,
        mark_session_entry: _,
        read: r,
    } = prepared;
    read = r;
    _permit = p;
    workspace = w;

    let launch = ctx.launch();
    let snapshot = &metadata.value;
    let transcript_note = format!(
        " · transcript {transcript_lines} lines, final through {final_lines}; this read starts at line {first_line} · {transcript_path}",
    );
    let mut status_with_log = status_line(&task, snapshot, &task_reader, true, None);
    status_with_log.push_str(&transcript_note);
    let mut status_no_log = status_line(&task, snapshot, &task_reader, false, None);
    status_no_log.push_str(&transcript_note);
    let mut warnings_list = task_warnings(snapshot);
    warnings_list.extend(transcript_warnings);
    if let Some(enc_warn) = encoding_warning(&window) {
        warnings_list.push(enc_warn);
    }
    let warnings = warning_text(&warnings_list);
    let background = background(service, ctx, &task, Action::Transcript)?;
    let footer = background
        .value
        .as_ref()
        .map_or(String::new(), |(line, _)| format!("\n{line}"));
    let text = layout(
        &window,
        StatusLines {
            with_log: status_with_log,
            without_log: status_no_log,
            snapshot,
            fresh: task_reader.is_fresh(),
        },
        &footer,
        view.max_bytes,
        "(no transcript lines since the last read)",
    );
    let candidate = success(text, task_io_ok(snapshot), &warnings);
    let mut lease = admit_mcp_frame(
        ctx,
        delivery,
        Action::Transcript,
        &task,
        &candidate,
        &mut None,
    )?;
    drop(window);
    let (candidate, payload_owner) =
        retain_text_result(candidate, workspace, &mut lease, launch, &task)?;
    let state = read.commit();
    task_reader.mark_delivered();
    commit_agent_report(ctx.session, &entry, &task_reader, snapshot, background);
    drop(state);
    let meta = TaskMeta {
        bare_task_id: task.id.clone(),
        log_path: crate::files::path::display_path(&task.log_path),
    };
    Ok((candidate, lease, Some(meta), payload_owner))
}

async fn render_screen_agent(
    service: &Service,
    prepared: PreparedScreen,
    ctx: &ExecContext<'_>,
    delivery: &AgentDelivery<'_>,
) -> Result<
    (
        CallToolResult,
        Option<Lease>,
        Option<TaskMeta>,
        crate::lease::Owner,
    ),
    ServiceError,
> {
    let read;
    let _permit;
    let workspace;
    let PreparedScreen {
        workspace: w,
        permit: p,
        metadata,
        screen_rows,
        cols,
        rows_count,
        cursor,
        window,
        action,
        task,
        entry,
        reader: task_reader,
        view,
        ended,
        mut startup_warning,
        mark_session_entry: _,
        read: r,
    } = prepared;
    read = r;
    _permit = p;
    workspace = w;

    let launch = ctx.launch();
    let snapshot = &metadata.value;
    let mut warnings_list = task_warnings(snapshot);
    if let Some(warning) = startup_warning.as_mut().and_then(ProbeWarning::message) {
        warnings_list.push(warning.to_owned());
    }
    if let Some(enc_warn) = encoding_warning(&window) {
        warnings_list.push(enc_warn);
    }
    let warnings = warning_text(&warnings_list);
    let background = background(service, ctx, &task, action)?;
    let footer = background
        .value
        .as_ref()
        .map_or(String::new(), |(line, _)| format!("\n{line}"));
    let (row, col) = (cursor.0 + 1, cursor.1 + 1);
    let screen_note = format!(
        "{} · screen {cols}x{rows_count}, cursor row {row} col {col}",
        wait_note(&ended, view.conditions),
    );
    let room = view.max_bytes;
    let full_len =
        screen_rows.iter().map(String::len).sum::<usize>() + screen_rows.len().saturating_sub(1);
    let need_log = needs_log_in_status(snapshot, 0, None) || full_len > room;
    let mut status = status_line(&task, snapshot, &task_reader, need_log, None);
    status.push_str(&screen_note);
    if task_reader.is_fresh() {
        status.push_str(NEW_READER_NOTE);
    }
    let (body, _, _) = output::fit_screen_rows(&screen_rows, room, true);
    let shown = if body.is_empty() && room >= "(blank screen)".len() {
        "(blank screen)"
    } else {
        &body
    };
    let text = format!("{shown}\n\n{status}{footer}");
    let candidate = success(text, task_io_ok(snapshot), &warnings);
    let mut lease = admit_mcp_frame(ctx, delivery, action, &task, &candidate, &mut None)?;
    drop(window);
    drop(body);
    drop(screen_rows);
    let (candidate, payload_owner) =
        retain_text_result(candidate, workspace, &mut lease, launch, &task)?;
    if let Some(warning) = startup_warning {
        warning.commit();
    }
    let state = read.commit();
    task_reader.mark_delivered();
    commit_agent_report(ctx.session, &entry, &task_reader, snapshot, background);
    drop(state);
    let meta = TaskMeta {
        bare_task_id: task.id.clone(),
        log_path: crate::files::path::display_path(&task.log_path),
    };
    Ok((candidate, lease, Some(meta), payload_owner))
}

fn render_list_agent(
    prepared: PreparedList,
    ctx: &ExecContext<'_>,
    delivery: &AgentDelivery<'_>,
) -> Result<
    (
        CallToolResult,
        Option<Lease>,
        Option<TaskMeta>,
        crate::lease::Owner,
    ),
    ServiceError,
> {
    let PreparedList {
        mut workspace,
        permit: _permit,
        total_count,
        items,
        parent_filter,
        state_filter,
        view,
    } = prepared;
    let launch = ctx.launch();
    let session = ctx.session;
    let mut blocks = Vec::with_capacity(items.len());
    for item in items {
        let running = item.snapshot.state == "running";
        let size = item.task.pty.then(|| item.task.size());
        let summary_text = item
            .children
            .as_ref()
            .and_then(|summary| format_child_summary(summary, running));
        let via_parent = if parent_filter.is_none() {
            item.parent_task_id.as_deref()
        } else {
            None
        };
        let top_row = TopRow::new(
            &item.task,
            &item.snapshot,
            size,
            summary_text.as_deref(),
            via_parent,
        );
        let mut block_lines = vec![top_row.full_line()];
        let mut delivered_tasks = Vec::new();
        let mut folded_finished = Vec::new();
        let mut folded_count = 0usize;
        if let Some(children) = item.children {
            folded_count = children.done + children.failed;
            folded_finished = children.folded_finished;
            delivered_tasks.reserve(1 + children.active_tasks.len());
            let parent_task_id_str = item.task.id.clone();
            let active_shown = children.active_tasks.len();
            let mut child_rows = Vec::with_capacity(active_shown);
            for (child, child_snapshot) in children.active_tasks {
                let child_size = child.pty.then(|| child.size());
                block_lines.push(format_child_row(&child, &child_snapshot, child_size));
                child_rows.push((child, child_snapshot));
            }
            if state_filter != Some(ListState::Finished) && children.active > active_shown {
                let more = children.active - active_shown;
                block_lines.push(format!(
                    "  └─ ... {more} more active; list with parentTaskId=\"{parent_task_id_str}\" state=\"active\""
                ));
            }
            delivered_tasks.push((item.task, item.snapshot));
            delivered_tasks.extend(child_rows);
        } else {
            delivered_tasks.push((item.task, item.snapshot));
        }
        blocks.push(ListBlock {
            top_row,
            lines: block_lines,
            delivered_tasks,
            folded_finished,
            folded_count,
        });
    }

    let (kept, text) = if blocks.is_empty() {
        (Vec::new(), "No tasks.".to_string())
    } else {
        fit_lines(&blocks, view.truncate, view.max_bytes)
    };
    let folded_children: usize = kept
        .iter()
        .map(|item| blocks[item.index].folded_count)
        .sum();
    let truncated = total_count > blocks.len();
    let omitted_by_budget = kept.len() < blocks.len();
    let shown = kept.len();
    let showing_note = match (truncated, folded_children > 0) {
        (true, true) if omitted_by_budget => Some(format!(
            "Showing {shown} of {total_count} top-level tasks ({folded_children} folded children; raise maxBytes or limit, or pass parentTaskId to inspect)."
        )),
        (true, true) => Some(format!(
            "Showing {shown} of {total_count} top-level tasks ({folded_children} folded children; pass parentTaskId to inspect)."
        )),
        (false, true) => Some(format!(
            "{folded_children} folded codemode children hidden; pass parentTaskId to inspect."
        )),
        (true, false) if omitted_by_budget => Some(format!(
            "Showing {shown} of {total_count} tasks; raise maxBytes or limit to see more."
        )),
        (true, false) => Some(format!(
            "Showing {shown} of {total_count} tasks; raise limit to see more."
        )),
        (false, false) => None,
    };
    let warnings: Vec<String> = kept
        .iter()
        .flat_map(|item| &blocks[item.index].delivered_tasks[..item.kept_tasks])
        .flat_map(|(task, snapshot)| {
            task_warnings(snapshot)
                .into_iter()
                .map(|warning| format!("{}: {warning}", task.id))
        })
        .collect();
    let mut suffix = warning_text(&warnings);
    if let Some(note) = showing_note {
        suffix.push('\n');
        suffix.push_str(&note);
    }
    let listed = success(text, true, &suffix);

    let outbound = measure_outbound_len(delivery, &listed)?;
    if outbound > launch.outbound_limit() {
        return Err(ServiceError::Overload(
            format!(
                "The list response was {outbound} bytes, over the frame limit; retry list with a smaller limit."
            ),
            None,
        ));
    }
    let payload = result_payload_capacity(&listed);
    let wire = if let Some(daemon_ipc) = &delivery.daemon_ipc {
        ipc_wire_len(daemon_ipc, &listed)?
    } else {
        outbound.saturating_mul(if delivery.http_response { 0 } else { 1 })
    };
    let retained = payload.saturating_add(wire);
    if retained > workspace.bytes() {
        resize_metadata_workspace(&mut workspace, retained, launch, Action::List)
            .map_err(|message| ServiceError::Overload(message, None))?;
    }
    if ctx.cancel.is_cancelled() {
        return Err(ServiceError::Cancelled(
            "Cancelled while listing tasks; retry list.".into(),
        ));
    }
    // Only an admitted candidate commits the fragments it actually displays.
    for item in kept {
        let block = &blocks[item.index];
        for (row_index, (task, snapshot)) in
            block.delivered_tasks[..item.kept_tasks].iter().enumerate()
        {
            let ack = if row_index == 0 {
                item.top_ack
            } else {
                full_ack(snapshot, false)
            };
            if let Some(entry) = session.existing(task) {
                if ack.root {
                    entry.mark_root_reported(snapshot);
                }
                if ack.tree_ended {
                    entry.mark_tree_end_reported();
                }
                if ack.keeps {
                    entry.mark_keeps_reported(snapshot);
                }
            }
        }
        if item.top_ack.summary {
            for (child, tree_ended) in &block.folded_finished {
                if let Some(entry) = session.existing(child) {
                    entry.mark_finished_reported(*tree_ended);
                }
            }
        }
    }
    drop(blocks);
    workspace.shrink_to(retained);
    let owner = crate::lease::Owner::new(workspace.split_off(payload));
    Ok((listed, Some(workspace), None, owner))
}

#[cfg(test)]
pub(crate) fn render_list_items_for_test(
    _service: &Service,
    items: Vec<crate::tasks::ListedTask>,
    max_bytes: usize,
    ctx: ExecContext<'_>,
) -> String {
    let prepared = PreparedList {
        total_count: items.len(),
        items,
        parent_filter: None,
        state_filter: None,
        view: crate::service::schema::View {
            inline_markers: false,
            keep_nul: false,
            truncate: Truncate::HeadTail,
            max_bytes,
            raw: false,
            encoding: None,
            screen: false,
            conditions: false,
        },
        workspace: Lease::empty(ctx.launch().memory()),
        permit: None,
    };
    let (result, _lease, _meta, _owner) =
        render_list_agent(prepared, &ctx, &AgentDelivery::default()).expect("test list render");
    result.content[0].as_text().unwrap().text.clone()
}

struct StatusLines<'a> {
    with_log: String,
    without_log: String,
    snapshot: &'a Snapshot,
    fresh: bool,
}

fn layout(
    window: &Window,
    status: StatusLines<'_>,
    footer: &str,
    max_bytes: usize,
    empty: &str,
) -> String {
    let StatusLines {
        with_log,
        without_log,
        snapshot,
        fresh,
    } = status;
    let cut_lines = window.cut_lines;
    let omitted = window.omitted;
    let mut status = if needs_log_in_status(snapshot, cut_lines, omitted) {
        with_log
    } else {
        without_log
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
    format!("{body}\n\n{status}{footer}")
}

#[derive(Clone, Copy, Default)]
struct RowAck {
    root: bool,
    tree_ended: bool,
    keeps: bool,
    summary: bool,
}

fn full_ack(snapshot: &Snapshot, has_summary: bool) -> RowAck {
    RowAck {
        root: true,
        tree_ended: snapshot.tree_ended,
        keeps: snapshot.kept && snapshot.held_known,
        summary: has_summary,
    }
}

struct KeptBlock {
    index: usize,
    kept_tasks: usize,
    top_ack: RowAck,
}

struct TopRow {
    head: String,
    tree_full: Option<String>,
    tree_brief: Option<String>,
    tree_count: Option<String>,
    elapsed_pty: String,
    summary: Option<String>,
    tail: String,
}

impl TopRow {
    fn new(
        task: &Task,
        snapshot: &Snapshot,
        size: Option<(u16, u16)>,
        summary: Option<&str>,
        via_parent: Option<&str>,
    ) -> Self {
        let code = snapshot
            .exit_code
            .map_or(String::new(), |code| format!(" {code}"));
        let head = format!("{} [{}{code}]", task.id, snapshot.state);
        let tree_full = tree_marker(task, snapshot);
        let (tree_brief, tree_count) =
            if snapshot.kept && snapshot.held_known && !snapshot.held.is_empty() {
                let first = &snapshot.held[0];
                let name =
                    clean_member_name(&first.name).unwrap_or_else(|| format!("pid {}", first.pid));
                let suffix = if snapshot.kill_error.is_some() {
                    " (kill failed)"
                } else {
                    ""
                };
                let brief = if snapshot.held.len() == 1 {
                    format!(" · keeps {name}{suffix}")
                } else {
                    format!(" · keeps {name} +{}{suffix}", snapshot.held.len() - 1)
                };
                let count = if snapshot.held.len() == 1 {
                    format!(" · keeps 1 process{suffix}")
                } else {
                    format!(" · keeps {} processes{suffix}", snapshot.held.len())
                };
                (Some(brief), Some(count))
            } else {
                (None, None)
            };
        let sep = if tree_full.is_some() { " · " } else { " " };
        let pty = match size {
            Some((cols, rows)) => format!(" pty {cols}x{rows}"),
            None => String::new(),
        };
        let elapsed_pty = format!("{sep}{:.1}s{pty}", snapshot.elapsed.as_secs_f64());
        let summary = summary.map(|summary| format!(" · {summary}"));
        let owner = match &task.started_by {
            Some((_, call)) => format!("{} call {call}", task.owner),
            None => task.owner.clone(),
        };
        let via = match via_parent {
            Some(pid) => format!(" · via {pid}"),
            None => String::new(),
        };
        let tail = format!(" · {owner}{via} · {}", format_command(&task.command));
        Self {
            head,
            tree_full,
            tree_brief,
            tree_count,
            elapsed_pty,
            summary,
            tail,
        }
    }

    fn full_line(&self) -> String {
        self.line_with_tree(self.tree_full.as_deref().unwrap_or(""))
    }

    fn line_with_tree(&self, tree: &str) -> String {
        format!(
            "{}{tree}{}{}{}",
            self.head,
            self.elapsed_pty,
            self.summary.as_deref().unwrap_or(""),
            self.tail
        )
    }
}

struct ListBlock {
    top_row: TopRow,
    lines: Vec<String>,
    delivered_tasks: Vec<(Arc<Task>, Snapshot)>,
    folded_finished: Vec<(Arc<Task>, bool)>,
    folded_count: usize,
}

impl ListBlock {
    fn full_top_ack(&self) -> RowAck {
        full_ack(&self.delivered_tasks[0].1, self.top_row.summary.is_some())
    }
}

fn format_command(command: &str) -> String {
    command
        .chars()
        .take(120)
        .map(|c| {
            if c.is_control() || matches!(c, '\u{2028}' | '\u{2029}') {
                ' '
            } else {
                c
            }
        })
        .collect()
}

fn tree_marker(task: &Task, snapshot: &Snapshot) -> Option<String> {
    if snapshot.state == "running" {
        return task.keep.then(|| " · keepTree".into());
    }
    if snapshot.kept {
        let kill_failed = snapshot.kill_error.is_some();
        let body = if !snapshot.held_known {
            if kill_failed {
                "processes (unconfirmed, kill failed)".into()
            } else {
                "processes (unconfirmed)".into()
            }
        } else if kill_failed {
            format!("{} (kill failed)", members(&snapshot.held))
        } else {
            members(&snapshot.held)
        };
        return Some(format!(" · keeps {body}"));
    }
    if snapshot.tree_ended {
        return Some(" · kept processes ended".into());
    }
    task.keep.then(|| " · keepTree".into())
}

fn format_child_summary(summary: &ChildSummary, running: bool) -> Option<String> {
    if !summary.should_show(running) {
        return None;
    }
    if summary.total == 0 {
        return Some(if running {
            format!("{} queued", summary.queued)
        } else {
            format!("{} cancelled", summary.cancelled)
        });
    }
    let mut parts = Vec::new();
    if summary.done > 0 {
        parts.push(format!("{} done", summary.done));
    }
    if summary.failed > 0 {
        parts.push(format!("{} failed", summary.failed));
    }
    if summary.warned > 0 {
        parts.push(format!("{} warned", summary.warned));
    }
    if summary.tree_ended > 0 {
        parts.push(format!("{} kept ended", summary.tree_ended));
    }
    if summary.active > 0 {
        parts.push(format!("{} active", summary.active));
    }
    if running && summary.queued > 0 {
        parts.push(format!("{} queued", summary.queued));
    }
    if !running && summary.cancelled > 0 {
        parts.push(format!("{} cancelled", summary.cancelled));
    }
    let noun = if summary.total == 1 { "task" } else { "tasks" };
    Some(format!("{} {noun} ({})", summary.total, parts.join(", ")))
}

fn format_child_row(task: &Task, snapshot: &Snapshot, size: Option<(u16, u16)>) -> String {
    let code = snapshot
        .exit_code
        .map_or(String::new(), |code| format!(" {code}"));
    let tree = tree_marker(task, snapshot);
    let sep = if tree.is_some() { " · " } else { " " };
    let pty = match size {
        Some((cols, rows)) => format!(" pty {cols}x{rows}"),
        None => String::new(),
    };
    format!(
        "  └─ {} [{}{code}]{}{sep}{:.1}s{pty} · {}",
        task.id,
        snapshot.state,
        tree.as_deref().unwrap_or(""),
        snapshot.elapsed.as_secs_f64(),
        format_command(&task.command),
    )
}

fn cut_candidate(mut candidate_full: String, budget: usize) -> String {
    const MARK: &str = " … [cut]";
    if candidate_full.len() + MARK.len() <= budget {
        candidate_full.push_str(MARK);
    } else {
        let mut end = budget - MARK.len();
        while !candidate_full.is_char_boundary(end) {
            end -= 1;
        }
        candidate_full.truncate(end);
        candidate_full.push_str(MARK);
    }
    candidate_full
}

fn fit_row(row: &TopRow, snapshot: &Snapshot, budget: usize) -> (String, RowAck) {
    const MARK_LEN: usize = " … [cut]".len();
    let full_line = row.full_line();
    let has_summary = row.summary.is_some();
    if full_line.len() <= budget {
        return (full_line, full_ack(snapshot, has_summary));
    }
    if budget >= 20 {
        let head_len = row.head.len();
        let tree_len = row.tree_full.as_deref().map_or(0, str::len);
        let elapsed_len = row.elapsed_pty.len();
        let summary_len = row.summary.as_deref().map_or(0, str::len);
        let keeps_full = snapshot.kept && snapshot.held_known;

        // Tier 1: head + tree_full + elapsed_pty + summary.
        if head_len + tree_len + elapsed_len + summary_len + MARK_LEN <= budget {
            return (
                cut_candidate(full_line, budget),
                full_ack(snapshot, has_summary),
            );
        }
        // Tier 2: head + tree_full + elapsed_pty (drop summary ack).
        if has_summary && head_len + tree_len + elapsed_len + MARK_LEN <= budget {
            return (
                cut_candidate(full_line, budget),
                RowAck {
                    root: true,
                    tree_ended: snapshot.tree_ended,
                    keeps: keeps_full,
                    summary: false,
                },
            );
        }
        // Tier 3: head + tree_full.
        if row.tree_full.is_some() && head_len + tree_len + MARK_LEN <= budget {
            return (
                cut_candidate(full_line, budget),
                RowAck {
                    root: true,
                    tree_ended: snapshot.tree_ended,
                    keeps: keeps_full,
                    summary: false,
                },
            );
        }
        // Tier 4: head + tree_brief (same granularity as footer brief; confirms keeps).
        if let Some(tree_brief) = &row.tree_brief
            && head_len + tree_brief.len() + MARK_LEN <= budget
        {
            return (
                cut_candidate(row.line_with_tree(tree_brief), budget),
                RowAck {
                    root: true,
                    tree_ended: false,
                    keeps: true,
                    summary: false,
                },
            );
        }
        // Tier 5: head + tree_count (count only; keeps stays unconfirmed).
        if let Some(tree_count) = &row.tree_count
            && head_len + tree_count.len() + MARK_LEN <= budget
        {
            return (
                cut_candidate(row.line_with_tree(tree_count), budget),
                RowAck {
                    root: true,
                    tree_ended: false,
                    keeps: false,
                    summary: false,
                },
            );
        }
        // Tier 6: head only.
        if head_len + MARK_LEN <= budget {
            return (
                cut_candidate(full_line, budget),
                RowAck {
                    root: true,
                    tree_ended: false,
                    keeps: false,
                    summary: false,
                },
            );
        }
    }
    // Tier 7: hard cut, confirm nothing.
    let mut line = full_line;
    output::cut_to(&mut line, budget + 1);
    (line, RowAck::default())
}

fn block_len(lines: &[String]) -> usize {
    lines.iter().map(String::len).sum::<usize>() + lines.len().saturating_sub(1)
}

fn fit_single_block(block: &ListBlock, budget: usize) -> (usize, RowAck, String) {
    let mut kept_lines = block.lines.len();
    while kept_lines > 1 && block_len(&block.lines[..kept_lines]) > budget {
        kept_lines -= 1;
    }
    let kept_tasks = kept_lines.min(block.delivered_tasks.len());
    if kept_lines == 1 && block.lines[0].len() > budget {
        let (line, ack) = fit_row(&block.top_row, &block.delivered_tasks[0].1, budget);
        return (kept_tasks, ack, line);
    }
    (
        kept_tasks,
        block.full_top_ack(),
        block.lines[..kept_lines].join("\n"),
    )
}

fn fit_lines(blocks: &[ListBlock], truncate: Truncate, budget: usize) -> (Vec<KeptBlock>, String) {
    let total_bytes: usize = blocks
        .iter()
        .flat_map(|block| &block.lines)
        .map(|line| line.len() + 1)
        .sum();
    if total_bytes <= budget + 1 {
        let kept = blocks
            .iter()
            .enumerate()
            .map(|(index, block)| KeptBlock {
                index,
                kept_tasks: block.delivered_tasks.len(),
                top_ack: block.full_top_ack(),
            })
            .collect();
        let text = blocks
            .iter()
            .flat_map(|block| block.lines.iter().map(String::as_str))
            .collect::<Vec<_>>()
            .join("\n");
        return (kept, text);
    }
    if blocks.len() == 1 {
        let (kept_tasks, top_ack, text) = fit_single_block(&blocks[0], budget);
        return (
            vec![KeptBlock {
                index: 0,
                kept_tasks,
                top_ack,
            }],
            text,
        );
    }
    let marker =
        |omitted: usize| format!("... [{omitted} tasks omitted; raise maxBytes to see them] ...");
    let mut room = budget.saturating_sub(marker(blocks.len()).len());
    let mut head = 0;
    let mut tail = blocks.len();
    let mut want_head = truncate != Truncate::Tail;
    let mut want_tail = truncate != Truncate::Head;
    while head < tail && (want_head || want_tail) {
        if want_head {
            let cost = block_len(&blocks[head].lines) + 1;
            if cost <= room {
                head += 1;
                room -= cost;
            } else {
                want_head = false;
            }
        }
        if want_tail && head < tail {
            let cost = block_len(&blocks[tail - 1].lines) + 1;
            if cost <= room {
                tail -= 1;
                room -= cost;
            } else {
                want_tail = false;
            }
        }
    }
    if head == 0 && tail == blocks.len() {
        let edge = if truncate == Truncate::Tail {
            blocks.len() - 1
        } else {
            0
        };
        let note = marker(blocks.len() - 1);
        let edge_budget = budget.saturating_sub(note.len() + 1);
        if blocks[edge].lines.len() > 1 && blocks[edge].lines[0].len() <= edge_budget {
            let (kept_tasks, top_ack, trimmed) = fit_single_block(&blocks[edge], edge_budget);
            let text = if truncate == Truncate::Tail {
                format!("{note}\n{trimmed}")
            } else {
                format!("{trimmed}\n{note}")
            };
            return (
                vec![KeptBlock {
                    index: edge,
                    kept_tasks,
                    top_ack,
                }],
                text,
            );
        }
    }
    let mut note = marker(tail - head);
    output::cut_to(&mut note, budget + 1);
    let text = blocks[..head]
        .iter()
        .flat_map(|block| block.lines.iter().map(String::as_str))
        .chain([note.as_str()])
        .chain(
            blocks[tail..]
                .iter()
                .flat_map(|block| block.lines.iter().map(String::as_str)),
        )
        .collect::<Vec<_>>()
        .join("\n");
    let kept = (0..head)
        .chain(tail..blocks.len())
        .map(|index| KeptBlock {
            index,
            kept_tasks: blocks[index].delivered_tasks.len(),
            top_ack: blocks[index].full_top_ack(),
        })
        .collect();
    (kept, text)
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
            crate::files::path::display_path(&task.log_path)
        ));
    }
    if snapshot.state == "running" {
        line.push_str(" · poll to continue, kill to stop");
    }
    if snapshot.expired {
        line.push_str(" · killed by killAfterMs");
    }
    if snapshot.kept {
        let names = held_members_text(snapshot);
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
                report.cancelled_calls.len()
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
        if snapshot.state != "running" && report.running.len() > 3 {
            line.push_str(&format!(
                "; list active with exec list parentTaskId=\"{}\" state=\"active\"",
                task.id
            ));
        }
    }
    line
}

fn held_members_text(snapshot: &Snapshot) -> String {
    if !snapshot.held_known {
        "processes (unconfirmed)".into()
    } else {
        members(&snapshot.held)
    }
}

/// Names processes as `adb.exe (4120), sleep (77) and 3 more`.
fn members(members: &[Member]) -> String {
    if members.is_empty() {
        return "processes".into();
    }
    let mut text = members
        .iter()
        .take(3)
        .map(|member| match clean_member_name(&member.name) {
            Some(clean) => format!("{clean} ({})", member.pid),
            None => format!("pid {}", member.pid),
        })
        .collect::<Vec<_>>()
        .join(", ");
    if members.len() > 3 {
        text.push_str(&format!(" and {} more", members.len() - 3));
    }
    text
}

fn rpc_result_len(delivery: &AgentDelivery<'_>, result: &CallToolResult) -> Result<usize, String> {
    crate::lease::json_len(result)
        .map(|n| n.saturating_add(delivery.rpc_id_bytes).saturating_add(256))
        .map_err(|err| format!("Cannot measure result JSON: {err}."))
}

fn ipc_wire_len(daemon_ipc: &DaemonDelivery<'_>, result: &CallToolResult) -> Result<usize, String> {
    crate::lease::json_len(result)
        .map(|rpc_body| {
            rpc_body
                .saturating_add(daemon_ipc.ipc_body_overhead)
                .saturating_add(4)
        })
        .map_err(|err| format!("Cannot measure result JSON: {err}."))
}

fn local_wire_len(delivery: &AgentDelivery<'_>, result: &CallToolResult) -> Result<usize, String> {
    if let Some(daemon_ipc) = &delivery.daemon_ipc {
        ipc_wire_len(daemon_ipc, result)
    } else {
        rpc_result_len(delivery, result)
    }
}

fn measure_outbound_len(
    delivery: &AgentDelivery<'_>,
    result: &CallToolResult,
) -> Result<usize, String> {
    let rpc_body = crate::lease::json_len(result)
        .map_err(|err| format!("Cannot measure result JSON: {err}."))?;
    let rpc_bound = rpc_body
        .saturating_add(delivery.rpc_id_bytes)
        .saturating_add(256);
    if let Some(daemon_ipc) = &delivery.daemon_ipc {
        let ipc_body = rpc_body.saturating_add(daemon_ipc.ipc_body_overhead);
        Ok(rpc_bound.max(ipc_body))
    } else {
        Ok(rpc_bound)
    }
}

enum PlannedAttachment {
    Select(tasks::state::AttachmentLease),
    Degraded { index: usize, warning: String },
}

enum PackStep {
    Delivered {
        index: usize,
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

#[derive(Default)]
struct AttachmentBundle {
    steps: Vec<PackStep>,
    tail_warnings: Vec<String>,
    lease: Option<Lease>,
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

/// Formats complete warnings outside the body budget.
fn warning_text(warnings: &[String]) -> String {
    let mut text = String::new();
    for warning in warnings {
        if text.is_empty() {
            text.push('\n');
        }
        text.push_str("\nWarning: ");
        text.push_str(warning);
    }
    text
}

fn success(mut text: String, io_ok: bool, suffix: &str) -> CallToolResult {
    text.push_str(suffix);
    let content = vec![ContentBlock::text(text)];
    if io_ok {
        CallToolResult::success(content)
    } else {
        CallToolResult::error(content)
    }
}

/// The status line note for a wait a condition ended, or that reached `waitMs` with
/// `returnWhen` conditions (`conditions`) none of which held.
fn wait_note(ended: &WaitEnd, conditions: bool) -> String {
    let quote = |text: &str| {
        let mut short: String = text.chars().take(NOTE_CHARS).collect();
        if short.len() < text.len() {
            short.push('…');
        }
        format!("{short:?}")
    };
    match ended {
        WaitEnd::OutputContains(text, _) => format!(" · wait: output matched {}", quote(text)),
        WaitEnd::ScreenContains(text) => format!(" · wait: screen matched {}", quote(text)),
        WaitEnd::ScreenAppears(text) => {
            format!(" · wait: screen text appeared {}", quote(text))
        }
        WaitEnd::ScreenGone(text) => format!(" · wait: screen text gone {}", quote(text)),
        WaitEnd::Quiet => " · wait: output quiet".into(),
        WaitEnd::MaxWait if conditions => " · wait: waitMs elapsed".into(),
        WaitEnd::TaskEnded | WaitEnd::TreeEnded | WaitEnd::MaxWait => String::new(),
    }
}
