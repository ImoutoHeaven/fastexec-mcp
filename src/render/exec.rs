//! Agent `exec` entrypoints and window/transcript/screen rendering.

use super::budget::{
    AttachmentBundle, PackStep, PlannedAttachment, admit_mcp_frame, held_members_text,
    local_wire_len, measure_outbound_len, members, result_payload_capacity, retain_text_result,
    success, wait_note, warning_text,
};
use super::list::render_list_agent;
use super::types::{AgentDelivery, TrackedResult, error_result, media_content_block, schema_error};
use crate::env::ProbeWarning;
use crate::lease::Lease;
use crate::media;
use crate::service::core::{
    PreparedSnapshot, ScriptReport, ServiceError, TaskMeta, encoding_warning, kill_pending_note,
    metadata_members, metadata_workspace_bytes, output_incomplete, overload_task_message,
    resize_metadata_workspace, task_io_ok, task_warnings,
};
use crate::service::schema::{Action, ExecPolicy, Request};
use crate::service::{
    Consumer, ExecContext, PreparedExec, PreparedScreen, PreparedTranscript, PreparedWindow,
    Service, StagedRead,
};
use crate::tasks;
use crate::tasks::session::{OutsideClaim, Reader, Session, TaskEntry};
use crate::tasks::state::{Snapshot, Task};
use crate::term::output::{self, Window, WindowSpec};
use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::Value;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;

const NEW_READER_NOTE: &str = " · new reader";
/// Largest background footer.
const FOOTER_LIMIT: usize = 512;

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

// Footer text and shown snapshots stay owned until their report marks are committed.
// `claim` drops last so rollback releases `_workspace` before unlocking `outside_consumed`.
pub(super) struct Background<'a> {
    pub(super) value: Option<(String, Vec<tasks::Shown>)>,
    external_cursor: Option<u64>,
    event_bus: Option<Arc<crate::events::EventBus>>,
    _workspace: Lease,
    pub(super) claim: OutsideClaim<'a>,
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

pub(super) fn background<'a>(
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
