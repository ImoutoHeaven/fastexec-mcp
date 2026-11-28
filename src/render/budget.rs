//! Payload capacity accounting, outbound frame admission, attachment packing types,
//! and shared formatting helpers for agent rendering.

use super::types::{AgentDelivery, DaemonDelivery};
use crate::env::LaunchContext;
use crate::lease::Lease;
use crate::platform::members::Member;
use crate::platform::process::clean_member_name;
use crate::service::ExecContext;
use crate::service::core::{ServiceError, TaskMeta, overload_task_message};
use crate::service::schema::Action;
use crate::tasks::state::{AttachmentLease, Snapshot, Task};
use crate::wait::WaitEnd;
use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::Value;

const NOTE_CHARS: usize = 60;

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

pub(super) fn retain_text_result(
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

pub(super) fn admit_mcp_frame(
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

pub(super) fn held_members_text(snapshot: &Snapshot) -> String {
    if !snapshot.held_known {
        "processes (unconfirmed)".into()
    } else {
        members(&snapshot.held)
    }
}

/// Names processes as `adb.exe (4120), sleep (77) and 3 more`.
pub(super) fn members(members: &[Member]) -> String {
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

pub(super) fn ipc_wire_len(
    daemon_ipc: &DaemonDelivery<'_>,
    result: &CallToolResult,
) -> Result<usize, String> {
    crate::lease::json_len(result)
        .map(|rpc_body| {
            rpc_body
                .saturating_add(daemon_ipc.ipc_body_overhead)
                .saturating_add(4)
        })
        .map_err(|err| format!("Cannot measure result JSON: {err}."))
}

pub(super) fn local_wire_len(
    delivery: &AgentDelivery<'_>,
    result: &CallToolResult,
) -> Result<usize, String> {
    if let Some(daemon_ipc) = &delivery.daemon_ipc {
        ipc_wire_len(daemon_ipc, result)
    } else {
        rpc_result_len(delivery, result)
    }
}

pub(super) fn measure_outbound_len(
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

pub(super) enum PlannedAttachment {
    Select(AttachmentLease),
    Degraded { index: usize, warning: String },
}

pub(super) enum PackStep {
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
    pub(super) fn index(&self) -> usize {
        match self {
            Self::Delivered { index, .. } | Self::Degraded { index, .. } => *index,
        }
    }
}

#[derive(Default)]
pub(super) struct AttachmentBundle {
    pub(super) steps: Vec<PackStep>,
    pub(super) tail_warnings: Vec<String>,
    pub(super) lease: Option<Lease>,
}

/// Formats complete warnings outside the body budget.
pub(super) fn warning_text(warnings: &[String]) -> String {
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

pub(super) fn success(mut text: String, io_ok: bool, suffix: &str) -> CallToolResult {
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
pub(super) fn wait_note(ended: &WaitEnd, conditions: bool) -> String {
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
