//! Programmatic encoder for nested `codemode` tool calls: typed `Serialize` DTOs, attachment
//! handle registration with rollback, and single-peak bridge memory admission.

use super::bridge::BRIDGE_LIMIT;
use super::control::{CallInfo, Outcome, ScriptHandle};
use crate::files::{
    self, EditError, EditFailure, EditMatch, FileEditData, FileError, FileReadData, FileWriteData,
    MediaPayload,
};
use crate::lease::{Lease, Owner};
use crate::platform::process;
use crate::service::render::{
    encoding_warning, kill_pending_note, output_incomplete, task_io_ok, task_warnings,
};
use crate::service::schema::Action;
use crate::service::{
    ExecContext, PreparedExec, PreparedList, PreparedScreen, PreparedTranscript, PreparedWindow,
    ProgramDelivery, ServiceError,
};
use crate::session::{LaunchContext, ProbeWarning};
use crate::tasks::state::{Snapshot, Task};
use crate::term::output;
use crate::wait::WaitEnd;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

/// Outcome of encoding a Program tool call, tracking whether `script.reserve_result` was
/// already called inside `admit_bridge_result`.
pub(crate) struct ProgramOutcome {
    pub(crate) outcome: Outcome,
    pub(crate) pre_reserved: bool,
}

impl ProgramOutcome {
    pub(crate) fn value(text: String, lease: Lease) -> Self {
        Self {
            outcome: Outcome::Value(crate::ipc::Owned::new(text, Owner::new(lease))),
            pre_reserved: true,
        }
    }

    pub(crate) fn limit() -> Self {
        Self {
            outcome: Outcome::Limit,
            pre_reserved: false,
        }
    }

    pub(crate) fn memory(task_meta: Option<(String, String)>) -> Self {
        Self {
            outcome: Outcome::Memory(task_meta),
            pre_reserved: false,
        }
    }

    pub(crate) fn error(message: &'static str, pre_reserved: bool) -> Self {
        Self {
            outcome: Outcome::Error(message),
            pre_reserved,
        }
    }

    pub(crate) fn cancelled(message: &'static str) -> Self {
        Self {
            outcome: Outcome::Cancelled(message),
            pre_reserved: false,
        }
    }
}

/// Registers opaque attachment handles before final admission; drops unregister them unless disarmed.
struct NestedAttachments<'a> {
    script: &'a ScriptHandle,
    ids: Vec<u64>,
}

impl<'a> NestedAttachments<'a> {
    fn new(script: &'a ScriptHandle) -> Self {
        Self {
            script,
            ids: Vec::new(),
        }
    }

    fn disarm(&mut self) {
        self.ids.clear();
    }
}

impl Drop for NestedAttachments<'_> {
    fn drop(&mut self) {
        for id in self.ids.drain(..) {
            self.script.unregister_attachment(id);
        }
    }
}

#[derive(Serialize)]
struct OkEnvelope<'a, T: Serialize> {
    ok: bool,
    value: &'a T,
}

#[derive(Serialize)]
struct ErrEnvelope<'a, R: Serialize> {
    ok: bool,
    code: &'static str,
    error: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<&'a R>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TaskMetaDto<'a> {
    task_id: &'a str,
    log_path: &'a str,
}

#[derive(Serialize)]
struct MemberDto<'a> {
    pid: u32,
    name: &'a str,
}

fn members_dto(members: &[process::Member]) -> Vec<MemberDto<'_>> {
    members
        .iter()
        .map(|m| MemberDto {
            pid: m.pid,
            name: &m.name,
        })
        .collect()
}

#[derive(Serialize)]
struct WaitEndedByDto<'a> {
    condition: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<&'a str>,
}

fn wait_ended_dto(ended: &WaitEnd) -> WaitEndedByDto<'_> {
    let (condition, text) = ended.parts();
    WaitEndedByDto { condition, text }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StartedTaskDto<'a> {
    task_id: &'a str,
    state: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NestedAttachmentDto {
    index: usize,
    kind: &'static str,
    mime_type: String,
    bytes: u64,
    #[serde(rename = "__hid")]
    hid: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CodemodeDto<'a> {
    #[serde(skip_serializing_if = "is_empty_slice")]
    cancelled_calls: &'a [CallInfo],
    #[serde(skip_serializing_if = "Vec::is_empty")]
    started_tasks: Vec<StartedTaskDto<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    attachments: Vec<NestedAttachmentDto>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExecWindowDto<'a> {
    action: &'static str,
    task_id: String,
    kind: &'static str,
    state: &'static str,
    exit_code: Option<i32>,
    pty: bool,
    elapsed_ms: u64,
    output: &'a str,
    truncated: bool,
    log_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    wait_ended_by: Option<WaitEndedByDto<'a>>,
    #[serde(skip_serializing_if = "is_zero_u64")]
    omitted_lines: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    omitted_range: Option<[u64; 2]>,
    #[serde(skip_serializing_if = "is_zero_u64")]
    cut_lines: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    cols: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rows: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<[u16; 2]>,
    #[serde(skip_serializing_if = "is_zero_usize")]
    omitted_rows: usize,
    #[serde(skip_serializing_if = "is_false")]
    new_reader: bool,
    #[serde(skip_serializing_if = "is_false")]
    keep_tree: bool,
    #[serde(skip_serializing_if = "is_false")]
    kept_running: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    held: Vec<MemberDto<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    leftovers: Vec<MemberDto<'a>>,
    #[serde(skip_serializing_if = "is_false")]
    lifetime_expired: bool,
    #[serde(skip_serializing_if = "is_false")]
    forced_end: bool,
    #[serde(skip_serializing_if = "is_false")]
    output_open: bool,
    #[serde(skip_serializing_if = "is_false")]
    output_incomplete: bool,
    #[serde(skip_serializing_if = "is_zero_u64")]
    dropped_bytes: u64,
    #[serde(skip_serializing_if = "is_false")]
    log_evicted: bool,
    #[serde(skip_serializing_if = "is_zero_u64")]
    encoding_errors: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    capture_error: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    log_error: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_warning: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_error: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kill_error: Option<&'a str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    codemode: Option<CodemodeDto<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExecTranscriptDto<'a> {
    action: &'static str,
    task_id: String,
    kind: &'static str,
    state: &'static str,
    exit_code: Option<i32>,
    pty: bool,
    elapsed_ms: u64,
    output: &'a str,
    truncated: bool,
    log_path: String,
    transcript_path: &'a str,
    transcript_lines: u64,
    final_lines: u64,
    first_line: u64,
    #[serde(skip_serializing_if = "is_zero_u64")]
    omitted_lines: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    omitted_range: Option<[u64; 2]>,
    #[serde(skip_serializing_if = "is_zero_u64")]
    cut_lines: u64,
    #[serde(skip_serializing_if = "is_false")]
    new_reader: bool,
    #[serde(skip_serializing_if = "is_false")]
    transcript_incomplete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    transcript_error: Option<&'a str>,
    #[serde(skip_serializing_if = "is_zero_u64")]
    encoding_errors: u64,
    #[serde(skip_serializing_if = "is_false")]
    keep_tree: bool,
    #[serde(skip_serializing_if = "is_false")]
    kept_running: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    held: Vec<MemberDto<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    leftovers: Vec<MemberDto<'a>>,
    #[serde(skip_serializing_if = "is_false")]
    lifetime_expired: bool,
    #[serde(skip_serializing_if = "is_false")]
    forced_end: bool,
    #[serde(skip_serializing_if = "is_false")]
    output_open: bool,
    #[serde(skip_serializing_if = "is_false")]
    output_incomplete: bool,
    #[serde(skip_serializing_if = "is_zero_u64")]
    dropped_bytes: u64,
    #[serde(skip_serializing_if = "is_false")]
    log_evicted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    capture_error: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    log_error: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_warning: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_error: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kill_error: Option<&'a str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ListChildrenDto<'a> {
    total: usize,
    #[serde(skip_serializing_if = "is_zero_usize")]
    done: usize,
    #[serde(skip_serializing_if = "is_zero_usize")]
    failed: usize,
    #[serde(skip_serializing_if = "is_zero_usize")]
    warned: usize,
    #[serde(skip_serializing_if = "is_zero_usize")]
    active: usize,
    #[serde(skip_serializing_if = "is_zero_usize")]
    queued: usize,
    #[serde(skip_serializing_if = "is_zero_usize")]
    cancelled: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    active_tasks: Vec<ListTaskDto<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ListTaskDto<'a> {
    kind: &'static str,
    task_id: &'a str,
    state: &'static str,
    exit_code: Option<i32>,
    pty: bool,
    elapsed_ms: u64,
    command: String,
    log_path: String,
    owner: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cols: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rows: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    session: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    call_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    parent_task_id: Option<&'a str>,
    #[serde(skip_serializing_if = "is_false")]
    keep_tree: bool,
    #[serde(skip_serializing_if = "is_false")]
    kept_running: bool,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    held: Vec<MemberDto<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    leftovers: Vec<MemberDto<'a>>,
    #[serde(skip_serializing_if = "is_false")]
    lifetime_expired: bool,
    #[serde(skip_serializing_if = "is_false")]
    forced_end: bool,
    #[serde(skip_serializing_if = "is_false")]
    output_open: bool,
    #[serde(skip_serializing_if = "is_false")]
    output_incomplete: bool,
    #[serde(skip_serializing_if = "is_zero_u64")]
    dropped_bytes: u64,
    #[serde(skip_serializing_if = "is_false")]
    log_evicted: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    capture_error: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    log_error: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_warning: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_error: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kill_error: Option<&'a str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    children: Option<ListChildrenDto<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExecListDto<'a> {
    action: &'static str,
    total_count: usize,
    tasks: Vec<ListTaskDto<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReadTextDto<'a> {
    kind: &'static str,
    path: &'a str,
    text: &'a str,
    start_line: u64,
    end_line: Option<u64>,
    shown_lines: u64,
    total_lines: u64,
    truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_offset: Option<u64>,
    #[serde(skip_serializing_if = "is_false")]
    line_too_long: bool,
    #[serde(skip_serializing_if = "is_false")]
    lossy: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ReadMediaDto<'a> {
    kind: &'static str,
    path: &'a str,
    mime_type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_bytes: Option<u64>,
    attachable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    note: Option<&'a str>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WriteDto<'a> {
    path: &'a str,
    bytes: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EditDto<'a> {
    path: &'a str,
    replacements: usize,
    edits: &'a [EditMatch],
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct EditFailedDto<'a> {
    path: &'a str,
    failures: &'a [EditFailure],
}

fn is_false(v: &bool) -> bool {
    !*v
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

fn is_zero_usize(v: &usize) -> bool {
    *v == 0
}

fn is_empty_slice<T>(v: &[T]) -> bool {
    v.is_empty()
}

fn action_str(action: Action) -> &'static str {
    match action {
        Action::Start => "start",
        Action::Poll => "poll",
        Action::Kill => "kill",
        Action::List => "list",
        Action::Transcript => "transcript",
    }
}

fn io_failure_message(snapshot: &Snapshot) -> String {
    let mut parts = Vec::new();
    if let Some(err) = &snapshot.input_error {
        parts.push(err.clone());
    }
    if let Some(err) = &snapshot.capture_error {
        parts.push(format!("output capture failed: {err}"));
    }
    if let Some(err) = &snapshot.log_error {
        parts.push(format!("log write failed: {err}"));
    }
    if parts.is_empty() {
        "Tool operation failed.".into()
    } else {
        parts.join("; ")
    }
}

fn list_task_dto<'a>(
    task: &'a Task,
    snapshot: &'a Snapshot,
    size: Option<(u16, u16)>,
    parent_task_id: Option<&'a str>,
    children: Option<ListChildrenDto<'a>>,
) -> ListTaskDto<'a> {
    ListTaskDto {
        kind: if task.is_script() { "codemode" } else { "bash" },
        task_id: &task.id,
        state: snapshot.state,
        exit_code: snapshot.exit_code,
        pty: task.pty,
        elapsed_ms: snapshot.elapsed.as_millis() as u64,
        command: task.command.chars().take(120).collect(),
        log_path: files::path::display_path(&task.log_path),
        owner: &task.owner,
        cols: size.map(|(cols, _)| cols),
        rows: size.map(|(_, rows)| rows),
        session: task.started_by.as_ref().map(|(s, _)| s.as_str()),
        call_id: task.started_by.as_ref().map(|(_, id)| *id),
        parent_task_id,
        keep_tree: task.keep,
        kept_running: snapshot.kept,
        held: members_dto(&snapshot.held),
        leftovers: members_dto(&snapshot.leftovers),
        lifetime_expired: snapshot.expired,
        forced_end: snapshot.forced_end,
        output_open: snapshot.output_open,
        output_incomplete: output_incomplete(snapshot),
        dropped_bytes: snapshot.dropped,
        log_evicted: snapshot.evicted,
        capture_error: snapshot.capture_error.as_deref(),
        log_error: snapshot.log_error.as_deref(),
        input_warning: snapshot.input_warning.as_deref(),
        input_error: snapshot.input_error.as_deref(),
        kill_error: snapshot.kill_error.as_deref(),
        warnings: task_warnings(snapshot),
        children,
    }
}

/// Single-peak bridge admission:
/// 1. Measure `wire_len` without allocating and verify `<= BRIDGE_LIMIT`.
/// 2. Resize existing held lease in place to `max(held, retained_data + wire_len)` (falling back
///    to `control_memory()` for `Kill`/`List`).
/// 3. Reserve `wire_len` on `script.reserve_result(pending_id, wire_len)` once.
/// 4. Serialize into `Vec::with_capacity(wire_len)` and return `(text, lease)` so the caller can
///    commit state and drop raw data BEFORE calling `lease.shrink_to(text.capacity())`.
fn admit_bridge_result<T: Serialize>(
    envelope: &T,
    held_lease: Option<Lease>,
    retained_data: usize,
    launch: &LaunchContext,
    delivery: &ProgramDelivery<'_>,
    cancel: Option<&CancellationToken>,
    task_meta: Option<(String, String)>,
) -> Result<(String, Lease), ProgramOutcome> {
    let Ok(wire_len) = crate::ipc::json_len(envelope) else {
        return Err(ProgramOutcome::error(
            "Cannot measure the nested tool result.",
            false,
        ));
    };
    if wire_len > BRIDGE_LIMIT {
        return Err(ProgramOutcome::limit());
    }
    let Some(peak) = retained_data.checked_add(wire_len) else {
        return Err(ProgramOutcome::memory(task_meta));
    };
    let lease = match held_lease {
        Some(mut existing) => {
            let target = existing.bytes().max(peak);
            if target > existing.bytes() && !existing.try_grow(target - existing.bytes()) {
                let replacement = launch.memory().try_acquire(target).or_else(|| {
                    (delivery.is_control && target <= crate::lease::CONTROL_LIMIT)
                        .then(|| launch.control_memory().try_acquire(target))
                        .flatten()
                });
                let Some(replacement) = replacement else {
                    return Err(ProgramOutcome::memory(task_meta));
                };
                existing = replacement;
            }
            existing
        }
        None => {
            let target = peak.max(128);
            let acquired = launch.memory().try_acquire(target).or_else(|| {
                (delivery.is_control && target <= crate::lease::CONTROL_LIMIT)
                    .then(|| launch.control_memory().try_acquire(target))
                    .flatten()
            });
            let Some(acquired) = acquired else {
                return Err(ProgramOutcome::memory(task_meta));
            };
            acquired
        }
    };
    if cancel.is_some_and(CancellationToken::is_cancelled) {
        return Err(ProgramOutcome::cancelled(
            "Cancelled before nested output was committed.",
        ));
    }
    if !delivery
        .script
        .reserve_result(delivery.pending_id, wire_len)
    {
        return Err(ProgramOutcome::limit());
    }
    let mut buf = Vec::with_capacity(wire_len);
    if serde_json::to_writer(&mut buf, envelope).is_err() {
        return Err(ProgramOutcome::error(
            "Cannot encode the nested tool result.",
            true,
        ));
    }
    let text = String::from_utf8(buf).expect("JSON serialization emits UTF-8");
    Ok((text, lease))
}

pub(crate) fn encode_exec_program(
    prepared: PreparedExec,
    ctx: &ExecContext<'_>,
    delivery: &ProgramDelivery<'_>,
) -> ProgramOutcome {
    match prepared {
        PreparedExec::Window(prepared) => encode_window(*prepared, ctx, delivery),
        PreparedExec::Transcript(prepared) => encode_transcript(*prepared, ctx, delivery),
        PreparedExec::Screen(prepared) => encode_screen(*prepared, ctx, delivery),
        PreparedExec::List(prepared) => encode_list(prepared, ctx, delivery),
    }
}

fn encode_window(
    mut prepared: PreparedWindow,
    ctx: &ExecContext<'_>,
    delivery: &ProgramDelivery<'_>,
) -> ProgramOutcome {
    let mut registrations = NestedAttachments::new(delivery.script);
    let mut final_seen = prepared.initial_seen;
    let mut nested_handles = Vec::with_capacity(prepared.unseen.len());
    for att in prepared.unseen.drain(..) {
        let meta = att.meta().clone();
        let hid = delivery.script.register_attachment(att);
        registrations.ids.push(hid);
        nested_handles.push(NestedAttachmentDto {
            index: meta.index,
            kind: meta.kind.as_str(),
            mime_type: meta.mime,
            bytes: meta.size,
            hid,
        });
        final_seen = meta.index;
    }

    let snapshot = &prepared.metadata.value;
    let mut warnings = task_warnings(snapshot);
    if prepared.ended.is_none()
        && let Some(note) = kill_pending_note(&prepared.task, snapshot)
    {
        warnings.push(note.to_owned());
    }
    if prepared.view.max_bytes < prepared.requested_max_bytes {
        warnings.push(
            "kill output window narrowed to fit the 8 MiB control reserve; poll taskId or read logPath for remaining output"
                .into(),
        );
    }
    if let Some(warning) = prepared
        .startup_warning
        .as_mut()
        .and_then(ProbeWarning::message)
    {
        warnings.push(warning.to_owned());
    }
    if let Some(enc_warn) = encoding_warning(&prepared.window) {
        warnings.push(enc_warn);
    }

    let codemode = match &prepared.script {
        Some(report) => {
            let started_tasks: Vec<StartedTaskDto<'_>> = report
                .started_tasks
                .iter()
                .map(|s| StartedTaskDto {
                    task_id: &s.task_id,
                    state: s.state,
                })
                .collect();
            let dto = CodemodeDto {
                cancelled_calls: &report.cancelled_calls,
                started_tasks,
                attachments: nested_handles,
            };
            (!dto.cancelled_calls.is_empty()
                || !dto.started_tasks.is_empty()
                || !dto.attachments.is_empty())
            .then_some(dto)
        }
        None if !nested_handles.is_empty() => Some(CodemodeDto {
            cancelled_calls: &[],
            started_tasks: Vec::new(),
            attachments: nested_handles,
        }),
        None => None,
    };

    let omitted_lines = prepared
        .window
        .omitted
        .map_or(0, |(first, last)| last - first + 1);
    let omitted_range = prepared.window.omitted.map(|(first, last)| [first, last]);
    let log_path = files::path::display_path(&prepared.task.log_path);
    let task_meta = Some((prepared.task.id.clone(), log_path.clone()));
    let dto = ExecWindowDto {
        action: action_str(prepared.action),
        task_id: prepared.reader.handle(&prepared.task.id),
        kind: if prepared.task.is_script() {
            "codemode"
        } else {
            "bash"
        },
        state: snapshot.state,
        exit_code: snapshot.exit_code,
        pty: prepared.task.pty,
        elapsed_ms: snapshot.elapsed.as_millis() as u64,
        output: &prepared.window.text,
        truncated: prepared.window.truncated(),
        log_path,
        wait_ended_by: prepared.ended.as_ref().map(wait_ended_dto),
        omitted_lines,
        omitted_range,
        cut_lines: prepared.window.cut_lines,
        cols: None,
        rows: None,
        cursor: None,
        omitted_rows: 0,
        new_reader: prepared.reader.is_fresh(),
        keep_tree: prepared.task.keep,
        kept_running: snapshot.kept,
        held: members_dto(&snapshot.held),
        leftovers: members_dto(&snapshot.leftovers),
        lifetime_expired: snapshot.expired,
        forced_end: snapshot.forced_end,
        output_open: snapshot.output_open,
        output_incomplete: output_incomplete(snapshot),
        dropped_bytes: snapshot.dropped,
        log_evicted: snapshot.evicted,
        encoding_errors: prepared.window.bad_lines,
        capture_error: snapshot.capture_error.as_deref(),
        log_error: snapshot.log_error.as_deref(),
        input_warning: snapshot.input_warning.as_deref(),
        input_error: snapshot.input_error.as_deref(),
        kill_error: snapshot.kill_error.as_deref(),
        warnings,
        codemode,
    };

    let io_ok = task_io_ok(snapshot);
    let io_msg = (!io_ok).then(|| io_failure_message(snapshot));
    let retained_data = prepared.window.text.capacity();
    let launch = ctx.launch();
    let workspace = std::mem::replace(&mut prepared.workspace, Lease::empty(launch.memory()));

    let encoded = if io_ok {
        let envelope = OkEnvelope {
            ok: true,
            value: &dto,
        };
        admit_bridge_result(
            &envelope,
            Some(workspace),
            retained_data,
            launch,
            delivery,
            Some(&ctx.cancel),
            task_meta,
        )
    } else {
        let envelope = ErrEnvelope {
            ok: false,
            code: "IO",
            error: io_msg.as_deref().unwrap_or("Tool operation failed."),
            result: Some(&dto),
        };
        admit_bridge_result(
            &envelope,
            Some(workspace),
            retained_data,
            launch,
            delivery,
            Some(&ctx.cancel),
            task_meta,
        )
    };

    drop(dto);
    match encoded {
        Ok((text, mut lease)) => {
            registrations.disarm();
            if let Some(warning) = prepared.startup_warning.take() {
                warning.commit();
            }
            prepared.read.state.attachments_seen = final_seen;
            let state = prepared.read.commit();
            prepared.reader.mark_delivered();
            prepared.reader.mark_reported(&prepared.metadata.value);
            if prepared.mark_session_entry {
                prepared.entry.mark_reported(&prepared.metadata.value);
            }
            drop(prepared.window);
            drop(prepared.metadata);
            drop(prepared.script);
            drop(prepared.permit);
            drop(state);
            lease.shrink_to(text.capacity());
            ProgramOutcome::value(text, lease)
        }
        Err(outcome) => outcome,
    }
}

fn encode_transcript(
    mut prepared: PreparedTranscript,
    ctx: &ExecContext<'_>,
    delivery: &ProgramDelivery<'_>,
) -> ProgramOutcome {
    let snapshot = &prepared.metadata.value;
    let mut warnings = task_warnings(snapshot);
    warnings.extend(prepared.transcript_warnings.iter().cloned());
    if let Some(enc_warn) = encoding_warning(&prepared.window) {
        warnings.push(enc_warn);
    }
    let omitted_lines = prepared
        .window
        .omitted
        .map_or(0, |(first, last)| last - first + 1);
    let omitted_range = prepared.window.omitted.map(|(first, last)| [first, last]);
    let log_path = files::path::display_path(&prepared.task.log_path);
    let task_meta = Some((prepared.task.id.clone(), log_path.clone()));
    let dto = ExecTranscriptDto {
        action: "transcript",
        task_id: prepared.reader.handle(&prepared.task.id),
        kind: if prepared.task.is_script() {
            "codemode"
        } else {
            "bash"
        },
        state: snapshot.state,
        exit_code: snapshot.exit_code,
        pty: prepared.task.pty,
        elapsed_ms: snapshot.elapsed.as_millis() as u64,
        output: &prepared.window.text,
        truncated: prepared.window.truncated(),
        log_path,
        transcript_path: &prepared.transcript_path,
        transcript_lines: prepared.transcript_lines,
        final_lines: prepared.final_lines,
        first_line: prepared.first_line,
        omitted_lines,
        omitted_range,
        cut_lines: prepared.window.cut_lines,
        new_reader: prepared.reader.is_fresh(),
        transcript_incomplete: prepared.transcript_incomplete,
        transcript_error: prepared.transcript_error.as_deref(),
        encoding_errors: prepared.window.bad_lines,
        keep_tree: prepared.task.keep,
        kept_running: snapshot.kept,
        held: members_dto(&snapshot.held),
        leftovers: members_dto(&snapshot.leftovers),
        lifetime_expired: snapshot.expired,
        forced_end: snapshot.forced_end,
        output_open: snapshot.output_open,
        output_incomplete: output_incomplete(snapshot),
        dropped_bytes: snapshot.dropped,
        log_evicted: snapshot.evicted,
        capture_error: snapshot.capture_error.as_deref(),
        log_error: snapshot.log_error.as_deref(),
        input_warning: snapshot.input_warning.as_deref(),
        input_error: snapshot.input_error.as_deref(),
        kill_error: snapshot.kill_error.as_deref(),
        warnings,
    };

    let io_ok = task_io_ok(snapshot);
    let io_msg = (!io_ok).then(|| io_failure_message(snapshot));
    let retained_data = prepared.window.text.capacity();
    let launch = ctx.launch();
    let workspace = std::mem::replace(&mut prepared.workspace, Lease::empty(launch.memory()));

    let encoded = if io_ok {
        let envelope = OkEnvelope {
            ok: true,
            value: &dto,
        };
        admit_bridge_result(
            &envelope,
            Some(workspace),
            retained_data,
            launch,
            delivery,
            Some(&ctx.cancel),
            task_meta,
        )
    } else {
        let envelope = ErrEnvelope {
            ok: false,
            code: "IO",
            error: io_msg.as_deref().unwrap_or("Tool operation failed."),
            result: Some(&dto),
        };
        admit_bridge_result(
            &envelope,
            Some(workspace),
            retained_data,
            launch,
            delivery,
            Some(&ctx.cancel),
            task_meta,
        )
    };

    drop(dto);
    match encoded {
        Ok((text, mut lease)) => {
            let state = prepared.read.commit();
            prepared.reader.mark_delivered();
            prepared.reader.mark_reported(&prepared.metadata.value);
            if prepared.mark_session_entry {
                prepared.entry.mark_reported(&prepared.metadata.value);
            }
            drop(prepared.window);
            drop(prepared.metadata);
            drop(prepared.permit);
            drop(state);
            lease.shrink_to(text.capacity());
            ProgramOutcome::value(text, lease)
        }
        Err(outcome) => outcome,
    }
}

fn encode_screen(
    mut prepared: PreparedScreen,
    ctx: &ExecContext<'_>,
    delivery: &ProgramDelivery<'_>,
) -> ProgramOutcome {
    let snapshot = &prepared.metadata.value;
    let mut warnings = task_warnings(snapshot);
    if let Some(warning) = prepared
        .startup_warning
        .as_mut()
        .and_then(ProbeWarning::message)
    {
        warnings.push(warning.to_owned());
    }
    if let Some(enc_warn) = encoding_warning(&prepared.window) {
        warnings.push(enc_warn);
    }
    let (body, omitted_rows, line_cut) =
        output::fit_screen_rows(&prepared.screen_rows, prepared.view.max_bytes, false);
    let (row, col) = (prepared.cursor.0 + 1, prepared.cursor.1 + 1);
    let log_path = files::path::display_path(&prepared.task.log_path);
    let task_meta = Some((prepared.task.id.clone(), log_path.clone()));
    let dto = ExecWindowDto {
        action: action_str(prepared.action),
        task_id: prepared.reader.handle(&prepared.task.id),
        kind: if prepared.task.is_script() {
            "codemode"
        } else {
            "bash"
        },
        state: snapshot.state,
        exit_code: snapshot.exit_code,
        pty: prepared.task.pty,
        elapsed_ms: snapshot.elapsed.as_millis() as u64,
        output: &body,
        truncated: omitted_rows > 0 || line_cut,
        log_path,
        wait_ended_by: Some(wait_ended_dto(&prepared.ended)),
        omitted_lines: 0,
        omitted_range: None,
        cut_lines: u64::from(line_cut),
        cols: Some(prepared.cols),
        rows: Some(prepared.rows_count),
        cursor: Some([row, col]),
        omitted_rows,
        new_reader: prepared.reader.is_fresh(),
        keep_tree: prepared.task.keep,
        kept_running: snapshot.kept,
        held: members_dto(&snapshot.held),
        leftovers: members_dto(&snapshot.leftovers),
        lifetime_expired: snapshot.expired,
        forced_end: snapshot.forced_end,
        output_open: snapshot.output_open,
        output_incomplete: output_incomplete(snapshot),
        dropped_bytes: snapshot.dropped,
        log_evicted: snapshot.evicted,
        encoding_errors: prepared.window.bad_lines,
        capture_error: snapshot.capture_error.as_deref(),
        log_error: snapshot.log_error.as_deref(),
        input_warning: snapshot.input_warning.as_deref(),
        input_error: snapshot.input_error.as_deref(),
        kill_error: snapshot.kill_error.as_deref(),
        warnings,
        codemode: None,
    };

    let io_ok = task_io_ok(snapshot);
    let io_msg = (!io_ok).then(|| io_failure_message(snapshot));
    let retained_data = body.capacity();
    let launch = ctx.launch();
    let workspace = std::mem::replace(&mut prepared.workspace, Lease::empty(launch.memory()));

    let encoded = if io_ok {
        let envelope = OkEnvelope {
            ok: true,
            value: &dto,
        };
        admit_bridge_result(
            &envelope,
            Some(workspace),
            retained_data,
            launch,
            delivery,
            Some(&ctx.cancel),
            task_meta,
        )
    } else {
        let envelope = ErrEnvelope {
            ok: false,
            code: "IO",
            error: io_msg.as_deref().unwrap_or("Tool operation failed."),
            result: Some(&dto),
        };
        admit_bridge_result(
            &envelope,
            Some(workspace),
            retained_data,
            launch,
            delivery,
            Some(&ctx.cancel),
            task_meta,
        )
    };

    drop(dto);
    drop(body);
    match encoded {
        Ok((text, mut lease)) => {
            if let Some(warning) = prepared.startup_warning.take() {
                warning.commit();
            }
            let state = prepared.read.commit();
            prepared.reader.mark_delivered();
            prepared.reader.mark_reported(&prepared.metadata.value);
            if prepared.mark_session_entry {
                prepared.entry.mark_reported(&prepared.metadata.value);
            }
            drop(prepared.screen_rows);
            drop(prepared.window);
            drop(prepared.metadata);
            drop(prepared.permit);
            drop(state);
            lease.shrink_to(text.capacity());
            ProgramOutcome::value(text, lease)
        }
        Err(outcome) => outcome,
    }
}

fn encode_list(
    mut prepared: PreparedList,
    ctx: &ExecContext<'_>,
    delivery: &ProgramDelivery<'_>,
) -> ProgramOutcome {
    let tasks: Vec<ListTaskDto<'_>> = prepared
        .items
        .iter()
        .map(|item| {
            let running = item.snapshot.state == "running";
            let size = item.task.pty.then(|| item.task.size());
            let children = item.children.as_ref().and_then(|c| {
                c.should_show(running).then(|| {
                    let active_tasks = c
                        .active_tasks
                        .iter()
                        .map(|(child, snap)| {
                            let child_size = child.pty.then(|| child.size());
                            list_task_dto(child, snap, child_size, None, None)
                        })
                        .collect();
                    ListChildrenDto {
                        total: c.total,
                        done: c.done,
                        failed: c.failed,
                        warned: c.warned,
                        active: c.active,
                        queued: if running { c.queued } else { 0 },
                        cancelled: if !running { c.cancelled } else { 0 },
                        active_tasks,
                    }
                })
            });
            list_task_dto(
                &item.task,
                &item.snapshot,
                size,
                item.parent_task_id.as_deref(),
                children,
            )
        })
        .collect();
    let dto = ExecListDto {
        action: "list",
        total_count: prepared.total_count,
        tasks,
    };
    let envelope = OkEnvelope {
        ok: true,
        value: &dto,
    };
    let launch = ctx.launch();
    let workspace = std::mem::replace(&mut prepared.workspace, Lease::empty(launch.memory()));
    let encoded = admit_bridge_result(
        &envelope,
        Some(workspace),
        0,
        launch,
        delivery,
        Some(&ctx.cancel),
        None,
    );
    drop(dto);
    match encoded {
        Ok((text, mut lease)) => {
            prepared.commit_all(ctx.session);
            lease.shrink_to(text.capacity());
            ProgramOutcome::value(text, lease)
        }
        Err(outcome) => outcome,
    }
}

pub(crate) fn encode_service_error(
    err: ServiceError,
    ctx: &ExecContext<'_>,
    delivery: &ProgramDelivery<'_>,
) -> ProgramOutcome {
    let (code, message, task_meta) = match err {
        ServiceError::Validation(msg) => ("VALIDATION", msg, None),
        ServiceError::NotFound(msg) => ("NOT_FOUND", msg, None),
        ServiceError::Cancelled(msg) => ("CANCELLED", msg, None),
        ServiceError::Overload(msg, meta) => ("OVERLOAD", msg, meta),
        ServiceError::Plain(msg) => ("IO", msg, None),
    };
    let meta_dto = task_meta.as_ref().map(|m| TaskMetaDto {
        task_id: &m.bare_task_id,
        log_path: &m.log_path,
    });
    let fallback_meta = task_meta
        .as_ref()
        .map(|m| (m.bare_task_id.clone(), m.log_path.clone()));
    let envelope = ErrEnvelope {
        ok: false,
        code,
        error: &message,
        result: meta_dto.as_ref(),
    };
    match admit_bridge_result(
        &envelope,
        None,
        0,
        ctx.launch(),
        delivery,
        None,
        fallback_meta,
    ) {
        Ok((text, mut lease)) => {
            drop(message);
            lease.shrink_to(text.capacity());
            ProgramOutcome::value(text, lease)
        }
        Err(outcome) => outcome,
    }
}

fn file_error_code(err: &FileError) -> &'static str {
    match err {
        FileError::Validation(_) => "VALIDATION",
        FileError::NotFound(_) => "NOT_FOUND",
        FileError::Limit(_) => "LIMIT",
        FileError::Overload(_) => "OVERLOAD",
        FileError::Cancelled(_) => "CANCELLED",
        FileError::Io(_) => "IO",
    }
}

pub(crate) fn encode_read_program(
    result: Result<FileReadData, FileError>,
    launch: &LaunchContext,
    delivery: &ProgramDelivery<'_>,
    cancel: &CancellationToken,
) -> ProgramOutcome {
    match result {
        Ok(FileReadData::Text(mut slice)) => {
            let dto = ReadTextDto {
                kind: "text",
                path: &slice.path,
                text: &slice.text,
                start_line: slice.start_line,
                end_line: slice.end_line,
                shown_lines: slice.shown_lines,
                total_lines: slice.total_lines,
                truncated: slice.truncated,
                next_offset: slice.next_offset,
                line_too_long: slice.line_too_long,
                lossy: slice.lossy,
            };
            let envelope = OkEnvelope {
                ok: true,
                value: &dto,
            };
            let retained_data = slice.text.capacity();
            let lease = std::mem::replace(&mut slice.lease, Lease::empty(launch.memory()));
            let encoded = admit_bridge_result(
                &envelope,
                Some(lease),
                retained_data,
                launch,
                delivery,
                Some(cancel),
                None,
            );
            drop(slice);
            match encoded {
                Ok((text, mut lease)) => {
                    lease.shrink_to(text.capacity());
                    ProgramOutcome::value(text, lease)
                }
                Err(outcome) => outcome,
            }
        }
        Ok(FileReadData::Media(MediaPayload::Descriptor {
            kind,
            path,
            mime_type,
            bytes,
            source_bytes,
            attachable,
            note,
        })) => {
            let dto = ReadMediaDto {
                kind: kind.as_str(),
                path: &path,
                mime_type: &mime_type,
                bytes,
                source_bytes,
                attachable,
                note: note.as_deref(),
            };
            let envelope = OkEnvelope {
                ok: true,
                value: &dto,
            };
            let encoded =
                admit_bridge_result(&envelope, None, 0, launch, delivery, Some(cancel), None);
            drop(path);
            drop(mime_type);
            drop(note);
            match encoded {
                Ok((text, mut lease)) => {
                    lease.shrink_to(text.capacity());
                    ProgramOutcome::value(text, lease)
                }
                Err(outcome) => outcome,
            }
        }
        Ok(FileReadData::Media(MediaPayload::Inline { .. })) => {
            unreachable!("ReadPolicy::PROGRAM uses MediaReadMode::DescriptorOnly")
        }
        Err(err) => {
            let code = file_error_code(&err);
            let msg = err.into_message();
            let envelope: ErrEnvelope<'_, ()> = ErrEnvelope {
                ok: false,
                code,
                error: &msg,
                result: None,
            };
            let encoded = admit_bridge_result(&envelope, None, 0, launch, delivery, None, None);
            drop(msg);
            match encoded {
                Ok((text, mut lease)) => {
                    lease.shrink_to(text.capacity());
                    ProgramOutcome::value(text, lease)
                }
                Err(outcome) => outcome,
            }
        }
    }
}

pub(crate) fn encode_write_program(
    result: Result<FileWriteData, FileError>,
    launch: &LaunchContext,
    delivery: &ProgramDelivery<'_>,
    _cancel: &CancellationToken,
) -> ProgramOutcome {
    match result {
        Ok(data) => {
            let dto = WriteDto {
                path: &data.path,
                bytes: data.bytes,
            };
            let envelope = OkEnvelope {
                ok: true,
                value: &dto,
            };
            let encoded = admit_bridge_result(&envelope, None, 0, launch, delivery, None, None);
            drop(data);
            match encoded {
                Ok((text, mut lease)) => {
                    lease.shrink_to(text.capacity());
                    ProgramOutcome::value(text, lease)
                }
                Err(outcome) => outcome,
            }
        }
        Err(err) => {
            let code = file_error_code(&err);
            let msg = err.into_message();
            let envelope: ErrEnvelope<'_, ()> = ErrEnvelope {
                ok: false,
                code,
                error: &msg,
                result: None,
            };
            let encoded = admit_bridge_result(&envelope, None, 0, launch, delivery, None, None);
            drop(msg);
            match encoded {
                Ok((text, mut lease)) => {
                    lease.shrink_to(text.capacity());
                    ProgramOutcome::value(text, lease)
                }
                Err(outcome) => outcome,
            }
        }
    }
}

pub(crate) fn encode_edit_program(
    result: Result<FileEditData, EditError>,
    launch: &LaunchContext,
    delivery: &ProgramDelivery<'_>,
    _cancel: &CancellationToken,
) -> ProgramOutcome {
    match result {
        Ok(data) => {
            let dto = EditDto {
                path: &data.path,
                replacements: data.replacements,
                edits: &data.edits,
            };
            let envelope = OkEnvelope {
                ok: true,
                value: &dto,
            };
            let encoded = admit_bridge_result(&envelope, None, 0, launch, delivery, None, None);
            drop(data);
            match encoded {
                Ok((text, mut lease)) => {
                    lease.shrink_to(text.capacity());
                    ProgramOutcome::value(text, lease)
                }
                Err(outcome) => outcome,
            }
        }
        Err(err) => {
            if let Some(failures) = err.failures {
                let msg = err.error.into_message();
                let path = err.path.unwrap_or_default();
                let details = EditFailedDto {
                    path: &path,
                    failures: &failures,
                };
                let envelope = ErrEnvelope {
                    ok: false,
                    code: "EDIT_FAILED",
                    error: &msg,
                    result: Some(&details),
                };
                let encoded = admit_bridge_result(&envelope, None, 0, launch, delivery, None, None);
                drop(path);
                drop(failures);
                drop(msg);
                match encoded {
                    Ok((text, mut lease)) => {
                        lease.shrink_to(text.capacity());
                        ProgramOutcome::value(text, lease)
                    }
                    Err(outcome) => outcome,
                }
            } else {
                let code = file_error_code(&err.error);
                let msg = err.error.into_message();
                let envelope: ErrEnvelope<'_, ()> = ErrEnvelope {
                    ok: false,
                    code,
                    error: &msg,
                    result: None,
                };
                let encoded = admit_bridge_result(&envelope, None, 0, launch, delivery, None, None);
                drop(msg);
                match encoded {
                    Ok((text, mut lease)) => {
                        lease.shrink_to(text.capacity());
                        ProgramOutcome::value(text, lease)
                    }
                    Err(outcome) => outcome,
                }
            }
        }
    }
}
