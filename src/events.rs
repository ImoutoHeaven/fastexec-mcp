//! Bounded in-memory event bus for MCP Events (`task.exited` and `external.event`).

use crate::lease::{self, BytePool, Lease};
use crate::tasks;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

pub const EVENT_TASK_EXITED: &str = "task.exited";
pub const EVENT_EXTERNAL: &str = "external.event";

pub const DEFAULT_MAX_EVENTS: usize = 4096;
pub const DEFAULT_MAX_BYTES: usize = 8 << 20;
pub const MAX_EXTERNAL_EVENT_BYTES: usize = 64 * 1024;
pub const MAX_POLL_EVENTS: usize = 64;
pub const MAX_SESSION_CURSORS: usize = 4096;
pub const MAX_EVENT_STREAMS: usize = 64;
pub const POLL_NEXT_ACTIVE_MS: u64 = 2000;
pub const POLL_NEXT_IDLE_MS: u64 = 5000;
pub const DEFAULT_HEARTBEAT_MS: u64 = 30_000;
pub const MIN_HEARTBEAT_MS: u64 = 5_000;
pub const MAX_HEARTBEAT_MS: u64 = 30_000;
const DEFAULT_BROADCAST_CAPACITY: usize = 1024;
const ENTRY_FIXED_OVERHEAD: usize = 128;
const DEDUP_FIXED_OVERHEAD: usize = 64;
const MAX_SUMMARY_LABEL_CHARS: usize = 48;
const MAX_SUMMARY_GROUPS: usize = 3;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis().min(u64::MAX as u128) as u64)
}

/// Formats UTC epoch milliseconds as RFC 3339 (`YYYY-MM-DDTHH:MM:SS.mmmZ`).
fn format_rfc3339_ms(unix_ms: u64) -> String {
    let total_secs = unix_ms / 1000;
    let millis = unix_ms % 1000;
    let days = (total_secs / 86_400) as i64;
    let secs_of_day = total_secs % 86_400;
    let hour = secs_of_day / 3600;
    let minute = (secs_of_day % 3600) / 60;
    let second = secs_of_day % 60;

    // Howard Hinnant's civil-from-days algorithm avoids a datetime dependency.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + i64::from(month <= 2);

    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventError {
    InvalidParams(String),
    NotFound {
        kind: &'static str,
        message: String,
    },
    Forbidden(String),
    ResourceExhausted {
        limit: &'static str,
        max: usize,
        message: String,
    },
}

impl EventError {
    pub fn code(&self) -> i32 {
        match self {
            Self::InvalidParams(_) => -32602,
            Self::NotFound { .. } => -32023,
            Self::Forbidden(_) => -32024,
            Self::ResourceExhausted { .. } => -32025,
        }
    }

    pub fn data(&self) -> Option<Value> {
        match self {
            Self::NotFound { kind, .. } => Some(json!({ "kind": kind })),
            Self::ResourceExhausted { limit, max, .. } => {
                Some(json!({ "limit": limit, "max": max }))
            }
            Self::InvalidParams(_) | Self::Forbidden(_) => None,
        }
    }
}

impl std::fmt::Display for EventError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidParams(msg)
            | Self::NotFound { message: msg, .. }
            | Self::Forbidden(msg)
            | Self::ResourceExhausted { message: msg, .. } => f.write_str(msg),
        }
    }
}

impl std::error::Error for EventError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventTypeDescriptor {
    pub name: Cow<'static, str>,
    pub description: Cow<'static, str>,
    pub delivery: Vec<Cow<'static, str>>,
    pub input_schema: Value,
    pub payload_schema: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventsListResult {
    pub result_type: Cow<'static, str>,
    pub events: Vec<EventTypeDescriptor>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskExitedInput {
    pub task_id: String,
    pub session_id: Option<String>,
    pub state: String,
    pub exit_code: Option<i32>,
    pub expired: bool,
    pub kept_processes: u64,
    pub tree_ended: bool,
    pub log_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ExternalEventInput {
    /// Optional explicit event type; external callers may only emit `external.event`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,
    pub source: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    pub name: String,
    pub upstream_event_id: String,
    pub data: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_truncated: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PublishResult {
    pub event_id: String,
    pub cursor: String,
    pub deduplicated: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PollRequest {
    pub name: String,
    #[serde(default)]
    pub arguments: Option<Value>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub max_age_ms: Option<u64>,
    #[serde(default)]
    pub max_events: Option<usize>,
}

/// Event item returned inside `events/poll` (`events[]` intentionally omits `cursor`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PolledEvent {
    pub event_id: String,
    pub name: String,
    pub timestamp: String,
    pub data: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PollResponse {
    pub result_type: Cow<'static, str>,
    pub events: Vec<PolledEvent>,
    pub cursor: String,
    pub truncated: bool,
    pub has_more: bool,
    pub next_poll_ms: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SubscribeRequest {
    pub name: String,
    #[serde(default)]
    pub arguments: Option<Value>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub max_age_ms: Option<u64>,
    #[serde(default)]
    pub heartbeat_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ActiveNotice {
    pub cursor: String,
    pub truncated: bool,
    pub heartbeat_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct StreamEvent {
    pub event_id: String,
    pub name: String,
    pub timestamp: String,
    pub data: Value,
    pub cursor: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StreamItem {
    Active(ActiveNotice),
    Event(StreamEvent),
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum TaskScope {
    #[default]
    All,
    Session,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct TaskExitedArgs {
    task_id: Option<String>,
    #[serde(default)]
    scope: TaskScope,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct ExternalEventArgs {
    source: Option<String>,
    topic: Option<String>,
    name: Option<String>,
}

#[derive(Debug, Clone)]
enum EventFilter {
    TaskExited {
        task_id: Option<String>,
        scope_session: Option<String>,
    },
    External {
        source: Option<String>,
        topic: Option<String>,
        name: Option<String>,
    },
}

impl EventFilter {
    fn parse(
        name: &str,
        arguments: Option<Value>,
        caller_session: Option<&str>,
    ) -> Result<Self, EventError> {
        let args = arguments.unwrap_or_else(|| json!({}));
        match name {
            EVENT_TASK_EXITED => {
                let parsed: TaskExitedArgs = serde_json::from_value(args)
                    .map_err(|e| EventError::InvalidParams(format!("invalid arguments: {e}")))?;
                let scope_session = match parsed.scope {
                    TaskScope::All => None,
                    TaskScope::Session => {
                        let session = caller_session.ok_or_else(|| {
                            EventError::InvalidParams(
                                "scope \"session\" requires an active session".to_owned(),
                            )
                        })?;
                        Some(session.to_owned())
                    }
                };
                Ok(Self::TaskExited {
                    task_id: parsed.task_id,
                    scope_session,
                })
            }
            EVENT_EXTERNAL => {
                let parsed: ExternalEventArgs = serde_json::from_value(args)
                    .map_err(|e| EventError::InvalidParams(format!("invalid arguments: {e}")))?;
                Ok(Self::External {
                    source: parsed.source,
                    topic: parsed.topic,
                    name: parsed.name,
                })
            }
            other => Err(EventError::NotFound {
                kind: "event",
                message: format!("unknown event type {other:?}"),
            }),
        }
    }

    fn matches(&self, record: &EventRecord) -> bool {
        match (self, &record.meta) {
            (
                Self::TaskExited {
                    task_id,
                    scope_session,
                },
                EventMeta::TaskExited {
                    task_id: rec_task_id,
                    session_id: rec_session,
                },
            ) => {
                if let Some(want) = task_id
                    && want != rec_task_id
                {
                    return false;
                }
                if let Some(want_session) = scope_session
                    && rec_session.as_deref() != Some(want_session.as_str())
                {
                    return false;
                }
                true
            }
            (
                Self::External {
                    source,
                    topic,
                    name,
                },
                EventMeta::External {
                    source: rec_source,
                    topic: rec_topic,
                    upstream_name: rec_name,
                    ..
                },
            ) => {
                if let Some(want) = source
                    && want != rec_source
                {
                    return false;
                }
                if let Some(want) = topic
                    && rec_topic.as_deref() != Some(want.as_str())
                {
                    return false;
                }
                if let Some(want) = name
                    && want != rec_name
                {
                    return false;
                }
                true
            }
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
enum EventMeta {
    TaskExited {
        task_id: String,
        session_id: Option<String>,
    },
    External {
        source: String,
        topic: Option<String>,
        upstream_name: String,
        upstream_event_id: String,
    },
}

#[derive(Debug)]
struct EventRecord {
    seq: u64,
    event_id: String,
    name: &'static str,
    timestamp: String,
    emitted_ms: u64,
    data: Value,
    meta: EventMeta,
    lease: Lease,
}

impl EventRecord {
    fn to_polled(&self) -> PolledEvent {
        PolledEvent {
            event_id: self.event_id.clone(),
            name: self.name.to_owned(),
            timestamp: self.timestamp.clone(),
            data: self.data.clone(),
        }
    }

    fn to_stream(&self, boot_epoch: &str) -> StreamEvent {
        StreamEvent {
            event_id: self.event_id.clone(),
            name: self.name.to_owned(),
            timestamp: self.timestamp.clone(),
            data: self.data.clone(),
            cursor: format_cursor(boot_epoch, self.seq),
        }
    }
}

struct BusState {
    ring: VecDeque<Arc<EventRecord>>,
    next_seq: u64,
    ring_bytes: usize,
    dedup: HashMap<(String, String), u64>,
    session_cursors: HashMap<String, (u64, u64)>,
    session_cursor_clock: u64,
}

impl BusState {
    fn new() -> Self {
        Self {
            ring: VecDeque::new(),
            next_seq: 1,
            ring_bytes: 0,
            dedup: HashMap::new(),
            session_cursors: HashMap::new(),
            session_cursor_clock: 0,
        }
    }

    fn latest_seq(&self) -> u64 {
        self.next_seq - 1
    }

    fn oldest_seq(&self) -> Option<u64> {
        self.ring.front().map(|entry| entry.seq)
    }

    fn evict_oldest(&mut self) -> bool {
        let Some(evicted) = self.ring.pop_front() else {
            return false;
        };
        self.ring_bytes = self.ring_bytes.saturating_sub(evicted.lease.bytes());
        if let EventMeta::External {
            source,
            upstream_event_id,
            ..
        } = &evicted.meta
        {
            let key = (source.clone(), upstream_event_id.clone());
            if self.dedup.get(&key).copied() == Some(evicted.seq) {
                self.dedup.remove(&key);
            }
        }
        drop(evicted);
        true
    }
}

/// Process-scoped event bus shared by stdio, daemon IPC, and Streamable HTTP paths.
pub struct EventBus {
    boot_epoch: String,
    max_events: usize,
    max_bytes: usize,
    max_session_cursors: usize,
    max_streams: usize,
    active_streams: AtomicUsize,
    pool: Arc<BytePool>,
    state: Mutex<BusState>,
    tx: broadcast::Sender<Arc<EventRecord>>,
}

/// RAII guard tracking an active `events/stream` across HTTP, stdio, and daemon IPC.
pub struct StreamGuard {
    bus: Arc<EventBus>,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.bus.active_streams.fetch_sub(1, Ordering::AcqRel);
    }
}

impl EventBus {
    pub fn new() -> std::io::Result<Self> {
        let boot_epoch = tasks::random_chars(8)?;
        Ok(Self::with_config(
            boot_epoch,
            DEFAULT_MAX_EVENTS,
            DEFAULT_MAX_BYTES,
            DEFAULT_BROADCAST_CAPACITY,
            Arc::clone(lease::process_pool()),
        ))
    }

    pub fn with_config(
        boot_epoch: String,
        max_events: usize,
        max_bytes: usize,
        broadcast_capacity: usize,
        pool: Arc<BytePool>,
    ) -> Self {
        let (tx, _) = broadcast::channel(broadcast_capacity.max(1));
        Self {
            boot_epoch,
            max_events: max_events.max(1),
            max_bytes: max_bytes.max(1),
            max_session_cursors: MAX_SESSION_CURSORS,
            max_streams: MAX_EVENT_STREAMS,
            active_streams: AtomicUsize::new(0),
            pool,
            state: Mutex::new(BusState::new()),
            tx,
        }
    }

    pub fn boot_epoch(&self) -> &str {
        &self.boot_epoch
    }

    pub fn active_streams(&self) -> usize {
        self.active_streams.load(Ordering::Acquire)
    }

    pub fn try_acquire_stream(self: &Arc<Self>) -> Result<StreamGuard, EventError> {
        let mut current = self.active_streams.load(Ordering::Acquire);
        loop {
            if current >= self.max_streams {
                return Err(EventError::ResourceExhausted {
                    limit: "active_event_streams",
                    max: self.max_streams,
                    message: format!("active event streams reached limit ({})", self.max_streams),
                });
            }
            match self.active_streams.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(StreamGuard {
                        bus: Arc::clone(self),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }

    #[cfg(test)]
    pub fn current_cursor(&self) -> String {
        let state = lock(&self.state);
        format_cursor(&self.boot_epoch, state.latest_seq())
    }

    pub fn list_event_types(&self) -> EventsListResult {
        EventsListResult {
            result_type: Cow::Borrowed("complete"),
            events: vec![
                EventTypeDescriptor {
                    name: Cow::Borrowed(EVENT_TASK_EXITED),
                    description: Cow::Borrowed(
                        "Emitted when a fastexec task exits, is killed, or expires.",
                    ),
                    delivery: vec![Cow::Borrowed("push"), Cow::Borrowed("poll")],
                    input_schema: json!({
                        "type": "object",
                        "properties": {
                            "taskId": { "type": "string" },
                            "scope": { "type": "string", "enum": ["session", "all"] }
                        },
                        "additionalProperties": false
                    }),
                    payload_schema: json!({
                        "type": "object",
                        "properties": {
                            "taskId": { "type": "string" },
                            "state": { "type": "string" },
                            "exitCode": { "type": "integer" },
                            "expired": { "type": "boolean" },
                            "keptProcesses": { "type": "integer", "minimum": 0 },
                            "treeEnded": { "type": "boolean" },
                            "logPath": { "type": "string" }
                        },
                        "required": ["taskId", "state", "expired", "keptProcesses", "treeEnded"],
                        "additionalProperties": false
                    }),
                },
                EventTypeDescriptor {
                    name: Cow::Borrowed(EVENT_EXTERNAL),
                    description: Cow::Borrowed("External event injected via fastexec daemon emit."),
                    delivery: vec![Cow::Borrowed("push"), Cow::Borrowed("poll")],
                    input_schema: json!({
                        "type": "object",
                        "properties": {
                            "source": { "type": "string" },
                            "topic": { "type": "string" },
                            "name": { "type": "string" }
                        },
                        "additionalProperties": false
                    }),
                    payload_schema: json!({
                        "type": "object",
                        "properties": {
                            "source": { "type": "string" },
                            "topic": { "type": "string" },
                            "name": { "type": "string" },
                            "upstreamEventId": { "type": "string" },
                            "data": {},
                            "upstreamTruncated": { "type": "boolean" }
                        },
                        "required": ["source", "name", "upstreamEventId", "data"],
                        "additionalProperties": false
                    }),
                },
            ],
        }
    }

    pub fn publish_task_exited(&self, input: TaskExitedInput) -> Result<PublishResult, EventError> {
        self.publish_task_exited_at(input, now_unix_ms())
    }

    fn publish_task_exited_at(
        &self,
        input: TaskExitedInput,
        now_ms: u64,
    ) -> Result<PublishResult, EventError> {
        if input.task_id.trim().is_empty() {
            return Err(EventError::InvalidParams(
                "taskId must not be empty".to_owned(),
            ));
        }
        let mut obj = serde_json::Map::new();
        obj.insert("taskId".to_owned(), Value::String(input.task_id.clone()));
        obj.insert("state".to_owned(), Value::String(input.state));
        if let Some(code) = input.exit_code {
            obj.insert("exitCode".to_owned(), Value::Number(code.into()));
        }
        obj.insert("expired".to_owned(), Value::Bool(input.expired));
        obj.insert(
            "keptProcesses".to_owned(),
            Value::Number(input.kept_processes.into()),
        );
        obj.insert("treeEnded".to_owned(), Value::Bool(input.tree_ended));
        if let Some(log_path) = input.log_path {
            obj.insert("logPath".to_owned(), Value::String(log_path));
        }
        let data = Value::Object(obj);
        let data_bytes = serde_json::to_vec(&data)
            .map_err(|e| EventError::InvalidParams(format!("cannot serialize event: {e}")))?
            .len();
        let meta = EventMeta::TaskExited {
            task_id: input.task_id,
            session_id: input.session_id,
        };
        let entry_bytes = estimate_entry_bytes(EVENT_TASK_EXITED, data_bytes, &meta);
        self.enqueue(EVENT_TASK_EXITED, data, meta, entry_bytes, now_ms, None)
    }

    pub fn publish_external(&self, input: ExternalEventInput) -> Result<PublishResult, EventError> {
        self.publish_external_at(input, now_unix_ms())
    }

    fn publish_external_at(
        &self,
        input: ExternalEventInput,
        now_ms: u64,
    ) -> Result<PublishResult, EventError> {
        if let Some(event_type) = input.event_type.as_deref()
            && event_type != EVENT_EXTERNAL
        {
            return Err(EventError::Forbidden(format!(
                "external emitters may only publish {EVENT_EXTERNAL:?}, not {event_type:?}"
            )));
        }
        if input.source.trim().is_empty() {
            return Err(EventError::InvalidParams(
                "external event requires non-empty `source`".to_owned(),
            ));
        }
        if input.name.trim().is_empty() {
            return Err(EventError::InvalidParams(
                "external event requires non-empty `name`".to_owned(),
            ));
        }
        if input.upstream_event_id.trim().is_empty() {
            return Err(EventError::InvalidParams(
                "external event requires non-empty `upstreamEventId`".to_owned(),
            ));
        }

        let mut obj = serde_json::Map::new();
        obj.insert("source".to_owned(), Value::String(input.source.clone()));
        if let Some(topic) = input.topic.as_ref().filter(|t| !t.is_empty()) {
            obj.insert("topic".to_owned(), Value::String(topic.clone()));
        }
        obj.insert("name".to_owned(), Value::String(input.name.clone()));
        obj.insert(
            "upstreamEventId".to_owned(),
            Value::String(input.upstream_event_id.clone()),
        );
        obj.insert("data".to_owned(), input.data);
        if let Some(truncated) = input.upstream_truncated {
            obj.insert("upstreamTruncated".to_owned(), Value::Bool(truncated));
        }
        let payload = Value::Object(obj);
        let payload_bytes = serde_json::to_vec(&payload)
            .map_err(|e| EventError::InvalidParams(format!("cannot serialize event: {e}")))?
            .len();
        if payload_bytes > MAX_EXTERNAL_EVENT_BYTES {
            return Err(EventError::InvalidParams(format!(
                "external.event payload ({payload_bytes} bytes) exceeds {MAX_EXTERNAL_EVENT_BYTES} byte limit"
            )));
        }

        let dedup_key = (input.source.clone(), input.upstream_event_id.clone());
        let meta = EventMeta::External {
            source: input.source,
            topic: input.topic.filter(|t| !t.is_empty()),
            upstream_name: input.name,
            upstream_event_id: input.upstream_event_id,
        };
        let entry_bytes = estimate_entry_bytes(EVENT_EXTERNAL, payload_bytes, &meta);
        self.enqueue(
            EVENT_EXTERNAL,
            payload,
            meta,
            entry_bytes,
            now_ms,
            Some(dedup_key),
        )
    }

    fn enqueue(
        &self,
        name: &'static str,
        data: Value,
        meta: EventMeta,
        entry_bytes: usize,
        now_ms: u64,
        dedup_key: Option<(String, String)>,
    ) -> Result<PublishResult, EventError> {
        let mut state = lock(&self.state);
        if let Some(key) = &dedup_key
            && let Some(&existing_seq) = state.dedup.get(key)
        {
            return Ok(PublishResult {
                event_id: format_event_id(&self.boot_epoch, existing_seq),
                cursor: format_cursor(&self.boot_epoch, existing_seq),
                deduplicated: true,
            });
        }

        if entry_bytes > self.max_bytes {
            return Err(EventError::ResourceExhausted {
                limit: "event_bus_bytes",
                max: self.max_bytes,
                message: format!(
                    "event size ({entry_bytes} bytes) exceeds event bus budget ({} bytes)",
                    self.max_bytes
                ),
            });
        }

        // Subscribers keep evicted records charged until their final reference is dropped.
        while state.ring.len() >= self.max_events
            || state.ring_bytes.saturating_add(entry_bytes) > self.max_bytes
        {
            if !state.evict_oldest() {
                break;
            }
        }

        let lease = loop {
            if let Some(lease) = self.pool.try_acquire(entry_bytes) {
                break lease;
            }
            if !state.evict_oldest() {
                return Err(EventError::ResourceExhausted {
                    limit: "process_memory",
                    max: self.pool.limit(),
                    message: "process byte budget is full; event was not enqueued".to_owned(),
                });
            }
        };

        let seq = state.next_seq;
        state.next_seq = seq.saturating_add(1);
        let event_id = format_event_id(&self.boot_epoch, seq);
        let cursor = format_cursor(&self.boot_epoch, seq);
        let record = Arc::new(EventRecord {
            seq,
            event_id: event_id.clone(),
            name,
            timestamp: format_rfc3339_ms(now_ms),
            emitted_ms: now_ms,
            data,
            meta,
            lease,
        });

        state.ring_bytes = state.ring_bytes.saturating_add(record.lease.bytes());
        state.ring.push_back(Arc::clone(&record));
        if let Some(key) = dedup_key {
            state.dedup.insert(key, seq);
        }
        // Publish under the sequence lock so concurrent writers cannot reorder delivery.
        let _ = self.tx.send(record);
        drop(state);
        Ok(PublishResult {
            event_id,
            cursor,
            deduplicated: false,
        })
    }

    pub fn poll(
        &self,
        request: PollRequest,
        caller_session: Option<&str>,
    ) -> Result<PollResponse, EventError> {
        self.poll_at(request, caller_session, now_unix_ms())
    }

    fn poll_at(
        &self,
        request: PollRequest,
        caller_session: Option<&str>,
        now_ms: u64,
    ) -> Result<PollResponse, EventError> {
        let filter = EventFilter::parse(&request.name, request.arguments, caller_session)?;
        let limit = match request.max_events {
            Some(0) => {
                return Err(EventError::InvalidParams(
                    "maxEvents must be at least 1".to_owned(),
                ));
            }
            Some(n) => n.min(MAX_POLL_EVENTS),
            None => MAX_POLL_EVENTS,
        };

        let state = lock(&self.state);
        let latest_seq = state.latest_seq();
        let Some(raw_cursor) = request.cursor.as_deref() else {
            return Ok(PollResponse {
                result_type: Cow::Borrowed("complete"),
                events: Vec::new(),
                cursor: format_cursor(&self.boot_epoch, latest_seq),
                truncated: false,
                has_more: false,
                next_poll_ms: POLL_NEXT_IDLE_MS,
            });
        };

        let Some((cursor_epoch, cursor_seq)) = parse_cursor(raw_cursor) else {
            return Err(EventError::InvalidParams(format!(
                "invalid cursor {raw_cursor:?}"
            )));
        };

        if cursor_epoch != self.boot_epoch || cursor_seq > latest_seq {
            return Ok(PollResponse {
                result_type: Cow::Borrowed("complete"),
                events: Vec::new(),
                cursor: format_cursor(&self.boot_epoch, latest_seq),
                truncated: true,
                has_more: false,
                next_poll_ms: POLL_NEXT_IDLE_MS,
            });
        }

        let mut truncated = match state.oldest_seq() {
            Some(oldest) => cursor_seq.saturating_add(1) < oldest,
            None => cursor_seq < latest_seq,
        };

        let cutoff_ms = request.max_age_ms.map(|age| now_ms.saturating_sub(age));

        let mut events = Vec::new();
        let mut last_returned_seq = cursor_seq;
        let mut has_more = false;

        for rec in &state.ring {
            if rec.seq <= cursor_seq {
                continue;
            }
            if !filter.matches(rec) {
                continue;
            }
            if let Some(cutoff) = cutoff_ms
                && rec.emitted_ms < cutoff
            {
                truncated = true;
                continue;
            }
            if events.len() < limit {
                last_returned_seq = rec.seq;
                events.push(rec.to_polled());
            } else {
                has_more = true;
                break;
            }
        }

        let next_cursor_seq = if has_more {
            last_returned_seq
        } else {
            latest_seq
        };
        let next_poll_ms = if !events.is_empty() || has_more {
            POLL_NEXT_ACTIVE_MS
        } else {
            POLL_NEXT_IDLE_MS
        };

        Ok(PollResponse {
            result_type: Cow::Borrowed("complete"),
            events,
            cursor: format_cursor(&self.boot_epoch, next_cursor_seq),
            truncated,
            has_more,
            next_poll_ms,
        })
    }

    pub fn subscribe(
        self: &Arc<Self>,
        request: SubscribeRequest,
        caller_session: Option<&str>,
    ) -> Result<EventSubscription, EventError> {
        self.subscribe_at(request, caller_session, now_unix_ms())
    }

    fn subscribe_at(
        self: &Arc<Self>,
        request: SubscribeRequest,
        caller_session: Option<&str>,
        now_ms: u64,
    ) -> Result<EventSubscription, EventError> {
        let filter = EventFilter::parse(&request.name, request.arguments, caller_session)?;
        let heartbeat_ms = request
            .heartbeat_ms
            .unwrap_or(DEFAULT_HEARTBEAT_MS)
            .clamp(MIN_HEARTBEAT_MS, MAX_HEARTBEAT_MS);

        let state = lock(&self.state);
        // Subscribe while holding the lock so no event is missed between backlog and live stream.
        let rx = self.tx.subscribe();
        let latest_seq = state.latest_seq();

        let (initial_cursor_seq, truncated, backlog) = match request.cursor.as_deref() {
            None => (latest_seq, false, VecDeque::new()),
            Some(raw_cursor) => {
                let Some((cursor_epoch, cursor_seq)) = parse_cursor(raw_cursor) else {
                    return Err(EventError::InvalidParams(format!(
                        "invalid cursor {raw_cursor:?}"
                    )));
                };
                if cursor_epoch != self.boot_epoch || cursor_seq > latest_seq {
                    (latest_seq, true, VecDeque::new())
                } else {
                    let mut truncated = match state.oldest_seq() {
                        Some(oldest) => cursor_seq.saturating_add(1) < oldest,
                        None => cursor_seq < latest_seq,
                    };
                    let cutoff_ms = request.max_age_ms.map(|age| now_ms.saturating_sub(age));
                    let mut backlog = VecDeque::new();
                    for rec in &state.ring {
                        if rec.seq <= cursor_seq || !filter.matches(rec) {
                            continue;
                        }
                        if let Some(cutoff) = cutoff_ms
                            && rec.emitted_ms < cutoff
                        {
                            truncated = true;
                            continue;
                        }
                        backlog.push_back(Arc::clone(rec));
                    }
                    let start_seq = backlog
                        .front()
                        .map_or(latest_seq, |record| record.seq.saturating_sub(1));
                    (start_seq, truncated, backlog)
                }
            }
        };

        let initial_active = ActiveNotice {
            cursor: format_cursor(&self.boot_epoch, initial_cursor_seq),
            truncated,
            heartbeat_ms,
        };

        Ok(EventSubscription {
            bus: Arc::clone(self),
            filter,
            heartbeat_ms,
            initial_active,
            backlog,
            rx,
            last_sent_seq: initial_cursor_seq,
        })
    }

    /// Peeks unseen `external.event` metadata for legacy `background()` notices without advancing
    /// the session cursor. Returns `(sanitized_line, newest_seq)` when at least one unseen event
    /// exists. Never includes untrusted `data`.
    pub fn peek_external_background_summary(&self, session_id: &str) -> Option<(String, u64)> {
        let state = lock(&self.state);
        let last_seen = state
            .session_cursors
            .get(session_id)
            .map_or(0, |&(seq, _)| seq);
        let mut total = 0usize;
        let mut newest_seq = last_seen;
        let mut group_order: Vec<String> = Vec::new();
        let mut group_counts: HashMap<String, usize> = HashMap::new();

        for rec in &state.ring {
            if rec.seq <= last_seen {
                continue;
            }
            if let EventMeta::External {
                source,
                topic,
                upstream_name,
                ..
            } = &rec.meta
            {
                total += 1;
                newest_seq = rec.seq;
                let label = format_sanitized_label(source, topic.as_deref(), upstream_name);
                let count = group_counts.entry(label.clone()).or_insert(0);
                if *count == 0 {
                    group_order.push(label);
                }
                *count += 1;
            }
        }

        if total == 0 {
            return None;
        }

        let mut parts = Vec::new();
        for label in group_order.iter().take(MAX_SUMMARY_GROUPS) {
            let count = group_counts[label];
            if count > 1 {
                parts.push(format!("{label} x{count}"));
            } else {
                parts.push(label.clone());
            }
        }
        let extra_groups = group_order.len().saturating_sub(MAX_SUMMARY_GROUPS);
        let suffix = if extra_groups > 0 {
            format!(", +{extra_groups} more")
        } else {
            String::new()
        };
        let line = format!(
            "External events: {total} new ({}{suffix})",
            parts.join(", ")
        );
        Some((line, newest_seq))
    }

    /// Advances the session's `background()` external-event cursor after a notice is committed.
    pub fn commit_external_background_cursor(&self, session_id: &str, seq: u64) {
        let mut state = lock(&self.state);
        state.session_cursor_clock = state.session_cursor_clock.saturating_add(1);
        let tick = state.session_cursor_clock;
        if let Some((cur, last_tick)) = state.session_cursors.get_mut(session_id) {
            if seq > *cur {
                *cur = seq;
            }
            *last_tick = tick;
            return;
        }
        if state.session_cursors.len() >= self.max_session_cursors
            && let Some(oldest_key) = state
                .session_cursors
                .iter()
                .min_by_key(|(_, (_, t))| *t)
                .map(|(k, _)| k.clone())
        {
            state.session_cursors.remove(&oldest_key);
        }
        state
            .session_cursors
            .insert(session_id.to_owned(), (seq, tick));
    }

    /// Removes a session's stored `background()` external-event cursor on session teardown.
    pub fn remove_session(&self, session_id: &str) {
        lock(&self.state).session_cursors.remove(session_id);
    }

    /// Convenience wrapper that reads and immediately commits the session's external-event summary.
    #[cfg(test)]
    pub fn external_background_summary(&self, session_id: &str) -> Option<String> {
        let (summary, seq) = self.peek_external_background_summary(session_id)?;
        self.commit_external_background_cursor(session_id, seq);
        Some(summary)
    }

    #[cfg(test)]
    fn ring_len(&self) -> usize {
        lock(&self.state).ring.len()
    }

    #[cfg(test)]
    fn ring_bytes(&self) -> usize {
        lock(&self.state).ring_bytes
    }

    #[cfg(test)]
    fn dedup_len(&self) -> usize {
        lock(&self.state).dedup.len()
    }
}

pub struct EventSubscription {
    bus: Arc<EventBus>,
    filter: EventFilter,
    heartbeat_ms: u64,
    initial_active: ActiveNotice,
    backlog: VecDeque<Arc<EventRecord>>,
    rx: broadcast::Receiver<Arc<EventRecord>>,
    last_sent_seq: u64,
}

impl EventSubscription {
    pub fn initial_active(&self) -> ActiveNotice {
        self.initial_active.clone()
    }

    pub fn heartbeat_ms(&self) -> u64 {
        self.heartbeat_ms
    }

    pub fn current_cursor(&self) -> String {
        format_cursor(self.bus.boot_epoch(), self.last_sent_seq)
    }

    pub async fn next(&mut self) -> Option<StreamItem> {
        if let Some(rec) = self.backlog.pop_front() {
            self.last_sent_seq = self.last_sent_seq.max(rec.seq);
            return Some(StreamItem::Event(rec.to_stream(self.bus.boot_epoch())));
        }

        loop {
            match self.rx.recv().await {
                Ok(rec) => {
                    if rec.seq <= self.last_sent_seq {
                        continue;
                    }
                    self.last_sent_seq = rec.seq;
                    if self.filter.matches(&rec) {
                        return Some(StreamItem::Event(rec.to_stream(self.bus.boot_epoch())));
                    }
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    // Resubscribe under lock so stale buffered frames cannot trigger a second lag.
                    let (latest_seq, fresh_rx) = {
                        let state = lock(&self.bus.state);
                        (state.latest_seq(), self.bus.tx.subscribe())
                    };
                    self.rx = fresh_rx;
                    self.last_sent_seq = latest_seq;
                    return Some(StreamItem::Active(ActiveNotice {
                        cursor: format_cursor(self.bus.boot_epoch(), latest_seq),
                        truncated: true,
                        heartbeat_ms: self.heartbeat_ms,
                    }));
                }
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    }
}

fn format_cursor(boot_epoch: &str, seq: u64) -> String {
    format!("{boot_epoch}:{seq}")
}

fn format_event_id(boot_epoch: &str, seq: u64) -> String {
    format!("{boot_epoch}-{seq}")
}

fn parse_cursor(raw: &str) -> Option<(&str, u64)> {
    let (epoch, seq_str) = raw.split_once(':')?;
    if epoch.is_empty() || seq_str.is_empty() {
        return None;
    }
    let seq = seq_str.parse::<u64>().ok()?;
    Some((epoch, seq))
}

fn estimate_entry_bytes(name: &str, data_bytes: usize, meta: &EventMeta) -> usize {
    let meta_bytes = match meta {
        EventMeta::TaskExited {
            task_id,
            session_id,
        } => task_id.len() + session_id.as_ref().map_or(0, String::len),
        EventMeta::External {
            source,
            topic,
            upstream_name,
            upstream_event_id,
        } => {
            // Account for both stored metadata and the dedup index key copy.
            source.len().saturating_mul(2)
                + topic.as_ref().map_or(0, String::len)
                + upstream_name.len()
                + upstream_event_id.len().saturating_mul(2)
                + DEDUP_FIXED_OVERHEAD
        }
    };
    ENTRY_FIXED_OVERHEAD
        .saturating_add(name.len())
        .saturating_add(data_bytes)
        .saturating_add(meta_bytes)
}

fn sanitize_label_part(raw: &str) -> String {
    let mut out = String::new();
    let mut last_space = false;
    for ch in raw.chars() {
        if out.chars().count() >= MAX_SUMMARY_LABEL_CHARS {
            break;
        }
        if ch.is_control() || ch == '\u{007f}' {
            if !last_space && !out.is_empty() {
                out.push(' ');
                last_space = true;
            }
            continue;
        }
        let cleaned = match ch {
            '/' | ',' | '(' | ')' => '_',
            other => other,
        };
        if cleaned.is_whitespace() {
            if !last_space && !out.is_empty() {
                out.push(' ');
                last_space = true;
            }
        } else {
            out.push(cleaned);
            last_space = false;
        }
    }
    let trimmed = out.trim();
    if trimmed.is_empty() {
        "?".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn format_sanitized_label(source: &str, topic: Option<&str>, name: &str) -> String {
    let source = sanitize_label_part(source);
    let name = sanitize_label_part(name);
    match topic.filter(|t| !t.trim().is_empty()) {
        Some(topic) => format!("{source}/{}/{name}", sanitize_label_part(topic)),
        None => format!("{source}/{name}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_bus(
        max_events: usize,
        max_bytes: usize,
        broadcast_cap: usize,
        pool_limit: usize,
    ) -> (Arc<EventBus>, Arc<BytePool>) {
        let pool = BytePool::new(pool_limit);
        let bus = Arc::new(EventBus::with_config(
            "ep000001".to_owned(),
            max_events,
            max_bytes,
            broadcast_cap,
            Arc::clone(&pool),
        ));
        (bus, pool)
    }

    fn ext_input(source: &str, id: &str, name: &str, data: Value) -> ExternalEventInput {
        ExternalEventInput {
            event_type: None,
            source: source.to_owned(),
            topic: Some("pr".to_owned()),
            name: name.to_owned(),
            upstream_event_id: id.to_owned(),
            data,
            upstream_truncated: None,
        }
    }

    #[test]
    fn list_event_types_exposes_only_v1_events_with_schemas() {
        let (bus, _) = test_bus(64, 1 << 20, 16, 2 << 20);
        let list = bus.list_event_types();
        assert_eq!(list.result_type, "complete");
        let names: Vec<&str> = list.events.iter().map(|e| e.name.as_ref()).collect();
        assert_eq!(names, vec![EVENT_TASK_EXITED, EVENT_EXTERNAL]);
        for desc in &list.events {
            let delivery: Vec<&str> = desc.delivery.iter().map(AsRef::as_ref).collect();
            assert_eq!(delivery, vec!["push", "poll"]);
            assert_eq!(desc.input_schema["type"], "object");
            assert_eq!(desc.payload_schema["type"], "object");
        }
    }

    #[test]
    fn ring_eviction_releases_leases_before_charging_new_events() {
        // Measure one event's exact charge, then set max_events=2 and pool_limit=2*charge.
        let (probe_bus, probe_pool) = test_bus(10, 1 << 20, 8, 1 << 20);
        probe_bus
            .publish_external_at(ext_input("gh", "e1", "push", json!({"x": 1})), 1000)
            .unwrap();
        let single_charge = probe_pool.used();
        assert!(single_charge > 0);
        drop(probe_bus);
        assert_eq!(probe_pool.used(), 0);

        let (bus, pool) = test_bus(2, 1 << 20, 8, single_charge * 2);
        bus.publish_external_at(ext_input("gh", "e1", "push", json!({"x": 1})), 1000)
            .unwrap();
        bus.publish_external_at(ext_input("gh", "e2", "push", json!({"x": 2})), 1001)
            .unwrap();
        assert_eq!(pool.used(), single_charge * 2);

        // Third publish only succeeds because oldest lease is released before charging.
        bus.publish_external_at(ext_input("gh", "e3", "push", json!({"x": 3})), 1002)
            .unwrap();
        assert_eq!(bus.ring_len(), 2);
        assert_eq!(bus.dedup_len(), 2);
        assert_eq!(pool.used(), single_charge * 2);

        drop(bus);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn concurrent_publications_reach_subscribers_in_sequence_order() {
        const WRITERS: usize = 16;
        const PER_WRITER: usize = 64;
        let count = WRITERS * PER_WRITER;
        let (bus, _) = test_bus(count, 8 << 20, count, 16 << 20);
        let mut rx = bus.tx.subscribe();
        let start = std::sync::Barrier::new(WRITERS);
        std::thread::scope(|scope| {
            for writer in 0..WRITERS {
                let bus = &bus;
                let start = &start;
                scope.spawn(move || {
                    start.wait();
                    for index in 0..PER_WRITER {
                        bus.publish_external(ext_input(
                            "parallel",
                            &format!("{writer}-{index}"),
                            "ready",
                            json!({}),
                        ))
                        .unwrap();
                    }
                });
            }
        });
        for seq in 1..=count as u64 {
            assert_eq!(rx.try_recv().unwrap().seq, seq);
        }
        assert!(matches!(
            rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn evicted_events_remain_charged_until_subscribers_release_them() {
        for replay in [false, true] {
            let (bus, pool) = test_bus(1, 1 << 20, 8, 2 << 20);
            let cursor = bus.current_cursor();
            if replay {
                bus.publish_external(ext_input("src", "e1", "ready", json!({})))
                    .unwrap();
            }
            let sub = bus
                .subscribe(
                    SubscribeRequest {
                        name: EVENT_EXTERNAL.to_owned(),
                        cursor: Some(cursor),
                        ..Default::default()
                    },
                    None,
                )
                .unwrap();
            if !replay {
                bus.publish_external(ext_input("src", "e1", "ready", json!({})))
                    .unwrap();
            }
            let charge = pool.used();
            bus.publish_external(ext_input("src", "e2", "ready", json!({})))
                .unwrap();
            assert_eq!(bus.ring_len(), 1);
            assert_eq!(pool.used(), charge * 2, "replay={replay}");
            drop(sub);
            assert_eq!(pool.used(), charge, "replay={replay}");
            drop(bus);
            assert_eq!(pool.used(), 0);
        }
    }

    #[test]
    fn ring_byte_limit_evicts_oldest_and_respects_64kib_external_cap() {
        let (bus, pool) = test_bus(100, 1024, 8, 1 << 20);
        for i in 0..10 {
            bus.publish_external_at(
                ext_input(
                    "src",
                    &format!("id-{i}"),
                    "ev",
                    json!({"pad": "a".repeat(200)}),
                ),
                1000 + i,
            )
            .unwrap();
        }
        assert!(bus.ring_bytes() <= 1024);
        assert_eq!(pool.used(), bus.ring_bytes());
        assert!(bus.ring_len() < 10);

        // Oversized external.event (> 64 KiB) is rejected.
        let big = ext_input(
            "src",
            "too-big",
            "ev",
            json!({"blob": "x".repeat(MAX_EXTERNAL_EVENT_BYTES)}),
        );
        let err = bus.publish_external(big).unwrap_err();
        assert_eq!(err.code(), -32602);
    }

    #[test]
    fn external_dedup_shares_ring_lifecycle_and_blocks_task_forgery() {
        let (bus, _) = test_bus(2, 1 << 20, 8, 1 << 20);

        let mut forged = ext_input("gh", "f1", "fake", json!({}));
        forged.event_type = Some(EVENT_TASK_EXITED.to_owned());
        assert_eq!(bus.publish_external(forged).unwrap_err().code(), -32024);

        let mut forged_prefix = ext_input("gh", "f2", "fake", json!({}));
        forged_prefix.event_type = Some("task.matched".to_owned());
        assert_eq!(
            bus.publish_external(forged_prefix).unwrap_err().code(),
            -32024
        );

        let first = bus
            .publish_external_at(ext_input("gh", "u1", "pr.merged", json!({"n": 1})), 1000)
            .unwrap();
        assert!(!first.deduplicated);

        let dup = bus
            .publish_external_at(ext_input("gh", "u1", "pr.merged", json!({"n": 999})), 1005)
            .unwrap();
        assert!(dup.deduplicated);
        assert_eq!(dup.cursor, first.cursor);
        assert_eq!(dup.event_id, first.event_id);
        assert_eq!(bus.ring_len(), 1);

        // Push two more events to evict `u1` from the 2-slot ring; dedup entry must also evict.
        bus.publish_external_at(ext_input("gh", "u2", "pr.merged", json!({})), 1010)
            .unwrap();
        bus.publish_external_at(ext_input("gh", "u3", "pr.merged", json!({})), 1020)
            .unwrap();
        assert_eq!(bus.dedup_len(), 2);

        let readded = bus
            .publish_external_at(ext_input("gh", "u1", "pr.merged", json!({"n": 2})), 1030)
            .unwrap();
        assert!(!readded.deduplicated);
        assert_ne!(readded.cursor, first.cursor);
    }

    #[test]
    fn publish_task_exited_preserves_fields_and_strict_json_integers() {
        let (bus, _) = test_bus(16, 1 << 20, 8, 1 << 20);
        let input = TaskExitedInput {
            task_id: "abcd1234-1".to_owned(),
            session_id: Some("sess-a".to_owned()),
            state: "exited".to_owned(),
            exit_code: Some(0),
            expired: false,
            kept_processes: 2,
            tree_ended: false,
            log_path: Some("/tmp/task.log".to_owned()),
        };
        let start_cursor = bus.current_cursor();
        bus.publish_task_exited_at(input, 5000).unwrap();

        let resp = bus
            .poll_at(
                PollRequest {
                    name: EVENT_TASK_EXITED.to_owned(),
                    arguments: Some(json!({ "scope": "session" })),
                    cursor: Some(start_cursor.clone()),
                    max_age_ms: None,
                    max_events: None,
                },
                Some("sess-a"),
                5010,
            )
            .unwrap();
        assert_eq!(resp.events.len(), 1);
        let data = &resp.events[0].data;
        assert_eq!(data["taskId"], "abcd1234-1");
        assert_eq!(data["state"], "exited");
        assert!(data["exitCode"].is_i64());
        assert_eq!(data["exitCode"].as_i64(), Some(0));
        assert_eq!(data["expired"], false);
        assert!(data["keptProcesses"].is_u64());
        assert_eq!(data["keptProcesses"].as_u64(), Some(2));
        assert_eq!(data["treeEnded"], false);
        assert_eq!(data["logPath"], "/tmp/task.log");

        // Another session with scope=session must not see sess-a's event.
        let other = bus
            .poll_at(
                PollRequest {
                    name: EVENT_TASK_EXITED.to_owned(),
                    arguments: Some(json!({ "scope": "session" })),
                    cursor: Some(start_cursor),
                    max_age_ms: None,
                    max_events: None,
                },
                Some("sess-b"),
                5010,
            )
            .unwrap();
        assert!(other.events.is_empty());
    }

    #[test]
    fn poll_cursor_null_boot_epoch_eviction_max_age_and_pagination() {
        let (bus, _) = test_bus(100, 2 << 20, 16, 4 << 20);
        for i in 1..=70u64 {
            bus.publish_external_at(
                ext_input("ci", &format!("e{i}"), "build", json!({ "i": i })),
                1000 + i * 100,
            )
            .unwrap();
        }

        // Missing cursor (same as JSON cursor: null) starts at tail and ignores maxAgeMs.
        let tail = bus
            .poll_at(
                PollRequest {
                    name: EVENT_EXTERNAL.to_owned(),
                    arguments: None,
                    cursor: None,
                    max_age_ms: Some(1),
                    max_events: Some(10),
                },
                None,
                999_999,
            )
            .unwrap();
        assert!(tail.events.is_empty());
        assert!(!tail.truncated);
        assert!(!tail.has_more);
        assert_eq!(tail.next_poll_ms, POLL_NEXT_IDLE_MS);
        assert_eq!(tail.cursor, "ep000001:70");

        // Mismatched boot_epoch sets truncated=true and returns current valid cursor.
        let wrong_epoch = bus
            .poll_at(
                PollRequest {
                    name: EVENT_EXTERNAL.to_owned(),
                    cursor: Some("old00000:1".to_owned()),
                    ..Default::default()
                },
                None,
                10_000,
            )
            .unwrap();
        assert!(wrong_epoch.truncated);
        assert!(wrong_epoch.events.is_empty());
        assert_eq!(wrong_epoch.cursor, "ep000001:70");

        // Pagination caps at min(maxEvents, 64), sets hasMore and nextPollMs=2000, and omits cursor on items.
        let page1 = bus
            .poll_at(
                PollRequest {
                    name: EVENT_EXTERNAL.to_owned(),
                    cursor: Some("ep000001:0".to_owned()),
                    max_events: Some(200),
                    ..Default::default()
                },
                None,
                10_000,
            )
            .unwrap();
        assert_eq!(page1.events.len(), 64);
        assert!(page1.has_more);
        assert!(!page1.truncated);
        assert_eq!(page1.next_poll_ms, POLL_NEXT_ACTIVE_MS);
        assert_eq!(page1.cursor, "ep000001:64");
        let serialized_item = serde_json::to_value(&page1.events[0]).unwrap();
        assert!(serialized_item.get("cursor").is_none());

        let page2 = bus
            .poll_at(
                PollRequest {
                    name: EVENT_EXTERNAL.to_owned(),
                    cursor: Some(page1.cursor),
                    max_events: Some(64),
                    ..Default::default()
                },
                None,
                10_000,
            )
            .unwrap();
        assert_eq!(page2.events.len(), 6);
        assert!(!page2.has_more);
        assert_eq!(page2.next_poll_ms, POLL_NEXT_ACTIVE_MS);
        assert_eq!(page2.cursor, "ep000001:70");

        // Non-empty cursor + maxAgeMs skips expired events and sets truncated=true.
        // Event 70 is at 8000ms, event 69 at 7900ms; at now=8050 with maxAgeMs=100, cutoff=7950.
        let aged = bus
            .poll_at(
                PollRequest {
                    name: EVENT_EXTERNAL.to_owned(),
                    cursor: Some("ep000001:68".to_owned()),
                    max_age_ms: Some(100),
                    ..Default::default()
                },
                None,
                8050,
            )
            .unwrap();
        assert!(aged.truncated);
        assert_eq!(aged.events.len(), 1);
        assert_eq!(aged.events[0].data["data"]["i"], 70);

        // Evicted seq sets truncated=true and still delivers surviving ring records.
        let (small_bus, _) = test_bus(2, 1 << 20, 8, 1 << 20);
        for i in 1..=4u64 {
            small_bus
                .publish_external_at(ext_input("ci", &format!("s{i}"), "ev", json!({"i": i})), i)
                .unwrap();
        }
        let evicted = small_bus
            .poll_at(
                PollRequest {
                    name: EVENT_EXTERNAL.to_owned(),
                    cursor: Some("ep000001:1".to_owned()),
                    ..Default::default()
                },
                None,
                100,
            )
            .unwrap();
        assert!(evicted.truncated);
        assert_eq!(evicted.events.len(), 2);
        assert_eq!(evicted.events[0].data["data"]["i"], 3);
        assert_eq!(evicted.events[1].data["data"]["i"], 4);
        assert_eq!(evicted.cursor, "ep000001:4");

        // Unknown event type returns -32023 NotFound with kind=event.
        let not_found = small_bus
            .poll(
                PollRequest {
                    name: "task.matched".to_owned(),
                    ..Default::default()
                },
                None,
            )
            .unwrap_err();
        assert_eq!(not_found.code(), -32023);
        assert_eq!(not_found.data(), Some(json!({ "kind": "event" })));
    }

    #[tokio::test]
    async fn initial_active_cursor_keeps_expired_prefix_out_of_reconnect_backlog() {
        let (bus, _) = test_bus(32, 1 << 20, 8, 1 << 20);
        for (id, at) in [("old", 1_000), ("fresh", 5_000), ("later", 5_500)] {
            bus.publish_external_at(ext_input("src", id, "ready", json!({})), at)
                .unwrap();
        }
        let sub = bus
            .subscribe_at(
                SubscribeRequest {
                    name: EVENT_EXTERNAL.to_owned(),
                    cursor: Some("ep000001:0".to_owned()),
                    max_age_ms: Some(1_000),
                    ..Default::default()
                },
                None,
                5_600,
            )
            .unwrap();
        let active = sub.initial_active();
        assert!(active.truncated);
        assert_eq!(active.cursor, "ep000001:1");
        drop(sub);

        let mut resumed = bus
            .subscribe_at(
                SubscribeRequest {
                    name: EVENT_EXTERNAL.to_owned(),
                    cursor: Some(active.cursor),
                    ..Default::default()
                },
                None,
                5_600,
            )
            .unwrap();
        for id in ["fresh", "later"] {
            let StreamItem::Event(event) = resumed.next().await.unwrap() else {
                panic!("expected replay event");
            };
            assert_eq!(event.data["upstreamEventId"], id);
        }
        assert_eq!(resumed.current_cursor(), "ep000001:3");

        let expired = bus
            .subscribe_at(
                SubscribeRequest {
                    name: EVENT_EXTERNAL.to_owned(),
                    cursor: Some("ep000001:0".to_owned()),
                    max_age_ms: Some(100),
                    ..Default::default()
                },
                None,
                10_000,
            )
            .unwrap();
        assert!(expired.initial_active().truncated);
        assert_eq!(expired.initial_active().cursor, "ep000001:3");
    }

    #[tokio::test]
    async fn subscribe_streams_live_events_and_recovers_slow_consumer_with_truncated_active() {
        let (bus, _) = test_bus(32, 1 << 20, 2, 1 << 20);
        let mut sub = bus
            .subscribe(
                SubscribeRequest {
                    name: EVENT_EXTERNAL.to_owned(),
                    heartbeat_ms: Some(120_000),
                    ..Default::default()
                },
                None,
            )
            .unwrap();
        let active = sub.initial_active();
        assert_eq!(active.heartbeat_ms, MAX_HEARTBEAT_MS);
        assert!(!active.truncated);
        assert_eq!(active.cursor, "ep000001:0");

        // Normal live event.
        bus.publish_external(ext_input("gh", "live-1", "pr", json!({"ok": true})))
            .unwrap();
        match sub.next().await.unwrap() {
            StreamItem::Event(ev) => {
                assert_eq!(ev.cursor, "ep000001:1");
                assert_eq!(ev.name, EVENT_EXTERNAL);
            }
            other => panic!("expected live event, got {other:?}"),
        }

        // Overflow broadcast_cap=2 to trigger slow-consumer active(truncated: true).
        for i in 2..=5 {
            bus.publish_external(ext_input("gh", &format!("lag-{i}"), "pr", json!({"i": i})))
                .unwrap();
        }
        match sub.next().await.unwrap() {
            StreamItem::Active(notice) => {
                assert!(notice.truncated);
                assert_eq!(notice.cursor, "ep000001:5");
                assert_eq!(notice.heartbeat_ms, MAX_HEARTBEAT_MS);
            }
            other => panic!("expected truncated active notice, got {other:?}"),
        }

        // Stream stays alive and delivers subsequent events.
        bus.publish_external(ext_input("gh", "after-lag", "pr", json!({"ok": 6})))
            .unwrap();
        match sub.next().await.unwrap() {
            StreamItem::Event(ev) => {
                assert_eq!(ev.cursor, "ep000001:6");
                assert_eq!(ev.data["data"]["ok"], 6);
            }
            other => panic!("expected post-lag event, got {other:?}"),
        }
    }

    #[test]
    fn legacy_background_summary_tracks_per_session_cursor_and_excludes_raw_data() {
        let (bus, _) = test_bus(32, 1 << 20, 8, 1 << 20);
        bus.publish_external(ExternalEventInput {
            event_type: None,
            source: "git\nhub".to_owned(),
            topic: Some("pr\r\x1b[31m".to_owned()),
            name: "merged".to_owned(),
            upstream_event_id: "id-1".to_owned(),
            data: json!({"secret": "DO_NOT_LEAK_THIS_PAYLOAD"}),
            upstream_truncated: None,
        })
        .unwrap();
        bus.publish_external(ExternalEventInput {
            event_type: None,
            source: "git\nhub".to_owned(),
            topic: Some("pr\r\x1b[31m".to_owned()),
            name: "merged".to_owned(),
            upstream_event_id: "id-2".to_owned(),
            data: json!({"secret": "ANOTHER_SECRET"}),
            upstream_truncated: None,
        })
        .unwrap();

        let s1 = bus.external_background_summary("sess-1").unwrap();
        assert!(!s1.contains('\n'));
        assert!(!s1.contains('\r'));
        assert!(!s1.contains("DO_NOT_LEAK"));
        assert!(!s1.contains("ANOTHER_SECRET"));
        assert!(s1.contains("2 new"));
        assert!(s1.contains("x2"));

        // Second read for sess-1 sees nothing new; sess-2 still sees both events.
        assert!(bus.external_background_summary("sess-1").is_none());
        let s2 = bus.external_background_summary("sess-2").unwrap();
        assert_eq!(s1, s2);
    }

    #[test]
    fn rfc3339_formatting_matches_known_epoch_timestamps() {
        assert_eq!(format_rfc3339_ms(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(
            format_rfc3339_ms(1_700_000_000_123),
            "2023-11-14T22:13:20.123Z"
        );
    }

    #[test]
    fn remove_session_clears_cursor_and_evicts_oldest_when_capped() {
        let pool = BytePool::new(1 << 20);
        let mut bus = EventBus::with_config("ep000002".to_owned(), 32, 1 << 20, 16, pool);
        bus.max_session_cursors = 3;
        bus.publish_external(ext_input("gh", "e1", "push", json!({})))
            .unwrap();

        assert!(bus.external_background_summary("s1").is_some());
        assert!(bus.external_background_summary("s1").is_none());
        bus.remove_session("s1");
        assert!(bus.external_background_summary("s1").is_some());

        // Touch s2, s3, then s4: cap is 3 so oldest (s1) is evicted.
        assert!(bus.external_background_summary("s2").is_some());
        assert!(bus.external_background_summary("s3").is_some());
        assert!(bus.external_background_summary("s4").is_some());
        assert_eq!(lock(&bus.state).session_cursors.len(), 3);
        assert!(!lock(&bus.state).session_cursors.contains_key("s1"));
        assert!(lock(&bus.state).session_cursors.contains_key("s4"));
    }

    #[test]
    fn stream_guard_enforces_active_stream_limit_and_releases_on_drop() {
        let pool = BytePool::new(1 << 20);
        let mut raw_bus = EventBus::with_config("ep000003".to_owned(), 16, 1 << 20, 8, pool);
        raw_bus.max_streams = 2;
        let bus = Arc::new(raw_bus);

        let g1 = bus.try_acquire_stream().unwrap();
        let g2 = bus.try_acquire_stream().unwrap();
        assert_eq!(bus.active_streams(), 2);

        let err = bus.try_acquire_stream().err().unwrap();
        assert_eq!(err.code(), -32025);
        assert_eq!(
            err.data(),
            Some(json!({ "limit": "active_event_streams", "max": 2 }))
        );

        drop(g1);
        assert_eq!(bus.active_streams(), 1);
        let _g3 = bus.try_acquire_stream().unwrap();
        assert_eq!(bus.active_streams(), 2);
        drop(g2);
        drop(_g3);
        assert_eq!(bus.active_streams(), 0);
    }
}
