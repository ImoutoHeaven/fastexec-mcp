//! Owned MCP request buffers and non-blocking admission before parsing.
use crate::lease::Owner;
use crate::lease::{self, BytePool, Lease};
use rmcp::model::ClientJsonRpcMessage;
use serde::Deserialize;
use std::sync::Arc;
const SHORT_REQUEST: usize = 64 * 1024;
const SHORT_PEAK: usize = 8 * SHORT_REQUEST;
pub(crate) struct Buffer {
    bytes: Vec<u8>,
    pool: Arc<BytePool>,
    control: Arc<BytePool>,
    in_control: bool,
    lease: Lease,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Failure {
    Limit,
    Budget,
    Syntax,
    Data,
}
pub(crate) struct Parsed {
    pub message: ClientJsonRpcMessage,
    pub owner: Owner,
}
impl Default for Buffer {
    fn default() -> Self {
        Self::new(
            Arc::clone(lease::process_pool()),
            Arc::clone(lease::control_pool()),
        )
    }
}
impl std::ops::Deref for Buffer {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}
impl Buffer {
    pub fn new(pool: Arc<BytePool>, control: Arc<BytePool>) -> Self {
        let lease = Lease::empty(&pool);
        Self {
            bytes: Vec::new(),
            pool,
            control,
            in_control: false,
            lease,
        }
    }
    fn resize(&mut self, target: usize, input_len: usize) -> bool {
        let short = input_len <= SHORT_REQUEST && target <= SHORT_PEAK;
        if (!self.in_control || short) && self.lease.try_resize(target) {
            return true;
        }
        if let Some(lease) = self.pool.try_acquire(target) {
            self.lease = lease;
            self.in_control = false;
            return true;
        }
        if short && let Some(lease) = self.control.try_acquire(target) {
            self.lease = lease;
            self.in_control = true;
            return true;
        }
        false
    }
    pub fn append(&mut self, bytes: &[u8], limit: usize) -> Result<(), Failure> {
        let needed = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or(Failure::Limit)?;
        if needed > limit {
            return Err(Failure::Limit);
        }
        if needed > self.bytes.capacity() {
            let preferred = self
                .bytes
                .capacity()
                .saturating_mul(2)
                .max(needed)
                .min(limit);
            let mut admitted = None;
            for capacity in [preferred, needed] {
                if self.resize(self.bytes.capacity().saturating_add(capacity), needed) {
                    admitted = Some(capacity);
                    break;
                }
            }
            let capacity = admitted.ok_or(Failure::Budget)?;
            let mut replacement = Vec::with_capacity(capacity);
            replacement.extend_from_slice(&self.bytes);
            self.bytes = replacement;
            self.lease.shrink_to(self.bytes.capacity());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
    pub fn parse(mut self, stdio: bool) -> Result<Option<Parsed>, Failure> {
        let len = self.bytes.len();
        if !self.resize(self.bytes.capacity().saturating_add(3 * len), len) {
            return Err(Failure::Budget);
        }
        let raw = if stdio {
            self.bytes
                .strip_prefix(crate::ingress::UTF8_BOM)
                .unwrap_or(&self.bytes)
        } else {
            &self.bytes
        };
        if stdio && raw.iter().all(u8::is_ascii_whitespace) {
            return Ok(None);
        }
        // Slice decoding can retain twice the longest escaped token as scratch,
        // alongside the decoded Value and original bytes.
        let value: serde_json::Value =
            serde_json::from_slice(raw).map_err(|error| match error.classify() {
                serde_json::error::Category::Syntax | serde_json::error::Category::Eof => {
                    Failure::Syntax
                }
                _ => Failure::Data,
            })?;
        // Borrow the Value to avoid owned serde Content string copies. Retained
        // space covers SDK params, metadata, tool conversion and the request ID.
        let retained = 4 * len;
        let peak = self
            .bytes
            .capacity()
            .saturating_add(value_capacity(&value))
            .saturating_add(retained);
        if !self.resize(peak, len) {
            return Err(Failure::Budget);
        }
        let parsed = ClientJsonRpcMessage::deserialize(&value);
        if parsed.is_err()
            && stdio
            && let Some(method) = value.get("method").and_then(serde_json::Value::as_str)
            && ((value.get("id").is_none() && !crate::ingress::is_standard_mcp_method(method))
                || (method.starts_with("notifications/")
                    && !crate::ingress::is_standard_mcp_notification(method)))
        {
            return Ok(None);
        }
        let message = parsed.map_err(|_| Failure::Data)?;
        drop(value);
        drop(std::mem::take(&mut self.bytes));
        self.lease.shrink_to(retained);
        // A short input may borrow control space for classification. Ordinary
        // work and long-lived peer metadata must return to the main pool before
        // dispatch, so media pressure cannot consume the control reserve.
        if self.in_control && !is_control(&message) {
            let Some(lease) = self.pool.try_acquire(retained) else {
                drop(message);
                return Err(Failure::Budget);
            };
            self.lease = lease;
            self.in_control = false;
        }
        Ok(Some(Parsed {
            message,
            owner: Owner::new(std::sync::Mutex::new(self.lease)),
        }))
    }
}
fn is_control(message: &ClientJsonRpcMessage) -> bool {
    use rmcp::model::{ClientNotification, ClientRequest, JsonRpcMessage};
    match message {
        JsonRpcMessage::Request(request) => match &request.request {
            ClientRequest::PingRequest(_)
            | ClientRequest::ListToolsRequest(_)
            | ClientRequest::DiscoverRequest(_) => true,
            ClientRequest::CallToolRequest(call) => {
                call.params.name == "exec"
                    && call
                        .params
                        .arguments
                        .as_ref()
                        .and_then(|arguments| arguments.get("action"))
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|action| matches!(action, "list" | "kill"))
            }
            _ => false,
        },
        JsonRpcMessage::Notification(notification) => matches!(
            notification.notification,
            ClientNotification::CancelledNotification(_)
                | ClientNotification::InitializedNotification(_)
        ),
        _ => false,
    }
}

fn value_capacity(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::String(s) => s.capacity(),
        serde_json::Value::Array(items) => items.iter().map(value_capacity).sum(),
        serde_json::Value::Object(map) => map
            .iter()
            .map(|(key, value)| key.capacity() + value_capacity(value))
            .sum(),
        _ => 0,
    }
}
impl Parsed {
    pub fn into_message(self) -> ClientJsonRpcMessage {
        use rmcp::model::{GetExtensions, JsonRpcMessage};
        let Self { mut message, owner } = self;
        match &mut message {
            JsonRpcMessage::Request(request) => {
                request.request.extensions_mut().insert(owner);
            }
            JsonRpcMessage::Notification(notification) => {
                notification.notification.extensions_mut().insert(owner);
            }
            // The official SDK has no response/peer-info owner hook. Its later
            // copies and queues are outside this application's byte accounting.
            JsonRpcMessage::Response(_) | JsonRpcMessage::Error(_) => {}
        }
        message
    }
}
// A cancellation tombstone keeps only its own ID allocation after the rest of
// the notification is consumed. Transfer that portion without charging twice.
pub(crate) fn retain_id(owner: &Owner, id: &rmcp::model::RequestId) -> Owner {
    let bytes = match id {
        rmcp::model::RequestId::String(id) => id.len(),
        _ => 0,
    };
    if bytes == 0 {
        return Owner::default();
    }
    let Some(held) = owner.get::<std::sync::Mutex<Lease>>() else {
        return owner.clone();
    };
    let mut held = held.lock().unwrap_or_else(|error| error.into_inner());
    if bytes > held.bytes() {
        return owner.clone();
    }
    Owner::new(held.split_off(bytes))
}

pub(crate) const UTF8_BOM: &[u8; 3] = b"\xEF\xBB\xBF";

pub(crate) fn is_standard_mcp_notification(method: &str) -> bool {
    matches!(
        method,
        "notifications/cancelled"
            | "notifications/initialized"
            | "notifications/message"
            | "notifications/progress"
            | "notifications/prompts/list_changed"
            | "notifications/resources/list_changed"
            | "notifications/resources/updated"
            | "notifications/roots/list_changed"
            | "notifications/subscriptions/acknowledged"
            | "notifications/tools/list_changed"
    )
}

pub(crate) fn is_standard_mcp_method(method: &str) -> bool {
    matches!(
        method,
        "initialize"
            | "ping"
            | "server/discover"
            | "prompts/get"
            | "prompts/list"
            | "resources/list"
            | "resources/read"
            | "resources/subscribe"
            | "resources/unsubscribe"
            | "resources/templates/list"
            | "subscriptions/listen"
            | "tools/call"
            | "tools/list"
            | "completion/complete"
            | "elicitation/create"
            | "logging/setLevel"
            | "roots/list"
            | "sampling/createMessage"
    ) || is_standard_mcp_notification(method)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::{GetExtensions, JsonRpcMessage};

    fn buffered(raw: &[u8], pool: &Arc<BytePool>) -> Buffer {
        let mut buffer = Buffer::new(Arc::clone(pool), BytePool::new(0));
        buffer.append(raw, crate::http::LIMIT).unwrap();
        buffer
    }

    #[test]
    fn growth_charges_old_and_new_capacity_and_refuses_before_allocating() {
        for (limit, fits) in [(32, false), (33, true)] {
            let pool = BytePool::new(limit);
            let mut buffer = buffered(b"1234567890123456", &pool);
            assert_eq!(pool.used(), 16);
            assert_eq!(buffer.append(b"7", 64).is_ok(), fits);
            assert_eq!(buffer.len(), if fits { 17 } else { 16 });
            assert_eq!(pool.used(), buffer.capacity());
            drop(buffer);
            assert_eq!(pool.used(), 0);
        }
    }

    #[test]
    fn parse_peak_is_admitted_before_sdk_strings_are_copied() {
        let raw = br#"{"jsonrpc":"2.0","id":"caf\u00e9","method":"ping"}"#;
        let value: serde_json::Value = serde_json::from_slice(raw).unwrap();
        let peak = raw.len() * 5 + value_capacity(&value);
        let refused = BytePool::new(peak - 1);
        assert!(matches!(
            buffered(raw, &refused).parse(true),
            Err(Failure::Budget)
        ));
        assert_eq!(refused.used(), 0);
        let pool = BytePool::new(peak);
        let message = buffered(raw, &pool)
            .parse(true)
            .unwrap()
            .unwrap()
            .into_message();
        assert_eq!(pool.used(), 4 * raw.len());
        assert_eq!(serde_json::to_value(&message).unwrap(), value);
        drop(message);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn control_reserve_accepts_short_requests_but_cannot_grow_large_ones() {
        let main = BytePool::new(0);
        let control = BytePool::new(SHORT_PEAK);
        let mut short = Buffer::new(Arc::clone(&main), Arc::clone(&control));
        short
            .append(
                br#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#,
                SHORT_REQUEST,
            )
            .unwrap();
        let message = short.parse(false).unwrap().unwrap().into_message();
        assert!(control.used() > 0);
        assert_eq!(main.used(), 0);
        drop(message);
        assert_eq!(control.used(), 0);
        let mut growing = Buffer::new(main, Arc::clone(&control));
        growing
            .append(&vec![b' '; SHORT_REQUEST], SHORT_REQUEST + 1)
            .unwrap();
        assert_eq!(
            growing.append(b" ", SHORT_REQUEST + 1),
            Err(Failure::Budget)
        );
        assert_eq!(growing.len(), SHORT_REQUEST);
        drop(growing);
        assert_eq!(control.used(), 0);
    }

    #[test]
    fn ordinary_calls_and_peer_metadata_leave_control_before_dispatch() {
        let ordinary = br#"{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"read","arguments":{"path":"sample.txt"}}}"#;
        let initialize = br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"reader","version":"1"}}}"#;
        let control = BytePool::new(SHORT_PEAK);
        for raw in [ordinary.as_slice(), initialize.as_slice()] {
            let mut buffer = Buffer::new(BytePool::new(0), Arc::clone(&control));
            buffer.append(raw, SHORT_REQUEST).unwrap();
            assert!(matches!(buffer.parse(false), Err(Failure::Budget)));
            assert_eq!(control.used(), 0);
        }
        for action in ["list", "kill"] {
            let raw = format!(
                r#"{{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{{"name":"exec","arguments":{{"action":"{action}"}}}}}}"#
            );
            let mut buffer = Buffer::new(BytePool::new(0), Arc::clone(&control));
            buffer.append(raw.as_bytes(), SHORT_REQUEST).unwrap();
            let message = buffer.parse(false).unwrap().unwrap().into_message();
            assert!(control.used() > 0);
            drop(message);
            assert_eq!(control.used(), 0);
        }
        let main = BytePool::new(4096);
        let blocker = main.try_acquire(4096).unwrap();
        let mut buffer = Buffer::new(Arc::clone(&main), Arc::clone(&control));
        buffer.append(ordinary, SHORT_REQUEST).unwrap();
        drop(blocker);
        let message = buffer.parse(false).unwrap().unwrap().into_message();
        assert_eq!(control.used(), 0);
        assert_eq!(main.used(), request_charge(ordinary));
        drop(message);
        assert_eq!(main.used(), 0);
    }

    fn request_charge(raw: &[u8]) -> usize {
        4 * raw.len()
    }

    #[test]
    fn cancellation_tombstone_transfers_only_the_shared_id_charge() {
        let raw = br#"{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"one","reason":"done"}}"#;
        let pool = BytePool::new(4096);
        let message = buffered(raw, &pool)
            .parse(false)
            .unwrap()
            .unwrap()
            .into_message();
        let JsonRpcMessage::Notification(notification) = &message else {
            panic!("notification")
        };
        let rmcp::model::ClientNotification::CancelledNotification(cancel) =
            &notification.notification
        else {
            panic!("cancel")
        };
        let id = cancel.params.request_id.clone().unwrap();
        let owner = notification
            .notification
            .extensions()
            .get::<Owner>()
            .unwrap()
            .clone();
        let requests = crate::mcp::RequestLeases::default();
        assert!(requests.on_cancel_owned(&id, owner));
        assert_eq!(pool.used(), request_charge(raw));
        drop(message);
        assert_eq!(pool.used(), 3);
        requests.on_request(&id);
        drop(id);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn stdio_bom_and_custom_notification_behavior_is_preserved() {
        let pool = BytePool::new(4096);
        assert!(
            buffered(b"\xef\xbb\xbf  \n", &pool)
                .parse(true)
                .unwrap()
                .is_none()
        );
        assert_eq!(pool.used(), 0);
        let raw = br#"{"jsonrpc":"2.0","method":"notifications/custom"}"#;
        let expected: ClientJsonRpcMessage = serde_json::from_slice(raw).unwrap();
        let message = buffered(raw, &pool)
            .parse(true)
            .unwrap()
            .unwrap()
            .into_message();
        assert_eq!(
            serde_json::to_value(&message).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
        drop(message);
        assert_eq!(pool.used(), 0);
        assert!(matches!(
            buffered(b"[]", &pool).parse(false),
            Err(Failure::Data)
        ));
        assert_eq!(pool.used(), 0);
    }
}
