//! Owned MCP request buffers and non-blocking admission before parsing.
use crate::lease::{BytePool, Lease, Owner};
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
pub(crate) const SNIFF_HEAD_LIMIT: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Failure {
    Limit,
    Budget { id: Option<rmcp::model::RequestId> },
    Syntax,
    InvalidRequest { id: Option<rmcp::model::RequestId> },
    InvalidParams { id: rmcp::model::RequestId },
}
pub(crate) struct Parsed {
    pub message: ClientJsonRpcMessage,
    pub owner: Owner,
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
    pub(crate) fn clear(&mut self) {
        self.bytes = Vec::new();
        self.in_control = false;
        self.lease = Lease::empty(&self.pool);
    }
    pub(crate) fn take(&mut self) -> Self {
        let replacement = Self::new(Arc::clone(&self.pool), Arc::clone(&self.control));
        std::mem::replace(self, replacement)
    }
    pub(crate) fn pool(&self) -> &Arc<BytePool> {
        &self.pool
    }
    pub(crate) fn control(&self) -> &Arc<BytePool> {
        &self.control
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
            let capacity = admitted.ok_or(Failure::Budget { id: None })?;
            let mut replacement = Vec::with_capacity(capacity);
            replacement.extend_from_slice(&self.bytes);
            self.bytes = replacement;
            self.lease.shrink_to(self.bytes.capacity());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
    pub fn parse(mut self, stdio: bool) -> Result<Option<Parsed>, Failure> {
        if stdio
            && self
                .bytes
                .strip_prefix(UTF8_BOM)
                .unwrap_or(&self.bytes)
                .iter()
                .all(u8::is_ascii_whitespace)
        {
            return Ok(None);
        }
        let len = self.bytes.len();
        if !self.resize(self.bytes.capacity().saturating_add(3 * len), len) {
            if stdio && is_complete_json_notification(&self.bytes, &[]) {
                return Ok(None);
            }
            let id = stdio
                .then(|| sniff_request_id(&self.bytes, len <= SNIFF_HEAD_LIMIT))
                .flatten();
            return Err(Failure::Budget { id });
        }
        let raw = if stdio {
            self.bytes.strip_prefix(UTF8_BOM).unwrap_or(&self.bytes)
        } else {
            &self.bytes
        };
        // Slice decoding can retain twice the longest escaped token as scratch,
        // alongside the decoded Value and original bytes.
        let value: serde_json::Value = serde_json::from_slice(raw).map_err(|_| Failure::Syntax)?;
        // Borrow the Value to avoid owned serde Content string copies. Retained
        // space covers SDK params, metadata, tool conversion and the request ID.
        let retained = 4 * len;
        let peak = self
            .bytes
            .capacity()
            .saturating_add(value_capacity(&value))
            .saturating_add(retained);
        if !self.resize(peak, len) {
            if stdio && value.as_object().is_some_and(|obj| !obj.contains_key("id")) {
                return Ok(None);
            }
            let id = stdio.then(|| extract_value_request_id(&value)).flatten();
            return Err(Failure::Budget { id });
        }
        let message = match ClientJsonRpcMessage::deserialize(&value) {
            Ok(rmcp::model::JsonRpcMessage::Request(req))
                if matches!(
                    &req.request,
                    rmcp::model::ClientRequest::CustomRequest(custom)
                        if is_typed_client_request(&custom.method)
                ) =>
            {
                return Err(Failure::InvalidParams { id: req.id });
            }
            Ok(message) => message,
            Err(_) => return classify_deserialize_failure(&value, stdio),
        };
        drop(value);
        drop(std::mem::take(&mut self.bytes));
        self.lease.shrink_to(retained);
        // A short input may borrow control space for classification. Ordinary
        // work and long-lived peer metadata must return to the main pool before
        // dispatch, so media pressure cannot consume the control reserve.
        if self.in_control && !is_control(&message) {
            let Some(lease) = self.pool.try_acquire(retained) else {
                if stdio && matches!(message, rmcp::model::JsonRpcMessage::Notification(_)) {
                    drop(message);
                    return Ok(None);
                }
                let id = match &message {
                    rmcp::model::JsonRpcMessage::Request(req) if stdio => Some(req.id.clone()),
                    _ => None,
                };
                drop(message);
                return Err(Failure::Budget { id });
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

fn is_typed_client_request(method: &str) -> bool {
    matches!(
        method,
        "ping"
            | "initialize"
            | "server/discover"
            | "completion/complete"
            | "logging/setLevel"
            | "prompts/get"
            | "prompts/list"
            | "resources/list"
            | "resources/templates/list"
            | "resources/read"
            | "subscriptions/listen"
            | "resources/subscribe"
            | "resources/unsubscribe"
            | "tools/call"
            | "tools/list"
            | "tasks/get"
            | "tasks/update"
            | "tasks/cancel"
            | "skills/list"
            | "skills/get"
            | "resources/directory/read"
    )
}

fn extract_value_request_id(value: &serde_json::Value) -> Option<rmcp::model::RequestId> {
    match value.as_object()?.get("id")? {
        serde_json::Value::String(s) => Some(rmcp::model::RequestId::String(Arc::from(s.as_str()))),
        serde_json::Value::Number(n) => n.as_i64().map(rmcp::model::RequestId::Number),
        _ => None,
    }
}

fn classify_deserialize_failure(
    value: &serde_json::Value,
    stdio: bool,
) -> Result<Option<Parsed>, Failure> {
    let Some(obj) = value.as_object() else {
        return Err(Failure::InvalidRequest { id: None });
    };
    if !obj.contains_key("id") {
        if stdio {
            return Ok(None);
        }
        return Err(Failure::InvalidRequest { id: None });
    }
    let id = extract_value_request_id(value);
    if obj.get("jsonrpc").and_then(serde_json::Value::as_str) == Some("2.0")
        && obj
            .get("method")
            .and_then(serde_json::Value::as_str)
            .is_some()
        && !obj.contains_key("result")
        && !obj.contains_key("error")
        && let Some(id) = id
    {
        return Err(Failure::InvalidParams { id });
    }
    Err(Failure::InvalidRequest { id })
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

/// Scans at most [`SNIFF_HEAD_LIMIT`] bytes for a top-level JSON-RPC `"id"` field.
pub(crate) fn sniff_request_id(raw: &[u8], complete: bool) -> Option<rmcp::model::RequestId> {
    let truncated = !complete || raw.len() > SNIFF_HEAD_LIMIT;
    let head = &raw[..raw.len().min(SNIFF_HEAD_LIMIT)];
    let bytes = head.strip_prefix(UTF8_BOM).unwrap_or(head);
    let mut i = skip_ws(bytes, 0);
    if bytes.get(i) != Some(&b'{') {
        return None;
    }
    i += 1;
    loop {
        i = skip_ws(bytes, i);
        match bytes.get(i)? {
            b'}' => return None,
            b'"' => {}
            _ => return None,
        }
        let key_end = scan_string_end(bytes, i)?;
        let is_id = bytes[i..key_end] == *b"\"id\""
            || serde_json::from_slice::<String>(&bytes[i..key_end])
                .ok()
                .as_deref()
                == Some("id");
        i = skip_ws(bytes, key_end);
        if bytes.get(i) != Some(&b':') {
            return None;
        }
        i = skip_ws(bytes, i + 1);
        if is_id {
            return match bytes.get(i)? {
                b'"' => {
                    let val_end = scan_string_end(bytes, i)?;
                    let s: String = serde_json::from_slice(&bytes[i..val_end]).ok()?;
                    Some(rmcp::model::RequestId::String(Arc::from(s.as_str())))
                }
                b'-' | b'0'..=b'9' => {
                    let start = i;
                    while let Some(&b) = bytes.get(i) {
                        if matches!(b, b'-' | b'+' | b'0'..=b'9' | b'.' | b'e' | b'E') {
                            i += 1;
                        } else {
                            break;
                        }
                    }
                    if i == bytes.len() && truncated {
                        return None;
                    }
                    let num: serde_json::Number = serde_json::from_slice(&bytes[start..i]).ok()?;
                    num.as_i64().map(rmcp::model::RequestId::Number)
                }
                _ => None,
            };
        }
        i = skip_value(bytes, i)?;
        i = skip_ws(bytes, i);
        if bytes.get(i) != Some(&b',') {
            return None;
        }
        i += 1;
    }
}

fn skip_ws(bytes: &[u8], mut i: usize) -> usize {
    while bytes.get(i).is_some_and(u8::is_ascii_whitespace) {
        i += 1;
    }
    i
}

fn scan_string_end(bytes: &[u8], start_quote: usize) -> Option<usize> {
    let mut i = start_quote + 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b'"' => return Some(i + 1),
            0x00..=0x1f => return None,
            _ => i += 1,
        }
    }
    None
}

fn skip_value(bytes: &[u8], mut i: usize) -> Option<usize> {
    match bytes.get(i)? {
        b'"' => scan_string_end(bytes, i),
        b'{' | b'[' => {
            let mut depth = 1usize;
            i += 1;
            while i < bytes.len() && depth > 0 {
                match bytes[i] {
                    b'"' => i = scan_string_end(bytes, i)?,
                    b'{' | b'[' => {
                        depth += 1;
                        i += 1;
                    }
                    b'}' | b']' => {
                        depth -= 1;
                        i += 1;
                    }
                    _ => i += 1,
                }
            }
            (depth == 0).then_some(i)
        }
        _ => {
            let start = i;
            while let Some(&b) = bytes.get(i) {
                if b.is_ascii_whitespace() || matches!(b, b',' | b'}' | b']' | b'"' | b'{' | b'[') {
                    break;
                }
                i += 1;
            }
            (i > start).then_some(i)
        }
    }
}

/// Checks without heap allocation whether `prefix` + `suffix` is a complete, syntactically valid
/// top-level JSON object `{ ... }` (allowing a leading UTF-8 BOM and RFC 8259 whitespace) with no
/// top-level `"id"` key.
pub(crate) fn is_complete_json_notification(prefix: &[u8], suffix: &[u8]) -> bool {
    const MAX_DEPTH: usize = 127;
    enum State {
        ObjectFirstOrClose,
        ObjectKey,
        ArrayFirstOrClose,
        Value,
        AfterValue,
    }

    let chain = ByteChain {
        first: prefix,
        second: suffix,
    };
    let mut i = if chain.starts_with_at(0, UTF8_BOM) {
        3
    } else {
        0
    };
    i = chain.skip_rfc_ws(i);
    if chain.get(i) != Some(b'{') {
        return false;
    }
    i += 1;
    let mut stack: u128 = 1;
    let mut depth: usize = 1;
    let mut state = State::ObjectFirstOrClose;

    while depth > 0 {
        match state {
            State::ObjectFirstOrClose => {
                i = chain.skip_rfc_ws(i);
                if chain.get(i) == Some(b'}') {
                    i += 1;
                    depth -= 1;
                    state = State::AfterValue;
                } else {
                    state = State::ObjectKey;
                }
            }
            State::ObjectKey => {
                i = chain.skip_rfc_ws(i);
                let Some((after_key, is_id)) = chain.scan_json_string(i) else {
                    return false;
                };
                if depth == 1 && is_id {
                    return false;
                }
                i = chain.skip_rfc_ws(after_key);
                if chain.get(i) != Some(b':') {
                    return false;
                }
                i += 1;
                state = State::Value;
            }
            State::ArrayFirstOrClose => {
                i = chain.skip_rfc_ws(i);
                if chain.get(i) == Some(b']') {
                    i += 1;
                    depth -= 1;
                    state = State::AfterValue;
                } else {
                    state = State::Value;
                }
            }
            State::Value => {
                i = chain.skip_rfc_ws(i);
                match chain.get(i) {
                    Some(b'{') => {
                        if depth >= MAX_DEPTH {
                            return false;
                        }
                        stack |= 1u128 << depth;
                        depth += 1;
                        i += 1;
                        state = State::ObjectFirstOrClose;
                    }
                    Some(b'[') => {
                        if depth >= MAX_DEPTH {
                            return false;
                        }
                        stack &= !(1u128 << depth);
                        depth += 1;
                        i += 1;
                        state = State::ArrayFirstOrClose;
                    }
                    Some(b'"') => {
                        let Some((next_i, _)) = chain.scan_json_string(i) else {
                            return false;
                        };
                        i = next_i;
                        state = State::AfterValue;
                    }
                    Some(b'-' | b'0'..=b'9') => {
                        let Some(next_i) = chain.scan_json_number(i) else {
                            return false;
                        };
                        i = next_i;
                        state = State::AfterValue;
                    }
                    Some(b't') if chain.starts_with_at(i, b"true") => {
                        i += 4;
                        state = State::AfterValue;
                    }
                    Some(b'f') if chain.starts_with_at(i, b"false") => {
                        i += 5;
                        state = State::AfterValue;
                    }
                    Some(b'n') if chain.starts_with_at(i, b"null") => {
                        i += 4;
                        state = State::AfterValue;
                    }
                    _ => return false,
                }
            }
            State::AfterValue => {
                i = chain.skip_rfc_ws(i);
                let in_object = ((stack >> (depth - 1)) & 1) == 1;
                match (in_object, chain.get(i)) {
                    (true, Some(b',')) => {
                        i += 1;
                        state = State::ObjectKey;
                    }
                    (true, Some(b'}')) => {
                        i += 1;
                        depth -= 1;
                    }
                    (false, Some(b',')) => {
                        i += 1;
                        state = State::Value;
                    }
                    (false, Some(b']')) => {
                        i += 1;
                        depth -= 1;
                    }
                    _ => return false,
                }
            }
        }
    }
    chain.skip_rfc_ws(i) == chain.len()
}

struct ByteChain<'a> {
    first: &'a [u8],
    second: &'a [u8],
}

impl<'a> ByteChain<'a> {
    fn len(&self) -> usize {
        self.first.len() + self.second.len()
    }

    fn get(&self, idx: usize) -> Option<u8> {
        if idx < self.first.len() {
            Some(self.first[idx])
        } else {
            self.second.get(idx - self.first.len()).copied()
        }
    }

    fn skip_rfc_ws(&self, mut i: usize) -> usize {
        while matches!(self.get(i), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            i += 1;
        }
        i
    }

    fn starts_with_at(&self, idx: usize, pat: &[u8]) -> bool {
        pat.iter()
            .enumerate()
            .all(|(off, &expected)| self.get(idx + off) == Some(expected))
    }

    fn scan_hex4(&self, idx: usize) -> Option<u16> {
        let mut val = 0u16;
        for off in 0..4 {
            let digit = match self.get(idx + off)? {
                b @ b'0'..=b'9' => b - b'0',
                b @ b'a'..=b'f' => b - b'a' + 10,
                b @ b'A'..=b'F' => b - b'A' + 10,
                _ => return None,
            };
            val = (val << 4) | u16::from(digit);
        }
        Some(val)
    }

    fn scan_utf8_non_ascii(&self, idx: usize) -> Option<usize> {
        let is_cont = |b: u8| (0x80..=0xBF).contains(&b);
        match self.get(idx)? {
            0xC2..=0xDF => is_cont(self.get(idx + 1)?).then_some(idx + 2),
            0xE0 => {
                let b1 = self.get(idx + 1)?;
                ((0xA0..=0xBF).contains(&b1) && is_cont(self.get(idx + 2)?)).then_some(idx + 3)
            }
            0xE1..=0xEC | 0xEE..=0xEF => {
                (is_cont(self.get(idx + 1)?) && is_cont(self.get(idx + 2)?)).then_some(idx + 3)
            }
            0xED => {
                let b1 = self.get(idx + 1)?;
                ((0x80..=0x9F).contains(&b1) && is_cont(self.get(idx + 2)?)).then_some(idx + 3)
            }
            0xF0 => {
                let b1 = self.get(idx + 1)?;
                ((0x90..=0xBF).contains(&b1)
                    && is_cont(self.get(idx + 2)?)
                    && is_cont(self.get(idx + 3)?))
                .then_some(idx + 4)
            }
            0xF1..=0xF3 => (is_cont(self.get(idx + 1)?)
                && is_cont(self.get(idx + 2)?)
                && is_cont(self.get(idx + 3)?))
            .then_some(idx + 4),
            0xF4 => {
                let b1 = self.get(idx + 1)?;
                ((0x80..=0x8F).contains(&b1)
                    && is_cont(self.get(idx + 2)?)
                    && is_cont(self.get(idx + 3)?))
                .then_some(idx + 4)
            }
            _ => None,
        }
    }

    fn scan_json_string(&self, quote_idx: usize) -> Option<(usize, bool)> {
        if self.get(quote_idx) != Some(b'"') {
            return None;
        }
        let mut i = quote_idx + 1;
        let mut head = [0u8; 2];
        let mut decoded_len = 0usize;
        let mut push_byte = |b: u8| {
            if decoded_len < 2 {
                head[decoded_len] = b;
            }
            decoded_len = decoded_len.saturating_add(1);
        };
        while let Some(b) = self.get(i) {
            match b {
                b'"' => return Some((i + 1, decoded_len == 2 && head == *b"id")),
                0x00..=0x1f => return None,
                b'\\' => {
                    let esc = self.get(i + 1)?;
                    match esc {
                        b'"' | b'\\' | b'/' => {
                            push_byte(esc);
                            i += 2;
                        }
                        b'b' => {
                            push_byte(0x08);
                            i += 2;
                        }
                        b'f' => {
                            push_byte(0x0c);
                            i += 2;
                        }
                        b'n' => {
                            push_byte(b'\n');
                            i += 2;
                        }
                        b'r' => {
                            push_byte(b'\r');
                            i += 2;
                        }
                        b't' => {
                            push_byte(b'\t');
                            i += 2;
                        }
                        b'u' => {
                            let u1 = self.scan_hex4(i + 2)?;
                            let (ch, next_i) = match u1 {
                                0xD800..=0xDBFF => {
                                    if !self.starts_with_at(i + 6, b"\\u") {
                                        return None;
                                    }
                                    let u2 = self.scan_hex4(i + 8)?;
                                    if !(0xDC00..=0xDFFF).contains(&u2) {
                                        return None;
                                    }
                                    let code = 0x10000
                                        + ((u32::from(u1 - 0xD800) << 10) | u32::from(u2 - 0xDC00));
                                    (char::from_u32(code)?, i + 12)
                                }
                                0xDC00..=0xDFFF => return None,
                                _ => (char::from_u32(u32::from(u1))?, i + 6),
                            };
                            let mut utf8 = [0u8; 4];
                            for &ub in ch.encode_utf8(&mut utf8).as_bytes() {
                                push_byte(ub);
                            }
                            i = next_i;
                        }
                        _ => return None,
                    }
                }
                0x20..=0x7f => {
                    push_byte(b);
                    i += 1;
                }
                _ => {
                    let next_i = self.scan_utf8_non_ascii(i)?;
                    for idx in i..next_i {
                        push_byte(self.get(idx)?);
                    }
                    i = next_i;
                }
            }
        }
        None
    }

    fn scan_json_number(&self, start: usize) -> Option<usize> {
        let mut i = start;
        if self.get(i) == Some(b'-') {
            i += 1;
        }
        match self.get(i)? {
            b'0' => i += 1,
            b'1'..=b'9' => {
                i += 1;
                while matches!(self.get(i), Some(b'0'..=b'9')) {
                    i += 1;
                }
            }
            _ => return None,
        }
        if self.get(i) == Some(b'.') {
            i += 1;
            if !matches!(self.get(i), Some(b'0'..=b'9')) {
                return None;
            }
            while matches!(self.get(i), Some(b'0'..=b'9')) {
                i += 1;
            }
        }
        if matches!(self.get(i), Some(b'e' | b'E')) {
            i += 1;
            if matches!(self.get(i), Some(b'+' | b'-')) {
                i += 1;
            }
            if !matches!(self.get(i), Some(b'0'..=b'9')) {
                return None;
            }
            while matches!(self.get(i), Some(b'0'..=b'9')) {
                i += 1;
            }
        }
        let in_range = if i <= self.first.len() {
            serde_json::from_slice::<serde_json::Number>(&self.first[start..i]).is_ok()
        } else if start >= self.first.len() {
            let s = start - self.first.len();
            let e = i - self.first.len();
            serde_json::from_slice::<serde_json::Number>(&self.second[s..e]).is_ok()
        } else {
            use std::io::Read as _;
            let left = &self.first[start..];
            let right = &self.second[..i - self.first.len()];
            serde_json::from_reader::<_, serde_json::Number>(left.chain(right)).is_ok()
        };
        in_range.then_some(i)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rmcp::model::{GetExtensions, JsonRpcMessage};

    const LIMIT: usize = 4 << 20;

    fn buffered(raw: &[u8], pool: &Arc<BytePool>) -> Buffer {
        let mut buffer = Buffer::new(Arc::clone(pool), BytePool::new(0));
        buffer.append(raw, LIMIT).unwrap();
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
            Err(Failure::Budget { .. })
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
            Err(Failure::Budget { id: None })
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
            assert!(matches!(buffer.parse(false), Err(Failure::Budget { .. })));
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
            Err(Failure::InvalidRequest { id: None })
        ));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn clear_and_take_preserve_isolated_pools_and_release_charges() {
        let pool = BytePool::new(128);
        let control = BytePool::new(64);
        let mut buf = Buffer::new(Arc::clone(&pool), Arc::clone(&control));
        assert!(Arc::ptr_eq(buf.pool(), &pool));
        assert!(Arc::ptr_eq(buf.control(), &control));

        buf.append(&[b'x'; 80], LIMIT).unwrap();
        assert_eq!(pool.used(), buf.capacity());
        buf.clear();
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.capacity(), 0);
        assert_eq!(pool.used(), 0);
        assert!(Arc::ptr_eq(buf.pool(), &pool));
        assert!(Arc::ptr_eq(buf.control(), &control));
        assert_eq!(
            buf.append(&[b'x'; 200], LIMIT),
            Err(Failure::Budget { id: None })
        );

        buf.append(&[b'y'; 80], LIMIT).unwrap();
        let taken = buf.take();
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.capacity(), 0);
        assert!(Arc::ptr_eq(buf.pool(), &pool));
        assert!(Arc::ptr_eq(buf.control(), &control));
        assert_eq!(taken.len(), 80);
        drop(taken);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn stdio_early_parse_budget_drops_only_syntactically_valid_notifications() {
        let noti = br#"{"jsonrpc":"2.0","method":"notifications/initialized","params":{"nested":{"id":9},"s":"a\nb\uD83D\uDE00"}}"#;
        let tight = BytePool::new(noti.len());
        assert!(buffered(noti, &tight).parse(true).unwrap().is_none());
        assert_eq!(tight.used(), 0);
        assert_eq!(
            buffered(noti, &tight).parse(false).err(),
            Some(Failure::Budget { id: None })
        );

        for (split_noti, expected) in [
            (
                &b"\xef\xbb\xbf { \"method\" : \"x\", \"arr\" : [true, false, null, -0.5e+2] }\r\n"
                    [..],
                true,
            ),
            (&br#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#[..], false),
            (
                &br#"{"jsonrpc":"2.0","id":null,"method":"ping"}"#[..],
                false,
            ),
            (
                &br#"{"jsonrpc":"2.0","i\u0064":"esc","method":"ping"}"#[..],
                false,
            ),
            (&br#"{"jsonrpc":"2.0","method":"x",}"#[..], false),
            (&br#"{"jsonrpc":"2.0","method":"x"} trailing"#[..], false),
            (&br#"{"jsonrpc":"2.0","params":[1,2}}"#[..], false),
            (&br#"{"jsonrpc":"2.0","n":01}"#[..], false),
            (&br#"{"jsonrpc":"2.0","n":1e999}"#[..], false),
            (&br#"{"jsonrpc":"2.0","s":"\uD800"}"#[..], false),
            (&b"{\"jsonrpc\":\"2.0\",\"s\":\"\xff\"}"[..], false),
            (&b"[{\"jsonrpc\":\"2.0\"}]"[..], false),
        ] {
            for split in 0..=split_noti.len() {
                let (a, b) = split_noti.split_at(split);
                assert_eq!(
                    is_complete_json_notification(a, b),
                    expected,
                    "split={split} input={split_noti:?}"
                );
            }
        }

        // Exact depth boundary matches serde_json::Value recursion limit (127 containers ok, 128 rejected).
        for depth in [127usize, 128] {
            let mut nested = String::from(r#"{"v":"#);
            for _ in 1..depth {
                nested.push('[');
            }
            nested.push('0');
            for _ in 1..depth {
                nested.push(']');
            }
            nested.push('}');
            let serde_ok = serde_json::from_slice::<serde_json::Value>(nested.as_bytes()).is_ok();
            assert_eq!(serde_ok, depth == 127);
            assert_eq!(
                is_complete_json_notification(nested.as_bytes(), &[]),
                serde_ok
            );
        }
    }
}
