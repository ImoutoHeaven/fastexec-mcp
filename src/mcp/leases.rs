use super::ingress;
use crate::events;
use crate::lease;
use crate::render;
use crate::render::types::TrackedResult;
use crate::tasks::session::Session;
use rmcp::RoleServer;
use rmcp::model::{CallToolResult, ContentBlock};
use rmcp::service::RequestContext;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// The token position authenticated before rmcp receives a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Principal(pub usize);

#[derive(Default)]
struct HttpResponseLeaseState {
    closed: bool,
    lease: Option<lease::Lease>,
}

/// Shared outbound response lease slot bridged from `Server` to HTTP `LeaseBody` across
/// `tokio::spawn` boundaries via `http::request::Parts::extensions`.
#[derive(Clone, Default)]
pub(crate) struct HttpResponseLease(Arc<std::sync::Mutex<HttpResponseLeaseState>>);

impl HttpResponseLease {
    pub(crate) fn attach(&self, outbound: lease::Lease) -> bool {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.closed {
            drop(state);
            drop(outbound);
            return false;
        }
        let replaced = state.lease.replace(outbound);
        drop(state);
        drop(replaced);
        true
    }

    pub(crate) fn close(&self) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.closed = true;
        let dropped = state.lease.take();
        drop(state);
        drop(dropped);
    }
}

pub(crate) struct RegisteredSession {
    pub(super) session: Arc<Session>,
    pub(super) event_bus: Arc<events::EventBus>,
}

impl Drop for RegisteredSession {
    fn drop(&mut self) {
        self.event_bus.remove_session(&self.session.id);
    }
}

/// The base inferred from a request: its Host or HTTP/2 `:authority`, over the scheme the client saw.
pub(super) fn inferred_base(parts: &::http::request::Parts) -> Result<String, String> {
    let mut hosts = parts.headers.get_all(::http::header::HOST).iter();
    let authority =
        match hosts.next() {
            Some(raw) => {
                if hosts.next().is_some() {
                    return Err(
                        "Cannot make a transfer URL: the request carries multiple Host headers."
                            .to_owned(),
                    );
                }
                let host = raw.to_str().map_err(|_| {
                    "Cannot make a transfer URL: Host header is not valid ASCII.".to_owned()
                })?;
                if host.contains('@') {
                    return Err(
                        "Cannot make a transfer URL: the request host carries userinfo.".to_owned(),
                    );
                }
                host.parse::<::http::uri::Authority>()
                    .map_err(|_| format!("Cannot make a transfer URL: {host} is not a host."))?
            }
            None => parts.uri.authority().cloned().ok_or_else(|| {
                "Cannot make a transfer URL: the request names no Host.".to_owned()
            })?,
        };
    if authority.as_str().contains('@') {
        return Err("Cannot make a transfer URL: the request host carries userinfo.".to_owned());
    }
    if authority.host().is_empty() {
        return Err("Cannot make a transfer URL: the request names no Host.".to_owned());
    }
    let scheme = forwarded_scheme(&parts.headers)
        .or_else(|| parts.uri.scheme_str().and_then(known_scheme))
        .unwrap_or("http");
    Ok(format!("{scheme}://{authority}/"))
}

/// The scheme the client saw: the first `X-Forwarded-Proto` token when it names `http` or `https`.
fn forwarded_scheme(headers: &::http::HeaderMap) -> Option<&'static str> {
    headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .and_then(known_scheme)
}

fn known_scheme(token: &str) -> Option<&'static str> {
    if token.eq_ignore_ascii_case("https") {
        Some("https")
    } else if token.eq_ignore_ascii_case("http") {
        Some("http")
    } else {
        None
    }
}

pub(super) fn deliver_tracked(
    pool: &Arc<lease::BytePool>,
    control_pool: &Arc<lease::BytePool>,
    stdio: &RequestLeases,
    context: &RequestContext<RoleServer>,
    mut tracked: TrackedResult,
    is_control: bool,
    overload_message: impl FnOnce(Option<&crate::service::core::TaskMeta>) -> String,
) -> CallToolResult {
    let use_control = is_control || tracked.result.is_error == Some(true);
    let b64_bytes =
        render::unleased_result_payload_capacity(&tracked.result, &tracked.payload_owner);
    let exact_json = lease::json_len(&tracked.result).unwrap_or(usize::MAX);
    let id_bytes = lease::json_len(&context.id).unwrap_or(usize::MAX);
    let outbound_bytes = exact_json.saturating_add(id_bytes).saturating_add(256);
    let admitted = b64_bytes
        .checked_add(outbound_bytes)
        .and_then(|peak| admit_delivery(&mut tracked.lease, pool, control_pool, peak, use_control));
    let Some(mut held) = admitted else {
        let meta = tracked.task_meta.take();
        drop(tracked);
        let err = CallToolResult::error(vec![ContentBlock::text(overload_message(meta.as_ref()))]);
        let err_outbound = lease::json_len(&err)
            .unwrap_or(usize::MAX)
            .saturating_add(id_bytes)
            .saturating_add(256);
        let err_peak = render::result_payload_capacity(&err).saturating_add(err_outbound);
        if let Some(mut err_lease) = (err_peak <= lease::CONTROL_LIMIT)
            .then(|| control_pool.try_acquire(err_peak))
            .flatten()
        {
            let outbound = err_lease.split_off(err_outbound);
            return stdio.deliver(
                context.id.clone(),
                &context.ct,
                err,
                err_lease,
                outbound,
                Default::default(),
            );
        }
        return err;
    };
    let outbound = held.split_off(outbound_bytes.min(held.bytes()));
    stdio.deliver(
        context.id.clone(),
        &context.ct,
        tracked.result,
        held,
        outbound,
        tracked.payload_owner,
    )
}

// A failed growth keeps the old lease until the caller drops the owned result.
fn admit_delivery(
    held: &mut Option<lease::Lease>,
    pool: &Arc<lease::BytePool>,
    control: &Arc<lease::BytePool>,
    peak: usize,
    use_control: bool,
) -> Option<lease::Lease> {
    if let Some(previous) = held.as_mut() {
        if previous.try_resize(peak) {
            return held.take();
        }
    } else if let Some(acquired) = pool.try_acquire(peak) {
        return Some(acquired);
    }
    if use_control && peak <= lease::CONTROL_LIMIT {
        let acquired = control.try_acquire(peak)?;
        drop(held.take());
        return Some(acquired);
    }
    None
}

pub(super) fn reserve_response_content(
    pool: &Arc<lease::BytePool>,
    bytes: usize,
) -> Result<lease::Lease, rmcp::ErrorData> {
    pool.try_acquire(bytes).ok_or_else(|| {
        rmcp::ErrorData::internal_error(
            "Server active memory budget is full while preparing the response",
            None,
        )
    })
}

pub(super) fn reserve_control_response_content(
    pool: &Arc<lease::BytePool>,
    control: &Arc<lease::BytePool>,
    bytes: usize,
) -> Result<lease::Lease, rmcp::ErrorData> {
    pool.try_acquire(bytes)
        .or_else(|| {
            (bytes <= lease::CONTROL_LIMIT)
                .then(|| control.try_acquire(bytes))
                .flatten()
        })
        .ok_or_else(|| {
            rmcp::ErrorData::internal_error(
                "Server active memory budget is full while preparing the response",
                None,
            )
        })
}

// SDK parsing happened before this boundary. Reserve only the subsequent
// application copies of parameters/IDs and pass this owner into actual workers.
pub(super) fn application_input_owner<T: serde::Serialize>(
    parameters_and_id: &T,
    existing: Option<&lease::Owner>,
    pool: &Arc<lease::BytePool>,
    control: &Arc<lease::BytePool>,
    is_control: bool,
) -> Result<lease::Owner, &'static str> {
    if let Some(owner) = existing {
        return Ok(owner.clone());
    }
    const REFUSED: &str = "Server active memory budget is full while preparing tool arguments; the operation was not dispatched.";
    let length = lease::json_len(parameters_and_id).map_err(|_| REFUSED)?;
    let bytes = length.checked_mul(4).ok_or(REFUSED)?;
    let held = pool
        .try_acquire(bytes)
        .or_else(|| {
            (is_control && length <= lease::SHORT_INPUT)
                .then(|| control.try_acquire(bytes))
                .flatten()
        })
        .ok_or(REFUSED)?;
    Ok(lease::Owner::new(held))
}

pub(super) fn request_input_owner(context: &RequestContext<RoleServer>) -> crate::lease::Owner {
    context
        .extensions
        .get::<crate::lease::Owner>()
        .cloned()
        .unwrap_or_default()
}

pub(super) fn http_context(context: &RequestContext<RoleServer>) -> bool {
    context
        .extensions
        .get::<::http::request::Parts>()
        .is_some_and(|parts| parts.extensions.get::<Principal>().is_some())
}

/// Prepaid application payload, staging and wire bytes for each stdio response ID.
#[derive(Default)]
pub(crate) struct RequestLeases(std::sync::Mutex<RequestLeaseState>);

#[derive(Default)]
struct RequestLeaseState {
    closed: bool,
    entries: std::collections::HashMap<
        rmcp::model::RequestId,
        (lease::Lease, lease::Lease, lease::Owner),
    >,
    cancelled: std::collections::VecDeque<(rmcp::model::RequestId, crate::lease::Owner)>,
}

const MAX_RECENT_CANCELLED: usize = 1024;

impl RequestLeases {
    pub(crate) fn on_request(&self, id: &rmcp::model::RequestId) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.cancelled.retain(|(entry, _)| entry != id);
    }

    // A complete queued response remains deliverable after a late cancellation.
    #[cfg(test)]
    pub(crate) fn on_cancel(&self, id: &rmcp::model::RequestId) -> bool {
        self.on_cancel_owned(id, crate::lease::Owner::default())
    }

    pub(crate) fn on_cancel_owned(
        &self,
        id: &rmcp::model::RequestId,
        owner: crate::lease::Owner,
    ) -> bool {
        let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if state.closed || state.entries.contains_key(id) {
            return false;
        }
        if !state.cancelled.iter().any(|(entry, _)| entry == id) {
            if state.cancelled.len() >= MAX_RECENT_CANCELLED {
                state.cancelled.pop_front();
            }
            state
                .cancelled
                .push_back((id.clone(), ingress::retain_id(&owner, id)));
        }
        true
    }

    pub(crate) fn register_response(
        &self,
        id: rmcp::model::RequestId,
        cancel: &CancellationToken,
        staging: lease::Lease,
        outbound: lease::Lease,
        payload_owner: lease::Owner,
    ) -> bool {
        let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
        if state.closed
            || cancel.is_cancelled()
            || state.cancelled.iter().any(|(entry, _)| entry == &id)
        {
            drop(state);
            drop((staging, outbound, payload_owner));
            return false;
        }
        let replaced = state.entries.insert(id, (staging, outbound, payload_owner));
        drop(state);
        drop(replaced);
        true
    }

    pub(crate) fn deliver(
        &self,
        id: rmcp::model::RequestId,
        cancel: &CancellationToken,
        result: CallToolResult,
        staging: lease::Lease,
        outbound: lease::Lease,
        payload_owner: lease::Owner,
    ) -> CallToolResult {
        if !self.register_response(id, cancel, staging, outbound, payload_owner) {
            drop(result);
            return CallToolResult::error(vec![ContentBlock::text(
                "Cancelled before the response was queued.",
            )]);
        }
        result
    }

    #[cfg(test)]
    pub(crate) fn insert(
        &self,
        id: rmcp::model::RequestId,
        cancel: &CancellationToken,
        staging: lease::Lease,
        outbound: lease::Lease,
    ) {
        self.deliver(
            id,
            cancel,
            CallToolResult::success(Vec::new()),
            staging,
            outbound,
            Default::default(),
        );
    }

    pub(crate) fn take(
        &self,
        id: &rmcp::model::RequestId,
    ) -> Option<(lease::Lease, lease::Lease, lease::Owner)> {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.cancelled.retain(|(entry, _)| entry != id);
        state.entries.remove(id)
    }

    pub(crate) fn close(&self) {
        let mut state = self.0.lock().unwrap_or_else(|p| p.into_inner());
        state.closed = true;
        state.cancelled.clear();
        // The SDK may still own queued responses during shutdown. Their leases
        // remain with the session until every producer and transport owner ends.
    }
}
