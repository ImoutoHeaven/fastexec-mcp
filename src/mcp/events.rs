use super::server::{Backend, Server, client_name};
use crate::events;
use crate::ipc;
use crate::lease;
use rmcp::RoleServer;
use rmcp::service::RequestContext;
use std::sync::Arc;

fn event_error_to_rmcp(err: events::EventError, control: &Arc<lease::BytePool>) -> rmcp::ErrorData {
    let message_bytes = match &err {
        events::EventError::InvalidParams(message)
        | events::EventError::NotFound { message, .. }
        | events::EventError::Forbidden(message)
        | events::EventError::ResourceExhausted { message, .. } => message.len(),
    };
    let Some(held) = control.try_acquire(message_bytes.saturating_add(256)) else {
        return rmcp::ErrorData::new(
            rmcp::model::ErrorCode(-32025),
            "Server active memory budget is full while preparing the event error",
            None,
        );
    };
    lease::Owned::new(err, lease::Owner::new(held))
        .map(ipc::IpcEventError::from)
        .run(ipc_event_error_to_rmcp)
}

fn ipc_event_error_to_rmcp(err: ipc::IpcEventError) -> rmcp::ErrorData {
    rmcp::ErrorData::new(rmcp::model::ErrorCode(err.code), err.message, err.data)
}

pub(crate) fn decode_mcp_header_value(raw: &str) -> Option<String> {
    use base64::Engine as _;
    if let Some(rest) = raw.strip_prefix("=?base64?") {
        let encoded = rest.strip_suffix("?=")?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .ok()?;
        return String::from_utf8(bytes).ok();
    }
    if raw.is_empty()
        || raw.starts_with(' ')
        || raw.ends_with(' ')
        || !raw.bytes().all(|b| (0x20..=0x7E).contains(&b))
    {
        return None;
    }
    Some(raw.to_owned())
}

fn reject_unexpected_mcp_name_header(
    context: &RequestContext<RoleServer>,
) -> Result<(), rmcp::ErrorData> {
    let Some(parts) = context.extensions.get::<::http::request::Parts>() else {
        return Ok(());
    };
    if parts.headers.get_all("mcp-name").iter().next().is_some() {
        return Err(rmcp::ErrorData::header_mismatch(
            "events/list does not accept Mcp-Name header",
            None,
        ));
    }
    Ok(())
}

/// Validates the HTTP `Mcp-Name` header against `params.name` (supporting `=?base64?...?=`
/// decoding). Missing header is allowed; duplicate, malformed, or mismatched headers return
/// `ErrorData::header_mismatch` (HTTP 400 / -32020).
fn validate_mcp_name_header(
    context: &RequestContext<RoleServer>,
    expected_name: &str,
) -> Result<(), rmcp::ErrorData> {
    let Some(parts) = context.extensions.get::<::http::request::Parts>() else {
        return Ok(());
    };
    let mut values = parts.headers.get_all("mcp-name").iter();
    let Some(raw_header) = values.next() else {
        return Ok(());
    };
    if values.next().is_some() {
        return Err(rmcp::ErrorData::header_mismatch(
            "duplicate Mcp-Name header is not allowed",
            None,
        ));
    }
    let raw_str = raw_header.to_str().map_err(|_| {
        rmcp::ErrorData::header_mismatch("Mcp-Name header is not valid ASCII", None)
    })?;
    let decoded = decode_mcp_header_value(raw_str)
        .ok_or_else(|| rmcp::ErrorData::header_mismatch("Mcp-Name header is malformed", None))?;
    if decoded != expected_name {
        return Err(rmcp::ErrorData::header_mismatch(
            format!("Mcp-Name header ({decoded:?}) does not match params.name ({expected_name:?})"),
            None,
        ));
    }
    Ok(())
}

pub(super) fn build_event_notification(
    method: &str,
    params: serde_json::Value,
    subscription_id: &rmcp::model::RequestId,
) -> rmcp::model::ServerNotification {
    let mut notif = rmcp::model::CustomNotification::new(method, Some(params));
    rmcp::model::GetMeta::get_meta_mut(&mut notif).set_subscription_id(subscription_id.clone());
    rmcp::model::ServerNotification::CustomNotification(notif)
}

pub(crate) fn event_error_payload(error: ipc::IpcEventError) -> serde_json::Value {
    events::value_object([("error", error.into())])
}

pub(super) async fn send_event_notification(
    context: &RequestContext<RoleServer>,
    method: &str,
    params: lease::Owned<serde_json::Value>,
    subscription_id: &rmcp::model::RequestId,
    metadata: &lease::Owner,
) -> bool {
    use rmcp::model::GetExtensions;

    let lease::Owned { value, owner } = params;
    let mut notification = build_event_notification(method, value, subscription_id);
    // The peer may keep this notification queued after the request is cancelled.
    notification
        .extensions_mut()
        .insert(lease::Owner::new((owner, metadata.clone())));
    tokio::select! {
        biased;
        _ = context.ct.cancelled() => false,
        sent = context.peer.send_notification(notification) => sent.is_ok(),
    }
}

fn stream_complete_result(subscription_id: &rmcp::model::RequestId) -> rmcp::model::CustomResult {
    let id_value = serde_json::to_value(subscription_id).unwrap_or(serde_json::Value::Null);
    rmcp::model::CustomResult::new(events::value_object([
        (
            "resultType",
            serde_json::Value::String("complete".to_owned()),
        ),
        (
            "_meta",
            events::value_object([("io.modelcontextprotocol/subscriptionId", id_value)]),
        ),
    ]))
}

impl Server {
    pub(super) async fn handle_events_list(
        &self,
        params: Option<serde_json::Value>,
        context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::CustomResult, rmcp::ErrorData> {
        reject_unexpected_mcp_name_header(&context)?;
        if let Some(value) = params
            && !value.is_object()
            && !value.is_null()
        {
            return Err(rmcp::ErrorData::invalid_params(
                "events/list params must be an object",
                None,
            ));
        }
        let result = match &self.backend {
            Backend::Local { service, .. } => service
                .event_bus()
                .list_event_types()
                .map_err(|err| event_error_to_rmcp(err, self.pools().1))?,
            Backend::Daemon(endpoint) => endpoint
                .events_list(&client_name(&context), context.ct.clone())
                .await
                .map_err(|error| error.run(ipc_event_error_to_rmcp))?,
        };
        Ok(result
            .map(serde_json::Value::from)
            .run(rmcp::model::CustomResult::new))
    }

    pub(super) async fn handle_events_poll(
        &self,
        params: Option<serde_json::Value>,
        context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::CustomResult, rmcp::ErrorData> {
        let Some(raw) = params else {
            return Err(rmcp::ErrorData::invalid_params(
                "events/poll requires params with `name`",
                None,
            ));
        };
        let poll_req: events::PollRequest = serde_json::from_value(raw)
            .map_err(|e| rmcp::ErrorData::invalid_params(format!("invalid params: {e}"), None))?;
        validate_mcp_name_header(&context, &poll_req.name)?;

        let response = match &self.backend {
            Backend::Local { service, .. } => {
                let caller_session = self.session(&context).ok().map(|s| s.id.clone());
                service
                    .event_bus()
                    .poll(poll_req, caller_session.as_deref())
                    .map_err(|err| event_error_to_rmcp(err, self.pools().1))?
            }
            Backend::Daemon(endpoint) => endpoint
                .events_poll(&client_name(&context), poll_req, context.ct.clone())
                .await
                .map_err(|error| error.run(ipc_event_error_to_rmcp))?,
        };
        Ok(response
            .map(serde_json::Value::from)
            .run(rmcp::model::CustomResult::new))
    }

    pub(crate) async fn handle_events_stream(
        &self,
        params: Option<serde_json::Value>,
        context: RequestContext<RoleServer>,
    ) -> Result<rmcp::model::CustomResult, rmcp::ErrorData> {
        let Some(raw) = params else {
            return Err(rmcp::ErrorData::invalid_params(
                "events/stream requires params with name",
                None,
            ));
        };
        let sub_req: events::SubscribeRequest = serde_json::from_value(raw)
            .map_err(|e| rmcp::ErrorData::invalid_params(format!("invalid params: {e}"), None))?;
        validate_mcp_name_header(&context, &sub_req.name)?;
        let (pool, control) = self.pools();
        // Fixed metadata covers subscription ID copies, cursors, and terminal error envelopes.
        let metadata_bytes = lease::json_len(&(&sub_req, &context.id))
            .unwrap_or(usize::MAX)
            .saturating_mul(3)
            .saturating_add(2048);
        let metadata = lease::Owner::new(
            pool.try_acquire(metadata_bytes)
                .or_else(|| control.try_acquire(metadata_bytes))
                .ok_or_else(|| {
                    ipc::IpcEventError::memory_full(
                        pool,
                        control,
                        "Server active memory budget is full while preparing event subscription",
                    )
                    .run(ipc_event_error_to_rmcp)
                })?,
        );
        let sub_id = context.id.clone();
        let terminated = |error| lease::Owned::new(event_error_payload(error), metadata.clone());
        match &self.backend {
            Backend::Local { service, .. } => {
                let _guard = service
                    .event_bus()
                    .try_acquire_stream()
                    .map_err(|err| event_error_to_rmcp(err, control))?;
                let caller_session = self.session(&context).ok().map(|s| s.id.clone());
                let mut sub = service
                    .event_bus()
                    .subscribe(sub_req, caller_session.as_deref())
                    .map_err(|err| event_error_to_rmcp(err, control))?;
                let initial = sub
                    .initial_active()
                    .map_err(|err| event_error_to_rmcp(err, control))?
                    .map(serde_json::Value::from);
                if !send_event_notification(
                    &context,
                    "notifications/events/active",
                    initial,
                    &sub_id,
                    &metadata,
                )
                .await
                {
                    return Ok(stream_complete_result(&sub_id));
                }

                let heartbeat = std::time::Duration::from_millis(sub.heartbeat_ms());
                let sleep = tokio::time::sleep(heartbeat);
                tokio::pin!(sleep);
                loop {
                    tokio::select! {
                        biased;
                        _ = context.ct.cancelled() => {
                            // Client cancelled: rmcp drops the response when local_ct_pool entry is gone.
                            return Ok(stream_complete_result(&sub_id));
                        }
                        item = sub.next() => {
                            match item {
                                Ok(Some(lease::Owned { value, owner })) => {
                                    let (method, payload) = match value {
                                        events::StreamItem::Active(active) => ("notifications/events/active", active.into()),
                                        events::StreamItem::Event(event) => ("notifications/events/event", event.into()),
                                    };
                                    if !send_event_notification(&context, method, lease::Owned::new(payload, owner), &sub_id, &metadata).await {
                                        return Ok(stream_complete_result(&sub_id));
                                    }
                                    sleep.as_mut().reset(tokio::time::Instant::now() + heartbeat);
                                }
                                end => {
                                    let error = match end {
                                        Err(error) => error.into(),
                                        Ok(None) => ipc::IpcEventError {
                                            code: -32000,
                                            message: "event bus closed".to_owned(),
                                            data: None,
                                        },
                                        Ok(Some(_)) => unreachable!(),
                                    };
                                    send_event_notification(&context, "notifications/events/terminated", terminated(error), &sub_id, &metadata).await;
                                    return Ok(stream_complete_result(&sub_id));
                                }
                            }
                        }
                        _ = &mut sleep => {
                            let cursor = match sub.current_cursor() {
                                Ok(cursor) => cursor,
                                Err(error) => {
                                    send_event_notification(&context, "notifications/events/terminated", terminated(error.into()), &sub_id, &metadata).await;
                                    return Ok(stream_complete_result(&sub_id));
                                }
                            };
                            let payload = cursor.map(|cursor| events::value_object([("cursor", serde_json::Value::String(cursor))]));
                            if !send_event_notification(&context, "notifications/events/heartbeat", payload, &sub_id, &metadata).await {
                                return Ok(stream_complete_result(&sub_id));
                            }
                            sleep.as_mut().reset(tokio::time::Instant::now() + heartbeat);
                        }
                    }
                }
            }
            Backend::Daemon(endpoint) => {
                let client = client_name(&context);
                let (initial, mut stream) = endpoint
                    .events_stream(&client, sub_req, context.ct.clone())
                    .await
                    .map_err(|error| error.run(ipc_event_error_to_rmcp))?;

                if !send_event_notification(
                    &context,
                    "notifications/events/active",
                    initial.map(serde_json::Value::from),
                    &sub_id,
                    &metadata,
                )
                .await
                {
                    stream.cancel().await;
                    return Ok(stream_complete_result(&sub_id));
                }

                let mut terminated_sent = false;
                loop {
                    tokio::select! {
                        biased;
                        _ = context.ct.cancelled() => {
                            stream.cancel().await;
                            return Ok(stream_complete_result(&sub_id));
                        }
                        frame = stream.next() => {
                            let Some((frame, held)) = frame else {
                                if !terminated_sent {
                                    let error = if stream.overflowed() {
                                        ipc::IpcEventError {
                                            code: -32025,
                                            message:
                                                "endpoint event stream queue full due to slow consumer"
                                                    .to_owned(),
                                            data: None,
                                        }
                                    } else if stream.link_closed() {
                                        ipc::IpcEventError {
                                            code: -32000,
                                            message: "daemon connection closed".to_owned(),
                                            data: None,
                                        }
                                    } else {
                                        ipc::IpcEventError {
                                            code: -32025,
                                            message: "endpoint active memory budget full during event stream".to_owned(),
                                            data: None,
                                        }
                                    };
                                    send_event_notification(
                                        &context,
                                        "notifications/events/terminated",
                                        terminated(error),
                                        &sub_id,
                                        &metadata,
                                    )
                                    .await;
                                }
                                stream.cancel().await;
                                return Ok(stream_complete_result(&sub_id));
                            };
                            let owned = lease::Owned::new(frame, lease::Owner::new(held));
                            let (method, payload) = match owned.value {
                                ipc::EventWireFrame::Active(active) => {
                                    ("notifications/events/active", active.into())
                                }
                                ipc::EventWireFrame::Event(event) => {
                                    ("notifications/events/event", event.into())
                                }
                                ipc::EventWireFrame::Heartbeat { cursor } => {
                                    let payload = events::value_object([(
                                        "cursor",
                                        serde_json::Value::String(cursor),
                                    )]);
                                    ("notifications/events/heartbeat", payload)
                                }
                                ipc::EventWireFrame::Error(error) => {
                                    ("notifications/events/error", event_error_payload(error))
                                }
                                ipc::EventWireFrame::Terminated(error) => {
                                    terminated_sent = true;
                                    (
                                        "notifications/events/terminated",
                                        event_error_payload(error),
                                    )
                                }
                                ipc::EventWireFrame::Complete => {
                                    return Ok(stream_complete_result(&sub_id));
                                }
                                ipc::EventWireFrame::ListResult(_)
                                | ipc::EventWireFrame::PollResult(_) => continue,
                            };
                            if !send_event_notification(
                                &context,
                                method,
                                lease::Owned::new(payload, owned.owner),
                                &sub_id,
                                &metadata,
                            )
                            .await
                            {
                                stream.cancel().await;
                                return Ok(stream_complete_result(&sub_id));
                            }
                        }
                    }
                }
            }
        }
    }
}
