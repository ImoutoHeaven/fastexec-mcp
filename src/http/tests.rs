use super::*;
use bytes::Bytes;
use http::{Method, Version};
use http_body_util::Full;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn headers(value: &str) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(header::AUTHORIZATION, value.parse().unwrap());
    headers
}

fn digest(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

#[test]
fn the_tokens_admit_only_themselves_and_return_their_positions() {
    let tokens = ["0123456789abcdef", "fedcba9876543210", "a_different_token"];
    let digests = tokens.map(digest);
    for (index, token) in tokens.iter().enumerate() {
        for scheme in ["Bearer", "bearer", "BEARER"] {
            assert_eq!(
                authorized(&headers(&format!("{scheme} {token}")), &digests),
                Some(index)
            );
        }
    }
    for value in [
        "Bearer 0123456789abcdefx",
        "Bearer 0123456789abcde",
        "Bearer  0123456789abcdef",
        "Bearer ",
        "Basic 0123456789abcdef",
        "0123456789abcdef",
    ] {
        assert_eq!(authorized(&headers(value), &digests), None);
    }
    assert_eq!(authorized(&HeaderMap::new(), &digests), None);
    assert_eq!(authorized(&headers("Bearer 0123456789abcdef"), &[]), None);
}

#[test]
fn the_origins_are_this_listener_and_what_the_configuration_adds() {
    let bound = SocketAddr::from(([127, 0, 0, 2], 8787));
    let origins = allowed_origins(bound, &["https://app.example".to_owned()]);
    assert!(origins.contains(&"http://127.0.0.1:8787".to_owned()));
    assert!(origins.contains(&"http://localhost:8787".to_owned()));
    assert!(origins.contains(&"http://[::1]:8787".to_owned()));
    // The address actually bound is one this listener's own client may name.
    assert!(origins.contains(&"http://127.0.0.2:8787".to_owned()));
    assert!(origins.contains(&"https://app.example".to_owned()));
    assert!(allowed_hosts(bound, false).contains(&"127.0.0.2".to_owned()));
    // A daemon reachable through a proxy trusts the proxy for both.
    assert!(allowed_hosts(bound, true).is_empty());
}

#[derive(Clone)]
struct PocEventsHandler {
    gate: Arc<tokio::sync::Notify>,
}

impl rmcp::ServerHandler for PocEventsHandler {
    async fn on_custom_request(
        &self,
        request: rmcp::model::CustomRequest,
        context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<rmcp::model::CustomResult, rmcp::ErrorData> {
        use rmcp::model::{CustomNotification, CustomResult, ErrorCode, ServerNotification};
        use serde_json::json;

        match request.method.as_str() {
            "events/list" => Ok(CustomResult::new(json!({
                "resultType": "complete",
                "events": []
            }))),
            "events/poll" => Err(rmcp::ErrorData::new(
                ErrorCode(-32023),
                "event not found",
                Some(json!({"kind": "event"})),
            )),
            "events/stream" => {
                context
                    .peer
                    .send_notification(ServerNotification::CustomNotification(
                        CustomNotification::new(
                            "notifications/events/active",
                            Some(json!({
                                "cursor": "1:0",
                                "truncated": false,
                                "heartbeatMs": 30000
                            })),
                        ),
                    ))
                    .await
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;
                self.gate.notified().await;
                context
                    .peer
                    .send_notification(ServerNotification::CustomNotification(
                        CustomNotification::new(
                            "notifications/events/event",
                            Some(json!({
                                "eventId": "evt-1",
                                "name": "task.exited",
                                "cursor": "1:1",
                                "data": {"taskId": "t1"}
                            })),
                        ),
                    ))
                    .await
                    .map_err(|e| rmcp::ErrorData::internal_error(e.to_string(), None))?;
                Ok(CustomResult::new(json!({
                    "resultType": "complete"
                })))
            }
            _ => Err(rmcp::ErrorData::new(
                ErrorCode::METHOD_NOT_FOUND,
                request.method,
                None,
            )),
        }
    }
}

fn poc_service(
    gate: Arc<tokio::sync::Notify>,
    bound: SocketAddr,
) -> StreamableHttpService<PocEventsHandler, Sessions> {
    let mut config = StreamableHttpServerConfig::default();
    config.legacy_session_mode = true;
    config.stateless_protocol_metadata_required = false;
    config.json_response = true;
    config.sse_keep_alive = Some(Duration::from_secs(15));
    config.sse_retry = None;
    config.allowed_hosts = allowed_hosts(bound, false);
    let sessions = Arc::new(Sessions::new(SessionConfig::default(), MAX_HTTP_SESSIONS));
    StreamableHttpService::new(
        move || {
            Ok(PocEventsHandler {
                gate: Arc::clone(&gate),
            })
        },
        sessions,
        config,
    )
}

fn modern_post_body(id: u64, method: &str, params: serde_json::Value) -> Bytes {
    let mut map = params.as_object().cloned().unwrap_or_default();
    map.insert(
        "_meta".to_owned(),
        serde_json::json!({
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientInfo": {"name": "test", "version": "1"},
            "io.modelcontextprotocol/clientCapabilities": {}
        }),
    );
    Bytes::from(
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": map
        })
        .to_string(),
    )
}

#[tokio::test]
async fn rmcp_json_response_returns_json_for_single_reply_and_business_error() {
    use tower_service::Service as _;

    let gate = Arc::new(tokio::sync::Notify::new());
    let mut service = poc_service(gate, SocketAddr::from(([127, 0, 0, 1], 0)));

    let list_req = Request::post("/mcp")
        .header("host", "localhost")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "events/list")
        .body(Full::new(modern_post_body(
            1,
            "events/list",
            serde_json::json!({}),
        )))
        .unwrap();
    let list_res = service.call(list_req).await.unwrap();
    assert_eq!(list_res.status(), StatusCode::OK);
    let content_type = list_res.headers()[header::CONTENT_TYPE].to_str().unwrap();
    assert!(
        content_type.starts_with("application/json"),
        "unexpected content-type: {content_type}"
    );
    let body: serde_json::Value =
        serde_json::from_slice(&list_res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["result"]["resultType"], "complete");

    let err_req = Request::post("/mcp")
        .header("host", "localhost")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "events/poll")
        .body(Full::new(modern_post_body(
            2,
            "events/poll",
            serde_json::json!({"name": "missing"}),
        )))
        .unwrap();
    let err_res = service.call(err_req).await.unwrap();
    assert_eq!(err_res.status(), StatusCode::OK);
    let content_type = err_res.headers()[header::CONTENT_TYPE].to_str().unwrap();
    assert!(
        content_type.starts_with("application/json"),
        "unexpected content-type: {content_type}"
    );
    let body: serde_json::Value =
        serde_json::from_slice(&err_res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["error"]["code"], -32023);
    assert_eq!(body["error"]["data"]["kind"], "event");
}

#[tokio::test]
async fn rmcp_custom_request_flushes_sse_event_notifications_before_complete() {
    use tower_service::Service as _;

    let gate = Arc::new(tokio::sync::Notify::new());
    let mut service = poc_service(Arc::clone(&gate), SocketAddr::from(([127, 0, 0, 1], 0)));

    let req = Request::post("/mcp")
        .header("host", "localhost")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "events/stream")
        .body(Full::new(modern_post_body(
            7,
            "events/stream",
            serde_json::json!({"name": "task.exited"}),
        )))
        .unwrap();
    let res = service.call(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let content_type = res.headers()[header::CONTENT_TYPE].to_str().unwrap();
    assert!(
        content_type.starts_with("text/event-stream"),
        "unexpected content-type: {content_type}"
    );

    let mut body = res.into_body();
    let mut sse_buf = String::new();
    let mut data_frames = Vec::new();
    let read_next_data = |buf: &mut String, out: &mut Vec<serde_json::Value>| {
        while let Some(pos) = buf.find("\n\n") {
            let block = buf[..pos].to_owned();
            buf.drain(..pos + 2);
            for line in block.lines() {
                if let Some(payload) = line.strip_prefix("data:") {
                    let trimmed = payload.trim();
                    if !trimmed.is_empty() {
                        out.push(serde_json::from_str(trimmed).unwrap());
                    }
                }
            }
        }
    };

    // Must observe the first SSE frame before releasing the handler gate.
    tokio::time::timeout(Duration::from_secs(5), async {
        while data_frames.is_empty() {
            let frame = body.frame().await.expect("stream ended early").unwrap();
            if let Some(data) = frame.data_ref() {
                sse_buf.push_str(std::str::from_utf8(data).unwrap());
                read_next_data(&mut sse_buf, &mut data_frames);
            }
        }
    })
    .await
    .expect("timed out waiting for immediate active SSE frame");
    assert_eq!(data_frames[0]["method"], "notifications/events/active");

    gate.notify_one();

    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(frame) = body.frame().await {
            if let Some(data) = frame.unwrap().data_ref() {
                sse_buf.push_str(std::str::from_utf8(data).unwrap());
                read_next_data(&mut sse_buf, &mut data_frames);
            }
        }
    })
    .await
    .expect("timed out waiting for remaining SSE frames");

    assert_eq!(data_frames.len(), 3, "frames: {data_frames:?}");
    assert_eq!(data_frames[1]["method"], "notifications/events/event");
    assert_eq!(data_frames[2]["id"], 7);
    assert_eq!(data_frames[2]["result"]["resultType"], "complete");
}

#[tokio::test]
async fn http2_h2c_concurrent_requests_and_refusal_omit_connection_close() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound = listener.local_addr().unwrap();
    let stopped = CancellationToken::new();
    let token = "phase1-h2c-token";
    let gate = Arc::new(tokio::sync::Notify::new());
    let layer = Bearer {
        digests: Arc::from([digest(token)]),
        table: Arc::new(Transfers::new()),
        memory: BytePool::new(1 << 20),
        transfer_slots: Arc::new(Semaphore::new(MAX_TRANSFER_CONNECTIONS)),
        stopped: stopped.clone(),
        inner: poc_service(gate, bound),
    };
    let server_task = tokio::spawn(http_wire::serve_connections(
        listener,
        layer,
        stopped.clone(),
        None,
    ));

    let stream = tokio::net::TcpStream::connect(bound).await.unwrap();
    let (mut sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .unwrap();
    let conn_task = tokio::spawn(async move {
        let _ = conn.await;
    });

    let make_mcp_req = |id: u64| {
        Request::builder()
            .method(Method::POST)
            .uri(format!("http://{bound}/mcp"))
            .version(Version::HTTP_2)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::ACCEPT, "application/json, text/event-stream")
            .header(header::CONTENT_TYPE, "application/json")
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", "events/list")
            .body(Full::new(modern_post_body(
                id,
                "events/list",
                serde_json::json!({}),
            )))
            .unwrap()
    };

    let f1 = sender.send_request(make_mcp_req(10));
    let f2 = sender.send_request(make_mcp_req(11));
    let (r1, r2) = tokio::join!(f1, f2);
    for (res, expected_id) in [(r1.unwrap(), 10), (r2.unwrap(), 11)] {
        assert_eq!(res.version(), Version::HTTP_2);
        assert_eq!(res.status(), StatusCode::OK);
        assert!(
            res.headers()[header::CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("application/json")
        );
        let body: serde_json::Value =
            serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(body["id"], expected_id);
        assert_eq!(body["result"]["resultType"], "complete");
    }

    // HTTP/2 refusal and transfer error must omit `Connection: close` (RFC 9113 §8.2.2).
    let unauth = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{bound}/mcp"))
        .version(Version::HTTP_2)
        .body(Full::new(Bytes::new()))
        .unwrap();
    let unauth_res = sender.send_request(unauth).await.unwrap();
    assert_eq!(unauth_res.version(), Version::HTTP_2);
    assert_eq!(unauth_res.status(), StatusCode::UNAUTHORIZED);
    assert!(!unauth_res.headers().contains_key(header::CONNECTION));

    let missing_transfer = Request::builder()
        .method(Method::GET)
        .uri(format!("http://{bound}/t/invalid"))
        .version(Version::HTTP_2)
        .body(Full::new(Bytes::new()))
        .unwrap();
    let transfer_res = sender.send_request(missing_transfer).await.unwrap();
    assert_eq!(transfer_res.version(), Version::HTTP_2);
    assert_eq!(transfer_res.status(), StatusCode::NOT_FOUND);
    assert!(!transfer_res.headers().contains_key(header::CONNECTION));

    // HTTP/1.1 still sets `Connection: close`.
    assert_eq!(
        http_wire::refusal(Version::HTTP_11, StatusCode::UNAUTHORIZED, "Bearer")
            .headers()
            .get(header::CONNECTION)
            .and_then(|v| v.to_str().ok()),
        Some("close")
    );
    assert_eq!(
        http_wire::transfer_empty(Version::HTTP_11, StatusCode::NOT_FOUND, &[])
            .headers()
            .get(header::CONNECTION)
            .and_then(|v| v.to_str().ok()),
        Some("close")
    );

    drop(sender);
    stopped.cancel();
    let _ = server_task.await;
    let _ = conn_task.await;
}

fn real_http_service(
    bound: SocketAddr,
) -> (
    StreamableHttpService<Server, Sessions>,
    Arc<crate::service::Service>,
) {
    let tasks = crate::tasks::Tasks::new(crate::tasks::Limits::OFF).unwrap();
    let service = Arc::new(crate::service::Service::new(tasks, true));
    let launch = Arc::new(LaunchContext::current().unwrap());
    let tools = Tools {
        codemode: true,
        transfer: false,
        yolo: false,
    };
    let (instructions, tool_instructions) =
        crate::config::compose_instructions(true, false, None, InstructionsToolMode::Off);
    let router = Arc::new(Server::build_router(&launch, tools, None));
    let principals: Arc<[Arc<Session>]> = vec![Session::new(
        "t1".to_owned(),
        "http-t1".to_owned(),
        Arc::clone(&launch),
    )]
    .into();

    let svc_clone = Arc::clone(&service);
    let factory = move || {
        let backend = Backend::Local {
            service: Arc::clone(&svc_clone),
            session: Arc::new(OnceLock::new()),
            launch: Arc::clone(&launch),
            daemon: true,
        };
        let mut server = Server::with_shared(
            backend,
            Arc::clone(&router),
            Arc::clone(&instructions),
            tool_instructions.clone(),
            None,
            tools,
        );
        server.principals = Some(Arc::clone(&principals));
        Ok(server)
    };

    let mut config = StreamableHttpServerConfig::default();
    config.legacy_session_mode = true;
    config.stateless_protocol_metadata_required = false;
    config.json_response = true;
    config.sse_keep_alive = Some(Duration::from_secs(15));
    config.sse_retry = None;
    config.allowed_hosts = allowed_hosts(bound, false);
    let sessions = Arc::new(Sessions::new(SessionConfig::default(), MAX_HTTP_SESSIONS));
    (
        StreamableHttpService::new(factory, sessions, config),
        service,
    )
}

#[tokio::test]
async fn three_client_suites_modern_events_modern_plain_and_legacy_2025() {
    use base64::Engine as _;
    use tower_service::Service as _;

    let (mut http_svc, core_service) = real_http_service(SocketAddr::from(([127, 0, 0, 1], 0)));

    // --- Suite 1: 2026-07-28 + Events ---
    // 1a. server/discover advertises io.modelcontextprotocol/events with listChanged: false.
    let mut disc_req = Request::post("/mcp")
        .header("host", "localhost")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "server/discover")
        .body(Full::new(modern_post_body(
            1,
            "server/discover",
            serde_json::json!({}),
        )))
        .unwrap();
    disc_req.extensions_mut().insert(Principal(0));
    let disc_res = http_svc.call(disc_req).await.unwrap();
    assert_eq!(disc_res.status(), StatusCode::OK);
    let disc_body: serde_json::Value =
        serde_json::from_slice(&disc_res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(
        disc_body["result"]["capabilities"]["extensions"]["io.modelcontextprotocol/events"],
        serde_json::json!({ "listChanged": false })
    );

    // 1b. events/list returns application/json with resultType: "complete" and v1 events.
    let mut list_req = Request::post("/mcp")
        .header("host", "localhost")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "events/list")
        .body(Full::new(modern_post_body(
            2,
            "events/list",
            serde_json::json!({}),
        )))
        .unwrap();
    list_req.extensions_mut().insert(Principal(0));
    let list_res = http_svc.call(list_req).await.unwrap();
    assert_eq!(list_res.status(), StatusCode::OK);
    assert!(
        list_res.headers()[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
    let list_body: serde_json::Value =
        serde_json::from_slice(&list_res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(list_body["result"]["resultType"], "complete");
    assert_eq!(list_body["result"]["events"].as_array().unwrap().len(), 2);

    // 1c. events/poll with base64 Mcp-Name succeeds (without client declaring events cap).
    let start_cursor = core_service.event_bus().current_cursor();
    core_service
        .event_bus()
        .publish_external(crate::events::ExternalEventInput {
            event_type: None,
            source: "github".to_owned(),
            topic: Some("pr".to_owned()),
            name: "merged".to_owned(),
            event_id: "u-1".to_owned(),
            timestamp: None,
            data: serde_json::json!({ "secret": "TOP_SECRET_DATA" }),
        })
        .unwrap();
    let b64_name = format!(
        "=?base64?{}?=",
        base64::engine::general_purpose::STANDARD.encode("external.event")
    );
    let mut poll_req = Request::post("/mcp")
        .header("host", "localhost")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "events/poll")
        .header("mcp-name", b64_name)
        .body(Full::new(modern_post_body(
            3,
            "events/poll",
            serde_json::json!({
                "name": "external.event",
                "cursor": start_cursor
            }),
        )))
        .unwrap();
    poll_req.extensions_mut().insert(Principal(0));
    let poll_res = http_svc.call(poll_req).await.unwrap();
    assert_eq!(poll_res.status(), StatusCode::OK);
    let poll_body: serde_json::Value =
        serde_json::from_slice(&poll_res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(poll_body["result"]["resultType"], "complete");
    assert_eq!(poll_body["result"]["events"].as_array().unwrap().len(), 1);
    assert!(poll_body["result"]["events"][0].get("cursor").is_none());

    // 1d. Mcp-Name mismatch -> HTTP 400 + -32020.
    let mut bad_hdr = Request::post("/mcp")
        .header("host", "localhost")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "events/poll")
        .header("mcp-name", "task.exited")
        .body(Full::new(modern_post_body(
            4,
            "events/poll",
            serde_json::json!({ "name": "external.event" }),
        )))
        .unwrap();
    bad_hdr.extensions_mut().insert(Principal(0));
    let bad_res = http_svc.call(bad_hdr).await.unwrap();
    assert_eq!(bad_res.status(), StatusCode::BAD_REQUEST);
    let bad_body: serde_json::Value =
        serde_json::from_slice(&bad_res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(bad_body["error"]["code"], -32020);

    // 1e. Unknown event -> HTTP 200 + -32023; events/subscribe -> HTTP 404 + -32601.
    let mut unknown_req = Request::post("/mcp")
        .header("host", "localhost")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "events/poll")
        .body(Full::new(modern_post_body(
            5,
            "events/poll",
            serde_json::json!({ "name": "no.such.event" }),
        )))
        .unwrap();
    unknown_req.extensions_mut().insert(Principal(0));
    let unknown_res = http_svc.call(unknown_req).await.unwrap();
    assert_eq!(unknown_res.status(), StatusCode::OK);
    let unknown_body: serde_json::Value =
        serde_json::from_slice(&unknown_res.into_body().collect().await.unwrap().to_bytes())
            .unwrap();
    assert_eq!(unknown_body["error"]["code"], -32023);
    assert_eq!(unknown_body["error"]["data"]["kind"], "event");

    let mut sub_req = Request::post("/mcp")
        .header("host", "localhost")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "events/subscribe")
        .body(Full::new(modern_post_body(
            6,
            "events/subscribe",
            serde_json::json!({}),
        )))
        .unwrap();
    sub_req.extensions_mut().insert(Principal(0));
    let sub_res = http_svc.call(sub_req).await.unwrap();
    assert_eq!(sub_res.status(), StatusCode::NOT_FOUND);

    // 1f. events/stream delivers active + event with numeric subscriptionId preserved.
    let stream_cursor = core_service.event_bus().current_cursor();
    let mut stream_req = Request::post("/mcp")
        .header("host", "localhost")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "events/stream")
        .header("mcp-name", "external.event")
        .body(Full::new(modern_post_body(
            77,
            "events/stream",
            serde_json::json!({
                "name": "external.event",
                "cursor": stream_cursor
            }),
        )))
        .unwrap();
    stream_req.extensions_mut().insert(Principal(0));
    let stream_res = http_svc.call(stream_req).await.unwrap();
    assert_eq!(stream_res.status(), StatusCode::OK);
    assert!(
        stream_res.headers()[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );
    let mut stream_body = stream_res.into_body();
    let first_chunk = tokio::time::timeout(Duration::from_secs(5), stream_body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    let first_text = String::from_utf8(first_chunk.to_vec()).unwrap();
    let active_msg: serde_json::Value = first_text
        .lines()
        .find_map(|l| l.strip_prefix("data:"))
        .map(|d| serde_json::from_str(d.trim()).unwrap())
        .unwrap();
    assert_eq!(active_msg["method"], "notifications/events/active");
    assert_eq!(active_msg["params"]["heartbeatMs"], 30000);
    // Numeric request id 77 must remain a JSON integer, not "77".
    assert_eq!(
        active_msg["params"]["_meta"]["io.modelcontextprotocol/subscriptionId"].as_i64(),
        Some(77)
    );
    drop(stream_body);

    // --- Suite 2: 2026-07-28 without Events ---
    // Calls tools/call directly; receives JSON response with sanitized external event summary
    // and never receives any notifications/events/*.
    let mut call_req = Request::post("/mcp")
        .header("host", "localhost")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "tools/call")
        .header("mcp-name", "codemode")
        .body(Full::new(modern_post_body(
            10,
            "tools/call",
            serde_json::json!({
                "name": "codemode",
                "arguments": { "code": "return 42" }
            }),
        )))
        .unwrap();
    call_req.extensions_mut().insert(Principal(0));
    let call_res = http_svc.call(call_req).await.unwrap();
    assert_eq!(call_res.status(), StatusCode::OK);
    let call_text = String::from_utf8(
        call_res
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(!call_text.contains("notifications/events/"));
    assert!(
        call_text.contains("External events: 1 new"),
        "unexpected call_text: {call_text}"
    );
    assert!(!call_text.contains("TOP_SECRET_DATA"));

    // --- Suite 3: 2025-11-25 legacy client ---
    let mut init_req = Request::post("/mcp")
        .header("host", "localhost")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "clientInfo": { "name": "legacy", "version": "1.0" }
                }
            })
            .to_string(),
        )))
        .unwrap();
    init_req.extensions_mut().insert(Principal(0));
    let init_res = http_svc.call(init_req).await.unwrap();
    assert_eq!(init_res.status(), StatusCode::OK);
    let session_id = init_res.headers()["mcp-session-id"]
        .to_str()
        .unwrap()
        .to_owned();
    let init_text = String::from_utf8(
        init_res
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    let init_msg: serde_json::Value = init_text
        .lines()
        .find_map(|l| l.strip_prefix("data:"))
        .map(|d| serde_json::from_str(d.trim()).unwrap())
        .unwrap();
    assert!(
        init_msg["result"]["capabilities"]
            .get("extensions")
            .is_none(),
        "legacy initialize must not advertise io.modelcontextprotocol/events"
    );

    // Complete legacy handshake with notifications/initialized.
    let mut inited_req = Request::post("/mcp")
        .header("host", "localhost")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("mcp-session-id", &session_id)
        .header("mcp-protocol-version", "2025-11-25")
        .body(Full::new(Bytes::from(
            serde_json::json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized"
            })
            .to_string(),
        )))
        .unwrap();
    inited_req.extensions_mut().insert(Principal(0));
    let inited_res = http_svc.call(inited_req).await.unwrap();
    assert_eq!(inited_res.status(), StatusCode::ACCEPTED);

    // Calling events/list on a 2025-11-25 session must return -32601.
    let mut legacy_ev = Request::post("/mcp")
        .header("host", "localhost")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("mcp-session-id", &session_id)
        .header("mcp-protocol-version", "2025-11-25")
        .body(Full::new(Bytes::from(
            serde_json::json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "events/list",
                "params": {}
            })
            .to_string(),
        )))
        .unwrap();
    legacy_ev.extensions_mut().insert(Principal(0));
    let legacy_res = http_svc.call(legacy_ev).await.unwrap();
    let legacy_text = String::from_utf8(
        legacy_res
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    let legacy_msg: serde_json::Value = legacy_text
        .lines()
        .filter_map(|l| l.strip_prefix("data:"))
        .map(str::trim)
        .find(|s| !s.is_empty())
        .or(Some(legacy_text.trim()))
        .map(|d| serde_json::from_str(d).unwrap())
        .unwrap();
    assert_eq!(legacy_msg["error"]["code"], -32601);

    // --- Suite 4: Strict Mcp-Name header checks ---
    // 4a. Duplicate Mcp-Name header -> HTTP 400 + -32020.
    let mut dup_hdr = Request::post("/mcp")
        .header("host", "localhost")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "events/poll")
        .header("mcp-name", "external.event")
        .header("mcp-name", "external.event")
        .body(Full::new(modern_post_body(
            30,
            "events/poll",
            serde_json::json!({ "name": "external.event" }),
        )))
        .unwrap();
    dup_hdr.extensions_mut().insert(Principal(0));
    let dup_res = http_svc.call(dup_hdr).await.unwrap();
    assert_eq!(dup_res.status(), StatusCode::BAD_REQUEST);
    let dup_body: serde_json::Value =
        serde_json::from_slice(&dup_res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(dup_body["error"]["code"], -32020);

    // 4b. Bare Mcp-Name with surrounding space or HTAB -> HTTP 400 + -32020.
    for raw_val in [" external.event", "external.event ", "external\tevent"] {
        let mut ws_hdr = Request::post("/mcp")
            .header("host", "localhost")
            .header("accept", "application/json, text/event-stream")
            .header("content-type", "application/json")
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", "events/poll")
            .header("mcp-name", raw_val)
            .body(Full::new(modern_post_body(
                31,
                "events/poll",
                serde_json::json!({ "name": "external.event" }),
            )))
            .unwrap();
        ws_hdr.extensions_mut().insert(Principal(0));
        let ws_res = http_svc.call(ws_hdr).await.unwrap();
        assert_eq!(ws_res.status(), StatusCode::BAD_REQUEST, "val={raw_val:?}");
        let ws_body: serde_json::Value =
            serde_json::from_slice(&ws_res.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(ws_body["error"]["code"], -32020, "val={raw_val:?}");
    }

    // 4c. events/list with unexpected Mcp-Name header -> HTTP 400 + -32020.
    let mut list_hdr = Request::post("/mcp")
        .header("host", "localhost")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "events/list")
        .header("mcp-name", "external.event")
        .body(Full::new(modern_post_body(
            32,
            "events/list",
            serde_json::json!({}),
        )))
        .unwrap();
    list_hdr.extensions_mut().insert(Principal(0));
    let list_hdr_res = http_svc.call(list_hdr).await.unwrap();
    assert_eq!(list_hdr_res.status(), StatusCode::BAD_REQUEST);
    let list_hdr_body: serde_json::Value =
        serde_json::from_slice(&list_hdr_res.into_body().collect().await.unwrap().to_bytes())
            .unwrap();
    assert_eq!(list_hdr_body["error"]["code"], -32020);
}

#[tokio::test]
async fn transfer_subquota_returns_503_while_preserving_mcp_control_plane() {
    let dir = std::env::temp_dir().join(format!(
        "fastexec-transfer-subquota-{}",
        crate::id::random_chars(12).unwrap()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let sample = dir.join("payload.bin");
    std::fs::File::create(&sample)
        .unwrap()
        .set_len(64 << 20)
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound = listener.local_addr().unwrap();
    let stopped = CancellationToken::new();
    let token = "subquota-test-token";
    let table = Arc::new(Transfers::new());
    let id = table.create(sample.clone()).unwrap();
    let inner = poc_service(Arc::new(tokio::sync::Notify::new()), bound);
    let layer = Bearer {
        digests: Arc::from([digest(token)]),
        table: Arc::clone(&table),
        memory: BytePool::new(1 << 20),
        transfer_slots: Arc::new(Semaphore::new(2)),
        stopped: stopped.clone(),
        inner,
    };
    let server_task = tokio::spawn(http_wire::serve_connections(
        listener,
        layer,
        stopped.clone(),
        None,
    ));

    let open_transfer = || async {
        let mut s = tokio::net::TcpStream::connect(bound).await.unwrap();
        let req = format!("GET /t/{id} HTTP/1.1\r\nHost: {bound}\r\n\r\n");
        s.write_all(req.as_bytes()).await.unwrap();
        let mut head = [0u8; 64];
        let n = s.read(&mut head).await.unwrap();
        (s, String::from_utf8_lossy(&head[..n]).into_owned())
    };

    // Hold both `/t/` sub-quota slots with unread streaming download bodies.
    let (s1, h1) = open_transfer().await;
    assert!(h1.starts_with("HTTP/1.1 200 OK"), "{h1}");
    let (s2, h2) = open_transfer().await;
    assert!(h2.starts_with("HTTP/1.1 200 OK"), "{h2}");

    // 3rd `/t/` request gets 503 Service Unavailable with Connection: close and Content-Length: 0.
    let mut s3 = tokio::net::TcpStream::connect(bound).await.unwrap();
    let req3 = format!("GET /t/{id} HTTP/1.1\r\nHost: {bound}\r\n\r\n");
    s3.write_all(req3.as_bytes()).await.unwrap();
    let mut buf3 = Vec::new();
    s3.read_to_end(&mut buf3).await.unwrap();
    let text3 = String::from_utf8_lossy(&buf3);
    assert!(
        text3.starts_with("HTTP/1.1 503 Service Unavailable\r\n"),
        "{text3}"
    );
    assert!(
        text3.to_ascii_lowercase().contains("connection: close\r\n"),
        "{text3}"
    );
    assert!(
        text3.to_ascii_lowercase().contains("content-length: 0\r\n"),
        "{text3}"
    );

    // Meanwhile `/mcp` control plane still succeeds!
    let mut mcp_stream = tokio::net::TcpStream::connect(bound).await.unwrap();
    let body = modern_post_body(1, "events/list", serde_json::json!({}));
    let mcp_req = format!(
        "POST /mcp HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\nAccept: application/json, text/event-stream\r\nContent-Type: application/json\r\nMcp-Protocol-Version: 2026-07-28\r\nMcp-Method: events/list\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    mcp_stream.write_all(mcp_req.as_bytes()).await.unwrap();
    mcp_stream.write_all(&body).await.unwrap();
    let mut mcp_out = Vec::new();
    mcp_stream.read_to_end(&mut mcp_out).await.unwrap();
    let mcp_text = String::from_utf8_lossy(&mcp_out);
    assert!(mcp_text.starts_with("HTTP/1.1 200 OK\r\n"), "{mcp_text}");

    drop(s1);
    drop(s2);
    stopped.cancel();
    let _ = server_task.await;
    let _ = std::fs::remove_dir_all(&dir);
}

enum TestHttpSender {
    Http1(hyper::client::conn::http1::SendRequest<Full<Bytes>>),
    Http2(hyper::client::conn::http2::SendRequest<Full<Bytes>>),
}

impl TestHttpSender {
    async fn connect(bound: SocketAddr, http2: bool) -> (Self, tokio::task::JoinHandle<()>) {
        let stream = tokio::net::TcpStream::connect(bound).await.unwrap();
        if http2 {
            let (sender, connection) =
                hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
                    .await
                    .unwrap();
            let task = tokio::spawn(async move {
                let _ = connection.await;
            });
            (Self::Http2(sender), task)
        } else {
            let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
                .await
                .unwrap();
            let task = tokio::spawn(async move {
                let _ = connection.await;
            });
            (Self::Http1(sender), task)
        }
    }

    async fn try_send(
        &mut self,
        request: Request<Full<Bytes>>,
    ) -> Result<Response<Incoming>, hyper::Error> {
        match self {
            Self::Http1(sender) => {
                sender.ready().await?;
                sender.send_request(request).await
            }
            Self::Http2(sender) => {
                sender.ready().await?;
                sender.send_request(request).await
            }
        }
    }

    async fn send(&mut self, request: Request<Full<Bytes>>) -> Response<Incoming> {
        self.try_send(request).await.unwrap()
    }
}

#[tokio::test]
async fn transfer_connection_quota_survives_completed_requests_on_http1_and_http2() {
    let dir = std::env::temp_dir().join(format!(
        "fastexec-transfer-connections-{}",
        crate::id::random_chars(12).unwrap()
    ));
    std::fs::create_dir(&dir).unwrap();
    let sample = dir.join("ordinary.txt");
    std::fs::write(&sample, b"ordinary").unwrap();

    for http2 in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = listener.local_addr().unwrap();
        let stopped = CancellationToken::new();
        let token = "transfer-connections-test-token";
        let table = Arc::new(Transfers::new());
        let id = table.create(sample.clone()).unwrap();
        let connections = Arc::new(Semaphore::new(2));
        let layer = Bearer {
            digests: Arc::from([digest(token)]),
            table,
            memory: BytePool::new(1 << 20),
            transfer_slots: Arc::new(Semaphore::new(2)),
            stopped: stopped.clone(),
            inner: poc_service(Arc::new(tokio::sync::Notify::new()), bound),
        };
        let server_task = tokio::spawn(http_wire::serve_connections(
            listener,
            layer,
            stopped.clone(),
            Some(Arc::clone(&connections)),
        ));
        let version = if http2 {
            Version::HTTP_2
        } else {
            Version::HTTP_11
        };
        let transfer_request = || {
            Request::get(format!("http://{bound}/t/{id}"))
                .version(version)
                .header(header::HOST, bound.to_string())
                .body(Full::new(Bytes::new()))
                .unwrap()
        };
        let (mut first, first_task) = TestHttpSender::connect(bound, http2).await;
        for _ in 0..2 {
            let response = first.send(transfer_request()).await;
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(&body[..], b"ordinary");
            assert_eq!(connections.available_permits(), 1);
        }
        let (mut second, second_task) = TestHttpSender::connect(bound, http2).await;
        let response = second.send(transfer_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        response.into_body().collect().await.unwrap();
        assert_eq!(connections.available_permits(), 0);

        let (mut refused, refused_task) = TestHttpSender::connect(bound, http2).await;
        let response = refused.send(transfer_request()).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers().contains_key(header::CONNECTION), !http2);
        response.into_body().collect().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), refused_task)
            .await
            .expect("a refused transfer connection must close after its response")
            .unwrap();
        drop(refused);

        let (mut control, control_task) = TestHttpSender::connect(bound, http2).await;
        let request = Request::post(format!("http://{bound}/mcp"))
            .version(version)
            .header(header::HOST, "localhost")
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::ACCEPT, "application/json, text/event-stream")
            .header(header::CONTENT_TYPE, "application/json")
            .header("mcp-protocol-version", "2026-07-28")
            .header("mcp-method", "events/list")
            .body(Full::new(modern_post_body(
                1,
                "events/list",
                serde_json::json!({}),
            )))
            .unwrap();
        let response = control.send(request).await;
        assert_eq!(response.status(), StatusCode::OK);
        response.into_body().collect().await.unwrap();

        drop(first);
        tokio::time::timeout(Duration::from_secs(5), first_task)
            .await
            .unwrap()
            .unwrap();
        let released = tokio::time::timeout(Duration::from_secs(5), connections.acquire())
            .await
            .expect("closing the connection must release its transfer slot")
            .unwrap();
        drop(released);
        let (mut replacement, replacement_task) = TestHttpSender::connect(bound, http2).await;
        let response = replacement.send(transfer_request()).await;
        assert_eq!(response.status(), StatusCode::OK);
        response.into_body().collect().await.unwrap();
        assert_eq!(connections.available_permits(), 0);

        drop((second, control, replacement));
        stopped.cancel();
        server_task.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            second_task.await.unwrap();
            control_task.await.unwrap();
            replacement_task.await.unwrap();
        })
        .await
        .unwrap();
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn g12_transfer_routes_stay_isolated_to_injected_byte_pool() {
    const EXACT_NAME: &str = "http::tests::g12_transfer_routes_stay_isolated_to_injected_byte_pool";
    if !std::env::args().any(|arg| arg == "--exact") {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", EXACT_NAME, "--nocapture"])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(verify_g12_transfer_pool_isolation());
}

async fn verify_g12_transfer_pool_isolation() {
    assert_eq!(crate::lease::process_pool().used(), 0);
    let download_payload = vec![b'x'; 8192];

    for shared_bearer in [true, false] {
        let dir = std::env::temp_dir().join(format!(
            "fastexec-g12-transfer-{}",
            crate::id::random_chars(12).unwrap()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let nested_upload = dir
            .join("nested")
            .join("deeper")
            .join("leaf")
            .join("uploaded.bin");
        let existing_download = dir.join("existing.bin");
        std::fs::write(&existing_download, &download_payload).unwrap();

        let table = Arc::new(Transfers::new());
        let upload_id = table.create(nested_upload.clone()).unwrap();
        let download_id = table.create(existing_download.clone()).unwrap();

        let pool = BytePool::new(64 * 1024);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = listener.local_addr().unwrap();
        let stopped = CancellationToken::new();

        let server_task = if shared_bearer {
            let layer = Bearer {
                digests: Arc::from([digest("g12-token")]),
                table: Arc::clone(&table),
                memory: Arc::clone(&pool),
                transfer_slots: Arc::new(Semaphore::new(8)),
                stopped: stopped.clone(),
                inner: poc_service(Arc::new(tokio::sync::Notify::new()), bound),
            };
            tokio::spawn(http_wire::serve_connections(
                listener,
                layer,
                stopped.clone(),
                None,
            ))
        } else {
            tokio::spawn(http_wire::serve_transfers(
                listener,
                Arc::clone(&table),
                Arc::clone(&pool),
                stopped.clone(),
            ))
        };

        let make_req = |method: Method, path: String, body: Bytes, version: Version| {
            Request::builder()
                .method(method)
                .uri(format!("http://{bound}{path}"))
                .version(version)
                .header(header::HOST, bound.to_string())
                .body(Full::new(body))
                .unwrap()
        };

        // (a1) Fully saturated isolated pool: HEAD, PUT (missing + existing), and GET fail with
        // zero writes on disk and zero fallback to global `process_pool()`.
        let full_hold = pool.try_acquire(pool.limit()).unwrap();
        let (mut h2_client, h2_conn) = TestHttpSender::connect(bound, true).await;

        let head_refused = h2_client
            .send(make_req(
                Method::HEAD,
                format!("/t/{upload_id}"),
                Bytes::new(),
                Version::HTTP_2,
            ))
            .await;
        assert_eq!(head_refused.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(crate::lease::process_pool().used(), 0);

        let put_missing_refused = h2_client
            .send(make_req(
                Method::PUT,
                format!("/t/{upload_id}?offset=0"),
                Bytes::from_static(b"blocked"),
                Version::HTTP_2,
            ))
            .await;
        assert_eq!(
            put_missing_refused.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert!(!dir.join("nested").exists());
        assert!(!nested_upload.exists());
        assert_eq!(crate::lease::process_pool().used(), 0);

        let put_existing_refused = h2_client
            .send(make_req(
                Method::PUT,
                format!("/t/{download_id}?offset=0"),
                Bytes::from_static(b"corrupt"),
                Version::HTTP_2,
            ))
            .await;
        assert_eq!(
            put_existing_refused.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            std::fs::read(&existing_download).unwrap(),
            download_payload,
            "refused upload must leave the existing file untouched"
        );
        assert_eq!(crate::lease::process_pool().used(), 0);

        let get_refused = h2_client
            .send(make_req(
                Method::GET,
                format!("/t/{download_id}"),
                Bytes::new(),
                Version::HTTP_2,
            ))
            .await;
        assert_eq!(get_refused.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(crate::lease::process_pool().used(), 0);

        drop(full_hold);
        assert_eq!(pool.used(), 0);

        // (a2) Tight pool where `presence` succeeds (`upload_native` bytes fit), then
        // `create_target_parents_in` exceeds remaining capacity before touching disk.
        let upload_native = crate::files::path::native_path_bytes(&nested_upload).unwrap();
        let tight_parent_hold = pool.try_acquire(pool.limit() - upload_native).unwrap();
        let put_parent_refused = h2_client
            .send(make_req(
                Method::PUT,
                format!("/t/{upload_id}?offset=0"),
                Bytes::from_static(b"partial"),
                Version::HTTP_2,
            ))
            .await;
        assert_eq!(
            put_parent_refused.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert!(
            !dir.join("nested").exists(),
            "parent creation must abort before mkdir when isolated pool lacks target workspace"
        );
        assert!(!nested_upload.exists());
        assert_eq!(crate::lease::process_pool().used(), 0);
        drop(tight_parent_hold);
        assert_eq!(pool.used(), 0);

        // (a3) Tight pool where `open_regular` fits (`download_native` bytes), then `FileBody`
        // chunk copy (`8192` bytes) exceeds remaining capacity and aborts the stream.
        let download_native = crate::files::path::native_path_bytes(&existing_download).unwrap();
        assert!(download_native < download_payload.len());
        let tight_chunk_hold = pool.try_acquire(pool.limit() - download_native).unwrap();
        let direct_tight = http_wire::serve_transfer(
            Request::get(format!("/t/{download_id}"))
                .body(Full::new(Bytes::new()))
                .unwrap(),
            Arc::clone(&table),
            Arc::clone(&pool),
            stopped.clone(),
            None,
        )
        .await;
        assert_eq!(direct_tight.status(), StatusCode::OK);
        assert_eq!(
            direct_tight
                .into_body()
                .frame()
                .await
                .unwrap()
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::OutOfMemory
        );
        assert_eq!(crate::lease::process_pool().used(), 0);

        let chunk_failed = match h2_client
            .try_send(make_req(
                Method::GET,
                format!("/t/{download_id}"),
                Bytes::new(),
                Version::HTTP_2,
            ))
            .await
        {
            Err(_) => true,
            Ok(res) => res.into_body().collect().await.is_err(),
        };
        assert!(
            chunk_failed,
            "streaming download body must fail when chunk lease exceeds isolated pool"
        );
        assert_eq!(crate::lease::process_pool().used(), 0);
        drop(tight_chunk_hold);
        tokio::time::timeout(Duration::from_secs(5), async {
            while pool.used() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        // (c) Ample isolated pool capacity: multi-level parent creation + upload + resume + HEAD
        // + GET succeed with zero global pool usage and `pool.used() == 0` once settled.
        let (mut h1_client, h1_conn) = TestHttpSender::connect(bound, false).await;

        let init_put = h1_client
            .send(make_req(
                Method::PUT,
                format!("/t/{upload_id}?offset=0"),
                Bytes::new(),
                Version::HTTP_11,
            ))
            .await;
        assert_eq!(init_put.status(), StatusCode::OK);
        assert_eq!(
            init_put.into_body().collect().await.unwrap().to_bytes(),
            Bytes::from_static(b"0")
        );
        assert!(nested_upload.is_file());
        assert_eq!(crate::lease::process_pool().used(), 0);

        let part1 = h1_client
            .send(make_req(
                Method::PUT,
                format!("/t/{upload_id}?offset=0"),
                Bytes::from_static(b"hello "),
                Version::HTTP_11,
            ))
            .await;
        assert_eq!(part1.status(), StatusCode::OK);
        assert_eq!(
            part1.into_body().collect().await.unwrap().to_bytes(),
            Bytes::from_static(b"6")
        );
        assert_eq!(crate::lease::process_pool().used(), 0);

        let part2 = h1_client
            .send(make_req(
                Method::PUT,
                format!("/t/{upload_id}?offset=6"),
                Bytes::from_static(b"isolated pool"),
                Version::HTTP_11,
            ))
            .await;
        assert_eq!(part2.status(), StatusCode::OK);
        assert_eq!(
            part2.into_body().collect().await.unwrap().to_bytes(),
            Bytes::from_static(b"19")
        );
        assert_eq!(crate::lease::process_pool().used(), 0);

        let head_ok = h1_client
            .send(make_req(
                Method::HEAD,
                format!("/t/{upload_id}"),
                Bytes::new(),
                Version::HTTP_11,
            ))
            .await;
        assert_eq!(head_ok.status(), StatusCode::OK);
        assert_eq!(head_ok.headers()[header::CONTENT_LENGTH], "19");
        head_ok.into_body().collect().await.unwrap();
        assert_eq!(crate::lease::process_pool().used(), 0);

        let get_uploaded = h1_client
            .send(make_req(
                Method::GET,
                format!("/t/{upload_id}"),
                Bytes::new(),
                Version::HTTP_11,
            ))
            .await;
        assert_eq!(get_uploaded.status(), StatusCode::OK);
        assert_eq!(
            &get_uploaded.into_body().collect().await.unwrap().to_bytes()[..],
            b"hello isolated pool"
        );
        assert_eq!(crate::lease::process_pool().used(), 0);

        let get_existing = h1_client
            .send(make_req(
                Method::GET,
                format!("/t/{download_id}"),
                Bytes::new(),
                Version::HTTP_11,
            ))
            .await;
        assert_eq!(get_existing.status(), StatusCode::OK);
        assert_eq!(
            &get_existing.into_body().collect().await.unwrap().to_bytes()[..],
            &download_payload[..]
        );
        assert_eq!(crate::lease::process_pool().used(), 0);

        // Direct in-flight check: while holding a streamed download frame, `pool.used()` reflects
        // the live chunk lease and `process_pool().used()` remains strictly zero.
        let direct_res = http_wire::serve_transfer(
            Request::get(format!("/t/{download_id}"))
                .body(Full::new(Bytes::new()))
                .unwrap(),
            Arc::clone(&table),
            Arc::clone(&pool),
            stopped.clone(),
            None,
        )
        .await;
        assert_eq!(direct_res.status(), StatusCode::OK);
        let mut direct_body = direct_res.into_body();
        let held_chunk = direct_body
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        assert_eq!(&held_chunk[..], &download_payload[..]);
        assert!(pool.used() >= download_payload.len());
        assert_eq!(crate::lease::process_pool().used(), 0);
        drop(held_chunk);
        drop(direct_body);

        drop((h1_client, h2_client));
        stopped.cancel();
        server_task.await.unwrap();
        let _ = h1_conn.await;
        let _ = h2_conn.await;

        assert_eq!(pool.used(), 0);
        assert_eq!(crate::lease::process_pool().used(), 0);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[tokio::test]
async fn http2_authority_mints_transfer_url_without_host_header_and_blocks_userinfo() {
    let dir = std::env::temp_dir().join(format!(
        "fastexec-h2-transfer-authority-{}",
        crate::id::random_chars(12).unwrap()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let sample = dir.join("payload.txt");
    std::fs::write(&sample, b"http2-authority-ok").unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound = listener.local_addr().unwrap();
    let stopped = CancellationToken::new();
    let token = "0123456789abcdef";
    let tasks = crate::tasks::Tasks::new(crate::tasks::Limits::OFF).unwrap();
    let service = Arc::new(crate::service::Service::new(tasks, true));
    let launch = Arc::new(LaunchContext::new(dir.clone(), Vec::new()));
    let table = Arc::new(Transfers::new());
    let server_task = tokio::spawn(serve(HttpServeArgs {
        listener,
        digests: vec![digest(token)],
        tools: Tools {
            codemode: false,
            transfer: true,
            yolo: false,
        },
        launch,
        public_root: None,
        extra_instructions: None,
        instructions_tool: InstructionsToolMode::Off,
        skills: None,
        service,
        transfers: Arc::clone(&table),
        memory: BytePool::new(1 << 20),
        control_memory: BytePool::new(256 * 1024),
        stopped: stopped.clone(),
    }));

    let (mut h2_client, h2_conn) = TestHttpSender::connect(bound, true).await;
    let body = modern_post_body(
        1,
        "tools/call",
        serde_json::json!({
            "name": "transfer",
            "arguments": { "path": sample.to_string_lossy() }
        }),
    );

    // RFC 9113 HTTP/2 request with `:authority` and `:scheme`, carrying no `Host` header.
    let ok_req = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{bound}/mcp"))
        .version(Version::HTTP_2)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header(header::CONTENT_TYPE, "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "tools/call")
        .header("mcp-name", "transfer")
        .body(Full::new(body.clone()))
        .unwrap();
    let ok_res = h2_client.send(ok_req).await;
    assert_eq!(ok_res.status(), StatusCode::OK);
    let ok_json: serde_json::Value =
        serde_json::from_slice(&ok_res.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_ne!(
        ok_json["result"]["isError"],
        serde_json::json!(true),
        "{ok_json}"
    );
    let text = ok_json["result"]["content"][0]["text"].as_str().unwrap();
    let url = text
        .split_whitespace()
        .find(|part| part.starts_with(&format!("http://{bound}/t/")))
        .expect("minted transfer URL");

    let dl_req = Request::builder()
        .method(Method::GET)
        .uri(url)
        .version(Version::HTTP_2)
        .body(Full::new(Bytes::new()))
        .unwrap();
    let dl_res = h2_client.send(dl_req).await;
    assert_eq!(dl_res.status(), StatusCode::OK);
    assert_eq!(
        &dl_res.into_body().collect().await.unwrap().to_bytes()[..],
        b"http2-authority-ok"
    );

    // Userinfo in `:authority` must never produce a credential-bearing URL.
    let evil_uri = http::Uri::builder()
        .scheme("http")
        .authority(format!("evil@{bound}"))
        .path_and_query("/mcp")
        .build()
        .unwrap();
    let evil_req = Request::builder()
        .method(Method::POST)
        .uri(evil_uri)
        .version(Version::HTTP_2)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header(header::CONTENT_TYPE, "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "tools/call")
        .header("mcp-name", "transfer")
        .body(Full::new(body))
        .unwrap();
    let evil_res = h2_client.send(evil_req).await;
    if evil_res.status() == StatusCode::OK {
        let evil_json: serde_json::Value =
            serde_json::from_slice(&evil_res.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(
            evil_json["result"]["isError"],
            serde_json::json!(true),
            "{evil_json}"
        );
        let err_text = evil_json["result"]["content"][0]["text"].as_str().unwrap();
        assert!(!err_text.contains("evil"), "{err_text}");
    } else {
        assert!(evil_res.status().is_client_error());
    }

    drop(h2_client);
    stopped.cancel();
    server_task.await.unwrap();
    let _ = h2_conn.await;
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn skills_http_response_lease_holds_daemon_memory_until_body_dropped() {
    let dir = std::env::temp_dir().join(format!(
        "fastexec-http-skills-lease-{}",
        crate::id::random_chars(8).unwrap()
    ));
    let skill_dir = dir.join("big");
    std::fs::create_dir_all(&skill_dir).unwrap();
    std::fs::write(
        skill_dir.join("SKILL.md"),
        b"---\nname: big\ndescription: Big skill\n---\n# Big\n",
    )
    .unwrap();
    let payload_size = 2 * 1024 * 1024;
    std::fs::write(skill_dir.join("data.txt"), vec![b'a'; payload_size]).unwrap();

    let process_before = crate::lease::process_pool().used();
    let daemon_memory = BytePool::new(8 << 20);
    let catalog = Arc::new(crate::skills::load(&skill_dir, &daemon_memory).unwrap());
    let baseline = daemon_memory.used();
    assert!(
        baseline > 0,
        "catalog metadata must be leased from daemon_memory"
    );
    assert_eq!(
        crate::lease::process_pool().used(),
        process_before,
        "skills::load must not charge global process_pool"
    );

    // Direct unit check for early-disconnect `closed == true` branch on HttpResponseLease.
    let closed_slot = HttpResponseLease::default();
    closed_slot.close();
    let probe = daemon_memory.try_acquire(4096).unwrap();
    assert_eq!(daemon_memory.used(), baseline + 4096);
    assert!(!closed_slot.attach(probe));
    assert_eq!(
        daemon_memory.used(),
        baseline,
        "attaching to a closed HttpResponseLease must immediately release outbound lease"
    );

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let bound = listener.local_addr().unwrap();
    let stopped = CancellationToken::new();
    let token = "0123456789abcdef";
    let tasks = crate::tasks::Tasks::new(crate::tasks::Limits::OFF).unwrap();
    let service = Arc::new(crate::service::Service::new(tasks, true));
    let launch = Arc::new(LaunchContext::new(dir.clone(), Vec::new()));
    let table = Arc::new(Transfers::new());
    let server_task = tokio::spawn(serve(HttpServeArgs {
        listener,
        digests: vec![digest(token)],
        tools: Tools::default(),
        launch,
        public_root: None,
        extra_instructions: None,
        instructions_tool: InstructionsToolMode::Off,
        skills: Some(catalog),
        service,
        transfers: table,
        memory: Arc::clone(&daemon_memory),
        control_memory: BytePool::new(256 * 1024),
        stopped: stopped.clone(),
    }));

    // HTTP/2 has a 64 KiB initial stream window, so a 256 KiB response stays suspended in
    // LeaseBody until the client reads or disconnects.
    let (mut h2_client, h2_conn) = TestHttpSender::connect(bound, true).await;
    let body = modern_post_body(
        1,
        "resources/read",
        serde_json::json!({"uri": "skill://big/data.txt"}),
    );
    let req = Request::builder()
        .method(Method::POST)
        .uri(format!("http://{bound}/mcp"))
        .version(Version::HTTP_2)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::ACCEPT, "application/json, text/event-stream")
        .header(header::CONTENT_TYPE, "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "resources/read")
        .header("mcp-name", "skill://big/data.txt")
        .body(Full::new(body))
        .unwrap();
    let res = h2_client.send(req).await;
    assert_eq!(res.status(), StatusCode::OK);

    // While the response body is unread (flow-control blocked), daemon_memory holds the lease.
    let held = daemon_memory.used();
    assert!(
        held >= baseline + payload_size,
        "expected LeaseBody to hold >= {} bytes, got {held} (baseline {baseline})",
        baseline + payload_size
    );

    // Drop the response and client early (simulating client disconnect) -> lease returns to baseline.
    drop(res);
    drop(h2_client);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while daemon_memory.used() != baseline && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        daemon_memory.used(),
        baseline,
        "LeaseBody::drop must release outbound lease back to daemon_memory"
    );

    stopped.cancel();
    server_task.await.unwrap();
    let _ = h2_conn.await;
    std::fs::remove_dir_all(dir).unwrap();
}
