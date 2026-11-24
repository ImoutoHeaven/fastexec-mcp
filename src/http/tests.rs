use super::*;

async fn download_fixture(
    content: &[u8],
    memory: Arc<crate::lease::BytePool>,
) -> (PathBuf, Arc<Transfers>, FileBody) {
    let directory = std::env::temp_dir().join(format!(
        "fastexec-download-budget-{}",
        crate::tasks::random_chars(12).unwrap()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("ordinary.txt");
    std::fs::write(&path, content).unwrap();
    let table = Arc::new(Transfers::new());
    let id = table.create(path.clone()).unwrap();
    let file = tokio::fs::File::open(&path).await.unwrap();
    let body = FileBody {
        file: Some(file.take(content.len() as u64)),
        memory,
        _admitted: table.admit(&id).unwrap(),
    };
    (directory, table, body)
}

#[tokio::test]
async fn download_chunk_admission_ends_a_refused_body() {
    const CONTENT: &[u8] = b"ordinary";
    for cap in [CONTENT.len() - 1, CONTENT.len(), CONTENT.len() + 1] {
        let memory = crate::lease::BytePool::new(cap);
        let (directory, _table, mut body) = download_fixture(CONTENT, Arc::clone(&memory)).await;
        let frame = body.frame().await.unwrap();
        if cap < CONTENT.len() {
            assert_eq!(frame.unwrap_err().kind(), std::io::ErrorKind::OutOfMemory);
            assert!(hyper::body::Body::is_end_stream(&body));
            assert!(body.frame().await.is_none());
        } else {
            let bytes = frame.unwrap().into_data().unwrap();
            assert_eq!(&bytes[..], CONTENT);
            assert_eq!(memory.used(), CONTENT.len());
            drop(bytes);
        }
        drop(body);
        assert_eq!(memory.used(), 0);
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[tokio::test]
async fn download_frame_slices_keep_their_lease_after_body_drop() {
    const CONTENT: &[u8] = b"ordinary";
    let memory = crate::lease::BytePool::new(CONTENT.len());
    let (directory, table, mut body) = download_fixture(CONTENT, Arc::clone(&memory)).await;
    let bytes = body.frame().await.unwrap().unwrap().into_data().unwrap();
    let retained = bytes.slice(1..);
    assert_eq!(memory.used(), CONTENT.len());
    drop(bytes);
    drop(body);
    assert!(table.is_idle(std::time::Instant::now() + transfer::IDLE));
    assert_eq!(memory.used(), CONTENT.len());
    assert_eq!(&retained[..], &CONTENT[1..]);
    drop(retained);
    assert_eq!(memory.used(), 0);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn native_file_worker_admits_before_io_and_releases_success_and_error() {
    let directory = std::env::temp_dir().join(format!(
        "fastexec-native-transfer-{}",
        crate::tasks::random_chars(12).unwrap()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = Arc::new(directory.join("ordinary.txt"));
    let required = crate::files::path::native_path_bytes(&path).unwrap();
    for cap in [required - 1, required, required + 1] {
        let pool = crate::lease::BytePool::new(cap);
        let operation_pool = Arc::clone(&pool);
        let result = transfer_file(Arc::clone(&path), Arc::clone(&pool), move |path| {
            assert_eq!(operation_pool.used(), required);
            std::fs::File::create(path)
        })
        .await;
        if cap < required {
            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::OutOfMemory);
            assert!(!path.exists(), "refusal must precede the native file call");
        } else {
            drop(result.unwrap());
            assert!(path.exists());
            std::fs::remove_file(path.as_path()).unwrap();
        }
        assert_eq!(pool.used(), 0);
    }
    let pool = crate::lease::BytePool::new(required);
    let operation_pool = Arc::clone(&pool);
    let error = transfer_file(path, Arc::clone(&pool), move |path| {
        assert_eq!(operation_pool.used(), required);
        std::fs::metadata(path)
    })
    .await
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    assert_eq!(pool.used(), 0);
    std::fs::remove_dir(directory).unwrap();
}

#[tokio::test]
async fn native_file_worker_keeps_its_lease_after_waiter_cancellation() {
    let path = Arc::new(std::env::temp_dir());
    let required = crate::files::path::native_path_bytes(&path).unwrap();
    let pool = crate::lease::BytePool::new(required);
    let worker_pool = Arc::clone(&pool);
    let operation_pool = Arc::clone(&pool);
    let (ready, observed) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let waiter = tokio::spawn(async move {
        transfer_file(path, worker_pool, move |path| {
            assert_eq!(operation_pool.used(), required);
            ready.send(()).unwrap();
            blocked.recv_timeout(Duration::from_secs(5)).unwrap();
            assert_eq!(operation_pool.used(), required);
            std::fs::metadata(path)
        })
        .await
    });
    observed.await.unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(pool.used(), required);
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.used() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[test]
fn metadata_worker_keeps_the_shared_path_after_its_waiter_is_dropped() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let table = Arc::new(Transfers::new());
        let id = table
            .create(PathBuf::from(format!(
                "future-transfer-{}",
                crate::tasks::random_chars(12).unwrap()
            )))
            .unwrap();
        let admitted = table.admit(&id).unwrap();
        let shared = admitted.path();
        let weak = Arc::downgrade(&shared);
        let (ready, observed) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            ready.send(()).unwrap();
            blocked.recv_timeout(Duration::from_secs(5)).unwrap();
        });
        observed.await.unwrap();
        let mut pending = Box::pin(presence(shared));
        std::future::poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(pending);
        drop(admitted);
        assert!(table.is_idle(std::time::Instant::now() + transfer::IDLE));
        assert!(
            weak.upgrade().is_some(),
            "the queued worker still owns the path"
        );
        release.send(()).unwrap();
        blocker.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while weak.upgrade().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    });
}

// Windows may refuse replacing a destination with an open handle.
#[cfg(unix)]
#[tokio::test]
async fn downloads_size_the_same_open_handle_it_streams() {
    let directory = std::env::temp_dir().join(format!(
        "fastexec-transfer-replace-{}",
        crate::tasks::random_chars(12).unwrap()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("file.txt");
    let replacement = directory.join("replacement.txt");
    let table = Arc::new(Transfers::new());
    let id = table.create(path.clone()).unwrap();
    for partial in [false, true] {
        std::fs::write(&path, b"original").unwrap();
        std::fs::write(&replacement, b"new").unwrap();
        let mut headers = HeaderMap::new();
        if partial {
            headers.insert(header::RANGE, "bytes=3-".parse().unwrap());
        }
        let response = transfer_get(Version::HTTP_11, &headers, table.admit(&id).unwrap()).await;
        std::fs::rename(&replacement, &path).unwrap();
        let (status, length, body) = if partial {
            assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes 3-7/8");
            (StatusCode::PARTIAL_CONTENT, "5", &b"ginal"[..])
        } else {
            (StatusCode::OK, "8", &b"original"[..])
        };
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()[header::CONTENT_LENGTH], length);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            body
        );
    }
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test(start_paused = true)]
async fn idle_upload_flushes_accepted_prefix_and_releases_turn() {
    use hyper::body::Frame;
    struct Stalled(Option<Bytes>);
    impl hyper::body::Body for Stalled {
        type Data = Bytes;
        type Error = Infallible;
        fn poll_frame(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
            match self.0.take() {
                Some(bytes) => Poll::Ready(Some(Ok(Frame::data(bytes)))),
                None => Poll::Pending,
            }
        }
    }

    let directory = std::env::temp_dir().join(format!(
        "fastexec-transfer-idle-{}",
        crate::tasks::random_chars(12).unwrap()
    ));
    let path = directory.join("file.txt");
    let table = Arc::new(Transfers::new());
    let id = table.create(path.clone()).unwrap();
    let admitted = table.admit(&id).unwrap();
    let target = prepare_upload(&admitted, 0).await.unwrap();
    let input = Stalled(Some(Bytes::from_static(b"accepted")));
    let response = write_upload(
        Version::HTTP_11,
        admitted,
        target,
        0,
        input,
        CancellationToken::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    assert_eq!(std::fs::read(&path).unwrap(), b"accepted");
    let admitted = table.admit(&id).unwrap();
    let resume = prepare_upload(&admitted, 8).await.unwrap();
    let response = write_upload(
        Version::HTTP_11,
        admitted,
        resume,
        8,
        Full::new(Bytes::from_static(b"!")),
        CancellationToken::new(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(std::fs::read(&path).unwrap(), b"accepted!");
    std::fs::remove_dir_all(directory).unwrap();
}

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

/// Every value that names an address clients reach, normalized to what a URL is minted from.
#[test]
fn a_public_root_keeps_what_it_states_and_gains_one_trailing_slash() {
    let valid = [
        ("http://h", "http://h/"),
        ("http://h/", "http://h/"),
        ("https://h.example/mac", "https://h.example/mac/"),
        ("https://h.example/mac/", "https://h.example/mac/"),
        ("HTTP://Host.Example/Mac", "http://Host.Example/Mac/"),
        (
            "https://HtTpS.example:8443/mac/",
            "https://HtTpS.example:8443/mac/",
        ),
        ("http://127.0.0.1:8765/mac/", "http://127.0.0.1:8765/mac/"),
        ("https://h:65535/", "https://h:65535/"),
        // A bracketed IPv6 host is an authority like any other, with or without a port.
        ("http://[::1]", "http://[::1]/"),
        ("http://[::1]:80/", "http://[::1]:80/"),
        ("http://[::1]:8080/mac", "http://[::1]:8080/mac/"),
        (
            "https://[2001:db8:0:0:0:0:0:1]:443/files",
            "https://[2001:db8:0:0:0:0:0:1]:443/files/",
        ),
        ("http://[::ffff:127.0.0.1]/", "http://[::ffff:127.0.0.1]/"),
        // Encoded bytes are what a proxy rewrites to, so they are kept as stated.
        ("https://h/a%20b/", "https://h/a%20b/"),
        ("https://h/%2E%2E/%2e/", "https://h/%2E%2E/%2e/"),
    ];
    for (value, expected) in valid {
        assert_eq!(valid_root(value).as_deref(), Ok(expected), "{value:?}");
    }
}

/// Each way a value cannot name an address, with the reason it is refused for.
#[test]
fn a_public_root_that_cannot_be_used_is_refused_with_its_reason() {
    let refused = [
        ("", "must start with http:// or https://"),
        (
            "http://h/ mac/",
            "contains whitespace or a control character",
        ),
        (
            "http://h/a\u{7}",
            "contains whitespace or a control character",
        ),
        ("https://h/a?b=c", "carries a query or a fragment"),
        ("https://h/a#b", "carries a query or a fragment"),
        ("ftp://h/mac/", "must start with http:// or https://"),
        ("h/mac/", "must start with http:// or https://"),
        ("http:///mac/", "names no host"),
        ("http://user@h/mac/", "carries userinfo"),
        ("http://user:secret@h/mac/", "carries userinfo"),
        ("http://h:/", "has an empty port"),
        ("http://h:/mac/", "has an empty port"),
        ("http://h:0/", "has a port outside 1-65535"),
        ("http://h:65536/", "has a port outside 1-65535"),
        ("http://h:abc/", "has a port that is not a decimal number"),
        ("http://h:8a80/", "has a port that is not a decimal number"),
        ("http://h:-1/", "has a port that is not a decimal number"),
        ("http://[::1]:/", "has an empty port"),
        ("http://[::1]:70000/", "has a port outside 1-65535"),
        // `http::Uri` reads a bracketed authority loosely, so these are judged on the raw text.
        ("http://[]/", "names no host"),
        ("http://[]:80/mac/", "names no host"),
        (
            "http://[localhost]/",
            "has a bracketed host that is not an IPv6 address",
        ),
        (
            "http://[127.0.0.1]:80/mac/",
            "has a bracketed host that is not an IPv6 address",
        ),
        (
            "http://[:::1]/",
            "has a bracketed host that is not an IPv6 address",
        ),
        (
            "http://[::1::]/",
            "has a bracketed host that is not an IPv6 address",
        ),
        ("http://[::1]junk/mac/", "has text after a bracketed host"),
        (
            "http://[::1]junk:abc/mac/",
            "has text after a bracketed host",
        ),
        ("http://[::1:8080/mac/", "has an unclosed bracketed host"),
        ("http://h/./mac/", "has a dot path segment"),
        ("http://h/../mac/", "has a dot path segment"),
        ("http://h/mac/.", "has a dot path segment"),
        ("http://h//mac/", "has an empty path segment"),
        ("http://h/mac//deep/", "has an empty path segment"),
    ];
    for (value, reason) in refused {
        let message = valid_root(value).expect_err(value);
        // The reason and the variable, and never the value: a mistyped value can carry userinfo
        // and this message reaches a terminal and a daemon log.
        assert_eq!(
            message,
            format!("FASTEXEC_PUBLIC_URL {reason}"),
            "{value:?}"
        );
    }
}

/// The variable is unset or it is a value; a value that is not UTF-8 cannot be a URL.
#[test]
fn an_unset_public_root_is_no_root_and_an_unreadable_one_is_refused() {
    assert_eq!(root_of(None), Ok(None));
    assert_eq!(
        root_of(Some(std::ffi::OsStr::new("https://h/mac"))),
        Ok(Some("https://h/mac/".to_owned()))
    );
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let message = root_of(Some(std::ffi::OsStr::from_bytes(b"https://h/\xff")))
            .expect_err("a value that is not UTF-8");
        assert!(
            message.starts_with("FASTEXEC_PUBLIC_URL ") && message.contains("UTF-8"),
            "{message}"
        );
    }
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
) -> StreamableHttpService<PocEventsHandler, LocalSessionManager> {
    let mut config = StreamableHttpServerConfig::default();
    config.legacy_session_mode = false;
    config.stateless_protocol_metadata_required = true;
    config.json_response = true;
    config.sse_keep_alive = Some(Duration::from_secs(15));
    config.allowed_hosts = allowed_hosts(bound, false);
    let sessions = Arc::new(LocalSessionManager::default());
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
        stopped: stopped.clone(),
        inner: poc_service(gate, bound),
    };
    let server_task = tokio::spawn(serve_connections(listener, layer, stopped.clone()));

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
        refusal(Version::HTTP_11, StatusCode::UNAUTHORIZED, "Bearer")
            .headers()
            .get(header::CONNECTION)
            .and_then(|v| v.to_str().ok()),
        Some("close")
    );
    assert_eq!(
        transfer_empty(Version::HTTP_11, StatusCode::NOT_FOUND, &[])
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
    StreamableHttpService<Server, LocalSessionManager>,
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
    let render = Render {
        output_mode: crate::service::render::OutputMode::Text,
        structured_content: true,
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
            render,
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
    config.legacy_session_mode = false;
    config.stateless_protocol_metadata_required = true;
    config.json_response = true;
    config.sse_keep_alive = Some(Duration::from_secs(15));
    config.allowed_hosts = allowed_hosts(bound, false);
    let sessions = Arc::new(LocalSessionManager::default());
    (
        StreamableHttpService::new(factory, sessions, config),
        service,
    )
}

#[tokio::test]
async fn current_http_supports_events_and_plain_tool_calls() {
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
            upstream_event_id: "u-1".to_owned(),
            data: serde_json::json!({ "secret": "TOP_SECRET_DATA" }),
            upstream_truncated: None,
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

    for method in ["events/poll", "events/stream"] {
        for name in [None, Some("task.exited")] {
            let mut request = Request::post("/mcp")
                .header("host", "localhost")
                .header("accept", "application/json, text/event-stream")
                .header("content-type", "application/json")
                .header("mcp-protocol-version", "2026-07-28")
                .header("mcp-method", method);
            if let Some(name) = name {
                request = request.header("mcp-name", name);
            }
            let mut request = request
                .body(Full::new(modern_post_body(
                    4,
                    method,
                    serde_json::json!({ "name": "external.event" }),
                )))
                .unwrap();
            request.extensions_mut().insert(Principal(0));
            let response = http_svc.call(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let body: serde_json::Value =
                serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            assert_eq!(body["error"]["code"], -32020, "{body}");
        }
    }

    // 1e. Unknown event -> HTTP 200 + -32023; events/subscribe -> HTTP 404 + -32601.
    let mut unknown_req = Request::post("/mcp")
        .header("host", "localhost")
        .header("accept", "application/json, text/event-stream")
        .header("content-type", "application/json")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "events/poll")
        .header("mcp-name", "no.such.event")
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
}

#[tokio::test]
async fn http_requires_current_request_metadata() {
    use tower_service::Service as _;

    let (mut service, _) = real_http_service(SocketAddr::from(([127, 0, 0, 1], 0)));
    for (version, missing, expected) in [
        (
            "2026-07-28",
            Some("io.modelcontextprotocol/protocolVersion"),
            rmcp::model::ErrorCode::INVALID_PARAMS,
        ),
        (
            "2026-07-28",
            Some("io.modelcontextprotocol/clientCapabilities"),
            rmcp::model::ErrorCode::INVALID_PARAMS,
        ),
        (
            "2025-11-25",
            None,
            rmcp::model::ErrorCode::UNSUPPORTED_PROTOCOL_VERSION,
        ),
    ] {
        let mut body: serde_json::Value =
            serde_json::from_slice(&modern_post_body(1, "tools/list", serde_json::json!({})))
                .unwrap();
        let metadata = body["params"]["_meta"].as_object_mut().unwrap();
        metadata.insert(
            "io.modelcontextprotocol/protocolVersion".to_owned(),
            serde_json::json!(version),
        );
        if let Some(missing) = missing {
            metadata.remove(missing);
        }
        let mut request = Request::post("/mcp")
            .header("host", "localhost")
            .header("accept", "application/json, text/event-stream")
            .header("content-type", "application/json")
            .header("mcp-protocol-version", version)
            .header("mcp-method", "tools/list")
            .body(Full::new(Bytes::from(body.to_string())))
            .unwrap();
        request.extensions_mut().insert(Principal(0));
        let response = service.call(request).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "{version} {missing:?}"
        );
        let answer: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(answer["error"]["code"], expected.0, "{answer}");
    }

    for include_header in [false, true] {
        let mut request = Request::post("/mcp")
            .header("host", "localhost")
            .header("accept", "application/json, text/event-stream")
            .header("content-type", "application/json")
            .header("mcp-method", "tools/list");
        if include_header {
            request = request.header("mcp-protocol-version", "2026-07-28");
        }
        let mut request = request
            .body(Full::new(modern_post_body(
                2,
                "tools/list",
                serde_json::json!({}),
            )))
            .unwrap();
        request.extensions_mut().insert(Principal(0));
        let response = service.call(request).await.unwrap();
        assert_eq!(
            response.status(),
            if include_header {
                StatusCode::OK
            } else {
                StatusCode::BAD_REQUEST
            },
        );
    }
}
