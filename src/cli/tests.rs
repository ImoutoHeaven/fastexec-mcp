//! Unit tests for CLI daemon flags, deadlines, event budgets, and stdio transport.

use super::daemon::{
    BoundedLine, DaemonFlags, NO_ANSWER, blocking_within, connect_emit_daemon, daemon_flags,
    parse_emit_payload, read_bounded_line, within,
};
use super::stdio::{StdioTransport, encode_stdio_frame};
use crate::config::{InstructionsToolMode, Tools};
use crate::env::LaunchContext;
use crate::events;
use crate::lease;
use crate::mcp::{Backend, RequestLeases, Server, decode_mcp_header_value};
use crate::render;
use crate::render::types::TrackedResult;
use crate::service::Service;
use crate::tasks::{Limits, Tasks};
use rmcp::model::{CallToolResult, ContentBlock};
use rmcp::service::RequestContext;
use std::sync::{Arc, OnceLock};
use tokio_util::sync::CancellationToken;

#[cfg(test)]
mod event_budget_tests {
    use super::*;

    #[tokio::test]
    async fn subscription_metadata_refusal_reports_resource_exhausted() {
        for control_limit in [0, 512] {
            let pool = lease::BytePool::new(0);
            let control = lease::BytePool::new(control_limit);
            let launch = Arc::new(
                LaunchContext::new(std::env::temp_dir(), Vec::new())
                    .with_memory(Arc::clone(&pool))
                    .with_control_memory(Arc::clone(&control)),
            );
            let server = Server::new(
                Backend::Local {
                    service: Arc::new(Service::new(Tasks::new(Limits::OFF).unwrap(), false)),
                    session: Arc::new(OnceLock::new()),
                    launch: Arc::clone(&launch),
                    daemon: false,
                },
                &launch,
                None,
                Tools::default(),
                None,
                InstructionsToolMode::Off,
            );
            let (_input, read) = tokio::io::duplex(64);
            let mut peer = rmcp::service::serve_directly(
                server.clone(),
                StdioTransport::new(read, tokio::io::sink(), Arc::new(RequestLeases::default())),
                None,
            );
            let context =
                RequestContext::new(rmcp::model::RequestId::Number(1), peer.peer().clone());
            let error = server
                .handle_events_stream(Some(serde_json::json!({"name": "external.event"})), context)
                .await
                .unwrap_err();
            assert_eq!(error.code.0, -32025);
            assert_eq!(
                error.data,
                (control_limit != 0)
                    .then(|| serde_json::json!({"limit": "process_memory", "max": pool.limit()}))
            );
            assert_eq!(pool.used(), 0);
            assert_eq!(control.used(), 0);
            peer.close().await.unwrap();
        }
    }
}

#[cfg(test)]
mod daemon_flag_tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<DaemonFlags, String> {
        daemon_flags(
            "start",
            &args
                .iter()
                .map(std::ffi::OsString::from)
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn idle_exit_is_opt_in_and_combines_with_http_in_either_order() {
        assert!(!parse(&[]).unwrap().idle_exit.unwrap_or_default());
        assert_eq!(
            parse(&["--idle-exit", "off"]).unwrap().idle_exit,
            Some(false)
        );
        assert_eq!(parse(&["--idle-exit", "on"]).unwrap().idle_exit, Some(true));
        for args in [
            ["--http", "127.0.0.1:0", "--idle-exit", "on"],
            ["--idle-exit", "on", "--http", "127.0.0.1:0"],
        ] {
            let flags = parse(&args).unwrap();
            assert_eq!(flags.http.as_deref(), Some("127.0.0.1:0"));
            assert_eq!(flags.idle_exit, Some(true));
        }
    }

    #[test]
    fn malformed_or_repeated_daemon_flags_are_rejected() {
        for args in [
            vec!["--idle-exit"],
            vec!["--idle-exit", "true"],
            vec!["--idle-exit", "on", "--idle-exit", "off"],
            vec!["--http", "127.0.0.1:0", "--http", "127.0.0.1:1"],
            vec!["--unknown", "off"],
        ] {
            assert!(parse(&args).is_err(), "accepted {args:?}");
        }
    }
}

#[cfg(test)]
mod cli_deadline_tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn pending_probe_uses_remaining_command_deadline() {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(10);
        assert_eq!(
            within::<()>(deadline, std::future::pending())
                .await
                .unwrap_err(),
            NO_ANSWER
        );
    }

    #[test]
    fn slow_preflight_does_not_hold_runtime_shutdown() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let (release, wait) = std::sync::mpsc::channel();
        let (started, start) = std::sync::mpsc::channel();
        let (finished, done) = std::sync::mpsc::channel();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3600);
        let task = runtime.spawn(blocking_within(deadline, move || {
            started.send(()).unwrap();
            // A regression must fail instead of hanging runtime shutdown forever.
            wait.recv_timeout(std::time::Duration::from_secs(30))
                .unwrap();
            finished.send(()).unwrap();
            Ok(())
        }));
        runtime.block_on(tokio::task::yield_now());
        start
            .recv_timeout(std::time::Duration::from_secs(30))
            .unwrap();
        let result = runtime.block_on(async {
            tokio::time::pause();
            tokio::time::advance(std::time::Duration::from_secs(7200)).await;
            task.await.unwrap()
        });
        assert_eq!(result.unwrap_err(), NO_ANSWER);
        drop(runtime);
        release.send(()).unwrap();
        done.recv_timeout(std::time::Duration::from_secs(30))
            .unwrap();
    }
}

#[cfg(test)]
mod stdio_transport_tests {
    use super::*;
    use rmcp::transport::Transport;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn stdio_transport_isolates_progress_frees_cancelled_requests_and_shrinks_line_buf() {
        let pool = lease::BytePool::new(2000);
        let leases = Arc::new(RequestLeases::default());
        let (mut client_write, server_read) = tokio::io::duplex(256 * 1024);
        let (server_write, mut client_read) = tokio::io::duplex(64 * 1024);
        let mut transport = StdioTransport::new(server_read, server_write, Arc::clone(&leases));

        // 1. Oversized (> 64 KiB) incoming request line shrinks `line_buf` capacity back to 0.
        let big_arg = "A".repeat(80 * 1024);
        let req1_line = format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\",\"params\":{{\"name\":\"write\",\"arguments\":{{\"path\":\"a\",\"content\":\"{big_arg}\"}}}}}}\n"
        );
        client_write.write_all(req1_line.as_bytes()).await.unwrap();
        let msg1 = transport.receive().await.expect("request 1");
        assert!(matches!(msg1, rmcp::model::JsonRpcMessage::Request(_)));
        assert_eq!(
            transport.line_buf.capacity(),
            0,
            "line_buf over 64 KiB must drop its backing capacity after parsing"
        );

        // Stage 700 bytes for request 1 and 800 bytes for request 2.
        let cancel1 = CancellationToken::new();
        let mut h1 = pool.try_acquire(700).unwrap();
        let out1 = h1.split_off(400);
        leases.insert(rmcp::model::RequestId::Number(1), &cancel1, h1, out1);

        let cancel2 = CancellationToken::new();
        let mut h2 = pool.try_acquire(800).unwrap();
        let out2 = h2.split_off(500);
        leases.insert(rmcp::model::RequestId::Number(2), &cancel2, h2, out2);
        assert_eq!(pool.used(), 1500);

        // 2. Unqueued progress notifications and other responses must NOT touch request 1 or 2's leases.
        let progress = rmcp::model::ServerJsonRpcMessage::notification(
            rmcp::model::ServerNotification::ProgressNotification(
                rmcp::model::ProgressNotification::new(
                    rmcp::model::ProgressNotificationParam::new(
                        rmcp::model::ProgressToken(rmcp::model::NumberOrString::Number(9)),
                        1.0,
                    )
                    .with_message("still running"),
                ),
            ),
        );
        transport.send(progress).await.unwrap();
        assert_eq!(
            pool.used(),
            1500,
            "progress notification must not consume staged tool response leases"
        );

        // A completed response remains charged and deliverable after a late cancellation.
        client_write.write_all(concat!(
            "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":1}}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":11,\"method\":\"ping\"}\n",
        ).as_bytes()).await.unwrap();
        let next = transport.receive().await.unwrap();
        assert!(matches!(next, rmcp::model::JsonRpcMessage::Request(_)));
        assert_eq!(pool.used(), 1500);
        transport
            .send(rmcp::model::ServerJsonRpcMessage::response(
                rmcp::model::ServerResult::CallToolResult(CallToolResult::success(Vec::new())),
                rmcp::model::RequestId::Number(1),
            ))
            .await
            .unwrap();
        assert_eq!(pool.used(), 800);

        // Cancellation before a complete response is registered still reaches RMCP.
        client_write.write_all(
            b"{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":10}}\n"
        ).await.unwrap();
        let cancelled = transport.receive().await.unwrap();
        assert!(matches!(
            cancelled,
            rmcp::model::JsonRpcMessage::Notification(_)
        ));
        let late = pool.try_acquire(100).unwrap();
        leases.insert(
            rmcp::model::RequestId::Number(10),
            &cancel1,
            lease::Lease::empty(&pool),
            late,
        );
        assert_eq!(pool.used(), 800);

        // 4. Sending response for request 2 releases both staging and outbound leases once flushed.
        let resp2 = rmcp::model::ServerJsonRpcMessage::response(
            rmcp::model::ServerResult::CallToolResult(CallToolResult::success(vec![
                ContentBlock::text("done"),
            ])),
            rmcp::model::RequestId::Number(2),
        );
        transport.send(resp2).await.unwrap();
        assert_eq!(pool.used(), 0);

        let mut wire_out = vec![0u8; 4096];
        let n = client_read.read(&mut wire_out).await.unwrap();
        let text = std::str::from_utf8(&wire_out[..n]).unwrap();
        assert!(text.contains("\"notifications/progress\""), "{text}");
        assert!(text.contains("\"id\":2"), "{text}");

        // Closing rejects new work but retains queued leases until session owners end.
        let leftover = pool.try_acquire(300).unwrap();
        leases.insert(
            rmcp::model::RequestId::Number(3),
            &CancellationToken::new(),
            lease::Lease::empty(&pool),
            leftover,
        );
        assert_eq!(pool.used(), 300);
        transport.close().await.unwrap();
        assert_eq!(pool.used(), 300);
        drop(transport);
        assert_eq!(pool.used(), 300);
        drop(leases);
        assert_eq!(pool.used(), 0);
    }
    #[tokio::test]
    async fn stdio_send_claims_lease_before_first_poll() {
        use rmcp::transport::Transport;
        let pool = lease::BytePool::new(1 << 20);
        let leases = Arc::new(RequestLeases::default());
        let mut transport =
            StdioTransport::new(tokio::io::empty(), tokio::io::sink(), Arc::clone(&leases));
        let id = rmcp::model::RequestId::Number(7);
        let held = pool.try_acquire(128 << 10).unwrap();
        leases.insert(
            id.clone(),
            &CancellationToken::new(),
            lease::Lease::empty(&pool),
            held,
        );
        let result = CallToolResult::success(vec![ContentBlock::text("ordinary response")]);
        let sending = transport.send(rmcp::model::ServerJsonRpcMessage::response(
            rmcp::model::ServerResult::CallToolResult(result),
            id.clone(),
        ));
        leases.on_cancel(&id);
        assert_eq!(
            pool.used(),
            128 << 10,
            "the unpolled send still owns the response"
        );
        drop(sending);
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn completed_stdio_response_ignores_late_cancellation() {
        use rmcp::transport::Transport;
        use tokio::io::AsyncWriteExt;
        let pool = lease::BytePool::new(4096);
        let leases = Arc::new(RequestLeases::default());
        let (mut input, read) = tokio::io::duplex(4096);
        let mut transport = StdioTransport::new(read, tokio::io::sink(), Arc::clone(&leases));
        let id = rmcp::model::RequestId::Number(7);
        leases.insert(
            id.clone(),
            &CancellationToken::new(),
            lease::Lease::empty(&pool),
            pool.try_acquire(1000).unwrap(),
        );
        input.write_all(concat!(
            "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":{\"requestId\":7}}\n",
            "{\"jsonrpc\":\"2.0\",\"id\":8,\"method\":\"ping\"}\n"
        ).as_bytes()).await.unwrap();
        let next = transport.receive().await.unwrap();
        assert!(
            matches!(next, rmcp::model::JsonRpcMessage::Request(_)),
            "a completed result must stay queued instead of forwarding a late cancellation"
        );
        assert_eq!(pool.used(), 1000);
        drop(leases.take(&id));
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn stdio_handoff_keeps_prepaid_payload_without_second_admission() {
        for send in [false, true] {
            let result = CallToolResult::success(vec![ContentBlock::text("already admitted")]);
            let id = rmcp::model::RequestId::Number(7);
            let payload_bytes = render::result_payload_capacity(&result);
            let wire_bytes =
                lease::json_len(&result).unwrap() + lease::json_len(&id).unwrap() + 256;
            let pool = lease::BytePool::new(payload_bytes + wire_bytes);
            let launch = Arc::new(
                LaunchContext::new(std::env::current_dir().unwrap(), Vec::new())
                    .with_memory(Arc::clone(&pool))
                    .with_control_memory(lease::BytePool::new(0)),
            );
            let tasks = Tasks::new(Limits::OFF).unwrap();
            let leases = Arc::new(RequestLeases::default());
            let mut server = Server::new(
                Backend::Local {
                    service: Arc::new(Service::new(tasks, false)),
                    session: Arc::new(OnceLock::new()),
                    launch: Arc::clone(&launch),
                    daemon: false,
                },
                &launch,
                None,
                Tools::default(),
                None,
                InstructionsToolMode::Off,
            );
            server.stdio_leases = Some(Arc::clone(&leases));
            let (_input, read) = tokio::io::duplex(64);
            let mut peer = rmcp::service::serve_directly(
                server.clone(),
                StdioTransport::new(read, tokio::io::sink(), Arc::new(RequestLeases::default())),
                None,
            );
            let context = RequestContext::new(id.clone(), peer.peer().clone());
            let tracked = TrackedResult {
                result,
                overload: false,
                task_meta: None,
                lease: pool.try_acquire(wire_bytes),
                payload_owner: lease::Owner::new(pool.try_acquire(payload_bytes).unwrap()),
            };
            assert_eq!(pool.used(), pool.limit());
            let result = server.deliver_local(&context, tracked, &launch, false);
            assert_eq!(result.is_error, Some(false), "{result:?}");
            assert_eq!(
                result.content[0].as_text().unwrap().text,
                "already admitted"
            );
            assert_eq!(
                pool.used(),
                pool.limit(),
                "handoff transfers existing payment"
            );

            let mut transport =
                StdioTransport::new(tokio::io::empty(), tokio::io::sink(), Arc::clone(&leases));
            let sending = transport.send(rmcp::model::ServerJsonRpcMessage::response(
                rmcp::model::ServerResult::CallToolResult(result),
                id,
            ));
            assert_eq!(
                pool.used(),
                pool.limit(),
                "an unpolled send owns the payload"
            );
            if send {
                sending.await.unwrap();
            } else {
                drop(sending);
            }
            assert_eq!(pool.used(), 0);
            peer.close().await.unwrap();
        }
    }

    #[test]
    fn stdio_encoding_keeps_exact_capacity_charged() {
        let item = serde_json::json!({"text": "x".repeat(65_535)});
        let expected = lease::json_len(&item).unwrap() + 1;
        let pool = lease::BytePool::new(expected);
        let mut lease = lease::Lease::empty(&pool);
        let bytes = encode_stdio_frame(&item, Some(&mut lease)).unwrap();
        assert_eq!(bytes.len(), expected);
        assert_eq!(bytes.capacity(), lease.bytes());
        assert_eq!(bytes.last(), Some(&b'\n'));
        drop(bytes);
        drop(lease);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn daemon_emit_flags_and_payload_parsing() {
        let flags = daemon_flags(
            "emit",
            &[
                "--namespace".into(),
                "custom-ns".into(),
                "--source".into(),
                "github".into(),
                "--topic".into(),
                "pr".into(),
                "--stream".into(),
            ],
        )
        .unwrap();
        assert_eq!(
            flags.namespace.as_deref(),
            Some(std::ffi::OsStr::new("custom-ns"))
        );
        assert_eq!(flags.source.as_deref(), Some("github"));
        assert_eq!(flags.topic.as_deref(), Some("pr"));
        assert!(flags.stream);
        assert!(daemon_flags("emit", &["--http".into(), "127.0.0.1:0".into()]).is_err());

        let parsed = parse_emit_payload(
            r#"{"eventId":"evt-42","name":"pr.merged","timestamp":"2026-11-25T00:00:00Z","data":{"n":1},"cursor":"c-1","_meta":{"trace":"1"}}"#,
            Some("github"),
            Some("pr"),
        )
        .unwrap();
        assert_eq!(parsed.source, "github");
        assert_eq!(parsed.topic.as_deref(), Some("pr"));
        assert_eq!(parsed.name, "pr.merged");
        assert_eq!(parsed.event_id, "evt-42");
        assert_eq!(parsed.timestamp.as_deref(), Some("2026-11-25T00:00:00Z"));

        for rejected in [
            r#"{"upstreamEventId":"evt-42","name":"pr.merged","data":{"n":1}}"#,
            r#"{"eventId":"evt-42","name":"pr.merged","data":{"n":1},"truncated":true}"#,
            r#"{"eventId":"evt-42","name":"pr.merged","data":{"n":1},"upstreamTruncated":true}"#,
            r#"{"eventId":"evt-42","name":"pr.merged","data":{"n":1},"extra":1}"#,
            r#"{"eventId":"evt-42","name":"pr.merged"}"#,
            r#"{"eventId":"evt-42","name":"pr.merged","timestamp":"","data":{}}"#,
            r#"{"eventType":"task.exited","eventId":"evt-42","name":"pr.merged","data":{}}"#,
        ] {
            assert!(
                parse_emit_payload(rejected, Some("github"), None).is_err(),
                "unexpectedly accepted {rejected}"
            );
        }

        // Rejects oversized payloads (> 64 KiB).
        let huge = format!(
            r#"{{"source":"s","name":"n","eventId":"1","data":"{}"}}"#,
            "x".repeat(events::MAX_EXTERNAL_EVENT_BYTES)
        );
        assert!(parse_emit_payload(&huge, None, None).is_err());
    }

    #[test]
    fn mcp_name_header_decoding_handles_plain_and_base64() {
        use base64::Engine as _;
        assert_eq!(
            decode_mcp_header_value("external.event").as_deref(),
            Some("external.event")
        );
        let encoded = format!(
            "=?base64?{}?=",
            base64::engine::general_purpose::STANDARD.encode("external.event")
        );
        assert_eq!(
            decode_mcp_header_value(&encoded).as_deref(),
            Some("external.event")
        );
        for malformed in [
            "",
            " external.event",
            "external.event ",
            "external\tevent",
            "external\x7fevent",
            "=?base64?!!!invalid!!!?=",
            "=?base64?unclosed",
        ] {
            assert!(
                decode_mcp_header_value(malformed).is_none(),
                "unexpectedly accepted {malformed:?}"
            );
        }
    }

    #[tokio::test]
    async fn read_bounded_line_skips_oversized_line_and_recovers_next() {
        let mut input = Vec::new();
        input.extend_from_slice(b"ok1\r\n");
        input.extend_from_slice(&[b'x'; 128]);
        input.push(b'\n');
        input.extend_from_slice(b"ok2\n");
        input.extend_from_slice(&[b'y'; 64]);
        let mut reader = tokio::io::BufReader::with_capacity(16, &input[..]);
        let mut buf = Vec::new();

        assert_eq!(
            read_bounded_line(&mut reader, &mut buf, 16).await.unwrap(),
            BoundedLine::Line(b"ok1\r\n".to_vec())
        );
        assert_eq!(
            read_bounded_line(&mut reader, &mut buf, 16).await.unwrap(),
            BoundedLine::TooLong
        );
        assert_eq!(
            read_bounded_line(&mut reader, &mut buf, 16).await.unwrap(),
            BoundedLine::Line(b"ok2\n".to_vec())
        );
        assert_eq!(
            read_bounded_line(&mut reader, &mut buf, 16).await.unwrap(),
            BoundedLine::TooLong
        );
        assert_eq!(
            read_bounded_line(&mut reader, &mut buf, 16).await.unwrap(),
            BoundedLine::Eof
        );
    }

    #[tokio::test]
    async fn stdio_transport_drops_pre_init_and_custom_notifications_and_preserves_error_ids() {
        use tokio::io::AsyncBufReadExt;

        let leases = Arc::new(RequestLeases::default());
        let (mut client_write, server_read) = tokio::io::duplex(64 * 1024);
        let (server_write, client_read) = tokio::io::duplex(64 * 1024);
        let mut transport = StdioTransport::new(server_read, server_write, Arc::clone(&leases));
        let mut out_lines = tokio::io::BufReader::new(client_read).lines();

        // 1. Pre-init notifications (standard, custom, and malformed without id) are dropped,
        // including after `server/discover` and `ping` before `initialize`.
        client_write
            .write_all(
                concat!(
                    "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n",
                    "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/custom\",\"params\":{\"x\":1}}\n",
                    "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":123}\n",
                    "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"server/discover\",\"params\":{}}\n",
                    "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/roots/list_changed\"}\n",
                    "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"ping\"}\n",
                    "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/custom\"}\n",
                )
                .as_bytes(),
            )
            .await
            .unwrap();

        let disc = transport.receive().await.expect("discover");
        assert!(matches!(
            disc,
            rmcp::model::JsonRpcMessage::Request(ref r)
                if matches!(r.request, rmcp::model::ClientRequest::DiscoverRequest(_))
        ));
        let ping = transport.receive().await.expect("ping");
        assert!(matches!(
            ping,
            rmcp::model::JsonRpcMessage::Request(ref r)
                if matches!(r.request, rmcp::model::ClientRequest::PingRequest(_))
        ));

        // 2. Malformed syntax -> -32700 (id: null); valid JSON object errors preserve string/integer id!
        client_write
            .write_all(
                concat!(
                    "{not valid json\n",
                    "{\"jsonrpc\":\"1.0\",\"id\":\"bad-ver\",\"method\":\"ping\"}\n",
                    "{\"jsonrpc\":\"2.0\",\"id\":42,\"method\":999}\n",
                    "{\"jsonrpc\":\"2.0\",\"id\":\"bad-params\",\"method\":\"tools/call\",\"params\":\"wrong\"}\n",
                    "{\"jsonrpc\":\"2.0\",\"id\":-7,\"method\":\"initialize\",\"params\":{\"bad\":1}}\n",
                    "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"clientInfo\":{\"name\":\"t\",\"version\":\"1\"}}}\n",
                    "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/custom\",\"params\":{\"ignored\":true}}\n",
                    "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}\n",
                )
                .as_bytes(),
            )
            .await
            .unwrap();

        let init = transport.receive().await.expect("initialize");
        assert!(matches!(
            init,
            rmcp::model::JsonRpcMessage::Request(ref r)
                if matches!(r.request, rmcp::model::ClientRequest::InitializeRequest(_))
        ));
        // CustomNotification after initialize is also dropped; standard InitializedNotification arrives.
        let inited = transport.receive().await.expect("initialized notification");
        assert!(matches!(
            inited,
            rmcp::model::JsonRpcMessage::Notification(ref n)
                if matches!(n.notification, rmcp::model::ClientNotification::InitializedNotification(_))
        ));

        let mut errs = Vec::new();
        for _ in 0..5 {
            let line = out_lines.next_line().await.unwrap().unwrap();
            errs.push(serde_json::from_str::<serde_json::Value>(&line).unwrap());
        }
        assert_eq!(errs[0]["error"]["code"], -32700);
        assert!(errs[0]["id"].is_null());
        assert_eq!(errs[1]["error"]["code"], -32600);
        assert_eq!(errs[1]["id"], "bad-ver");
        assert_eq!(errs[2]["error"]["code"], -32600);
        assert_eq!(errs[2]["id"], 42);
        assert_eq!(errs[3]["error"]["code"], -32602);
        assert_eq!(errs[3]["id"], "bad-params");
        assert_eq!(errs[4]["error"]["code"], -32602);
        assert_eq!(errs[4]["id"], -7);
    }

    #[tokio::test]
    async fn stdio_transport_survives_mid_line_overload_oversized_and_budget_errors() {
        use tokio::io::AsyncBufReadExt;

        let leases = Arc::new(RequestLeases::default());
        let (mut client_write, server_read) = tokio::io::duplex(256 * 1024);
        let (server_write, client_read) = tokio::io::duplex(64 * 1024);
        let tight_main = lease::BytePool::new(4096);
        let tight_ctrl = lease::BytePool::new(4096);
        let mut transport = StdioTransport::with_pools(
            server_read,
            server_write,
            Arc::clone(&leases),
            Arc::clone(&tight_main),
            Arc::clone(&tight_ctrl),
        );
        transport.max_line_length = 40 * 1024;
        let mut out_lines = tokio::io::BufReader::new(client_read).lines();

        let mid_overload_1 = format!(
            "{{\"jsonrpc\":\"2.0\",\"params\":{{\"nested\":{{\"id\":999}}}},\"id\":\"mid-1\",\"pad\":\"{}\"}}\n",
            "x".repeat(20 * 1024)
        );
        // Second over-budget request after `reset_line_buf()` must still hit `tight_main`/`tight_ctrl`.
        let mid_overload_2 = format!(
            "{{\"jsonrpc\":\"2.0\",\"params\":{{\"nested\":{{\"id\":999}}}},\"id\":\"mid-2\",\"pad\":\"{}\"}}\n",
            "x".repeat(20 * 1024)
        );
        // Fits `append` (< 4096) then fails `self.line_buf.take().parse(true)` (`4 * len` > 4096).
        let parse_overload = format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":\"parse-3\",\"method\":\"ping\",\"pad\":\"{}\"}}\n",
            "z".repeat(1400)
        );
        // Over-budget line after `take()` must also stay bound to `tight_main`/`tight_ctrl`.
        let mid_overload_4 = format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":\"mid-4\",\"method\":\"ping\",\"pad\":\"{}\"}}\n",
            "x".repeat(20 * 1024)
        );
        client_write
            .write_all(mid_overload_1.as_bytes())
            .await
            .unwrap();
        client_write
            .write_all(mid_overload_2.as_bytes())
            .await
            .unwrap();
        client_write
            .write_all(parse_overload.as_bytes())
            .await
            .unwrap();
        client_write
            .write_all(mid_overload_4.as_bytes())
            .await
            .unwrap();
        client_write
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":98,\"method\":\"ping\"}\n")
            .await
            .unwrap();

        let ping_98 = transport
            .receive()
            .await
            .expect("session must survive consecutive mid-line and parse budget errors");
        match ping_98 {
            rmcp::model::JsonRpcMessage::Request(req) => {
                assert_eq!(req.id, rmcp::model::RequestId::Number(98));
            }
            other => panic!("expected ping request 98, got {other:?}"),
        }

        // Lower `max_line_length` below BufReader's 8 KiB chunk so an oversized line trips `-32600`
        // before `append` checks the 4 KiB pool.
        transport.max_line_length = 6 * 1024;
        let oversized = format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":88,\"method\":\"tools/call\",\"params\":{{\"pad\":\"{}\"}}}}\n",
            "y".repeat(10 * 1024)
        );
        client_write.write_all(oversized.as_bytes()).await.unwrap();
        client_write
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":99,\"method\":\"ping\"}\n")
            .await
            .unwrap();

        let next = transport
            .receive()
            .await
            .expect("session must survive oversized lines");
        assert!(
            tight_main.used() > 0,
            "accepted ping must charge tight_main"
        );
        assert_eq!(tight_ctrl.used(), 0);
        match &next {
            rmcp::model::JsonRpcMessage::Request(req) => {
                assert_eq!(req.id, rmcp::model::RequestId::Number(99));
            }
            other => panic!("expected ping request 99, got {other:?}"),
        }
        drop(next);
        assert_eq!(tight_main.used(), 0);
        assert_eq!(tight_ctrl.used(), 0);

        for expected_id in [
            serde_json::json!("mid-1"),
            serde_json::json!("mid-2"),
            serde_json::json!("parse-3"),
            serde_json::json!("mid-4"),
        ] {
            let err: serde_json::Value =
                serde_json::from_str(&out_lines.next_line().await.unwrap().unwrap()).unwrap();
            assert_eq!(err["id"], expected_id);
            assert_eq!(err["error"]["code"], -32603);
        }

        let err_oversized: serde_json::Value =
            serde_json::from_str(&out_lines.next_line().await.unwrap().unwrap()).unwrap();
        assert_eq!(err_oversized["id"], 88);
        assert_eq!(err_oversized["error"]["code"], -32600);

        // Outbound unleased `send` also honors the instance's isolated pools rather than global pools.
        let mut zero_transport = StdioTransport::with_pools(
            tokio::io::empty(),
            tokio::io::sink(),
            Arc::new(RequestLeases::default()),
            lease::BytePool::new(0),
            lease::BytePool::new(0),
        );
        let progress = rmcp::model::ServerJsonRpcMessage::notification(
            rmcp::model::ServerNotification::ProgressNotification(
                rmcp::model::ProgressNotification::new(
                    rmcp::model::ProgressNotificationParam::new(
                        rmcp::model::ProgressToken(rmcp::model::NumberOrString::Number(1)),
                        1.0,
                    ),
                ),
            ),
        );
        let send_err = zero_transport.send(progress).await.unwrap_err();
        assert_eq!(send_err.kind(), std::io::ErrorKind::OutOfMemory);
    }

    #[tokio::test]
    async fn stdio_early_budget_exhaustion_silently_drops_complete_notifications() {
        use tokio::io::AsyncBufReadExt;

        let leases = Arc::new(RequestLeases::default());
        let (mut client_write, server_read) = tokio::io::duplex(256 * 1024);
        let (server_write, client_read) = tokio::io::duplex(64 * 1024);
        let tight_main = lease::BytePool::new(4096);
        let tight_ctrl = lease::BytePool::new(512);
        let mut transport = StdioTransport::with_pools(
            server_read,
            server_write,
            Arc::clone(&leases),
            Arc::clone(&tight_main),
            Arc::clone(&tight_ctrl),
        );
        transport.max_line_length = 32 * 1024;
        let mut out_lines = tokio::io::BufReader::new(client_read).lines();

        // 1. Complete single-line notifications failing `append` (~6 KiB > 4096) or `parse` `3 * len`
        // (~2 KiB > 4096 / 4) must produce zero stdout error frames.
        let append_stage_noti = format!(
            "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/custom\",\"params\":{{\"id\":\"nested\",\"pad\":\"{}\"}}}}\n",
            "n".repeat(6000)
        );
        let parse_stage_noti = format!(
            "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\",\"params\":{{\"pad\":\"{}\"}}}}\n",
            "m".repeat(2000)
        );
        // 2. Invalid JSON syntax, escaped `"id"` key, `"id": null`, truncated mid-line (> 8 KiB
        // BufReader chunk), and oversized (> 32 KiB max_line_length) notifications MUST still emit errors.
        let bad_syntax = format!(
            "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/custom\",\"pad\":\"{}\",}}\n",
            "b".repeat(6000)
        );
        let escaped_id_req = format!(
            "{{\"jsonrpc\":\"2.0\",\"i\\u0064\":\"esc-1\",\"method\":\"ping\",\"pad\":\"{}\"}}\n",
            "e".repeat(6000)
        );
        let null_id_req = format!(
            "{{\"jsonrpc\":\"2.0\",\"id\":null,\"method\":\"ping\",\"pad\":\"{}\"}}\n",
            "q".repeat(2000)
        );
        let truncated_mid_line_noti = format!(
            "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/custom\",\"pad\":\"{}\"}}\n",
            "t".repeat(16 * 1024)
        );
        let oversized_noti = format!(
            "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/custom\",\"pad\":\"{}\"}}\n",
            "o".repeat(36 * 1024)
        );

        for line in [
            append_stage_noti.as_str(),
            parse_stage_noti.as_str(),
            bad_syntax.as_str(),
            escaped_id_req.as_str(),
            null_id_req.as_str(),
            truncated_mid_line_noti.as_str(),
            "{\"jsonrpc\":\"2.0\",\"id\":76,\"method\":\"ping\"}\n",
        ] {
            client_write.write_all(line.as_bytes()).await.unwrap();
        }

        let ping_76 = transport.receive().await.expect("ping 76");
        match ping_76 {
            rmcp::model::JsonRpcMessage::Request(req) => {
                assert_eq!(req.id, rmcp::model::RequestId::Number(76));
            }
            other => panic!("expected ping 76, got {other:?}"),
        }

        // Lower `max_line_length` below BufReader's 8 KiB chunk so `oversized_noti` hits `-32600`.
        transport.max_line_length = 7 * 1024;
        client_write
            .write_all(oversized_noti.as_bytes())
            .await
            .unwrap();
        client_write
            .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":77,\"method\":\"ping\"}\n")
            .await
            .unwrap();

        let ping = transport.receive().await.expect("ping 77");
        match ping {
            rmcp::model::JsonRpcMessage::Request(req) => {
                assert_eq!(req.id, rmcp::model::RequestId::Number(77));
            }
            other => panic!("expected ping 77, got {other:?}"),
        }
        assert_eq!(tight_main.used(), 0);
        drop(transport);

        // Multi-chunk complete notification (10 KiB across 8 KiB BufReader split, failing second append).
        let (mut multi_write, multi_read) = tokio::io::duplex(64 * 1024);
        let (multi_srv_write, multi_cli_read) = tokio::io::duplex(16 * 1024);
        let multi_main = lease::BytePool::new(9000);
        let multi_ctrl = lease::BytePool::new(512);
        let mut multi_transport = StdioTransport::with_pools(
            multi_read,
            multi_srv_write,
            Arc::new(RequestLeases::default()),
            Arc::clone(&multi_main),
            Arc::clone(&multi_ctrl),
        );
        let multi_chunk_noti = format!(
            "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/custom\",\"pad\":\"{}\"}}\n",
            "k".repeat(9800)
        );
        multi_write
            .write_all(multi_chunk_noti.as_bytes())
            .await
            .unwrap();
        drop(multi_write);
        assert!(multi_transport.receive().await.is_none());
        assert_eq!(multi_main.used(), 0);
        drop(multi_transport);
        let mut multi_out = Vec::new();
        tokio::io::BufReader::new(multi_cli_read)
            .read_to_end(&mut multi_out)
            .await
            .unwrap();
        assert!(
            multi_out.is_empty(),
            "multi-chunk complete notification must not write error frame: {:?}",
            String::from_utf8_lossy(&multi_out)
        );

        let mut errs = Vec::new();
        while let Some(line) = out_lines.next_line().await.unwrap() {
            errs.push(serde_json::from_str::<serde_json::Value>(&line).unwrap());
        }
        assert_eq!(errs.len(), 5, "unexpected error frames: {errs:?}");
        assert!(errs[0]["id"].is_null());
        assert_eq!(errs[0]["error"]["code"], -32603);
        assert_eq!(errs[1]["id"], "esc-1");
        assert_eq!(errs[1]["error"]["code"], -32603);
        assert!(errs[2]["id"].is_null());
        assert_eq!(errs[2]["error"]["code"], -32603);
        assert!(errs[3]["id"].is_null());
        assert_eq!(errs[3]["error"]["code"], -32603);
        assert!(errs[4]["id"].is_null());
        assert_eq!(errs[4]["error"]["code"], -32600);
    }
}

#[cfg(test)]
mod g15_http_daemon_wiring_tests {
    use super::*;
    use crate::daemon::{self, Daemon, PendingHttp};
    use crate::ipc::{self, OsText, ToDaemon, ToEndpoint};
    use crate::transport::{self, Endpoint, Listener};
    use bytes::Bytes;
    use http::{Request, StatusCode, header};
    use http_body_util::{BodyExt, Full};
    use hyper_util::rt::TokioIo;
    use sha2::{Digest, Sha256};
    use std::time::Duration;
    use tokio::time::timeout;

    fn temp_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "fastexec-{label}-{}",
            crate::id::random_chars(12).unwrap()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    async fn ipc_hello(
        client: &mut transport::Client,
        session: &str,
        cwd: &std::path::Path,
        extra_env: Vec<(&str, &str)>,
    ) {
        let mut launch = LaunchContext::new(
            cwd.to_path_buf(),
            extra_env
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        );
        launch = launch.for_session();
        let (wire_cwd, mut wire_env) = ipc::launch_to_wire(&launch);
        wire_env.push((OsText::Text("G15_TAG".into()), OsText::Text("1".into())));
        let hello = ToDaemon::Hello {
            protocol: ipc::PROTOCOL,
            namespace: "fastexec".to_owned(),
            build: env!("CARGO_PKG_VERSION").to_owned(),
            session: session.to_owned(),
            connection: 1,
            client_name: "g15-cli".to_owned(),
            cwd: wire_cwd,
            env: wire_env,
        };
        ipc::write_frame_idle(client, &hello, Duration::from_secs(5))
            .await
            .unwrap();
        let welcome = ipc::read_frame::<_, ToEndpoint>(client, lease::FRAME_LIMIT)
            .await
            .unwrap();
        assert!(matches!(welcome, Some(ToEndpoint::Welcome { .. })));
    }

    #[tokio::test]
    async fn g15_cli_spawn_http_wires_daemon_port_status_mcp_requests_and_graceful_shutdown() {
        let root = temp_dir("g15-http-full");
        let logs = root.join("logs");
        crate::platform::runtime::create_private(&logs).unwrap();
        let tasks = Tasks::new_in(Limits::OFF, logs).unwrap();

        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = tcp.local_addr().unwrap();
        let token = "g15-bearer-secret";
        let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();

        let daemon = Daemon::with_config(
            Arc::clone(&tasks),
            "fastexec".to_owned(),
            env!("CARGO_PKG_VERSION").to_owned(),
            Some(bound.to_string()),
            None,
            daemon::Config::default(),
        );
        let launch = Arc::new(LaunchContext::new(root.clone(), Vec::new()));
        let http_task = daemon.attach_http(
            PendingHttp {
                listener: tcp,
                digests: vec![digest],
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
            },
            crate::cli::daemon::spawn_http,
        );

        let ipc_ep = Endpoint::new("g15-cli-http");
        let ipc_listener = Listener::bind(ipc_ep.name()).unwrap();
        let run_task = tokio::spawn(Arc::clone(&daemon).run(ipc_listener));

        // 1. IPC Status reports exact bound HTTP address.
        let mut ipc_client = transport::connect(ipc_ep.name()).await.unwrap();
        ipc_hello(&mut ipc_client, "g15sess1", &root, Vec::new()).await;
        ipc::write_frame_idle(&mut ipc_client, &ToDaemon::Status, Duration::from_secs(5))
            .await
            .unwrap();
        match ipc::read_frame::<_, ToEndpoint>(&mut ipc_client, lease::FRAME_LIMIT)
            .await
            .unwrap()
        {
            Some(ToEndpoint::Status { http, .. }) => {
                assert_eq!(http.as_deref(), Some(bound.to_string().as_str()));
            }
            other => panic!("expected Status frame, got {other:?}"),
        }

        // 2. HTTP /mcp rejects unauthenticated calls and serves authenticated MCP ping.
        {
            let stream = tokio::net::TcpStream::connect(bound).await.unwrap();
            let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
                .await
                .unwrap();
            let conn_task = tokio::spawn(async move {
                let _ = conn.await;
            });
            let unauth = Request::post("/mcp")
                .header(header::HOST, bound.to_string())
                .body(Full::new(Bytes::new()))
                .unwrap();
            let unauth_res = sender.send_request(unauth).await.unwrap();
            assert_eq!(unauth_res.status(), StatusCode::UNAUTHORIZED);
            drop(sender);
            let _ = conn_task.await;
        }
        {
            let stream = tokio::net::TcpStream::connect(bound).await.unwrap();
            let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
                .await
                .unwrap();
            let conn_task = tokio::spawn(async move {
                let _ = conn.await;
            });
            let body = serde_json::to_vec(&serde_json::json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "server/discover",
                "params": {
                    "_meta": {
                        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                        "io.modelcontextprotocol/clientInfo": {"name": "g15", "version": "1"},
                        "io.modelcontextprotocol/clientCapabilities": {}
                    }
                }
            }))
            .unwrap();
            let req = Request::post("/mcp")
                .header(header::HOST, "localhost")
                .header(header::AUTHORIZATION, format!("Bearer {token}"))
                .header(header::ACCEPT, "application/json, text/event-stream")
                .header(header::CONTENT_TYPE, "application/json")
                .header("mcp-protocol-version", "2026-07-28")
                .header("mcp-method", "server/discover")
                .body(Full::new(Bytes::from(body)))
                .unwrap();
            let res = sender.send_request(req).await.unwrap();
            assert_eq!(res.status(), StatusCode::OK);
            let payload: serde_json::Value =
                serde_json::from_slice(&res.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            assert_eq!(payload["id"], 1);
            assert_eq!(
                payload["result"]["capabilities"]["extensions"]["io.modelcontextprotocol/events"],
                serde_json::json!({ "listChanged": false })
            );
            drop(sender);
            let _ = conn_task.await;
        }

        // 3. IPC Stop cleanly shuts down both Daemon::run and http::serve.
        ipc::write_frame_idle(&mut ipc_client, &ToDaemon::Stop, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(
            ipc::read_frame::<_, ToEndpoint>(&mut ipc_client, lease::FRAME_LIMIT)
                .await
                .unwrap()
                .is_none()
        );
        timeout(Duration::from_secs(5), run_task)
            .await
            .expect("daemon run exited")
            .unwrap();
        timeout(Duration::from_secs(5), http_task)
            .await
            .expect("http::serve exited")
            .unwrap();
        assert!(tokio::net::TcpStream::connect(bound).await.is_err());

        tasks.shutdown();
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn g15_ipc_only_daemon_lazily_serves_transfers_and_stops_cleanly() {
        let root = temp_dir("g15-lazy-xfer");
        let logs = root.join("logs");
        crate::platform::runtime::create_private(&logs).unwrap();
        std::fs::write(root.join("payload.txt"), b"lazy-transfer-ok").unwrap();
        let tasks = Tasks::new_in(Limits::OFF, logs).unwrap();

        let daemon = Daemon::with_config(
            Arc::clone(&tasks),
            "fastexec".to_owned(),
            env!("CARGO_PKG_VERSION").to_owned(),
            None,
            None,
            daemon::Config::default(),
        );
        let ipc_ep = Endpoint::new("g15-lazy-ipc");
        let ipc_listener = Listener::bind(ipc_ep.name()).unwrap();
        let run_task = tokio::spawn(Arc::clone(&daemon).run(ipc_listener));

        let mut ipc_client = transport::connect(ipc_ep.name()).await.unwrap();
        ipc_hello(
            &mut ipc_client,
            "g15xfer1",
            &root,
            vec![("FASTEXEC_TRANSFER", "true")],
        )
        .await;

        let mint_call = ToDaemon::Call {
            id: 7,
            tool: "transfer".to_owned(),
            request_id: "7".to_owned(),
            arguments: serde_json::json!({ "path": "payload.txt" }),
        };
        ipc::write_frame_idle(&mut ipc_client, &mint_call, Duration::from_secs(5))
            .await
            .unwrap();
        let url = match ipc::read_frame::<_, ToEndpoint>(&mut ipc_client, lease::FRAME_LIMIT)
            .await
            .unwrap()
        {
            Some(ToEndpoint::Result { id: 7, result }) => {
                assert_eq!(result.is_error, Some(false));
                let text = result.content[0].as_text().unwrap().text.clone();
                text.split_whitespace()
                    .find(|tok| tok.starts_with("http://127.0.0.1:"))
                    .expect("transfer URL")
                    .to_owned()
            }
            other => panic!("expected transfer Result, got {other:?}"),
        };

        let uri: http::Uri = url.parse().unwrap();
        let addr: std::net::SocketAddr = uri.authority().unwrap().as_str().parse().unwrap();
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        let conn_task = tokio::spawn(async move {
            let _ = conn.await;
        });
        let get_req = Request::get(uri.path())
            .header(header::HOST, addr.to_string())
            .body(Full::new(Bytes::new()))
            .unwrap();
        let get_res = sender.send_request(get_req).await.unwrap();
        assert_eq!(get_res.status(), StatusCode::OK);
        let downloaded = get_res.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&downloaded[..], b"lazy-transfer-ok");
        drop(sender);
        let _ = conn_task.await;

        daemon.stop();
        timeout(Duration::from_secs(5), run_task)
            .await
            .expect("ipc-only daemon stopped")
            .unwrap();
        tasks.shutdown();
        let _ = std::fs::remove_dir_all(&root);
    }

    #[tokio::test]
    async fn daemon_emit_handshake_verifies_welcome_protocol_namespace_and_build() {
        let root = temp_dir("emit-welcome-check");
        let ep = Endpoint::new("emit-fake-daemon");
        let mut listener = Listener::bind(ep.name()).unwrap();
        let paths = crate::platform::runtime::Paths {
            root: root.clone(),
            ns: root.join("ns"),
            endpoint: ep.name().to_owned(),
        };
        let identity = crate::platform::runtime::Identity {
            namespace: "expected-ns".to_owned(),
            build: "expected-build".to_owned(),
        };

        let server = tokio::spawn(async move {
            let welcomes = [
                ToEndpoint::Welcome {
                    protocol: ipc::PROTOCOL + 1,
                    namespace: "expected-ns".to_owned(),
                    build: "expected-build".to_owned(),
                    epoch: "ep-1".to_owned(),
                },
                ToEndpoint::Welcome {
                    protocol: ipc::PROTOCOL,
                    namespace: "rogue-ns".to_owned(),
                    build: "expected-build".to_owned(),
                    epoch: "ep-1".to_owned(),
                },
                ToEndpoint::Welcome {
                    protocol: ipc::PROTOCOL,
                    namespace: "expected-ns".to_owned(),
                    build: "rogue-build".to_owned(),
                    epoch: "ep-1".to_owned(),
                },
                ToEndpoint::Welcome {
                    protocol: ipc::PROTOCOL,
                    namespace: "expected-ns".to_owned(),
                    build: "expected-build".to_owned(),
                    epoch: "ep-1".to_owned(),
                },
            ];
            for welcome in welcomes {
                let mut stream = listener.accept().await.unwrap();
                let hello = ipc::read_frame::<_, ToDaemon>(&mut stream, lease::FRAME_LIMIT)
                    .await
                    .unwrap();
                assert!(matches!(hello, Some(ToDaemon::HelloEmit { .. })));
                ipc::write_frame_idle(&mut stream, &welcome, Duration::from_secs(5))
                    .await
                    .unwrap();
            }
        });

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let Err(proto_err) = connect_emit_daemon(&identity, &paths, deadline).await else {
            panic!("mismatched protocol must fail");
        };
        assert!(
            proto_err.contains("speaks protocol"),
            "unexpected error: {proto_err}"
        );

        let Err(ns_err) = connect_emit_daemon(&identity, &paths, deadline).await else {
            panic!("mismatched namespace must fail");
        };
        assert_eq!(
            ns_err,
            "the daemon serves namespace rogue-ns, not expected-ns"
        );

        let Err(build_err) = connect_emit_daemon(&identity, &paths, deadline).await else {
            panic!("mismatched build must fail");
        };
        assert_eq!(
            build_err,
            "the daemon runs build rogue-build, not expected-build"
        );

        let _accepted = connect_emit_daemon(&identity, &paths, deadline)
            .await
            .expect("matching Welcome must succeed");

        timeout(Duration::from_secs(5), server)
            .await
            .expect("fake emit daemon finished")
            .unwrap();
        let _ = std::fs::remove_dir_all(&root);
    }
}
