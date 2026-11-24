//! Unit tests for CLI daemon flags, deadlines, event budgets, and stdio transport.

use super::daemon::{
    BoundedLine, DaemonFlags, NO_ANSWER, blocking_within, daemon_flags, parse_emit_payload,
    read_bounded_line, within,
};
use super::stdio::{StdioTransport, encode_stdio_frame};
use crate::config::{InstructionsToolMode, Tools};
use crate::events;
use crate::files;
use crate::ipc;
use crate::lease;
use crate::mcp::{Backend, RequestLeases, Server, decode_mcp_header_value};
use crate::service::Service;
use crate::service::render as service_render;
use crate::session::LaunchContext;
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
            let payload_bytes = service_render::result_payload_capacity(&result);
            let wire_bytes = ipc::json_len(&result).unwrap() + ipc::json_len(&id).unwrap() + 256;
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
            let tracked = files::TrackedResult {
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
        let expected = ipc::json_len(&item).unwrap() + 1;
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

        // Accepts mcpcli eventId/truncated aliases and fills source/topic from CLI flags.
        let parsed = parse_emit_payload(
            r#"{"eventId":"evt-42","name":"pr.merged","data":{"n":1},"truncated":true}"#,
            Some("github"),
            Some("pr"),
        )
        .unwrap();
        assert_eq!(parsed.source, "github");
        assert_eq!(parsed.topic.as_deref(), Some("pr"));
        assert_eq!(parsed.name, "pr.merged");
        assert_eq!(parsed.upstream_event_id, "evt-42");
        assert_eq!(parsed.upstream_truncated, Some(true));

        // Rejects oversized payloads (> 64 KiB).
        let huge = format!(
            r#"{{"source":"s","name":"n","upstreamEventId":"1","data":"{}"}}"#,
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
        assert!(decode_mcp_header_value("=?base64?!!!invalid!!!?=").is_none());
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
}
