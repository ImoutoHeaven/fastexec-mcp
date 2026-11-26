use super::*;
use crate::env::LaunchContext;
use crate::id::parse_handle;
use crate::service::core::{
    RenderSlot, output_incomplete, script_report, task_io_ok, task_warnings, text_workspace_bytes,
};
use crate::service::{ExecContext, Service, ServiceError};
use crate::tasks::session::{Session, TaskEntry};
use crate::tasks::state::Snapshot;
use crate::tasks::{self, Tasks};
use base64::Engine;
use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::{Value, json};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Wake, Waker};
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

struct ReadWake {
    memory: Arc<crate::lease::BytePool>,
    slots: Arc<Semaphore>,
    used: AtomicUsize,
    permits: AtomicUsize,
    woken: AtomicBool,
}

impl Wake for ReadWake {
    fn wake(self: Arc<Self>) {
        self.used.store(self.memory.used(), Ordering::Relaxed);
        self.permits
            .store(self.slots.available_permits(), Ordering::Relaxed);
        self.woken.store(true, Ordering::Release);
    }
}

fn script_fixture() -> (Arc<Service>, Arc<Tasks>, Arc<Session>) {
    let tasks = Tasks::new(tasks::Limits::OFF).unwrap();
    let service = Arc::new(Service::new(Arc::clone(&tasks), false));
    let launch = Arc::new(LaunchContext::new(std::env::temp_dir(), Vec::new()));
    let session = Session::new("script-test".into(), "script-test".into(), launch);
    (service, tasks, session)
}

fn script_ctx(session: &Arc<Session>) -> ExecContext<'_> {
    ExecContext {
        session,
        launch: None,
        cancel: CancellationToken::new(),
        progress: None,
        call_id: None,
        parent_task_id: None,
        queued_guard: None,
        on_started: None,
        input_owner: Default::default(),
    }
}

async fn run_codemode_agent(
    service: &Arc<Service>,
    request: crate::codemode::control::Request,
    mut ctx: ExecContext<'_>,
    delivery: AgentDelivery<'_>,
    transfer: bool,
) -> TrackedResult {
    let slot = delivery.daemon_ipc.as_ref().map(|d| d.slot);
    let outcome = crate::codemode::prepare_script(service, request, &mut ctx, slot, transfer).await;
    finish_exec_agent(service, outcome, &ctx, &delivery).await
}

fn text_of(result: &CallToolResult) -> String {
    result
        .content
        .iter()
        .filter_map(|b| b.as_text().map(|t| t.text.clone()))
        .collect()
}

fn task_handle_of(result: &CallToolResult) -> String {
    assert!(result.structured_content.is_none(), "{result:?}");
    let text = text_of(result);
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix('[')
            && let Some((_, after)) = rest.split_once("] ")
            && let Some(token) = after.split(" · ").next()
            && token.contains('.')
        {
            return token.to_owned();
        }
    }
    panic!("no task handle found in:\n{text}");
}

async fn list_fixture() -> (Arc<Service>, Arc<Tasks>, Arc<Session>, Arc<TaskEntry>) {
    let (service, tasks, session) = script_fixture();
    let ((task, entry, _), _) = crate::codemode::spawn_script(
        &tasks,
        crate::codemode::ScriptSpawnArgs {
            code: "return undefined;".into(),
            launch: Arc::clone(&session.launch),
            kill_after_ms: None,
            transfer: false,
            call_id: None,
            service: Arc::clone(&service),
            input_owner: Default::default(),
        },
        &session,
        "abcd".into(),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
        .await
        .unwrap()
        .unwrap();
    assert!(!entry.is_reported());
    (service, tasks, session, entry)
}

#[test]
fn wait_note_formats_agent_status_without_changing_match_ownership() {
    let text = "ready 世界".to_owned();
    let memory = crate::lease::BytePool::new(128);
    let lease = memory.try_acquire(text.capacity()).unwrap();
    let ended = WaitEnd::OutputContains(text, lease);
    let retained = memory.used();
    assert_eq!(
        wait_note(&ended, true),
        r#" · wait: output matched "ready 世界""#
    );
    assert_eq!(ended.parts(), ("output_contains", Some("ready 世界")));
    assert_eq!(memory.used(), retained);
    assert_eq!(
        wait_note(&WaitEnd::MaxWait, true),
        " · wait: waitMs elapsed"
    );
    assert_eq!(wait_note(&WaitEnd::MaxWait, false), "");
    drop(ended);
    assert_eq!(memory.used(), 0);
}

#[tokio::test]
async fn background_metadata_keeps_owner_and_claim_until_commit_and_only_kill_uses_control() {
    let (service, tasks, session, noticed_entry) = list_fixture().await;
    let noticed = tasks.all().pop().unwrap();
    let ((task, _entry, reader), _) = crate::codemode::spawn_script(
        &tasks,
        crate::codemode::ScriptSpawnArgs {
            code: "return undefined;".into(),
            launch: Arc::clone(&session.launch),
            kill_after_ms: None,
            transfer: false,
            call_id: None,
            service: Arc::clone(&service),
            input_owner: Default::default(),
        },
        &session,
        "meta".into(),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
        .await
        .unwrap()
        .unwrap();
    // Under an exhausted main pool with control memory available, poll fails with Overload and
    // leaves noticed_entry unreported, while kill uses control memory and delivers the background footer.
    let memory = crate::lease::BytePool::new(0);
    let control = crate::lease::BytePool::new(2 << 20);
    let launch = Arc::new(
        session
            .launch
            .for_session()
            .with_memory(Arc::clone(&memory))
            .with_control_memory(Arc::clone(&control)),
    );
    let mut poll_ctx = script_ctx(&session);
    poll_ctx.launch = Some(&launch);
    let polled = exec_agent_json(
        &service,
        json!({"action": "poll", "taskId": reader.handle(&task.id), "waitMs": 0}),
        poll_ctx,
        AgentDelivery::default(),
    )
    .await;
    assert_eq!(polled.result.is_error, Some(true));
    assert!(polled.result.structured_content.is_none());
    assert!(polled.overload);
    assert!(text_of(&polled.result).contains("Do not retry start or resend input"));
    assert!(!noticed_entry.is_reported());
    assert_eq!((memory.used(), control.used()), (0, 0));
    drop(polled);

    let mut kill_ctx = script_ctx(&session);
    kill_ctx.launch = Some(&launch);
    let killed = exec_agent_json(
        &service,
        json!({"action": "kill", "taskId": reader.handle(&task.id)}),
        kill_ctx,
        AgentDelivery::default(),
    )
    .await;
    assert_eq!(killed.result.is_error, Some(false), "{:?}", killed.result);
    assert!(killed.result.structured_content.is_none());
    assert!(text_of(&killed.result).contains(&noticed.id));
    assert!(noticed_entry.is_reported());
    assert!(!session.claim_outside().is_claimed());
    assert!(control.used() > 0);
    assert_eq!(memory.used(), 0);
    drop(killed);
    assert_eq!((memory.used(), control.used()), (0, 0));
    tasks.shutdown();
}

#[test]
fn response_snapshot_cancellation_releases_metadata_but_keeps_queued_window_owned() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (service, tasks, session) = script_fixture();
        let ((task, entry, _), _) = crate::codemode::spawn_script(
            &tasks,
            crate::codemode::ScriptSpawnArgs {
                code: "text('hello');".into(),
                launch: Arc::clone(&session.launch),
                kill_after_ms: None,
                transfer: false,
                call_id: None,
                service: Arc::clone(&service),
                input_owner: Default::default(),
            },
            &session,
            "meta".into(),
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
            .await
            .unwrap()
            .unwrap();
        let reader = entry.reader(Some("9999")).unwrap();
        assert!(reader.is_fresh());
        let memory = crate::lease::BytePool::new(2 << 20);
        let launch = Arc::new(
            session
                .launch
                .for_session()
                .with_memory(Arc::clone(&memory)),
        );
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            let _ = ready_tx.send(());
            let _ = blocked.recv();
        });
        ready_rx.await.unwrap();
        let slots = Arc::new(Semaphore::new(1));
        let slot = RenderSlot::new(&slots);
        let mut ctx = script_ctx(&session);
        ctx.launch = Some(&launch);
        let delivery = AgentDelivery {
            daemon_ipc: Some(DaemonDelivery {
                slot: &slot,
                ipc_body_overhead: crate::ipc::result_body_overhead(1),
            }),
            ..Default::default()
        };
        let mut operation = Box::pin(exec_agent_json(
            &service,
            json!({"action":"poll", "taskId":reader.handle(&task.id), "waitMs":0, "maxBytes":100}),
            ctx,
            delivery,
        ));
        if let std::task::Poll::Ready(result) =
            std::future::poll_fn(|cx| std::task::Poll::Ready(operation.as_mut().poll(cx))).await
        {
            panic!("the queued output worker must suspend: {:?}", result.result);
        }
        assert!(memory.used() > text_workspace_bytes(100));
        drop(operation);
        drop(slot);
        assert_eq!(memory.used(), text_workspace_bytes(100));
        assert_eq!(slots.available_permits(), 0);
        let observed = Arc::new(ReadWake {
            memory: Arc::clone(&memory),
            slots: Arc::clone(&slots),
            used: AtomicUsize::new(usize::MAX),
            permits: AtomicUsize::new(usize::MAX),
            woken: AtomicBool::new(false),
        });
        let waker = Waker::from(Arc::clone(&observed));
        let mut next_read = Box::pin(reader.read_gate().lock_owned());
        assert!(
            next_read
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        release.send(()).unwrap();
        blocker.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !observed.woken.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // The worker's unlock wakes the next reader before any later tuple fields drop.
        assert_eq!(observed.used.load(Ordering::Relaxed), 0);
        assert_eq!(observed.permits.load(Ordering::Relaxed), 1);
        let state = next_read.await;
        assert_eq!((state.cursor, state.attachments_seen), (0, 0));
        assert!(reader.is_fresh());
        assert!(!entry.is_reported());
        drop(state);
        tasks.shutdown();
    });
}

#[tokio::test]
async fn list_refusal_and_cancellation_leave_entries_due() {
    let (service, tasks, session, entry) = list_fixture().await;
    let main = crate::lease::BytePool::new(0);
    let control = crate::lease::BytePool::new(0);
    let blocked = Arc::new(
        session
            .launch
            .for_session()
            .with_memory(Arc::clone(&main))
            .with_control_memory(Arc::clone(&control)),
    );
    let mut ctx = script_ctx(&session);
    ctx.launch = Some(&blocked);
    let refused = exec_agent_json(
        &service,
        json!({"action":"list"}),
        ctx,
        AgentDelivery::default(),
    )
    .await;
    assert!(refused.overload);
    assert!(refused.result.structured_content.is_none());
    assert!(!entry.is_reported());
    assert_eq!((main.used(), control.used()), (0, 0));
    drop(refused);

    let pool = crate::lease::BytePool::new(1 << 20);
    let small_frame = Arc::new(
        session
            .launch
            .for_session()
            .with_memory(Arc::clone(&main))
            .with_control_memory(Arc::clone(&pool))
            .with_outbound_limit(64),
    );
    let mut ctx = script_ctx(&session);
    ctx.launch = Some(&small_frame);
    let refused = exec_agent_json(
        &service,
        json!({"action":"list"}),
        ctx,
        AgentDelivery::default(),
    )
    .await;
    assert!(refused.overload, "{:?}", refused.result);
    assert!(refused.result.structured_content.is_none());
    assert!(
        refused.result.content[0]
            .as_text()
            .unwrap()
            .text
            .contains("frame limit")
    );
    assert!(!entry.is_reported());
    assert_eq!(pool.used(), 0);
    drop(refused);

    let mut cancelled = script_ctx(&session);
    cancelled.cancel.cancel();
    cancelled.launch = Some(&small_frame);
    let refused = exec_agent_json(
        &service,
        json!({"action":"list"}),
        cancelled,
        AgentDelivery::default(),
    )
    .await;
    assert_eq!(refused.result.is_error, Some(true));
    assert!(refused.result.structured_content.is_none());
    assert!(!entry.is_reported());
    assert_eq!(pool.used(), 0);
    tasks.shutdown();
}

#[tokio::test]
async fn list_result_owns_payload_and_only_local_wire_after_admission() {
    for (http_response, daemon_call_id) in [
        (false, None),
        (true, None),
        (false, Some(7_u64)),
        (false, Some(u64::MAX)),
    ] {
        let (service, tasks, session, entry) = list_fixture().await;
        let main = crate::lease::BytePool::new(0);
        let control = crate::lease::BytePool::new(1 << 20);
        let launch = Arc::new(
            session
                .launch
                .for_session()
                .with_memory(Arc::clone(&main))
                .with_control_memory(Arc::clone(&control)),
        );
        let slots = Arc::new(Semaphore::new(1));
        let slot = RenderSlot::new(&slots);
        let mut ctx = script_ctx(&session);
        ctx.launch = Some(&launch);
        let mut delivery = AgentDelivery {
            http_response,
            ..Default::default()
        };
        if let Some(id) = daemon_call_id {
            delivery.daemon_ipc = Some(DaemonDelivery {
                slot: &slot,
                ipc_body_overhead: crate::ipc::result_body_overhead(id),
            });
            ctx.call_id = Some(id);
        }
        let mut result = exec_agent_json(&service, json!({"action":"list"}), ctx, delivery).await;
        assert_eq!(result.result.is_error, Some(false), "{:?}", result.result);
        assert!(result.result.structured_content.is_none());
        assert!(entry.is_reported());
        let payload = result_payload_capacity(&result.result);
        assert!(payload > 0);
        assert_eq!(
            unleased_result_payload_capacity(&result.result, &result.payload_owner),
            0
        );
        let expected_wire = if let Some(id) = daemon_call_id {
            crate::ipc::frame_len(&crate::ipc::ToEndpoint::Result {
                id,
                result: result.result.clone(),
            })
            .unwrap()
        } else if http_response {
            0
        } else {
            crate::lease::json_len(&result.result).unwrap() + 256
        };
        let wire = result.lease.as_ref().map_or(0, Lease::bytes);
        assert_eq!(wire, expected_wire);
        assert_eq!(control.used(), payload + wire);
        assert_eq!(main.used(), 0);
        drop(result.lease.take());
        assert_eq!(control.used(), payload);
        drop(result);
        assert_eq!(control.used(), 0);
        tasks.shutdown();
    }
}

#[test]
fn text_buffer_result_capacity_includes_resource_metadata_without_double_counting() {
    fn copied(text: &str, expected: &mut usize) -> String {
        let mut value = String::with_capacity(text.len() + 32);
        value.push_str(text);
        *expected += value.capacity();
        value
    }
    fn meta(expected: &mut usize) -> Option<rmcp::model::MetaObject> {
        let mut fields = serde_json::Map::new();
        fields.insert(
            copied("note", expected),
            Value::String(copied("sample", expected)),
        );
        Some(rmcp::model::MetaObject(fields))
    }
    fn annotations(expected: &mut usize) -> Option<rmcp::model::Annotations> {
        let mut annotations = rmcp::model::Annotations::default();
        annotations.last_modified = Some(copied("2026-11-20", expected));
        Some(annotations)
    }
    let png_url = pixel_url();
    let png = png_url.split_once(',').unwrap().1;
    let wav = base64::engine::general_purpose::STANDARD.encode(
        b"RIFF\x26\0\0\0WAVEfmt \x10\0\0\0\x01\0\x01\0\x40\x1f\0\0\x80\x3e\0\0\x02\0\x10\0data\x02\0\0\0\0\0",
    );
    let mut expected = 0;
    let mut text = rmcp::model::TextContent::new(copied("hello", &mut expected));
    text.meta = meta(&mut expected);
    text.annotations = annotations(&mut expected);
    let mut image = rmcp::model::ImageContent::new(
        copied(png, &mut expected),
        copied("image/png", &mut expected),
    );
    image.meta = meta(&mut expected);
    let mut audio = rmcp::model::AudioContent::new(
        copied(&wav, &mut expected),
        copied("audio/wav", &mut expected),
    );
    audio.annotations = annotations(&mut expected);
    let resource = rmcp::model::ResourceContents::BlobResourceContents {
        uri: copied("file:///sample.png", &mut expected),
        mime_type: Some(copied("image/png", &mut expected)),
        blob: copied(png, &mut expected),
        meta: meta(&mut expected),
    };
    let mut embedded = rmcp::model::EmbeddedResource::new(resource);
    embedded.meta = meta(&mut expected);
    embedded.annotations = annotations(&mut expected);
    let mut link = rmcp::model::Resource::new(
        copied("file:///sample.txt", &mut expected),
        copied("sample", &mut expected),
    );
    link.title = Some(copied("Sample", &mut expected));
    link.description = Some(copied("A small file", &mut expected));
    link.mime_type = Some(copied("text/plain", &mut expected));
    link.icons = Some(vec![
        rmcp::model::Icon::new(copied("file:///icon.png", &mut expected))
            .with_mime_type(copied("image/png", &mut expected))
            .with_sizes(vec![copied("16x16", &mut expected)]),
    ]);
    link.meta = meta(&mut expected);
    link.annotations = annotations(&mut expected);
    let mut result = CallToolResult::success(vec![
        ContentBlock::Text(text),
        ContentBlock::Image(image),
        ContentBlock::Audio(audio),
        ContentBlock::Resource(embedded),
        ContentBlock::ResourceLink(link),
    ]);
    result.meta = meta(&mut expected);
    assert_eq!(result_payload_capacity(&result), expected);
    let pool = crate::lease::BytePool::new(expected);
    let retained = expected / 2;
    let result = TrackedResult::owned(
        result,
        crate::lease::Owner::new(pool.try_acquire(retained).unwrap()),
    );
    assert_eq!(
        unleased_result_payload_capacity(&result.result, &result.payload_owner),
        expected - retained
    );
    drop(result);
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn rpc_id_budget_refusal_precedes_reader_commit_on_local_and_daemon_paths() {
    for daemon in [false, true] {
        let tasks = Tasks::new(tasks::Limits::OFF).unwrap();
        let service = Arc::new(Service::new(Arc::clone(&tasks), daemon));
        let pool = crate::lease::BytePool::new(2 << 20);
        let launch = Arc::new(
            crate::env::LaunchContext::new(std::env::temp_dir(), Vec::new())
                .with_memory(Arc::clone(&pool))
                .with_outbound_limit(2048),
        );
        let session = Session::new("rpc-id".into(), "rpc-id".into(), Arc::clone(&launch));
        let ((task, _, _), _) = crate::codemode::spawn_script(
            &tasks,
            crate::codemode::ScriptSpawnArgs {
                code: format!("text('hello'); attach({});", json!(pixel_url())),
                launch,
                kill_after_ms: None,
                transfer: false,
                call_id: None,
                service: Arc::clone(&service),
                input_owner: Default::default(),
            },
            &session,
            "abcd".into(),
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
            .await
            .unwrap()
            .unwrap();
        let entry = session.entry(&task);
        let reader = entry.reader(Some("9999")).unwrap();
        let render_pool = Arc::new(Semaphore::new(1));
        let slot = RenderSlot::new(&render_pool);
        let mut ctx = script_ctx(&session);
        let mut delivery = AgentDelivery {
            rpc_id_bytes: crate::lease::json_len(&"r".repeat(2048)).unwrap(),
            ..Default::default()
        };
        if daemon {
            ctx.call_id = Some(u64::MAX);
            delivery.daemon_ipc = Some(DaemonDelivery {
                slot: &slot,
                ipc_body_overhead: crate::ipc::result_body_overhead(u64::MAX),
            });
        }
        let request = json!({
            "action":"poll", "taskId":format!("{}.9999", task.id),
            "waitMs":0, "maxBytes":100,
        });
        let refused = exec_agent_json(&service, request.clone(), ctx, delivery).await;
        assert!(refused.overload, "{:?}", refused.result);
        assert!(refused.result.structured_content.is_none());
        assert!(
            refused.result.content[0]
                .as_text()
                .unwrap()
                .text
                .contains("frame limit")
        );
        {
            let state = reader.read_gate().lock_owned().await;
            assert_eq!(state.cursor, 0);
            assert_eq!(state.attachments_seen, 0);
            assert!(reader.is_fresh());
        }
        drop(refused);
        assert_eq!(pool.used(), 0);
        let mut ctx = script_ctx(&session);
        let mut delivery = AgentDelivery {
            rpc_id_bytes: crate::lease::json_len(&"small").unwrap(),
            ..Default::default()
        };
        if daemon {
            ctx.call_id = Some(u64::MAX);
            delivery.daemon_ipc = Some(DaemonDelivery {
                slot: &slot,
                ipc_body_overhead: crate::ipc::result_body_overhead(u64::MAX),
            });
        }
        let delivered = exec_agent_json(&service, request, ctx, delivery).await;
        assert_eq!(
            delivered.result.is_error,
            Some(false),
            "{:?}",
            delivered.result
        );
        assert!(delivered.result.structured_content.is_none());
        assert_eq!(image_count(&delivered.result), 1);
        {
            let state = reader.read_gate().lock_owned().await;
            assert!(state.cursor > 0);
            assert_eq!(state.attachments_seen, 1);
        }
        let payload = result_payload_capacity(&delivered.result);
        let unleased_payload =
            unleased_result_payload_capacity(&delivered.result, &delivered.payload_owner);
        let expected_wire = if daemon {
            crate::ipc::frame_len(&crate::ipc::ToEndpoint::Result {
                id: u64::MAX,
                result: delivered.result.clone(),
            })
            .unwrap()
        } else {
            crate::lease::json_len(&delivered.result).unwrap()
                + crate::lease::json_len(&"small").unwrap()
                + 256
        };
        assert_eq!(
            delivered.lease.as_ref().unwrap().bytes(),
            unleased_payload + expected_wire
        );
        assert_eq!(pool.used(), payload + expected_wire);
        drop(delivered);
        assert_eq!(pool.used(), 0);
        tasks.shutdown();
    }
}

#[tokio::test]
async fn daemon_ipc_wire_lease_and_tight_pool_boundary_are_exact_across_call_ids() {
    for call_id in [7_u64, u64::MAX] {
        // 1. Text-only poll admission: exact wire lease and tight BytePool boundary.
        let tasks = Tasks::new(tasks::Limits::OFF).unwrap();
        let service = Arc::new(Service::new(Arc::clone(&tasks), true));
        let prep_pool = crate::lease::BytePool::new(4 << 20);
        let prep_launch =
            Arc::new(LaunchContext::new(std::env::temp_dir(), Vec::new()).with_memory(prep_pool));
        let session = Session::new(
            "g10-poll".into(),
            "g10-poll".into(),
            Arc::clone(&prep_launch),
        );
        let ((task, entry, _), _) = crate::codemode::spawn_script(
            &tasks,
            crate::codemode::ScriptSpawnArgs {
                code: "text('exact-ipc-wire');".into(),
                launch: Arc::clone(&prep_launch),
                kill_after_ms: None,
                transfer: false,
                call_id: None,
                service: Arc::clone(&service),
                input_owner: Default::default(),
            },
            &session,
            "init".into(),
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
            .await
            .unwrap()
            .unwrap();
        let slots = Arc::new(Semaphore::new(1));
        let slot = RenderSlot::new(&slots);
        let delivery = || AgentDelivery {
            daemon_ipc: Some(DaemonDelivery {
                slot: &slot,
                ipc_body_overhead: crate::ipc::result_body_overhead(call_id),
            }),
            ..Default::default()
        };

        let probe_reader = entry.reader(Some("prb0")).unwrap();
        let mut probe_ctx = script_ctx(&session);
        probe_ctx.call_id = Some(call_id);
        let probe_req: Request = serde_json::from_value(json!({
            "action": "poll",
            "taskId": probe_reader.handle(&task.id),
            "waitMs": 0,
            "maxBytes": 100
        }))
        .unwrap();
        let probe_prepared = service
            .exec_core(
                probe_req,
                ExecPolicy::AGENT,
                Consumer::Agent,
                &mut probe_ctx,
                Some(&slot),
            )
            .await
            .unwrap();
        let probed = finish_exec_agent(&service, Ok(probe_prepared), &probe_ctx, &delivery()).await;
        assert_eq!(probed.result.is_error, Some(false));
        let poll_payload = result_payload_capacity(&probed.result);
        let poll_wire = crate::ipc::frame_len(&crate::ipc::ToEndpoint::Result {
            id: call_id,
            result: probed.result.clone(),
        })
        .unwrap();
        assert_eq!(probed.lease.as_ref().unwrap().bytes(), poll_wire);
        drop(probed);

        for (key, limit, expect_ok) in [
            ("tght", poll_payload + poll_wire - 1, false),
            ("tght", poll_payload + poll_wire, true),
        ] {
            let reader = entry.reader(Some(key)).unwrap();
            let mut prep_ctx = script_ctx(&session);
            prep_ctx.call_id = Some(call_id);
            let req: Request = serde_json::from_value(json!({
                "action": "poll",
                "taskId": reader.handle(&task.id),
                "waitMs": 0,
                "maxBytes": 100
            }))
            .unwrap();
            let prepared = service
                .exec_core(
                    req,
                    ExecPolicy::AGENT,
                    Consumer::Agent,
                    &mut prep_ctx,
                    Some(&slot),
                )
                .await
                .unwrap();
            let tight_pool = crate::lease::BytePool::new(limit);
            let zero_control = crate::lease::BytePool::new(0);
            let tight_launch = Arc::new(
                prep_launch
                    .for_session()
                    .with_memory(Arc::clone(&tight_pool))
                    .with_control_memory(zero_control),
            );
            let mut admit_ctx = script_ctx(&session);
            admit_ctx.launch = Some(&tight_launch);
            admit_ctx.call_id = Some(call_id);
            let outcome = finish_exec_agent(&service, Ok(prepared), &admit_ctx, &delivery()).await;
            if !expect_ok {
                assert!(outcome.overload, "{:?}", outcome.result);
                assert!(reader.is_fresh());
                assert_eq!(tight_pool.used(), 0);
            } else {
                assert_eq!(outcome.result.is_error, Some(false), "{:?}", outcome.result);
                assert_eq!(
                    outcome.lease.as_ref().unwrap().bytes(),
                    crate::ipc::frame_len(&crate::ipc::ToEndpoint::Result {
                        id: call_id,
                        result: outcome.result.clone(),
                    })
                    .unwrap()
                );
                assert_eq!(tight_pool.used(), poll_payload + poll_wire);
                assert!(!reader.is_fresh());
                drop(outcome);
                assert_eq!(tight_pool.used(), 0);
            }
        }
        tasks.shutdown();

        // 2. List admission: exact wire lease and tight BytePool boundary.
        let (service, tasks, session, list_entry) = list_fixture().await;
        let list_task = tasks.all().pop().unwrap();
        let snap = list_task.snapshot();
        let view = || crate::service::schema::View {
            inline_markers: false,
            keep_nul: false,
            truncate: Truncate::HeadTail,
            max_bytes: 4096,
            raw: false,
            encoding: None,
            screen: false,
            conditions: false,
        };
        let make_list = |pool: &Arc<crate::lease::BytePool>| {
            PreparedExec::List(PreparedList {
                workspace: Lease::empty(pool),
                permit: None,
                total_count: 1,
                items: vec![crate::tasks::ListedTask {
                    task: Arc::clone(&list_task),
                    snapshot: snap.clone(),
                    parent_task_id: None,
                    children: None,
                }],
                parent_filter: None,
                state_filter: None,
                view: view(),
            })
        };
        let probe_pool = crate::lease::BytePool::new(1 << 20);
        let mut probe_ctx = script_ctx(&session);
        probe_ctx.call_id = Some(call_id);
        let probed_list = finish_exec_agent(
            &service,
            Ok(make_list(&probe_pool)),
            &probe_ctx,
            &delivery(),
        )
        .await;
        let list_payload = result_payload_capacity(&probed_list.result);
        let list_wire = crate::ipc::frame_len(&crate::ipc::ToEndpoint::Result {
            id: call_id,
            result: probed_list.result.clone(),
        })
        .unwrap();
        assert_eq!(probed_list.lease.as_ref().unwrap().bytes(), list_wire);
        drop(probed_list);

        let (service, tasks2, session2, list_entry2) = list_fixture().await;
        let list_task2 = tasks2.all().pop().unwrap();
        let snap2 = list_task2.snapshot();
        for (limit, expect_ok) in [
            (list_payload + list_wire - 1, false),
            (list_payload + list_wire, true),
        ] {
            let tight_pool = crate::lease::BytePool::new(limit);
            let zero_control = crate::lease::BytePool::new(0);
            let tight_launch = Arc::new(
                session2
                    .launch
                    .for_session()
                    .with_memory(Arc::clone(&tight_pool))
                    .with_control_memory(zero_control),
            );
            let mut ctx = script_ctx(&session2);
            ctx.launch = Some(&tight_launch);
            ctx.call_id = Some(call_id);
            let prepared = PreparedExec::List(PreparedList {
                workspace: Lease::empty(&tight_pool),
                permit: None,
                total_count: 1,
                items: vec![crate::tasks::ListedTask {
                    task: Arc::clone(&list_task2),
                    snapshot: snap2.clone(),
                    parent_task_id: None,
                    children: None,
                }],
                parent_filter: None,
                state_filter: None,
                view: view(),
            });
            let outcome = finish_exec_agent(&service, Ok(prepared), &ctx, &delivery()).await;
            if !expect_ok {
                assert!(outcome.overload, "{:?}", outcome.result);
                assert!(!list_entry2.is_reported());
                assert_eq!(tight_pool.used(), 0);
            } else {
                assert_eq!(outcome.result.is_error, Some(false), "{:?}", outcome.result);
                assert!(list_entry2.is_reported());
                assert_eq!(
                    outcome.lease.as_ref().unwrap().bytes(),
                    crate::ipc::frame_len(&crate::ipc::ToEndpoint::Result {
                        id: call_id,
                        result: outcome.result.clone(),
                    })
                    .unwrap()
                );
                assert_eq!(tight_pool.used(), list_payload + list_wire);
                drop(outcome);
                assert_eq!(tight_pool.used(), 0);
            }
        }
        let _ = list_entry;
        tasks.shutdown();
        tasks2.shutdown();
    }
}

#[tokio::test]
async fn daemon_outbound_limit_compares_against_ipc_body_without_header_prefix() {
    let slots = Arc::new(Semaphore::new(1));
    let slot = RenderSlot::new(&slots);
    let ipc_body_overhead = 300usize;
    let delivery = || AgentDelivery {
        rpc_id_bytes: 0,
        daemon_ipc: Some(DaemonDelivery {
            slot: &slot,
            ipc_body_overhead,
        }),
        ..Default::default()
    };
    assert!(ipc_body_overhead > 256);

    // 1. Poll path: `outbound_limit == ipc_body - 1` refuses, `outbound_limit == ipc_body` admits.
    let (service, tasks, session, entry) = list_fixture().await;
    let task = tasks.all().pop().unwrap();
    let probe_reader = entry.reader(Some("9001")).unwrap();
    let mut probe_ctx = script_ctx(&session);
    probe_ctx.call_id = Some(7);
    let probed = exec_agent_json(
        &service,
        json!({"action": "poll", "taskId": probe_reader.handle(&task.id), "waitMs": 0}),
        probe_ctx,
        delivery(),
    )
    .await;
    assert_eq!(probed.result.is_error, Some(false));
    let rpc_body = crate::lease::json_len(&probed.result).unwrap();
    let ipc_body = rpc_body + ipc_body_overhead;
    let ipc_wire = ipc_body + 4;
    assert_eq!(probed.lease.as_ref().unwrap().bytes(), ipc_wire);
    drop(probed);

    let reader = entry.reader(Some("9002")).unwrap();
    let req = json!({"action": "poll", "taskId": reader.handle(&task.id), "waitMs": 0});
    let short_launch = Arc::new(
        session
            .launch
            .for_session()
            .with_outbound_limit(ipc_body - 1),
    );
    let mut short_ctx = script_ctx(&session);
    short_ctx.launch = Some(&short_launch);
    short_ctx.call_id = Some(7);
    let refused = exec_agent_json(&service, req.clone(), short_ctx, delivery()).await;
    assert!(refused.overload, "{:?}", refused.result);
    assert!(text_of(&refused.result).contains("frame limit"));
    assert!(reader.is_fresh());

    let exact_launch = Arc::new(session.launch.for_session().with_outbound_limit(ipc_body));
    let mut exact_ctx = script_ctx(&session);
    exact_ctx.launch = Some(&exact_launch);
    exact_ctx.call_id = Some(7);
    let admitted = exec_agent_json(&service, req, exact_ctx, delivery()).await;
    assert_eq!(
        admitted.result.is_error,
        Some(false),
        "{:?}",
        admitted.result
    );
    assert!(ipc_wire > ipc_body);
    assert_eq!(admitted.lease.as_ref().unwrap().bytes(), ipc_wire);
    assert!(!reader.is_fresh());
    drop(admitted);
    tasks.shutdown();

    // 2. List path: `outbound_limit == ipc_body - 1` refuses, `outbound_limit == ipc_body` admits.
    let (service, tasks, session, _) = list_fixture().await;
    let mut probe_ctx = script_ctx(&session);
    probe_ctx.call_id = Some(7);
    let probed_list =
        exec_agent_json(&service, json!({"action": "list"}), probe_ctx, delivery()).await;
    let list_rpc_body = crate::lease::json_len(&probed_list.result).unwrap();
    let list_ipc_body = list_rpc_body + ipc_body_overhead;
    let list_ipc_wire = list_ipc_body + 4;
    drop(probed_list);

    let (service2, tasks2, session2, entry2) = list_fixture().await;
    // Match list output byte length by using the same fixture shape; re-probe exact body on service2
    // without committing by cloning the rendered result via a dry probe on another session view.
    let probe_session = Session::new(
        "script-test".into(),
        "script-test".into(),
        Arc::clone(&session2.launch),
    );
    let mut dry_ctx = script_ctx(&probe_session);
    dry_ctx.call_id = Some(7);
    let dry_list = exec_agent_json(
        &service2,
        json!({"action": "list", "scope": "all"}),
        dry_ctx,
        delivery(),
    )
    .await;
    let exact_list_body = crate::lease::json_len(&dry_list.result).unwrap() + ipc_body_overhead;
    let _ = (list_ipc_body, list_ipc_wire);
    drop(dry_list);

    let short_list_launch = Arc::new(
        session2
            .launch
            .for_session()
            .with_outbound_limit(exact_list_body - 1),
    );
    let mut short_list_ctx = script_ctx(&session2);
    short_list_ctx.launch = Some(&short_list_launch);
    short_list_ctx.call_id = Some(7);
    let refused_list = exec_agent_json(
        &service2,
        json!({"action": "list"}),
        short_list_ctx,
        delivery(),
    )
    .await;
    assert!(refused_list.overload, "{:?}", refused_list.result);
    assert!(text_of(&refused_list.result).contains("frame limit"));
    assert!(!entry2.is_reported());

    let exact_list_launch = Arc::new(
        session2
            .launch
            .for_session()
            .with_outbound_limit(exact_list_body),
    );
    let mut exact_list_ctx = script_ctx(&session2);
    exact_list_ctx.launch = Some(&exact_list_launch);
    exact_list_ctx.call_id = Some(7);
    let admitted_list = exec_agent_json(
        &service2,
        json!({"action": "list"}),
        exact_list_ctx,
        delivery(),
    )
    .await;
    assert_eq!(
        admitted_list.result.is_error,
        Some(false),
        "{:?}",
        admitted_list.result
    );
    assert!(entry2.is_reported());
    assert_eq!(
        admitted_list.lease.as_ref().unwrap().bytes(),
        exact_list_body + 4
    );
    drop(admitted_list);
    tasks.shutdown();
    tasks2.shutdown();
}

#[tokio::test]
async fn script_report_uses_control_only_for_kill_and_keeps_one_snapshot() {
    let tasks = Tasks::new(tasks::Limits::OFF).unwrap();
    let service = Arc::new(Service::new(Arc::clone(&tasks), false));
    let memory = crate::lease::BytePool::new(0);
    let control = crate::lease::BytePool::new(4 << 20);
    let launch = Arc::new(
        crate::env::LaunchContext::new(std::env::temp_dir(), Vec::new())
            .with_memory(Arc::clone(&memory))
            .with_control_memory(Arc::clone(&control)),
    );
    let session = Session::new("report".into(), "report".into(), Arc::clone(&launch));
    let ((task, _, _), _) = crate::codemode::spawn_script(
        &tasks,
        crate::codemode::ScriptSpawnArgs {
            code: "return undefined;".into(),
            launch,
            kill_after_ms: None,
            transfer: false,
            call_id: None,
            service: Arc::clone(&service),
            input_owner: Default::default(),
        },
        &session,
        "abcd".into(),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
        .await
        .unwrap()
        .unwrap();
    let handle = format!("{}.abcd", task.id);
    task.script().unwrap().record_started(handle.clone());
    for action in [Action::Start, Action::Poll] {
        let error = script_report(&tasks, &task, &session.launch, action)
            .err()
            .unwrap();
        assert!(matches!(
            error,
            ServiceError::Overload(_, Some(meta)) if meta.bare_task_id == task.id
        ));
        assert_eq!(control.used(), 0);
    }
    let report = script_report(&tasks, &task, &session.launch, Action::Kill)
        .unwrap()
        .unwrap();
    assert_eq!(report.started_tasks[0].task_id, handle);
    assert_eq!(report.started_tasks[0].state, "exited");
    assert!(report.running.is_empty());
    assert!(report.cancelled_calls.is_empty());
    let held = control.used();
    assert!(held > 0);
    task.script()
        .unwrap()
        .record_started("task-other.abcd".into());
    assert_eq!(report.started_tasks.len(), 1);
    assert_eq!(control.used(), held);
    drop(report);
    assert_eq!(control.used(), 0);

    let result = exec_agent_json(
        &service,
        json!({"action": "kill", "taskId": task.id, "maxBytes": 100}),
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await;
    assert_eq!(result.result.is_error, Some(false), "{:?}", result.result);
    assert!(result.result.structured_content.is_none());
    assert!(control.used() > 0);
    drop(result);
    assert_eq!(control.used(), 0);
    assert_eq!(memory.used(), 0);
    tasks.shutdown();
}

fn pixel_url() -> String {
    let mut bytes = Vec::new();
    image::DynamicImage::new_rgb8(1, 1)
        .write_to(
            &mut std::io::Cursor::new(&mut bytes),
            image::ImageFormat::Png,
        )
        .unwrap();
    format!(
        "data:image/png;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(bytes)
    )
}

fn image_count(result: &CallToolResult) -> usize {
    serde_json::to_value(&result.content)
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .filter(|block| block["type"] == "image")
        .count()
}

#[tokio::test]
async fn agent_text_result_keeps_its_payload_owner_after_rendering() {
    let tasks = Tasks::new(tasks::Limits::OFF).unwrap();
    let service = Arc::new(Service::new(Arc::clone(&tasks), false));
    let pool = crate::lease::BytePool::new(2 << 20);
    let launch = Arc::new(
        crate::env::LaunchContext::new(std::env::temp_dir(), Vec::new())
            .with_memory(Arc::clone(&pool)),
    );
    let session = Session::new("text-owner".into(), "text-owner".into(), launch);
    let mut result = run_codemode_agent(
        &service,
        serde_json::from_value(json!({"code": "console.log('hello 世界');", "maxBytes": 100}))
            .unwrap(),
        script_ctx(&session),
        AgentDelivery::default(),
        false,
    )
    .await;
    assert_eq!(result.result.is_error, Some(false), "{:?}", result.result);
    assert!(result.result.structured_content.is_none());
    let retained = result_payload_capacity(&result.result);
    assert!(retained > 0);
    let wire = result.lease.as_ref().unwrap().bytes();
    assert!(wire >= crate::lease::json_len(&result.result).unwrap());
    assert_eq!(pool.used(), retained + wire);
    drop(result.lease.take());
    assert_eq!(pool.used(), retained);
    assert_eq!(
        unleased_result_payload_capacity(&result.result, &result.payload_owner),
        0
    );
    tokio::task::yield_now().await;
    assert_eq!(pool.used(), retained);
    drop(result);
    assert_eq!(pool.used(), 0);
    tasks.shutdown();
}

#[cfg(unix)]
#[tokio::test]
async fn cancellation_keeps_attachment_worker_memory_and_reader_pinned() {
    use crate::service::core::PreparedSnapshot;
    use std::os::unix::{ffi::OsStrExt, fs::OpenOptionsExt};
    let tasks = Tasks::new(tasks::Limits::OFF).unwrap();
    let service = Arc::new(Service::new(Arc::clone(&tasks), false));
    let pool = crate::lease::BytePool::new(1 << 20);
    let launch = Arc::new(
        crate::env::LaunchContext::new(std::env::temp_dir(), Vec::new())
            .with_memory(Arc::clone(&pool)),
    );
    let session = Session::new("pack-cancel".into(), "pack-cancel".into(), launch);
    let result = run_codemode_agent(
        &service,
        serde_json::from_value(
            json!({"code": format!("attach({})", json!(pixel_url())), "maxBytes": 100}),
        )
        .unwrap(),
        script_ctx(&session),
        AgentDelivery::default(),
        false,
    )
    .await
    .result;
    assert_eq!(result.is_error, Some(false), "{result:?}");
    let handle = task_handle_of(&result);
    let (id, _) = parse_handle(&handle).unwrap();
    let id = id.to_owned();
    drop(result);
    let task = tasks.find(&id).unwrap();
    // Caller report and metadata may release on cancellation; the blocked worker
    // must keep its own buffers charged.
    let report_charge = {
        let before = pool.used();
        let report = task.script().unwrap().report(&pool, None).unwrap();
        let charge = pool.used() - before;
        drop(report);
        assert_eq!(pool.used(), before);
        charge
    };
    let metadata_charge = {
        let before = pool.used();
        let mut metadata = PreparedSnapshot::new(&task, &session.launch, Action::Poll).unwrap();
        metadata
            .refresh(&task, &session.launch, Action::Poll)
            .unwrap();
        let charge = pool.used() - before;
        assert!(charge > 0);
        drop(metadata);
        assert_eq!(pool.used(), before);
        charge
    };
    let path = task.attachment_path(1, "image/png");
    let original = std::fs::read(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    let name = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    let entry = session.entry(&task);
    let reader = entry.reader(Some("9999")).unwrap();
    let request =
        json!({"action": "poll", "taskId": format!("{id}.9999"), "waitMs": 0, "maxBytes": 100});
    let cancel = CancellationToken::new();
    let cancelled = cancel.clone();
    let running_service = Arc::clone(&service);
    let running_session = Arc::clone(&session);
    let response = tokio::spawn(async move {
        let mut ctx = script_ctx(&running_session);
        ctx.cancel = cancelled;
        exec_agent_json(&running_service, request, ctx, AgentDelivery::default())
            .await
            .result
    });
    // Opening the writer succeeds only once the packing worker has opened the FIFO.
    // Keep it empty so the real worker remains blocked in its attachment read.
    let writer = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&path)
            {
                Ok(file) => break file,
                Err(error) if error.raw_os_error() == Some(libc::ENXIO) => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                Err(error) => panic!("{error}"),
            }
        }
    })
    .await
    .unwrap();
    let held = pool.used();
    assert!(held > report_charge + metadata_charge);
    cancel.cancel();
    let cancelled = tokio::time::timeout(Duration::from_secs(5), response)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cancelled.is_error, Some(true));
    assert_eq!(
        pool.used(),
        held - report_charge - metadata_charge,
        "only the caller's report and metadata may release before the blocking worker exits"
    );
    assert!(reader.read_gate().try_lock_owned().is_err());
    drop(writer);
    let state = tokio::time::timeout(Duration::from_secs(5), reader.read_gate().lock_owned())
        .await
        .unwrap();
    assert_eq!(state.cursor, 0);
    assert_eq!(state.attachments_seen, 0);
    assert!(reader.is_fresh());
    assert_eq!(pool.used(), 0);
    drop(state);
    std::fs::remove_file(&path).unwrap();
    std::fs::write(&path, original).unwrap();
    tasks.shutdown();
}

#[tokio::test]
async fn codemode_attachments_deliver_all_without_count_cap_and_reattach_on_bare_id() {
    let (service, tasks, session) = script_fixture();
    let code = format!("for(let i=0;i<6;i++) attach({});", json!(pixel_url()));
    let result = run_codemode_agent(
        &service,
        serde_json::from_value(json!({
            "code": code, "maxBytes": 100, "truncate": "tail",
        }))
        .unwrap(),
        script_ctx(&session),
        AgentDelivery::default(),
        false,
    )
    .await
    .result;
    assert_eq!(result.is_error, Some(false), "{result:?}");
    assert!(result.structured_content.is_none());
    assert_eq!(image_count(&result), 6, "{result:?}");
    let text = text_of(&result);
    assert!(text.contains("Attached: "), "{text}");
    for idx in 1..=6 {
        assert!(text.contains(&format!("#{idx} (image/png,")), "{text}");
    }
    let handle = task_handle_of(&result);
    let empty = exec_agent_json(
        &service,
        json!({"action":"poll","taskId":handle,"waitMs":0}),
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await
    .result;
    assert!(empty.structured_content.is_none());
    assert_eq!(image_count(&empty), 0);
    let bare = parse_handle(&handle).unwrap().0;
    let reread = exec_agent_json(
        &service,
        json!({"action":"poll","taskId":bare,"waitMs":0}),
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await
    .result;
    assert!(reread.structured_content.is_none());
    assert_eq!(image_count(&reread), 6);
    let reread_text = text_of(&reread);
    assert!(reread_text.contains("Attached: "), "{reread_text}");
    for idx in 1..=6 {
        assert!(
            reread_text.contains(&format!("#{idx} (image/png,")),
            "{reread_text}"
        );
    }
    tasks.shutdown();
}

#[tokio::test]
async fn codemode_greedy_packing_eof_continuation_identical_dedup_and_single_oversize_skip() {
    let tasks = Tasks::new(tasks::Limits::OFF).unwrap();
    let service = Arc::new(Service::new(Arc::clone(&tasks), false));
    // Tight 4600-byte outbound frame limit: fits ONE ~1600-byte PNG (~2136 B b64) per response,
    // refuses two together, and skips a single ~3600-byte PNG (~4800 B b64 > 4600 B solo cap).
    let launch = Arc::new(
        crate::env::LaunchContext::new(std::env::temp_dir(), Vec::new()).with_outbound_limit(4600),
    );
    let session = Session::new("pack-test".into(), "pack-test".into(), launch);

    let base_png = base64::engine::general_purpose::STANDARD
        .decode(pixel_url().split_once(',').unwrap().1)
        .unwrap();
    let mut medium_png = base64::engine::general_purpose::STANDARD
        .decode(pixel_url().split_once(',').unwrap().1)
        .unwrap();
    medium_png.resize(1600, 0);
    let mut huge_png = base_png.clone();
    huge_png.resize(3600, 0);

    let med_url = format!(
        "data:image/png;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(&medium_png)
    );
    let huge_url = format!(
        "data:image/png;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(&huge_png)
    );
    let small_url = pixel_url();

    // Attach TWO byte-identical medium PNGs (#1 and #2), ONE oversized PNG (#3), and ONE small PNG (#4).
    let code = format!(
        "text('done-line'); attach({med_url:?}); attach({med_url:?}); attach({huge_url:?}); attach({small_url:?});"
    );
    let first = run_codemode_agent(
        &service,
        serde_json::from_value(json!({"code": code})).unwrap(),
        script_ctx(&session),
        AgentDelivery::default(),
        false,
    )
    .await
    .result;
    assert_eq!(first.is_error, Some(false), "{first:?}");
    assert!(first.structured_content.is_none());
    assert_eq!(image_count(&first), 1);
    let t1 = text_of(&first);
    assert!(t1.contains("done-line"), "{t1}");
    assert!(t1.contains("[exited 0]"), "{t1}");
    assert!(t1.contains("Attached: #1 (image/png, 1600 B)"), "{t1}");
    let handle = task_handle_of(&first);

    // Text is ALREADY at EOF: inputless poll #1 delivers identical attachment #2!
    let second = exec_agent_json(
        &service,
        json!({"action": "poll", "taskId": handle, "waitMs": 0}),
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await
    .result;
    assert_eq!(second.is_error, Some(false), "{second:?}");
    assert!(second.structured_content.is_none());
    assert_eq!(image_count(&second), 1);
    let t2 = text_of(&second);
    assert!(t2.contains("Attached: #2 (image/png, 1600 B)"), "{t2}");

    // Inputless poll #2: attachment #3 exceeds the 4600-byte outbound frame limit on its own,
    // so it emits a visible degradation receipt naming #3 and advances straight through to #4!
    let third = exec_agent_json(
        &service,
        json!({"action": "poll", "taskId": handle, "waitMs": 0}),
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await
    .result;
    assert_eq!(third.is_error, Some(false), "{third:?}");
    assert!(third.structured_content.is_none());
    assert_eq!(image_count(&third), 1);
    let t3 = text_of(&third);
    assert!(t3.contains("Attached: #4 (image/png,"), "{t3}");
    let combined = format!("{t1}\n{t2}\n{t3}");
    assert_eq!(
        combined
            .lines()
            .filter(|line| line.contains("attachment #3") && line.contains("exceeds"))
            .count(),
        1,
        "{combined}"
    );

    // Inputless poll #3: nothing left.
    let fourth = exec_agent_json(
        &service,
        json!({"action": "poll", "taskId": handle, "waitMs": 0}),
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await
    .result;
    assert!(fourth.structured_content.is_none());
    assert_eq!(image_count(&fourth), 0);

    // Re-attach via bare taskId: visible `Attached: #<index>` summary!
    let bare = parse_handle(&handle).unwrap().0;
    let p1 = exec_agent_json(
        &service,
        json!({"action": "poll", "taskId": bare, "waitMs": 0}),
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await
    .result;
    assert!(p1.structured_content.is_none());
    let p1_text = p1.content[0].as_text().unwrap().text.clone();
    assert!(
        p1_text.contains("Attached: #1 (image/png, 1600 B)"),
        "{p1_text}"
    );

    // Corrupted attachment size leaves fresh reader uncommitted until restored.
    let att1_path = tasks.find(bare).unwrap().attachment_path(1, "image/png");
    std::fs::OpenOptions::new()
        .write(true)
        .open(&att1_path)
        .unwrap()
        .set_len(1599)
        .unwrap();
    let fresh_req = json!({"action": "poll", "taskId": format!("{bare}.9999"), "waitMs": 0});
    let changed = exec_agent_json(
        &service,
        fresh_req.clone(),
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await
    .result;
    assert_eq!(changed.is_error, Some(true));
    assert!(changed.structured_content.is_none());
    assert!(
        serde_json::to_string(&changed.content)
            .unwrap()
            .contains("attachment size changed")
    );
    std::fs::write(&att1_path, &medium_png).unwrap();
    let recovered = exec_agent_json(
        &service,
        fresh_req,
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await
    .result;
    assert!(recovered.structured_content.is_none());
    assert_eq!(image_count(&recovered), 1);
    tasks.shutdown();
}

#[tokio::test]
async fn script_poll_rejects_presence_even_when_false_or_empty() {
    let (service, tasks, session) = script_fixture();
    let result = run_codemode_agent(
        &service,
        serde_json::from_value(json!({"code":"return 1"})).unwrap(),
        script_ctx(&session),
        AgentDelivery::default(),
        false,
    )
    .await
    .result;
    let task_id = task_handle_of(&result);
    for accepted in [
        json!({"eof":false,"screen":false,"raw":false}),
        json!({"input":null,"eof":null,"keys":null,"screen":null,"raw":null,"encoding":null}),
        json!({"keys":[]}),
        json!({"keys":[],"keyDelayMs":150}),
    ] {
        let mut request = json!({"action":"poll","taskId":task_id,"waitMs":0});
        request
            .as_object_mut()
            .unwrap()
            .extend(accepted.as_object().unwrap().clone());
        let ok = exec_agent_json(
            &service,
            request,
            script_ctx(&session),
            AgentDelivery::default(),
        )
        .await
        .result;
        assert_eq!(ok.is_error, Some(false), "{ok:?}");
        assert!(ok.structured_content.is_none());
    }
    for fields in [
        json!({"input":""}),
        json!({"eof":true}),
        json!({"keys":["Enter"]}),
        json!({"keyDelayMs":0}),
        json!({"cols":80}),
        json!({"rows":24}),
        json!({"screen":true}),
        json!({"raw":true}),
        json!({"encoding":"utf-8"}),
        json!({"returnWhen":{"screenContains":["x"]}}),
    ] {
        let mut request = json!({"action":"poll","taskId":task_id,"waitMs":0});
        request
            .as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        let result = exec_agent_json(
            &service,
            request,
            script_ctx(&session),
            AgentDelivery::default(),
        )
        .await
        .result;
        assert_eq!(result.is_error, Some(true), "{result:?}");
        assert!(result.structured_content.is_none());
        assert!(
            serde_json::to_string(&result.content)
                .unwrap()
                .contains("A codemode task takes no input")
        );
    }
    let result = exec_agent_json(
        &service,
        json!({"action":"transcript","taskId":task_id}),
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await
    .result;
    assert!(result.structured_content.is_none());
    assert!(
        serde_json::to_string(&result.content)
            .unwrap()
            .contains("has no terminal")
    );
    tasks.shutdown();
}

#[tokio::test]
async fn critical_warnings_survive_small_budgets() {
    let (service, tasks, session) = script_fixture();
    let snapshot = Snapshot {
        state: "exited",
        exit_code: Some(0),
        forced_end: false,
        elapsed: Duration::ZERO,
        lines: 1,
        dropped: 128,
        evicted: true,
        output_open: true,
        expired: false,
        log_error: None,
        capture_error: Some("read failed".into()),
        input_warning: Some("canonical input may be discarded".into()),
        input_error: Some("stdin write failed".into()),
        leftovers: Vec::new(),
        kept: false,
        tree_ended: true,
        held: Vec::new(),
        held_known: true,
        kill_error: None,
    };
    assert!(output_incomplete(&snapshot));
    assert!(!task_io_ok(&snapshot));
    let warnings = task_warnings(&snapshot);
    assert_eq!(warnings.len(), 6);

    let ((task, _, reader), _) = crate::codemode::spawn_script(
        &tasks,
        crate::codemode::ScriptSpawnArgs {
            code: "text('captured prefix');".into(),
            launch: Arc::clone(&session.launch),
            kill_after_ms: None,
            transfer: false,
            call_id: None,
            service: Arc::clone(&service),
            input_owner: Default::default(),
        },
        &session,
        "wr01".into(),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
        .await
        .unwrap()
        .unwrap();
    task.inject_io_errors(Some("read failed"), Some("log write failed"));
    let polled = exec_agent_json(
        &service,
        json!({
            "action": "poll",
            "taskId": reader.handle(&task.id),
            "waitMs": 0,
            "maxBytes": 100
        }),
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await
    .result;
    assert_eq!(polled.is_error, Some(true), "{polled:?}");
    assert!(polled.structured_content.is_none());
    let text = text_of(&polled);
    assert!(text.find("[exited 0]").unwrap() < text.find("Warning:").unwrap());
    assert!(
        text.contains("Warning: output capture failed (read failed)"),
        "{text}"
    );
    assert!(
        text.contains("Warning: log write failed (log write failed)"),
        "{text}"
    );
    tasks.shutdown();
}

#[test]
fn first_committed_render_gets_outside_count_while_an_earlier_read_is_pending() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (service, tasks, owner) = script_fixture();
        run_codemode_agent(
            &service,
            serde_json::from_value(json!({"code":"while (true) {}","waitMs":0})).unwrap(),
            script_ctx(&owner),
            AgentDelivery::default(),
            false,
        )
        .await;
        let completed = run_codemode_agent(
            &service,
            serde_json::from_value(json!({"code":"return 'ready'"})).unwrap(),
            script_ctx(&owner),
            AgentDelivery::default(),
            false,
        )
        .await
        .result;
        let handle = task_handle_of(&completed);
        let bare = parse_handle(&handle).unwrap().0;
        let request = json!({"action":"poll","taskId":bare,"waitMs":0});

        for cancel_earlier in [false, true] {
            let session = Session::new(
                format!("reader-{cancel_earlier}"),
                "reader".into(),
                Arc::clone(&owner.launch),
            );
            let (entered, ready) = tokio::sync::oneshot::channel();
            let (release, blocked) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                entered.send(()).unwrap();
                blocked.recv().unwrap();
            });
            ready.await.unwrap();
            let cancel = CancellationToken::new();
            let mut ctx = script_ctx(&session);
            ctx.cancel = cancel.clone();
            let mut earlier = Box::pin(async {
                exec_agent_json(&service, request.clone(), ctx, AgentDelivery::default())
                    .await
                    .result
            });
            let pending = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(earlier.as_mut().poll(cx).is_pending())
            })
            .await;
            release.send(()).unwrap();
            blocker.await.unwrap();
            assert!(
                pending,
                "the first render must stop at its queued blocking read"
            );

            // Its I/O may finish, but the earlier future cannot commit until we poll it again.
            let first = exec_agent_json(
                &service,
                request.clone(),
                script_ctx(&session),
                AgentDelivery::default(),
            )
            .await
            .result;
            let text = text_of(&first);
            assert!(
                text.contains("1 task running outside this session"),
                "the first committed render must carry the count: {text}"
            );
            if cancel_earlier {
                cancel.cancel();
            }
            let later = earlier.await;
            let text = text_of(&later);
            assert!(!text.contains("running outside this session"), "{text}");
            assert_eq!(later.is_error, Some(cancel_earlier), "{later:?}");
        }
        tasks.shutdown();
    });
}

#[tokio::test]
async fn a_large_list_row_does_not_hide_the_opposite_end() {
    let (service, tasks, session) = script_fixture();
    let ((first, _, _), _) = crate::codemode::spawn_script(
        &tasks,
        crate::codemode::ScriptSpawnArgs {
            code: format!("return '{}';", "x".repeat(200)),
            launch: Arc::clone(&session.launch),
            kill_after_ms: None,
            transfer: false,
            call_id: None,
            service: Arc::clone(&service),
            input_owner: Default::default(),
        },
        &session,
        "lg01".into(),
    )
    .unwrap();
    let ((second, _, _), _) = crate::codemode::spawn_script(
        &tasks,
        crate::codemode::ScriptSpawnArgs {
            code: "return 1;".into(),
            launch: Arc::clone(&session.launch),
            kill_after_ms: None,
            transfer: false,
            call_id: None,
            service: Arc::clone(&service),
            input_owner: Default::default(),
        },
        &session,
        "lg02".into(),
    )
    .unwrap();
    for task in [&first, &second] {
        tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
            .await
            .unwrap()
            .unwrap();
    }
    let listed = exec_agent_json(
        &service,
        json!({"action": "list", "maxBytes": 120, "truncate": "head_tail"}),
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await
    .result;
    assert_eq!(listed.is_error, Some(false), "{listed:?}");
    assert!(listed.structured_content.is_none());
    let text = text_of(&listed);
    assert!(
        text.contains(&second.id),
        "short row should remain visible: {text}"
    );
    assert!(text.contains("omitted"), "omission marker expected: {text}");
    tasks.shutdown();
}

#[test]
fn screen_row_omission_keeps_and_cuts_the_bottom_row_when_it_exceeds_budget() {
    let single = vec!["bottom-".repeat(30)];
    let (body, omitted, line_cut) = crate::term::output::fit_screen_rows(&single, 100, true);
    assert_eq!(omitted, 0);
    assert!(line_cut);
    assert!(body.starts_with("bottom-bottom-"), "{body}");
    assert!(body.ends_with(" … [cut]"), "{body}");
    assert!(body.len() <= 100);

    let multi = vec!["top-1".into(), "top-2".into(), "bottom-".repeat(30)];
    let (body, omitted, line_cut) = crate::term::output::fit_screen_rows(&multi, 100, true);
    assert_eq!(omitted, 2);
    assert!(line_cut);
    assert!(
        body.starts_with("... [2 top rows omitted] ...\nbottom-"),
        "{body}"
    );
    assert!(body.ends_with(" … [cut]"), "{body}");
    assert!(body.len() <= 100);
}

#[tokio::test]
async fn status_line_appends_parent_task_id_hint_only_when_more_than_three_children_running() {
    let (service, tasks, session) = script_fixture();
    // Keep at most 3 background script slots occupied so the 4th slot remains free for the
    // parent script under test; a 4th distinct reader handle on the first running task exercises
    // report.running.len() == 4 without exceeding the 4-script concurrency cap.
    let mut running_handles = Vec::new();
    let mut first_running_id = String::new();
    for idx in 0..3usize {
        let ((child, _, reader), _) = crate::codemode::spawn_script(
            &tasks,
            crate::codemode::ScriptSpawnArgs {
                code: "await sleep(30000);".into(),
                launch: Arc::clone(&session.launch),
                kill_after_ms: None,
                transfer: false,
                call_id: None,
                service: Arc::clone(&service),
                input_owner: Default::default(),
            },
            &session,
            format!("rn0{idx}"),
        )
        .unwrap();
        if idx == 0 {
            first_running_id = child.id.clone();
        }
        running_handles.push(reader.handle(&child.id));
    }
    running_handles.push(format!("{first_running_id}.rn03"));

    for (count, expect_hint) in [(3usize, false), (4usize, true)] {
        // Exited branch.
        let ((exited_task, _, exited_reader), _) = crate::codemode::spawn_script(
            &tasks,
            crate::codemode::ScriptSpawnArgs {
                code: "return undefined;".into(),
                launch: Arc::clone(&session.launch),
                kill_after_ms: None,
                transfer: false,
                call_id: None,
                service: Arc::clone(&service),
                input_owner: Default::default(),
            },
            &session,
            format!("ex0{count}"),
        )
        .unwrap();
        tokio::time::timeout(
            Duration::from_secs(5),
            exited_task.done().wait_for(|done| *done),
        )
        .await
        .unwrap()
        .unwrap();
        for handle in &running_handles[..count] {
            exited_task.script().unwrap().record_started(handle.clone());
        }
        let polled = exec_agent_json(
            &service,
            json!({
                "action": "poll",
                "taskId": exited_reader.handle(&exited_task.id),
                "waitMs": 0
            }),
            script_ctx(&session),
            AgentDelivery::default(),
        )
        .await
        .result;
        assert_ne!(polled.is_error, Some(true), "{polled:?}");
        assert!(polled.structured_content.is_none());
        let exited_text = polled.content[0].as_text().unwrap().text.as_str();
        let exited_hint = format!(
            "; list active with exec list parentTaskId=\"{}\" state=\"active\"",
            exited_task.id
        );
        assert!(exited_text.contains("[exited 0]"), "{exited_text}");
        assert!(
            exited_text.contains("tasks it started still running:"),
            "{exited_text}"
        );
        assert_eq!(
            exited_text.contains(&exited_hint),
            expect_hint,
            "exited branch count={count}: {exited_text}"
        );

        // Killed branch.
        let ((killed_task, _, killed_reader), _) = crate::codemode::spawn_script(
            &tasks,
            crate::codemode::ScriptSpawnArgs {
                code: "await sleep(30000);".into(),
                launch: Arc::clone(&session.launch),
                kill_after_ms: None,
                transfer: false,
                call_id: None,
                service: Arc::clone(&service),
                input_owner: Default::default(),
            },
            &session,
            format!("kk0{count}"),
        )
        .unwrap();
        for handle in &running_handles[..count] {
            killed_task.script().unwrap().record_started(handle.clone());
        }
        let killed = exec_agent_json(
            &service,
            json!({
                "action": "kill",
                "taskId": killed_reader.handle(&killed_task.id)
            }),
            script_ctx(&session),
            AgentDelivery::default(),
        )
        .await
        .result;
        assert_ne!(killed.is_error, Some(true), "{killed:?}");
        assert!(killed.structured_content.is_none());
        let killed_text = killed.content[0].as_text().unwrap().text.as_str();
        let killed_hint = format!(
            "; list active with exec list parentTaskId=\"{}\" state=\"active\"",
            killed_task.id
        );
        assert!(killed_text.contains("[killed]"), "{killed_text}");
        assert!(
            killed_text.contains("tasks it started keep running:"),
            "{killed_text}"
        );
        assert_eq!(
            killed_text.contains(&killed_hint),
            expect_hint,
            "killed branch count={count}: {killed_text}"
        );
    }

    tasks.shutdown();
}

#[tokio::test]
async fn combined_background_footer_fits_and_rejected_results_keep_events_unseen() {
    let (service, tasks, session, _) = list_fixture().await;
    let task = tasks.all().pop().unwrap();
    let ((other, _, _), _) = crate::codemode::spawn_script(
        &tasks,
        crate::codemode::ScriptSpawnArgs {
            code: "return undefined;".into(),
            launch: Arc::clone(&session.launch),
            kill_after_ms: None,
            transfer: false,
            call_id: None,
            service: Arc::clone(&service),
            input_owner: Default::default(),
        },
        &session,
        "more".into(),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), other.done().wait_for(|done| *done))
        .await
        .unwrap()
        .unwrap();
    for index in 0..3 {
        service
            .event_bus
            .publish_external(crate::events::ExternalEventInput {
                event_type: None,
                source: format!("{index}{}", "源".repeat(47)),
                topic: Some("题".repeat(48)),
                name: "名".repeat(48),
                event_id: format!("footer-event-{index}"),
                timestamp: None,
                data: json!({}),
            })
            .unwrap();
    }

    let launch = Arc::new(session.launch.for_session().with_outbound_limit(1));
    let mut refused_ctx = script_ctx(&session);
    refused_ctx.launch = Some(&launch);
    let request = json!({"action":"poll", "taskId":task.id, "waitMs":0});
    let refused = exec_agent_json(
        &service,
        request.clone(),
        refused_ctx,
        AgentDelivery::default(),
    )
    .await
    .result;
    assert_eq!(refused.is_error, Some(true));
    assert!(refused.structured_content.is_none());

    let next = exec_agent_json(
        &service,
        request.clone(),
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await
    .result;
    assert!(next.structured_content.is_none());
    let text = &next.content[0].as_text().unwrap().text;
    assert!(text.contains(&other.id), "{text}");
    assert!(text.contains("External events: 3 new"), "{text}");
    let footer_start = text.find("\n(Background:").unwrap();
    assert!(
        text.len() - footer_start <= 512,
        "footer exceeded 512 bytes: {}",
        &text[footer_start..]
    );

    let once = exec_agent_json(
        &service,
        request,
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await
    .result;
    assert!(once.structured_content.is_none());
    let text = &once.content[0].as_text().unwrap().text;
    assert!(!text.contains("External events:"), "{text}");
    tasks.shutdown();
}

#[tokio::test]
async fn capacity_refusals_mark_agent_results_as_overload() {
    for script in [false, true] {
        let tasks = Tasks::new(tasks::Limits {
            max_running: if script { 1 } else { 0 },
            max_scripts: 0,
            ..tasks::Limits::OFF
        })
        .unwrap();
        let service = Arc::new(Service::new(Arc::clone(&tasks), false));
        let session = Session::new(
            "capacity".into(),
            "capacity".into(),
            Arc::new(LaunchContext::current().unwrap()),
        );
        let result = if script {
            run_codemode_agent(
                &service,
                serde_json::from_value(json!({"code": "return undefined;"})).unwrap(),
                script_ctx(&session),
                AgentDelivery::default(),
                false,
            )
            .await
        } else {
            exec_agent_json(
                &service,
                json!({"action": "start", "command": "true"}),
                script_ctx(&session),
                AgentDelivery::default(),
            )
            .await
        };
        assert!(tasks.all().is_empty());
        tasks.shutdown();
        assert_eq!(result.result.is_error, Some(true));
        assert!(result.overload, "{}", text_of(&result.result));
        assert!(result.task_meta.is_none());
    }
}

#[tokio::test]
async fn busy_renderer_preserves_overload_and_task_recovery_metadata() {
    let (service, tasks, session, entry) = list_fixture().await;
    let task = tasks.all().pop().unwrap();
    let reader = entry.reader(Some("b005")).unwrap();
    let pool = crate::lease::BytePool::new(0);
    let launch = Arc::new(session.launch.for_session().with_memory(Arc::clone(&pool)));
    let slots = Arc::new(Semaphore::new(0));
    let slot = RenderSlot::new(&slots);
    let mut ctx = script_ctx(&session);
    ctx.launch = Some(&launch);
    let refused = exec_agent_json(
        &service,
        json!({"action": "poll", "taskId": reader.handle(&task.id), "waitMs": 0}),
        ctx,
        AgentDelivery {
            daemon_ipc: Some(DaemonDelivery {
                slot: &slot,
                ipc_body_overhead: crate::ipc::result_body_overhead(5),
            }),
            ..Default::default()
        },
    )
    .await;
    tasks.shutdown();
    let text = text_of(&refused.result);
    assert!(refused.overload, "{text}");
    let meta = refused.task_meta.unwrap();
    assert_eq!(meta.bare_task_id, task.id);
    assert_eq!(
        meta.log_path,
        crate::files::path::display_path(&task.log_path)
    );
    assert!(text.contains("daemon is rendering other results"), "{text}");
    assert!(
        text.contains("Do not resend input, keys, or resize."),
        "{text}"
    );
    assert!(reader.is_fresh());
    assert!(!entry.is_reported());
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn three_render_slot_pools_prevent_busy_ordinary_renders_from_starving_kill_or_list() {
    let (service, tasks, session) = script_fixture();
    let ((task, entry, reader), _) = crate::codemode::spawn_script(
        &tasks,
        crate::codemode::ScriptSpawnArgs {
            code: "text('live-output'); await sleep(60000);".into(),
            launch: Arc::clone(&session.launch),
            kill_after_ms: None,
            transfer: false,
            call_id: Some(11),
            service: Arc::clone(&service),
            input_owner: Default::default(),
        },
        &session,
        "s001".into(),
    )
    .unwrap();
    let handle = reader.handle(&task.id);

    // Occupy both ordinary render slots (capacity 2), leaving independent kill (1) and list (1) free.
    let render_pool = Arc::new(Semaphore::new(2));
    let kill_pool = Arc::new(Semaphore::new(1));
    let list_pool = Arc::new(Semaphore::new(1));
    let _held_ordinary = Arc::clone(&render_pool)
        .acquire_many_owned(2)
        .await
        .unwrap();
    assert_eq!(render_pool.available_permits(), 0);

    let busy_slot = RenderSlot::new(&render_pool);
    let mut poll_ctx = script_ctx(&session);
    poll_ctx.call_id = Some(12);
    let poll_refused = exec_agent_json(
        &service,
        json!({"action": "poll", "taskId": handle, "waitMs": 0}),
        poll_ctx,
        AgentDelivery {
            daemon_ipc: Some(DaemonDelivery {
                slot: &busy_slot,
                ipc_body_overhead: crate::ipc::result_body_overhead(12),
            }),
            ..Default::default()
        },
    )
    .await;
    assert!(poll_refused.overload, "{:?}", poll_refused.result);
    assert!(
        reader.read_gate().try_lock_owned().is_ok(),
        "try_hold() failure must immediately release the reader gate for kill"
    );

    // Independent list pool succeeds without waiting on ordinary render slots.
    let list_slot = RenderSlot::new(&list_pool);
    let mut list_ctx = script_ctx(&session);
    list_ctx.call_id = Some(13);
    let listed = exec_agent_json(
        &service,
        json!({"action": "list"}),
        list_ctx,
        AgentDelivery {
            daemon_ipc: Some(DaemonDelivery {
                slot: &list_slot,
                ipc_body_overhead: crate::ipc::result_body_overhead(13),
            }),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(listed.result.is_error, Some(false), "{:?}", listed.result);
    assert!(text_of(&listed.result).contains(&task.id));
    drop(listed);
    drop(list_slot);
    assert_eq!(list_pool.available_permits(), 1);

    // Independent kill pool succeeds on the same reader handle while ordinary slots remain full.
    let kill_slot = RenderSlot::new(&kill_pool);
    let mut kill_ctx = script_ctx(&session);
    kill_ctx.call_id = Some(14);
    let killed = exec_agent_json(
        &service,
        json!({"action": "kill", "taskId": handle}),
        kill_ctx,
        AgentDelivery {
            daemon_ipc: Some(DaemonDelivery {
                slot: &kill_slot,
                ipc_body_overhead: crate::ipc::result_body_overhead(14),
            }),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(killed.result.is_error, Some(false), "{:?}", killed.result);
    assert!(text_of(&killed.result).contains("[killed]"));
    drop(killed);
    drop(kill_slot);
    assert_eq!(kill_pool.available_permits(), 1);
    assert!(entry.is_reported());
    tasks.shutdown();
}

#[tokio::test]
async fn kill_defers_queue_head_oversize_attachment_until_subsequent_poll_skips_it() {
    let tasks = Tasks::new(tasks::Limits::OFF).unwrap();
    let service = Arc::new(Service::new(Arc::clone(&tasks), false));
    let launch = Arc::new(
        crate::env::LaunchContext::new(std::env::temp_dir(), Vec::new()).with_outbound_limit(4600),
    );
    let session = Session::new("kill-oversize".into(), "kill-oversize".into(), launch);

    let mut huge_png = base64::engine::general_purpose::STANDARD
        .decode(pixel_url().split_once(',').unwrap().1)
        .unwrap();
    huge_png.resize(3600, 0);
    let huge_url = format!(
        "data:image/png;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(&huge_png)
    );
    let small_url = pixel_url();

    let ((task, entry, reader), _) = crate::codemode::spawn_script(
        &tasks,
        crate::codemode::ScriptSpawnArgs {
            code: format!("attach({huge_url:?}); attach({small_url:?}); await sleep(60000);"),
            launch: Arc::clone(&session.launch),
            kill_after_ms: None,
            transfer: false,
            call_id: None,
            service: Arc::clone(&service),
            input_owner: Default::default(),
        },
        &session,
        "k001".into(),
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while task.unseen_attachments(0).len() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let handle = reader.handle(&task.id);
    let killed = exec_agent_json(
        &service,
        json!({"action": "kill", "taskId": handle}),
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await
    .result;
    assert_eq!(killed.is_error, Some(false), "{killed:?}");
    assert_eq!(image_count(&killed), 0);
    {
        let state = reader.read_gate().lock_owned().await;
        assert_eq!(
            state.attachments_seen, 0,
            "kill must never advance attachments_seen past a queue-head oversize attachment"
        );
    }
    assert!(entry.is_reported());

    // Subsequent inputless poll skips queue-head #1 with a degradation warning and delivers #2.
    let polled = exec_agent_json(
        &service,
        json!({"action": "poll", "taskId": handle, "waitMs": 0}),
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await
    .result;
    assert_eq!(polled.is_error, Some(false), "{polled:?}");
    assert_eq!(image_count(&polled), 1);
    let text = text_of(&polled);
    assert!(
        text.contains("attachment #1") && text.contains("exceeds"),
        "{text}"
    );
    assert!(text.contains("Attached: #2 (image/png,"), "{text}");
    tasks.shutdown();
}

#[tokio::test]
async fn publish_transfer_formats_agent_result_and_retains_input_owner() {
    let pool = crate::lease::BytePool::new(64);
    let launch = || {
        Arc::new(crate::env::LaunchContext::new(
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")),
            Vec::new(),
        ))
    };
    let table = Arc::new(crate::transfer::Transfers::new());

    for (path, expect_err, snippet) in [
        ("Cargo.toml", false, "bytes"),
        (
            "missing-transfer-target.bin",
            false,
            "does not exist yet; a PUT creates it",
        ),
        (".", true, "it is a directory"),
    ] {
        let result = super::files::publish_transfer(
            crate::transfer::Request { path: path.into() },
            launch(),
            &table,
            "http://localhost/",
            crate::lease::Owner::new(pool.try_acquire(64).unwrap()),
        )
        .await;
        assert_eq!(
            result.result.is_error,
            Some(expect_err),
            "{:?}",
            result.result
        );
        assert!(result.result.structured_content.is_none());
        assert!(
            text_of(&result.result).contains(snippet),
            "{}",
            text_of(&result.result)
        );
        assert_eq!(pool.used(), 64);
        drop(result);
        assert_eq!(pool.used(), 0);
    }
}

#[tokio::test]
async fn background_drop_releases_workspace_before_unlocking_outside_claim() {
    let (service, tasks, session) = script_fixture();
    let other_session = Session::new(
        "outside-owner".into(),
        "outside-owner".into(),
        Arc::clone(&session.launch),
    );
    let ((outside_task, _, _), _) = crate::codemode::spawn_script(
        &tasks,
        crate::codemode::ScriptSpawnArgs {
            code: "await sleep(60000);".into(),
            launch: Arc::clone(&other_session.launch),
            kill_after_ms: None,
            transfer: false,
            call_id: None,
            service: Arc::clone(&service),
            input_owner: Default::default(),
        },
        &other_session,
        "out1".into(),
    )
    .unwrap();
    let ((foreground_task, _, _), _) = crate::codemode::spawn_script(
        &tasks,
        crate::codemode::ScriptSpawnArgs {
            code: "return undefined;".into(),
            launch: Arc::clone(&session.launch),
            kill_after_ms: None,
            transfer: false,
            call_id: None,
            service: Arc::clone(&service),
            input_owner: Default::default(),
        },
        &session,
        "fg01".into(),
    )
    .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        foreground_task.done().wait_for(|done| *done),
    )
    .await
    .unwrap()
    .unwrap();

    let pool = crate::lease::BytePool::new(1 << 20);
    let launch = Arc::new(session.launch.for_session().with_memory(Arc::clone(&pool)));
    let mut ctx = script_ctx(&session);
    ctx.launch = Some(&launch);
    let bg = super::background(&service, &ctx, &foreground_task, Action::Poll).unwrap();
    assert!(bg.claim.is_claimed());
    assert!(pool.used() > 0);

    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let waiter_session = Arc::clone(&session);
    let waiter_pool = Arc::clone(&pool);
    let waiter = tokio::task::spawn_blocking(move || {
        ready_tx.send(()).unwrap();
        let next_claim = waiter_session.claim_outside();
        let used_at_acquire = waiter_pool.used();
        (next_claim.is_claimed(), used_at_acquire)
    });
    ready_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    drop(bg);
    let (claimed, used_at_acquire) = waiter.await.unwrap();
    assert!(claimed, "rollback must leave outside notice unconsumed");
    assert_eq!(
        used_at_acquire, 0,
        "workspace must return to pool before OutsideClaim unlocks"
    );
    let _ = outside_task.kill();
    tasks.shutdown();
}

#[tokio::test]
async fn list_showing_note_uses_kept_count_when_both_limit_and_max_bytes_truncate() {
    let (service, tasks, session) = script_fixture();
    for index in 0..4 {
        let code = format!("text('item-{index}-{}');", "x".repeat(80));
        let ((task, _, _), _) = crate::codemode::spawn_script(
            &tasks,
            crate::codemode::ScriptSpawnArgs {
                code,
                launch: Arc::clone(&session.launch),
                kill_after_ms: None,
                transfer: false,
                call_id: None,
                service: Arc::clone(&service),
                input_owner: Default::default(),
            },
            &session,
            format!("b{index:03}"),
        )
        .unwrap();
        tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
            .await
            .unwrap()
            .unwrap();
    }

    // `kept.len() == blocks.len()`: original wording stays verbatim.
    let full_limited = exec_agent_json(
        &service,
        json!({"action": "list", "limit": 3, "maxBytes": 4096}),
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await;
    let full_text = text_of(&full_limited.result);
    assert!(
        full_text.ends_with("Showing 3 of 4 tasks; raise limit to see more."),
        "{full_text}"
    );

    // `kept.len() < blocks.len()` and `total_count > kept.len()`: uses `kept.len()` and mentions `maxBytes` or `limit`.
    let narrow_limited = exec_agent_json(
        &service,
        json!({"action": "list", "limit": 3, "truncate": "head", "maxBytes": 240}),
        script_ctx(&session),
        AgentDelivery::default(),
    )
    .await;
    let narrow_text = text_of(&narrow_limited.result);
    assert!(
        narrow_text.contains("tasks omitted; raise maxBytes"),
        "{narrow_text}"
    );
    assert!(
        narrow_text.ends_with("Showing 1 of 4 tasks; raise maxBytes or limit to see more."),
        "{narrow_text}"
    );

    // Same dual truncation when the kept top-level task also has folded codemode children.
    let all = tasks.all();
    let make_folded = |max_bytes: usize| {
        let items = all[..3]
            .iter()
            .enumerate()
            .map(|(idx, task)| crate::tasks::ListedTask {
                task: Arc::clone(task),
                snapshot: task.snapshot(),
                parent_task_id: None,
                children: (idx == 0).then(|| ChildSummary {
                    total: 1,
                    done: 1,
                    ..Default::default()
                }),
            })
            .collect();
        PreparedExec::List(PreparedList {
            workspace: Lease::empty(session.launch.memory()),
            permit: None,
            total_count: 5,
            items,
            parent_filter: None,
            state_filter: None,
            view: crate::service::schema::View {
                inline_markers: false,
                keep_nul: false,
                truncate: Truncate::Head,
                max_bytes,
                raw: false,
                encoding: None,
                screen: false,
                conditions: false,
            },
        })
    };

    let folded_full = finish_exec_agent(
        &service,
        Ok(make_folded(4096)),
        &script_ctx(&session),
        &AgentDelivery::default(),
    )
    .await;
    let folded_full_text = text_of(&folded_full.result);
    assert!(
        folded_full_text.ends_with(
            "Showing 3 of 5 top-level tasks (1 folded children; pass parentTaskId to inspect)."
        ),
        "{folded_full_text}"
    );

    let folded_narrow = finish_exec_agent(
        &service,
        Ok(make_folded(240)),
        &script_ctx(&session),
        &AgentDelivery::default(),
    )
    .await;
    let folded_narrow_text = text_of(&folded_narrow.result);
    assert!(
        folded_narrow_text.ends_with(
            "Showing 1 of 5 top-level tasks (1 folded children; raise maxBytes or limit, or pass parentTaskId to inspect)."
        ),
        "{folded_narrow_text}"
    );

    tasks.shutdown();
}
