use super::render::*;
use super::schema::*;
use super::*;
use crate::session::TaskEntry;
use crate::tasks::state::Snapshot;
use crate::term::output::{Cleaner, Window, WindowBuilder, WindowSpec};
use base64::Engine;
use tokio::sync::Semaphore;

fn script_fixture() -> (Arc<Service>, Arc<Tasks>, Arc<Session>) {
    let tasks = Tasks::new(tasks::Limits::OFF).unwrap();
    let service = Arc::new(Service::new(Arc::clone(&tasks), false));
    let launch = Arc::new(crate::session::LaunchContext::new(
        std::env::temp_dir(),
        Vec::new(),
    ));
    let session = Session::new("script-test".into(), "script-test".into(), launch);
    (service, tasks, session)
}

fn script_call(session: &Arc<Session>, nested: bool) -> Call<'_> {
    Call {
        session,
        launch: None,
        render: Render {
            output_mode: OutputMode::Both,
            structured_content: true,
        },
        cancel: CancellationToken::new(),
        progress: None,
        call_id: None,
        nested,
        started: None,
        queued_guard: None,
        render_slot: None,
        http_response: false,
        rpc_id_bytes: 0,
        input_owner: Default::default(),
    }
}

async fn list_fixture() -> (Arc<Service>, Arc<Tasks>, Arc<Session>, Arc<TaskEntry>) {
    let (service, tasks, session) = script_fixture();
    let (task, entry, _) = tasks
        .start_script(
            tasks::ScriptArgs {
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

#[tokio::test]
async fn background_metadata_keeps_owner_and_claim_until_commit_and_only_kill_uses_control() {
    let (service, tasks, session, noticed_entry) = list_fixture().await;
    let noticed = tasks.all().pop().unwrap();
    let (task, entry, reader) = tasks
        .start_script(
            tasks::ScriptArgs {
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
    let call = script_call(&session, false);
    let probe = service
        .background(&call, &task, false, Action::Poll)
        .unwrap();
    assert!(probe.value.as_ref().unwrap().0.contains(&noticed.id));
    let required = probe._workspace.bytes();
    drop(probe);
    for limit in [required - 1, required, required + 1] {
        let memory = crate::lease::BytePool::new(limit);
        let control = crate::lease::BytePool::new(required);
        let launch = Arc::new(
            session
                .launch
                .for_session()
                .with_memory(Arc::clone(&memory))
                .with_control_memory(Arc::clone(&control)),
        );
        let mut call = script_call(&session, false);
        call.launch = Some(&launch);
        let notice = service.background(&call, &task, false, Action::Poll);
        assert_eq!(notice.is_ok(), limit >= required);
        assert_eq!(control.used(), 0);
        if limit >= required {
            assert_eq!(memory.used(), required);
        } else {
            assert!(
                matches!(&notice, Err(ServiceError::Overload(_, Some(meta))) if meta.bare_task_id == task.id)
            );
        }
        drop(notice);
        assert_eq!(memory.used(), 0);
        assert!(!noticed_entry.is_reported());
        let claim = session.claim_outside();
        assert!(claim.is_claimed());
        drop(claim);
    }
    let memory = crate::lease::BytePool::new(0);
    let control = crate::lease::BytePool::new(required);
    let launch = Arc::new(
        session
            .launch
            .for_session()
            .with_memory(Arc::clone(&memory))
            .with_control_memory(Arc::clone(&control)),
    );
    let mut call = script_call(&session, false);
    call.launch = Some(&launch);
    for action in [Action::Start, Action::Poll, Action::Transcript] {
        let error = service
            .background(&call, &task, false, action)
            .err()
            .unwrap();
        assert!(error.contains("Do not retry start or resend input"));
        assert_eq!((memory.used(), control.used()), (0, 0));
    }
    let notice = service
        .background(&call, &task, false, Action::Kill)
        .unwrap();
    assert_eq!(control.used(), required);
    assert!(!noticed_entry.is_reported());
    drop(notice);
    assert_eq!(control.used(), 0);
    assert!(!noticed_entry.is_reported());
    let notice = service
        .background(&call, &task, false, Action::Kill)
        .unwrap();
    report(
        &session,
        &entry,
        &reader,
        &task.snapshot(),
        notice,
        false,
        false,
    );
    assert!(noticed_entry.is_reported());
    assert!(!session.claim_outside().is_claimed());
    assert_eq!((memory.used(), control.used()), (0, 0));
    tasks.shutdown();
}

#[tokio::test]
async fn response_snapshot_owns_its_workspace_and_uses_control_only_for_kill() {
    let (_service, tasks, session, _entry) = list_fixture().await;
    let task = tasks.all().pop().unwrap();
    let probe = RenderSnapshot::new(&task, &script_call(&session, false), Action::Poll).unwrap();
    let required = probe.workspace.bytes();
    assert!(required > 0);
    drop(probe);
    for limit in [required - 1, required, required + 1] {
        let memory = crate::lease::BytePool::new(limit);
        let control = crate::lease::BytePool::new(required);
        let launch = Arc::new(
            session
                .launch
                .for_session()
                .with_memory(Arc::clone(&memory))
                .with_control_memory(Arc::clone(&control)),
        );
        let mut call = script_call(&session, false);
        call.launch = Some(&launch);
        let snapshot = RenderSnapshot::new(&task, &call, Action::Poll);
        assert_eq!(snapshot.is_ok(), limit >= required);
        assert_eq!(control.used(), 0);
        if limit >= required {
            assert_eq!(memory.used(), required);
        }
        drop(snapshot);
        assert_eq!(memory.used(), 0);
    }
    let memory = crate::lease::BytePool::new(0);
    let control = crate::lease::BytePool::new(required);
    let launch = Arc::new(
        session
            .launch
            .for_session()
            .with_memory(Arc::clone(&memory))
            .with_control_memory(Arc::clone(&control)),
    );
    let mut call = script_call(&session, false);
    call.launch = Some(&launch);
    for action in [Action::Start, Action::Poll, Action::Transcript] {
        let error = RenderSnapshot::new(&task, &call, action).err().unwrap();
        assert!(
            matches!(&error, ServiceError::Overload(_, Some(meta)) if meta.bare_task_id == task.id)
        );
        assert!(error.contains("Do not retry start or resend input"));
        assert_eq!((memory.used(), control.used()), (0, 0));
    }
    let snapshot = RenderSnapshot::new(&task, &call, Action::Kill).unwrap();
    assert_eq!(snapshot.value.state, "exited");
    assert_eq!(control.used(), required);
    drop(snapshot);
    assert_eq!((memory.used(), control.used()), (0, 0));
    tasks.shutdown();
}

#[test]
fn response_snapshot_cancellation_releases_metadata_but_keeps_queued_window_owned() {
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Wake, Waker};

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
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (service, tasks, session) = script_fixture();
        let (task, entry, _) = tasks
            .start_script(
                tasks::ScriptArgs {
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
        let mut call = script_call(&session, false);
        call.launch = Some(&launch);
        call.render_slot = Some(&slot);
        let mut operation = Box::pin(service.call_json_tracked(
            json!({"action":"poll", "taskId":reader.handle(&task.id), "waitMs":0, "maxBytes":100}),
            call,
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
async fn nested_precommit_exact_memory_quota_and_cancellation_keep_old_reservations() {
    let (_, tasks, session) = script_fixture();
    let result = CallToolResult::success(vec![ContentBlock::text("ordinary output")]);
    let wire = nested_wire_bytes(&result).unwrap();
    let peak = result_payload_capacity(&result) + wire;
    let control = crate::lease::BytePool::new(peak);
    for (limit, fits) in [(peak - 1, false), (peak, true)] {
        let pool = crate::lease::BytePool::new(limit);
        let launch = Arc::new(
            session
                .launch
                .for_session()
                .with_memory(Arc::clone(&pool))
                .with_control_memory(Arc::clone(&control)),
        );
        let mut call = script_call(&session, true);
        call.launch = Some(&launch);
        let admitted = admit_nested_result(&call, Action::Poll, &result, &mut None);
        assert_eq!(admitted.is_ok(), fits);
        assert_eq!(control.used(), 0, "ordinary replies do not use control");
        if fits {
            assert_eq!(pool.used(), peak);
        }
        drop(admitted);
        assert_eq!(pool.used(), 0);
    }
    let pool = crate::lease::BytePool::new(peak);
    let launch = Arc::new(
        session
            .launch
            .for_session()
            .with_memory(Arc::clone(&pool))
            .with_control_memory(Arc::clone(&control)),
    );
    let (script, worker) = codemode::prepare(tokio::runtime::Handle::current()).unwrap();
    script.test_pending_result(7, 8, wire - 1);
    let mut call = script_call(&session, true);
    call.launch = Some(&launch);
    call.started = Some(codemode::control::NestedCall {
        script: &script,
        pending_id: 7,
        parent_task_id: "aaaaaaaa-0",
    });
    let mut held = pool.try_acquire(16);
    assert!(admit_nested_result(&call, Action::Poll, &result, &mut held).is_err());
    assert_eq!(held.as_ref().unwrap().bytes(), 16);
    assert_eq!((pool.used(), script.test_bridge_bytes()), (16, 8));
    script.test_bridge_limit(wire);
    call.cancel.cancel();
    assert!(admit_nested_result(&call, Action::Poll, &result, &mut held).is_err());
    assert_eq!((pool.used(), script.test_bridge_bytes()), (16, 8));
    call.cancel = CancellationToken::new();
    let admitted = admit_nested_result(&call, Action::Poll, &result, &mut held).unwrap();
    assert!(held.is_none());
    assert_eq!((pool.used(), script.test_bridge_bytes()), (peak, wire));
    drop(admitted);
    assert_eq!(pool.used(), 0);
    let full = crate::lease::BytePool::new(16);
    let launch = Arc::new(
        session
            .launch
            .for_session()
            .with_memory(Arc::clone(&full))
            .with_control_memory(Arc::clone(&control)),
    );
    let mut call = script_call(&session, true);
    call.launch = Some(&launch);
    let mut held = full.try_acquire(16);
    assert!(admit_nested_result(&call, Action::Poll, &result, &mut held).is_err());
    assert_eq!((full.used(), control.used()), (16, 0));
    let admitted = admit_nested_result(&call, Action::Kill, &result, &mut held).unwrap();
    assert!(held.is_none());
    assert_eq!((full.used(), control.used()), (0, peak));
    drop(admitted);
    assert_eq!(control.used(), 0);
    drop(worker);
    tasks.shutdown();
}

#[tokio::test]
async fn nested_precommit_list_quota_refusal_and_cancel_leave_reported_state_due() {
    let (service, tasks, session, entry) = list_fixture().await;
    let (script, worker) = codemode::prepare(tokio::runtime::Handle::current()).unwrap();
    script.test_pending_result(1, 8, 64);
    let make_call = || {
        let mut call = script_call(&session, true);
        call.started = Some(codemode::control::NestedCall {
            script: &script,
            pending_id: 1,
            parent_task_id: "aaaaaaaa-0",
        });
        call
    };
    let refused = service
        .call_json_tracked(json!({"action":"list"}), make_call())
        .await;
    assert!(refused.overload, "{:?}", refused.result);
    assert!(!entry.is_reported());
    assert_eq!(script.test_bridge_bytes(), 8);
    drop(refused);
    script.test_bridge_limit(16 * 1024);
    let cancelled = make_call();
    cancelled.cancel.cancel();
    let refused = service
        .call_json_tracked(json!({"action":"list"}), cancelled)
        .await;
    assert_eq!(refused.result.is_error, Some(true));
    assert!(!entry.is_reported());
    assert_eq!(script.test_bridge_bytes(), 8);
    let delivered = service
        .call_json_tracked(json!({"action":"list"}), make_call())
        .await;
    assert_eq!(
        delivered.result.is_error,
        Some(false),
        "{:?}",
        delivered.result
    );
    assert!(entry.is_reported());
    let wire = nested_wire_bytes(&delivered.result).unwrap();
    assert_eq!(script.test_bridge_bytes(), wire);
    assert!(delivered.lease.as_ref().unwrap().bytes() >= wire);
    drop(delivered);
    drop(worker);
    tasks.shutdown();
}

#[tokio::test]
async fn nested_precommit_reader_and_attachment_watermarks_survive_refusal_and_cancel() {
    let tasks = Tasks::new(tasks::Limits::OFF).unwrap();
    let service = Arc::new(Service::new(Arc::clone(&tasks), false));
    let pool = crate::lease::BytePool::new(2 << 20);
    let launch = Arc::new(
        crate::session::LaunchContext::new(std::env::temp_dir(), Vec::new())
            .with_memory(Arc::clone(&pool)),
    );
    let session = Session::new(
        "nested-commit".into(),
        "nested-commit".into(),
        Arc::clone(&launch),
    );
    let (task, entry, _) = tasks
        .start_script(
            tasks::ScriptArgs {
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
    let reader = entry.reader(Some("9999")).unwrap();
    let handle = format!("{}.9999", task.id);
    let (script, worker) = codemode::prepare(tokio::runtime::Handle::current()).unwrap();
    script.record_started(handle.clone());
    script.test_pending_result(1, 8, 64);
    let make_call = || {
        let mut call = script_call(&session, true);
        call.render.output_mode = OutputMode::Structured;
        call.started = Some(codemode::control::NestedCall {
            script: &script,
            pending_id: 1,
            parent_task_id: "aaaaaaaa-0",
        });
        call
    };
    let request = json!({"action":"poll", "taskId":handle, "waitMs":0, "maxBytes":100});
    let refused = service
        .call_json_tracked(request.clone(), make_call())
        .await;
    assert!(refused.overload, "{:?}", refused.result);
    let meta = refused.task_meta.as_ref().unwrap();
    assert_eq!(meta.bare_task_id, task.id);
    let text = refused.result.structured_content.as_ref().unwrap()["error"]
        .as_str()
        .unwrap();
    assert!(text.contains(&task.id) && text.contains(&meta.log_path));
    assert!(text.contains("Do not retry start or resend input"));
    assert_eq!(script.test_bridge_bytes(), 8);
    assert_eq!(script.test_attachment_count(), 0);
    let report = script.report(&pool, None).unwrap();
    assert!(
        report.started.contains(&handle),
        "started task is recoverable from the outer report"
    );
    drop(report);
    {
        let state = reader.read_gate().lock_owned().await;
        assert_eq!((state.cursor, state.attachments_seen), (0, 0));
        assert!(reader.is_fresh());
    }
    assert!(!entry.is_reported());
    drop(refused);
    assert_eq!(pool.used(), 0);
    let cancelled = make_call();
    cancelled.cancel.cancel();
    let refused = service.call_json_tracked(request.clone(), cancelled).await;
    assert_eq!(refused.result.is_error, Some(true));
    assert_eq!(script.test_bridge_bytes(), 8);
    assert_eq!(script.test_attachment_count(), 0);
    assert!(reader.is_fresh());
    drop(refused);
    script.test_bridge_limit(16 * 1024);
    let delivered = service.call_json_tracked(request, make_call()).await;
    assert_eq!(
        delivered.result.is_error,
        Some(false),
        "{:?}",
        delivered.result
    );
    let wire = nested_wire_bytes(&delivered.result).unwrap();
    assert_eq!(script.test_bridge_bytes(), wire);
    assert!(delivered.lease.as_ref().unwrap().bytes() >= wire);
    assert_eq!(script.test_attachment_count(), 1);
    let attachment =
        &delivered.result.structured_content.as_ref().unwrap()["codemode"]["attachments"][0];
    assert!(
        attachment["__hid"].as_u64().unwrap() >= 2,
        "refused registrations were removed"
    );
    {
        let state = reader.read_gate().lock_owned().await;
        assert!(state.cursor > 0);
        assert_eq!(state.attachments_seen, 1);
        assert!(!reader.is_fresh());
    }
    assert!(entry.is_reported());
    drop(delivered);
    assert_eq!(pool.used(), 0);
    drop(worker);
    tasks.shutdown();
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
    let mut call = script_call(&session, false);
    call.launch = Some(&blocked);
    let refused = service
        .call_json_tracked(json!({"action":"list"}), call)
        .await;
    assert!(refused.overload);
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
            .with_outbound_limit(512),
    );
    let mut call = script_call(&session, false);
    call.launch = Some(&small_frame);
    let refused = service
        .call_json_tracked(json!({"action":"list"}), call)
        .await;
    assert!(refused.overload, "{:?}", refused.result);
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

    let mut cancelled = script_call(&session, false);
    cancelled.cancel.cancel();
    cancelled.launch = Some(&small_frame);
    let refused = service
        .call_json_tracked(json!({"action":"list"}), cancelled)
        .await;
    assert_eq!(refused.result.is_error, Some(true));
    assert!(!entry.is_reported());
    assert_eq!(pool.used(), 0);
    tasks.shutdown();
}

#[tokio::test]
async fn list_result_owns_payload_and_only_local_wire_after_admission() {
    for mode in 0..4 {
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
        let mut call = script_call(&session, mode == 3);
        call.launch = Some(&launch);
        call.http_response = mode == 1;
        if mode == 2 {
            call.render_slot = Some(&slot);
            call.call_id = Some(7);
        }
        let mut result = service
            .call_json_tracked(json!({"action":"list"}), call)
            .await;
        assert_eq!(result.result.is_error, Some(false), "{:?}", result.result);
        assert!(entry.is_reported());
        let payload = result_payload_capacity(&result.result);
        assert!(payload > 0);
        assert_eq!(
            unleased_result_payload_capacity(&result.result, &result.payload_owner),
            0
        );
        let wire = result.lease.as_ref().map_or(0, Lease::bytes);
        assert_eq!(wire == 0, mode == 1);
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
    result.structured_content = Some(Value::String(copied("metadata", &mut expected)));
    assert_eq!(result_payload_capacity(&result), expected);
    let pool = crate::lease::BytePool::new(expected);
    let retained = expected / 2;
    let result = files::TrackedResult::owned(
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
            crate::session::LaunchContext::new(std::env::temp_dir(), Vec::new())
                .with_memory(Arc::clone(&pool))
                .with_outbound_limit(2048),
        );
        let session = Session::new("rpc-id".into(), "rpc-id".into(), Arc::clone(&launch));
        let (task, _, _) = tasks
            .start_script(
                tasks::ScriptArgs {
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
        let mut call = script_call(&session, false);
        call.rpc_id_bytes = crate::ipc::json_len(&"r".repeat(2048)).unwrap();
        if daemon {
            call.call_id = Some(u64::MAX);
            call.render_slot = Some(&slot);
        }
        let request = json!({
            "action":"poll", "taskId":format!("{}.9999", task.id),
            "waitMs":0, "maxBytes":100,
        });
        let refused = service.call_json_tracked(request.clone(), call).await;
        assert!(refused.overload, "{:?}", refused.result);
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
        let mut call = script_call(&session, false);
        call.rpc_id_bytes = crate::ipc::json_len(&"small").unwrap();
        if daemon {
            call.call_id = Some(u64::MAX);
            call.render_slot = Some(&slot);
        }
        let delivered = service.call_json_tracked(request, call).await;
        assert_eq!(
            delivered.result.is_error,
            Some(false),
            "{:?}",
            delivered.result
        );
        assert_eq!(image_count(&delivered.result), 1);
        {
            let state = reader.read_gate().lock_owned().await;
            assert!(state.cursor > 0);
            assert_eq!(state.attachments_seen, 1);
        }
        if daemon {
            let local = wire_result_len(Some(u64::MAX), &delivered.result).unwrap();
            assert!(delivered.lease.as_ref().unwrap().bytes() >= local);
        }
        drop(delivered);
        assert_eq!(pool.used(), 0);
        tasks.shutdown();
    }
}

#[tokio::test]
async fn script_report_uses_control_only_for_kill_and_keeps_one_snapshot() {
    let tasks = Tasks::new(tasks::Limits::OFF).unwrap();
    let service = Arc::new(Service::new(Arc::clone(&tasks), false));
    let memory = crate::lease::BytePool::new(0);
    let control = crate::lease::BytePool::new(4 << 20);
    let launch = Arc::new(
        crate::session::LaunchContext::new(std::env::temp_dir(), Vec::new())
            .with_memory(Arc::clone(&memory))
            .with_control_memory(Arc::clone(&control)),
    );
    let session = Session::new("report".into(), "report".into(), Arc::clone(&launch));
    let (task, _, _) = tasks
        .start_script(
            tasks::ScriptArgs {
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
    let call = script_call(&session, false);
    for action in [Action::Start, Action::Poll] {
        let error = script_report(&tasks, &task, &call, action).err().unwrap();
        assert!(matches!(
            error,
            ServiceError::Overload(_, Some(meta)) if meta.bare_task_id == task.id
        ));
        assert_eq!(control.used(), 0);
    }
    let report = script_report(&tasks, &task, &call, Action::Kill)
        .unwrap()
        .unwrap();
    assert_eq!(report.value["startedTasks"][0]["taskId"], handle);
    assert_eq!(report.value["startedTasks"][0]["state"], "exited");
    assert!(report.running.is_empty());
    assert_eq!(report.cancelled, 0);
    let held = control.used();
    assert!(held > 0);
    let candidate = report.value.clone();
    task.script()
        .unwrap()
        .record_started("task-other.abcd".into());
    assert_eq!(report.value["startedTasks"].as_array().unwrap().len(), 1);
    assert_eq!(candidate["startedTasks"].as_array().unwrap().len(), 1);
    assert_eq!(control.used(), held);
    drop(candidate);
    drop(report);
    assert_eq!(control.used(), 0);

    let result = service
        .call_json_tracked(
            json!({"action": "kill", "taskId": task.id, "maxBytes": 100}),
            script_call(&session, false),
        )
        .await;
    assert_eq!(result.result.is_error, Some(false), "{:?}", result.result);
    assert_eq!(
        result.result.structured_content.as_ref().unwrap()["codemode"]["startedTasks"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
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
async fn nested_text_result_keeps_its_payload_owner_after_rendering() {
    let tasks = Tasks::new(tasks::Limits::OFF).unwrap();
    let service = Arc::new(Service::new(Arc::clone(&tasks), false));
    let pool = crate::lease::BytePool::new(2 << 20);
    let launch = Arc::new(
        crate::session::LaunchContext::new(std::env::temp_dir(), Vec::new())
            .with_memory(Arc::clone(&pool)),
    );
    let session = Session::new("text-owner".into(), "text-owner".into(), launch);
    let mut result = service
        .codemode_tracked(
            serde_json::from_value(json!({"code": "console.log('hello 世界');", "maxBytes": 100}))
                .unwrap(),
            script_call(&session, true),
            false,
        )
        .await;
    assert_eq!(result.result.is_error, Some(false), "{:?}", result.result);
    let retained = result_payload_capacity(&result.result);
    assert!(retained > 0);
    let wire = result.lease.as_ref().unwrap().bytes();
    assert!(wire >= nested_wire_bytes(&result.result).unwrap());
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
    use std::os::unix::{ffi::OsStrExt, fs::OpenOptionsExt};
    let tasks = Tasks::new(tasks::Limits::OFF).unwrap();
    let service = Arc::new(Service::new(Arc::clone(&tasks), false));
    let pool = crate::lease::BytePool::new(1 << 20);
    let launch = Arc::new(
        crate::session::LaunchContext::new(std::env::temp_dir(), Vec::new())
            .with_memory(Arc::clone(&pool)),
    );
    let session = Session::new("pack-cancel".into(), "pack-cancel".into(), launch);
    let result = service
        .codemode(
            serde_json::from_value(
                json!({"code": format!("attach({})", json!(pixel_url())), "maxBytes": 100}),
            )
            .unwrap(),
            script_call(&session, true),
            false,
        )
        .await;
    assert_eq!(result.is_error, Some(false), "{result:?}");
    let data = result.structured_content.as_ref().unwrap();
    let (id, _) = parse_handle(data["taskId"].as_str().unwrap()).unwrap();
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
        let call = script_call(&session, false);
        let mut metadata = RenderSnapshot::new(&task, &call, Action::Poll).unwrap();
        metadata.refresh(&task, &call, Action::Poll).unwrap();
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
        let mut call = script_call(&running_session, false);
        call.cancel = cancelled;
        running_service.call_json(request, call).await
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
    for mode in [OutputMode::Both, OutputMode::Structured] {
        let mut call = script_call(&session, false);
        call.render.output_mode = mode;
        let result = service
            .codemode(
                serde_json::from_value(json!({
                    "code": code, "maxBytes": 100, "truncate": "tail",
                }))
                .unwrap(),
                call,
                false,
            )
            .await;
        assert_eq!(result.is_error, Some(false), "{result:?}");
        assert_eq!(image_count(&result), 6, "{result:?}");
        let data = result.structured_content.unwrap();
        let delivered = data["codemode"]["attachmentsDelivered"].as_array().unwrap();
        assert_eq!(delivered.len(), 6);
        for (idx, item) in delivered.iter().enumerate() {
            assert_eq!(item["index"], idx + 1);
            assert_eq!(item["kind"], "image");
            assert_eq!(item["mimeType"], "image/png");
        }
        let handle = data["taskId"].as_str().unwrap();
        let empty = service
            .call_json(
                json!({"action":"poll","taskId":handle,"waitMs":0}),
                script_call(&session, false),
            )
            .await;
        assert_eq!(image_count(&empty), 0);
        let bare = parse_handle(handle).unwrap().0;
        let reread = service
            .call_json(
                json!({"action":"poll","taskId":bare,"waitMs":0}),
                script_call(&session, false),
            )
            .await;
        assert_eq!(image_count(&reread), 6);
        assert_eq!(
            reread.structured_content.unwrap()["codemode"]["attachmentsDelivered"]
                .as_array()
                .unwrap()
                .len(),
            6
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
        crate::session::LaunchContext::new(std::env::temp_dir(), Vec::new())
            .with_outbound_limit(4600),
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
    let first = service
        .codemode(
            serde_json::from_value(json!({"code": code})).unwrap(),
            script_call(&session, false),
            false,
        )
        .await;
    assert_eq!(first.is_error, Some(false), "{first:?}");
    assert_eq!(image_count(&first), 1);
    let sc1 = first.structured_content.unwrap();
    assert_eq!(sc1["state"], "exited");
    assert_eq!(sc1["output"], "done-line");
    let del1 = sc1["codemode"]["attachmentsDelivered"].as_array().unwrap();
    assert_eq!(del1.len(), 1);
    assert_eq!(del1[0]["index"], 1);
    let handle = sc1["taskId"].as_str().unwrap().to_owned();

    // Text is ALREADY at EOF: inputless poll #1 delivers identical attachment #2!
    let second = service
        .call_json(
            json!({"action": "poll", "taskId": handle, "waitMs": 0}),
            script_call(&session, false),
        )
        .await;
    assert_eq!(second.is_error, Some(false), "{second:?}");
    assert_eq!(image_count(&second), 1);
    let sc2 = second.structured_content.unwrap();
    let del2 = sc2["codemode"]["attachmentsDelivered"].as_array().unwrap();
    assert_eq!(del2.len(), 1);
    assert_eq!(del2[0]["index"], 2);

    // Inputless poll #2: attachment #3 exceeds the 4600-byte outbound frame limit on its own,
    // so it emits a visible degradation receipt naming #3 and advances straight through to #4!
    let third = service
        .call_json(
            json!({"action": "poll", "taskId": handle, "waitMs": 0}),
            script_call(&session, false),
        )
        .await;
    assert_eq!(third.is_error, Some(false), "{third:?}");
    assert_eq!(image_count(&third), 1);
    let sc3 = third.structured_content.unwrap();
    let del3 = sc3["codemode"]["attachmentsDelivered"].as_array().unwrap();
    assert_eq!(del3.len(), 1);
    assert_eq!(del3[0]["index"], 4);
    let all_warnings: Vec<String> = [&sc1, &sc2, &sc3]
        .into_iter()
        .filter_map(|sc| sc["warnings"].as_array())
        .flatten()
        .filter_map(|w| w.as_str().map(str::to_owned))
        .collect();
    assert_eq!(
        all_warnings
            .iter()
            .filter(|w| w.contains("attachment #3") && w.contains("exceeds"))
            .count(),
        1,
        "{all_warnings:?}"
    );

    // Inputless poll #3: nothing left.
    let fourth = service
        .call_json(
            json!({"action": "poll", "taskId": handle, "waitMs": 0}),
            script_call(&session, false),
        )
        .await;
    assert_eq!(image_count(&fourth), 0);

    // Re-attach via bare taskId with `structured_content: false`: visible `Attached: #<index>` summary!
    let bare = parse_handle(&handle).unwrap().0;
    let plain_render = Render {
        output_mode: OutputMode::Text,
        structured_content: false,
    };
    let make_plain_call = || Call {
        session: &session,
        launch: None,
        render: plain_render,
        cancel: CancellationToken::new(),
        progress: None,
        call_id: None,
        nested: false,
        started: None,
        queued_guard: None,
        render_slot: None,
        http_response: false,
        rpc_id_bytes: 0,
        input_owner: Default::default(),
    };
    let p1 = service
        .call_json(
            json!({"action": "poll", "taskId": bare, "waitMs": 0}),
            make_plain_call(),
        )
        .await;
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
    let changed = service
        .call_json(fresh_req.clone(), script_call(&session, false))
        .await;
    assert_eq!(changed.is_error, Some(true));
    assert!(
        serde_json::to_string(&changed.content)
            .unwrap()
            .contains("attachment size changed")
    );
    std::fs::write(&att1_path, &medium_png).unwrap();
    let recovered = service
        .call_json(fresh_req, script_call(&session, false))
        .await;
    assert_eq!(image_count(&recovered), 1);
    tasks.shutdown();
}

#[tokio::test]
async fn nested_task_reads_consume_reader_state_without_consuming_session_notices() {
    let (service, tasks, session) = script_fixture();
    let result = service
        .codemode(
            serde_json::from_value(json!({"code":"return 7"})).unwrap(),
            script_call(&session, true),
            false,
        )
        .await;
    let data = result.structured_content.unwrap();
    let handle = data["taskId"].as_str().unwrap();
    let (id, key) = parse_handle(handle).unwrap();
    let task = tasks.find(id).unwrap();
    let entry = session.entry(&task);
    assert!(entry.reader(key).unwrap().is_reported());
    assert!(!entry.is_reported());
    service
        .call_json(
            json!({"action":"poll","taskId":handle,"waitMs":0}),
            script_call(&session, true),
        )
        .await;
    service
        .call_json(
            json!({"action":"kill","taskId":handle}),
            script_call(&session, true),
        )
        .await;
    assert!(!entry.is_reported());
    assert!(
        tasks
            .background(
                &session,
                "another-task",
                512,
                &mut |_| Ok(()),
                &mut |_| panic!("script has no native members")
            )
            .unwrap()
            .0
            .is_some()
    );
    service
        .codemode(
            serde_json::from_value(json!({"code":"return 8"})).unwrap(),
            script_call(&session, false),
            false,
        )
        .await;
    assert!(entry.is_reported());
    tasks.shutdown();
}

#[tokio::test]
async fn nested_list_marks_only_delivered_session_tasks() {
    let (service, tasks, session) = script_fixture();
    let mut entries = Vec::new();
    for code in ["return 7", "return 8"] {
        let result = service
            .codemode(
                serde_json::from_value(json!({"code":code})).unwrap(),
                script_call(&session, true),
                false,
            )
            .await;
        let data = result.structured_content.unwrap();
        let (id, _) = parse_handle(data["taskId"].as_str().unwrap()).unwrap();
        let task = tasks.find(id).unwrap();
        entries.push(session.entry(&task));
    }
    let listed = service
        .call_json(
            json!({"action":"list","limit":1,"maxBytes":100}),
            script_call(&session, true),
        )
        .await;
    assert_eq!(
        listed.structured_content.unwrap()["tasks"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert!(!entries[0].is_reported(), "an unselected task stays due");
    assert!(entries[1].is_reported(), "the delivered task is reported");
    tasks.shutdown();
}

#[tokio::test]
async fn script_poll_rejects_presence_even_when_false_or_empty() {
    let (service, tasks, session) = script_fixture();
    let result = service
        .codemode(
            serde_json::from_value(json!({"code":"return 1"})).unwrap(),
            script_call(&session, false),
            false,
        )
        .await;
    let data = result.structured_content.unwrap();
    for accepted in [
        json!({"eof":false,"screen":false,"raw":false}),
        json!({"input":null,"eof":null,"keys":null,"screen":null,"raw":null,"encoding":null}),
        json!({"keys":[]}),
        json!({"keys":[],"keyDelayMs":150}),
    ] {
        let mut request = json!({"action":"poll","taskId":data["taskId"],"waitMs":0});
        request
            .as_object_mut()
            .unwrap()
            .extend(accepted.as_object().unwrap().clone());
        let ok = service
            .call_json(request, script_call(&session, false))
            .await;
        assert_eq!(ok.is_error, Some(false), "{ok:?}");
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
        let mut request = json!({"action":"poll","taskId":data["taskId"],"waitMs":0});
        request
            .as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        let result = service
            .call_json(request, script_call(&session, false))
            .await;
        assert_eq!(result.is_error, Some(true), "{result:?}");
        assert!(
            serde_json::to_string(&result.content)
                .unwrap()
                .contains("A codemode task takes no input")
        );
    }
    let result = service
        .call_json(
            json!({"action":"transcript","taskId":data["taskId"]}),
            script_call(&session, false),
        )
        .await;
    assert!(
        serde_json::to_string(&result.content)
            .unwrap()
            .contains("has no terminal")
    );
    tasks.shutdown();
}

#[tokio::test]
async fn critical_warnings_survive_small_budgets_and_every_render_mode() {
    let service = Service::new(Tasks::new(tasks::Limits::OFF).unwrap(), false);
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
        kill_error: None,
    };
    assert!(output_incomplete(&snapshot));
    let warnings = task_warnings(&snapshot);
    assert_eq!(warnings.len(), 6);
    for mode in [OutputMode::Text, OutputMode::Both, OutputMode::Structured] {
        for structured_content in [false, true] {
            for raw in [false, true] {
                let render = Render {
                    output_mode: mode,
                    structured_content,
                }
                .effective();
                let view = View {
                    nested: false,
                    owned_by_script: false,
                    truncate: Truncate::HeadTail,
                    max_bytes: 100,
                    raw,
                    encoding: None,
                    screen: false,
                    conditions: false,
                };
                let mut cleaner = Cleaner::default();
                let mut builder = WindowBuilder::new(
                    WindowSpec {
                        truncate: view.truncate,
                        budget: view.max_bytes,
                        raw,
                        clean: true,
                        keep_nul: false,
                        include_markers: true,
                        encoding: encoding_rs::UTF_8,
                        source: "log",
                    },
                    &mut cleaner,
                    1,
                );
                builder.push(b"captured prefix\n");
                let window = builder.finish();
                let mut structured = json!({ "ok": task_io_ok(&snapshot), "warnings": warnings,
                    "exitCode": snapshot.exit_code, "outputIncomplete": output_incomplete(&snapshot) });
                let warnings_text = warning_text(&structured["warnings"]);
                let text = service.layout(
                    render,
                    &window,
                    format!("[exited 0] task.reader · log {}", "x".repeat(200)),
                    "[exited 0] task.reader".to_owned(),
                    &snapshot,
                    false,
                    "",
                    view.max_bytes,
                    "(no new output)",
                    &mut structured,
                );
                let result = suppress(success(text, structured, &warnings_text), render);
                let value = serde_json::to_value(result).unwrap();
                assert_eq!(value["isError"], true);
                let text = value["content"][0]["text"].as_str().unwrap();
                assert!(text.find("[exited 0]").unwrap() < text.find("Warning:").unwrap());
                for warning in &warnings {
                    assert!(text.contains(&format!("Warning: {warning}")), "{text}");
                }
                if structured_content {
                    assert_eq!(value["structuredContent"]["exitCode"], 0);
                    assert_eq!(value["structuredContent"]["outputIncomplete"], true);
                    assert_eq!(value["structuredContent"]["warnings"], json!(warnings));
                } else {
                    assert!(value.get("structuredContent").is_none());
                }
            }
        }
    }
    service.tasks.shutdown();
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
        service
            .codemode(
                serde_json::from_value(json!({"code":"while (true) {}","waitMs":0})).unwrap(),
                script_call(&owner, false),
                false,
            )
            .await;
        let completed = service
            .codemode(
                serde_json::from_value(json!({"code":"return 'ready'"})).unwrap(),
                script_call(&owner, false),
                false,
            )
            .await;
        let data = completed.structured_content.unwrap();
        let bare = parse_handle(data["taskId"].as_str().unwrap()).unwrap().0;
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
            let mut call = script_call(&session, false);
            call.cancel = cancel.clone();
            let mut earlier = Box::pin(service.call_json(request.clone(), call));
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
            let first = service
                .call_json(request.clone(), script_call(&session, false))
                .await;
            let text = serde_json::to_value(&first.content).unwrap()[0]["text"]
                .as_str()
                .unwrap()
                .to_owned();
            assert!(
                text.contains("1 task running outside this session"),
                "the first committed render must carry the count: {text}"
            );
            if cancel_earlier {
                cancel.cancel();
            }
            let later = earlier.await;
            let text = serde_json::to_value(&later.content).unwrap()[0]["text"]
                .as_str()
                .unwrap()
                .to_owned();
            assert!(!text.contains("running outside this session"), "{text}");
            assert_eq!(later.is_error, Some(cancel_earlier), "{later:?}");
        }
        tasks.shutdown();
    });
}

#[tokio::test]
async fn cancellation_during_blocking_render_keeps_reader_fresh_and_gate_pinned() {
    let entry = Arc::new(TaskEntry::default());
    let reader = entry.reader(None).unwrap();
    fn read_payload(state: &mut ReadState) -> Window {
        let bytes = b"prefix\n\x1b[";
        let mut builder = WindowBuilder::new(
            WindowSpec {
                truncate: Truncate::Head,
                budget: 1024,
                raw: false,
                clean: true,
                keep_nul: false,
                include_markers: true,
                encoding: encoding_rs::UTF_8,
                source: "log",
            },
            &mut state.cleaner,
            state.lines_before + 1,
        );
        builder.push(&bytes[state.cursor as usize..]);
        let window = builder.finish();
        state.cursor = bytes.len() as u64;
        state.lines_before += window.newlines;
        window
    }

    let rendering = Arc::clone(&reader);
    let reporting = Arc::clone(&entry);
    let mut state = reader.read_gate().lock_owned().await;
    state.shown = (7, 2);
    let initial_cleaner = format!("{:?}", state.cleaner);
    let cancel = CancellationToken::new();
    let cancelled = cancel.clone();
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let call = tokio::spawn(async move {
        read_result(&cancelled, async move {
            let read = tokio::task::spawn_blocking(move || {
                let mut read = StagedRead::new(state);
                let window = read_payload(&mut read.state);
                assert_eq!(window.text, "prefix");
                read.state.shown = (77, 7);
                read.state.attachments_seen = 9;
                entered.send(()).unwrap();
                blocked.recv().unwrap();
                read
            })
            .await
            .unwrap();
            let state = read.commit();
            rendering.mark_delivered();
            let snapshot = Snapshot {
                state: "exited",
                exit_code: Some(0),
                forced_end: false,
                elapsed: Duration::ZERO,
                lines: 0,
                dropped: 0,
                evicted: false,
                output_open: false,
                expired: false,
                log_error: None,
                capture_error: None,
                input_warning: None,
                input_error: None,
                leftovers: Vec::new(),
                kept: false,
                tree_ended: true,
                held: Vec::new(),
                kill_error: None,
            };
            rendering.mark_reported(&snapshot);
            reporting.mark_reported(&snapshot);
            drop(state);
            Ok(success(String::new(), Value::Null, ""))
        })
        .await
    });
    started.await.unwrap();
    cancel.cancel();
    let result = tokio::time::timeout(Duration::from_secs(5), call)
        .await
        .unwrap()
        .unwrap();
    assert!(result.unwrap_err().contains("Cancelled"));
    assert!(reader.is_fresh());
    assert!(!reader.is_reported() && !entry.is_reported() && !entry.tree_end_reported());
    assert!(reader.read.try_lock().is_err());
    release.send(()).unwrap();
    let state = tokio::time::timeout(Duration::from_secs(5), reader.read_gate().lock_owned())
        .await
        .unwrap();
    assert_eq!(state.cursor, 0);
    assert_eq!(state.lines_before, 0);
    assert_eq!(state.shown, (7, 2));
    assert_eq!(state.attachments_seen, 0);
    assert_eq!(format!("{:?}", state.cleaner), initial_cleaner);
    assert!(reader.is_fresh());
    assert!(!reader.is_reported() && !entry.is_reported());
    let mut read = StagedRead::new(state);
    let output = read_payload(&mut read.state).text;
    assert_eq!(
        output, "prefix",
        "the retry must read and clean the whole original payload"
    );
    read_result(&CancellationToken::new(), async {
        let state = read.commit();
        reader.mark_delivered();
        drop(state);
        Ok(success(output, Value::Null, ""))
    })
    .await
    .unwrap();
    let state = reader.read_gate().lock_owned().await;
    assert_eq!(state.cursor, b"prefix\n\x1b[".len() as u64);
    assert_eq!(state.lines_before, 1);
    assert_eq!(state.shown, (7, 2));
    assert!(!reader.is_fresh());
}

#[tokio::test]
async fn a_cancelled_token_wins_over_a_ready_result() {
    let reader = TaskEntry::default().reader(None).unwrap();
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = read_result(&cancel, async {
        reader.mark_delivered();
        Ok(success(String::new(), Value::Null, ""))
    })
    .await;
    assert!(result.is_err());
    assert!(reader.is_fresh());
}

#[test]
fn a_large_list_row_does_not_hide_the_opposite_end() {
    let block = |lines: Vec<String>| ListBlock {
        lines,
        delivered_tasks: Vec::new(),
        folded_finished: Vec::new(),
        folded_count: 0,
    };
    for (rows, want, omitted_at_start) in [
        (
            vec![block(vec!["x".repeat(200)]), block(vec!["short".into()])],
            1,
            true,
        ),
        (
            vec![block(vec!["short".into()]), block(vec!["x".repeat(200)])],
            0,
            false,
        ),
    ] {
        let (kept, text) = fit_lines(&rows, Truncate::HeadTail, 100);
        let indexes: Vec<usize> = kept.into_iter().map(|(index, _)| index).collect();
        assert_eq!(indexes, [want]);
        assert_eq!(text.starts_with("... [1 task"), omitted_at_start);
        assert!(text.contains("short"));
        assert!(text.len() <= 100);
    }
}

#[test]
fn fit_lines_keeps_parent_and_children_atomic_and_trims_single_block_bottom_up() {
    let block = |lines: Vec<String>| ListBlock {
        lines,
        delivered_tasks: Vec::new(),
        folded_finished: Vec::new(),
        folded_count: 0,
    };
    let blocks = vec![
        block(vec![
            "p1 [running] 1.0s · owner · [codemode] a".into(),
            "  └─ c1 [running] 0.5s · cmd1".into(),
            "  └─ c2 [running] 0.4s · cmd2".into(),
        ]),
        block(vec![
            "p2 [running] 1.0s · owner · [codemode] b".into(),
            "  └─ c3 [running] 0.5s · cmd3".into(),
        ]),
        block(vec!["p3 [exited 0] 0.2s · owner · echo ok".into()]),
    ];
    let (kept, text) = fit_lines(&blocks, Truncate::HeadTail, 215);
    assert!(text.len() <= 215, "{text}");
    assert!(
        text.contains("... [1 tasks omitted; raise maxBytes to see them] ..."),
        "{text}"
    );
    // Never emits an indented child without its parent header right above it.
    let mut saw_parent = false;
    for line in text.lines() {
        if line.starts_with("  └─") {
            assert!(saw_parent, "orphaned child line in:\n{text}");
        } else {
            saw_parent = !line.starts_with("... [");
        }
    }
    let indexes: Vec<usize> = kept.into_iter().map(|(index, _)| index).collect();
    assert_eq!(indexes, [0, 2]);

    // Single oversized block trims indented children from the bottom before cutting the parent.
    let single = vec![block(vec![
        "p1 [running] 1.0s · owner · [codemode] a".into(),
        "  └─ c1 [running] 0.5s · cmd1".into(),
        "  └─ c2 [running] 0.4s · cmd2".into(),
    ])];
    let (kept, text) = fit_lines(&single, Truncate::HeadTail, 80);
    assert_eq!(kept, [(0, 0)]);
    assert!(text.starts_with("p1 [running]"), "{text}");
    assert!(text.contains("c1 [running]"), "{text}");
    assert!(!text.contains("c2 [running]"), "{text}");
    assert!(text.len() <= 80, "{text}");
}

#[test]
fn screen_row_omission_keeps_and_cuts_the_bottom_row_when_it_exceeds_budget() {
    let single = vec!["bottom-".repeat(30)];
    let (body, omitted) = fit_screen_rows(&single, 100);
    assert_eq!(omitted, 0);
    assert!(body.starts_with("bottom-bottom-"), "{body}");
    assert!(body.ends_with(" … [cut]"), "{body}");
    assert!(body.len() <= 100);

    let multi = vec!["top-1".into(), "top-2".into(), "bottom-".repeat(30)];
    let (body, omitted) = fit_screen_rows(&multi, 100);
    assert_eq!(omitted, 2);
    assert!(
        body.starts_with("... [2 top rows omitted] ...\nbottom-"),
        "{body}"
    );
    assert!(body.ends_with(" … [cut]"), "{body}");
    assert!(body.len() <= 100);
}

#[tokio::test]
async fn fit_lines_trims_real_delivered_tasks_bottom_up_and_keeps_mark_reported_consistent() {
    let (service, tasks, session) = script_fixture();
    let mut created = Vec::new();
    for key in ["pr01", "at01", "ab01", "fp01", "fk01"] {
        let (task, entry, _) = tasks
            .start_script(
                tasks::ScriptArgs {
                    code: "return undefined;".into(),
                    launch: Arc::clone(&session.launch),
                    kill_after_ms: None,
                    transfer: false,
                    call_id: None,
                    service: Arc::clone(&service),
                    input_owner: Default::default(),
                },
                &session,
                key.into(),
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
            .await
            .unwrap()
            .unwrap();
        created.push((task, entry));
    }
    let mut created = created.into_iter();
    let (parent, parent_entry) = created.next().unwrap();
    let (active_top, active_top_entry) = created.next().unwrap();
    let (active_bottom, active_bottom_entry) = created.next().unwrap();
    let (folded_plain, folded_plain_entry) = created.next().unwrap();
    let (folded_kept, folded_kept_entry) = created.next().unwrap();

    let exited_snapshot = || Snapshot {
        state: "exited",
        exit_code: Some(0),
        forced_end: false,
        elapsed: Duration::from_millis(10),
        lines: 0,
        dropped: 0,
        evicted: false,
        output_open: false,
        expired: false,
        log_error: None,
        capture_error: None,
        input_warning: None,
        input_error: None,
        leftovers: Vec::new(),
        kept: true,
        tree_ended: false,
        held: Vec::new(),
        kill_error: None,
    };

    let parent_line = format!("{} [exited 0] 0.1s · owner · [codemode] parent", parent.id);
    let top_line = format!("  └─ {} [exited 0] 0.1s · keeps sleep · top", active_top.id);
    let bottom_line = format!(
        "  └─ {} [exited 0] 0.1s · keeps sleep · bottom-padding-0123456789",
        active_bottom.id
    );
    let budget = parent_line.len() + 1 + top_line.len();
    let blocks = vec![ListBlock {
        lines: vec![parent_line, top_line, bottom_line],
        delivered_tasks: vec![
            (Arc::clone(&parent), parent.snapshot()),
            (Arc::clone(&active_top), exited_snapshot()),
            (Arc::clone(&active_bottom), exited_snapshot()),
        ],
        folded_finished: vec![
            (Arc::clone(&folded_plain), false),
            (Arc::clone(&folded_kept), true),
        ],
        folded_count: 2,
    }];

    let (kept, text) = fit_lines(&blocks, Truncate::HeadTail, budget);
    assert_eq!(kept, vec![(0, 2)]);
    assert!(text.contains(&parent.id), "{text}");
    assert!(text.contains(&active_top.id), "{text}");
    assert!(!text.contains(&active_bottom.id), "{text}");

    // Mirror Service::list commit on surviving kept_tasks and folded_finished.
    for (block_index, kept_tasks) in kept {
        let block = &blocks[block_index];
        for (task, snapshot) in &block.delivered_tasks[..kept_tasks] {
            if let Some(entry) = session.existing(task) {
                entry.mark_reported(snapshot);
                entry.mark_keeps_reported(snapshot);
            }
        }
        for (child, tree_ended) in &block.folded_finished {
            if let Some(entry) = session.existing(child) {
                entry.mark_finished_reported(*tree_ended);
            }
        }
    }

    assert!(parent_entry.is_reported());
    assert!(active_top_entry.is_reported());
    assert!(!active_bottom_entry.is_reported());
    assert!(folded_plain_entry.is_reported());
    assert!(!folded_plain_entry.tree_end_reported());
    assert!(folded_kept_entry.is_reported());
    assert!(folded_kept_entry.tree_end_reported());

    tasks.shutdown();
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
        let (child, _, reader) = tasks
            .start_script(
                tasks::ScriptArgs {
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
        let (exited_task, _, exited_reader) = tasks
            .start_script(
                tasks::ScriptArgs {
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
        let polled = service
            .call_json(
                json!({
                    "action": "poll",
                    "taskId": exited_reader.handle(&exited_task.id),
                    "waitMs": 0
                }),
                script_call(&session, false),
            )
            .await;
        assert_ne!(polled.is_error, Some(true), "{polled:?}");
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
        let (killed_task, _, killed_reader) = tasks
            .start_script(
                tasks::ScriptArgs {
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
        let killed = service
            .call_json(
                json!({
                    "action": "kill",
                    "taskId": killed_reader.handle(&killed_task.id)
                }),
                script_call(&session, false),
            )
            .await;
        assert_ne!(killed.is_error, Some(true), "{killed:?}");
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
async fn list_warnings_follow_the_delivered_rows_for_direct_and_nested_calls() {
    let (service, tasks, session, warned_entry) = list_fixture().await;
    let warned = tasks.all().pop().unwrap();
    warned.inject_io_errors(Some("omitted-row-warning"), None);
    let (newest, _, _) = tasks
        .start_script(
            tasks::ScriptArgs {
                code: "return undefined;".into(),
                launch: Arc::clone(&session.launch),
                kill_after_ms: None,
                transfer: false,
                call_id: None,
                service: Arc::clone(&service),
                input_owner: Default::default(),
            },
            &session,
            "next".into(),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), newest.done().wait_for(|done| *done))
        .await
        .unwrap()
        .unwrap();

    let request = json!({"action":"list", "truncate":"head", "maxBytes":100});
    let direct = service
        .call_json(request.clone(), script_call(&session, false))
        .await;
    let text = direct.content[0].as_text().unwrap().text.as_str();
    assert!(!text.contains(&warned.id), "{text}");
    assert!(!text.contains("Warning:"), "{text}");
    let data = direct.structured_content.unwrap();
    assert!(data.get("warnings").is_none(), "{data}");
    assert!(
        data["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|task| task["taskId"] == warned.id
                && task["warnings"]
                    .as_array()
                    .is_some_and(|warnings| !warnings.is_empty())),
        "{data}"
    );
    assert!(!warned_entry.is_reported());

    let nested = service
        .call_json(request, script_call(&session, true))
        .await;
    let data = nested.structured_content.unwrap();
    assert!(
        data["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|warning| warning.as_str().unwrap().contains("omitted-row-warning")),
        "{data}"
    );
    assert!(warned_entry.is_reported());
    tasks.shutdown();
}

#[tokio::test]
async fn combined_background_footer_fits_and_rejected_results_keep_events_unseen() {
    let (service, tasks, session, _) = list_fixture().await;
    let task = tasks.all().pop().unwrap();
    let (other, _, _) = tasks
        .start_script(
            tasks::ScriptArgs {
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
                upstream_event_id: format!("footer-event-{index}"),
                data: json!({}),
                upstream_truncated: None,
            })
            .unwrap();
    }
    let call = script_call(&session, false);
    let notice = service
        .background(&call, &task, false, Action::Poll)
        .unwrap();
    let footer = &notice.value.as_ref().unwrap().0;
    assert!(footer.contains(&other.id), "{footer}");
    assert!(footer.contains("External events: 3 new"), "{footer}");
    assert!(
        footer.len() <= FOOTER_LIMIT,
        "{} bytes: {footer}",
        footer.len()
    );
    drop(notice);

    let launch = Arc::new(session.launch.for_session().with_outbound_limit(1));
    let mut refused_call = script_call(&session, false);
    refused_call.launch = Some(&launch);
    let request = json!({"action":"poll", "taskId":task.id, "waitMs":0});
    let refused = service.call_json(request.clone(), refused_call).await;
    assert_eq!(refused.is_error, Some(true));
    let next = service
        .call_json(request.clone(), script_call(&session, false))
        .await;
    let text = &next.content[0].as_text().unwrap().text;
    assert!(text.contains("External events: 3 new"), "{text}");
    let once = service
        .call_json(request, script_call(&session, false))
        .await;
    let text = &once.content[0].as_text().unwrap().text;
    assert!(!text.contains("External events:"), "{text}");
    tasks.shutdown();
}
