use super::core::*;
use super::render::*;
use super::schema::*;
use super::*;
use crate::session::TaskEntry;
use crate::tasks::state::Snapshot;
use crate::term::output::{Window, WindowBuilder, WindowSpec};
use base64::Engine;
use rmcp::model::{CallToolResult, ContentBlock};
use serde_json::json;
use tokio::sync::Semaphore;

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
async fn consumer_cancellation_is_independent_of_output_policy() {
    for (consumer, policy) in [
        (Consumer::Program, ExecPolicy::AGENT),
        (Consumer::Agent, ExecPolicy::PROGRAM),
    ] {
        let (service, tasks, session) = script_fixture();
        let missing_bash =
            std::env::temp_dir().join(format!("fastexec-missing-bash-{}", tasks.epoch()));
        assert!(!missing_bash.exists());
        let launch = Arc::new(LaunchContext::new(
            std::env::temp_dir(),
            vec![("FASTEXEC_BASH".into(), missing_bash.into_os_string())],
        ));
        let mut ctx = script_ctx(&session);
        ctx.launch = Some(&launch);
        ctx.cancel.cancel();
        let request = serde_json::from_value(json!({
            "action": "start", "command": "printf started", "waitMs": 0, "maxBytes": 100
        }))
        .unwrap();
        let error = service
            .exec_core(request, policy, consumer, &mut ctx, None)
            .await
            .err()
            .expect("a cancelled or invalid start must fail");
        if consumer == Consumer::Program {
            assert!(matches!(error, ServiceError::Cancelled(_)), "{error:?}");
        } else {
            assert!(
                error.message().starts_with("Invalid FASTEXEC_BASH"),
                "Agent start must reach launch validation: {error:?}"
            );
        }
        assert!(tasks.all().is_empty());
        tasks.shutdown();
    }
}

#[tokio::test]
async fn consumer_reporting_is_independent_of_output_policy() {
    for (consumer, policy) in [
        (Consumer::Program, ExecPolicy::AGENT),
        (Consumer::Agent, ExecPolicy::PROGRAM),
    ] {
        for owned in [false, true] {
            let (service, tasks, session, entry) = list_fixture().await;
            let task = tasks.all().pop().unwrap();
            let reader = entry.reader(Some("abcd")).unwrap();
            let (script, worker) = codemode::prepare(tokio::runtime::Handle::current()).unwrap();
            if owned {
                script.record_started(reader.handle(&task.id));
            }
            let mut ctx = script_ctx(&session);
            ctx.on_started = Some(&script);
            let request = serde_json::from_value(json!({
                "action": "poll",
                "taskId": reader.handle(&task.id),
                "waitMs": 0,
                "maxBytes": 100
            }))
            .unwrap();
            let PreparedExec::Window(prepared) = service
                .exec_core(request, policy, consumer, &mut ctx, None)
                .await
                .unwrap()
            else {
                panic!("expected a window");
            };
            assert_eq!(
                prepared.mark_session_entry,
                consumer == Consumer::Agent || owned,
                "{consumer:?}, owned: {owned}"
            );
            drop(prepared);
            assert!(!entry.is_reported(), "preparation cannot commit a report");
            drop(worker);
            tasks.shutdown();
        }
    }
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
    let (task, _entry, reader) = tasks
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
    let polled = service
        .exec_agent_json(
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
    let killed = service
        .exec_agent_json(
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

#[tokio::test]
async fn response_snapshot_owns_its_workspace_and_uses_control_only_for_kill() {
    let (_service, tasks, session, _entry) = list_fixture().await;
    let task = tasks.all().pop().unwrap();
    let probe = PreparedSnapshot::new(&task, &session.launch, Action::Poll).unwrap();
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
        let snapshot = PreparedSnapshot::new(&task, &launch, Action::Poll);
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
    for action in [Action::Start, Action::Poll, Action::Transcript] {
        let error = PreparedSnapshot::new(&task, &launch, action).err().unwrap();
        assert!(
            matches!(&error, ServiceError::Overload(_, Some(meta)) if meta.bare_task_id == task.id)
        );
        assert!(
            error
                .message()
                .contains("Do not retry start or resend input")
        );
        assert_eq!((memory.used(), control.used()), (0, 0));
    }
    let snapshot = PreparedSnapshot::new(&task, &launch, Action::Kill).unwrap();
    assert_eq!(snapshot.value.state, "exited");
    assert_eq!(control.used(), required);
    drop(snapshot);
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
        let mut ctx = script_ctx(&session);
        ctx.launch = Some(&launch);
        let delivery = AgentDelivery {
            render_slot: Some(&slot),
            ..Default::default()
        };
        let mut operation = Box::pin(service.exec_agent_json(
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
async fn preparation_errors_release_workspace_before_waking_the_next_reader() {
    for failure in ["log", "transcript", "report"] {
        let (service, tasks, session) = script_fixture();
        let (task, entry, _) = tasks
            .start_script(
                tasks::ScriptArgs {
                    code: "text('retained output');".into(),
                    launch: Arc::clone(&session.launch),
                    kill_after_ms: None,
                    transfer: false,
                    call_id: None,
                    service: Arc::clone(&service),
                    input_owner: Default::default(),
                },
                &session,
                "read".into(),
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
            .await
            .unwrap()
            .unwrap();
        let reader = entry.reader(Some("next")).unwrap();
        let metadata = PreparedSnapshot::new(&task, &session.launch, Action::Poll).unwrap();
        let required = text_workspace_bytes(100) + metadata.workspace.bytes();
        drop(metadata);
        if failure == "report" {
            task.script()
                .unwrap()
                .record_started(reader.handle(&task.id));
        } else if failure == "log" {
            std::fs::remove_file(&task.log_path).unwrap();
        }
        let memory = crate::lease::BytePool::new(required);
        let launch = Arc::new(
            session
                .launch
                .for_session()
                .with_memory(Arc::clone(&memory)),
        );
        let slots = Arc::new(Semaphore::new(1));
        let observed = Arc::new(ReadWake {
            memory: Arc::clone(&memory),
            slots,
            used: AtomicUsize::new(usize::MAX),
            permits: AtomicUsize::new(usize::MAX),
            woken: AtomicBool::new(false),
        });
        let gate = reader.read_gate().lock_owned().await;
        let waker = Waker::from(Arc::clone(&observed));
        let mut next_read = Box::pin(reader.read_gate().lock_owned());
        assert!(
            next_read
                .as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
        let mut ctx = script_ctx(&session);
        ctx.launch = Some(&launch);
        let request: Request = serde_json::from_value(json!({
            "action": "poll", "taskId": reader.handle(&task.id), "maxBytes": 100
        }))
        .unwrap();
        let view = validate(&request, ExecPolicy::PROGRAM).unwrap();
        let result = if failure == "transcript" {
            service
                .prepare_transcript(
                    &ctx,
                    None,
                    Arc::clone(&task),
                    Arc::clone(&entry),
                    Arc::clone(&reader),
                    gate,
                    view,
                    Consumer::Program,
                )
                .await
        } else {
            service
                .prepare_window_or_screen(
                    &ctx,
                    None,
                    Action::Poll,
                    Arc::clone(&task),
                    Arc::clone(&entry),
                    Arc::clone(&reader),
                    gate,
                    view,
                    None,
                    None,
                    Consumer::Program,
                )
                .await
        };
        assert!(
            result.is_err(),
            "{failure} must fail after the blocking read"
        );
        assert!(observed.woken.load(Ordering::Acquire));
        assert_eq!(
            observed.used.load(Ordering::Relaxed),
            0,
            "{failure} unlocked before cleanup"
        );
        assert_eq!(observed.permits.load(Ordering::Relaxed), 1);
        let state = next_read.await;
        assert_eq!((state.cursor, state.attachments_seen), (0, 0));
        assert!(reader.is_fresh());
        assert!(!entry.is_reported());
        drop(state);
        tasks.shutdown();
    }
}

#[tokio::test]
async fn program_result_shrinks_workspace_before_waking_the_next_reader() {
    let (service, tasks, session, entry) = list_fixture().await;
    let task = tasks.all().pop().unwrap();
    let reader = entry.reader(Some("next")).unwrap();
    let memory = crate::lease::BytePool::new(2 << 20);
    let launch = Arc::new(
        session
            .launch
            .for_session()
            .with_memory(Arc::clone(&memory)),
    );
    let mut ctx = script_ctx(&session);
    ctx.launch = Some(&launch);
    let request = serde_json::from_value(json!({
        "action": "poll", "taskId": reader.handle(&task.id), "waitMs": 0, "maxBytes": 100
    }))
    .unwrap();
    let PreparedExec::Window(mut prepared) = service
        .exec_core(
            request,
            ExecPolicy::PROGRAM,
            Consumer::Program,
            &mut ctx,
            None,
        )
        .await
        .unwrap()
    else {
        panic!("expected a window");
    };
    let slots = Arc::new(Semaphore::new(1));
    prepared.permit = Some(Arc::new(Arc::clone(&slots).try_acquire_owned().unwrap()));
    let observed = Arc::new(ReadWake {
        memory: Arc::clone(&memory),
        slots,
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
    let (script, worker) = codemode::prepare(tokio::runtime::Handle::current()).unwrap();
    script.test_pending_result(1, 0, 16 << 10);
    let encoded = codemode::encode::encode_exec_program(
        PreparedExec::Window(prepared),
        &ctx,
        &ProgramDelivery {
            script: &script,
            pending_id: 1,
            is_control: false,
        },
    );
    let codemode::control::Outcome::Value(payload) = encoded.outcome else {
        panic!("expected a Program result");
    };
    assert!(observed.woken.load(Ordering::Acquire));
    assert_eq!(
        observed.used.load(Ordering::Relaxed),
        payload.value.capacity()
    );
    assert_eq!(observed.permits.load(Ordering::Relaxed), 1);
    drop(next_read.await);
    drop(payload);
    assert_eq!(memory.used(), 0);
    drop(worker);
    tasks.shutdown();
}

#[tokio::test]
async fn nested_precommit_list_quota_refusal_and_cancel_leave_reported_state_due() {
    let (service, tasks, session, entry) = list_fixture().await;
    let task = tasks.all().pop().unwrap();
    let (script, worker) = codemode::prepare(tokio::runtime::Handle::current()).unwrap();
    script.record_started(format!("{}.abcd", task.id));
    script.test_pending_result(1, 8, 64);
    let make_ctx = || {
        let mut ctx = script_ctx(&session);
        ctx.parent_task_id = Some("aaaaaaaa-0".into());
        ctx.on_started = Some(&script);
        ctx
    };
    let make_delivery = || ProgramDelivery {
        script: &script,
        pending_id: 1,
        is_control: true,
    };
    let list_req = || -> Request { serde_json::from_value(json!({"action":"list"})).unwrap() };
    let refused = service
        .exec_program(list_req(), make_ctx(), make_delivery())
        .await;
    assert!(matches!(refused.outcome, codemode::control::Outcome::Limit));
    assert!(!entry.is_reported());
    assert_eq!(script.test_bridge_bytes(), 8);
    drop(refused);
    script.test_bridge_limit(16 * 1024);
    let cancelled = make_ctx();
    cancelled.cancel.cancel();
    let refused = service
        .exec_program(list_req(), cancelled, make_delivery())
        .await;
    match refused.outcome {
        codemode::control::Outcome::Cancelled(_) => {}
        codemode::control::Outcome::Value(payload) => {
            let parsed: Value = serde_json::from_str(&payload.value).unwrap();
            assert_eq!(parsed["ok"], false);
            assert_eq!(parsed["code"], "CANCELLED");
            drop(payload);
        }
        _ => panic!("expected Cancelled or CANCELLED error envelope"),
    }
    assert!(!entry.is_reported());
    let delivered = service
        .exec_program(list_req(), make_ctx(), make_delivery())
        .await;
    let codemode::control::Outcome::Value(payload) = delivered.outcome else {
        panic!("expected Value outcome");
    };
    assert!(entry.is_reported());
    assert_eq!(script.test_bridge_bytes(), payload.value.len());
    drop(payload);
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
    let make_ctx = || {
        let mut ctx = script_ctx(&session);
        ctx.parent_task_id = Some("aaaaaaaa-0".into());
        ctx.on_started = Some(&script);
        ctx
    };
    let make_delivery = || ProgramDelivery {
        script: &script,
        pending_id: 1,
        is_control: false,
    };
    let make_req = || -> Request {
        serde_json::from_value(
            json!({"action":"poll", "taskId":handle, "waitMs":0, "maxBytes":100}),
        )
        .unwrap()
    };
    let refused = service
        .exec_program(make_req(), make_ctx(), make_delivery())
        .await;
    assert!(matches!(refused.outcome, codemode::control::Outcome::Limit));
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
    script.test_bridge_limit(16 * 1024);
    let cancelled = make_ctx();
    cancelled.cancel.cancel();
    let refused = service
        .exec_program(make_req(), cancelled, make_delivery())
        .await;
    match refused.outcome {
        codemode::control::Outcome::Cancelled(_) => {}
        codemode::control::Outcome::Value(payload) => {
            let parsed: Value = serde_json::from_str(&payload.value).unwrap();
            assert_eq!(parsed["ok"], false);
            assert_eq!(parsed["code"], "CANCELLED");
            drop(payload);
        }
        _ => panic!("expected Cancelled or CANCELLED error envelope"),
    }
    assert_eq!(script.test_attachment_count(), 0);
    assert!(reader.is_fresh());
    let delivered = service
        .exec_program(make_req(), make_ctx(), make_delivery())
        .await;
    let codemode::control::Outcome::Value(payload) = delivered.outcome else {
        panic!("expected Value outcome");
    };
    assert_eq!(script.test_bridge_bytes(), payload.value.len());
    assert_eq!(script.test_attachment_count(), 1);
    let parsed: Value = serde_json::from_str(&payload.value).unwrap();
    let attachment = &parsed["value"]["codemode"]["attachments"][0];
    assert_eq!(
        attachment["__hid"].as_u64(),
        Some(1),
        "a refused batch leaves its undelivered slot available"
    );
    {
        let state = reader.read_gate().lock_owned().await;
        assert!(state.cursor > 0);
        assert_eq!(state.attachments_seen, 1);
        assert!(!reader.is_fresh());
    }
    assert!(entry.is_reported());
    drop(payload);
    assert_eq!(pool.used(), script.test_attachment_bytes());
    assert!(pool.used() > 0, "the script retains its attachment handle");
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
    let mut ctx = script_ctx(&session);
    ctx.launch = Some(&blocked);
    let refused = service
        .exec_agent_json(json!({"action":"list"}), ctx, AgentDelivery::default())
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
    let refused = service
        .exec_agent_json(json!({"action":"list"}), ctx, AgentDelivery::default())
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
    let refused = service
        .exec_agent_json(
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
    for mode in 0..3 {
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
            http_response: mode == 1,
            ..Default::default()
        };
        if mode == 2 {
            delivery.render_slot = Some(&slot);
            ctx.call_id = Some(7);
        }
        let mut result = service
            .exec_agent_json(json!({"action":"list"}), ctx, delivery)
            .await;
        assert_eq!(result.result.is_error, Some(false), "{:?}", result.result);
        assert!(result.result.structured_content.is_none());
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
        let mut ctx = script_ctx(&session);
        let mut delivery = AgentDelivery {
            rpc_id_bytes: crate::ipc::json_len(&"r".repeat(2048)).unwrap(),
            ..Default::default()
        };
        if daemon {
            ctx.call_id = Some(u64::MAX);
            delivery.render_slot = Some(&slot);
        }
        let request = json!({
            "action":"poll", "taskId":format!("{}.9999", task.id),
            "waitMs":0, "maxBytes":100,
        });
        let refused = service
            .exec_agent_json(request.clone(), ctx, delivery)
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
            rpc_id_bytes: crate::ipc::json_len(&"small").unwrap(),
            ..Default::default()
        };
        if daemon {
            ctx.call_id = Some(u64::MAX);
            delivery.render_slot = Some(&slot);
        }
        let delivered = service.exec_agent_json(request, ctx, delivery).await;
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
        if daemon {
            let local = crate::ipc::json_len(&delivered.result).unwrap();
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

    let result = service
        .exec_agent_json(
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
        crate::session::LaunchContext::new(std::env::temp_dir(), Vec::new())
            .with_memory(Arc::clone(&pool)),
    );
    let session = Session::new("text-owner".into(), "text-owner".into(), launch);
    let mut result = service
        .codemode_agent(
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
    assert!(wire >= crate::ipc::json_len(&result.result).unwrap());
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
        .codemode_agent(
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
        running_service
            .exec_agent_json(request, ctx, AgentDelivery::default())
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
    let result = service
        .codemode_agent(
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
    let empty = service
        .exec_agent_json(
            json!({"action":"poll","taskId":handle,"waitMs":0}),
            script_ctx(&session),
            AgentDelivery::default(),
        )
        .await
        .result;
    assert!(empty.structured_content.is_none());
    assert_eq!(image_count(&empty), 0);
    let bare = parse_handle(&handle).unwrap().0;
    let reread = service
        .exec_agent_json(
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
        .codemode_agent(
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
    let second = service
        .exec_agent_json(
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
    let third = service
        .exec_agent_json(
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
    let fourth = service
        .exec_agent_json(
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
    let p1 = service
        .exec_agent_json(
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
    let changed = service
        .exec_agent_json(
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
    let recovered = service
        .exec_agent_json(fresh_req, script_ctx(&session), AgentDelivery::default())
        .await
        .result;
    assert!(recovered.structured_content.is_none());
    assert_eq!(image_count(&recovered), 1);
    tasks.shutdown();
}

#[tokio::test]
async fn nested_task_reads_consume_reader_state_without_consuming_session_notices() {
    let (service, tasks, session) = script_fixture();
    let (task, entry, reader) = tasks
        .start_script(
            tasks::ScriptArgs {
                code: "return 7".into(),
                launch: Arc::clone(&session.launch),
                kill_after_ms: None,
                transfer: false,
                call_id: None,
                service: Arc::clone(&service),
                input_owner: Default::default(),
            },
            &session,
            "r001".into(),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
        .await
        .unwrap()
        .unwrap();
    let handle = reader.handle(&task.id);
    // An unrelated nested script polling/killing an outer session task must NOT mark the outer
    // session's TaskEntry reported or advance EventBus external_background_cursor.
    service
        .event_bus
        .publish_external(crate::events::ExternalEventInput {
            event_type: None,
            source: "src".into(),
            topic: None,
            name: "evt".into(),
            upstream_event_id: "nested-cursor-test".into(),
            data: json!({}),
            upstream_truncated: None,
        })
        .unwrap();
    let (script, worker) = codemode::prepare(tokio::runtime::Handle::current()).unwrap();
    script.test_pending_result(1, 0, 16 * 1024);
    let mut ctx = script_ctx(&session);
    ctx.on_started = Some(&script);
    let poll_req: Request =
        serde_json::from_value(json!({"action":"poll","taskId":handle,"waitMs":0})).unwrap();
    let poll_out = service
        .exec_program(
            poll_req,
            ctx,
            ProgramDelivery {
                script: &script,
                pending_id: 1,
                is_control: false,
            },
        )
        .await;
    assert!(matches!(
        poll_out.outcome,
        codemode::control::Outcome::Value(_)
    ));
    assert!(reader.is_reported());
    assert!(!entry.is_reported());

    script.test_pending_result(2, 0, 16 * 1024);
    let mut ctx = script_ctx(&session);
    ctx.on_started = Some(&script);
    let kill_req: Request =
        serde_json::from_value(json!({"action":"kill","taskId":handle})).unwrap();
    let kill_out = service
        .exec_program(
            kill_req,
            ctx,
            ProgramDelivery {
                script: &script,
                pending_id: 2,
                is_control: true,
            },
        )
        .await;
    assert!(matches!(
        kill_out.outcome,
        codemode::control::Outcome::Value(_)
    ));
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
    // An outer Agent call still sees both the unreported task AND the external event!
    let outer = service
        .codemode_agent(
            serde_json::from_value(json!({"code":"return 8"})).unwrap(),
            script_ctx(&session),
            AgentDelivery::default(),
            false,
        )
        .await
        .result;
    let outer_text = text_of(&outer);
    assert!(outer_text.contains(&task.id), "{outer_text}");
    assert!(
        outer_text.contains("External events: 1 new"),
        "{outer_text}"
    );
    assert!(entry.is_reported());
    drop(worker);
    tasks.shutdown();
}

#[tokio::test]
async fn nested_list_marks_only_delivered_session_tasks() {
    let (service, tasks, session) = script_fixture();
    let mut started = Vec::new();
    for (idx, code) in ["return 7", "return 8", "return 9"].into_iter().enumerate() {
        let (task, entry, reader) = tasks
            .start_script(
                tasks::ScriptArgs {
                    code: code.into(),
                    launch: Arc::clone(&session.launch),
                    kill_after_ms: None,
                    transfer: false,
                    call_id: None,
                    service: Arc::clone(&service),
                    input_owner: Default::default(),
                },
                &session,
                format!("r00{idx}"),
            )
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
            .await
            .unwrap()
            .unwrap();
        started.push((task, entry, reader));
    }
    let (script, worker) = codemode::prepare(tokio::runtime::Handle::current()).unwrap();
    // Script owns l000 (omitted by limit: 2) and l002 (delivered), but not l001 (outer task).
    script.record_started(started[0].2.handle(&started[0].0.id));
    script.record_started(started[2].2.handle(&started[2].0.id));
    script.test_pending_result(1, 0, 16 * 1024);
    let mut ctx = script_ctx(&session);
    ctx.on_started = Some(&script);
    let req: Request = serde_json::from_value(json!({"action":"list","limit":2})).unwrap();
    let listed = service
        .exec_program(
            req,
            ctx,
            ProgramDelivery {
                script: &script,
                pending_id: 1,
                is_control: true,
            },
        )
        .await;
    let codemode::control::Outcome::Value(payload) = listed.outcome else {
        panic!("expected Value");
    };
    let parsed: Value = serde_json::from_str(&payload.value).unwrap();
    assert_eq!(parsed["value"]["tasks"].as_array().unwrap().len(), 2);
    assert!(
        !started[0].1.is_reported(),
        "an unselected script-owned task stays due"
    );
    assert!(
        !started[1].1.is_reported(),
        "an outer-session task stays due even when delivered in nested list"
    );
    assert!(
        started[2].1.is_reported(),
        "a delivered script-owned task is reported"
    );
    drop(worker);
    tasks.shutdown();
}

#[tokio::test]
async fn script_poll_rejects_presence_even_when_false_or_empty() {
    let (service, tasks, session) = script_fixture();
    let result = service
        .codemode_agent(
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
        let ok = service
            .exec_agent_json(request, script_ctx(&session), AgentDelivery::default())
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
        let result = service
            .exec_agent_json(request, script_ctx(&session), AgentDelivery::default())
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
    let result = service
        .exec_agent_json(
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

    let (task, _, reader) = tasks
        .start_script(
            tasks::ScriptArgs {
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
    let polled = service
        .exec_agent_json(
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
        service
            .codemode_agent(
                serde_json::from_value(json!({"code":"while (true) {}","waitMs":0})).unwrap(),
                script_ctx(&owner),
                AgentDelivery::default(),
                false,
            )
            .await;
        let completed = service
            .codemode_agent(
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
                service
                    .exec_agent_json(request.clone(), ctx, AgentDelivery::default())
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
            let first = service
                .exec_agent_json(
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
                held_known: true,
                kill_error: None,
            };
            rendering.mark_reported(&snapshot);
            reporting.mark_reported(&snapshot);
            drop(state);
            Ok(())
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
        Ok(output)
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
        Ok(())
    })
    .await;
    assert!(result.is_err());
    assert!(reader.is_fresh());
}

#[tokio::test]
async fn a_large_list_row_does_not_hide_the_opposite_end() {
    let (service, tasks, session) = script_fixture();
    let (first, _, _) = tasks
        .start_script(
            tasks::ScriptArgs {
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
    let (second, _, _) = tasks
        .start_script(
            tasks::ScriptArgs {
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
    let listed = service
        .exec_agent_json(
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
            .exec_agent_json(
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
            .exec_agent_json(
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
        .exec_agent_json(
            request.clone(),
            script_ctx(&session),
            AgentDelivery::default(),
        )
        .await
        .result;
    assert!(direct.structured_content.is_none());
    let text = direct.content[0].as_text().unwrap().text.as_str();
    assert!(!text.contains(&warned.id), "{text}");
    assert!(!text.contains("Warning:"), "{text}");
    assert!(!warned_entry.is_reported());

    let (script, worker) = codemode::prepare(tokio::runtime::Handle::current()).unwrap();
    script.test_pending_result(1, 0, 16 * 1024);
    let bad_req: Request = serde_json::from_value(request).unwrap();
    let bad_nested = service
        .exec_program(
            bad_req,
            script_ctx(&session),
            ProgramDelivery {
                script: &script,
                pending_id: 1,
                is_control: true,
            },
        )
        .await;
    let codemode::control::Outcome::Value(bad_payload) = bad_nested.outcome else {
        panic!("expected validation error envelope");
    };
    let bad_json: Value = serde_json::from_str(&bad_payload.value).unwrap();
    assert_eq!(bad_json["ok"], false);
    assert_eq!(bad_json["code"], "VALIDATION");
    assert!(!warned_entry.is_reported());

    script.test_pending_result(2, 0, 16 * 1024);
    let plain_req: Request = serde_json::from_value(json!({"action":"list"})).unwrap();
    let nested = service
        .exec_program(
            plain_req,
            script_ctx(&session),
            ProgramDelivery {
                script: &script,
                pending_id: 2,
                is_control: true,
            },
        )
        .await;
    let codemode::control::Outcome::Value(payload) = nested.outcome else {
        panic!("expected Value");
    };
    let data: Value = serde_json::from_str(&payload.value).unwrap();
    assert!(data["value"].get("warnings").is_none(), "{data}");
    assert!(
        data["value"]["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|task| task["taskId"] == warned.id
                && task["warnings"].as_array().is_some_and(|w| w
                    .iter()
                    .any(|s| s.as_str().unwrap().contains("omitted-row-warning")))),
        "{data}"
    );
    assert!(
        !warned_entry.is_reported(),
        "nested list must not mark an outer-session task reported"
    );
    drop(worker);
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

    let launch = Arc::new(session.launch.for_session().with_outbound_limit(1));
    let mut refused_ctx = script_ctx(&session);
    refused_ctx.launch = Some(&launch);
    let request = json!({"action":"poll", "taskId":task.id, "waitMs":0});
    let refused = service
        .exec_agent_json(request.clone(), refused_ctx, AgentDelivery::default())
        .await
        .result;
    assert_eq!(refused.is_error, Some(true));
    assert!(refused.structured_content.is_none());

    let next = service
        .exec_agent_json(
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

    let once = service
        .exec_agent_json(request, script_ctx(&session), AgentDelivery::default())
        .await
        .result;
    assert!(once.structured_content.is_none());
    let text = &once.content[0].as_text().unwrap().text;
    assert!(!text.contains("External events:"), "{text}");
    tasks.shutdown();
}

#[tokio::test]
async fn program_transcript_dto_emits_raw_output_and_all_fields() {
    let (_service, tasks, session, entry) = list_fixture().await;
    let task = tasks.all().pop().unwrap();
    let reader = entry.reader(Some("tr01")).unwrap();
    let gate = reader.read_gate().lock_owned().await;
    let mut read = StagedRead::new(gate);
    read.state.shown = (128, 12);

    let mut cleaner = crate::term::output::Cleaner::default();
    let mut builder = WindowBuilder::new(
        WindowSpec {
            truncate: Truncate::Tail,
            budget: 16,
            raw: false,
            clean: false,
            keep_nul: true,
            include_markers: false,
            encoding: encoding_rs::UTF_8,
            source: "transcript",
        },
        &mut cleaner,
        1,
    );
    builder.push(b"line-1\nline-2\nline-3\n");
    let window = builder.finish();
    assert!(!window.text.contains("omitted"), "{}", window.text);

    let metadata = PreparedSnapshot::new(&task, &session.launch, Action::Transcript).unwrap();
    let prepared = PreparedExec::Transcript(Box::new(PreparedTranscript {
        workspace: crate::lease::Lease::empty(session.launch.memory()),
        permit: None,
        metadata,
        window,
        transcript_path: "/tmp/test.transcript".into(),
        transcript_lines: 12,
        final_lines: 10,
        first_line: 1,
        transcript_incomplete: true,
        transcript_error: Some("spool truncated".into()),
        transcript_warnings: vec!["transcript incomplete".into()],
        task: Arc::clone(&task),
        entry: Arc::clone(&entry),
        reader: Arc::clone(&reader),
        view: View {
            truncate: Truncate::Tail,
            max_bytes: 16,
            raw: false,
            encoding: None,
            screen: false,
            conditions: false,
            inline_markers: false,
            keep_nul: true,
        },
        mark_session_entry: false,
        read,
    }));

    let (script, worker) = codemode::prepare(tokio::runtime::Handle::current()).unwrap();
    script.test_pending_result(1, 0, 16 * 1024);
    let outcome = codemode::encode::encode_exec_program(
        prepared,
        &script_ctx(&session),
        &ProgramDelivery {
            script: &script,
            pending_id: 1,
            is_control: false,
        },
    )
    .outcome;
    let codemode::control::Outcome::Value(payload) = outcome else {
        panic!("expected encoded transcript value");
    };
    let parsed: Value = serde_json::from_str(&payload.value).unwrap();
    assert_eq!(parsed["ok"], true);
    let v = &parsed["value"];
    assert_eq!(v["action"], "transcript");
    assert_eq!(v["taskId"], reader.handle(&task.id));
    assert_eq!(v["transcriptPath"], "/tmp/test.transcript");
    assert_eq!(v["transcriptLines"], 12);
    assert_eq!(v["finalLines"], 10);
    assert_eq!(v["firstLine"], 1);
    assert_eq!(v["truncated"], true);
    assert_eq!(v["transcriptIncomplete"], true);
    assert_eq!(v["transcriptError"], "spool truncated");
    assert!(v["omittedLines"].as_u64().unwrap() >= 1);
    assert!(v["omittedRange"].is_array());
    assert!(!v["output"].as_str().unwrap().contains("omitted"));
    drop(payload);
    drop(worker);
    tasks.shutdown();
}

#[tokio::test]
async fn program_bridge_admission_rollback_and_control_memory_fallback() {
    let tasks = Tasks::new(tasks::Limits::OFF).unwrap();
    let service = Arc::new(Service::new(Arc::clone(&tasks), false));
    let main_pool = crate::lease::BytePool::new(2 << 20);
    let control_pool = crate::lease::BytePool::new(2 << 20);
    let launch = Arc::new(
        crate::session::LaunchContext::new(std::env::temp_dir(), Vec::new())
            .with_memory(Arc::clone(&main_pool))
            .with_control_memory(Arc::clone(&control_pool)),
    );
    let session = Session::new("rb-ctrl".into(), "rb-ctrl".into(), Arc::clone(&launch));
    let (task, entry, _) = tasks
        .start_script(
            tasks::ScriptArgs {
                code: format!("text('rollback-body'); attach({});", json!(pixel_url())),
                launch: Arc::clone(&launch),
                kill_after_ms: None,
                transfer: false,
                call_id: None,
                service: Arc::clone(&service),
                input_owner: Default::default(),
            },
            &session,
            "rb01".into(),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
        .await
        .unwrap()
        .unwrap();

    // Verify ProbeWarning rolls back on uncommitted drop (such as failed bridge admission).
    let (_, _, probe_opt) =
        launch.task_env(std::path::Path::new("/nonexistent-bash-for-probe"), false);
    if let Some(mut probe) = probe_opt {
        assert!(probe.message().is_some());
        drop(probe);
        let (_, _, probe_again) =
            launch.task_env(std::path::Path::new("/nonexistent-bash-for-probe"), false);
        assert!(probe_again.unwrap().message().is_some());
    }

    let reader = entry.reader(Some("rb99")).unwrap();
    let handle = reader.handle(&task.id);
    let (script, worker) = codemode::prepare(tokio::runtime::Handle::current()).unwrap();
    script.record_started(handle.clone());
    // Tiny bridge quota forces admit_bridge_result to fail after StagedRead & NestedAttachments.
    script.test_pending_result(1, 0, 32);

    let mut ctx = script_ctx(&session);
    ctx.on_started = Some(&script);
    let poll_req: Request =
        serde_json::from_value(json!({"action": "poll", "taskId": handle, "waitMs": 0})).unwrap();
    let refused = service
        .exec_program(
            poll_req,
            ctx,
            ProgramDelivery {
                script: &script,
                pending_id: 1,
                is_control: false,
            },
        )
        .await;
    assert!(matches!(refused.outcome, codemode::control::Outcome::Limit));
    drop(refused);
    // Zero leaked leases, read_gate immediately acquirable, StagedRead & TaskEntry & NestedAttachments rolled back.
    assert_eq!((main_pool.used(), control_pool.used()), (0, 0));
    assert_eq!(script.test_attachment_count(), 0);
    assert!(!entry.is_reported());
    {
        let gate = reader
            .read_gate()
            .try_lock_owned()
            .expect("read_gate must not be blocked after failed bridge admission");
        assert_eq!((gate.cursor, gate.attachments_seen), (0, 0));
        assert!(reader.is_fresh());
    }

    // Exhaust 100% of main pool and verify Program List and Kill succeed via control_memory() fallback.
    let _held_main = main_pool
        .try_acquire(main_pool.limit() - main_pool.used())
        .unwrap();
    assert_eq!(main_pool.used(), main_pool.limit());

    script.test_pending_result(2, 0, 64 * 1024);
    let list_req: Request = serde_json::from_value(json!({"action": "list"})).unwrap();
    let list_out = service
        .exec_program(
            list_req,
            script_ctx(&session),
            ProgramDelivery {
                script: &script,
                pending_id: 2,
                is_control: true,
            },
        )
        .await;
    let codemode::control::Outcome::Value(list_payload) = list_out.outcome else {
        panic!("Program list must succeed via control_memory() when main pool is full");
    };
    assert!(control_pool.used() > 0);
    let list_json: Value = serde_json::from_str(&list_payload.value).unwrap();
    assert_eq!(list_json["ok"], true);
    assert_eq!(list_json["value"]["action"], "list");
    drop(list_payload);
    assert_eq!(control_pool.used(), 0);

    script.test_pending_result(3, 0, 64 * 1024);
    let mut kill_ctx = script_ctx(&session);
    kill_ctx.on_started = Some(&script);
    let kill_req: Request =
        serde_json::from_value(json!({"action": "kill", "taskId": handle})).unwrap();
    let kill_out = service
        .exec_program(
            kill_req,
            kill_ctx,
            ProgramDelivery {
                script: &script,
                pending_id: 3,
                is_control: true,
            },
        )
        .await;
    let codemode::control::Outcome::Value(kill_payload) = kill_out.outcome else {
        panic!("Program kill must succeed via control_memory() when main pool is full");
    };
    assert!(control_pool.used() > 0);
    let kill_json: Value = serde_json::from_str(&kill_payload.value).unwrap();
    assert_eq!(kill_json["ok"], true);
    assert_eq!(kill_json["value"]["action"], "kill");
    assert_eq!(kill_json["value"]["output"], "rollback-body");
    drop(kill_payload);
    assert_eq!(control_pool.used(), script.test_attachment_bytes());
    assert!(
        control_pool.used() > 0,
        "the script retains its attachment handle"
    );

    drop(_held_main);
    assert_eq!(main_pool.used(), 0);
    drop(worker);
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
            service
                .codemode_agent(
                    serde_json::from_value(json!({"code": "return undefined;"})).unwrap(),
                    script_ctx(&session),
                    AgentDelivery::default(),
                    false,
                )
                .await
        } else {
            service
                .exec_agent_json(
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
async fn cancelling_an_active_program_wait_preserves_cancelled_code() {
    let (service, tasks, session) = script_fixture();
    let (task, _, reader) = tasks
        .start_script(
            tasks::ScriptArgs {
                code: "await sleep(60000);".into(),
                launch: Arc::clone(&session.launch),
                kill_after_ms: None,
                transfer: false,
                call_id: None,
                service: Arc::clone(&service),
                input_owner: Default::default(),
            },
            &session,
            "r001".into(),
        )
        .unwrap();
    let (script, worker) = codemode::prepare(tokio::runtime::Handle::current()).unwrap();
    script.test_pending_result(1, 0, 16 << 10);
    let ctx = script_ctx(&session);
    let cancel = ctx.cancel.clone();
    let request = serde_json::from_value(json!({
        "action": "poll", "taskId": reader.handle(&task.id)
    }))
    .unwrap();
    let mut waiting = Box::pin(service.exec_program(
        request,
        ctx,
        ProgramDelivery {
            script: &script,
            pending_id: 1,
            is_control: false,
        },
    ));
    assert!(
        waiting
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    cancel.cancel();
    let result = waiting.await;
    let still_running = task.is_running();
    drop(worker);
    tasks.shutdown();
    assert!(still_running, "cancelling a wait must not kill its task");
    let codemode::control::Outcome::Value(payload) = result.outcome else {
        panic!("expected a CANCELLED error envelope");
    };
    let parsed: Value = serde_json::from_str(&payload.value).unwrap();
    assert_eq!(parsed["ok"], false);
    assert_eq!(parsed["code"], "CANCELLED");
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
    let refused = service
        .exec_agent_json(
            json!({"action": "poll", "taskId": reader.handle(&task.id), "waitMs": 0}),
            ctx,
            AgentDelivery {
                render_slot: Some(&slot),
                ..Default::default()
            },
        )
        .await;
    tasks.shutdown();
    let text = text_of(&refused.result);
    assert!(refused.overload, "{text}");
    let meta = refused.task_meta.unwrap();
    assert_eq!(meta.bare_task_id, task.id);
    assert_eq!(meta.log_path, files::path::display_path(&task.log_path));
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
async fn reader_capacity_is_overload_with_task_recovery_metadata() {
    let (service, tasks, session, entry) = list_fixture().await;
    let task = tasks.all().pop().unwrap();
    let busy: Vec<_> = (0..16)
        .map(|index| entry.reader(Some(&format!("{index:04}"))).unwrap())
        .collect();
    let (script, worker) = codemode::prepare(tokio::runtime::Handle::current()).unwrap();
    script.test_pending_result(1, 0, 16 << 10);
    let refused = service
        .exec_program(
            serde_json::from_value(json!({"action": "poll", "taskId": task.id, "waitMs": 0}))
                .unwrap(),
            script_ctx(&session),
            ProgramDelivery {
                script: &script,
                pending_id: 1,
                is_control: false,
            },
        )
        .await;
    drop(worker);
    tasks.shutdown();
    let codemode::control::Outcome::Value(payload) = refused.outcome else {
        panic!("expected an OVERLOAD error envelope");
    };
    let parsed: Value = serde_json::from_str(&payload.value).unwrap();
    assert_eq!(parsed["ok"], false);
    assert_eq!(parsed["code"], "OVERLOAD");
    assert_eq!(parsed["result"]["taskId"], task.id);
    assert_eq!(
        parsed["result"]["logPath"],
        files::path::display_path(&task.log_path)
    );
    assert!(!entry.is_reported());
    assert!(busy.iter().all(|reader| reader.is_fresh()));
    assert!(Arc::ptr_eq(&busy[0], &entry.reader(Some("0000")).unwrap(),));
}
