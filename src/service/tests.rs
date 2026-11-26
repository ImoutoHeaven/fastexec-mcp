use super::core::*;
use super::schema::*;
use super::*;
use crate::tasks::session::TaskEntry;
use crate::tasks::state::Snapshot;
use crate::term::output::{Window, WindowBuilder, WindowSpec};
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
    let launch = Arc::new(crate::env::LaunchContext::new(
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
            let script = crate::tasks::script::ScriptControl::new(std::thread::current());
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
            tasks.shutdown();
        }
    }
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

#[tokio::test]
async fn preparation_errors_release_workspace_before_waking_the_next_reader() {
    for failure in ["log", "transcript", "report"] {
        let (service, tasks, session) = script_fixture();
        let ((task, entry, _), _) = crate::codemode::spawn_script(
            &tasks,
            crate::codemode::ScriptSpawnArgs {
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
        let target = ReadTarget {
            task: Arc::clone(&task),
            entry: Arc::clone(&entry),
            reader: Arc::clone(&reader),
            state: gate,
        };
        let result = if failure == "transcript" {
            service
                .prepare_transcript(&ctx, None, target, view, Consumer::Program)
                .await
        } else {
            service
                .prepare_window_or_screen(
                    &ctx,
                    None,
                    target,
                    OutputRead {
                        action: Action::Poll,
                        view,
                        ended: None,
                        startup_warning: None,
                    },
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
async fn prepare_list_charges_main_pool_first_and_falls_back_to_control_when_full() {
    let (service, tasks, session, _entry) = list_fixture().await;
    let request: Request = serde_json::from_value(json!({"action": "list"})).unwrap();
    let view = || validate(&request, ExecPolicy::AGENT).unwrap();
    let filter = || ListFilter {
        scope: None,
        state: None,
        parent_task_id: None,
        limit: DEFAULT_LIST_LIMIT as usize,
    };

    // Free main pool: list workspace stays on main and leaves control untouched.
    let main = crate::lease::BytePool::new(1 << 20);
    let control = crate::lease::BytePool::new(1 << 20);
    let launch = Arc::new(
        session
            .launch
            .for_session()
            .with_memory(Arc::clone(&main))
            .with_control_memory(Arc::clone(&control)),
    );
    let mut ctx = script_ctx(&session);
    ctx.launch = Some(&launch);
    let PreparedExec::List(prepared) = service.prepare_list(&ctx, view(), filter(), None).unwrap()
    else {
        panic!("expected a list");
    };
    let required = prepared.workspace.bytes();
    assert!(required > 8192);
    assert_eq!(main.used(), required);
    assert_eq!(control.used(), 0);
    drop(prepared);
    assert_eq!((main.used(), control.used()), (0, 0));

    // Exhausted main pool: list workspace falls back to control cleanly.
    let held_main = main.try_acquire(main.limit()).unwrap();
    assert_eq!(main.used(), main.limit());
    let PreparedExec::List(fallback) = service.prepare_list(&ctx, view(), filter(), None).unwrap()
    else {
        panic!("expected a list");
    };
    assert_eq!(fallback.workspace.bytes(), required);
    assert_eq!(main.used(), main.limit());
    assert_eq!(control.used(), required);
    drop(fallback);
    drop(held_main);
    assert_eq!((main.used(), control.used()), (0, 0));

    // Partial main room (fits initial 8192 B, short for task metadata): migrates cleanly to control.
    let tight_main = crate::lease::BytePool::new(8192);
    let tight_launch = Arc::new(
        session
            .launch
            .for_session()
            .with_memory(Arc::clone(&tight_main))
            .with_control_memory(Arc::clone(&control)),
    );
    ctx.launch = Some(&tight_launch);
    let PreparedExec::List(migrated) = service.prepare_list(&ctx, view(), filter(), None).unwrap()
    else {
        panic!("expected a list");
    };
    assert_eq!(tight_main.used(), 0);
    assert_eq!(control.used(), required);
    drop(migrated);
    assert_eq!((tight_main.used(), control.used()), (0, 0));
    tasks.shutdown();
}
