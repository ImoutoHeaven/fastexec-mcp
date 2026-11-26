use super::bridge::{
    DIAGNOSTIC_RESERVE, DIAGNOSTIC_WORKSPACE, Host, MAX_STARTS, SLEEP_CHUNK_MS, STARTS_ERROR,
    SyncCallError, queue_permit, sleep, split_sleep,
};
use super::control::{
    BRIDGE_ERROR, BRIDGE_LIMIT, BridgeHandle, InFlightGuard, MAX_CODE, Outcome, ProgramDelivery,
    Report, Request, SettledCall, lock, validate,
};
use super::encode::{encode_exec_program, encode_service_error};
use super::vm::{STALLED, describe, pending_text};
use super::*;
use crate::env::LaunchContext;
use crate::id;
use crate::lease::BytePool;
use crate::service::Service;
use crate::tasks::script::{CallInfo, QueuedStartGuard, ScriptControl};
use crate::tasks::session::Session;
use crate::tasks::state::Task;
use crate::tasks::{Limits, Started, TASK_LOG_LIMIT, Tasks};
use base64::Engine;
use rquickjs::{Context, Function, Runtime, Value};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::Semaphore;

struct Fixture {
    tasks: Arc<Tasks>,
    service: Arc<Service>,
    session: Arc<Session>,
    dir: std::path::PathBuf,
}

impl Fixture {
    fn new() -> Self {
        Self::with_limits(Limits::OFF)
    }

    fn with_limits(limits: Limits) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "fastexec-script-test-{}",
            id::random_chars(12).unwrap()
        ));
        let tasks = Tasks::new_in(limits, dir.clone()).unwrap();
        let service = Arc::new(Service::new(Arc::clone(&tasks), false));
        let session = Session::new(
            "script-tests".into(),
            "script-tests".into(),
            Arc::new(LaunchContext::new(
                dir.clone(),
                std::env::vars_os().collect(),
            )),
        );
        Self {
            tasks,
            service,
            session,
            dir,
        }
    }

    fn start(&self, code: &str, deadline: Option<u64>) -> (Started, BridgeHandle) {
        spawn_script(
            &self.tasks,
            ScriptSpawnArgs {
                code: code.into(),
                launch: Arc::clone(&self.session.launch),
                kill_after_ms: deadline,
                transfer: false,
                call_id: None,
                service: Arc::clone(&self.service),
                input_owner: Default::default(),
            },
            &self.session,
            id::random_chars(4).unwrap(),
        )
        .unwrap()
    }

    async fn result(&self, code: &str) -> (crate::tasks::state::Snapshot, String) {
        let ((task, _, _), _) = self.start(code, Some(5000));
        complete(&task).await;
        (
            task.snapshot(),
            std::fs::read_to_string(&task.log_path).unwrap(),
        )
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.tasks.shutdown();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

async fn complete(task: &Task) {
    let mut done = task.done();
    tokio::time::timeout(Duration::from_secs(8), async {
        while !*done.borrow_and_update() {
            done.changed().await.unwrap();
        }
    })
    .await
    .expect("script did not finish");
}

#[test]
fn nested_precommit_small_quota_replacement_is_atomic() {
    let info = || CallInfo {
        tool: "exec".into(),
        action: None,
        task_id: None,
    };
    let mut report = Report {
        limit: Some(20),
        ..Default::default()
    };
    report.admit(1, info(), 8, Default::default()).unwrap();
    report.admit(2, info(), 2, Default::default()).unwrap();
    assert!(report.result(1, 18));
    assert_eq!(report.bridge_bytes, 20);
    assert!(!report.result(1, 19));
    assert_eq!((report.bridge_bytes, report.pending[&1].bytes), (20, 18));
    assert!(report.result(1, 3));
    assert_eq!(report.bridge_bytes, 5);
    report.remove(1);
    report.remove(2);
    assert_eq!(report.bridge_bytes, 0);
}

fn sync_error_message(err: &SyncCallError) -> &str {
    match err {
        SyncCallError::Type(msg) | SyncCallError::Limit(msg) | SyncCallError::Overload(msg) => msg,
    }
}

fn unwrap_sync_ok(res: Result<u64, crate::lease::Owned<SyncCallError>>) -> u64 {
    match res {
        Ok(id) => id,
        Err(err) => panic!(
            "expected Ok, got SyncCallError: {}",
            sync_error_message(&err.value)
        ),
    }
}

fn unwrap_sync_err(
    res: Result<u64, crate::lease::Owned<SyncCallError>>,
) -> crate::lease::Owned<SyncCallError> {
    match res {
        Ok(id) => panic!("expected Err, got Ok({id})"),
        Err(err) => err,
    }
}

#[tokio::test]
async fn text_buffer_bridge_reserves_exact_encoding_and_keeps_queue_ownership() {
    let expected = r#"{"ok":false,"code":"IO","error":"hello λ\n"}"#.to_owned();
    let fixture = Fixture::new();
    let target = expected.len().max(128);
    for (budget, fits) in [(target - 1, false), (target, true)] {
        let pool = BytePool::new(budget);
        let launch = Arc::new(
            LaunchContext::new(fixture.dir.clone(), Vec::new())
                .with_memory(Arc::clone(&pool))
                .with_control_memory(BytePool::new(0)),
        );
        let (_control, script, worker) = prepare_worker().unwrap();
        drop(worker);
        lock(&script.0.report)
            .admit(
                1,
                CallInfo {
                    tool: "exec".into(),
                    action: None,
                    task_id: None,
                },
                0,
                Default::default(),
            )
            .unwrap();
        let ctx = crate::service::ExecContext {
            session: &fixture.session,
            launch: Some(&launch),
            cancel: tokio_util::sync::CancellationToken::new(),
            progress: None,
            call_id: None,
            parent_task_id: None,
            queued_guard: None,
            on_started: None,
            input_owner: Default::default(),
        };
        let delivery = ProgramDelivery {
            script: &script,
            pending_id: 1,
            is_control: false,
        };
        let outcome = encode_service_error(
            crate::service::ServiceError::Io("hello λ\n".into()),
            &ctx,
            &delivery,
        )
        .outcome;
        if !fits {
            assert!(matches!(outcome, Outcome::Memory(_)));
            assert_eq!(pool.used(), 0);
            continue;
        }
        let (sender, receiver) = mpsc::channel();
        assert!(sender.send(SettledCall { id: 1, outcome }).is_ok());
        drop(sender);
        assert_eq!(pool.used(), expected.len());
        let SettledCall {
            outcome: Outcome::Value(payload),
            ..
        } = receiver.recv().unwrap()
        else {
            panic!("a completed bridge result");
        };
        assert_eq!(payload.value, expected);
        let runtime = Runtime::new().unwrap();
        let context = Context::full(&runtime).unwrap();
        context.with(|ctx| {
            let parse: Function = ctx
                .eval("(value) => { const p = JSON.parse(value); return p.error === 'hello λ\\n' && p.code === 'IO'; }")
                .unwrap();
            assert!(parse.call::<_, bool>((payload.value.as_str(),)).unwrap());
            assert_eq!(pool.used(), expected.len());
        });
        drop(payload);
        assert_eq!(pool.used(), 0);
    }
}

#[tokio::test]
async fn nested_control_wire_uses_bounded_reserve_and_keeps_queue_owner() {
    let expected = r#"{"ok":false,"code":"NOT_FOUND","error":"Unknown taskId."}"#.to_owned();
    let fixture = Fixture::new();
    let needed = expected.len().max(128);
    for main_limit in [0, 64] {
        for extra in [-1isize, 0, 1] {
            let pool = BytePool::new(main_limit);
            let control = BytePool::new(needed.saturating_add_signed(extra));
            let launch = Arc::new(
                LaunchContext::new(fixture.dir.clone(), Vec::new())
                    .with_memory(Arc::clone(&pool))
                    .with_control_memory(Arc::clone(&control)),
            );
            let (_control, script, worker) = prepare_worker().unwrap();
            drop(worker);
            lock(&script.0.report)
                .admit(
                    1,
                    CallInfo {
                        tool: "exec".into(),
                        action: Some("kill".into()),
                        task_id: None,
                    },
                    0,
                    Default::default(),
                )
                .unwrap();
            let ctx = crate::service::ExecContext {
                session: &fixture.session,
                launch: Some(&launch),
                cancel: tokio_util::sync::CancellationToken::new(),
                progress: None,
                call_id: None,
                parent_task_id: None,
                queued_guard: None,
                on_started: None,
                input_owner: Default::default(),
            };
            let delivery = ProgramDelivery {
                script: &script,
                pending_id: 1,
                is_control: true,
            };
            let outcome = encode_service_error(
                crate::service::ServiceError::NotFound("Unknown taskId.".into()),
                &ctx,
                &delivery,
            )
            .outcome;
            if extra < 0 {
                assert!(matches!(outcome, Outcome::Memory(_)));
                assert_eq!((pool.used(), control.used()), (0, 0));
                continue;
            }
            let (sender, receiver) = mpsc::channel();
            sender.send(SettledCall { id: 1, outcome }).unwrap();
            drop(sender);
            assert_eq!(pool.used(), 0);
            assert_eq!(control.used(), expected.len());
            let SettledCall {
                outcome: Outcome::Value(value),
                ..
            } = receiver.recv().unwrap()
            else {
                panic!("a control response");
            };
            assert_eq!(value.value, expected);
            drop(value);
            assert_eq!(control.used(), 0);
        }
    }
}

#[tokio::test]
async fn nested_control_replies_complete_with_main_full_and_sleep_stays_ordinary() {
    let mut fixture = Fixture::new();
    let main = BytePool::new(0);
    let control = BytePool::new(8 << 20);
    fixture.session = Session::new(
        "control".into(),
        "control".into(),
        Arc::new(
            fixture
                .session
                .launch
                .for_session()
                .with_memory(Arc::clone(&main))
                .with_control_memory(Arc::clone(&control)),
        ),
    );
    let (snapshot, output) = fixture.result(
        "const listed = await tools.exec({action:'list',scope:'all'});\n\
         if(!Array.isArray(listed.tasks)) throw new Error('list did not return');\n\
         try { await tools.exec({action:'kill',taskId:'aaaaaaaa-1'}); throw new Error('expected unknown task'); }\n\
         catch(e) { if(!String(e).includes('Unknown taskId')) throw e; }\n\
         try { await sleep(0); throw new Error('sleep used control reserve'); }\n\
         catch(e) { if(!String(e).includes('not dispatched')) throw e; }\n\
         text('control-complete');"
    ).await;
    assert_eq!(snapshot.exit_code, Some(0), "{output}");
    assert_eq!(output, "control-complete\n");
    assert_eq!((main.used(), control.used()), (0, 0));
}

#[tokio::test]
async fn text_buffer_nested_file_round_trip_releases_rust_staging() {
    let mut fixture = Fixture::new();
    let pool = BytePool::new(2 << 20);
    fixture.session = Session::new(
        "text-bridge".into(),
        "text-bridge".into(),
        Arc::new(
            fixture
                .session
                .launch
                .for_session()
                .with_memory(Arc::clone(&pool)),
        ),
    );
    let (snapshot, output) = fixture.result(
        "await tools.write({path:'small.txt',content:'alpha\\nbeta\\n'});\n\
         const whole = await tools.read({path:'small.txt'});\n\
         const slice = await tools.read({path:'small.txt',offset:2,limit:1});\n\
         if(whole.text !== 'alpha\\nbeta\\n' || slice.text !== 'beta\\n') throw new Error('text changed');\n\
         text('text-round-trip');"
    ).await;
    assert_eq!(snapshot.exit_code, Some(0), "{output}");
    assert_eq!(output, "text-round-trip\n");
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn text_buffer_bridge_control_classification_preserves_main_admission() {
    let fixture = Fixture::new();
    let memory = BytePool::new(0);
    let control = BytePool::new(4096);
    let launch = Arc::new(
        fixture
            .session
            .launch
            .for_session()
            .with_memory(Arc::clone(&memory))
            .with_control_memory(Arc::clone(&control)),
    );
    let (_control, script, worker) = prepare_worker().unwrap();
    let mut host = Host {
        service: Arc::clone(&fixture.service),
        session: Arc::clone(&fixture.session),
        launch,
        transfer: false,
        call_id: None,
        parent_task_id: "aaaaaaaa-0".into(),
        runtime: Handle::current(),
        script: script.clone(),
        start_permits: Arc::new(Semaphore::new(0)),
        io_permits: Arc::new(Semaphore::new(0)),
        next: 1,
        starts: 0,
    };
    let error = unwrap_sync_err(host.call("read", r#"{"path":"small.txt"}"#));
    assert!(sync_error_message(&error.value).contains("not dispatched"));
    assert!(lock(&script.0.report).pending.is_empty());
    assert!(
        control.used() > 0,
        "the synchronous error still owns its input workspace"
    );
    drop(error);
    assert_eq!(control.used(), 0);
    let sleep_error = unwrap_sync_err(host.call("sleep", "0"));
    assert!(sync_error_message(&sleep_error.value).contains("not dispatched"));
    assert!(lock(&script.0.report).pending.is_empty());
    drop(sleep_error);
    assert_eq!(control.used(), 0);
    unwrap_sync_ok(host.call("exec", r#"{"action":"kill","taskId":"aaaaaaaa-1"}"#));
    assert!(control.used() > 0);
    script.control().stop();
    tokio::time::timeout(Duration::from_secs(5), async {
        while *lock(&script.0.in_flight.0) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let pending = script.finish();
    assert!(
        control.used() > 0,
        "temporary report strings still hold their input allowance"
    );
    drop(pending);
    drop(host);
    drop(worker);
    // The waiting VM thread drops its receiver after the worker channel closes;
    // a completed control reply keeps its wire owner until that happens.
    tokio::time::timeout(Duration::from_secs(5), async {
        while control.used() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(memory.used(), 0);
    assert_eq!(control.used(), 0);
}

#[tokio::test]
async fn report_snapshot_reserves_spare_capacity_and_retains_its_owner() {
    let script = ScriptControl::new(std::thread::current());
    let mut tool = String::with_capacity(128);
    tool.push_str("read");
    let mut handle = String::with_capacity(256);
    handle.push_str("task-1.abcd");
    script.set_cancelled_calls(vec![CallInfo {
        tool,
        action: None,
        task_id: None,
    }]);
    script.record_started(handle);
    let memory = BytePool::new(1024);
    let control = BytePool::new(4096);
    assert!(script.report(&memory, None).is_none());
    assert_eq!(memory.used(), 0);
    let snapshot = script.report(&memory, Some(&control)).unwrap();
    assert!(control.used() > memory.limit());
    assert_eq!(snapshot.cancelled[0].tool, "read");
    assert_eq!(snapshot.started, ["task-1.abcd"]);
    script.record_started("task-2.abcd".into());
    assert_eq!(snapshot.started, ["task-1.abcd"]);
    assert_eq!(
        script
            .report(crate::lease::process_pool(), None)
            .unwrap()
            .started
            .len(),
        2
    );
    let retained = control.used();
    assert!(retained > 0);
    assert!(script.report(&memory, Some(&control)).is_none());
    assert_eq!(control.used(), retained);
    drop(snapshot);
    assert_eq!(control.used(), 0);
    assert_eq!(memory.used(), 0);
}

#[test]
fn bridge_budget_replaces_arguments_with_results_and_releases_them() {
    let info = || CallInfo {
        tool: "read".into(),
        action: None,
        task_id: None,
    };
    let mut report = Report::default();
    report
        .admit(1, info(), BRIDGE_LIMIT - 4, Default::default())
        .unwrap();
    report.admit(2, info(), 4, Default::default()).unwrap();
    assert_eq!(
        report.admit(3, info(), 1, Default::default()).unwrap_err(),
        BRIDGE_ERROR
    );
    assert!(!report.result(1, BRIDGE_LIMIT));
    assert_eq!(report.bridge_bytes, BRIDGE_LIMIT);
    assert_eq!(report.pending[&1].bytes, BRIDGE_LIMIT - 4);
    report.remove(1);
    assert_eq!(report.bridge_bytes, 4);
    assert!(report.result(2, BRIDGE_LIMIT));
    assert_eq!(report.bridge_bytes, BRIDGE_LIMIT);
    report.remove(2);
    assert_eq!(report.bridge_bytes, 0);
    report.remove(1);
    assert!(report.pending.is_empty());
    report.admit(3, info(), 8, Default::default()).unwrap();
    assert!(report.result(3, 12));
    assert_eq!(report.bridge_bytes, 12);
    report.remove(3);
    assert_eq!(report.bridge_bytes, 0);
}

#[tokio::test]
async fn queued_starts_keep_submission_order_when_workers_poll_in_reverse() {
    let pool = Arc::new(Semaphore::new(1));
    let occupied = pool.clone().acquire_owned().await.unwrap();
    let first = queue_permit(pool.clone());
    let second = queue_permit(pool.clone());
    let mut second = std::pin::pin!(second);
    drop(occupied);
    let first_permit = tokio::select! {
        biased;
        _ = &mut second => panic!("the second worker jumped the start queue"),
        permit = first => permit,
    };
    drop(first_permit);
    drop(second.await);
    assert_eq!(pool.available_permits(), 1);
}

#[tokio::test]
async fn start_history_limit_counts_admitted_calls_without_spawning_processes() {
    let fixture = Fixture::new();
    let runtime = Handle::current();
    let (_control, script, worker) = prepare_worker().unwrap();
    drop(worker);
    let mut host = Host {
        service: Arc::clone(&fixture.service),
        session: Arc::clone(&fixture.session),
        launch: Arc::clone(&fixture.session.launch),
        transfer: false,
        call_id: None,
        parent_task_id: "aaaaaaaa-0".into(),
        runtime,
        script: script.clone(),
        start_permits: Arc::new(Semaphore::new(0)),
        io_permits: Arc::new(Semaphore::new(0)),
        next: 1,
        starts: MAX_STARTS - 1,
    };
    let args = json!({"action":"start","command":"true"}).to_string();
    unwrap_sync_ok(host.call("exec", &args));
    assert_eq!(host.starts, MAX_STARTS);
    assert_eq!(
        sync_error_message(&unwrap_sync_err(host.call("exec", &args)).value),
        STARTS_ERROR
    );
    assert_eq!(host.starts, MAX_STARTS);
    // Unvalidated start requests resolve as tool errors without consuming start slots.
    unwrap_sync_ok(host.call("exec", &json!({"action":"start","command":""}).to_string()));
    assert_eq!(host.starts, MAX_STARTS);
    assert_eq!(lock(&script.0.report).pending.len(), 2);
    unwrap_sync_ok(host.call("read", &json!({"path":"missing"}).to_string()));
    assert_eq!(host.starts, MAX_STARTS);
    script.control().stop();
    tokio::task::yield_now().await;
    assert!(fixture.tasks.all().is_empty());
}

#[test]
fn pending_diagnostics_bound_long_identifiers_without_changing_the_report() {
    let calls = vec![
        CallInfo {
            tool: "exec".into(),
            action: Some("poll".into()),
            task_id: Some("任务".repeat(4096))
        };
        3
    ];
    let text = pending_text(&calls);
    assert!(text.len() <= DIAGNOSTIC_RESERVE as usize / 2);
    assert!(text.contains("2 call(s) omitted"));
    assert!(text.starts_with("exec poll 任务"));
    assert_eq!(calls[0].task_id.as_ref().unwrap().chars().count(), 8192);
}

#[test]
fn validates_code_and_only_script_options() {
    let launch = LaunchContext::current().unwrap();
    for code in ["", " \n\t", "text('\0')"] {
        let request: Request = serde_json::from_value(json!({"code": code})).unwrap();
        assert!(validate(&request, &launch).is_err());
    }
    let request: Request =
        serde_json::from_value(json!({"code": "x".repeat(MAX_CODE + 1)})).unwrap();
    assert!(validate(&request, &launch).unwrap_err().contains("256 KiB"));
    for extra in [
        json!({"cwd":"definitely-missing-relative-dir-fastexec"}),
        json!({"pty":true}),
        json!({"raw":true}),
        json!({"waitMs":240001}),
        json!({"returnWhen":{"screenContains":["x"]}}),
    ] {
        let mut value = json!({"code":"return 1"});
        value
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        assert!(
            serde_json::from_value::<Request>(value)
                .map_or(true, |r| validate(&r, &launch).is_err())
        );
    }
    let request: Request = serde_json::from_value(
        json!({"code":"return '\\0'", "cwd": ".", "waitMs":0, "killAfterMs":0}),
    )
    .unwrap();
    assert!(validate(&request, &launch).is_ok());
}

#[tokio::test]
async fn text_console_return_and_exit() {
    let fixture = Fixture::new();
    let (state, output) = fixture.result("text('first'); await null; console.info('a', {b:2}); console.warn('warn'); console.error('error'); console.debug('debug'); return {done:true};").await;
    assert_eq!(state.exit_code, Some(0), "{output}");
    assert_eq!(
        output,
        "first\na {\"b\":2}\nwarn\nerror\ndebug\n{\"done\":true}\n"
    );
    for (code, expected) in [
        ("return 'last'", "last\n"),
        ("return undefined", ""),
        (
            r#"
            const value = {
                value: 42,
                get [Symbol.toStringTag]() { throw new Error("tag unavailable"); },
            };
            console.log(value);
            value.self = value;
            console.log(value);
            return "done";
            "#,
            "{\"value\":42}\n[unprintable]\ndone\n",
        ),
        (
            "text('before'); try { exit(); } catch(e) { text('caught'); } text('after');",
            "before\n",
        ),
        (
            r#"
            const tools = globalThis.tools;
            const console = globalThis.console;
            const cyc = Object.create(null);
            cyc.self = cyc;
            const big = Object.create(null);
            big.n = 1n;
            Object.defineProperty(Error, Symbol.hasInstance, {
                value() { throw new Error('broken instanceof'); },
                configurable: true,
            });
            const oddErr = new TypeError('bad');
            oddErr.name = Object.create(null);
            oddErr.stack = '';
            console.log(cyc, big, Symbol('tag'), oddErr);
            return typeof tools.read;
            "#,
            "[object Object] [object Object] Symbol(tag) [object Object]: bad\nfunction\n",
        ),
    ] {
        let (state, output) = fixture.result(code).await;
        assert_eq!(state.exit_code, Some(0), "{output}");
        assert_eq!(output, expected);
    }
}

#[tokio::test]
async fn diagnostics_are_bounded_before_rust_copy_and_admitted_before_vm() {
    for fits in [false, true] {
        let mut fixture = Fixture::new();
        let control = BytePool::new(DIAGNOSTIC_WORKSPACE - usize::from(!fits));
        fixture.session = Session::new(
            "diagnostic".into(),
            "diagnostic".into(),
            Arc::new(
                fixture
                    .session
                    .launch
                    .for_session()
                    .with_control_memory(Arc::clone(&control)),
            ),
        );
        for code in [
            "throw 'é'.repeat(2100);",
            "const e = new Error('é'.repeat(2100)); e.stack = 'stack'.repeat(1100); throw e;",
        ] {
            let (state, output) = fixture.result(code).await;
            assert_eq!(state.exit_code, Some(1), "{output}");
            assert!(output.len() <= DIAGNOSTIC_RESERVE as usize, "{output}");
            if fits {
                assert!(output.starts_with("Script error: "));
                assert!(output.contains('é'));
                assert!(output.ends_with("...\n"));
            } else {
                assert_eq!(
                    output,
                    "Script error: diagnostic memory budget is full; the script was not started.\n"
                );
            }
            assert_eq!(control.used(), 0);
        }
    }
}

#[tokio::test]
async fn diagnostic_workspace_stays_with_running_vm_until_final_output() {
    let mut fixture = Fixture::new();
    let control = BytePool::new(DIAGNOSTIC_WORKSPACE);
    fixture.session = Session::new(
        "diagnostic".into(),
        "diagnostic".into(),
        Arc::new(
            fixture
                .session
                .launch
                .for_session()
                .with_control_memory(Arc::clone(&control)),
        ),
    );
    let ((task, _, _), _) = fixture.start("await sleep(60000);", Some(5000));
    tokio::time::timeout(Duration::from_secs(2), async {
        while control.used() != DIAGNOSTIC_WORKSPACE {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!*task.done().borrow());
    task.kill().unwrap();
    complete(&task).await;
    let output = std::fs::read_to_string(&task.log_path).unwrap();
    assert!(output.starts_with("Script stopped: "));
    assert_eq!(control.used(), 0);
}

#[tokio::test]
async fn failures_keep_source_lines_and_unhandled_rejections() {
    let fixture = Fixture::new();
    for (code, message) in [
        ("text('one');\nthrow new Error('boom')", "boom"),
        ("\nconst = 1", "SyntaxError"),
        (
            "Promise.reject(new Error('unhandled')); return 'done'",
            "unhandled",
        ),
        ("await new Promise(() => {})", STALLED),
    ] {
        let (state, output) = fixture.result(code).await;
        assert_eq!(state.exit_code, Some(1), "{output}");
        assert!(output.contains("Script error:"), "{output}");
        assert!(output.contains(message), "{output}");
        if code.starts_with("text('one')") {
            assert!(output.contains("codemode.js:2"), "{output}");
        }
    }
    let (state, output) = fixture.result("const promise = Promise.reject(new Error('handled')); await null; promise.catch(() => {}); return 'ok';").await;
    assert_eq!(state.exit_code, Some(0), "{output}");
    assert_eq!(output, "ok\n");

    for (code, expected) in [
        ("throw Symbol('boom')", "cannot be described"),
        ("throw Object.create(null)", "cannot be described"),
        (
            "const e = new Error('msg'); Object.defineProperty(e, 'name', {get(){throw 1}}); Object.defineProperty(e, 'stack', {value: Symbol('s')}); throw e;",
            "Error: msg",
        ),
    ] {
        let (state, output) = fixture.result(code).await;
        assert_eq!(state.exit_code, Some(1), "{output}");
        assert!(output.contains(expected), "{output}");
    }
    let rt = Runtime::new().unwrap();
    let ctx = Context::full(&rt).unwrap();
    ctx.with(|ctx| {
        let val: Value = ctx
            .eval("const e = new Error(); Object.defineProperty(e, 'name', {get(){throw 1}}); Object.defineProperty(e, 'message', {get(){throw 2}}); Object.defineProperty(e, 'stack', {get(){throw 3}}); e")
            .unwrap();
        assert_eq!(describe(&ctx, val), "Error: ");
        assert!(!ctx.has_exception());
        let sym: Value = ctx.eval("Symbol('x')").unwrap();
        assert!(describe(&ctx, sym).contains("cannot be described"));
        assert!(!ctx.has_exception());
    });
}

#[tokio::test]
async fn file_tools_resolve_errors_reject_bad_arguments_and_queue() {
    let fixture = Fixture::new();
    let code = r#"
        const write = await tools.write({path:'fixture.txt',content:'old\n'});
        if ('ok' in write || 'structuredContent' in write || write.bytes !== 4 || !write.path.endsWith('/fixture.txt')) throw new Error('write shape');
        const edit = await tools.edit({path:'fixture.txt',edits:[{oldText:'old',newText:'new'}]});
        if ('ok' in edit || edit.replacements !== 1 || edit.edits[0].line !== 1) throw new Error('edit failed');
        let missingErr = null;
        try { await tools.read({path:'missing.txt'}); } catch (e) { missingErr = e; }
        if (!(missingErr instanceof ToolError) || !(missingErr instanceof Error) || missingErr.name !== 'ToolError' || missingErr.tool !== 'read' || missingErr.code !== 'NOT_FOUND') throw new Error('missing file must reject with ToolError');
        let badExecErr = null;
        try { await tools.exec({action:'start',command:''}); } catch (e) { badExecErr = e; }
        if (!(badExecErr instanceof ToolError) || !(badExecErr instanceof Error) || badExecErr.name !== 'ToolError' || badExecErr.tool !== 'exec' || badExecErr.code !== 'VALIDATION') throw new Error('empty exec must reject with ToolError');
        for (const args of [null, [], 'x', {path:'fixture.txt',surprise:true}]) {
            let rejected = false;
            try { await tools.read(args); } catch (e) { if (e instanceof TypeError) rejected = true; }
            if (!rejected) throw new Error('invalid arguments resolved');
        }
        for (const args of [null, {action:'bogus'}, {action:'list',surprise:true}]) {
            let rejected = false;
            try { await tools.exec(args); } catch (e) { if (e instanceof TypeError) rejected = true; }
            if (!rejected) throw new Error('invalid exec schema resolved');
        }
        try { tools.codemode({code:'return 1'}); throw new Error('unknown tool accepted'); }
        catch(e) { if (!(e instanceof TypeError)) throw e; }
        const results = await Promise.all(Array.from({length:20}, () => tools.read({path:'fixture.txt'})));
        if (results.some(r => 'ok' in r || r.text !== 'new\n' || r.truncated !== false || r.totalLines !== 1)) throw new Error('queued read failed');
        text(results.length);
    "#;
    let (state, output) = fixture.result(code).await;
    assert_eq!(state.exit_code, Some(0), "{output}");
    assert_eq!(output, "20\n");
    assert_eq!(
        std::fs::read_to_string(fixture.dir.join("fixture.txt")).unwrap(),
        "new\n"
    );
}

#[tokio::test]
async fn bridge_survives_replaced_builtins() {
    let fixture = Fixture::new();
    std::fs::write(fixture.dir.join("fixture.txt"), "kept").unwrap();
    let (state, output) = fixture.result(r#"
        const promise = tools.read({path:'fixture.txt'});
        Map.prototype.get = Map.prototype.set = Map.prototype.delete = () => { throw Error('patched Map'); };
        JSON.parse = JSON.stringify = () => { throw Error('patched JSON'); };
        Promise.prototype.then = () => { throw Error('patched then'); };
        Function.prototype.call = Function.prototype.bind = () => { throw Error('patched function'); };
        const result = await promise;
        text({ok: result.text === 'kept' && !('ok' in result)});
        return 'done';
    "#).await;
    assert_eq!(state.exit_code, Some(0), "{output}");
    assert_eq!(output, "{\"ok\":true}\ndone\n");
}

#[tokio::test]
async fn returning_cancels_calls_that_were_not_awaited() {
    let fixture = Fixture::new();
    let ((task, _, _), _) =
        fixture.start("for (let i=0;i<20;i++) sleep(60000); return 'done';", None);
    complete(&task).await;
    assert_eq!(task.snapshot().exit_code, Some(0));
    let script = task.script().unwrap();
    assert!(script.cancel_token().is_cancelled());
    let report = script.report(crate::lease::process_pool(), None).unwrap();
    assert_eq!(report.cancelled.len(), 20);
    assert!(report.cancelled.iter().all(|call| call.tool == "sleep"));
    assert!(report.started.is_empty());
    assert_eq!(std::fs::read_to_string(&task.log_path).unwrap(), "done\n");
}

#[tokio::test]
async fn finishing_waits_for_a_slow_inflight_start_to_be_reported() {
    let fixture = Fixture::new();
    let ((task, _, _), bridge) = fixture.start("while (true) {}", None);
    // Hold an admitted start before its task is registered, past the old five-second drain.
    let in_flight = InFlightGuard::enter(bridge.clone());
    lock(&bridge.0.report)
        .admit(
            1,
            CallInfo {
                tool: "exec".into(),
                action: Some("start".into()),
                task_id: None,
            },
            0,
            Default::default(),
        )
        .unwrap();
    task.kill().unwrap();
    let still_running =
        tokio::time::timeout(Duration::from_secs(6), task.done().wait_for(|done| *done))
            .await
            .is_err();
    let handle = "late-start.abcd".to_owned();
    bridge.control().record_started(handle.clone());
    drop(in_flight);
    complete(&task).await;
    assert!(
        still_running,
        "script finished before the admitted start registered"
    );
    assert_eq!(
        bridge
            .control()
            .report(crate::lease::process_pool(), None)
            .unwrap()
            .started,
        vec![handle]
    );
    assert_eq!(task.snapshot().state, "killed");
}

#[tokio::test]
async fn interrupts_bytecode_regex_and_job_loops() {
    let fixture = Fixture::new();
    for code in [
        "while(true) {}",
        "while(true) await null",
        "new RegExp('(a+)+$').test('a'.repeat(100000)+'b')",
    ] {
        let ((task, _, _), _) = fixture.start(code, None);
        tokio::time::sleep(Duration::from_millis(30)).await;
        let start = Instant::now();
        task.kill().unwrap();
        complete(&task).await;
        assert!(
            start.elapsed() < Duration::from_millis(200),
            "stop took {:?}",
            start.elapsed()
        );
        let state = task.snapshot();
        assert_eq!(state.state, "killed");
        assert_eq!(state.exit_code, None);
        assert!(!state.expired);
    }
    let ((task, _, _), _) = fixture.start("while(true) {}", Some(100));
    complete(&task).await;
    let state = task.snapshot();
    assert_eq!(state.state, "killed");
    assert_eq!(state.exit_code, None);
    assert!(state.expired);
}

#[tokio::test]
async fn memory_stack_output_and_call_limits_finish_as_errors() {
    let fixture = Fixture::new();
    for code in [
        "const a=[]; while(true) a.push(new Array(65536).fill(1));",
        "function recurse(){return recurse()} recurse();",
        "const chunk='x'.repeat(1024*1024); for(let i=0;i<65;i++) text(chunk);",
        "await Promise.all(Array.from({length:1033}, () => tools.read({path:'missing'})));",
    ] {
        let (state, output) = fixture.result(code).await;
        assert_eq!(
            state.exit_code,
            Some(1),
            "{}",
            &output[output.len().saturating_sub(2048)..]
        );
        assert!(output.contains("Script error:"));
        assert!(output.len() <= TASK_LOG_LIMIT as usize);
    }
}

#[tokio::test]
async fn attach_path_admits_cwd_and_home_before_resolving() {
    for path in ["missing.png", "~/missing.png"] {
        for extra in [-1isize, 0, 1] {
            let mut fixture = Fixture::new();
            let home = fixture.dir.join("session-home");
            let launch = LaunchContext::new(
                fixture.dir.clone(),
                vec![
                    ("HOME".into(), home.clone().into_os_string()),
                    ("USERPROFILE".into(), home.into_os_string()),
                ],
            );
            let needed = launch.path_source_bytes(path) * 32 + 4096;
            let pool = crate::lease::BytePool::new(needed.checked_add_signed(extra).unwrap());
            fixture.session = Session::new(
                "attach-path-budget".into(),
                "attach-path-budget".into(),
                Arc::new(launch.with_memory(Arc::clone(&pool))),
            );
            let (state, output) = fixture.result(&format!("attach({path:?});")).await;
            assert_eq!(state.exit_code, Some(1), "{output}");
            if extra < 0 {
                assert!(
                    output.contains("budget") && output.contains("preparing the file path"),
                    "{output}"
                );
                assert!(!output.contains("Cannot read"), "{output}");
            } else {
                assert!(output.contains("Cannot read"), "{output}");
                assert!(!output.contains("memory budget"), "{output}");
            }
            assert_eq!(pool.used(), 0);
        }
    }
}

#[tokio::test]
async fn inline_whitespace_copy_is_admitted_before_allocation() {
    let mut fixture = Fixture::new();
    let pool = crate::lease::BytePool::new(4096);
    fixture.session = Session::new(
        "inline-budget".into(),
        "inline-budget".into(),
        Arc::new(
            LaunchContext::new(fixture.dir.clone(), Vec::new()).with_memory(Arc::clone(&pool)),
        ),
    );
    let mut bytes = Vec::new();
    image::DynamicImage::new_rgb8(1, 1)
        .write_to(
            &mut std::io::Cursor::new(&mut bytes),
            image::ImageFormat::Png,
        )
        .unwrap();
    let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
    let code = format!("attach('data:image/png;base64,' + ' '.repeat(8192) + {encoded:?});");
    let (state, output) = fixture.result(&code).await;
    assert_eq!(state.exit_code, Some(1), "{output}");
    assert!(output.contains("active memory budget is full"), "{output}");
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn nested_attachment_registry_reuses_handles_and_rolls_back_owners() {
    let fixture = Fixture::new();
    let ((producer, _, _), _) = fixture.start("return 'source';", Some(5000));
    let ((other, _, _), _) = fixture.start("return 'other source';", Some(5000));
    complete(&producer).await;
    complete(&other).await;
    let mut png = Vec::new();
    image::DynamicImage::new_rgb8(1, 1)
        .write_to(&mut std::io::Cursor::new(&mut png), image::ImageFormat::Png)
        .unwrap();
    for task in [&producer, &producer, &producer, &other] {
        fixture
            .tasks
            .store_attachment(task, crate::media::MediaKind::Image, "image/png", &png)
            .unwrap();
    }
    let mut attachments = producer.unseen_attachments(0);
    attachments.extend(other.unseen_attachments(0));
    assert_eq!(attachments[0].meta().index, attachments[3].meta().index);

    let pool = BytePool::new(64 * 1024);
    let (_control, script, worker) = prepare_worker().unwrap();
    drop(worker);
    {
        let mut batch = script.nested_attachments(&pool, None);
        assert_eq!(batch.register(attachments[0].clone()), Some(1));
        assert_eq!(batch.register(attachments[1].clone()), Some(2));
    }
    assert_eq!(script.test_attachment_count(), 0);
    assert_eq!(pool.used(), 0, "a wholly refused batch releases its table");

    {
        let mut batch = script.nested_attachments(&pool, None);
        assert_eq!(batch.register(attachments[0].clone()), Some(1));
        assert_eq!(batch.register(attachments[0].clone()), Some(1));
        batch.disarm();
    }
    let first_bytes = pool.used();
    assert_eq!(first_bytes, script.test_attachment_bytes());
    assert!(first_bytes > attachments[0].metadata_bytes());
    {
        let mut batch = script.nested_attachments(&pool, None);
        assert_eq!(batch.register(attachments[1].clone()), Some(2));
        assert_eq!(batch.register(attachments[0].clone()), Some(1));
    }
    let retained = pool.used();
    assert_eq!(retained, script.test_attachment_bytes());
    assert!(retained > first_bytes, "the grown table stays charged");
    assert_eq!(script.test_attachment_count(), 1);
    assert!(
        script
            .get_attachment(1)
            .unwrap()
            .same_attachment(&attachments[0])
    );
    assert!(script.get_attachment(2).is_none());

    let pressure = pool.try_acquire(pool.limit() - retained).unwrap();
    {
        let mut batch = script.nested_attachments(&pool, None);
        assert_eq!(batch.register(attachments[0].clone()), Some(1));
        assert_eq!(batch.register(attachments[1].clone()), None);
    }
    drop(pressure);
    assert_eq!(pool.used(), retained);
    {
        let mut batch = script.nested_attachments(&pool, None);
        assert_eq!(batch.register(attachments[1].clone()), Some(2));
        batch.disarm();
    }

    let table_bytes =
        pool.used() - attachments[0].metadata_bytes() - attachments[1].metadata_bytes();
    let growth_peak = attachments[2].metadata_bytes() + 2 * table_bytes;
    for (available, fits) in [(growth_peak - 1, false), (growth_peak, true)] {
        let resident = pool.used();
        let pressure = pool
            .try_acquire(pool.limit() - resident - available)
            .unwrap();
        {
            let mut batch = script.nested_attachments(&pool, None);
            let id = batch.register(attachments[2].clone());
            assert_eq!(id, fits.then_some(3));
            if fits {
                batch.disarm();
            }
        }
        drop(pressure);
        assert_eq!(script.test_attachment_count(), if fits { 3 } else { 2 });
        assert_eq!(pool.used(), script.test_attachment_bytes());
        if !fits {
            assert_eq!(pool.used(), resident);
        }
    }

    let mut uncommitted = script.nested_attachments(&pool, None);
    assert_eq!(uncommitted.register(attachments[3].clone()), Some(4));
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let waiter = {
        let script = script.clone();
        let pool = Arc::clone(&pool);
        let first = attachments[0].clone();
        let fourth = attachments[3].clone();
        std::thread::spawn(move || {
            entered_tx.send(()).unwrap();
            let mut batch = script.nested_attachments(&pool, None);
            let first = batch.register(first);
            let fourth = batch.register(fourth);
            batch.disarm();
            drop(batch);
            done_tx.send((first, fourth)).unwrap();
        })
    };
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(done_rx.try_recv().is_err());
    drop(uncommitted);
    assert_eq!(
        done_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        (Some(1), Some(4))
    );
    waiter.join().unwrap();
    assert_eq!(script.test_attachment_count(), 4);
    for (index, attachment) in attachments.iter().enumerate() {
        assert!(
            script
                .get_attachment(index as u64 + 1)
                .unwrap()
                .same_attachment(attachment)
        );
    }
    assert_eq!(pool.used(), script.test_attachment_bytes());
    drop(script.finish());
    assert_eq!(script.test_attachment_count(), 0);
    assert!(script.get_attachment(1).is_none());
    assert_eq!(pool.used(), 0);

    let primary = BytePool::new(attachments[0].metadata_bytes());
    let control = BytePool::new(64 * 1024);
    let (_control, fallback, worker) = prepare_worker().unwrap();
    drop(worker);
    for commit in [false, true] {
        {
            let mut batch = fallback.nested_attachments(&primary, Some(&control));
            assert_eq!(batch.register(attachments[0].clone()), Some(1));
            assert_eq!(batch.register(attachments[1].clone()), Some(2));
            if commit {
                batch.disarm();
            }
        }
        assert_eq!(fallback.test_attachment_count(), if commit { 2 } else { 0 });
        assert_eq!(
            primary.used() + control.used(),
            fallback.test_attachment_bytes()
        );
        assert_eq!(primary.used() > 0, commit);
        assert_eq!(control.used() > 0, commit);
    }
    drop(fallback.finish());
    assert_eq!((primary.used(), control.used()), (0, 0));
}

#[tokio::test]
async fn attach_covers_r4_4_inline_and_paths_opaque_handles_and_lease_cleanup() {
    let fixture = Fixture::new();
    let mut png_bytes = Vec::new();
    image::DynamicImage::new_rgb8(1, 1)
        .write_to(
            &mut std::io::Cursor::new(&mut png_bytes),
            image::ImageFormat::Png,
        )
        .unwrap();
    let png_b64 = base64::engine::general_purpose::STANDARD.encode(&png_bytes);

    // Minimal ISO BMFF `ftyp` MP4 and EBML WebM payloads for R4-4 parity checks.
    let mut mp4_bytes = Vec::new();
    mp4_bytes.extend_from_slice(&20u32.to_be_bytes());
    mp4_bytes.extend_from_slice(b"ftypisom");
    mp4_bytes.extend_from_slice(&0u32.to_be_bytes());
    mp4_bytes.extend_from_slice(b"mp42");
    std::fs::write(fixture.dir.join("clip.mp4"), &mp4_bytes).unwrap();
    let mp4_b64 = base64::engine::general_purpose::STANDARD.encode(&mp4_bytes);

    let webm_bytes: Vec<u8> = vec![
        0x1A, 0x45, 0xDF, 0xA3, 0x8B, 0x42, 0x82, 0x84, b'w', b'e', b'b', b'm', 0x42, 0x87, 0x81,
        0x02,
    ];
    std::fs::write(fixture.dir.join("clip.webm"), &webm_bytes).unwrap();
    let webm_b64 = base64::engine::general_purpose::STANDARD.encode(&webm_bytes);

    // 1. R4-4 parity: identical kind, MIME, and raw bytes across local path, MCP resource, and data URL;
    //    conflicting declared MIME (`video/mp4` on PNG bytes) and old `image()` global are rejected.
    let r4_code = format!(
        r#"
        RegExp.prototype.exec = String.prototype.replace = () => {{ throw new Error("patched"); }};
        if (typeof globalThis.image !== "undefined") throw new Error("legacy image() must be gone");

        const mp4Desc = await tools.read({{path: "clip.mp4"}});
        if (mp4Desc.attachable !== true || mp4Desc.kind !== "video" || mp4Desc.mimeType !== "video/mp4" || mp4Desc.bytes !== {mp4_len}) {{
            throw new Error("mp4Desc: " + JSON.stringify(mp4Desc));
        }}
        const a1 = attach("clip.mp4");
        const a2 = attach({{ type: "resource", resource: {{ blob: {mp4_b64:?}, mimeType: "video/mp4" }} }});
        const a3 = attach("data:video/mp4;base64," + {mp4_b64:?});
        for (const [idx, item] of [[1, a1], [2, a2], [3, a3]]) {{
            if (item.index !== idx || item.kind !== "video" || item.mimeType !== "video/mp4" || item.bytes !== {mp4_len}) {{
                throw new Error("mp4 mismatch at " + idx + ": " + JSON.stringify(item));
            }}
        }}

        const w1 = attach(await tools.read({{path: "clip.webm"}}));
        const w2 = attach({{ type: "resource", resource: {{ blob: {webm_b64:?}, mimeType: "video/webm" }} }});
        const w3 = attach("DATA:VIDEO/WEBM;BASE64,\n  " + {webm_b64:?});
        for (const [idx, item] of [[4, w1], [5, w2], [6, w3]]) {{
            if (item.index !== idx || item.kind !== "video" || item.mimeType !== "video/webm" || item.bytes !== {webm_len}) {{
                throw new Error("webm mismatch at " + idx + ": " + JSON.stringify(item));
            }}
        }}

        let conflictRejected = false;
        try {{
            attach("data:video/mp4;base64," + {png_b64:?});
        }} catch (e) {{
            if (e instanceof TypeError && e.message.includes("conflicts")) conflictRejected = true;
        }}
        if (!conflictRejected) throw new Error("conflicting MIME must reject");
        return "r4-4-ok";
        "#,
        mp4_len = mp4_bytes.len(),
        webm_len = webm_bytes.len(),
    );
    let ((r4_task, _, _), _) = fixture.start(&r4_code, Some(5000));
    complete(&r4_task).await;
    let r4_out = std::fs::read_to_string(&r4_task.log_path).unwrap();
    assert_eq!(r4_task.snapshot().exit_code, Some(0), "{r4_out}");
    assert_eq!(r4_out, "r4-4-ok\n");
    for idx in 1..=3 {
        assert_eq!(
            std::fs::read(r4_task.attachment_path(idx, "video/mp4")).unwrap(),
            mp4_bytes
        );
    }
    for idx in 4..=6 {
        assert_eq!(
            std::fs::read(r4_task.attachment_path(idx, "video/webm")).unwrap(),
            webm_bytes
        );
    }

    // 2. Opaque task attachment handles from nested `poll` and `kill`: reusable without consumption,
    //    unforgeable against clones/spoofed `__hid`, and `AttachmentLease` pins source files across
    //    source-task eviction until the consumer script exits or is cancelled!
    for action in ["poll", "kill"] {
        let ((producer, _, _), _) = fixture.start(
            &format!("attach('data:image/png;base64,{png_b64}'); attach('clip.mp4');"),
            Some(5000),
        );
        complete(&producer).await;
        let src_att1 = producer.attachment_path(1, "image/png");
        assert!(src_att1.is_file());

        let ((consumer, _, _), consumer_bridge) = fixture.start(
            &format!(
                r#"
                const r = await tools.exec({{action: {action:?}, taskId: {pid:?}}});
                const list = r.codemode.attachments;
                if (!Array.isArray(list) || list.length !== 2) throw new Error("expected 2 handles");
                const h = list[0];
                if (h.index !== 1 || h.kind !== "image" || h.mimeType !== "image/png" || h.bytes !== {png_len}) {{
                    throw new Error("handle meta: " + JSON.stringify(h));
                }}
                if ("__hid" in h || !Object.isFrozen(h)) throw new Error("handle must be opaque & frozen");

                // Attaching twice does NOT consume the handle.
                const c1 = attach(h);
                const c2 = attach(h);
                if (c1.index !== 1 || c2.index !== 2 || c2.bytes !== {png_len}) {{
                    throw new Error("reuse failed");
                }}

                // Forged or cloned objects cannot spoof an attachment handle.
                for (const fake of [{{ ...h }}, {{ index: 1, kind: "image", mimeType: "video/mp4", bytes: 1, __hid: 1 }}]) {{
                    let rejected = false;
                    try {{ attach(fake); }} catch (e) {{ if (e instanceof TypeError) rejected = true; }}
                    if (!rejected) throw new Error("forged handle accepted");
                }}

                const next = await tools.exec({{action: "poll", taskId: r.taskId, waitMs: 0}});
                if (next.codemode?.attachments) throw new Error("handles delivered twice");

                for (let i = 0; i < 2; i++) {{
                    const replay = await tools.exec({{action: "poll", taskId: {pid:?}, waitMs: 0}});
                    if (replay.codemode?.attachments?.length !== 2) throw new Error("fresh reader missed attachments");
                    if (attach(replay.codemode.attachments[0]).bytes !== {png_len}) throw new Error("replayed handle failed");
                }}
                if (attach(h).bytes !== {png_len}) throw new Error("original handle expired");

                text("PINNED");
                await sleep(30000);
                "#,
                pid = producer.id,
                png_len = png_bytes.len(),
            ),
            Some(10000),
        );

        // Wait until `consumer` reaches `PINNED`.
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if std::fs::read_to_string(&consumer.log_path)
                .unwrap_or_default()
                .contains("PINNED")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(
            std::fs::read_to_string(&consumer.log_path)
                .unwrap()
                .contains("PINNED")
        );

        assert_eq!(
            consumer_bridge.test_attachment_count(),
            2,
            "fresh readers reuse the same two source attachment slots"
        );

        // Evict `producer` while `consumer` is still running: `src_att1` stays pinned on disk!
        fixture.tasks.evict_log(&producer);
        assert!(
            src_att1.exists(),
            "AttachmentLease must pin source file while consumer script runs"
        );

        // Cancel/kill `consumer`: once `consumer` exits, its `AttachmentLease` drops and unlinks `src_att1`!
        consumer.kill().unwrap();
        complete(&consumer).await;
        assert!(
            !src_att1.exists(),
            "killing consumer script must release AttachmentLease and unlink evicted file"
        );
        assert_eq!(
            std::fs::read(consumer.attachment_path(1, "image/png")).unwrap(),
            png_bytes
        );
        assert_eq!(
            std::fs::read(consumer.attachment_path(2, "image/png")).unwrap(),
            png_bytes
        );
    }

    // 3. Per-task 1024 attachment cap (above old 64 cap) and disk-quota pre-write rejection with zero residue.
    let (many_state, many_out) = fixture
        .result(&format!(
            r#"
            for (let i = 1; i <= 1024; i++) {{
                const r = attach("data:image/png;base64,{png_b64}");
                if (r.index !== i) throw new Error("index " + r.index);
            }}
            let capHit = false;
            try {{ attach("data:image/png;base64,{png_b64}"); }}
            catch (e) {{ if (e instanceof RangeError && e.message.includes("1024")) capHit = true; }}
            if (!capHit) throw new Error("expected 1025th attach to throw RangeError");
            return "cap-1024-ok";
            "#
        ))
        .await;
    assert_eq!(many_state.exit_code, Some(0), "{many_out}");
    assert_eq!(many_out, "cap-1024-ok\n");

    // Tight disk quota rejects oversized attachment before writing any orphan file.
    let quota_dir = std::env::temp_dir().join(format!(
        "fastexec-script-quota-{}",
        id::random_chars(12).unwrap()
    ));
    let mut tight_tasks = Tasks::new_in(Limits::OFF, quota_dir.clone()).unwrap();
    Arc::get_mut(&mut tight_tasks).unwrap().set_log_limit(256);
    let tight_service = Arc::new(Service::new(Arc::clone(&tight_tasks), false));
    let tight_session = Session::new(
        "tight-quota".into(),
        "tight-quota".into(),
        Arc::new(LaunchContext::new(
            quota_dir.clone(),
            std::env::vars_os().collect(),
        )),
    );
    let payload_b64 = base64::engine::general_purpose::STANDARD.encode({
        let mut big_png = png_bytes.clone();
        big_png.resize(512, 0);
        big_png
    });
    let ((tight_task, _, _), _) = spawn_script(
        &tight_tasks,
        ScriptSpawnArgs {
                code: format!(
                    r#"
                    let quotaRejected = false;
                    try {{ attach("data:image/png;base64,{payload_b64}"); }}
                    catch (e) {{ if (e instanceof RangeError && e.message.includes("4 GiB")) quotaRejected = true; }}
                    if (!quotaRejected) throw new Error("expected disk quota RangeError");
                    return "quota-ok";
                    "#
                ),
                launch: Arc::clone(&tight_session.launch),
                kill_after_ms: Some(5000),
                transfer: false,
                call_id: None,
                service: tight_service,
                input_owner: Default::default(),
            },
            &tight_session,
            "q001".into(),
        )
        .unwrap();
    complete(&tight_task).await;
    let tight_out = std::fs::read_to_string(&tight_task.log_path).unwrap();
    assert_eq!(tight_task.snapshot().exit_code, Some(0), "{tight_out}");
    assert!(
        !tight_task.attachment_path(1, "image/png").exists(),
        "rejected attachment must leave no orphan file"
    );
    tight_tasks.shutdown();
    let _ = std::fs::remove_dir_all(&quota_dir);
}

#[tokio::test]
async fn text_encoder_and_decoder_handle_utf8_streams_surrogates_bom_and_tampered_builtins() {
    let fixture = Fixture::new();
    let code = r#"
        const enc = new TextEncoder();
        if (enc.encoding !== "utf-8" || Object.prototype.toString.call(enc) !== "[object TextEncoder]") {
            throw new Error("encoder metadata");
        }
        if (enc.encode().length !== 0) throw new Error("empty encode");

        // Round-trip UTF-8 + base64 for CJK and Astral plane characters.
        const sample = "你好，fastexec 🚀 café";
        const encoded = enc.encode(sample);
        let binary = "";
        for (let i = 0; i < encoded.length; i++) binary += String.fromCharCode(encoded[i]);
        const b64 = btoa(binary);
        const raw = atob(b64);
        const restoredBytes = Uint8Array.from(raw, c => c.charCodeAt(0));
        const dec = new TextDecoder("  UTF-8\t\n");
        if (dec.encoding !== "utf-8" || dec.fatal !== false || dec.ignoreBOM !== false) {
            throw new Error("decoder metadata");
        }
        if (dec.decode(restoredBytes) !== sample) throw new Error("utf8 base64 roundtrip");

        // Lone high/low surrogates become U+FFFD without corrupting neighbors or pairs.
        for (const [bad, expected] of [
            ["\uD800", "\uFFFD"],
            ["A\uD800B", "A\uFFFDB"],
            ["\uDC00X", "\uFFFDX"],
            ["\uD800\uD83D\uDE80", "\uFFFD🚀"],
        ]) {
            if (dec.decode(enc.encode(bad)) !== expected) {
                throw new Error("surrogate mismatch: " + expected);
            }
        }

        // encodeInto never writes partial multi-byte sequences when capacity runs short.
        const target = new Uint8Array(5);
        const step1 = enc.encodeInto("a🚀b", target);
        if (step1.read !== 3 || step1.written !== 5 || dec.decode(target) !== "a🚀") {
            throw new Error("encodeInto pair fit: " + JSON.stringify(step1));
        }
        const tight = new Uint8Array(4);
        const step2 = enc.encodeInto("a🚀", tight);
        if (step2.read !== 1 || step2.written !== 1 || tight[0] !== 0x61) {
            throw new Error("encodeInto stopped before 4-byte pair: " + JSON.stringify(step2));
        }

        // Subarray views, DataView, ArrayBuffer, BOM stripping, ignoreBOM, and streaming splits.
        const padded = new Uint8Array([0x99, 0xEF, 0xBB, 0xBF, 0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD, 0x88]);
        const sub = padded.subarray(1, 10);
        if (dec.decode(sub) !== "你好") throw new Error("default BOM strip on subarray");
        if (dec.decode(sub) !== "你好") throw new Error("second non-stream decode must also strip BOM");
        const keepBom = new TextDecoder("utf8", { ignoreBOM: true });
        if (keepBom.decode(new DataView(padded.buffer, 1, 9)) !== "\uFEFF你好") {
            throw new Error("ignoreBOM DataView");
        }

        // Split BOM and 4-byte emoji across 1-byte streaming chunks; keep mid-stream U+FEFF.
        const streamBytes = new Uint8Array([
            0xEF, 0xBB, 0xBF,
            0xF0, 0x9F, 0x9A, 0x80,
            0xEF, 0xBB, 0xBF
        ]);
        const streamDec = new TextDecoder();
        let streamed = "";
        streamed += streamDec.decode(new Uint8Array(0), { stream: true });
        for (let i = 0; i < streamBytes.length; i++) {
            streamed += streamDec.decode(streamBytes.subarray(i, i + 1), { stream: true });
        }
        streamed += streamDec.decode();
        if (streamed !== "🚀\uFEFF") throw new Error("streaming decode: " + JSON.stringify(streamed));

        // WHATWG maximal subpart replacement and fatal mode.
        if (dec.decode(new Uint8Array([0xE2, 0x82, 0x41])) !== "\uFFFDA") {
            throw new Error("maximal subpart truncated 3-byte");
        }
        if (dec.decode(new Uint8Array([0xF0, 0x80, 0x80, 0x41])) !== "\uFFFD\uFFFD\uFFFDA") {
            throw new Error("maximal subpart overlong 4-byte");
        }
        if (dec.decode(new Uint8Array([0xED, 0xA0, 0x80])) !== "\uFFFD\uFFFD\uFFFD") {
            throw new Error("CESU-8 surrogate rejected");
        }
        const fatalDec = new TextDecoder("utf-8", { fatal: true });
        for (const invalid of [
            new Uint8Array([0xFF]),
            new Uint8Array([0xE2, 0x82]),
            new Uint8Array([0xED, 0xA0, 0x80]),
        ]) {
            let threw = false;
            try { fatalDec.decode(invalid); } catch (e) { if (e instanceof TypeError) threw = true; }
            if (!threw) throw new Error("fatal decoder accepted invalid bytes");
        }

        // Streaming fatal error keeps doNotFlush and bomSeen per WHATWG Encoding § 7.2,
        // while an early fatal error before serializing does not prematurely mark bomSeen.
        const fatalStream = new TextDecoder("utf-8", { fatal: true });
        if (fatalStream.decode(new Uint8Array([0xEF, 0xBB, 0xBF, 0x41]), { stream: true }) !== "A") {
            throw new Error("initial BOM strip in fatal stream");
        }
        let midStreamThrew = false;
        try { fatalStream.decode(new Uint8Array([0xFF]), { stream: true }); }
        catch (e) { if (e instanceof TypeError) midStreamThrew = true; }
        if (!midStreamThrew) throw new Error("expected mid-stream TypeError");
        if (fatalStream.decode(new Uint8Array([0xEF, 0xBB, 0xBF, 0x42]), { stream: true }) !== "\uFEFFB") {
            throw new Error("mid-stream U+FEFF after fatal error must be preserved");
        }
        if (fatalStream.decode() !== "") throw new Error("stream flush");
        if (fatalStream.decode(new Uint8Array([0xEF, 0xBB, 0xBF, 0x43])) !== "C") {
            throw new Error("new stream after flush must strip leading BOM again");
        }

        const earlyFatal = new TextDecoder("utf-8", { fatal: true });
        let earlyThrew = false;
        try { earlyFatal.decode(new Uint8Array([0x41, 0xFF]), { stream: true }); }
        catch (e) { if (e instanceof TypeError) earlyThrew = true; }
        if (!earlyThrew) throw new Error("expected early TypeError");
        if (earlyFatal.decode(new Uint8Array([0xEF, 0xBB, 0xBF, 0x42]), { stream: true }) !== "B") {
            throw new Error("failed first chunk must not commit bomSeen");
        }

        // WebIDL ToString coercion (.toString() before .valueOf()) and argument validation order.
        const labelObj = { valueOf() { return 123; }, toString() { return "utf-8"; } };
        const strObj = { valueOf() { return 456; }, toString() { return "ok"; } };
        const coercedDec = new TextDecoder(labelObj);
        if (coercedDec.decode(enc.encode(strObj)) !== "ok") {
            throw new Error("ToString coercion order");
        }
        const intoBuf = new Uint8Array(4);
        if (enc.encodeInto(strObj, intoBuf).written !== 2 || dec.decode(intoBuf.subarray(0, 2)) !== "ok") {
            throw new Error("encodeInto ToString coercion");
        }
        let optsBeforeLabel = false;
        try { new TextDecoder("latin1", "bad-options"); }
        catch (e) { if (e instanceof TypeError) optsBeforeLabel = true; }
        if (!optsBeforeLabel) throw new Error("options TypeError must precede label RangeError");
        let inputBeforeOpts = false;
        try {
            dec.decode("not-bytes", { get stream() { throw new Error("options accessed too early"); } });
        } catch (e) {
            if (e instanceof TypeError) inputBeforeOpts = true;
        }
        if (!inputBeforeOpts) throw new Error("input TypeError must precede options getter");

        // Detached, mid-call detached, shrunk/grown Resizable ArrayBuffer, and immutable ArrayBuffer.
        const detAb = new Uint8Array([0x61, 0x62]).buffer;
        const detTa = new Uint8Array(detAb);
        const detDv = new DataView(detAb);
        detAb.transfer();
        if (dec.decode(detAb) !== "" || dec.decode(detTa) !== "" || dec.decode(detDv) !== "") {
            throw new Error("detached BufferSource must decode as empty");
        }
        const detInto = enc.encodeInto("abc", detTa);
        if (detInto.read !== 0 || detInto.written !== 0) {
            throw new Error("detached encodeInto must return 0/0");
        }
        const midAb = new Uint8Array([0x61, 0x62]).buffer;
        const midTa = new Uint8Array(midAb);
        if (dec.decode(midTa, { get stream() { midAb.transfer(); return false; } }) !== "") {
            throw new Error("mid-call detach must decode as empty");
        }
        const rab = new ArrayBuffer(8, { maxByteLength: 16 });
        const oobTa = new Uint8Array(rab, 4, 4);
        const oobDv = new DataView(rab, 4, 4);
        const trackTa = new Uint8Array(rab);
        rab.resize(2);
        if (dec.decode(oobTa) !== "" || dec.decode(oobDv) !== "") {
            throw new Error("OOB RAB views must decode as empty");
        }
        const oobInto = enc.encodeInto("abc", oobTa);
        if (oobInto.read !== 0 || oobInto.written !== 0) {
            throw new Error("OOB RAB encodeInto must return 0/0");
        }
        rab.resize(6);
        if (enc.encodeInto("hello!", trackTa).written !== 6 || dec.decode(trackTa) !== "hello!") {
            throw new Error("grown RAB tracking view failed");
        }
        if (typeof ArrayBuffer.prototype.transferToImmutable === "function") {
            const immAb = enc.encode("immutable-ok").buffer.transferToImmutable();
            const immTa = new Uint8Array(immAb);
            if (dec.decode(immAb) !== "immutable-ok" || dec.decode(immTa) !== "immutable-ok") {
                throw new Error("immutable ArrayBuffer decode failed");
            }
            for (const s of ["", "x"]) {
                let immRejected = false;
                try { enc.encodeInto(s, immTa); } catch (e) { if (e instanceof TypeError) immRejected = true; }
                if (!immRejected) throw new Error("immutable ArrayBuffer encodeInto must reject");
            }
        }

        // Unsupported labels and invalid arguments reject with RangeError / TypeError.
        let badLabel = false;
        try { new TextDecoder("latin1"); } catch (e) { if (e instanceof RangeError) badLabel = true; }
        if (!badLabel) throw new Error("latin1 should fail");
        for (const call of [
            () => TextEncoder(),
            () => TextDecoder(),
            () => enc.encode(Symbol("s")),
            () => enc.encodeInto(),
            () => enc.encodeInto("a"),
            () => enc.encodeInto(Symbol("s"), new Uint8Array(4)),
            () => enc.encodeInto("a", new Int8Array(4)),
            () => new TextDecoder(Symbol("s")),
            () => dec.decode("not-bytes"),
            () => TextEncoder.prototype.encode("a"),
            () => TextDecoder.prototype.decode(new Uint8Array(0)),
        ]) {
            let rejected = false;
            try { call(); } catch (e) { if (e instanceof TypeError) rejected = true; }
            if (!rejected) throw new Error("expected TypeError");
        }

        // Survives prototype mutation of Object (including get/set and [0]), TypedArray, Array, String, and WeakMap,
        // including surrogate pairs split across the 1024-unit internal chunk boundary and re-entrant options getters.
        const taProto = Object.getPrototypeOf(Uint8Array.prototype);
        const origTaLen = Object.getOwnPropertyDescriptor(taProto, "length");
        Object.defineProperty(taProto, "length", { __proto__: null, get() { throw new Error("patched TA length"); }, configurable: true });
        Object.defineProperty(Object.prototype, 0, { __proto__: null, set() { throw new Error("patched Object[0]"); }, configurable: true });
        Object.defineProperty(Object.prototype, "get", { __proto__: null, value() { throw new Error("patched desc get"); }, configurable: true });
        Object.defineProperty(Object.prototype, "set", { __proto__: null, value() { throw new Error("patched desc set"); }, configurable: true });
        Array.prototype.push = Array.prototype.join = () => { throw new Error("patched Array"); };
        String.prototype.charCodeAt = String.prototype.slice = () => { throw new Error("patched String"); };
        WeakMap.prototype.get = WeakSet.prototype.has = () => { throw new Error("patched Weak"); };
        const big = "中🚀ab".repeat(3000);
        const bigBytes = enc.encode(big);
        const nestedOpts = {
            get stream() {
                if (new TextDecoder().decode(enc.encode("nested-🚀")) !== "nested-🚀") {
                    throw new Error("re-entrant decode failed");
                }
                return false;
            }
        };
        if (dec.decode(bigBytes, nestedOpts) !== big) {
            throw new Error("failed multi-chunk roundtrip after prototype mutation");
        }
        const scratch = new Uint8Array(16);
        if (enc.encodeInto("中🚀", scratch).written !== 7 || dec.decode(scratch.subarray(0, 7)) !== "中🚀") {
            throw new Error("encodeInto failed under prototype mutation");
        }
        delete Object.prototype[0];
        delete Object.prototype.get;
        delete Object.prototype.set;
        Object.defineProperty(taProto, "length", origTaLen);
        return "codec-ok";
    "#;
    let (state, output) = fixture.result(code).await;
    assert_eq!(state.exit_code, Some(0), "{output}");
    assert_eq!(output, "codec-ok\n");
}

#[tokio::test]
async fn text_decoder_retains_unread_input_after_fatal_stream_errors() {
    let fixture = Fixture::new();
    let code = r#"
        const decoder = () => new TextDecoder("utf-8", { fatal: true });
        const rejects = (dec, bytes, stream = true) => {
            try { dec.decode(bytes, { stream }); }
            catch (error) { if (error instanceof TypeError) return; throw error; }
            throw new Error("expected invalid UTF-8");
        };
        const retained = decoder();
        const source = new Uint8Array([0xFF, 0x61]);
        rejects(retained, source);
        source[1] = 0x7A;
        if (retained.decode(new Uint8Array([0x62]), { stream: true }) !== "ab") {
            throw new Error("unread bytes must be copied and precede the next input");
        }
        const restored = decoder();
        rejects(restored, new Uint8Array([0xE2, 0x41, 0x42]));
        if (restored.decode() !== "AB") throw new Error("invalid continuation must be restored");
        const repeated = decoder();
        rejects(repeated, new Uint8Array([0xE2, 0xFF, 0x41]));
        rejects(repeated, new Uint8Array(0));
        if (repeated.decode(new Uint8Array([0x42])) !== "AB") {
            throw new Error("successive errors must consume only the offending sequence");
        }
        for (const prior of ["", "X"]) {
            const bom = decoder();
            if (prior) bom.decode(new Uint8Array([0x58]), { stream: true });
            rejects(bom, new Uint8Array([0x41, 0xFF, 0xEF, 0xBB, 0xBF, 0x42]));
            if (bom.decode() !== (prior ? "\uFEFFB" : "B")) {
                throw new Error("failed output must not change BOM state");
            }
        }
        const reset = decoder();
        rejects(reset, new Uint8Array([0xFF, 0x61]), false);
        if (reset.decode(new Uint8Array([0x62])) !== "b") {
            throw new Error("non-streaming failure must reset the input queue");
        }
        rejects(reset, new Uint8Array([0xFF, 0xE2]));
        rejects(reset, new Uint8Array(0), false);
        if (reset.decode(new Uint8Array([0x41])) !== "A") {
            throw new Error("failed flush must reset the decoder");
        }
        return "queue-ok";
    "#;
    let (state, output) = fixture.result(code).await;
    assert_eq!(state.exit_code, Some(0), "{output}");
    assert_eq!(output, "queue-ok\n");
}

#[tokio::test]
async fn nested_read_uses_the_one_mib_window_and_reads_long_lines() {
    let fixture = Fixture::new();
    let long_line = format!("{{\"payload\":\"{}\"}}\n", "x".repeat(120 * 1024));
    std::fs::write(fixture.dir.join("long.jsonl"), &long_line).unwrap();
    let many_lines: String = (1..=3500).map(|i| format!("row-{i}\n")).collect();
    std::fs::write(fixture.dir.join("rows.txt"), &many_lines).unwrap();
    let (state, output) = fixture
        .result(
            r#"
            const [longRes, rowsRes] = await Promise.all([
                tools.read({path: "long.jsonl"}),
                tools.read({path: "rows.txt"}),
            ]);
            if (longRes.truncated || longRes.lineTooLong) {
                throw new Error("long line truncated");
            }
            const parsed = JSON.parse(longRes.text);
            if (parsed.payload.length !== 120 * 1024) throw new Error("payload length");
            if (rowsRes.truncated || rowsRes.shownLines !== 3500 || rowsRes.totalLines !== 3500) {
                throw new Error("rows truncated: " + JSON.stringify(rowsRes));
            }
            return "nested-window-ok";
        "#,
        )
        .await;
    assert_eq!(state.exit_code, Some(0), "{output}");
    assert_eq!(output, "nested-window-ok\n");
}

#[tokio::test]
async fn sh_quote_image_notes_queue_cap_and_integrity_tool_errors() {
    let fixture = Fixture::new();
    // Create a small PNG and a sparse >100 MiB PNG header for unattachable descriptor read.
    let img =
        image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(4, 4, image::Rgb([1, 2, 3])));
    let mut png = Vec::new();
    img.write_to(&mut std::io::Cursor::new(&mut png), image::ImageFormat::Png)
        .unwrap();
    std::fs::write(fixture.dir.join("ok.png"), &png).unwrap();
    {
        use std::io::Write;
        let mut huge = std::fs::File::create(fixture.dir.join("huge.png")).unwrap();
        huge.write_all(&png[..33]).unwrap();
        huge.set_len(105 * 1024 * 1024).unwrap();
    }

    let (state, output) = fixture
        .result(
            r#"
            if (!Object.isFrozen(sh) || typeof sh.quote !== "function") {
                throw new Error("sh not frozen");
            }
            const cases = [
                ["", "''"],
                ["hello world", "'hello world'"],
                ["it's $HOME `id` \\ \n", "'it'\\''s $HOME `id` \\ \n'"],
                [42, "'42'"],
                [-3.5, "'-3.5'"],
            ];
            for (const [inp, exp] of cases) {
                const got = sh.quote(inp);
                if (got !== exp) throw new Error("sh.quote mismatch: " + got + " !== " + exp);
            }
            for (const bad of [null, undefined, true, {}, [], NaN, Infinity, -Infinity, "a\0b", Symbol("x")]) {
                let threw = false;
                try { sh.quote(bad); } catch (e) { if (e instanceof TypeError) threw = true; }
                if (!threw) throw new Error("expected TypeError for " + String(bad));
            }
            const okImg = await tools.read({path: "ok.png"});
            if (okImg.attachable !== true || okImg.bytes === undefined || okImg.data !== undefined) {
                throw new Error("okImg descriptor shape: " + JSON.stringify(okImg));
            }
            attach(okImg);
            const bigImg = await tools.read({path: "huge.png"});
            if (bigImg.attachable !== false || bigImg.bytes !== undefined || bigImg.sourceBytes !== 105 * 1024 * 1024 || typeof bigImg.note !== "string" || !bigImg.note.includes("exceeds FASTEXEC_MAX_MEDIA_BYTES")) {
                throw new Error("bigImg shape: " + JSON.stringify(bigImg));
            }
            let noteThrown = false;
            try { attach(bigImg); } catch (e) {
                if (e instanceof TypeError && e.message === bigImg.note) noteThrown = true;
            }
            if (!noteThrown) throw new Error("expected attach(bigImg) to throw bigImg.note");
            return "helpers-ok";
            "#,
        )
        .await;
    assert_eq!(state.exit_code, Some(0), "{output}");
    assert!(output.contains("helpers-ok"), "{output}");

    // End-to-end JS ToolError on injected captureError and logError: non-empty message
    // built from diagnostics and full report (output, exitCode, diagnostic) in err.result,
    // plus the 1025th pending call rejecting with ToolError(LIMIT).
    let ((seeded, _, _), _) = fixture.start("text('partial-out'); throw new Error('boom');", None);
    complete(&seeded).await;
    seeded.inject_io_errors(Some("pty pipe lost"), Some("disk full"));
    let seeded_id = seeded.id.clone();
    let (io_state, io_out) = fixture
        .result(&format!(
            r#"
            let caught = null;
            try {{
                await tools.exec({{action: "poll", taskId: {seeded_id:?}, waitMs: 0}});
            }} catch (e) {{
                caught = e;
            }}
            if (!caught || caught.name !== "ToolError" || caught.tool !== "exec") {{
                throw new Error("expected ToolError: " + String(caught));
            }}
            if (!caught.message.includes("output capture failed: pty pipe lost") ||
                !caught.message.includes("log write failed: disk full")) {{
                throw new Error("unexpected ToolError.message: " + caught.message);
            }}
            if (caught.result.exitCode !== 1 ||
                !caught.result.output.startsWith("partial-out\n") ||
                caught.result.captureError !== "pty pipe lost" ||
                caught.result.logError !== "disk full") {{
                throw new Error("unexpected ToolError.result: " + JSON.stringify(caught.result));
            }}

            const settled = await Promise.allSettled(
                Array.from({{length: 1025}}, () => tools.read({{path: "ok.png"}}))
            );
            const limitErrors = settled.filter(
                s => s.status === "rejected" && s.reason instanceof ToolError && s.reason instanceof Error && s.reason.code === "LIMIT"
            );
            if (limitErrors.length !== 1) {{
                throw new Error("expected 1 ToolError(LIMIT) at 1025th pending call, got " + limitErrors.length);
            }}
            return "io-and-queue-ok";
            "#
        ))
        .await;
    assert_eq!(io_state.exit_code, Some(0), "{io_out}");
    assert!(io_out.contains("io-and-queue-ok"), "{io_out}");
}

#[tokio::test]
async fn call_site_stacks_and_serializable_return_values() {
    let fixture = Fixture::new();
    let (st_uncaught, out_uncaught) = fixture
        .result("const x = 1;\nawait tools.read({path: 'no-such-file-xyz'});\nreturn x;")
        .await;
    assert_eq!(st_uncaught.exit_code, Some(1), "{out_uncaught}");
    assert!(out_uncaught.contains("codemode.js:2"), "{out_uncaught}");

    let (st_caught, out_caught) = fixture
        .result(
            r#"
            let toolStack = "";
            try { await tools.read({path: "missing-2"}); } catch (e) { toolStack = e.stack; }
            let typeStack = "";
            try { await tools.exec({action: "bogus"}); } catch (e) { typeStack = e.stack; }
            const holds = Array.from({length: 1024}, () => sleep(60000));
            let limitStack = "";
            try { await sleep(60000); } catch (e) { if (e?.name === "ToolError" && e.code === "LIMIT") limitStack = e.stack; }
            if (!toolStack.includes("codemode.js:3")) throw new Error("toolStack: " + toolStack);
            if (!typeStack.includes("codemode.js:5")) throw new Error("typeStack: " + typeStack);
            if (!limitStack.includes("codemode.js:8")) throw new Error("limitStack: " + limitStack);
            return "stacks-ok";
            "#,
        )
        .await;
    assert_eq!(st_caught.exit_code, Some(0), "{out_caught}");
    assert_eq!(out_caught, "stacks-ok\n");

    for bad in [
        "return Symbol('x')",
        "return () => 1",
        "return 1n",
        "const c = {}; c.self = c; return c;",
    ] {
        let (st, out) = fixture.result(bad).await;
        assert_eq!(st.exit_code, Some(1), "{bad}: {out}");
        assert!(
            out.contains("Script error: TypeError: the return value is not JSON-serializable"),
            "{bad}: {out}"
        );
    }

    let (st_null, out_null) = fixture.result("return null").await;
    assert_eq!((st_null.exit_code, out_null.as_str()), (Some(0), "null\n"));
    let (st_str, out_str) = fixture.result("return \"a\\nb\"").await;
    assert_eq!((st_str.exit_code, out_str.as_str()), (Some(0), "a\nb\n"));
}

#[test]
fn very_large_sleep_splits_preserve_time_and_allow_exact_chunk_subtraction() {
    for ms in [
        (1_u128 << 92) as f64,
        (1_u128 << 93) as f64,
        ((1_u128 << 93) + (1_u128 << 41)) as f64,
        (1_u128 << 94) as f64,
        f64::MAX,
    ] {
        let mut deferred = Vec::new();
        let part = split_sleep(ms, &mut deferred);
        assert!(part <= (1_u128 << 92) as f64);
        let chunk = SLEEP_CHUNK_MS as f64;
        assert_eq!(part - (part - chunk), chunk);
        let restored = deferred.iter().rev().fold(part, |sum, half| sum + half);
        assert_eq!(restored, ms);
        assert!(deferred.len() <= 932);
    }
}

#[tokio::test(start_paused = true)]
async fn long_sleep_keeps_every_chunk_of_the_requested_delay() {
    let chunk = Duration::from_millis(SLEEP_CHUNK_MS);
    let sleeping = tokio::spawn(sleep((SLEEP_CHUNK_MS * 2 + 1) as f64));
    tokio::task::yield_now().await;
    for _ in 0..2 {
        tokio::time::advance(chunk).await;
        tokio::task::yield_now().await;
        assert!(
            !sleeping.is_finished(),
            "sleep discarded the remaining delay"
        );
    }
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert!(sleeping.is_finished());
    sleeping.await.unwrap();
}

#[tokio::test]
async fn first_line_stack_columns_match_later_lines_and_keep_custom_stacks() {
    fn columns(output: &str, line: u32) -> Vec<u32> {
        let location = format!("codemode.js:{line}:");
        output
            .lines()
            .filter(|line| line.starts_with("    at "))
            .filter_map(|frame| frame.rsplit_once(&location))
            .map(|(_, column)| column.trim_end_matches(')').parse().unwrap())
            .collect()
    }

    let fixture = Fixture::new();
    for code in [
        "throw new Error('codemode.js:1:99');",
        "const = 1;",
        "return new Error('direct').stack;",
        "console.error(new Error('console'));",
        "await tools.read({path: 'missing-column.txt'});",
        "await tools.exec({action: 'bogus'});",
        "try { await tools.read({path: 'missing-column.txt'}); } catch (e) { return e.stack; }",
        "try { await tools.exec({action: 'bogus'}); } catch (e) { return e.stack; }",
        "const holds = Array.from({length: 1024}, () => sleep(60000)); try { await sleep(60000); } catch (e) { return e.stack; }",
        "const bad = {toJSON() { throw new Error('inner'); }}; try { await tools.write({path: 'unused', content: bad}); } catch (e) { return e.stack; }",
        "let saved; try { await tools.read({path: 'missing-column.txt'}); } catch (e) { saved = e; } try { await tools.write({path: 'unused', content: {toJSON() { throw saved; }}}); } catch (e) { return e.stack; }",
    ] {
        let (first_state, first) = fixture.result(code).await;
        let (later_state, later) = fixture.result(&format!("\n{code}")).await;
        assert_eq!(
            first_state.exit_code, later_state.exit_code,
            "{first}\n{later}"
        );
        let first_columns = columns(&first, 1);
        let later_columns = columns(&later, 2);
        assert!(!first_columns.is_empty(), "{code}: {first}");
        assert_eq!(first_columns, later_columns, "{code}:\n{first}\n{later}");
        if code.starts_with("throw new Error") {
            assert!(first.contains("Error: codemode.js:1:99"), "{first}");
        }
    }

    let custom = "    at custom (codemode.js:1:73)\n    at caller (custom.js:2:3)";
    for setup in [
        format!(
            "const error = new Error('custom'); Object.defineProperty(error, 'stack', {{value: {custom:?}, configurable: true}});"
        ),
        format!("const error = new Error('custom'); error.stack = {custom:?};"),
        format!(
            "class Custom extends Error {{ get stack() {{ return {custom:?}; }} }} const error = new Custom('custom');"
        ),
    ] {
        for ending in [
            "throw error;",
            "try { await tools.write({path: 'unused', content: {toJSON() { throw error; }}}); } catch (e) { return e.stack; }",
        ] {
            let (_, output) = fixture.result(&format!("{setup} {ending}")).await;
            assert!(output.contains(custom), "{output}");
        }
    }
}

#[tokio::test]
async fn sleep_primitive_race_kill_and_validation() {
    let fixture = Fixture::new();
    let ((child_script, _, _), _) = fixture.start("await sleep(10000);", None);
    let child_id = child_script.id.clone();
    let start_t = Instant::now();
    let (st, out) = fixture
        .result(&format!(
            r#"
            await sleep(-3);
            await sleep(0);
            for (const bad of [NaN, Infinity, -Infinity, "1", null, undefined, {{}}]) {{
                let ok = false;
                try {{ await sleep(bad); }} catch (e) {{ if (e instanceof TypeError) ok = true; }}
                if (!ok) throw new Error("accepted bad sleep: " + String(bad));
            }}
            const winner = await Promise.race([
                tools.exec({{action: "poll", taskId: {child_id:?}}}),
                sleep(100).then(() => "timeout"),
            ]);
            return winner;
            "#
        ))
        .await;
    assert_eq!(st.exit_code, Some(0), "{out}");
    assert_eq!(out, "timeout\n");
    assert!(start_t.elapsed() < Duration::from_secs(3));
    child_script.kill().unwrap();
    complete(&child_script).await;

    let ((sleeping, _, _), sleeping_bridge) = fixture.start(
        "const p = sleep(Number.MAX_VALUE); text('ready'); await p;",
        None,
    );
    let wait_deadline = Instant::now() + Duration::from_secs(10);
    while lock(&sleeping_bridge.0.report).pending.is_empty() {
        assert!(
            Instant::now() < wait_deadline,
            "sleeping script did not admit sleep(Number.MAX_VALUE) in time"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let kill_t = Instant::now();
    sleeping.kill().unwrap();
    complete(&sleeping).await;
    assert!(kill_t.elapsed() < Duration::from_secs(3));
    assert_eq!(sleeping.snapshot().state, "killed");
    let report = sleeping
        .script()
        .unwrap()
        .report(crate::lease::process_pool(), None)
        .unwrap();
    assert_eq!(report.cancelled.len(), 1);
    assert_eq!(report.cancelled[0].tool, "sleep");
}

#[tokio::test]
async fn passive_waits_and_control_calls_need_no_start_or_io_permits() {
    let fixture = Fixture::new();
    std::fs::write(fixture.dir.join("probe.txt"), "fast-read\n").unwrap();

    // Start 2 background script tasks (using 2 of Limits::OFF's 4 script slots) so the
    // orchestrator script (slot 3) can hold 8 passive polls across distinct readers,
    // read files, list, and kill without starvation.
    let ((bg1, _, _), _) = fixture.start("text('ready'); await sleep(60000);", None);
    let ((bg2, _, _), _) = fixture.start("await sleep(60000);", None);
    let id1 = bg1.id.clone();
    let id2 = bg2.id.clone();

    let t0 = Instant::now();
    let (st, out) = fixture
        .result(&format!(
            r#"
            const polls1 = Array.from({{length: 8}}, (_, i) =>
                tools.exec({{action: "poll", taskId: {id1:?} + ".a00" + i, waitMs: 2000}})
                    .then(() => "poll_done")
            );
            const polls2 = Array.from({{length: 8}}, (_, i) =>
                tools.exec({{action: "poll", taskId: {id2:?} + ".b00" + i}})
            );
            await sleep(20);
            // 16 passive polls are in flight simultaneously; read, list, and kill still run immediately.
            const raceWinner = await Promise.race([
                tools.read({{path: "probe.txt"}}).then(r => r.text.trim()),
                Promise.all(polls1).then(() => "polls_won"),
            ]);
            if (raceWinner !== "fast-read") throw new Error("read delayed: " + raceWinner);
            const listed = await tools.exec({{action: "list", state: "active"}});
            if (listed.tasks.length < 2) throw new Error("list failed");
            const k2 = await tools.exec({{action: "kill", taskId: {id2:?}}});
            if (k2.state !== "killed") throw new Error("kill failed: " + k2.state);
            const settled2 = await Promise.all(polls2);
            if (!settled2.every(s => s.state === "killed")) throw new Error("polls2 state");
            const k1 = await tools.exec({{action: "kill", taskId: {id1:?}}});
            if (k1.state !== "killed") throw new Error("k1 failed");
            await Promise.allSettled(polls1);
            return "permits-ok";
            "#
        ))
        .await;
    assert_eq!(st.exit_code, Some(0), "{out}");
    assert_eq!(out, "permits-ok\n");
    assert!(
        t0.elapsed() < Duration::from_secs(10),
        "took {:?}",
        t0.elapsed()
    );
}

#[tokio::test]
async fn sync_serialization_errors_keep_inner_frames_and_sleep_resolves_undefined() {
    let fixture = Fixture::new();
    let (st, out) = fixture
        .result(
            "const slept = await sleep(0);\n\
             if (slept !== undefined) throw new Error('expected undefined');\n\
             const bad = {\n\
                 toJSON() { throw new Error('inner-tojson'); }\n\
             };\n\
             try {\n\
                 await tools.write({ path: 'out.txt', content: bad });\n\
             } catch (e) {\n\
                 return e.stack;\n\
             }",
        )
        .await;
    assert_eq!(st.exit_code, Some(0), "{out}");
    assert!(
        out.contains("codemode.js:4") && out.contains("codemode.js:7"),
        "expected both inner toJSON frame and tools.write call frame: {out}"
    );
    assert!(!out.contains("codemode-prelude.js"), "{out}");
}

#[tokio::test]
async fn sh_template_join_quote_contract_and_prototype_pollution_immunity() {
    let fixture = Fixture::new();
    let (st, out) = fixture
        .result(
            r#"
            function assertEq(got, exp, label) {
                if (got !== exp) {
                    throw new Error(label + ": " + JSON.stringify(got) + " !== " + JSON.stringify(exp));
                }
            }
            function assertTypeError(fn, label) {
                let ok = false;
                try { fn(); } catch (e) { if (e instanceof TypeError) ok = true; }
                if (!ok) throw new Error("expected TypeError for " + label);
            }

            // 1. Frozen and non-constructible function objects without own .prototype.
            assertEq(typeof sh, "function", "typeof sh");
            assertEq(typeof sh.quote, "function", "typeof sh.quote");
            assertEq(typeof sh.join, "function", "typeof sh.join");
            assertEq(Object.isFrozen(sh), true, "sh frozen");
            assertEq(Object.isFrozen(sh.quote), true, "sh.quote frozen");
            assertEq(Object.isFrozen(sh.join), true, "sh.join frozen");
            for (const [name, fn] of [["sh", sh], ["sh.quote", sh.quote], ["sh.join", sh.join]]) {
                assertEq(
                    Object.prototype.hasOwnProperty.call(fn, "prototype"),
                    false,
                    name + " has no own prototype"
                );
                assertTypeError(() => new fn(), "new " + name);
            }
            for (const prop of ["quote", "join"]) {
                const desc = Object.getOwnPropertyDescriptor(sh, prop);
                assertEq(desc.writable, false, "sh." + prop + " writable");
                assertEq(desc.configurable, false, "sh." + prop + " configurable");
            }

            // 2. sh.quote valid and invalid inputs.
            assertEq(sh.quote(""), "''", "quote empty");
            assertEq(sh.quote("hello world"), "'hello world'", "quote spaces");
            assertEq(sh.quote("it's $HOME `id` \\ \n"), "'it'\\''s $HOME `id` \\ \n'", "quote special");
            assertEq(sh.quote(0), "'0'", "quote 0");
            assertEq(sh.quote(-3.5), "'-3.5'", "quote float");
            for (const bad of [null, undefined, true, false, {}, [], NaN, Infinity, -Infinity, "a\0b", Symbol("x"), 1n]) {
                assertTypeError(() => sh.quote(bad), "sh.quote(" + typeof bad + ")");
            }

            // 3. sh.join valid and invalid inputs, sparse arrays, and Proxy length tampering.
            assertEq(sh.join([]), "", "join empty");
            assertEq(sh.join(["solo"]), "'solo'", "join single");
            assertEq(
                sh.join(["a b", "it's", "line1\nline2", 42, -1.5, ""]),
                "'a b' 'it'\\''s' 'line1\nline2' '42' '-1.5' ''",
                "join multi"
            );
            for (const bad of [null, undefined, "abc", 123, { 0: "a", length: 1 }, true]) {
                assertTypeError(() => sh.join(bad), "sh.join non-array");
            }
            const sparse1 = new Array(1);
            const sparse2 = ["a", , "b"];
            for (const badArr of [
                [undefined],
                [null],
                [NaN],
                [Infinity],
                [-Infinity],
                [true],
                [{}],
                [["nested"]],
                ["a\0b"],
                [Symbol("x")],
                [1n],
                sparse1,
                sparse2,
            ]) {
                assertTypeError(() => sh.join(badArr), "sh.join invalid element");
            }
            for (const badLen of [NaN, -1, 1.5, Infinity, -Infinity, "0", "1", null, undefined]) {
                const proxied = new Proxy(["a"], {
                    get(target, prop, receiver) {
                        if (prop === "length") return badLen;
                        return Reflect.get(target, prop, receiver);
                    },
                });
                assertTypeError(() => sh.join(proxied), "sh.join Proxy length=" + String(badLen));
            }

            // 4. sh`...` tagged template: raw escapes, scalar+array, empty array, errors.
            assertEq(sh`printf '%s\n\t\s\\'`, "printf '%s\\n\\t\\s\\\\'", "raw escapes");
            assertEq(
                sh`rg ${"-n"} ${["foo bar", "a'b"]} ${123}`,
                "rg '-n' 'foo bar' 'a'\\''b' '123'",
                "mixed scalar and array"
            );
            assertEq(sh`cmd ${[]}`, "cmd ", "empty array trailing");
            assertEq(sh`ls ${[]} /tmp`, "ls  /tmp", "empty array middle");
            assertTypeError(() => sh(), "sh()");
            assertTypeError(() => sh("ls"), "sh('ls')");
            assertTypeError(() => sh(null), "sh(null)");
            assertTypeError(() => sh({}), "sh({})");
            assertTypeError(() => sh({ raw: "not-array" }), "sh raw non-array");
            assertTypeError(() => sh({ raw: [] }), "sh raw empty");
            assertTypeError(() => sh({ raw: ["a"] }, "x"), "sh raw too short");
            assertTypeError(() => sh({ raw: ["a", "b"] }), "sh raw too long");
            assertTypeError(() => sh({ raw: ["a\0b"] }), "sh raw NUL");
            assertTypeError(() => sh({ raw: [123] }), "sh raw non-string");
            assertTypeError(() => sh({ raw: [null] }), "sh raw null seg");

            const sparseRaw = new Array(2);
            sparseRaw[0] = "echo ";
            assertTypeError(() => sh({ raw: sparseRaw }, "x"), "sh sparse raw");

            let rawReads = 0;
            const oneShotOk = {
                get raw() {
                    rawReads++;
                    if (rawReads > 1) throw new Error("raw getter read more than once");
                    return ["echo ", " end"];
                },
            };
            assertEq(sh(oneShotOk, "hi"), "echo 'hi' end", "sh raw getter single read ok");
            assertEq(rawReads, 1, "raw getter read count ok");

            let badRawReads = 0;
            const oneShotMismatch = {
                get raw() {
                    badRawReads++;
                    return ["only-one"];
                },
            };
            assertTypeError(() => sh(oneShotMismatch, "extra"), "sh raw getter mismatch");
            assertEq(badRawReads, 1, "raw getter read count mismatch");

            for (const badLen of [NaN, -1, 1.5, Infinity, "1"]) {
                const proxiedRaw = new Proxy(["a"], {
                    get(target, prop, receiver) {
                        if (prop === "length") return badLen;
                        return Reflect.get(target, prop, receiver);
                    },
                });
                assertTypeError(() => sh({ raw: proxiedRaw }), "sh raw Proxy length=" + String(badLen));
            }

            for (const badVal of [undefined, null, true, {}, NaN, Infinity, -Infinity, [["nested"]], sparse1, "a\0b", Symbol("x"), 1n]) {
                assertTypeError(() => sh`echo ${badVal}`, "sh bad interpolation");
            }

            // 5. Prototype pollution immunity.
            Array.prototype.join = () => "POLLUTED_JOIN";
            Array.prototype.map = () => ["POLLUTED_MAP"];
            Array.prototype[Symbol.iterator] = function* () { yield "POLLUTED_ITER"; };
            Array.prototype[0] = "POLLUTED_ARR0";
            Object.prototype[0] = "POLLUTED_OBJ0";
            Object.prototype.raw = ["POLLUTED_RAW"];

            assertEq(sh.quote("it's"), "'it'\\''s'", "polluted quote");
            assertEq(sh.join(["a b", 7]), "'a b' '7'", "polluted join");
            assertEq(sh.join([]), "", "polluted join empty");
            assertEq(sh`printf '%s\n' ${["a b", "c'd"]}`, "printf '%s\\n' 'a b' 'c'\\''d'", "polluted template");
            assertTypeError(() => sh({}), "polluted Object.prototype.raw rejected");
            assertTypeError(() => sh.join(new Array(1)), "polluted sparse join rejected");
            const pollutedSparseRaw = new Array(1);
            assertTypeError(() => sh({ raw: pollutedSparseRaw }), "polluted sparse raw rejected");

            return "sh-contract-ok";
            "#,
        )
        .await;
    assert_eq!(st.exit_code, Some(0), "{out}");
    assert_eq!(out, "sh-contract-ok\n");
}

#[tokio::test]
async fn queued_start_guard_tracks_queued_disarmed_and_cancelled_starts() {
    let fixture = Fixture::new();
    let runtime = Handle::current();
    let (_control, script, worker) = prepare_worker().unwrap();
    drop(worker);
    let mut host = Host {
        service: Arc::clone(&fixture.service),
        session: Arc::clone(&fixture.session),
        launch: Arc::clone(&fixture.session.launch),
        transfer: false,
        call_id: None,
        parent_task_id: "aaaaaaaa-1".into(),
        runtime: runtime.clone(),
        script: script.clone(),
        start_permits: Arc::new(Semaphore::new(0)),
        io_permits: Arc::new(Semaphore::new(0)),
        next: 1,
        starts: 0,
    };
    let start_args = json!({"action":"start","command":"true"}).to_string();
    unwrap_sync_ok(host.call("exec", &start_args));
    unwrap_sync_ok(host.call("exec", &start_args));
    unwrap_sync_ok(host.call(
        "exec",
        &json!({"action":"start","command":"true","keys":[]}).to_string(),
    ));
    // Invalid start and non-start calls must not affect queued_starts.
    unwrap_sync_ok(host.call("exec", &json!({"action":"start","command":""}).to_string()));
    unwrap_sync_ok(host.call("read", &json!({"path":"missing"}).to_string()));
    assert_eq!(host.starts, 3);
    assert_eq!(script.control().list_counts(), (3, 0));

    script.control().stop();
    tokio::task::spawn_blocking(move || {
        script.cancel_and_drain();
        assert_eq!(script.control().list_counts(), (0, 3));
    })
    .await
    .unwrap();
    assert!(fixture.tasks.all().is_empty());

    // Disarming a guard before drop leaves cancelled_starts at 0 even if the script is killed later.
    let (_control, disarm_script, disarm_worker) = prepare_worker().unwrap();
    drop(disarm_worker);
    let mut guard = QueuedStartGuard::new(disarm_script.control_arc());
    assert_eq!(disarm_script.control().list_counts(), (1, 0));
    guard.disarm();
    assert_eq!(disarm_script.control().list_counts(), (0, 0));
    disarm_script.control().stop();
    drop(guard);
    assert_eq!(disarm_script.control().list_counts(), (0, 0));

    // Dropping an armed guard without cancellation (e.g. spawn error) decrements queued_starts only.
    let (_control, err_script, err_worker) = prepare_worker().unwrap();
    drop(err_worker);
    let err_guard = QueuedStartGuard::new(err_script.control_arc());
    assert_eq!(err_script.control().list_counts(), (1, 0));
    drop(err_guard);
    assert_eq!(err_script.control().list_counts(), (0, 0));
}

#[tokio::test]
async fn task_capacity_rejects_nested_start_with_overload() {
    let fixture = Fixture::with_limits(Limits {
        max_running: 1,
        ..Limits::OFF
    });
    let (state, output) = fixture
        .result(
            r#"
            try {
                await tools.exec({ action: "start", command: "printf should-not-run", waitMs: 0 });
            } catch (error) {
                if (!(error instanceof ToolError) || error.code !== "OVERLOAD") {
                    throw new Error("task capacity must be OVERLOAD: " + error.code);
                }
                if (error.result !== undefined) throw new Error("no task was started");
                return "task-capacity-ok";
            }
            throw new Error("a task was admitted over the running limit");
            "#,
        )
        .await;
    assert_eq!(state.exit_code, Some(0), "{output}");
    assert_eq!(output, "task-capacity-ok\n");
    assert_eq!(fixture.tasks.all().len(), 1);
}

#[tokio::test]
async fn program_tool_error_codes_partial_results_and_edit_matches() {
    let fixture = Fixture::new();
    std::fs::write(
        fixture.dir.join("edit.txt"),
        "exact line\n\u{201c}curly\u{201d} line\n",
    )
    .unwrap();
    // >4 MiB file for strict_whole_file LIMIT check.
    std::fs::write(fixture.dir.join("huge.txt"), vec![b'a'; (4 << 20) + 16]).unwrap();

    // Create a finished task with injected I/O errors so polling it via tools.exec triggers IO
    // while retaining output and exitCode on err.result.
    let ((io_task, _, _), _) = fixture.start(
        "text('retained-output'); throw new Error('boom');",
        Some(5000),
    );
    complete(&io_task).await;
    io_task.inject_io_errors(Some("pipe broken"), Some("disk full"));
    let io_id = io_task.id.clone();

    let script = format!(
        r#"
        const check = (cond, msg) => {{ if (!cond) throw new Error(msg); }};

        // 1. VALIDATION: head_tail on poll, maxBytes on list, empty start command
        for (const req of [
            {{ action: "list", maxBytes: 100 }},
            {{ action: "list", truncate: "head" }},
            {{ action: "poll", taskId: {io_id:?}, truncate: "head_tail", waitMs: 0 }},
            {{ action: "start", command: "" }},
        ]) {{
            let caught = null;
            try {{ await tools.exec(req); }} catch (e) {{ caught = e; }}
            check(caught?.name === "ToolError" && caught.code === "VALIDATION", "VALIDATION failed for " + JSON.stringify(req) + ": " + caught?.code);
        }}

        // 2. NOT_FOUND: missing file and unknown taskId
        let nfFile = null;
        try {{ await tools.read({{ path: "does-not-exist.txt" }}); }} catch (e) {{ nfFile = e; }}
        check(nfFile?.name === "ToolError" && nfFile.code === "NOT_FOUND", "NOT_FOUND file: " + nfFile?.code);
        let nfTask = null;
        try {{ await tools.exec({{ action: "poll", taskId: "zzzzzzzz-99", waitMs: 0 }}); }} catch (e) {{ nfTask = e; }}
        check(nfTask?.name === "ToolError" && nfTask.code === "NOT_FOUND", "NOT_FOUND task: " + nfTask?.code);

        // 3. LIMIT: strict_whole_file > 4 MiB on tools.read without offset/limit
        let limErr = null;
        try {{ await tools.read({{ path: "huge.txt" }}); }} catch (e) {{ limErr = e; }}
        check(limErr?.name === "ToolError" && limErr.code === "LIMIT", "LIMIT whole file: " + limErr?.code);

        // 4. EDIT_FAILED: structured failures array
        let editFail = null;
        try {{
            await tools.edit({{
                path: "edit.txt",
                edits: [{{ oldText: "missing-snippet", newText: "x" }}],
            }});
        }} catch (e) {{ editFail = e; }}
        check(
            editFail?.name === "ToolError"
                && editFail.code === "EDIT_FAILED"
                && Array.isArray(editFail.result?.failures)
                && editFail.result.failures.length === 1
                && editFail.result.failures[0].problem === "not_found",
            "EDIT_FAILED: " + JSON.stringify(editFail?.result)
        );

        // 5. tools.edit success: exact (fuzzy: false) + curly-quote (fuzzy: true)
        const edited = await tools.edit({{
            path: "edit.txt",
            edits: [
                {{ oldText: "exact line", newText: "EXACT" }},
                {{ oldText: "\"curly\" line", newText: "CURLY" }},
            ],
        }});
        check(
            edited.replacements === 2
                && edited.edits.length === 2
                && edited.edits[0].line === 1 && edited.edits[0].fuzzy === false
                && edited.edits[1].line === 2 && edited.edits[1].fuzzy === true,
            "edit matches: " + JSON.stringify(edited)
        );

        // 6. IO: task_io_ok == false rejects with ToolError(code="IO") retaining output & exitCode
        let ioErr = null;
        try {{ await tools.exec({{ action: "poll", taskId: {io_id:?}, waitMs: 0 }}); }} catch (e) {{ ioErr = e; }}
        check(
            ioErr?.name === "ToolError"
                && ioErr.code === "IO"
                && ioErr.result?.exitCode === 1
                && typeof ioErr.result?.output === "string"
                && ioErr.result.output.startsWith("retained-output\n")
                && ioErr.result.captureError === "pipe broken"
                && ioErr.result.logError === "disk full",
            "IO partial result: " + ioErr?.code + " " + JSON.stringify(ioErr?.result)
        );

        return "all-codes-ok";
        "#
    );
    let (state, out) = fixture.result(&script).await;
    assert_eq!(state.exit_code, Some(0), "{out}");
    assert_eq!(out, "all-codes-ok\n");

    // 7. OVERLOAD & CANCELLED via encode_service_error
    let (_control, handle, worker) = prepare_worker().unwrap();
    drop(worker);
    let ctx = crate::service::ExecContext {
        session: &fixture.session,
        launch: None,
        cancel: tokio_util::sync::CancellationToken::new(),
        progress: None,
        call_id: None,
        parent_task_id: None,
        queued_guard: None,
        on_started: None,
        input_owner: Default::default(),
    };
    lock(&handle.0.report)
        .admit(
            1,
            CallInfo {
                tool: "exec".into(),
                action: None,
                task_id: None,
            },
            0,
            Default::default(),
        )
        .unwrap();
    let overload = encode_service_error(
        crate::service::ServiceError::Overload(
            "memory full".into(),
            Some(crate::service::TaskMeta {
                bare_task_id: "aaaaaaaa-1".into(),
                log_path: "/tmp/a.log".into(),
            }),
        ),
        &ctx,
        &ProgramDelivery {
            script: &handle,
            pending_id: 1,
            is_control: false,
        },
    );
    assert!(overload.pre_reserved);
    let Outcome::Value(payload) = overload.outcome else {
        panic!("expected encoded error envelope");
    };
    let parsed: serde_json::Value = serde_json::from_str(&payload.value).unwrap();
    assert_eq!(parsed["ok"], false);
    assert_eq!(parsed["code"], "OVERLOAD");
    assert_eq!(parsed["result"]["taskId"].as_str(), Some("aaaaaaaa-1"));

    let cancelled = encode_service_error(
        crate::service::ServiceError::Cancelled("cancelled".into()),
        &ctx,
        &ProgramDelivery {
            script: &handle,
            pending_id: 2,
            is_control: false,
        },
    );
    assert!(!cancelled.pre_reserved);
    assert!(
        matches!(&cancelled.outcome, Outcome::Cancelled(msg) if msg == "cancelled"),
        "expected Outcome::Cancelled"
    );
    let read_cancelled = super::encode::encode_read_program(
        Err(crate::files::FileError::Cancelled("read cancelled".into())),
        &fixture.session.launch,
        &ProgramDelivery {
            script: &handle,
            pending_id: 3,
            is_control: false,
        },
        &ctx.cancel,
    );
    assert!(!read_cancelled.pre_reserved);
    assert!(
        matches!(&read_cancelled.outcome, Outcome::Cancelled(msg) if msg == "read cancelled"),
        "expected Outcome::Cancelled for FileError::Cancelled"
    );
}

mod exec_program_tests {
    use super::*;
    use crate::codemode::bridge::exec_program;
    use crate::files;
    use crate::render::types::{AgentDelivery, TrackedResult};
    use crate::service::core::PreparedSnapshot;
    use crate::service::schema::{Action, ExecPolicy, Request, View};
    use crate::service::{Consumer, ExecContext, PreparedExec, PreparedTranscript, StagedRead};
    use crate::tasks;
    use crate::tasks::session::TaskEntry;
    use crate::term::output::{Truncate, WindowBuilder, WindowSpec};
    use rmcp::model::CallToolResult;
    use serde_json::Value;
    use std::future::Future;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Wake as TaskWake, Waker};
    use tokio_util::sync::CancellationToken;

    struct ReadWake {
        memory: Arc<BytePool>,
        slots: Arc<Semaphore>,
        used: AtomicUsize,
        permits: AtomicUsize,
        woken: AtomicBool,
    }

    impl TaskWake for ReadWake {
        fn wake(self: Arc<Self>) {
            self.used.store(self.memory.used(), Ordering::Relaxed);
            self.permits
                .store(self.slots.available_permits(), Ordering::Relaxed);
            self.woken.store(true, Ordering::Release);
        }
    }

    fn script_fixture() -> (Arc<Service>, Arc<Tasks>, Arc<Session>) {
        let tasks = Tasks::new(Limits::OFF).unwrap();
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

    fn text_of(result: &CallToolResult) -> String {
        result
            .content
            .iter()
            .filter_map(|b| b.as_text().map(|t| t.text.clone()))
            .collect()
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

    async fn eval_script_agent(
        service: &Arc<Service>,
        request: super::Request,
        mut ctx: ExecContext<'_>,
        delivery: AgentDelivery<'_>,
    ) -> TrackedResult {
        let slot = delivery.daemon_ipc.as_ref().map(|d| d.slot);
        let prepared = prepare_script(service, request, &mut ctx, slot, false).await;
        crate::render::finish_exec_agent(service, prepared, &ctx, &delivery).await
    }

    async fn list_fixture() -> (Arc<Service>, Arc<Tasks>, Arc<Session>, Arc<TaskEntry>) {
        let (service, tasks, session) = script_fixture();
        let ((task, entry, _), _) = spawn_script(
            &tasks,
            ScriptSpawnArgs {
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
        let (_control, script, worker) = prepare_worker().unwrap();
        script.test_pending_result(1, 0, 16 << 10);
        let encoded = encode_exec_program(
            PreparedExec::Window(prepared),
            &ctx,
            &ProgramDelivery {
                script: &script,
                pending_id: 1,
                is_control: false,
            },
        );
        let Outcome::Value(payload) = encoded.outcome else {
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
        let (_control, script, worker) = prepare_worker().unwrap();
        script.control().record_started(format!("{}.abcd", task.id));
        script.test_pending_result(1, 8, 64);
        let make_ctx = || {
            let mut ctx = script_ctx(&session);
            ctx.parent_task_id = Some("aaaaaaaa-0".into());
            ctx.on_started = Some(script.control());
            ctx
        };
        let make_delivery = || ProgramDelivery {
            script: &script,
            pending_id: 1,
            is_control: true,
        };
        let list_req = || -> Request { serde_json::from_value(json!({"action":"list"})).unwrap() };
        let refused = exec_program(&service, list_req(), make_ctx(), make_delivery()).await;
        assert!(matches!(refused.outcome, Outcome::Limit));
        assert!(!entry.is_reported());
        assert_eq!(script.test_bridge_bytes(), 8);
        drop(refused);
        script.test_bridge_limit(16 * 1024);
        let cancelled = make_ctx();
        cancelled.cancel.cancel();
        let refused = exec_program(&service, list_req(), cancelled, make_delivery()).await;
        assert!(matches!(refused.outcome, Outcome::Cancelled(_)));
        assert!(!entry.is_reported());
        let delivered = exec_program(&service, list_req(), make_ctx(), make_delivery()).await;
        let Outcome::Value(payload) = delivered.outcome else {
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
            crate::env::LaunchContext::new(std::env::temp_dir(), Vec::new())
                .with_memory(Arc::clone(&pool)),
        );
        let session = Session::new(
            "nested-commit".into(),
            "nested-commit".into(),
            Arc::clone(&launch),
        );
        let ((task, entry, _), _) = spawn_script(
            &tasks,
            ScriptSpawnArgs {
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
        let (_control, script, worker) = prepare_worker().unwrap();
        script.control().record_started(handle.clone());
        script.test_pending_result(1, 8, 64);
        let make_ctx = || {
            let mut ctx = script_ctx(&session);
            ctx.parent_task_id = Some("aaaaaaaa-0".into());
            ctx.on_started = Some(script.control());
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
        let refused = exec_program(&service, make_req(), make_ctx(), make_delivery()).await;
        assert!(matches!(refused.outcome, Outcome::Limit));
        assert_eq!(script.test_bridge_bytes(), 8);
        assert_eq!(script.test_attachment_count(), 0);
        let report = script.control().report(&pool, None).unwrap();
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
        let refused = exec_program(&service, make_req(), cancelled, make_delivery()).await;
        assert!(matches!(refused.outcome, Outcome::Cancelled(_)));
        assert_eq!(script.test_attachment_count(), 0);
        assert!(reader.is_fresh());
        let delivered = exec_program(&service, make_req(), make_ctx(), make_delivery()).await;
        let Outcome::Value(payload) = delivered.outcome else {
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
    async fn nested_task_reads_consume_reader_state_without_consuming_session_notices() {
        let (service, tasks, session) = script_fixture();
        let ((task, entry, reader), _) = spawn_script(
            &tasks,
            ScriptSpawnArgs {
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
            .event_bus()
            .publish_external(crate::events::ExternalEventInput {
                event_type: None,
                source: "src".into(),
                topic: None,
                name: "evt".into(),
                event_id: "nested-cursor-test".into(),
                timestamp: None,
                data: json!({}),
            })
            .unwrap();
        let (_control, script, worker) = prepare_worker().unwrap();
        script.test_pending_result(1, 0, 16 * 1024);
        let mut ctx = script_ctx(&session);
        ctx.on_started = Some(script.control());
        let poll_req: Request =
            serde_json::from_value(json!({"action":"poll","taskId":handle,"waitMs":0})).unwrap();
        let poll_out = exec_program(
            &service,
            poll_req,
            ctx,
            ProgramDelivery {
                script: &script,
                pending_id: 1,
                is_control: false,
            },
        )
        .await;
        assert!(matches!(poll_out.outcome, Outcome::Value(_)));
        assert!(reader.is_reported());
        assert!(!entry.is_reported());

        script.test_pending_result(2, 0, 16 * 1024);
        let mut ctx = script_ctx(&session);
        ctx.on_started = Some(script.control());
        let kill_req: Request =
            serde_json::from_value(json!({"action":"kill","taskId":handle})).unwrap();
        let kill_out = exec_program(
            &service,
            kill_req,
            ctx,
            ProgramDelivery {
                script: &script,
                pending_id: 2,
                is_control: true,
            },
        )
        .await;
        assert!(matches!(kill_out.outcome, Outcome::Value(_)));
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
        let outer = eval_script_agent(
            &service,
            serde_json::from_value(json!({"code":"return 8"})).unwrap(),
            script_ctx(&session),
            AgentDelivery::default(),
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
            let ((task, entry, reader), _) = spawn_script(
                &tasks,
                ScriptSpawnArgs {
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
        let (_control, script, worker) = prepare_worker().unwrap();
        // Script owns l000 (omitted by limit: 2) and l002 (delivered), but not l001 (outer task).
        script
            .control()
            .record_started(started[0].2.handle(&started[0].0.id));
        script
            .control()
            .record_started(started[2].2.handle(&started[2].0.id));
        script.test_pending_result(1, 0, 16 * 1024);
        let mut ctx = script_ctx(&session);
        ctx.on_started = Some(script.control());
        let req: Request = serde_json::from_value(json!({"action":"list","limit":2})).unwrap();
        let listed = exec_program(
            &service,
            req,
            ctx,
            ProgramDelivery {
                script: &script,
                pending_id: 1,
                is_control: true,
            },
        )
        .await;
        let Outcome::Value(payload) = listed.outcome else {
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
    async fn list_warnings_follow_the_delivered_rows_for_direct_and_nested_calls() {
        let (service, tasks, session, warned_entry) = list_fixture().await;
        let warned = tasks.all().pop().unwrap();
        warned.inject_io_errors(Some("omitted-row-warning"), None);
        let ((newest, _, _), _) = spawn_script(
            &tasks,
            ScriptSpawnArgs {
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
        let direct = crate::render::exec_agent_json(
            &service,
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

        let (_control, script, worker) = prepare_worker().unwrap();
        script.test_pending_result(1, 0, 16 * 1024);
        let bad_req: Request = serde_json::from_value(request).unwrap();
        let bad_nested = exec_program(
            &service,
            bad_req,
            script_ctx(&session),
            ProgramDelivery {
                script: &script,
                pending_id: 1,
                is_control: true,
            },
        )
        .await;
        let Outcome::Value(bad_payload) = bad_nested.outcome else {
            panic!("expected validation error envelope");
        };
        let bad_json: Value = serde_json::from_str(&bad_payload.value).unwrap();
        assert_eq!(bad_json["ok"], false);
        assert_eq!(bad_json["code"], "VALIDATION");
        assert!(!warned_entry.is_reported());

        script.test_pending_result(2, 0, 16 * 1024);
        let plain_req: Request = serde_json::from_value(json!({"action":"list"})).unwrap();
        let nested = exec_program(
            &service,
            plain_req,
            script_ctx(&session),
            ProgramDelivery {
                script: &script,
                pending_id: 2,
                is_control: true,
            },
        )
        .await;
        let Outcome::Value(payload) = nested.outcome else {
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

        let (_control, script, worker) = prepare_worker().unwrap();
        script.test_pending_result(1, 0, 16 * 1024);
        let outcome = encode_exec_program(
            prepared,
            &script_ctx(&session),
            &ProgramDelivery {
                script: &script,
                pending_id: 1,
                is_control: false,
            },
        )
        .outcome;
        let Outcome::Value(payload) = outcome else {
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
            crate::env::LaunchContext::new(std::env::temp_dir(), Vec::new())
                .with_memory(Arc::clone(&main_pool))
                .with_control_memory(Arc::clone(&control_pool)),
        );
        let session = Session::new("rb-ctrl".into(), "rb-ctrl".into(), Arc::clone(&launch));
        let ((task, entry, _), _) = spawn_script(
            &tasks,
            ScriptSpawnArgs {
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
        let (_, _, probe_opt) = crate::platform::process::task_env(
            &launch,
            std::path::Path::new("/nonexistent-bash-for-probe"),
            false,
        );
        if let Some(mut probe) = probe_opt {
            assert!(probe.message().is_some());
            drop(probe);
            let (_, _, probe_again) = crate::platform::process::task_env(
                &launch,
                std::path::Path::new("/nonexistent-bash-for-probe"),
                false,
            );
            assert!(probe_again.unwrap().message().is_some());
        }

        let reader = entry.reader(Some("rb99")).unwrap();
        let handle = reader.handle(&task.id);
        let (_control, script, worker) = prepare_worker().unwrap();
        script.control().record_started(handle.clone());
        // Tiny bridge quota forces admit_bridge_result to fail after StagedRead & NestedAttachments.
        script.test_pending_result(1, 0, 32);

        let mut ctx = script_ctx(&session);
        ctx.on_started = Some(script.control());
        let poll_req: Request =
            serde_json::from_value(json!({"action": "poll", "taskId": handle, "waitMs": 0}))
                .unwrap();
        let refused = exec_program(
            &service,
            poll_req,
            ctx,
            ProgramDelivery {
                script: &script,
                pending_id: 1,
                is_control: false,
            },
        )
        .await;
        assert!(matches!(refused.outcome, Outcome::Limit));
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
        let list_out = exec_program(
            &service,
            list_req,
            script_ctx(&session),
            ProgramDelivery {
                script: &script,
                pending_id: 2,
                is_control: true,
            },
        )
        .await;
        let Outcome::Value(list_payload) = list_out.outcome else {
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
        kill_ctx.on_started = Some(script.control());
        let kill_req: Request =
            serde_json::from_value(json!({"action": "kill", "taskId": handle})).unwrap();
        let kill_out = exec_program(
            &service,
            kill_req,
            kill_ctx,
            ProgramDelivery {
                script: &script,
                pending_id: 3,
                is_control: true,
            },
        )
        .await;
        let Outcome::Value(kill_payload) = kill_out.outcome else {
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
    async fn cancelling_an_active_program_wait_preserves_cancelled_code() {
        let (service, tasks, session) = script_fixture();
        let ((task, _, reader), _) = spawn_script(
            &tasks,
            ScriptSpawnArgs {
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
        let (_control, script, worker) = prepare_worker().unwrap();
        script.test_pending_result(1, 0, 16 << 10);
        let ctx = script_ctx(&session);
        let cancel = ctx.cancel.clone();
        let request = serde_json::from_value(json!({
            "action": "poll", "taskId": reader.handle(&task.id)
        }))
        .unwrap();
        let mut waiting = Box::pin(exec_program(
            &service,
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
        assert!(!result.pre_reserved);
        let Outcome::Cancelled(message) = result.outcome else {
            panic!("expected Outcome::Cancelled");
        };
        assert!(message.to_lowercase().contains("cancel"), "{message}");
    }

    #[tokio::test]
    async fn reader_capacity_is_overload_with_task_recovery_metadata() {
        let (service, tasks, session, entry) = list_fixture().await;
        let task = tasks.all().pop().unwrap();
        let busy: Vec<_> = (0..16)
            .map(|index| entry.reader(Some(&format!("{index:04}"))).unwrap())
            .collect();
        let (_control, script, worker) = prepare_worker().unwrap();
        script.test_pending_result(1, 0, 16 << 10);
        let refused = exec_program(
            &service,
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
        let Outcome::Value(payload) = refused.outcome else {
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
}

#[tokio::test]
async fn g3_immutable_vm_thread_concurrent_kill_1000x_and_dense_bridge_unpark() {
    let fixture = Fixture::with_limits(Limits {
        max_running: 64,
        max_scripts: 16,
        max_finished: 8,
        finished_ttl: None,
    });

    // (a) 1000 concurrent kills spanning pre-job/pre-Runtime::new(), startup, and park() idle wait.
    let mut completed = 0usize;
    const TOTAL: usize = 1000;
    const BATCH: usize = 10;
    for batch_start in (0..TOTAL).step_by(BATCH) {
        let count = (TOTAL - batch_start).min(BATCH);
        let mut handles = Vec::with_capacity(count);
        for offset in 0..count {
            let idx = batch_start + offset;
            let tasks = Arc::clone(&fixture.tasks);
            let service = Arc::clone(&fixture.service);
            let session = Arc::clone(&fixture.session);
            handles.push(tokio::spawn(async move {
                let code = match idx % 3 {
                    0 => "await sleep(60000);",
                    1 => "await null; await sleep(60000);",
                    _ => "text('ready'); await sleep(60000);",
                };
                let ((task, _, _), _) = tokio::task::spawn_blocking({
                    let tasks = Arc::clone(&tasks);
                    let session = Arc::clone(&session);
                    move || {
                        let res = spawn_script(
                            &tasks,
                            ScriptSpawnArgs {
                                code: code.into(),
                                launch: Arc::clone(&session.launch),
                                kill_after_ms: None,
                                transfer: false,
                                call_id: None,
                                service,
                                input_owner: Default::default(),
                            },
                            &session,
                            format!("k{:03}", idx % 1000),
                        )
                        .unwrap();
                        if idx % 3 == 0 {
                            // Immediate kill on spawning thread races job delivery and pre-Runtime::new().
                            res.0.0.kill().unwrap();
                        }
                        res
                    }
                })
                .await
                .unwrap();

                match idx % 3 {
                    0 => {}
                    1 => {
                        if idx % 2 == 0 {
                            tokio::task::yield_now().await;
                        }
                        task.kill().unwrap();
                    }
                    _ => {
                        tokio::time::sleep(Duration::from_micros(300)).await;
                        task.kill().unwrap();
                    }
                }
                complete(&task).await;
                assert_eq!(task.snapshot().state, "killed");
                assert_eq!(task.snapshot().exit_code, None);
            }));
        }
        for handle in handles {
            handle.await.unwrap();
            completed += 1;
        }
    }
    assert_eq!(completed, TOTAL);

    // (b) Dense concurrent `await sleep(...)` and `await tools.exec(...)` wakeup stress.
    let ((emitter, _, _), _) = fixture.start(
        "for (let i = 0; i < 10; i++) { text('tick-' + i); await sleep(1); } return 'done';",
        Some(5000),
    );
    let emitter_id = emitter.id.clone();
    let (state, out) = fixture
        .result(&format!(
            r#"
            for (let round = 0; round < 30; round++) {{
                const wave = await Promise.all([
                    sleep(0),
                    sleep(1),
                    sleep(2),
                    tools.exec({{ action: "list" }}),
                    tools.exec({{ action: "poll", taskId: {emitter_id:?}, waitMs: 2 }}),
                ]);
                if (wave[3].action !== "list" || wave[4].action !== "poll") {{
                    throw new Error("unexpected response at round " + round);
                }}
            }}
            return "wake-ok";
            "#
        ))
        .await;
    complete(&emitter).await;
    assert_eq!(state.exit_code, Some(0), "{out}");
    assert_eq!(out, "wake-ok\n");
}

#[tokio::test]
async fn g4_post_publish_vm_and_diagnostic_failures_exit_1_with_is_error_false() {
    let fixture = Fixture::new();

    // 1. Diagnostic workspace (12 KiB) exhaustion after Published via prepare_script + finish_exec_agent.
    let small_control = BytePool::new(DIAGNOSTIC_WORKSPACE - 1);
    let diag_launch = Arc::new(
        fixture
            .session
            .launch
            .for_session()
            .with_control_memory(Arc::clone(&small_control)),
    );
    let mut ctx = crate::service::ExecContext {
        session: &fixture.session,
        launch: Some(&diag_launch),
        cancel: tokio_util::sync::CancellationToken::new(),
        progress: None,
        call_id: None,
        parent_task_id: None,
        queued_guard: None,
        on_started: None,
        input_owner: Default::default(),
    };
    let req: Request = serde_json::from_value(json!({ "code": "return 1;" })).unwrap();
    let delivery = crate::render::types::AgentDelivery::default();
    let prepared = prepare_script(&fixture.service, req, &mut ctx, None, false)
        .await
        .expect("post-publish diagnostic workspace exhaustion must not fail prepare_script");
    let tracked =
        crate::render::finish_exec_agent(&fixture.service, Ok(prepared), &ctx, &delivery).await;
    assert!(!tracked.overload);
    assert_eq!(tracked.result.is_error, Some(false), "{:?}", tracked.result);
    let text = tracked.result.content[0].as_text().unwrap().text.as_str();
    assert!(text.contains("[exited 1]"), "{text}");
    assert!(
        text.contains("Script error: diagnostic memory budget is full"),
        "{text}"
    );
    drop(tracked);
    assert_eq!(small_control.used(), 0);

    // 2. Post-publish QuickJS Context::full() initialization OOM under tiny VM memory limit.
    let ((oom_task, _, oom_reader), _) = spawn_script_inner(
        &fixture.tasks,
        ScriptSpawnArgs {
            code: "return 42;".into(),
            launch: Arc::clone(&fixture.session.launch),
            kill_after_ms: Some(5000),
            transfer: false,
            call_id: None,
            service: Arc::clone(&fixture.service),
            input_owner: Default::default(),
        },
        &fixture.session,
        "g4vm".into(),
        Some(1024),
    )
    .unwrap();
    complete(&oom_task).await;
    let snap = oom_task.snapshot();
    assert_eq!(snap.state, "exited");
    assert_eq!(snap.exit_code, Some(1));
    let poll_ctx = crate::service::ExecContext {
        session: &fixture.session,
        launch: None,
        cancel: tokio_util::sync::CancellationToken::new(),
        progress: None,
        call_id: None,
        parent_task_id: None,
        queued_guard: None,
        on_started: None,
        input_owner: Default::default(),
    };
    let polled = crate::render::exec_agent_json(
        &fixture.service,
        json!({
            "action": "poll",
            "taskId": oom_reader.handle(&oom_task.id),
            "waitMs": 0,
        }),
        poll_ctx,
        crate::render::types::AgentDelivery::default(),
    )
    .await;
    assert!(!polled.overload);
    assert_eq!(polled.result.is_error, Some(false), "{:?}", polled.result);
    let poll_text = polled.result.content[0].as_text().unwrap().text.as_str();
    assert!(poll_text.contains("[exited 1]"), "{poll_text}");
    assert!(poll_text.contains("Script error:"), "{poll_text}");
}

#[tokio::test]
async fn g5_kill_with_pending_nested_calls_refunds_leases_and_records_cancelled_ledger() {
    let mut fixture = Fixture::new();
    let ((bg, _, _), _) = fixture.start("await sleep(60000);", None);
    let bg_id = bg.id.clone();

    let pool = BytePool::new(4 << 20);
    fixture.session = Session::new(
        "g5-session".into(),
        "g5-session".into(),
        Arc::new(
            fixture
                .session
                .launch
                .for_session()
                .with_memory(Arc::clone(&pool)),
        ),
    );

    let script_code = format!(
        r#"
        const p1 = sleep(60000);
        const p2 = tools.exec({{ action: "poll", taskId: {bg_id:?}, waitMs: 60000 }});
        text("PENDING_READY");
        await Promise.all([p1, p2]);
        "#
    );
    let ((victim, _, _), victim_bridge) = fixture.start(&script_code, None);

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let ready = std::fs::read_to_string(&victim.log_path)
            .unwrap_or_default()
            .contains("PENDING_READY");
        if ready && lock(&victim_bridge.0.report).pending.len() == 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "victim did not queue both nested calls"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        pool.used() > 0,
        "pending nested call arguments must hold active BytePool leases"
    );
    assert!(victim_bridge.test_bridge_bytes() > 0);

    victim.kill().unwrap();
    complete(&victim).await;

    // Wait briefly for cancelled spawned futures to drop their hold_future input owners.
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.used() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("pending call leases must be refunded after kill");
    assert_eq!(victim_bridge.test_bridge_bytes(), 0);

    let report = victim
        .script()
        .unwrap()
        .report(crate::lease::process_pool(), None)
        .unwrap();
    assert_eq!(report.cancelled.len(), 2, "{:?}", report.cancelled);
    assert_eq!(
        report.cancelled[0],
        CallInfo {
            tool: "sleep".into(),
            action: None,
            task_id: None,
        }
    );
    assert_eq!(
        report.cancelled[1],
        CallInfo {
            tool: "exec".into(),
            action: Some("poll".into()),
            task_id: Some(bg_id),
        }
    );

    bg.kill().unwrap();
    complete(&bg).await;
}

#[tokio::test]
async fn g13_program_exec_preserves_nul_bytes_defaults_to_head_and_rejects_head_tail() {
    use crate::service::schema::{ExecPolicy, Request as ExecRequest, validate as validate_exec};
    use crate::term::output::{Cleaner, Truncate, WindowBuilder, WindowSpec};

    // 1. Verify ExecPolicy::PROGRAM view contract + Cleaner NUL preservation vs AGENT stripping.
    let raw_req: ExecRequest = serde_json::from_value(json!({
        "action": "poll",
        "taskId": "aaaaaaaa-1",
    }))
    .unwrap();
    let prog_view = validate_exec(&raw_req, ExecPolicy::PROGRAM).unwrap();
    assert!(prog_view.keep_nul);
    assert!(!prog_view.inline_markers);
    assert_eq!(prog_view.truncate, Truncate::Head);
    let mut cleaner = Cleaner::default();
    let mut prog_builder = WindowBuilder::new(
        WindowSpec {
            truncate: prog_view.truncate,
            budget: 1024,
            raw: false,
            clean: true,
            keep_nul: prog_view.keep_nul,
            include_markers: prog_view.inline_markers,
            encoding: encoding_rs::UTF_8,
            source: "log",
        },
        &mut cleaner,
        1,
    );
    prog_builder.push(b"a\x1b[31m\0b\x1b[0m\nc\0d\n");
    assert_eq!(prog_builder.finish().text, "a\0b\nc\0d");

    // 2. End-to-end nested `tools.exec` preserves `\0`, defaults to `head`, and rejects `head_tail`.
    let fixture = Fixture::new();
    let ((nul_task, _, _), _) = fixture.start("text('a\\0b\\nc\\0d');", Some(5000));
    let ((many_task, _, _), _) = fixture.start(
        "for (let i = 1; i <= 80; i++) text('row-' + String(i).padStart(3, '0'));",
        Some(5000),
    );
    complete(&nul_task).await;
    complete(&many_task).await;
    let nul_id = nul_task.id.clone();
    let many_id = many_task.id.clone();

    let (state, out) = fixture
        .result(&format!(
            r#"
            const nulRes = await tools.exec({{
                action: "poll",
                taskId: {nul_id:?},
                waitMs: 0,
            }});
            if (nulRes.output !== "a\0b\nc\0d") {{
                throw new Error("NUL mismatch: " + JSON.stringify(nulRes.output));
            }}
            if (nulRes.output.charCodeAt(1) !== 0 || nulRes.output.charCodeAt(5) !== 0) {{
                throw new Error("expected NUL code units");
            }}

            const many = await tools.exec({{
                action: "poll",
                taskId: {many_id:?},
                maxBytes: 100,
                waitMs: 0,
            }});
            if (!many.truncated || !many.output.startsWith("row-001\n") || many.output.includes("row-080")) {{
                throw new Error("expected head truncation: " + JSON.stringify(many));
            }}
            if (many.output.includes("omitted") || !Array.isArray(many.omittedRange) || many.omittedRange[1] !== 80) {{
                throw new Error("expected structured omittedRange without inline markers: " + JSON.stringify(many));
            }}

            for (const req of [
                {{ action: "start", command: "printf ok", truncate: "head_tail", loginShell: false }},
                {{ action: "poll", taskId: many.taskId, truncate: "head_tail", waitMs: 0 }},
            ]) {{
                let err = null;
                try {{ await tools.exec(req); }} catch (e) {{ err = e; }}
                if (!err || err.name !== "ToolError" || err.code !== "VALIDATION") {{
                    throw new Error("head_tail must reject with VALIDATION: " + JSON.stringify(req));
                }}
            }}
            return "g13-ok";
            "#
        ))
        .await;
    assert_eq!(state.exit_code, Some(0), "{out}");
    assert_eq!(out, "g13-ok\n");
}

#[tokio::test]
async fn settled_write_before_concurrent_kill_is_not_reported_cancelled() {
    let fixture = Fixture::new();
    let ((bg, _, _), _) = fixture.start("await sleep(60000);", None);
    let bg_id = bg.id.clone();

    let script_code = format!(
        r#"
        tools.write({{ path: "settled.txt", content: "persisted\n" }});
        tools.exec({{ action: "poll", taskId: {bg_id:?}, waitMs: 60000 }});
        while (true) {{}}
        "#
    );
    let ((victim, _, _), bridge) = fixture.start(&script_code, None);

    // Wait until tools.write has written to disk and settled into settled_rx, while
    // tools.exec poll remains the sole active in-flight call and QuickJS spins in while(true).
    let target = fixture.dir.join("settled.txt");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let written = std::fs::read_to_string(&target).ok();
        let in_flight = *lock(&bridge.0.in_flight.0);
        let pending_len = lock(&bridge.0.report).pending.len();
        if written.as_deref() == Some("persisted\n") && in_flight == 1 && pending_len == 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "write did not settle while poll stayed in flight (in_flight={in_flight}, pending={pending_len})"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    victim.kill().unwrap();
    complete(&victim).await;

    assert_eq!(std::fs::read_to_string(&target).unwrap(), "persisted\n");
    let report = victim
        .script()
        .unwrap()
        .report(crate::lease::process_pool(), None)
        .unwrap();
    assert_eq!(
        report.cancelled,
        vec![CallInfo {
            tool: "exec".into(),
            action: Some("poll".into()),
            task_id: Some(bg_id),
        }]
    );
    let log = std::fs::read_to_string(&victim.log_path).unwrap();
    assert!(
        log.contains("cancelled 1 pending call(s): exec poll") && !log.contains("write"),
        "{log}"
    );

    bg.kill().unwrap();
    complete(&bg).await;
}

#[tokio::test]
async fn report_result_releases_input_owner_and_done_has_zero_pool_bytes() {
    let pool = BytePool::new(1 << 20);
    let info = CallInfo {
        tool: "write".into(),
        action: None,
        task_id: None,
    };
    let mut report = Report {
        limit: Some(128),
        ..Default::default()
    };
    let owner = crate::lease::Owner::new(pool.try_acquire(1024).unwrap());
    report.admit(1, info, 32, owner).unwrap();
    assert_eq!(pool.used(), 1024);
    assert!(!report.result(1, 256));
    assert_eq!(pool.used(), 1024, "refused result keeps input_owner intact");
    assert!(report.result(1, 64));
    assert_eq!(
        pool.used(),
        0,
        "accepted result releases input_owner immediately"
    );
    report.remove(1);
    assert_eq!(report.bridge_bytes, 0);

    let mut fixture = Fixture::new();
    let launch = Arc::new(
        fixture
            .session
            .launch
            .for_session()
            .with_memory(Arc::clone(&pool)),
    );
    fixture.session = Session::new(
        "lease-release".into(),
        "lease-release".into(),
        Arc::clone(&launch),
    );

    // Normal exit: code lease and nested awaited call leases are already 0 when done fires.
    let code_owner = crate::lease::Owner::new(pool.try_acquire(2048).unwrap());
    let ((task, _, _), _) = spawn_script(
        &fixture.tasks,
        ScriptSpawnArgs {
            code: "const w = await tools.write({ path: 'done-lease.txt', content: 'x'.repeat(8192) }); return w.bytes;".into(),
            launch: Arc::clone(&launch),
            kill_after_ms: Some(5000),
            transfer: false,
            call_id: None,
            service: Arc::clone(&fixture.service),
            input_owner: code_owner,
        },
        &fixture.session,
        "lr01".into(),
    )
    .unwrap();
    complete(&task).await;
    assert_eq!(pool.used(), 0);
    assert_eq!(task.snapshot().exit_code, Some(0));

    // Unconsumed settled call + kill: 4x argument lease drops as soon as host settles,
    // and code + settled_rx leases drop before done wakes.
    let kill_code_owner = crate::lease::Owner::new(pool.try_acquire(1024).unwrap());
    let ((killed_task, _, _), bridge) = spawn_script(
        &fixture.tasks,
        ScriptSpawnArgs {
            code: "tools.write({ path: 'queued-lease.txt', content: 'y'.repeat(16384) }); while (true) {}".into(),
            launch,
            kill_after_ms: None,
            transfer: false,
            call_id: None,
            service: Arc::clone(&fixture.service),
            input_owner: kill_code_owner,
        },
        &fixture.session,
        "lr02".into(),
    )
    .unwrap();
    let queued_file = fixture.dir.join("queued-lease.txt");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if queued_file.exists() && *lock(&bridge.0.in_flight.0) == 0 {
            break;
        }
        assert!(Instant::now() < deadline, "nested write did not settle");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        pool.used() < 16384,
        "4x argument lease must already be released while result sits in settled_rx: used={}",
        pool.used()
    );
    killed_task.kill().unwrap();
    complete(&killed_task).await;
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn kill_near_bridge_limit_keeps_cancelled_polls_without_limit_rewrite() {
    let fixture = Fixture::new();
    let ((bg, _, bg_reader), _) = fixture.start("await sleep(60000);", None);
    let bg_handle = bg_reader.handle(&bg.id);

    // Two concurrent polls on the same reader handle: the first holds read_gate in wait,
    // while the second waits on read_gate (whose cancellation message is 61 bytes, longer
    // than each ~44-byte argument string).
    let script_code = format!(
        r#"
        const p1 = tools.exec({{ action: "poll", taskId: {bg_handle:?} }});
        const p2 = tools.exec({{ action: "poll", taskId: {bg_handle:?} }});
        text("BOTH_QUEUED");
        await Promise.all([p1, p2]);
        "#
    );
    let ((victim, _, _), bridge) = fixture.start(&script_code, None);

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let ready = std::fs::read_to_string(&victim.log_path)
            .unwrap_or_default()
            .contains("BOTH_QUEUED");
        let in_flight = *lock(&bridge.0.in_flight.0);
        let pending_len = lock(&bridge.0.report).pending.len();
        if ready && in_flight == 2 && pending_len == 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "polls did not enter flight (in_flight={in_flight}, pending={pending_len})"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Leave only 8 bytes (< 60 bytes) of headroom under BRIDGE_LIMIT so a 61-byte
    // cancellation message would fail Report::result if it were ever admitted there.
    let used = bridge.test_bridge_bytes();
    bridge.test_bridge_limit(used + 8);

    victim.kill().unwrap();
    complete(&victim).await;

    let log = std::fs::read_to_string(&victim.log_path).unwrap();
    assert!(!log.contains("256 MiB"), "{log}");
    assert!(!log.contains("LIMIT"), "{log}");
    assert!(log.contains("cancelled 2 pending call(s):"), "{log}");

    let report = victim
        .script()
        .unwrap()
        .report(crate::lease::process_pool(), None)
        .unwrap();
    let expected = CallInfo {
        tool: "exec".into(),
        action: Some("poll".into()),
        task_id: Some(bg_handle),
    };
    assert_eq!(report.cancelled, vec![expected.clone(), expected]);

    // Defense-in-depth: if Outcome::Cancelled ever reaches settled_rx while run_vm is parked,
    // run_vm exits without removing the pending entry from cancelledCalls.
    let ((parked, _, _), parked_bridge) = fixture.start("await sleep(60000);", None);
    let deadline = Instant::now() + Duration::from_secs(5);
    while lock(&parked_bridge.0.report).pending.len() != 1 {
        assert!(Instant::now() < deadline, "sleep was not admitted");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    parked_bridge
        .0
        .settled_tx
        .send(SettledCall {
            id: 1,
            outcome: Outcome::Cancelled("cancelled".into()),
        })
        .unwrap();
    parked.kill().unwrap();
    complete(&parked).await;
    let parked_report = parked
        .script()
        .unwrap()
        .report(crate::lease::process_pool(), None)
        .unwrap();
    assert_eq!(
        parked_report.cancelled,
        vec![CallInfo {
            tool: "sleep".into(),
            action: None,
            task_id: None,
        }]
    );

    bg.kill().unwrap();
    complete(&bg).await;
}
