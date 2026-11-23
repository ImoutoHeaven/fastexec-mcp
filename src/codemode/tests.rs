use super::bridge::{
    BRIDGE_ERROR, BRIDGE_LIMIT, DIAGNOSTIC_RESERVE, DIAGNOSTIC_WORKSPACE, Host, MAX_STARTS,
    SLEEP_CHUNK_MS, STARTS_ERROR, encode_nested_result, nested_envelope, queue_permit, sleep,
    split_sleep,
};
use super::control::{
    CallInfo, InFlightGuard, MAX_CODE, Outcome, Report, Request, Wake, lock, validate,
};
use super::vm::{STALLED, describe, pending_text};
use super::*;
use crate::files;
use crate::lease::BytePool;
use crate::service::Service;
use crate::session::{LaunchContext, Session};
use crate::tasks::state::Task;
use crate::tasks::{self, Limits, ScriptArgs, TASK_LOG_LIMIT, Tasks};
use base64::Engine;
use rquickjs::{Context, Function, Runtime, Value};
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

struct Fixture {
    tasks: Arc<Tasks>,
    service: Arc<Service>,
    session: Arc<Session>,
    dir: std::path::PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "fastexec-script-test-{}",
            tasks::random_chars(12).unwrap()
        ));
        let tasks = Tasks::new_in(Limits::OFF, dir.clone()).unwrap();
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

    fn start(&self, code: &str, deadline: Option<u64>) -> Arc<Task> {
        self.tasks
            .start_script(
                ScriptArgs {
                    code: code.into(),
                    launch: Arc::clone(&self.session.launch),
                    kill_after_ms: deadline,
                    transfer: false,
                    call_id: None,
                    service: Arc::clone(&self.service),
                    input_owner: Default::default(),
                },
                &self.session,
                tasks::random_chars(4).unwrap(),
            )
            .unwrap()
            .0
    }

    async fn result(&self, code: &str) -> (crate::tasks::state::Snapshot, String) {
        let task = self.start(code, Some(5000));
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
fn nested_precommit_prepaid_wire_covers_envelope_and_needs_no_new_admission() {
    let mut structured = rmcp::model::CallToolResult::success(Vec::new());
    structured.structured_content =
        Some(json!({"ok":true,"text":"hello λ\n","nested":{"value":"quote\""}}));
    let cases = [
        (
            structured,
            json!({"ok":true,"value":{"text":"hello λ\n","nested":{"value":"quote\""}}}),
        ),
        (
            rmcp::model::CallToolResult::error(vec![rmcp::model::ContentBlock::text(
                "ordinary error",
            )]),
            json!({"ok":false,"value":{"error":"ordinary error"}}),
        ),
        (
            rmcp::model::CallToolResult::success(Vec::new()),
            json!({"ok":false,"value":{"error":"Tool operation failed."}}),
        ),
    ];
    for (result, expected) in cases {
        let encoded = serde_json::to_string(&expected).unwrap();
        let wire = crate::ipc::json_len(&result).unwrap() + 256;
        assert!(128 + encoded.len() <= wire);
        let payload = crate::service::render::result_payload_capacity(&result);
        let pool = BytePool::new(payload + wire);
        let tracked = files::TrackedResult::owned(
            result,
            crate::lease::Owner::new(pool.try_acquire(payload).unwrap()),
        );
        let prepaid = pool.try_acquire(wire).unwrap();
        assert!(pool.try_acquire(1).is_none());
        let outcome = encode_nested_result(tracked, &pool, None, Some(prepaid));
        let Outcome::Value(value) = outcome else {
            panic!("the prepaid result must encode");
        };
        assert_eq!(value.value, encoded);
        assert_eq!(pool.used(), value.value.capacity());
        let (sender, receiver) = mpsc::channel();
        sender.send(value).unwrap();
        drop(sender);
        assert_eq!(pool.used(), encoded.len());
        drop(receiver);
        assert_eq!(pool.used(), 0);
    }
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

#[test]
fn text_buffer_nested_envelope_moves_existing_strings() {
    let pool = BytePool::new(512);
    let mut body = String::with_capacity(128);
    body.push_str("hello");
    let pointer = body.as_ptr();
    let mut fields = serde_json::Map::new();
    fields.insert("text".into(), serde_json::Value::String(body));
    let mut result = rmcp::model::CallToolResult::success(Vec::new());
    result.structured_content = Some(serde_json::Value::Object(fields));
    let tracked = files::TrackedResult {
        result,
        lease: pool.try_acquire(160),
        overload: false,
        task_meta: None,
        payload_owner: Default::default(),
    };
    let workspace = pool.try_acquire(128).unwrap();
    let envelope = nested_envelope(tracked);
    assert_eq!(
        envelope.value["value"]["text"].as_str().unwrap().as_ptr(),
        pointer
    );
    assert_eq!(pool.used(), 288);
    drop(envelope);
    assert_eq!(pool.used(), 128);
    drop(workspace);
    assert_eq!(pool.used(), 0);
}

#[test]
fn text_buffer_bridge_reserves_exact_encoding_and_keeps_queue_ownership() {
    let expected = serde_json::to_string(&json!({
        "ok": false, "value": { "error": "hello λ\n" }
    }))
    .unwrap();
    for (budget, fits) in [
        (128 + 128 + expected.len() - 1, false),
        (128 + 128 + expected.len(), true),
    ] {
        let pool = BytePool::new(budget);
        let mut body = String::with_capacity(128);
        body.push_str("hello λ\n");
        let tracked = files::TrackedResult {
            result: rmcp::model::CallToolResult::error(vec![rmcp::model::ContentBlock::text(body)]),
            lease: pool.try_acquire(128),
            overload: false,
            task_meta: None,
            payload_owner: Default::default(),
        };
        let outcome = encode_nested_result(tracked, &pool, None, None);
        if !fits {
            assert!(matches!(outcome, Outcome::Memory));
            assert_eq!(pool.used(), 0);
            continue;
        }
        let (sender, receiver) = mpsc::channel();
        assert!(sender.send(Wake::Settled { id: 1, outcome }).is_ok());
        drop(sender);
        assert_eq!(pool.used(), expected.len());
        let Wake::Settled {
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
                .eval("(value) => JSON.parse(value).value.error === 'hello λ\\n'")
                .unwrap();
            assert!(parse.call::<_, bool>((payload.value.as_str(),)).unwrap());
            assert_eq!(pool.used(), expected.len());
        });
        drop(payload);
        assert_eq!(pool.used(), 0);
    }
}

#[test]
fn nested_control_wire_uses_bounded_reserve_and_keeps_queue_owner() {
    let expected = serde_json::to_string(&json!({"ok":true,"value":{"tasks":[]}})).unwrap();
    for main_limit in [0, 128] {
        for extra in [-1isize, 0, 1] {
            let mut result = rmcp::model::CallToolResult::success(Vec::new());
            result.structured_content = Some(json!({"ok":true,"tasks":[]}));
            let payload = crate::service::render::result_payload_capacity(&result);
            let pool = BytePool::new(main_limit);
            let needed = payload + 128 + expected.len();
            let control = BytePool::new(needed.saturating_add_signed(extra));
            let result = files::TrackedResult::owned(
                result,
                crate::lease::Owner::new(control.try_acquire(payload).unwrap()),
            );
            let outcome = encode_nested_result(result, &pool, Some(&control), None);
            if extra < 0 {
                assert!(matches!(outcome, Outcome::Memory));
                assert_eq!((pool.used(), control.used()), (0, 0));
                continue;
            }
            let (sender, receiver) = mpsc::channel();
            sender.send(Wake::Settled { id: 1, outcome }).unwrap();
            drop(sender);
            assert_eq!(pool.used(), 0);
            assert_eq!(control.used(), expected.len());
            let Wake::Settled {
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
    let (script, worker) = prepare(Handle::current()).unwrap();
    let mut host = Host {
        service: Arc::clone(&fixture.service),
        session: Arc::clone(&fixture.session),
        launch,
        transfer: false,
        call_id: None,
        runtime: Handle::current(),
        script: script.clone(),
        start_permits: Arc::new(Semaphore::new(0)),
        io_permits: Arc::new(Semaphore::new(0)),
        next: 1,
        starts: 0,
    };
    let error = host.call("read", r#"{"path":"small.txt"}"#).unwrap_err();
    assert!(error.value.contains("not dispatched"));
    assert!(lock(&script.0.report).pending.is_empty());
    assert!(
        control.used() > 0,
        "the synchronous error still owns its input workspace"
    );
    drop(error);
    assert_eq!(control.used(), 0);
    let sleep_error = host.call("sleep", "0").unwrap_err();
    assert!(sleep_error.value.contains("not dispatched"));
    assert!(lock(&script.0.report).pending.is_empty());
    drop(sleep_error);
    assert_eq!(control.used(), 0);
    host.call("exec", r#"{"action":"kill","taskId":"aaaaaaaa-1"}"#)
        .unwrap();
    assert!(control.used() > 0);
    script.kill();
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
    let (script, _worker) = prepare(Handle::current()).unwrap();
    let mut tool = String::with_capacity(128);
    tool.push_str("read");
    let mut handle = String::with_capacity(256);
    handle.push_str("task-1.abcd");
    {
        let mut report = lock(&script.0.report);
        report.cancelled.push(CallInfo {
            tool,
            action: None,
            task_id: None,
        });
        report.started.push(handle);
    }
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
    assert_eq!(lock(&script.0.report).started.len(), 2);
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
    let (script, worker) = prepare(runtime.clone()).unwrap();
    drop(worker);
    let mut host = Host {
        service: Arc::clone(&fixture.service),
        session: Arc::clone(&fixture.session),
        launch: Arc::clone(&fixture.session.launch),
        transfer: false,
        call_id: None,
        runtime,
        script: script.clone(),
        start_permits: Arc::new(Semaphore::new(0)),
        io_permits: Arc::new(Semaphore::new(0)),
        next: 1,
        starts: MAX_STARTS - 1,
    };
    let args = json!({"action":"start","command":"true"}).to_string();
    host.call("exec", &args).unwrap();
    assert_eq!(host.starts, MAX_STARTS);
    assert_eq!(host.call("exec", &args).unwrap_err().value, STARTS_ERROR);
    assert_eq!(host.starts, MAX_STARTS);
    // Unvalidated start requests resolve as tool errors without consuming start slots.
    host.call("exec", &json!({"action":"start","command":""}).to_string())
        .unwrap();
    assert_eq!(host.starts, MAX_STARTS);
    assert_eq!(lock(&script.0.report).pending.len(), 2);
    host.call("read", &json!({"path":"missing"}).to_string())
        .unwrap();
    assert_eq!(host.starts, MAX_STARTS);
    script.kill();
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
    let task = fixture.start("await sleep(60000);", Some(5000));
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
        if (!missingErr || missingErr.name !== 'ToolError' || missingErr.tool !== 'read' || typeof missingErr.result?.error !== 'string') throw new Error('missing file must reject with ToolError');
        let badExecErr = null;
        try { await tools.exec({action:'start',command:''}); } catch (e) { badExecErr = e; }
        if (!badExecErr || badExecErr.name !== 'ToolError' || badExecErr.tool !== 'exec' || badExecErr.result?.action !== 'start') throw new Error('empty exec must reject with ToolError');
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
    let task = fixture.start(
        "for (let i=0;i<20;i++) tools.read({path:'missing'}); return 'done';",
        None,
    );
    complete(&task).await;
    assert_eq!(task.snapshot().exit_code, Some(0));
    let script = task.script().unwrap();
    assert!(script.0.cancel.is_cancelled());
    let report = script.report(crate::lease::process_pool(), None).unwrap();
    assert_eq!(report.cancelled.len(), 20);
    assert!(report.cancelled.iter().all(|call| call.tool == "read"));
    assert!(report.started.is_empty());
    assert_eq!(std::fs::read_to_string(&task.log_path).unwrap(), "done\n");
}

#[tokio::test]
async fn finishing_waits_for_a_slow_inflight_start_to_be_reported() {
    let fixture = Fixture::new();
    let task = fixture.start("while (true) {}", None);
    let script = task.script().unwrap().clone();
    // Hold an admitted start before its task is registered, past the old five-second drain.
    let in_flight = InFlightGuard::enter(script.clone());
    lock(&script.0.report)
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
    script.record_started(handle.clone());
    drop(in_flight);
    complete(&task).await;
    assert!(
        still_running,
        "script finished before the admitted start registered"
    );
    assert_eq!(
        script
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
        let task = fixture.start(code, None);
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
    let task = fixture.start("while(true) {}", Some(100));
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
    let r4_task = fixture.start(&r4_code, Some(5000));
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
        let producer = fixture.start(
            &format!("attach('data:image/png;base64,{png_b64}'); attach('clip.mp4');"),
            Some(5000),
        );
        complete(&producer).await;
        let src_att1 = producer.attachment_path(1, "image/png");
        assert!(src_att1.is_file());

        let consumer = fixture.start(
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
        tasks::random_chars(12).unwrap()
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
    let (tight_task, _, _) = tight_tasks
        .start_script(
            ScriptArgs {
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
    // plus the 1025th pending call rejecting with RangeError.
    let seeded = fixture.start("text('partial-out'); throw new Error('boom');", None);
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
            const rangeErrors = settled.filter(
                s => s.status === "rejected" && s.reason instanceof RangeError
            );
            if (rangeErrors.length !== 1) {{
                throw new Error("expected 1 RangeError at 1025th pending call, got " + rangeErrors.length);
            }}
            return "io-and-queue-ok";
            "#
        ))
        .await;
    assert_eq!(io_state.exit_code, Some(0), "{io_out}");
    assert!(io_out.contains("io-and-queue-ok"), "{io_out}");

    // Integrity failure without top-level `error` builds ToolError.message from inputError
    // and preserves output, exitCode, and diagnostics in ToolError.result.
    let mut synth =
        rmcp::model::CallToolResult::error(vec![rmcp::model::ContentBlock::text("status")]);
    synth.structured_content = Some(json!({
        "ok": false,
        "action": "poll",
        "taskId": "abcdefgh-1.2345",
        "state": "exited",
        "exitCode": 0,
        "output": "captured-before-err",
        "inputError": "INPUT_WRITE_FAILED: broken pipe",
    }));
    let parsed = nested_envelope(files::TrackedResult::plain(synth)).value;
    assert_eq!(parsed["ok"], false);
    assert!(parsed["value"].get("ok").is_none());
    assert_eq!(parsed["value"]["output"], "captured-before-err");
    assert_eq!(parsed["value"]["exitCode"], 0);
    assert_eq!(
        parsed["value"]["inputError"],
        "INPUT_WRITE_FAILED: broken pipe"
    );
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
            let rangeStack = "";
            try { await sleep(60000); } catch (e) { if (e instanceof RangeError) rangeStack = e.stack; }
            if (!toolStack.includes("codemode.js:3")) throw new Error("toolStack: " + toolStack);
            if (!typeStack.includes("codemode.js:5")) throw new Error("typeStack: " + typeStack);
            if (!rangeStack.includes("codemode.js:8")) throw new Error("rangeStack: " + rangeStack);
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
    let child_script = fixture.start("await sleep(10000);", None);
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

    let sleeping = fixture.start(
        "const p = sleep(Number.MAX_VALUE); text('ready'); await p;",
        None,
    );
    let wait_deadline = Instant::now() + Duration::from_secs(10);
    while lock(&sleeping.script().unwrap().0.report)
        .pending
        .is_empty()
    {
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
    let bg1 = fixture.start("text('ready'); await sleep(60000);", None);
    let bg2 = fixture.start("await sleep(60000);", None);
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
