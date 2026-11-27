// `Tasks::new` uses one directory per process, so these tests hold a std lock for their whole
// length: they serialize the binary's task tests, and a current-thread runtime is what they
// run on, so the guard across awaits is the point.
#![allow(clippy::await_holding_lock)]
// The task-starting scenarios run only where a task can start, so on Unix their helpers have
// no caller.
#![cfg_attr(not(windows), allow(dead_code))]

use super::*;

#[test]
fn event_ipc_encoding_reserves_overlap_and_keeps_queued_bytes_charged() {
    let frame = || {
        ipc::EventWireFrame::Event(crate::events::StreamEvent {
            event_id: "eeeeeeee-1".to_owned(),
            name: "external.event".to_owned(),
            timestamp: "2026-11-22T00:00:00.000Z".to_owned(),
            data: serde_json::Value::String("x".repeat(4096)),
            cursor: "eeeeeeee:1".to_owned(),
        })
    };
    let source_bytes = 4096;
    let encoded_bytes = ipc::frame_len(&ToEndpoint::EventFrame {
        id: 1,
        frame: frame(),
    })
    .unwrap();
    for fits in [false, true] {
        let pool = BytePool::new(source_bytes + encoded_bytes - usize::from(!fits));
        let reserved = Arc::new(Semaphore::new(65_536));
        let owner = crate::lease::Owner::new(pool.try_acquire(source_bytes).unwrap());
        let input = lease::Owned::new(frame(), owner);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let closing = CancellationToken::new();
        let queued = events::queue_event_frame(&tx, &pool, 1, input);
        assert_eq!(queued, fits);
        assert!(
            !closing.is_cancelled(),
            "ordinary event backpressure must not cancel the shared connection"
        );
        assert_eq!(
            reserved.available_permits(),
            65_536,
            "ordinary event frames must not consume the control reserve"
        );
        assert_eq!(pool.used(), if fits { encoded_bytes } else { 0 });
        if fits {
            let queued = rx.try_recv().unwrap();
            assert_eq!(queued.bytes.len(), encoded_bytes);
            drop(queued);
            assert_eq!(pool.used(), 0);
        } else {
            assert!(rx.try_recv().is_err());
        }
    }
}

use crate::ipc::OsText;
use crate::tasks::Limits;
use crate::transport::Endpoint;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// `Tasks::new` uses one directory per process, so only one test in the binary builds one at
/// a time; the daemon tests share that lock with the task tests.
fn one_at_a_time() -> MutexGuard<'static, ()> {
    crate::tasks::ONE_TASKS
        .lock()
        .unwrap_or_else(|error| error.into_inner())
}

/// A daemon for one test, holding the process-wide task lock and ending its tasks however
/// the test ends.
struct Fixture {
    daemon: Arc<Daemon>,
    _one_at_a_time: MutexGuard<'static, ()>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.daemon.tasks.shutdown();
    }
}

fn fixture() -> Fixture {
    fixture_with_config(Config::default())
}

fn fixture_with_config(config: Config) -> Fixture {
    let lock = one_at_a_time();
    let daemon = Daemon::with_config(
        Tasks::new(Limits::OFF).unwrap(),
        "fastexec".to_owned(),
        env!("CARGO_PKG_VERSION").to_owned(),
        None,
        None,
        config,
    );
    Fixture {
        daemon,
        _one_at_a_time: lock,
    }
}

fn fixture_with_write_stall(write_stall: Duration) -> Fixture {
    fixture_with_config(Config {
        write_stall,
        ..Config::default()
    })
}

fn hello(session: &str, connection: u64) -> ToDaemon {
    let (cwd, env) = ipc::launch_to_wire(&LaunchContext::current().unwrap());
    ToDaemon::Hello {
        protocol: ipc::PROTOCOL,
        namespace: "fastexec".to_owned(),
        build: env!("CARGO_PKG_VERSION").to_owned(),
        session: session.to_owned(),
        connection,
        client_name: "client".to_owned(),
        cwd,
        env,
    }
}

/// A Hello that also carries a variable only it knows, so a task proves where its
/// environment came from.
fn hello_with(session: &str, connection: u64, name: &str, value: &str) -> ToDaemon {
    let mut message = hello(session, connection);
    if let ToDaemon::Hello { env, .. } = &mut message {
        env.push((
            OsText::Text(name.to_owned()),
            OsText::Text(value.to_owned()),
        ));
    }
    message
}

fn call(id: u64, arguments: Value) -> ToDaemon {
    ToDaemon::Call {
        id,
        tool: "exec".to_owned(),
        request_id: id.to_string(),
        arguments,
    }
}

/// A task that waits far longer than a test runs, so a wait on it is still pending.
fn long_wait(id: u64) -> ToDaemon {
    call(
        id,
        json!({
            "action": "start", "command": "sleep 30", "loginShell": false, "waitMs": 30000
        }),
    )
}

struct Client {
    reader: ReadHalf<BoxedStream>,
    writer: WriteHalf<BoxedStream>,
    /// The task serving a duplex connection; a connection over a listener is served by the
    /// daemon's own accept loop and has none.
    serving: Option<tokio::task::JoinHandle<()>>,
}

async fn connect_to(daemon: &Arc<Daemon>, capacity: usize) -> Client {
    let (client, server) = tokio::io::duplex(capacity);
    let ticket = daemon.admit().expect("the daemon admits a connection");
    let daemon = Arc::clone(daemon);
    let serving = tokio::spawn(async move { daemon.serve(server, ticket).await });
    let client: BoxedStream = Box::pin(client);
    let (reader, writer) = tokio::io::split(client);
    Client {
        reader,
        writer,
        serving: Some(serving),
    }
}

/// A client over a real listener, so a test exercises the transport as a client would.
async fn connect_over(endpoint: &crate::transport::Endpoint) -> Client {
    let stream = crate::transport::connect(endpoint.name()).await.unwrap();
    let stream: BoxedStream = Box::pin(stream);
    let (reader, writer) = tokio::io::split(stream);
    Client {
        reader,
        writer,
        serving: None,
    }
}

async fn connect(daemon: &Arc<Daemon>) -> Client {
    connect_to(daemon, 1 << 20).await
}

impl Client {
    async fn send(&mut self, message: &ToDaemon) {
        ipc::write_frame(&mut self.writer, message).await.unwrap();
    }

    /// The next frame. The bound is a failure bound only: every test waits for what it
    /// needs, and a frame that never comes fails here instead of hanging Cargo.
    async fn next(&mut self) -> Option<ToEndpoint> {
        timeout(
            Duration::from_secs(60),
            ipc::read_frame(&mut self.reader, lease::FRAME_LIMIT),
        )
        .await
        .expect("a frame arrived")
        .unwrap()
    }

    async fn welcome(&mut self, session: &str, connection: u64) -> ToEndpoint {
        self.send(&hello(session, connection)).await;
        match self.next().await {
            Some(message @ ToEndpoint::Welcome { .. }) => message,
            other => panic!("expected a Welcome, got {other:?}"),
        }
    }

    /// The result of `id`, whatever progress frames came before it.
    async fn result(&mut self, id: u64) -> CallToolResult {
        loop {
            match self.next().await {
                Some(ToEndpoint::Result { id: got, result }) if got == id => return result,
                Some(ToEndpoint::Progress { .. }) => continue,
                other => panic!("expected the result of call {id}, got {other:?}"),
            }
        }
    }
}

fn text_of(result: &CallToolResult) -> String {
    result
        .content
        .iter()
        .filter_map(|block| block.as_text().map(|text| text.text.clone()))
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

#[cfg(windows)]
/// Starts `command` and returns `(taskId handle, full text)`.
async fn start(client: &mut Client, id: u64, command: &str) -> (String, String) {
    client
        .send(&call(
            id,
            json!({
                "action": "start", "command": command, "loginShell": false, "waitMs": 20000
            }),
        ))
        .await;
    let result = client.result(id).await;
    assert_ne!(result.is_error, Some(true), "{}", text_of(&result));
    assert!(result.structured_content.is_none(), "{result:?}");
    (task_handle_of(&result), text_of(&result))
}

#[cfg(windows)]
/// The text result of a `poll` that does not wait.
async fn poll(client: &mut Client, id: u64, task_id: &str) -> String {
    client
        .send(&call(
            id,
            json!({"action": "poll", "taskId": task_id, "waitMs": 0}),
        ))
        .await;
    let result = client.result(id).await;
    assert!(result.structured_content.is_none(), "{result:?}");
    text_of(&result)
}

/// Waits until `test` holds, so a test never guesses at a delay.
async fn until(test: impl Fn() -> bool) {
    timeout(Duration::from_secs(20), async {
        while !test() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the condition held");
}

#[tokio::test]
async fn ipc_ingress_control_round_trip_survives_full_main_pool() {
    let pool = BytePool::new(0);
    let fixture = fixture_with_config(Config {
        memory: Some(Arc::clone(&pool)),
        ..Config::default()
    });
    let daemon = &fixture.daemon;
    let mut client = connect(daemon).await;
    client.welcome("control", 1).await;
    client
        .send(&call(
            1,
            json!({"action":"start", "command":"printf hello"}),
        ))
        .await;
    let rejected = client.result(1).await;
    assert_eq!(rejected.is_error, Some(true));
    assert!(text_of(&rejected).contains("not dispatched"));
    assert!(daemon.tasks.all().is_empty());
    client.send(&call(2, json!({"action":"list"}))).await;
    assert_eq!(client.result(2).await.is_error, Some(false));
    client.send(&ToDaemon::Cancel { id: 1 }).await;
    client.send(&ToDaemon::Status).await;
    assert!(matches!(
        client.next().await,
        Some(ToEndpoint::Status { tasks: 0, .. })
    ));
    client.send(&ToDaemon::Stop).await;
    assert!(client.next().await.is_none());
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn a_hello_is_answered_with_the_epoch_or_a_reject_naming_the_field() {
    let fixture = fixture();
    let daemon = &fixture.daemon;
    let mut client = connect(daemon).await;
    let mut wrong = hello("s1", 1);
    if let ToDaemon::Hello { build, .. } = &mut wrong {
        *build = "9.9.9".to_owned();
    }
    client.send(&wrong).await;
    match client.next().await {
        Some(ToEndpoint::Reject { message }) => {
            assert!(message.contains("9.9.9"), "{message}");
            assert!(message.contains(env!("CARGO_PKG_VERSION")), "{message}");
        }
        other => panic!("expected a Reject, got {other:?}"),
    }
    assert_eq!(client.next().await, None, "the daemon said nothing more");

    let mut client = connect(daemon).await;
    let ToEndpoint::Welcome { epoch, .. } = client.welcome("s2", 1).await else {
        unreachable!("welcome returned a Welcome")
    };
    assert_eq!(epoch, daemon.tasks.epoch());
}

// shortcut: on Unix a task runs under the fastexec binary as its task host, which a unit-test
// binary is not, so this case runs only where a task can start here. Block 9's two-endpoint
// tests cover start, poll, and kill across sessions against the real binary on Linux; no
// test-host override is added for it.
#[cfg(windows)]
fn output_of(text: &str) -> &str {
    match text.split_once("\n\n[").map_or("", |(body, _)| body) {
        "(no new output)" => "",
        body => body,
    }
}

#[cfg(windows)]
#[tokio::test]
async fn a_start_runs_in_the_hellos_environment_and_list_names_its_session_and_call() {
    let fixture = fixture();
    let daemon = &fixture.daemon;
    let mut client = connect(daemon).await;
    client
        .send(&hello_with(
            "sessabc123",
            1,
            "FASTEXEC_IPC_PROBE",
            "ipc-value",
        ))
        .await;
    assert!(matches!(
        client.next().await,
        Some(ToEndpoint::Welcome { .. })
    ));

    // The variable exists only in this Hello, so only the Hello's launch context can supply
    // it: a task that sees it runs in what the handshake carried.
    let (task_id, started_text) =
        start(&mut client, 1, "printf '%s' \"$FASTEXEC_IPC_PROBE\"").await;
    assert_eq!(output_of(&started_text), "ipc-value");

    // A second session whose first six characters match, starting its own call 1.
    let mut other = connect(daemon).await;
    other.welcome("sessabc999", 1).await;
    let (second_id, _) = start(&mut other, 1, "printf other").await;

    client
        .send(&call(2, json!({"action": "list", "scope": "all"})))
        .await;
    let list = client.result(2).await;
    assert!(list.structured_content.is_none());
    let text = text_of(&list);
    let bare1 = task_id.split_once('.').unwrap().0;
    let bare2 = second_id.split_once('.').unwrap().0;
    assert!(text.contains(bare1), "{text}");
    assert!(text.contains(bare2), "{text}");
    assert!(text.contains("client-sessab"), "{text}");
    assert!(text.contains(" call 1"), "{text}");
}

// shortcut: on Unix a task runs under the fastexec binary as its task host, which a unit-test
// binary is not, so this case runs only where a task can start here. Block 9's two-endpoint
// tests cover start, poll, and kill across sessions against the real binary on Linux; no
// test-host override is added for it.
#[cfg(windows)]
#[tokio::test]
async fn a_second_session_reads_a_task_from_the_beginning() {
    let fixture = fixture();
    let daemon = &fixture.daemon;
    let mut first = connect(daemon).await;
    first.welcome("aaa111", 1).await;
    let (task_id, _) = start(&mut first, 1, "printf hi").await;
    // The start already read the output, so this session's next read sees nothing new.
    assert_eq!(output_of(&poll(&mut first, 2, &task_id).await), "");

    let mut second = connect(daemon).await;
    second.welcome("bbb222", 1).await;
    assert_eq!(
        output_of(&poll(&mut second, 1, &task_id).await),
        "hi",
        "a session that never read this task starts at its beginning"
    );
    // And the first session's cursor is where it was.
    assert_eq!(output_of(&poll(&mut first, 3, &task_id).await), "");
}

// shortcut: on Unix a task runs under the fastexec binary as its task host, which a unit-test
// binary is not, so this case runs only where a task can start here. Block 9's two-endpoint
// tests cover start, poll, and kill across sessions against the real binary on Linux; no
// test-host override is added for it.
#[cfg(windows)]
#[tokio::test]
async fn a_reconnect_keeps_the_cursors_and_an_old_generation_is_refused() {
    let fixture = fixture();
    let daemon = &fixture.daemon;
    let mut first = connect(daemon).await;
    first.welcome("sess11", 1).await;
    let (task_id, _) = start(&mut first, 1, "printf hi").await;
    // The connection goes away without a Bye.
    drop(first);

    let mut second = connect(daemon).await;
    second.welcome("sess11", 2).await;
    assert_eq!(
        output_of(&poll(&mut second, 1, &task_id).await),
        "",
        "the session kept its cursors"
    );

    // Neither the generation before nor the same one may take the session.
    for connection in [1, 2] {
        let mut older = connect(daemon).await;
        older.send(&hello("sess11", connection)).await;
        match older.next().await {
            Some(ToEndpoint::Reject { message }) => assert!(
                message.contains("is not newer than"),
                "connection {connection}: {message}"
            ),
            other => panic!("expected a Reject, got {other:?}"),
        }
    }
    // And the connection that does hold the session still works.
    assert_eq!(output_of(&poll(&mut second, 2, &task_id).await), "");
}

#[tokio::test]
async fn a_new_connection_takes_the_session_and_closes_the_old_one() {
    let fixture = fixture();
    let daemon = &fixture.daemon;
    let mut first = connect(daemon).await;
    first.welcome("sess22", 1).await;
    // A wait is pending on the first connection when the second one arrives.
    first.send(&long_wait(1)).await;

    let mut second = connect(daemon).await;
    second.welcome("sess22", 2).await;

    // What the old connection is promised is that it ends, not that nothing was queued on it:
    // a progress frame or a cancelled write can still be in the pipe.
    let mut drained = Vec::new();
    timeout(
        Duration::from_secs(20),
        first.reader.read_to_end(&mut drained),
    )
    .await
    .expect("the replaced connection ended")
    .expect("the drained bytes end");
    // And the connection that took the session still works.
    second.send(&call(2, json!({"action": "list"}))).await;
    let list = second.result(2).await;
    assert_ne!(list.is_error, Some(true));
    assert!(list.structured_content.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_call_on_a_closed_connection_queues_nothing() {
    let fixture = fixture();
    let daemon = &fixture.daemon;
    let session = Arc::new(Session::new(
        "closed01".to_owned(),
        "closed-01".to_owned(),
        Arc::new(crate::env::LaunchContext::new(
            std::env::temp_dir(),
            std::env::vars_os().collect(),
        )),
    ));
    let (outgoing, mut queued) = mpsc::unbounded_channel();
    // A connection that is already gone, with the budget ready: only the token decides.
    for id in 1..=32 {
        let connection = CancellationToken::new();
        connection.cancel();
        let in_flight = Arc::new(Mutex::new(HashMap::new()));
        run_call(
            Arc::clone(daemon),
            Arc::clone(&session),
            connection,
            CancellationToken::new(),
            id,
            "exec".to_owned(),
            json!({"action": "list"}),
            Arc::clone(&in_flight),
            outgoing.clone(),
            0,
            Default::default(),
        )
        .await;
        assert!(
            queued.try_recv().is_err(),
            "call {id} queued a result on a closed connection"
        );
    }
}

// shortcut: on Unix a task runs under the fastexec binary as its task host, which a unit-test
// binary is not, so this case runs only where a task can start here. Block 9's two-endpoint
// tests cover start, poll, and kill across sessions against the real binary on Linux; no
// test-host override is added for it.
#[cfg(windows)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_ends_in_flight_waits_with_eof_and_no_result() {
    let fixture = fixture();
    let daemon = &fixture.daemon;
    let mut client = connect(daemon).await;
    client.welcome("sess55", 1).await;
    // One task whose waits stay pending, and eight polls of it in flight on this connection.
    client
        .send(&call(
            1,
            json!({
                "action": "start", "command": "sleep 30", "loginShell": false, "waitMs": 0
            }),
        ))
        .await;
    let task_id = task_handle_of(&client.result(1).await);
    for id in 2..10 {
        client
            .send(&call(
                id,
                json!({"action": "poll", "taskId": task_id, "waitMs": 30000}),
            ))
            .await;
    }
    // The reader dispatches in order, so a list answered now proves all eight polls were
    // admitted before the stop rather than still sitting in the socket.
    client.send(&call(10, json!({"action": "list"}))).await;
    let _ = client.result(10).await;
    // A stop kills the task and ends every connection: each wait answers with nothing at all,
    // never with a result about a task the stop itself killed.
    daemon.stop();
    loop {
        match client.next().await {
            Some(ToEndpoint::Result { id, result }) => {
                panic!(
                    "a stopped daemon answered call {id}: {:?}",
                    text_of(&result)
                );
            }
            Some(ToEndpoint::Progress { .. }) => continue,
            Some(other) => panic!("unexpected frame from a stopped daemon: {other:?}"),
            None => break,
        }
    }
}

// shortcut: on Unix a task runs under the fastexec binary as its task host, which a unit-test
// binary is not, so this case runs only where a task can start here. Block 9's two-endpoint
// tests cover start, poll, and kill across sessions against the real binary on Linux; no
// test-host override is added for it.
#[cfg(windows)]
#[tokio::test]
async fn a_cancel_answers_the_wait_and_leaves_the_task_running() {
    let fixture = fixture();
    let daemon = &fixture.daemon;
    let mut client = connect(daemon).await;
    client.welcome("sess33", 1).await;
    client.send(&long_wait(1)).await;
    client.send(&ToDaemon::Cancel { id: 1 }).await;
    let result = client.result(1).await;
    // Whichever wait was running says it the same way: cancelled, and the task lives on.
    let cancelled = text_of(&result);
    assert!(cancelled.contains("Cancelled"), "{cancelled}");
    assert!(cancelled.contains("the task keeps running."), "{cancelled}");

    client.send(&call(2, json!({"action": "list"}))).await;
    let list = client.result(2).await;
    assert!(list.structured_content.is_none());
    assert!(text_of(&list).contains("running"), "{}", text_of(&list));
}

// shortcut: on Unix a task runs under the fastexec binary as its task host, which a unit-test
// binary is not, so this case runs only where a task can start here. Block 9's two-endpoint
// tests cover start, poll, and kill across sessions against the real binary on Linux; no
// test-host override is added for it.
#[cfg(windows)]
#[tokio::test]
async fn bye_ends_the_session_so_the_next_connection_starts_fresh() {
    let fixture = fixture();
    let daemon = &fixture.daemon;
    let mut first = connect(daemon).await;
    first.welcome("sess44", 1).await;
    let (task_id, _) = start(&mut first, 1, "printf hi").await;
    first.send(&ToDaemon::Bye).await;
    // The daemon is done with this connection once its stream ends.
    assert_eq!(
        timeout(Duration::from_secs(20), first.next())
            .await
            .expect("the connection ended"),
        None
    );

    let mut second = connect(daemon).await;
    second.welcome("sess44", 2).await;
    assert!(
        poll(&mut second, 1, &task_id).await.contains("hi"),
        "a Bye left no session behind, so this one reads from the beginning"
    );
}

// shortcut: on Unix a task runs under the fastexec binary as its task host, which a unit-test
// binary is not, so this case runs only where a task can start here. Block 9's two-endpoint
// tests cover start, poll, and kill across sessions against the real binary on Linux; no
// test-host override is added for it.
#[cfg(windows)]
#[tokio::test]
async fn the_connection_runs_at_most_32_calls_and_refuses_a_duplicate_id() {
    let fixture = fixture();
    let daemon = &fixture.daemon;
    let mut client = connect(daemon).await;
    client.welcome("sess55", 1).await;
    // One task, and 32 waits on it: what is counted is the connection's calls. The start does
    // not wait, so the task is alive for the whole test.
    client
        .send(&call(
            1,
            json!({"action": "start", "command": "sleep 30", "loginShell": false, "waitMs": 0}),
        ))
        .await;
    let task_id = task_handle_of(&client.result(1).await);
    for id in 2..=MAX_IN_FLIGHT as u64 + 1 {
        client
            .send(&call(
                id,
                json!({"action": "poll", "taskId": task_id, "waitMs": 30000}),
            ))
            .await;
    }
    // One more call than the connection may run, then an ID already in flight.
    client.send(&call(100, json!({"action": "list"}))).await;
    client.send(&call(2, json!({"action": "list"}))).await;

    let mut refused = HashMap::new();
    while refused.len() < 2 {
        match client.next().await {
            Some(ToEndpoint::Result { id, result }) => {
                refused.entry(id).or_insert_with(|| text_of(&result));
            }
            Some(ToEndpoint::Progress { .. }) => {}
            other => panic!("expected a refusal, got {other:?}"),
        }
    }
    assert_eq!(
        refused[&100],
        format!("{MAX_IN_FLIGHT} calls are already in flight on this connection.")
    );
    assert_eq!(refused[&2], "Call 2 is already in flight.");

    for id in 2..=MAX_IN_FLIGHT as u64 + 1 {
        client.send(&ToDaemon::Cancel { id }).await;
    }
    let mut cancelled = 0;
    while cancelled < MAX_IN_FLIGHT {
        match client.next().await {
            Some(ToEndpoint::Result { id, .. }) if (2..=MAX_IN_FLIGHT as u64 + 1).contains(&id) => {
                cancelled += 1;
            }
            Some(ToEndpoint::Result { .. }) | Some(ToEndpoint::Progress { .. }) => {}
            other => panic!("expected the cancelled waits, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn a_kill_or_list_or_overload_receipt_uses_reserved_when_main_memory_is_full() {
    // Leave room for the admitted text workspace; each pressure step below
    // still fills the whole pool (or leaves only the stated 64 bytes).
    let pool = BytePool::new(1 << 20);
    let fixture = fixture_with_config(Config {
        memory: Some(Arc::clone(&pool)),
        ..Config::default()
    });
    let daemon = &fixture.daemon;
    let mut client = connect(daemon).await;
    client.welcome("sess88", 1).await;

    // Start a codemode script that stores a small image and pauses, while memory is still free.
    let pixel =
        image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(2, 2, image::Rgb([1, 2, 3])));
    let mut png = Vec::new();
    pixel
        .write_to(&mut std::io::Cursor::new(&mut png), image::ImageFormat::Png)
        .unwrap();
    let data_url = format!(
        "data:image/png;base64,{}",
        crate::media::encode_base64(&png)
    );
    client
        .send(&ToDaemon::Call {
            id: 1,
            tool: "codemode".into(),
            request_id: "1".into(),
            arguments: json!({
                "code": format!("attach({data_url:?}); await sleep(30000);"),
                "waitMs": 50
            }),
        })
        .await;
    let started = client.result(1).await;
    let handle = task_handle_of(&started);
    let bare = handle.split_once('.').unwrap().0.to_owned();
    // Wait until the attachment is stored on the task.
    let task = daemon.tasks.find(&bare).unwrap();
    until(|| task.unseen_attachment_count(0) > 0).await;

    // Occupy 100% of the main active-byte pool.
    let held = pool
        .try_acquire(pool.limit() - pool.used())
        .expect("remaining main capacity");

    // 1. Ordinary call fails fast without deadlocking and delivers its overload receipt via RESERVED.
    client
        .send(&call(2, json!({"action": "no-such-action"})))
        .await;
    let overloaded = client.result(2).await;
    assert_eq!(overloaded.is_error, Some(true));
    assert!(
        text_of(&overloaded).contains("active memory budget"),
        "{}",
        text_of(&overloaded)
    );

    // 2. `list` succeeds via RESERVED despite 0 bytes in main pool.
    client.send(&call(3, json!({"action": "list"}))).await;
    let listed = client.result(3).await;
    assert_eq!(listed.is_error, Some(false));

    // 3. `kill` on a fresh reader with an unseen attachment terminates the script, defers
    //    the attachment so it stays unread, and returns its short receipt via RESERVED.
    client
        .send(&call(4, json!({"action": "kill", "taskId": bare})))
        .await;
    let killed = client.result(4).await;
    assert_eq!(killed.is_error, Some(false), "{killed:?}");
    let killed_text = text_of(&killed);
    assert!(killed_text.contains("[killed]"), "{killed_text}");
    let kill_handle = task_handle_of(&killed);
    assert_eq!(
        killed
            .content
            .iter()
            .filter(|b| b.as_image().is_some())
            .count(),
        0,
        "attachment stays deferred while main pool is full"
    );
    assert!(
        text_of(&killed).contains("poll taskId without input"),
        "{}",
        text_of(&killed)
    );

    // 4. Once main memory frees up, an inputless `poll` on that same reader handle retrieves the attachment!
    drop(held);
    client
        .send(&call(
            5,
            json!({"action": "poll", "taskId": kill_handle, "waitMs": 0}),
        ))
        .await;
    let polled = client.result(5).await;
    assert_eq!(polled.is_error, Some(false), "{polled:?}");
    assert_eq!(
        polled
            .content
            .iter()
            .filter(|b| b.as_image().is_some())
            .count(),
        1,
        "deferred attachment delivered on subsequent inputless poll"
    );
}

#[tokio::test]
async fn kill_narrows_over_reserved_dual_frame_and_overload_preserves_cursor_and_bare_id() {
    // Leave room for the admitted text workspace; each pressure step below
    // still fills the whole pool (or leaves only the stated 64 bytes).
    let pool = BytePool::new(1 << 20);
    let fixture = fixture_with_config(Config {
        memory: Some(Arc::clone(&pool)),
        ..Config::default()
    });
    let daemon = &fixture.daemon;

    let mut both_client = connect(daemon).await;
    let h = hello("bothkill", 1);
    both_client.send(&h).await;
    assert!(matches!(
        both_client.next().await,
        Some(ToEndpoint::Welcome { .. })
    ));

    // Start a codemode script whose JSON-escaped output (~11.5 MiB) exceeds 8 MiB RESERVED.
    both_client
        .send(&ToDaemon::Call {
            id: 1,
            tool: "codemode".into(),
            request_id: "1".into(),
            arguments: json!({
                "code": "for (let i = 0; i < 32; i++) text('\\x01'.repeat(60000)); await sleep(30000);",
                "waitMs": 50,
                "maxBytes": 100
            }),
        })
        .await;
    let started = both_client.result(1).await;
    assert_eq!(started.is_error, Some(false), "{started:?}");
    let start_handle = task_handle_of(&started);
    let bare_id = start_handle.split_once('.').unwrap().0.to_owned();
    let task = daemon.tasks.find(&bare_id).unwrap();
    until(|| task.snapshot().lines >= 32).await;

    // Exhaust 100% of main pool: kill with maxBytes=4194304 would exceed 8 MiB
    // RESERVED (~11.5 MiB JSON-escaped), so kill must narrow its window to fit RESERVED and still succeed!
    let held = pool.try_acquire(pool.limit() - pool.used()).unwrap();
    both_client
        .send(&call(
            2,
            json!({
                "action": "kill",
                "taskId": bare_id,
                "maxBytes": 4_194_304
            }),
        ))
        .await;
    let killed = both_client.result(2).await;
    assert_eq!(killed.is_error, Some(false), "{killed:?}");
    assert!(killed.structured_content.is_none());
    let killed_text = text_of(&killed);
    assert!(killed_text.contains("[killed]"), "{killed_text}");
    assert!(
        killed_text.contains("narrowed to fit the 8 MiB control reserve"),
        "{killed_text}"
    );
    drop(held);

    // When start/poll hits active-memory overload, read.commit() must NOT advance the cursor,
    // and the error receipt must name the bare taskId (never advising a blind retry of start).
    let mut nosc_client = connect(daemon).await;
    let h2 = hello("nosc01", 1);
    nosc_client.send(&h2).await;
    assert!(matches!(
        nosc_client.next().await,
        Some(ToEndpoint::Welcome { .. })
    ));

    let request = ToDaemon::Call {
        id: 10,
        tool: "codemode".into(),
        request_id: "10".into(),
        arguments: json!({
            "code": "text('exact-preserved-line');",
            "waitMs": 5000
        }),
    };
    // Admit this normal input first; the remaining response allowance is
    // still far below the 128 KiB text workspace, so this checks post-start
    // overload and cursor preservation rather than pre-dispatch rejection.
    let input_peak = lease::json_len(&request).unwrap() * 6;
    let held_most = pool
        .try_acquire(pool.limit() - pool.used() - input_peak - 64)
        .unwrap();
    nosc_client.send(&request).await;
    let overloaded = nosc_client.result(10).await;
    assert_eq!(overloaded.is_error, Some(true));
    assert!(overloaded.structured_content.is_none());
    let err_msg = text_of(&overloaded);
    assert!(
        err_msg.contains("Do not retry start or resend input"),
        "{err_msg}"
    );
    // Extract bare taskId from `(taskId: <id>, logPath: ...)` and confirm it has no `.reader` suffix.
    let after_tid = err_msg.split_once("taskId: ").unwrap().1;
    let reported_tid = after_tid.split_once(',').unwrap().0;
    assert!(
        !reported_tid.contains('.'),
        "expected bare taskId, got {reported_tid}"
    );

    drop(held_most);
    nosc_client
        .send(&call(
            11,
            json!({"action": "poll", "taskId": reported_tid, "waitMs": 0}),
        ))
        .await;
    let recovered = nosc_client.result(11).await;
    assert_eq!(recovered.is_error, Some(false), "{recovered:?}");
    assert!(
        text_of(&recovered).contains("exact-preserved-line"),
        "cursor must not have been consumed on overload: {}",
        text_of(&recovered)
    );
}

#[tokio::test]
async fn zero_progress_write_stall_disconnects_reclaims_lease_and_recovers_via_bare_task_id() {
    // Leave room for text rendering; the 64-byte pipe independently creates write pressure.
    let pool = BytePool::new(1 << 20);
    let fixture = fixture_with_config(Config {
        write_stall: Duration::from_millis(80),
        memory: Some(Arc::clone(&pool)),
        ..Config::default()
    });
    let daemon = &fixture.daemon;

    // 64-byte duplex pipe: client stops reading after Welcome so a >2 KiB result stalls completely.
    let mut stalled = connect_to(daemon, 64).await;
    stalled.welcome("stallrec", 1).await;
    stalled
        .send(&ToDaemon::Call {
            id: 1,
            tool: "codemode".into(),
            request_id: "1".into(),
            arguments: json!({
                "code": "text('RECOVER_LINE_' + 'X'.repeat(3000));",
                "waitMs": 5000
            }),
        })
        .await;
    let serving = stalled.serving.take().expect("duplex served");
    timeout(Duration::from_secs(10), serving)
        .await
        .expect("zero-progress write_stall closed connection")
        .expect("handler exited cleanly");
    until(|| pool.used() == 0).await;

    // Fresh client reconnects, discovers the bare taskId via `list`, and recovers full output.
    let mut fresh = connect(daemon).await;
    fresh.welcome("recover1", 1).await;
    fresh
        .send(&call(1, json!({"action": "list", "scope": "all"})))
        .await;
    let listed_res = fresh.result(1).await;
    assert!(listed_res.structured_content.is_none());
    let listed_text = text_of(&listed_res);
    let bare_id = listed_text
        .lines()
        .next()
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .to_owned();
    assert!(!bare_id.contains('.'));

    fresh
        .send(&call(
            2,
            json!({"action": "poll", "taskId": bare_id, "waitMs": 5000}),
        ))
        .await;
    let polled = fresh.result(2).await;
    assert_eq!(polled.is_error, Some(false));
    assert!(
        text_of(&polled).contains(&format!("RECOVER_LINE_{}", "X".repeat(3000))),
        "{}",
        text_of(&polled)
    );
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn held_render_slots_leave_a_receipt_and_do_not_block_kill_or_list() {
    let fixture = fixture();
    let daemon = &fixture.daemon;
    let mut client = connect(daemon).await;
    client.welcome("sessrender", 1).await;
    let held = Arc::clone(&daemon.render)
        .acquire_many_owned(RENDER_SLOTS as u32)
        .await
        .unwrap();

    let requests = [
        #[cfg(windows)]
        (
            "exec",
            json!({"action": "start", "command": "printf render-slot-test", "waitMs": 5000}),
        ),
        (
            "codemode",
            json!({"code": r#"const x = await tools.exec({action: "list"}); return Array.isArray(x.tasks) ? "render-slot-test" : "list failed";"#, "waitMs": 5000}),
        ),
    ];
    for (index, (tool, arguments)) in requests.into_iter().enumerate() {
        let call_id = (index as u64) * 4 + 1;
        client
            .send(&ToDaemon::Call {
                id: call_id,
                tool: tool.into(),
                request_id: call_id.to_string(),
                arguments,
            })
            .await;
        let receipt = client.result(call_id).await;
        assert_eq!(receipt.is_error, Some(true));
        assert!(receipt.structured_content.is_none());
        let receipt = text_of(&receipt);
        let mut lines = receipt.lines();
        assert!(
            lines
                .next()
                .unwrap()
                .contains("The task exists and may still be running"),
            "{tool}: {receipt}"
        );
        let handle = lines.next().unwrap().strip_prefix("taskId: ").unwrap();
        let path = lines.next().unwrap().strip_prefix("logPath: ").unwrap();
        assert!(receipt.contains("Its output was not read"));
        assert!(
            lines
                .next()
                .unwrap()
                .contains("Do not replay the original operation")
        );

        client
            .send(&call(call_id + 1, json!({"action": "list"})))
            .await;
        let listed_res = client.result(call_id + 1).await;
        assert!(listed_res.structured_content.is_none());
        let listed_text = text_of(&listed_res);
        let (task_id, _) = handle.split_once('.').unwrap();
        assert!(listed_text.contains(task_id), "{listed_text}");
        assert!(
            std::path::Path::new(path).exists(),
            "missing logPath {path}"
        );

        client
            .send(&call(
                call_id + 3,
                json!({"action": "poll", "taskId": handle, "waitMs": 0}),
            ))
            .await;
        let poll_receipt = client.result(call_id + 3).await;
        assert_eq!(poll_receipt.is_error, Some(true));
        assert!(poll_receipt.structured_content.is_none());
        assert!(text_of(&poll_receipt).contains("Do not resend input, keys, or resize."));

        // Read the same handle through the separate kill lane. The cursor was not advanced,
        // and a codemode callback ran even though the ordinary render slots were held.
        client
            .send(&call(
                call_id + 2,
                json!({"action": "kill", "taskId": handle}),
            ))
            .await;
        let killed = client.result(call_id + 2).await;
        assert!(killed.structured_content.is_none());
        let killed_text = text_of(&killed);
        assert!(killed_text.contains("render-slot-test"), "{killed_text}");
    }
    drop(held);
}

#[tokio::test]
async fn cancelling_waiting_file_work_does_not_dispatch_it() {
    let fixture = fixture();
    let daemon = &fixture.daemon;
    let mut client = connect(daemon).await;
    client.welcome("sesscancelwrite", 1).await;
    let held = Arc::clone(&daemon.render)
        .acquire_many_owned(RENDER_SLOTS as u32)
        .await
        .unwrap();
    let path = std::env::temp_dir().join(format!(
        "fastexec-cancel-render-write-{}-{}.txt",
        std::process::id(),
        crate::id::random_chars(12).unwrap(),
    ));
    assert!(!path.exists());
    client
        .send(&ToDaemon::Call {
            id: 1,
            tool: "write".into(),
            request_id: "1".into(),
            arguments: json!({"path": path, "content": "not written"}),
        })
        .await;
    client.send(&ToDaemon::Cancel { id: 1 }).await;
    let result = client.result(1).await;
    assert_eq!(result.is_error, Some(true));
    assert_eq!(
        text_of(&result),
        "Cancelled before the operation started; nothing was executed."
    );
    assert!(!path.exists());
    drop(held);

    client.send(&call(2, json!({"action": "list"}))).await;
    let listed = client.result(2).await;
    assert_ne!(listed.is_error, Some(true));
    assert!(listed.structured_content.is_none());
    assert!(!path.exists());
}

#[tokio::test]
async fn the_materialization_slot_is_not_released_while_detached_work_can_keep_its_value() {
    let pool = Arc::new(Semaphore::new(1));
    let slot = RenderSlot::new(&pool);
    let permit = slot.hold().await;
    let (start, started) = oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let worker = tokio::task::spawn_blocking(move || {
        let _ = start.send(());
        wait.recv_timeout(Duration::from_secs(10))
            .expect("the test releases the worker");
        (String::from("rendered output"), permit)
    });

    started.await.unwrap();
    drop(slot);
    assert!(pool.try_acquire().is_err());
    release.send(()).unwrap();
    let value = worker.await.unwrap();
    assert!(pool.try_acquire().is_err());
    drop(value);
    assert!(pool.try_acquire().is_ok());
}

#[tokio::test]
async fn a_reader_refusal_with_no_reserved_room_closes_the_connection() {
    let pool = BytePool::new(64 * 1024);
    let fixture = fixture_with_config(Config {
        memory: Some(Arc::clone(&pool)),
        ..Config::default()
    });
    let daemon = &fixture.daemon;
    let mut client = connect(daemon).await;
    client.welcome("sess99", 1).await;
    // Hold both shares, and send a duplicate ID so the reader refuses with no reserved room.
    let budget = pool.try_acquire(pool.limit()).expect("the budget is free");
    let reserved = Arc::clone(&daemon.reserved)
        .acquire_many_owned(RESERVED as u32)
        .await
        .expect("the reserved share is free");
    client
        .send(&call(1, json!({"action": "no-such-action"})))
        .await;
    // A duplicate ID is refused by the reader itself, which cannot wait for room.
    client.send(&call(1, json!({"action": "list"}))).await;
    assert_eq!(
        timeout(Duration::from_secs(20), client.next())
            .await
            .expect("the connection closed"),
        None,
        "a refusal with no room closes the connection"
    );
    drop((budget, reserved));
}

#[tokio::test]
async fn a_stalled_write_closes_the_connection() {
    let fixture = fixture_with_write_stall(Duration::from_millis(200));
    let daemon = &fixture.daemon;
    let mut client = connect_to(daemon, 64).await;
    // The Welcome is read, so the handshake is not what stalls.
    client.welcome("sess66", 1).await;
    // From here the client does not read, so the daemon's writes back up behind 64 bytes.
    for id in 1..=4 {
        client.send(&call(id, json!({"action": "list"}))).await;
    }
    // The connection is closed before the client reads anything, which is what makes the
    // stall the cause: reading first could let the blocked write through.
    let serving = client
        .serving
        .take()
        .expect("a duplex connection is served");
    timeout(Duration::from_secs(20), serving)
        .await
        .expect("the daemon closed the stalled connection")
        .expect("the connection handler ended cleanly");

    // What the client can still read is whatever fit in the pipe; the rest is gone.
    let mut drained = Vec::new();
    client
        .reader
        .read_to_end(&mut drained)
        .await
        .expect("the drained bytes end");
}

#[tokio::test]
async fn slow_forward_progress_outlives_write_stall_and_protocol_mismatch_is_rejected() {
    let stall = Duration::from_millis(80);
    let fixture = fixture_with_write_stall(stall);
    let daemon = &fixture.daemon;

    // 1. Old protocol (2) Hello is rejected immediately at handshake.
    let mut old_client = connect(daemon).await;
    let mut bad_hello = hello("oldproto", 1);
    if let ToDaemon::Hello { protocol, .. } = &mut bad_hello {
        *protocol = 2;
    }
    old_client.send(&bad_hello).await;
    match old_client.next().await {
        Some(ToEndpoint::Reject { message }) => {
            let expected = format!("protocol 2 is not {}", ipc::PROTOCOL);
            assert!(message.contains(&expected), "{message}");
        }
        other => panic!("expected Reject for protocol 2, got {other:?}"),
    }

    // 2. Slow reader draining 16 bytes every 25ms across > 250ms (> 3 * stall) still completes.
    let dir = std::env::temp_dir().join(format!(
        "fastexec-slow-stall-{}",
        crate::id::random_chars(8).unwrap()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("notes.txt"), "line1234567890\n".repeat(40)).unwrap();

    let mut slow_client = connect_to(daemon, 32).await;
    let mut h = hello("slowread", 1);
    if let ToDaemon::Hello { cwd, .. } = &mut h {
        *cwd = OsText::from(dir.as_os_str());
    }
    slow_client.send(&h).await;
    assert!(matches!(
        slow_client.next().await,
        Some(ToEndpoint::Welcome { .. })
    ));
    slow_client
        .send(&ToDaemon::Call {
            id: 1,
            tool: "read".into(),
            request_id: "1".into(),
            arguments: json!({"path": "notes.txt"}),
        })
        .await;

    let started = Instant::now();
    let mut raw = Vec::new();
    let mut buf = [0u8; 16];
    loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let n = slow_client.reader.read(&mut buf).await.unwrap();
        assert!(n > 0, "connection closed despite steady progress");
        raw.extend_from_slice(&buf[..n]);
        if raw.len() >= 4 {
            let body_len = u32::from_le_bytes(raw[..4].try_into().unwrap()) as usize;
            if raw.len() >= 4 + body_len {
                break;
            }
        }
    }
    assert!(
        started.elapsed() > stall * 2,
        "elapsed {:?} should exceed 2 * stall {:?}",
        started.elapsed(),
        stall
    );
    let decoded: Option<ToEndpoint> =
        ipc::read_frame(&mut std::io::Cursor::new(raw), lease::FRAME_LIMIT)
            .await
            .unwrap();
    assert!(matches!(decoded, Some(ToEndpoint::Result { id: 1, .. })));
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn an_aborted_serve_task_detaches_the_session() {
    let fixture = fixture();
    let daemon = &fixture.daemon;
    // A pipe small enough that the Welcome cannot be written while nobody reads it, so the
    // serving task is still inside the handshake when it is aborted.
    let mut client = connect_to(daemon, 64).await;
    client.send(&hello("abort1", 1)).await;
    until(|| lock(&daemon.sessions).contains_key("abort1")).await;

    let serving = client
        .serving
        .take()
        .expect("a duplex connection is served");
    serving.abort();
    let _ = serving.await;

    // The guard closed the attachment: nothing is live, and the entry is sweepable.
    let sessions = lock(&daemon.sessions);
    let entry = sessions.get("abort1").expect("the entry is still there");
    assert!(entry.live.is_none(), "the session was left attached");
    assert!(
        entry.detached_at.is_some(),
        "the session was never detached"
    );
    drop(sessions);
    daemon.sweep_sessions(Instant::now() + SESSION_TTL);
    assert!(!lock(&daemon.sessions).contains_key("abort1"));
}

#[tokio::test]
async fn a_frame_header_over_the_limit_closes_the_connection() {
    let fixture = fixture();
    let daemon = &fixture.daemon;
    let mut client = connect(daemon).await;
    client.welcome("sess77", 1).await;
    // A length the daemon will not read, and nothing behind it.
    let length = u32::try_from(ipc::REQUEST_LIMIT).unwrap() + 1;
    client
        .writer
        .write_all(&length.to_le_bytes())
        .await
        .unwrap();
    client.writer.flush().await.unwrap();
    assert_eq!(
        timeout(Duration::from_secs(20), client.next())
            .await
            .expect("the connection closed"),
        None
    );
}

#[tokio::test]
async fn sweep_drops_a_session_detached_past_the_ttl_and_keeps_an_attached_one() {
    let fixture = fixture();
    let daemon = &fixture.daemon;
    let mut gone = connect(daemon).await;
    gone.welcome("gone11", 1).await;
    drop(gone);
    let mut kept = connect(daemon).await;
    kept.welcome("kept22", 1).await;
    until(|| {
        lock(&daemon.sessions)
            .get("gone11")
            .is_some_and(|entry| entry.detached_at.is_some())
    })
    .await;

    daemon.sweep_sessions(Instant::now());
    assert_eq!(lock(&daemon.sessions).len(), 2, "a fresh detach stays");
    daemon.sweep_sessions(Instant::now() + SESSION_TTL);
    let sessions = lock(&daemon.sessions);
    assert!(
        !sessions.contains_key("gone11"),
        "the detached session was dropped"
    );
    assert!(
        sessions.contains_key("kept22"),
        "the attached session stays"
    );
    drop(sessions);
}
/// Runs `daemon` on `endpoint`, so the test talks to it over the transport as a client does.
fn run_on(daemon: &Arc<Daemon>, endpoint: &Endpoint) -> tokio::task::JoinHandle<()> {
    let listener = Listener::bind(endpoint.name()).unwrap();
    tokio::spawn(Arc::clone(daemon).run(listener))
}

#[tokio::test]
async fn two_clients_handshake_over_the_listener() {
    let fixture = fixture();
    let daemon = Arc::clone(&fixture.daemon);
    let endpoint = Endpoint::new("two-clients");
    let running = run_on(&daemon, &endpoint);

    let mut first = connect_over(&endpoint).await;
    first.welcome("first1", 1).await;
    // The second connection proves the listener had its next instance ready.
    let mut second = connect_over(&endpoint).await;
    second.welcome("second", 1).await;
    for (client, id) in [(&mut first, 1), (&mut second, 2)] {
        client.send(&call(id, json!({"action": "list"}))).await;
        let result = client.result(id).await;
        assert_ne!(result.is_error, Some(true), "{}", text_of(&result));
    }

    daemon.stop();
    timeout(Duration::from_secs(10), running)
        .await
        .expect("the daemon stopped")
        .unwrap();
}

#[tokio::test]
async fn the_daemon_admits_max_connections_and_refuses_the_next() {
    let fixture = fixture();
    let daemon = Arc::clone(&fixture.daemon);
    let endpoint = Endpoint::new("cap");
    let running = run_on(&daemon, &endpoint);

    let mut clients = Vec::new();
    for index in 0..MAX_CONNECTIONS {
        let mut client = connect_over(&endpoint).await;
        client.welcome(&format!("cap{index}"), 1).await;
        clients.push(client);
    }

    // The connection past the cap is accepted and dropped without a word.
    let mut refused = connect_over(&endpoint).await;
    assert_eq!(
        refused.next().await,
        None,
        "a connection past the cap was welcomed"
    );

    // One place comes free, and the next connection takes it.
    clients.pop();
    until(|| {
        let gate = lock(&daemon.gate);
        gate.live + gate.handshaking < MAX_CONNECTIONS
    })
    .await;
    let mut admitted = connect_over(&endpoint).await;
    admitted.welcome("after", 1).await;

    daemon.stop();
    timeout(Duration::from_secs(30), running)
        .await
        .expect("the daemon stopped")
        .unwrap();
}

#[tokio::test]
async fn an_idle_daemon_exits_on_its_own() {
    let fixture = fixture_with_config(Config {
        tick: Duration::from_millis(50),
        idle_after: Duration::from_millis(200),
        idle_exit: true,
        ..Config::default()
    });
    let daemon = Arc::clone(&fixture.daemon);
    let endpoint = Endpoint::new("idle-exit");
    let running = run_on(&daemon, &endpoint);

    let mut client = connect_over(&endpoint).await;
    client.welcome("idle01", 1).await;
    // A connected client keeps the daemon up well past `idle_after`.
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !running.is_finished(),
        "the daemon exited while a client was connected"
    );

    client.send(&ToDaemon::Bye).await;
    timeout(Duration::from_secs(5), running)
        .await
        .expect("the idle daemon exited")
        .unwrap();
    assert!(
        crate::transport::connect(endpoint.name()).await.is_err(),
        "the endpoint still accepts after the daemon exited"
    );
}

#[tokio::test]
async fn a_live_transfer_keeps_an_idle_exit_daemon_running() {
    let fixture = fixture_with_config(Config {
        tick: Duration::from_millis(50),
        idle_after: Duration::from_millis(200),
        idle_exit: true,
        ..Config::default()
    });
    let daemon = Arc::clone(&fixture.daemon);
    daemon
        .transfers()
        .create(std::env::temp_dir().join("pinned.bin"))
        .unwrap();
    let created = Instant::now();
    assert!(!daemon.idle_expired(created));
    assert!(!daemon.idle_expired(created + Duration::from_secs(30)));
    let expired = created + crate::transfer::IDLE;
    // The first check after the capability expires starts the daemon's own idle window.
    assert!(!daemon.idle_expired(expired));
    assert!(daemon.idle_expired(expired + Duration::from_millis(200)));
}

#[tokio::test]
async fn transfer_checks_disabled_ipc_target_before_bind_ip_conflicts_and_unspecified_addrs() {
    let fixture = fixture();
    let daemon = Arc::clone(&fixture.daemon);
    let dir = std::env::temp_dir().join(format!(
        "fastexec-daemon-transfer-unit-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let plain_launch = Arc::new(LaunchContext::new(dir.clone(), Vec::new()));
    let plain_session = Session::new("u1".into(), "u1".into(), Arc::clone(&plain_launch));
    let (outgoing, mut queued) = mpsc::unbounded_channel();

    // Raw IPC `"transfer"` on a session without `FASTEXEC_TRANSFER=true` is refused via `transfer_error`.
    run_call(
        Arc::clone(&daemon),
        Arc::clone(&plain_session),
        CancellationToken::new(),
        CancellationToken::new(),
        1,
        "transfer".into(),
        json!({"path": "file.bin"}),
        Arc::new(Mutex::new(HashMap::new())),
        outgoing.clone(),
        0,
        Default::default(),
    )
    .await;
    let disabled_frame = queued.recv().await.unwrap();
    let mut cursor = std::io::Cursor::new(disabled_frame.bytes);
    let Some(ToEndpoint::Result { result, .. }) =
        ipc::read_frame::<_, ToEndpoint>(&mut cursor, lease::FRAME_LIMIT)
            .await
            .unwrap()
    else {
        panic!("expected Result frame");
    };
    assert_eq!(result.is_error, Some(true));
    assert!(
        result.structured_content.is_none(),
        "direct MCP errors carry no structured_content"
    );
    assert_eq!(
        result.content,
        vec![ContentBlock::text(
            "transfer is disabled; set FASTEXEC_TRANSFER=true."
        )]
    );
    assert!(daemon.transfer_listener.get().is_none());

    // Malformed arguments on an enabled session also return pure text without binding `/t/`.
    let enabled_launch = Arc::new(LaunchContext::new(
        dir.clone(),
        vec![("FASTEXEC_TRANSFER".into(), "true".into())],
    ));
    let enabled_session = Session::new("u2".into(), "u2".into(), Arc::clone(&enabled_launch));
    run_call(
        Arc::clone(&daemon),
        Arc::clone(&enabled_session),
        CancellationToken::new(),
        CancellationToken::new(),
        2,
        "transfer".into(),
        json!({"unexpected": 1}),
        Arc::new(Mutex::new(HashMap::new())),
        outgoing,
        0,
        Default::default(),
    )
    .await;
    let bad_args_frame = queued.recv().await.unwrap();
    let mut cursor = std::io::Cursor::new(bad_args_frame.bytes);
    let Some(ToEndpoint::Result { result, .. }) =
        ipc::read_frame::<_, ToEndpoint>(&mut cursor, lease::FRAME_LIMIT)
            .await
            .unwrap()
    else {
        panic!("expected Result frame");
    };
    assert_eq!(result.is_error, Some(true));
    assert!(result.structured_content.is_none());
    assert!(
        result.content[0]
            .as_text()
            .unwrap()
            .text
            .starts_with("failed to deserialize parameters:")
    );
    assert!(daemon.transfer_listener.get().is_none());

    // Passing a directory fails before `ensure_transfer_listener` binds any port.
    let dir_result = daemon
        .transfer(
            crate::transfer::Request { path: ".".into() },
            &enabled_launch,
            Default::default(),
        )
        .await;
    assert_eq!(dir_result.result.is_error, Some(true));
    assert!(dir_result.result.structured_content.is_none());
    assert!(
        daemon.transfer_listener.get().is_none(),
        "directory target must not bind /t/ listener"
    );

    // Unspecified `0.0.0.0:0` without `FASTEXEC_PUBLIC_URL` fails before binding.
    let unspec_no_root = Arc::new(LaunchContext::new(
        dir.clone(),
        vec![
            ("FASTEXEC_TRANSFER".into(), "true".into()),
            ("FASTEXEC_HTTP_ALLOW_REMOTE".into(), "1".into()),
            ("FASTEXEC_TRANSFER_ADDR".into(), "0.0.0.0:0".into()),
        ],
    ));
    let unspec_err = daemon
        .transfer(
            crate::transfer::Request {
                path: "ok.bin".into(),
            },
            &unspec_no_root,
            Default::default(),
        )
        .await;
    assert_eq!(unspec_err.result.is_error, Some(true));
    assert!(unspec_err.result.structured_content.is_none());
    assert!(
        text_of(&unspec_err.result).contains("is an unspecified address; set FASTEXEC_PUBLIC_URL")
    );
    assert!(daemon.transfer_listener.get().is_none());

    // Valid mint binds `127.0.0.1:<port>`, and subsequent `0.0.0.0:0` (even with port 0 and
    // `FASTEXEC_PUBLIC_URL`) is rejected due to IP mismatch.
    let minted = daemon
        .transfer(
            crate::transfer::Request {
                path: "ok.bin".into(),
            },
            &enabled_launch,
            Default::default(),
        )
        .await;
    assert_eq!(minted.result.is_error, Some(false));
    assert!(minted.result.structured_content.is_none());
    let bound = *daemon.transfer_listener.get().expect("listener bound");
    assert_eq!(bound.ip(), std::net::Ipv4Addr::LOCALHOST);

    let conflict_launch = Arc::new(LaunchContext::new(
        dir.clone(),
        vec![
            ("FASTEXEC_TRANSFER".into(), "true".into()),
            ("FASTEXEC_HTTP_ALLOW_REMOTE".into(), "1".into()),
            ("FASTEXEC_TRANSFER_ADDR".into(), "0.0.0.0:0".into()),
            (
                "FASTEXEC_PUBLIC_URL".into(),
                "https://files.example/".into(),
            ),
        ],
    ));
    let conflict = daemon
        .transfer(
            crate::transfer::Request {
                path: "other.bin".into(),
            },
            &conflict_launch,
            Default::default(),
        )
        .await;
    assert_eq!(conflict.result.is_error, Some(true));
    assert!(conflict.result.structured_content.is_none());
    assert_eq!(
        text_of(&conflict.result),
        format!("the daemon already serves /t/ on {bound}, not 0.0.0.0:0; stop it first")
    );

    daemon.stop();

    // `--http 0.0.0.0:<port>` without `FASTEXEC_PUBLIC_URL` refuses stdio `Daemon::transfer`.
    let http_wildcard = Daemon::with_config(
        Arc::clone(&daemon.tasks),
        "fastexec".into(),
        env!("CARGO_PKG_VERSION").into(),
        Some("0.0.0.0:8765".into()),
        None,
        Config::default(),
    );
    let wildcard_err = http_wildcard
        .transfer(
            crate::transfer::Request {
                path: "ok.bin".into(),
            },
            &enabled_launch,
            Default::default(),
        )
        .await;
    assert_eq!(wildcard_err.result.is_error, Some(true));
    assert!(wildcard_err.result.structured_content.is_none());
    assert_eq!(
        text_of(&wildcard_err.result),
        "--http 0.0.0.0:8765 is an unspecified address; set FASTEXEC_PUBLIC_URL to the URL clients reach"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn a_default_daemon_runs_until_it_is_stopped() {
    let fixture = fixture_with_config(Config {
        tick: Duration::from_millis(50),
        idle_after: Duration::from_millis(100),
        ..Config::default()
    });
    let daemon = Arc::clone(&fixture.daemon);
    let endpoint = Endpoint::new("idle-off");
    let running = run_on(&daemon, &endpoint);

    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        !running.is_finished(),
        "the daemon exited although idleness is not a reason to"
    );

    daemon.stop();
    timeout(Duration::from_secs(5), running)
        .await
        .expect("the daemon stopped")
        .unwrap();
}

#[tokio::test]
async fn stopping_ends_a_connected_client_and_one_still_handshaking() {
    let fixture = fixture();
    let daemon = Arc::clone(&fixture.daemon);
    let endpoint = Endpoint::new("stopping");
    let running = run_on(&daemon, &endpoint);

    let mut client = connect_over(&endpoint).await;
    client.welcome("stop01", 1).await;
    // A second connection says nothing, so it is still inside its handshake.
    let mut silent = connect_over(&endpoint).await;
    until(|| lock(&daemon.gate).handshaking == 1).await;

    daemon.stop();
    // Both end at once: the silent one well before its 10 s handshake timeout would have.
    timeout(Duration::from_secs(5), async {
        assert_eq!(
            client.next().await,
            None,
            "the stopped daemon ended the connection"
        );
        assert_eq!(silent.next().await, None, "the stop left a handshake open");
    })
    .await
    .expect("the stop ended both connections");

    timeout(Duration::from_secs(5), running)
        .await
        .expect("the daemon stopped")
        .unwrap();
    let gate = lock(&daemon.gate);
    assert_eq!(
        (gate.handshaking, gate.live),
        (0, 0),
        "the gate still counts a connection"
    );
}

#[tokio::test]
async fn admitting_a_connection_restarts_the_idle_interval() {
    let fixture = fixture_with_config(Config {
        idle_after: Duration::from_millis(200),
        idle_exit: true,
        ..Config::default()
    });
    let daemon = &fixture.daemon;
    let start = Instant::now();
    // Idle from the first look, so the interval starts there.
    assert!(!daemon.idle_expired(start));
    // A connection that comes and goes between two ticks restarts the interval.
    let ticket = daemon.admit().expect("the daemon admits a connection");
    drop(ticket);
    let later = start + Duration::from_millis(200);
    assert!(
        !daemon.idle_expired(later),
        "the idle interval did not restart at the admission"
    );
    // A full `idle_after` after that admission, the daemon is idle long enough.
    assert!(daemon.idle_expired(later + Duration::from_millis(200)));
}

#[tokio::test]
async fn hello_emit_bypasses_sessions_deduplicates_and_blocks_task_forgery() {
    let fixture = fixture_with_config(Config::default());
    let daemon = &fixture.daemon;

    let mut emitter = connect(daemon).await;
    emitter
        .send(&ToDaemon::HelloEmit {
            protocol: ipc::PROTOCOL,
            build: env!("CARGO_PKG_VERSION").to_owned(),
        })
        .await;
    assert!(matches!(
        emitter.next().await,
        Some(ToEndpoint::Welcome { .. })
    ));
    assert!(
        lock(&daemon.sessions).is_empty(),
        "HelloEmit must not insert an entry into Daemon::sessions"
    );

    let ev = crate::events::ExternalEventInput {
        event_type: None,
        source: "github".to_owned(),
        topic: Some("pr".to_owned()),
        name: "merged".to_owned(),
        event_id: "id-1".to_owned(),
        timestamp: Some("2026-11-25T12:00:00Z".to_owned()),
        data: serde_json::json!({ "pr": 42 }),
    };
    emitter
        .send(&ToDaemon::EmitEvent { event: ev.clone() })
        .await;
    let first_cursor = match emitter.next().await {
        Some(ToEndpoint::EmitAck {
            ok: true,
            cursor: Some(c),
            deduplicated: false,
            ..
        }) => c,
        other => panic!("expected first EmitAck ok, got {other:?}"),
    };

    // Re-emitting the same (source, eventId) deduplicates.
    emitter.send(&ToDaemon::EmitEvent { event: ev }).await;
    match emitter.next().await {
        Some(ToEndpoint::EmitAck {
            ok: true,
            cursor: Some(c),
            deduplicated: true,
            ..
        }) => assert_eq!(c, first_cursor),
        other => panic!("expected deduplicated EmitAck, got {other:?}"),
    }

    // Forging task.exited via EmitEvent is rejected with -32024 Forbidden.
    emitter
        .send(&ToDaemon::EmitEvent {
            event: crate::events::ExternalEventInput {
                event_type: Some(crate::events::EVENT_TASK_EXITED.to_owned()),
                source: "github".to_owned(),
                topic: None,
                name: "forged".to_owned(),
                event_id: "id-2".to_owned(),
                timestamp: None,
                data: serde_json::json!({}),
            },
        })
        .await;
    match emitter.next().await {
        Some(ToEndpoint::EmitAck {
            ok: false,
            error: Some(err),
            ..
        }) => assert_eq!(err.code, -32024),
        other => panic!("expected -32024 Forbidden EmitAck, got {other:?}"),
    }

    emitter.send(&ToDaemon::Bye).await;
    assert_eq!(emitter.next().await, None);
}

#[tokio::test]
async fn event_ingress_budget_receipts_include_limits_and_keep_connection() {
    let pool = BytePool::new(128 << 10);
    let fixture = fixture_with_config(Config {
        memory: Some(Arc::clone(&pool)),
        ..Config::default()
    });
    let daemon = &fixture.daemon;
    let mut client = connect(daemon).await;
    client.welcome("eventsbudget", 1).await;
    let full = pool.try_acquire(pool.limit() - pool.used()).unwrap();
    for request in [
        ToDaemon::EventsList { id: 1 },
        ToDaemon::EventsPoll {
            id: 2,
            request: crate::events::PollRequest {
                name: crate::events::EVENT_EXTERNAL.into(),
                ..Default::default()
            },
        },
        ToDaemon::EventsStream {
            id: 3,
            request: crate::events::SubscribeRequest {
                name: crate::events::EVENT_EXTERNAL.into(),
                ..Default::default()
            },
        },
    ] {
        client.send(&request).await;
        let Some(ToEndpoint::EventFrame {
            frame: ipc::EventWireFrame::Error(error),
            ..
        }) = client.next().await
        else {
            panic!("the event request must receive a budget error");
        };
        assert_eq!(error.code, -32025);
        assert_eq!(
            error.data,
            Some(json!({"limit": "process_memory", "max": pool.limit()}))
        );
    }
    drop(full);
    client.send(&ToDaemon::EventsList { id: 4 }).await;
    assert!(matches!(
        client.next().await,
        Some(ToEndpoint::EventFrame {
            id: 4,
            frame: ipc::EventWireFrame::ListResult(_),
        })
    ));
    client.send(&ToDaemon::Bye).await;
    assert_eq!(client.next().await, None);
}

#[tokio::test]
async fn event_stream_keeps_input_charged_until_cancel_or_disconnect() {
    let pool = BytePool::new(128 << 10);
    let fixture = fixture_with_config(Config {
        memory: Some(Arc::clone(&pool)),
        ..Config::default()
    });
    let daemon = &fixture.daemon;
    for disconnect in [false, true] {
        let mut client = connect(daemon).await;
        client.welcome("eventsinput", 1).await;
        until(|| pool.used() == 0).await;
        let source = "ci";
        let topic = "build";
        let name = "done";
        let request = ToDaemon::EventsStream {
            id: 11,
            request: crate::events::SubscribeRequest {
                name: crate::events::EVENT_EXTERNAL.to_owned(),
                arguments: Some(json!({"source": source, "topic": topic, "name": name})),
                ..Default::default()
            },
        };
        client.send(&request).await;
        assert!(matches!(
            client.next().await,
            Some(ToEndpoint::EventFrame {
                id: 11,
                frame: ipc::EventWireFrame::Active(_),
            })
        ));
        // Status proves read_loop has finished dispatching the subscription.
        client.send(&ToDaemon::Status).await;
        assert!(matches!(
            client.next().await,
            Some(ToEndpoint::Status { .. })
        ));
        let retained = pool.used();
        assert!(
            retained >= source.len() + topic.len() + name.len(),
            "the active subscription still owns its filter strings, but only {retained} bytes remain charged"
        );

        client.send(&request).await;
        assert!(matches!(
            client.next().await,
            Some(ToEndpoint::EventFrame {
                id: 11,
                frame: ipc::EventWireFrame::Error(ipc::IpcEventError { code: -32602, .. }),
            })
        ));
        assert_eq!(
            pool.used(),
            retained,
            "a duplicate stream releases its input"
        );
        client
            .send(&ToDaemon::EventsStream {
                id: 12,
                request: crate::events::SubscribeRequest {
                    name: crate::events::EVENT_EXTERNAL.to_owned(),
                    arguments: Some(json!({"topic": 7})),
                    ..Default::default()
                },
            })
            .await;
        assert!(matches!(
            client.next().await,
            Some(ToEndpoint::EventFrame {
                id: 12,
                frame: ipc::EventWireFrame::Error(ipc::IpcEventError { code: -32602, .. }),
            })
        ));
        assert_eq!(
            pool.used(),
            retained,
            "an invalid filter releases its input"
        );
        assert_eq!(daemon.service.event_bus().active_streams(), 1);

        if disconnect {
            client.writer.shutdown().await.unwrap();
        } else {
            client.send(&ToDaemon::EventsCancel { id: 11 }).await;
            client.send(&ToDaemon::Status).await;
            assert!(matches!(
                client.next().await,
                Some(ToEndpoint::Status { .. })
            ));
        }
        until(|| pool.used() == 0).await;
        assert_eq!(daemon.service.event_bus().active_streams(), 0);
        if !disconnect {
            client.send(&ToDaemon::Bye).await;
        }
        assert_eq!(client.next().await, None);
        client.serving.take().unwrap().await.unwrap();
    }
}

#[tokio::test]
async fn event_cancel_releases_full_quota_before_reusing_request_id() {
    let fixture = fixture();
    let daemon = &fixture.daemon;
    let bus = Arc::clone(daemon.service().event_bus());
    let occupied: Vec<_> = (1..crate::events::MAX_EVENT_STREAMS)
        .map(|_| bus.try_acquire_stream().unwrap())
        .collect();
    let mut client = connect(daemon).await;
    client.welcome("eventsreuse", 1).await;
    let request = ToDaemon::EventsStream {
        id: 11,
        request: crate::events::SubscribeRequest {
            name: crate::events::EVENT_EXTERNAL.to_owned(),
            ..Default::default()
        },
    };
    client.send(&request).await;
    assert!(matches!(
        client.next().await,
        Some(ToEndpoint::EventFrame {
            id: 11,
            frame: ipc::EventWireFrame::Active(_),
        })
    ));
    assert_eq!(bus.active_streams(), crate::events::MAX_EVENT_STREAMS);

    for round in 0..4 {
        client.send(&ToDaemon::EventsCancel { id: 11 }).await;
        client.send(&request).await;
        let active = client.next().await;
        assert!(
            matches!(
                &active,
                Some(ToEndpoint::EventFrame {
                    id: 11,
                    frame: ipc::EventWireFrame::Active(_),
                })
            ),
            "{active:?}"
        );
        assert_eq!(bus.active_streams(), crate::events::MAX_EVENT_STREAMS);
        bus.publish_external(crate::events::ExternalEventInput {
            event_type: None,
            source: "quota".to_owned(),
            topic: None,
            name: "ready".to_owned(),
            event_id: round.to_string(),
            timestamp: None,
            data: serde_json::json!({}),
        })
        .unwrap();
        assert!(matches!(
            client.next().await,
            Some(ToEndpoint::EventFrame {
                id: 11,
                frame: ipc::EventWireFrame::Event(_),
            })
        ));
    }

    client.send(&ToDaemon::EventsCancel { id: 11 }).await;
    client.send(&ToDaemon::EventsList { id: 12 }).await;
    assert!(matches!(
        client.next().await,
        Some(ToEndpoint::EventFrame {
            id: 12,
            frame: ipc::EventWireFrame::ListResult(_),
        })
    ));
    assert_eq!(bus.active_streams(), occupied.len());
    drop(occupied);
    assert_eq!(bus.active_streams(), 0);
    client.send(&ToDaemon::Bye).await;
    assert_eq!(client.next().await, None);
}

#[tokio::test]
async fn daemon_events_rpc_and_active_http_stream_prevent_idle_exit() {
    let fixture = fixture_with_config(Config {
        idle_after: Duration::from_millis(200),
        idle_exit: true,
        ..Config::default()
    });
    let daemon = &fixture.daemon;

    // IPC EventsList / EventsStream / EventsCancel roundtrip.
    let mut client = connect(daemon).await;
    client.welcome("events1", 1).await;

    client.send(&ToDaemon::EventsList { id: 10 }).await;
    match client.next().await {
        Some(ToEndpoint::EventFrame {
            id: 10,
            frame: ipc::EventWireFrame::ListResult(res),
        }) => {
            assert_eq!(res.result_type, "complete");
            assert_eq!(res.events.len(), 2);
        }
        other => panic!("expected ListResult, got {other:?}"),
    }

    client
        .send(&ToDaemon::EventsStream {
            id: 11,
            request: crate::events::SubscribeRequest {
                name: crate::events::EVENT_EXTERNAL.to_owned(),
                ..Default::default()
            },
        })
        .await;
    match client.next().await {
        Some(ToEndpoint::EventFrame {
            id: 11,
            frame: ipc::EventWireFrame::Active(active),
        }) => assert_eq!(active.heartbeat_ms, crate::events::DEFAULT_HEARTBEAT_MS),
        other => panic!("expected Active frame, got {other:?}"),
    }

    daemon
        .service()
        .event_bus()
        .publish_external(crate::events::ExternalEventInput {
            event_type: None,
            source: "ci".to_owned(),
            topic: None,
            name: "done".to_owned(),
            event_id: "u-99".to_owned(),
            timestamp: Some("2026-11-25T10:00:00Z".to_owned()),
            data: serde_json::json!({ "ok": true }),
        })
        .unwrap();
    match client.next().await {
        Some(ToEndpoint::EventFrame {
            id: 11,
            frame: ipc::EventWireFrame::Event(ev),
        }) => {
            assert_eq!(ev.data["eventId"], "u-99");
            assert_eq!(ev.data["timestamp"], "2026-11-25T10:00:00Z");
            assert!(ev.data.get("upstreamEventId").is_none());
            assert!(ev.data.get("upstreamTruncated").is_none());
        }
        other => panic!("expected Event frame, got {other:?}"),
    }

    client.send(&ToDaemon::EventsCancel { id: 11 }).await;
    client.send(&ToDaemon::Bye).await;
    assert_eq!(client.next().await, None);
    // Wait for the connection's ticket to drop before checking idle_expired.
    timeout(Duration::from_secs(5), async {
        while {
            let gate = lock(&daemon.gate);
            gate.live + gate.handshaking > 0
        } {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    // An active HTTP/local EventBus stream keeps idle_expired() false even with zero IPC conns.
    let guard = daemon.service().event_bus().try_acquire_stream().unwrap();
    let t0 = Instant::now();
    assert!(!daemon.idle_expired(t0));
    assert!(!daemon.idle_expired(t0 + Duration::from_secs(10)));
    drop(guard);
    assert!(!daemon.idle_expired(t0 + Duration::from_secs(10)));
    assert!(daemon.idle_expired(t0 + Duration::from_secs(10) + Duration::from_millis(250)));
}

#[tokio::test]
async fn daemon_event_stream_overload_isolates_reserved_and_keeps_connection() {
    let pool = BytePool::new(128 << 10);
    let fixture = fixture_with_config(Config {
        memory: Some(Arc::clone(&pool)),
        ..Config::default()
    });
    let daemon = &fixture.daemon;
    let mut client = connect(daemon).await;
    client.welcome("streamoverload", 1).await;

    client
        .send(&ToDaemon::EventsStream {
            id: 21,
            request: crate::events::SubscribeRequest {
                name: crate::events::EVENT_EXTERNAL.to_owned(),
                ..Default::default()
            },
        })
        .await;
    assert!(matches!(
        client.next().await,
        Some(ToEndpoint::EventFrame {
            id: 21,
            frame: ipc::EventWireFrame::Active(_),
        })
    ));

    // Publish an external event into EventBus (which uses its own pool), then exhaust daemon.memory
    // so queue_event_frame cannot acquire main memory for the Event frame.
    let full = pool.try_acquire(pool.limit() - pool.used()).unwrap();
    daemon
        .service()
        .event_bus()
        .publish_external(crate::events::ExternalEventInput {
            event_type: None,
            source: "ci".to_owned(),
            topic: None,
            name: "build".to_owned(),
            event_id: "ev-mem-full".to_owned(),
            timestamp: None,
            data: serde_json::json!({"pad": "x".repeat(8192)}),
        })
        .unwrap();

    match client.next().await {
        Some(ToEndpoint::EventFrame {
            id: 21,
            frame: ipc::EventWireFrame::Terminated(err),
        }) => {
            assert_eq!(err.code, -32025);
            assert_eq!(
                err.message,
                "daemon active memory budget full during event stream"
            );
            assert_eq!(err.data, None);
        }
        other => panic!("expected Terminated(-32025), got {other:?}"),
    }
    assert!(matches!(
        client.next().await,
        Some(ToEndpoint::EventFrame {
            id: 21,
            frame: ipc::EventWireFrame::Complete,
        })
    ));

    drop(full);
    // The shared IPC connection remains healthy for subsequent RPCs.
    client.send(&ToDaemon::Status).await;
    assert!(matches!(
        client.next().await,
        Some(ToEndpoint::Status { .. })
    ));
    client.send(&ToDaemon::EventsList { id: 22 }).await;
    assert!(matches!(
        client.next().await,
        Some(ToEndpoint::EventFrame {
            id: 22,
            frame: ipc::EventWireFrame::ListResult(_),
        })
    ));
    client.send(&ToDaemon::Bye).await;
    assert_eq!(client.next().await, None);
}

#[tokio::test]
async fn g15_http_launch_handoff_bidirectional_stop_and_panic_propagation() {
    static CAPTURED: Mutex<Option<oneshot::Sender<(std::net::SocketAddr, CancellationToken)>>> =
        Mutex::new(None);

    fn recording_spawner(launch: HttpLaunch) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        let addr = launch.listener.local_addr().unwrap();
        let stopped = launch.stopped;
        if let Some(tx) = lock(&CAPTURED).take() {
            let _ = tx.send((addr, stopped.clone()));
        }
        Box::pin(async move {
            let _listener = launch.listener;
            stopped.cancelled().await;
        })
    }

    fn panicking_spawner(_launch: HttpLaunch) -> Pin<Box<dyn Future<Output = ()> + Send>> {
        Box::pin(async move {
            panic!("simulated HTTP server task failure");
        })
    }

    let _serial = one_at_a_time();

    // 1. Ephemeral HTTP port handoff, Status reporting, and daemon -> HTTP graceful shutdown.
    {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = tcp.local_addr().unwrap();
        let tasks = Tasks::new(Limits::OFF).unwrap();
        let daemon = Daemon::with_config(
            Arc::clone(&tasks),
            "fastexec".to_owned(),
            env!("CARGO_PKG_VERSION").to_owned(),
            Some(bound.to_string()),
            None,
            Config::default(),
        );
        let (tx, rx) = oneshot::channel();
        *lock(&CAPTURED) = Some(tx);

        let http_task = daemon.attach_http(
            PendingHttp {
                listener: tcp,
                digests: vec![[0xabu8; 32]],
                tools: crate::config::Tools::default(),
                launch: Arc::new(LaunchContext::current().unwrap()),
                public_root: None,
                extra_instructions: None,
                instructions_tool: crate::config::InstructionsToolMode::Off,
                skills: None,
            },
            recording_spawner,
        );
        let endpoint = Endpoint::new("g15-handoff");
        let running = run_on(&daemon, &endpoint);

        let (seen_addr, http_stopped) = timeout(Duration::from_secs(5), rx)
            .await
            .expect("spawner invoked")
            .unwrap();
        assert_eq!(seen_addr, bound);
        assert!(!http_stopped.is_cancelled());

        let mut client = connect_over(&endpoint).await;
        client.welcome("g15stat", 1).await;
        client.send(&ToDaemon::Status).await;
        match client.next().await {
            Some(ToEndpoint::Status { http, .. }) => {
                assert_eq!(http.as_deref(), Some(bound.to_string().as_str()));
            }
            other => panic!("expected Status, got {other:?}"),
        }

        client.send(&ToDaemon::Stop).await;
        assert_eq!(client.next().await, None);
        timeout(Duration::from_secs(5), running)
            .await
            .expect("daemon run stopped")
            .unwrap();
        timeout(Duration::from_secs(5), http_task)
            .await
            .expect("http task stopped")
            .unwrap();
        assert!(http_stopped.is_cancelled());
        tasks.shutdown();
    }

    // 2. Abnormal panic inside `spawn_http` triggers `StopOnDrop` and stops `Daemon::run`.
    {
        let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let bound = tcp.local_addr().unwrap();
        let tasks = Tasks::new(Limits::OFF).unwrap();
        let daemon = Daemon::with_config(
            Arc::clone(&tasks),
            "fastexec".to_owned(),
            env!("CARGO_PKG_VERSION").to_owned(),
            Some(bound.to_string()),
            None,
            Config::default(),
        );
        let endpoint = Endpoint::new("g15-panic");
        let running = run_on(&daemon, &endpoint);
        let mut client = connect_over(&endpoint).await;
        client.welcome("g15pan", 1).await;

        let http_task = daemon.attach_http(
            PendingHttp {
                listener: tcp,
                digests: vec![[0xcd; 32]],
                tools: crate::config::Tools::default(),
                launch: Arc::new(LaunchContext::current().unwrap()),
                public_root: None,
                extra_instructions: None,
                instructions_tool: crate::config::InstructionsToolMode::Off,
                skills: None,
            },
            panicking_spawner,
        );

        let err = timeout(Duration::from_secs(5), http_task)
            .await
            .expect("panicking http task finished")
            .expect_err("expected JoinError from panic");
        assert!(err.is_panic());

        timeout(Duration::from_secs(5), running)
            .await
            .expect("daemon stopped when http task panicked")
            .unwrap();
        assert_eq!(client.next().await, None);
        tasks.shutdown();
    }
}
