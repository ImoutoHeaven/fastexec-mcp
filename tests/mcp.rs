//! Contract tests through the real MCP stdio boundary: each test drives the built binary.

use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, Instant};

struct Server {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<Value>,
    next_id: u64,
    notifications: Vec<Value>,
    /// The IDs of the tasks `start` calls began, in order.
    started: Vec<String>,
}

impl Server {
    fn start() -> Server {
        Server::start_with(|_| {})
    }

    /// Starts the server with its temp directory (and so its log directory) under `temp`.
    fn start_with_temp(temp: &std::path::Path) -> Server {
        Server::start_with(|command| {
            command
                .env("TEMP", temp)
                .env("TMP", temp)
                .env("TMPDIR", temp);
        })
    }

    /// Starts the server after `configure` adjusts its launch, such as its environment.
    fn start_with(configure: impl FnOnce(&mut Command)) -> Server {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        command
            .env_remove("FASTEXEC_OUTPUT_MODE")
            .env_remove("FASTEXEC_STRUCTURED_CONTENT")
            .env_remove("FASTEXEC_TEST_TOTAL_LOG_LIMIT");
        configure(&mut command);
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn fastexec");
        let stdout = child.stdout.take().unwrap();
        let (tx, lines) = channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Ok(value) = serde_json::from_str(&line) {
                    let _ = tx.send(value);
                }
            }
        });
        let stdin = child.stdin.take();
        let mut server = Server {
            child,
            stdin,
            lines,
            next_id: 0,
            notifications: Vec::new(),
            started: Vec::new(),
        };
        server.request(
            "initialize",
            json!({"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "test", "version": "0"}}),
        );
        server.send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        server
    }

    fn send(&mut self, message: Value) {
        let stdin = self.stdin.as_mut().unwrap();
        writeln!(stdin, "{message}").unwrap();
        stdin.flush().unwrap();
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        self.response(id)
    }

    fn response(&mut self, id: u64) -> Value {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            let message = self
                .lines
                .recv_timeout(deadline - Instant::now())
                .expect("response in time");
            if message["id"] == json!(id) {
                return message;
            }
            self.notifications.push(message);
        }
    }

    /// Calls the tool and returns (structuredContent, text, isError).
    fn call(&mut self, arguments: Value) -> (Value, String, bool) {
        let start = arguments["action"] == "start";
        let message = self.request(
            "tools/call",
            json!({"name": "fastexec", "arguments": arguments}),
        );
        let result = &message["result"];
        assert!(result.is_object(), "protocol error: {message}");
        let text = result["content"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let is_error = result["isError"] == json!(true);
        if start && !is_error {
            // The status line, the last line that opens with `[`, names the task after the state.
            let id = text
                .lines()
                .rev()
                .find_map(|line| {
                    line.strip_prefix('[')?
                        .split_once("] ")?
                        .1
                        .split(' ')
                        .next()
                })
                .unwrap_or_else(|| panic!("no status line: {text}"));
            self.started.push(id.to_string());
        }
        (result["structuredContent"].clone(), text, is_error)
    }

    /// The ID of the `n`th task this server started, counting from 1.
    fn id(&self, n: usize) -> String {
        self.started[n - 1].clone()
    }

    fn ok(&mut self, arguments: Value) -> (Value, String) {
        let (structured, text, is_error) = self.call(arguments);
        assert!(!is_error, "unexpected error: {text}");
        (structured, text)
    }
}

impl Drop for Server {
    /// Closes stdin like a real host, so the server removes its own log directory.
    fn drop(&mut self) {
        drop(self.stdin.take());
        let deadline = Instant::now() + Duration::from_secs(5);
        while matches!(self.child.try_wait(), Ok(None)) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A path under the temp directory that is removed on drop, after a panic too.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Scratch {
        let path =
            std::env::temp_dir().join(format!("fastexec-test-{}-{name}", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(&path);
        Scratch(path)
    }
}

impl std::ops::Deref for Scratch {
    type Target = PathBuf;
    fn deref(&self) -> &PathBuf {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A scratch path bash can use on every platform.
fn scratch(name: &str) -> (Scratch, String) {
    let path = Scratch::new(name);
    let bash_path = path.to_string_lossy().replace('\\', "/");
    (path, bash_path)
}

fn file_len(path: &PathBuf) -> u64 {
    std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0)
}

/// Starts a background writer that appends to `bash_path` every 100 ms.
fn heartbeat(bash_path: &str) -> String {
    format!("(while :; do echo x >> '{bash_path}'; sleep 0.1; done) &")
}

/// Waits up to 30 s for a heartbeat's first line; a loaded machine can start it late.
fn wait_for_heartbeat(path: &PathBuf) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while file_len(path) == 0 {
        assert!(Instant::now() < deadline, "the heartbeat never started");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Waits up to 30 s for a file a command creates.
fn wait_for_file(path: &std::path::Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "{} never appeared",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn assert_stops_growing(path: &PathBuf) {
    std::thread::sleep(Duration::from_millis(800));
    let before = file_len(path);
    std::thread::sleep(Duration::from_millis(800));
    assert_eq!(file_len(path), before, "a descendant kept running");
    let _ = std::fs::remove_file(path);
}

#[test]
fn short_command_returns_its_final_result_in_one_call() {
    let mut server = Server::start();
    let (out, text) =
        server.ok(json!({"action": "start", "command": "echo hello; echo oops >&2; exit 3"}));
    assert_eq!(out["state"], "exited");
    assert_eq!(out["exitCode"], 3);
    assert_eq!(out["leftovers"], json!([]), "{text}");
    assert!(
        text.starts_with(&format!("hello\noops\n\n[exited 3] {}", server.id(1))),
        "{text}"
    );
    let (_, text) = server.ok(json!({"action": "start", "command": "true"}));
    assert!(text.starts_with("(no new output)"), "{text}");
    for pty in [false, true] {
        let (out, text) =
            server.ok(json!({"action": "start", "command": "kill -INT $$", "pty": pty}));
        assert_eq!(
            out["exitCode"], 130,
            "a signal exit reports 128 + signal (pty={pty}): {text}"
        );
    }
}

#[test]
fn output_modes_place_start_and_poll_body_without_losing_metadata() {
    let mut first_ids = Vec::new();
    for (mode, text_body, structured_body) in [
        (None, true, false),
        (Some("text"), true, false),
        (Some("both"), true, true),
        (Some("structured"), false, true),
    ] {
        let mut server = Server::start_with(|command| {
            if let Some(mode) = mode {
                command.env("FASTEXEC_OUTPUT_MODE", mode);
            }
        });
        let (out, text) = server.ok(json!({
            "action": "start", "command": "printf 'MODE_START\\n'; exit 3"
        }));
        assert_eq!(
            text.contains("MODE_START"),
            text_body,
            "mode={mode:?}: {text}"
        );
        assert_eq!(
            out.get("output").and_then(Value::as_str),
            structured_body.then_some("MODE_START"),
            "mode={mode:?}: {out}"
        );
        assert!(text.contains("[exited 3] "), "{text}");
        assert_eq!(out["taskId"], server.id(1), "{text}");
        first_ids.push(server.id(1));
        assert_eq!(out["state"], "exited");
        assert_eq!(out["exitCode"], 3);
        assert!(std::path::Path::new(out["logPath"].as_str().unwrap()).exists());

        server.ok(json!({
            "action": "start", "command": "read -r line; printf '%s\\n' \"$line\"", "waitMs": 0
        }));
        let (out, text) = server.ok(json!({
            "action": "poll", "taskId": server.id(2), "input": "MODE_POLL\n", "eof": true, "waitMs": 10000
        }));
        assert_eq!(
            text.contains("MODE_POLL"),
            text_body,
            "mode={mode:?}: {text}"
        );
        assert_eq!(
            out.get("output").and_then(Value::as_str),
            structured_body.then_some("MODE_POLL"),
            "mode={mode:?}: {out}"
        );
        assert_eq!(out["state"], "exited");
        assert_eq!(out["exitCode"], 0);
        let (out, text) = server.ok(json!({"action": "poll", "taskId": server.id(2), "waitMs": 0}));
        assert_eq!(text.contains("(no new output)"), text_body, "{text}");
        assert_eq!(
            out.get("output").and_then(Value::as_str),
            structured_body.then_some("")
        );
        assert!(
            text.contains(&format!("[exited 0] {}", server.id(2))),
            "{text}"
        );
        assert_eq!(out["omittedRange"], Value::Null);

        let (out, text, is_error) = server.call(json!({"action": "poll", "taskId": "missing"}));
        assert!(is_error && text.contains("Unknown taskId"), "{text}");
        assert_eq!(out["ok"], false);
    }
    // Each server draws its own random IDs, so the servers' first tasks differ.
    let mut distinct = first_ids.clone();
    distinct.sort();
    distinct.dedup();
    assert_eq!(distinct.len(), first_ids.len(), "{first_ids:?}");

    let result = Command::new(env!("CARGO_BIN_EXE_fastexec"))
        .env("FASTEXEC_OUTPUT_MODE", "bogus")
        .output()
        .unwrap();
    assert!(
        !result.status.success(),
        "an invalid mode must fail at startup"
    );
    assert!(String::from_utf8_lossy(&result.stderr).contains("FASTEXEC_OUTPUT_MODE"));
}

#[test]
fn structured_content_can_be_disabled_for_every_action() {
    for mode in ["text", "both", "structured"] {
        let mut server = Server::start_with(|command| {
            command
                .env("FASTEXEC_OUTPUT_MODE", mode)
                .env("FASTEXEC_STRUCTURED_CONTENT", "false");
        });
        let check = |server: &mut Server, arguments: Value, expected: &str, is_error: bool| {
            let (out, text, error) = server.call(arguments);
            assert_eq!(out, Value::Null, "mode={mode}: {text}");
            assert!(text.contains(expected), "mode={mode}: {text}");
            assert_eq!(error, is_error, "mode={mode}: {text}");
        };
        check(
            &mut server,
            json!({"action": "start", "command": "cat <<'EOF'\nHello, world!\nEOF"}),
            "Hello, world!\n\n[exited 0] ",
            false,
        );
        check(
            &mut server,
            json!({"action": "start", "command": "read -r line; printf '%s\\n' \"$line\"", "waitMs": 0}),
            "[running] ",
            false,
        );
        let id = server.id(2);
        check(
            &mut server,
            json!({"action": "poll", "taskId": id, "input": "POLL_BODY\n", "eof": true, "waitMs": 10000}),
            &format!("POLL_BODY\n\n[exited 0] {id}"),
            false,
        );
        check(
            &mut server,
            json!({"action": "list"}),
            &format!("{id} [exited 0]"),
            false,
        );
        check(
            &mut server,
            json!({"action": "kill", "taskId": id}),
            &format!("[exited 0] {id}"),
            false,
        );
        check(
            &mut server,
            json!({"action": "poll", "taskId": "missing"}),
            "Unknown taskId",
            true,
        );
        check(
            &mut server,
            json!({"action": "start"}),
            "start needs a non-empty",
            true,
        );
    }

    let result = Command::new(env!("CARGO_BIN_EXE_fastexec"))
        .env("FASTEXEC_OUTPUT_MODE", "text")
        .env("FASTEXEC_STRUCTURED_CONTENT", "bogus")
        .output()
        .unwrap();
    assert!(
        !result.status.success(),
        "an invalid switch must fail at startup"
    );
    assert!(String::from_utf8_lossy(&result.stderr).contains("FASTEXEC_STRUCTURED_CONTENT"));
}

#[test]
fn kill_after_ms_bounds_task_lifetime_independently_of_waits() {
    let mut server = Server::start();
    let (out, _) = server
        .ok(json!({"action": "start", "command": "sleep 30", "killAfterMs": 1000, "waitMs": 0}));
    assert_eq!(
        out["state"], "running",
        "the deadline outlives the ended wait"
    );
    let (out, text) = server.ok(json!({"action": "poll", "taskId": server.id(1), "waitMs": 15000}));
    assert_eq!(
        (out["state"].as_str(), &out["lifetimeExpired"]),
        (Some("killed"), &json!(true)),
        "{text}"
    );
    assert!(text.contains("killAfterMs"), "{text}");
    // Completion before the deadline and the disabled forms keep the natural result.
    for limit in [json!(60000), json!(0), Value::Null] {
        let (out, text) = server.ok(json!({"action": "start", "command": "sleep 1; exit 4", "killAfterMs": limit, "waitMs": 15000}));
        assert_eq!(
            (
                out["state"].as_str(),
                out["exitCode"].as_i64(),
                &out["lifetimeExpired"]
            ),
            (Some("exited"), Some(4), &json!(false)),
            "killAfterMs={limit}: {text}"
        );
    }
}

#[test]
fn background_footer_names_unreported_tasks_once_failures_first() {
    let markers = ["footer-0", "footer-7"].map(scratch);
    let [(_, zero), (_, seven)] = &markers;
    let mut server = Server::start();
    // The first task prints once both others have run their last command, and some time after.
    let first = format!(
        "until [ -e {zero} ] && [ -e {seven} ]; do sleep 0.1; done; sleep 2; echo both; sleep 60"
    );
    server.ok(json!({"action": "start", "command": first, "waitMs": 0}));
    server.ok(json!({"action": "start", "command": format!("sleep 1; touch {zero}; exit 0"), "waitMs": 0}));
    server.ok(json!({"action": "start", "command": format!("sleep 1; touch {seven}; exit 7"), "waitMs": 0}));
    let (_, text) = server.ok(json!({"action": "poll", "taskId": server.id(1), "waitMs": 60000, "returnWhen": {"outputContains": ["both"]}}));
    let footer = text.lines().last().unwrap();
    let expected = format!(
        "(Background: {} exited 7, {} exited 0.)",
        server.id(3),
        server.id(2)
    );
    assert_eq!(footer, expected, "{text}");

    // Reported completions leave the footer; running tasks stay in it.
    let (_, text) = server.ok(json!({"action": "start", "command": "true"}));
    let footer = text.lines().last().unwrap();
    assert!(
        footer.starts_with(&format!("(Background: {} running ", server.id(1)))
            && !footer.contains(&server.id(2)),
        "{text}"
    );

    // Many tasks: three IDs, then counts, within maxBytes.
    for _ in 0..5 {
        server.ok(json!({"action": "start", "command": "sleep 60", "waitMs": 0}));
    }
    let (_, text) =
        server.ok(json!({"action": "start", "command": "seq 1 5000", "maxBytes": 1024}));
    assert!(text.len() <= 1024, "{} bytes", text.len());
    let footer = text.lines().last().unwrap();
    assert!(
        footer.starts_with(&format!("(Background: {} running ", server.id(9)))
            && footer.ends_with("; 3 more; 6 running; use list.)"),
        "{text}"
    );
    let (_, text) = server.ok(json!({"action": "start", "command": "seq 1 5000", "maxBytes": 100}));
    assert!(text.len() <= 100, "{} bytes: {text}", text.len());
}

#[cfg(unix)]
#[test]
fn bash_override_must_exit_successfully() {
    use std::os::unix::fs::PermissionsExt;
    let dir = Scratch::new("fake-bash");
    std::fs::create_dir_all(&*dir).unwrap();
    let fake = dir.join("bash");
    std::fs::write(&fake, "#!/bin/sh\necho 'GNU bash, version 5.2'\nexit 1\n").unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut server = Server::start_with(|command| {
        command.env("FASTEXEC_BASH", &fake);
    });
    let (_, text, is_error) = server.call(json!({"action": "start", "command": "true"}));
    assert!(is_error && text.contains("Invalid FASTEXEC_BASH"), "{text}");
}

#[test]
fn long_command_yields_then_poll_returns_only_unseen_output() {
    let mut server = Server::start();
    let (out, text) = server.ok(json!({"action": "start", "command": "echo first; sleep 4; echo second", "waitMs": 30000, "returnWhen": {"outputContains": ["first"]}}));
    assert_eq!(out["state"], "running");
    assert!(text.starts_with("first\n"), "{text}");
    let (out, text) = server.ok(json!({"action": "poll", "taskId": server.id(1), "waitMs": 30000}));
    assert_eq!(
        (out["state"].as_str(), out["exitCode"].as_i64()),
        (Some("exited"), Some(0))
    );
    assert!(
        text.starts_with("second\n") && !text.contains("first") && !text.contains("wait:"),
        "a wait without returnWhen adds no note: {text}"
    );
}

#[test]
fn pipe_input_arrives_verbatim_and_eof_closes_stdin() {
    // Git for Windows' bin/bash.exe launcher retains an inherited stdin handle.
    // Use discovery's real usr/bin/bash.exe so closing fd 0 closes the pipe reader.
    let mut server = Server::start_with(|command| {
        command.env_remove("FASTEXEC_BASH");
    });
    server.ok(json!({"action": "start", "command": "while IFS= read -r l || [ -n \"$l\" ]; do echo \"got:$l\"; done; echo end", "waitMs": 0}));
    let (_, text) = server.ok(json!({"action": "poll", "taskId": server.id(1), "input": "a b\n", "waitMs": 30000, "returnWhen": {"outputContains": ["got:a b"]}}));
    assert!(text.starts_with("got:a b\n"), "{text}");
    let (out, text) = server
        .ok(json!({"action": "poll", "taskId": server.id(1), "input": "c", "eof": true, "waitMs": 10000}));
    assert_eq!(out["state"], "exited");
    assert!(text.starts_with("got:c\nend\n"), "{text}");
    let (_, text, is_error) =
        server.call(json!({"action": "poll", "taskId": server.id(1), "input": "late\n"}));
    assert!(is_error && text.contains("exited"), "{text}");
    let (out, _) = server.ok(json!({"action": "poll", "taskId": server.id(1), "waitMs": 0}));
    assert_eq!(
        out["state"], "exited",
        "a plain poll still reads a finished task"
    );

    // A program that closes its stdin refuses later input instead of queueing it.
    let (_, text) = server.ok(json!({"action": "start", "command": "exec 0<&-; echo stdin-closed; sleep 30", "waitMs": 30000, "returnWhen": {"outputContains": ["stdin-closed"]}}));
    assert!(text.starts_with("stdin-closed\n"), "{text}");
    // A write discovers the closed reader asynchronously. Wait for rejection,
    // rather than assuming the writer thread has finished after a fixed sleep.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (_, text, is_error) = server
            .call(json!({"action": "poll", "taskId": server.id(2), "input": "x\n", "waitMs": 50}));
        if is_error {
            assert!(text.contains("stdin is closed"), "{text}");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "closed stdin still accepts input: {text}"
        );
    }
    server.ok(json!({"action": "kill", "taskId": server.id(2)}));
}

#[test]
fn pty_task_sees_a_terminal_and_accepts_a_hidden_password() {
    let mut server = Server::start();
    let command = "[ -t 0 ] && echo tty; read -r -s -p 'Password: ' p; echo; echo \"len=${#p}\"";
    let (out, text) = server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 30000, "returnWhen": {"outputContains": ["Password:"]}}));
    assert_eq!(out["state"], "running", "{text}");
    assert!(text.contains("tty") && text.contains("Password:"), "{text}");
    let (out, text) = server.ok(
        json!({"action": "poll", "taskId": server.id(1), "input": "secret\r", "waitMs": 10000}),
    );
    assert_eq!(out["state"], "exited", "{text}");
    assert!(text.contains("len=6") && !text.contains("secret"), "{text}");
    let (_, text, is_error) =
        server.call(json!({"action": "poll", "taskId": server.id(1), "eof": true}));
    assert!(
        is_error && text.contains("EOF_UNSUPPORTED_IN_PTY"),
        "{text}"
    );
}

#[test]
fn pty_keys_follow_input_and_the_programs_cursor_mode() {
    let mut server = Server::start();
    // The program switches to application cursor mode (DECCKM), then dumps what it reads.
    let command = "stty raw -echo; printf '\\033[?1hready\\r\\n'; head -c 9 | od -An -tx1";
    let (_, text) = server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 30000, "returnWhen": {"outputContains": ["ready"]}}));
    assert!(text.contains("ready"), "{text}");
    let (_, text, is_error) =
        server.call(json!({"action": "poll", "taskId": server.id(1), "keys": ["Up", "C-Enter"]}));
    assert!(is_error && text.contains("C-Enter"), "{text}");
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "input": "ab", "keys": ["Enter", "Up", "C-c", "M-x"], "waitMs": 10000
    }));
    assert_eq!(out["state"], "exited", "{text}");
    assert!(
        text.contains("61 62 0d 1b 4f 41 03 1b 78"),
        "input, then keys in order, with Up in application cursor mode: {text}"
    );

    server.ok(json!({"action": "start", "command": "cat", "waitMs": 0}));
    let (_, text, is_error) =
        server.call(json!({"action": "poll", "taskId": server.id(2), "keys": ["Enter"]}));
    assert!(is_error && text.contains("PTY tasks only"), "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(2)}));
}

/// The task host shares the terminal's foreground group with bash and must leave Ctrl-C to it.
// ConPTY delivers Ctrl-C to msys programs after a variable delay, so Windows is left out.
#[cfg(unix)]
#[test]
fn ctrl_c_interrupts_the_program_of_a_pty_task() {
    use std::os::unix::process::CommandExt;
    // Also from a server that inherited SIGINT ignored and blocked, as a background job can.
    let hostile = Server::start_with(|command| unsafe {
        command.pre_exec(|| {
            let mut set: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut set);
            libc::sigaddset(&mut set, libc::SIGINT);
            libc::sigprocmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
            libc::signal(libc::SIGINT, libc::SIG_IGN);
            Ok(())
        });
    });
    for mut server in [Server::start(), hostile] {
        let (_, text) = server.ok(json!({"action": "start", "command": "echo ready; sleep 30; echo after", "pty": true, "waitMs": 10000, "returnWhen": {"outputContains": ["ready"]}}));
        assert!(text.contains("[running]"), "{text}");
        let (out, text) = server.ok(
            json!({"action": "poll", "taskId": server.id(1), "keys": ["C-c"], "waitMs": 10000}),
        );
        assert_eq!(out["state"], "exited", "{text}");
        assert_eq!(out["exitCode"], 130, "{text}");
        assert!(!text.contains("after"), "{text}");
    }
}

#[test]
fn keys_pause_after_input_and_follow_the_modes_set_meanwhile() {
    let mut server = Server::start();
    // The program dumps each read, which returns the bytes that have arrived, and switches to
    // application cursor mode after the first one.
    let command = concat!(
        "stty raw -echo; printf 'ready\\r\\n'; dd bs=64 count=1 2>/dev/null | od -An -tx1; ",
        "printf '\\033[?1h'; for i in 1 2; do dd bs=64 count=1 2>/dev/null | od -An -tx1; done"
    );
    // With a pause the keys come after the text, Up after the mode switch; without one, the
    // text and keys go out in one write, read at once, before it. The pause is longer than the
    // default, so a slow machine starts the program's next read in time; the default pause is
    // measured below.
    for (delay, first, up) in [
        (Some(1000), " 61 62\n", "1b 4f 41"),
        (Some(0), " 61 62 0d 1b 5b 41\n", "1b 5b 41"),
        (None, "", ""),
    ] {
        let (out, _) = server.ok(json!({
            "action": "start", "command": command, "pty": true, "waitMs": 20000,
            "returnWhen": {"outputContains": ["ready"]}
        }));
        let task = out["taskId"].clone();
        let mut poll = json!({
            "action": "poll", "taskId": task, "input": "ab", "keys": ["Enter", "Up"],
            "waitMs": 10000, "returnWhen": {"outputContains": [" 41"]}
        });
        if let Some(delay) = delay {
            poll["keyDelayMs"] = json!(delay);
        }
        let begun = Instant::now();
        let (_, text) = server.ok(poll);
        if delay.is_none() {
            // 150 ms before each of the two keys.
            assert!(begun.elapsed() >= Duration::from_millis(300), "{text}");
        }
        assert!(
            text.starts_with(first) && text.contains(up),
            "{first:?}, {up}: {text}"
        );
        server.ok(json!({"action": "kill", "taskId": task}));
    }
}

#[test]
fn pty_keys_follow_the_kitty_keyboard_protocol_the_program_pushes() {
    let mut server = Server::start();
    // The program pushes the disambiguation flag, queries the flags, and dumps what it reads;
    // then it pops the flag and dumps again.
    let command = concat!(
        "stty raw -echo; printf '\\033[>1u\\033[?u'; IFS= read -r -d u flags; ",
        "printf 'flags=%s\\r\\n' \"${flags#?}\"; head -c 14 | od -An -tx1; ",
        "printf '\\033[<upopped\\r\\n'; head -c 2 | od -An -tx1"
    );
    let (_, text) = server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 30000, "returnWhen": {"outputContains": ["flags=[?1"]}}));
    assert!(text.contains("flags=[?1"), "{text}");
    let (_, text) = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "keys": ["Escape", "C-i", "Tab"], "waitMs": 30000,
        "returnWhen": {"outputContains": ["popped"]}
    }));
    assert!(
        text.contains("1b 5b 32 37 75 1b 5b 31 30 35 3b 35 75 09") && text.contains("popped"),
        "Escape and C-i as CSI u, Tab as itself: {text}"
    );
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "keys": ["Escape", "Tab"], "waitMs": 10000
    }));
    assert_eq!(out["state"], "exited", "{text}");
    assert!(text.contains("1b 09"), "legacy bytes once popped: {text}");
}

#[test]
fn a_terminal_emulator_failure_leaves_the_output_flowing() {
    let mut server = Server::start();
    // alacritty_terminal 0.26 panics past 4096 kitty keyboard pushes.
    let command = "printf '\\033[>1u%.0s' $(seq 1 4097); printf 'alive\\r\\n'; read -r x; printf 'done\\r\\n'";
    let (_, text) = server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 30000, "returnWhen": {"outputContains": ["alive"]}}));
    assert!(text.contains("alive"), "{text}");
    let (_, text, is_error) =
        server.call(json!({"action": "poll", "taskId": server.id(1), "keys": ["Enter"]}));
    assert!(is_error && text.contains("stopped"), "{text}");
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "input": "\r", "waitMs": 10000
    }));
    assert_eq!(out["state"], "exited", "{text}");
    assert!(text.contains("done"), "{text}");
}

#[test]
fn screen_shows_the_rendered_terminal_and_marks_the_stream_read() {
    let mut server = Server::start();
    // Frame 1 is printed, then cleared and overwritten in place at row 2.
    let command =
        "printf 'frame-1\\r\\n'; sleep 0.5; printf '\\033[H\\033[2J\\033[2;3Hframe-2'; read -r x";
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "pty": true, "screen": true, "waitMs": 30000,
        "returnWhen": {"screenContains": ["frame-2"]}
    }));
    assert_eq!(out["state"], "running", "{text}");
    let body = text.split("\n\n[").next().unwrap();
    assert_eq!(body.trim_end(), "\n  frame-2", "{text}");
    assert_eq!(out["cursor"], json!([2, 10]), "{text}");
    let (_, text) = server.ok(json!({"action": "poll", "taskId": server.id(1), "waitMs": 0}));
    assert!(text.starts_with("(no new output)"), "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(1)}));

    // A full first row leaves the cursor in the last column, pending the wrap.
    let command = "printf '%0120d' 0; read -r x";
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "pty": true, "screen": true, "waitMs": 30000,
        "returnWhen": {"screenContains": ["0000000000"]}
    }));
    assert_eq!(out["cursor"], json!([1, 120]), "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(2)}));

    // A synchronized update the program leaves open shows once it times out.
    let command = "printf 'shown\\r\\n\\033[?2026hheld'; read -r x";
    let (_, text) = server.ok(json!({
        "action": "start", "command": command, "pty": true, "screen": true, "waitMs": 30000,
        "returnWhen": {"screenContains": ["held"]}
    }));
    assert_eq!(text.split("\n\n[").next().unwrap(), "shown\nheld", "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(3)}));
}

#[test]
fn return_when_ends_a_wait_on_unseen_output_text_or_quiet() {
    let mut server = Server::start();
    let command =
        "printf 'boot\\n'; sleep 0.5; printf '\\033[32mServer\\033[0m listening\\n'; sleep 30";
    let begun = Instant::now();
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "waitMs": 20000,
        "returnWhen": {"outputContains": ["error:", "Server listening"]}
    }));
    assert!(begun.elapsed() < Duration::from_secs(10), "{text}");
    assert_eq!(
        out["waitEndedBy"],
        json!({"condition": "output_contains", "text": "Server listening"}),
        "{text}"
    );
    assert_eq!(out["state"], "running", "{text}");
    assert!(
        text.contains("wait: output matched \"Server listening\""),
        "{text}"
    );
    // Output an earlier result showed does not match again.
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "waitMs": 500,
        "returnWhen": {"outputContains": ["listening"]}
    }));
    assert_eq!(
        out["waitEndedBy"],
        json!({"condition": "max_wait"}),
        "{text}"
    );
    assert!(text.contains("wait: waitMs elapsed"), "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(1)}));

    // Every output restarts the quiet time.
    let command = "echo a; sleep 0.5; echo b; sleep 0.5; echo c; sleep 30";
    let begun = Instant::now();
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "loginShell": false, "waitMs": 20000,
        "returnWhen": {"quietMs": 1500}
    }));
    assert!(begun.elapsed() >= Duration::from_millis(2400), "{text}");
    assert_eq!(out["waitEndedBy"], json!({"condition": "quiet"}), "{text}");
    assert!(text.starts_with("a\nb\nc\n"), "{text}");
    // The quiet time counts from the start of the wait, so a program already quiet ends a
    // wait that sees no output, after the full time.
    let begun = Instant::now();
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": server.id(2), "waitMs": 20000, "returnWhen": {"quietMs": 300}
    }));
    let waited = begun.elapsed();
    assert!(
        waited >= Duration::from_millis(300) && waited < Duration::from_secs(10),
        "{waited:?} {text}"
    );
    assert_eq!(out["waitEndedBy"], json!({"condition": "quiet"}), "{text}");
    assert!(text.starts_with("(no new output)"), "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(2)}));

    let (out, text) = server.ok(json!({
        "action": "start", "command": "echo hi",
        "returnWhen": {"outputContains": ["nope"]}
    }));
    assert_eq!(
        out["waitEndedBy"],
        json!({"condition": "task_ended"}),
        "{text}"
    );
}

#[test]
fn screen_contains_and_quiet_see_only_committed_frames() {
    let mut server = Server::start();
    // The visible text is assembled by cursor moves; the earlier text is erased.
    let command = "printf 'GONE\\r\\n'; sleep 0.3; printf '\\033[2J\\033[H\\033[1;4Hdy\\033[1;1HRea'; read -r x";
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 20000,
        "returnWhen": {"screenContains": ["Ready"]}
    }));
    assert_eq!(
        out["waitEndedBy"],
        json!({"condition": "screen_contains", "text": "Ready"}),
        "{text}"
    );
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "waitMs": 500,
        "returnWhen": {"screenContains": ["GONE"]}
    }));
    assert_eq!(
        out["waitEndedBy"],
        json!({"condition": "max_wait"}),
        "{text}"
    );
    server.ok(json!({"action": "kill", "taskId": server.id(1)}));

    // A synchronized update the program leaves open shows at its timeout, while the program
    // writes nothing more; until then the output is not quiet.
    let command = "printf 'shown\\r\\n\\033[?2026hheld'; read -r x";
    let begun = Instant::now();
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 20000,
        "returnWhen": {"screenContains": ["held"]}
    }));
    assert!(begun.elapsed() < Duration::from_secs(10), "{text}");
    assert_eq!(
        out["waitEndedBy"],
        json!({"condition": "screen_contains", "text": "held"}),
        "{text}"
    );
    server.ok(json!({"action": "kill", "taskId": server.id(2)}));
    let command =
        "stty -echo; printf 'shown\\r\\n'; read -r x; printf '\\033[?2026hheld'; read -r x";
    server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 20000,
        "returnWhen": {"screenContains": ["shown"]}
    }));
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": server.id(3), "keys": ["Enter"], "screen": true, "waitMs": 20000,
        "returnWhen": {"quietMs": 100}
    }));
    assert_eq!(out["waitEndedBy"], json!({"condition": "quiet"}), "{text}");
    assert_eq!(text.split("\n\n[").next().unwrap(), "shown\nheld", "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(3)}));
}

#[test]
fn screen_appears_and_gone_compare_with_the_screen_before_the_input() {
    let mut server = Server::start();
    // Enter shows a busy line, then replaces it with a done line like the old one.
    let command = concat!(
        "stty -echo; printf 'Done 1\\r\\n'; read -r x; printf 'Busy'; sleep 0.5; ",
        "printf '\\r\\033[KDone 1\\r\\n'; read -r x"
    );
    server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 20000,
        "returnWhen": {"screenContains": ["Done 1"]}
    }));
    // The old done line shows throughout; only the second one is new.
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "keys": ["Enter"], "screen": true, "waitMs": 20000,
        "returnWhen": {"screenAppears": ["Done 1"]}
    }));
    assert_eq!(
        out["waitEndedBy"],
        json!({"condition": "screen_appears", "text": "Done 1"}),
        "{text}"
    );
    assert_eq!(
        text.split("\n\n[").next().unwrap(),
        "Done 1\nDone 1",
        "{text}"
    );
    server.ok(json!({"action": "kill", "taskId": server.id(1)}));

    // The busy line is absent when the poll begins; the wait ends only once it has shown, for
    // half a second, and gone.
    server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 20000,
        "returnWhen": {"screenContains": ["Done 1"]}
    }));
    let begun = Instant::now();
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": server.id(2), "keys": ["Enter"], "screen": true, "waitMs": 20000,
        "returnWhen": {"screenGone": ["Busy"]}
    }));
    assert_eq!(
        out["waitEndedBy"],
        json!({"condition": "screen_gone", "text": "Busy"}),
        "{text}"
    );
    assert!(begun.elapsed() >= Duration::from_millis(400), "{text}");
    assert!(
        !text.split("\n\n[").next().unwrap().contains("Busy"),
        "{text}"
    );
    server.ok(json!({"action": "kill", "taskId": server.id(2)}));
}

#[test]
fn screen_and_transcript_render_the_same_terminal() {
    let mut server = Server::start();
    // Each row needs the terminal to act on a sequence: REP repeats the last character, IRM
    // inserts, a combining mark joins its base, a wide character fills two columns, combining
    // marks all stack on their cell (DEL between them moves nothing), a tab
    // moves to its stop, and deleting the first half of a wide character leaves a blank.
    let command = concat!(
        "printf 'rep x\\033[4b\\r\\n'; ",
        "printf 'irm abc\\r\\033[5C\\033[4hZ\\033[4l\\r\\n'; ",
        "printf 'mix e\\314\\201 \\344\\270\\255\\346\\226\\207!\\r\\n'; ",
        "printf 'cap e'; for i in $(seq 40); do printf '\\314\\201\\177'; done; printf '\\r\\n'; ",
        "printf 'tab a\\tb\\r\\n'; ",
        "printf 'dch \\344\\270\\255a\\r\\033[4C\\033[P\\r\\n'; read -r x"
    );
    let expected = [
        "rep xxxxx".to_string(),
        "irm aZbc".to_string(),
        "mix e\u{301} 中文!".to_string(),
        format!("cap e{}", "\u{301}".repeat(40)),
        "tab a   b".to_string(),
        "dch  a".to_string(),
    ]
    .join("\n");
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "pty": true, "screen": true, "waitMs": 30000,
        "returnWhen": {"screenContains": ["dch  a"]}
    }));
    assert_eq!(out["state"], "running", "{text}");
    assert_eq!(
        text.split("\n\n[").next().unwrap(),
        expected,
        "screen: {text}"
    );
    assert_eq!(out["cursor"], json!([7, 1]), "{text}");
    let (_, text) =
        server.ok(json!({"action": "transcript", "taskId": server.id(1), "truncate": "none"}));
    assert_eq!(
        text.split("\n\n[").next().unwrap(),
        expected,
        "transcript: {text}"
    );
    server.ok(json!({"action": "kill", "taskId": server.id(1)}));
}

#[test]
fn pty_programs_get_answers_to_terminal_queries() {
    let mut server = Server::start();
    // A cursor-position report (DSR 6) and primary device attributes (DA1), read raw.
    let command = concat!(
        "stty raw -echo; printf 'ab\\033[6n'; IFS= read -r -d R cpr; ",
        "printf '\\033[c'; IFS= read -r -d c da; ",
        "printf '\\r\\ncpr=%s da=%s\\r\\n' \"${cpr#?}\" \"${da#?}\""
    );
    let (out, text) =
        server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 10000}));
    assert_eq!(out["state"], "exited", "{text}");
    assert!(text.contains("cpr=[1;3 da=[?"), "{text}");
}

#[test]
fn transcript_recovers_every_line_a_program_pushed_off_the_screen() {
    let mut server = Server::start();
    // Like pi, a full redraw clears the screen and its history (ED2, ED3), then reprints the
    // lines it showed and new ones. Like `clear`, a clear before new lines keeps what showed.
    // Like Codex, history lines are then inserted through a scroll region above a fixed
    // composer row. All push lines past the 30-row screen.
    let command = concat!(
        "seq -f 'kept %g' 1 40; printf '\\033[2J\\033[H\\033[3J'; seq -f 'kept %g' 1 45; ",
        "printf '\\033[2J\\033[H\\033[3J'; seq -f 'plain %g' 1 40; ",
        "printf '\\033[30;1H> composer'; ",
        "for k in $(seq 1 40); do ",
        "printf '\\033[1;29r\\033[29;1H\\r\\ninsert %d\\033[r\\033[30;1H> composer\\033[K' $k; done"
    );
    let (out, text) =
        server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 30000}));
    assert_eq!(out["state"], "exited", "{text}");
    let (out, text) =
        server.ok(json!({"action": "transcript", "taskId": server.id(1), "truncate": "none"}));
    let body = text.split("\n\n[").next().unwrap();
    let lines: Vec<&str> = body.lines().collect();
    let expected: Vec<String> = (1..=45)
        .map(|k| format!("kept {k}"))
        .chain((1..=40).map(|k| format!("plain {k}")))
        .chain((1..=40).map(|k| format!("insert {k}")))
        .chain(["> composer".to_string()])
        .collect();
    let positions: Vec<usize> = expected
        .iter()
        .map(|line| {
            let found: Vec<usize> = (0..lines.len()).filter(|&i| lines[i] == line).collect();
            assert_eq!(found.len(), 1, "{line:?} must appear once: {text}");
            found[0]
        })
        .collect();
    assert!(positions.is_sorted(), "lines out of order: {text}");
    let file = std::fs::read_to_string(out["transcriptPath"].as_str().unwrap()).unwrap();
    assert_eq!(file.trim_end(), body.trim_end());
    // Output ended, so every line is final, and a second read has none left.
    assert_eq!(out["finalLines"], out["transcriptLines"], "{text}");
    let (out, text) = server.ok(json!({"action": "transcript", "taskId": server.id(1)}));
    assert!(
        text.starts_with("(no transcript lines since the last read)"),
        "{text}"
    );
    assert_eq!(out["firstLine"], json!(lines.len() + 1), "{text}");

    // A program on the alternate screen: the normal screen's history, then the session merged
    // from its frames. A page drawn in place, a pause, a second page drawn over it, a pause,
    // then lines that scroll the screen: the alternate screen keeps none of them.
    let command = concat!(
        "seq -f 'normal %g' 1 40; printf 'a\\tb\\n\\033[?1049h'; rows=$(seq 1 30 | sed 'p'); ",
        "printf '\\033[%d;1Hfirst %d\\033[K' $rows; sleep 0.5; ",
        "printf '\\033[%d;1Hsecond %d\\033[K' $rows; sleep 0.5; ",
        "printf '\\r\\nscrolled %d' $(seq 1 40); read -r x"
    );
    server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 30000,
        "returnWhen": {"outputContains": ["scrolled 40"], "quietMs": 3000}
    }));
    let (out, text) =
        server.ok(json!({"action": "transcript", "taskId": server.id(2), "truncate": "none"}));
    let lines: Vec<&str> = text.lines().collect();
    let normal = lines.iter().position(|line| *line == "normal 1");
    let marker = lines
        .iter()
        .position(|line| *line == "--- alternate screen ---");
    assert!(normal.is_some_and(|normal| Some(normal) < marker), "{text}");
    let session: Vec<&str> = lines[marker.unwrap() + 1..]
        .iter()
        .copied()
        .take_while(|line| !line.is_empty())
        .collect();
    let expected: Vec<String> = (1..=30)
        .map(|k| format!("first {k}"))
        .chain((1..=30).map(|k| format!("second {k}")))
        .chain((1..=40).map(|k| format!("scrolled {k}")))
        .collect();
    assert_eq!(session, expected, "{text}");
    assert!(
        lines.contains(&"a       b"),
        "a tab reads as spaces to its stop: {text}"
    );
    // The screen's rows are pending; the next read starts after the final lines and shows
    // the pending rows again, with the line typed since.
    let first = out["finalLines"].as_u64().unwrap() + 1;
    assert!(out["transcriptLines"].as_u64().unwrap() >= first, "{text}");
    server.ok(json!({"action": "poll", "taskId": server.id(2), "input": "typed", "waitMs": 30000, "returnWhen": {"screenContains": ["40typed"]}}));
    let (out, text) =
        server.ok(json!({"action": "transcript", "taskId": server.id(2), "truncate": "none"}));
    assert_eq!(out["firstLine"], json!(first), "{text}");
    assert!(!text.contains("normal 1\n"), "{text}");
    assert!(text.contains("scrolled 40typed"), "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(2)}));

    server.ok(json!({"action": "start", "command": "echo pipe"}));
    let (_, text, is_error) = server.call(json!({"action": "transcript", "taskId": server.id(3)}));
    assert!(is_error && text.contains("PTY tasks only"), "{text}");
}

#[test]
fn kill_and_root_exit_end_the_whole_process_tree() {
    // Scratch guards are declared before the server so they outlive it, also when unwinding:
    // the server stops its writers before the files are removed.
    let files = [false, true].map(|pty| {
        (
            scratch(&format!("killed-{pty}")),
            scratch(&format!("leaked-{pty}")),
        )
    });
    let mut server = Server::start();
    for (pty, ((killed, killed_bash), (leaked, leaked_bash))) in
        [false, true].into_iter().zip(&files)
    {
        // Job control moves the heartbeat into its own process group.
        let command = format!("set -m; {} bash -c 'sleep 300'", heartbeat(killed_bash));
        let (out, _) =
            server.ok(json!({"action": "start", "command": command, "pty": pty, "waitMs": 1000}));
        let id = out["taskId"].clone();
        wait_for_heartbeat(killed);
        let (out, _) = server.ok(json!({"action": "kill", "taskId": id}));
        assert_eq!(out["state"], "killed", "pty={pty}");
        assert_stops_growing(killed);
        let (again, text) =
            server.ok(json!({"action": "kill", "taskId": id, "waitMs": 0, "maxBytes": 100}));
        assert_eq!(again["state"], "killed");
        assert!(text.len() <= 100, "{} bytes: {text}", text.len());

        let command = format!("{} echo started", heartbeat(leaked_bash));
        let (out, text) =
            server.ok(json!({"action": "start", "command": command, "pty": pty, "waitMs": 60000}));
        assert_eq!(out["state"], "exited", "pty={pty}");
        assert!(
            text.contains("ended with bash: ") && text.contains("keepTree: true"),
            "the result names what bash left (pty={pty}): {text}"
        );
        assert!(!out["leftovers"].as_array().unwrap().is_empty(), "{text}");
        assert_eq!(out["keptRunning"], false);
        assert_stops_growing(leaked);
    }
}

#[test]
fn keep_tree_keeps_what_bash_leaves_until_kill_or_deadline() {
    let files = [false, true].map(|pty| {
        (
            scratch(&format!("kept-{pty}")),
            scratch(&format!("expiring-{pty}")),
        )
    });
    let mut server = Server::start();
    for (pty, ((kept, kept_bash), (expiring, expiring_bash))) in
        [false, true].into_iter().zip(&files)
    {
        let command = format!("{} echo started", heartbeat(kept_bash));
        let (out, text) = server.ok(json!({"action": "start", "command": command, "pty": pty, "keepTree": true, "waitMs": 10000}));
        assert_eq!(out["state"], "exited", "pty={pty}: {text}");
        assert_eq!(out["keptRunning"], true, "pty={pty}: {text}");
        assert!(
            text.contains("started") && text.contains(" running; kill ends them"),
            "{text}"
        );
        let id = out["taskId"].clone();
        std::thread::sleep(Duration::from_millis(800));
        let before = file_len(kept);
        std::thread::sleep(Duration::from_millis(800));
        assert!(
            file_len(kept) > before,
            "a kept process stopped (pty={pty})"
        );
        let (list, _) = server.ok(json!({"action": "list"}));
        let entry = list["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|task| task["taskId"] == id)
            .cloned()
            .unwrap();
        assert_eq!(entry["keptRunning"], true);
        assert!(!entry["held"].as_array().unwrap().is_empty(), "{entry}");
        let (out, text) = server.ok(json!({"action": "kill", "taskId": id}));
        assert_eq!(
            out["state"], "exited",
            "kill keeps the root's result: {text}"
        );
        assert_eq!(out["keptRunning"], false, "{text}");
        assert_eq!(out["held"], json!([]), "{text}");
        assert!(text.contains("kept processes ended"), "{text}");
        assert_stops_growing(kept);

        let command = format!("{} echo started", heartbeat(expiring_bash));
        let (out, text) = server.ok(json!({"action": "start", "command": command, "pty": pty, "keepTree": true, "killAfterMs": 8000, "waitMs": 60000}));
        assert_eq!(out["keptRunning"], true, "pty={pty}: {text}");
        let id = out["taskId"].clone();
        let (out, text) = server.ok(json!({"action": "poll", "taskId": id, "waitMs": 60000}));
        assert_eq!(
            out["waitEndedBy"]["condition"], "tree_ended",
            "killAfterMs ends the kept processes: {text}"
        );
        assert_eq!(out["exitCode"], 0, "and keeps the root's result: {text}");
        assert_stops_growing(expiring);
    }
    let (out, text) = server.ok(json!({"action": "start", "command": "true", "keepTree": true}));
    assert_eq!(
        out["keptRunning"], false,
        "nothing left, nothing kept: {text}"
    );
    assert_eq!(out["leftovers"], json!([]));
}

/// The total log limit evicts finished logs only: a task that keeps processes keeps its log and
/// transcript until they end. The debug build takes a 1 MiB limit for the test.
#[cfg(debug_assertions)]
#[test]
fn log_eviction_spares_tasks_that_keep_processes() {
    let mut server = Server::start_with(|command| {
        command.env("FASTEXEC_TEST_TOTAL_LOG_LIMIT", "1048576");
    });
    let marker = "KEPT-MARKER";
    for pty in [false, true] {
        let command = format!("sleep 300 & echo {marker}");
        let (out, text) = server.ok(json!({"action": "start", "command": command, "pty": pty, "keepTree": true, "loginShell": false, "waitMs": 10000}));
        assert_eq!(out["keptRunning"], true, "pty={pty}: {text}");
    }
    let (pipe, pty) = (server.id(1), server.id(2));
    let write = |bytes: u32| format!("head -c {bytes} /dev/zero | tr '\\0' x");
    let status = |server: &mut Server, id: &str| {
        server
            .ok(json!({"action": "poll", "taskId": id, "waitMs": 0}))
            .1
    };
    // Files are checked on disk: a transcript call would reserve log space itself.
    let files = |server: &mut Server, id: &str| {
        let (out, text) = server.ok(json!({"action": "poll", "taskId": id, "waitMs": 0}));
        let log = PathBuf::from(out["logPath"].as_str().unwrap());
        let transcript = log.with_file_name(format!("{id}.transcript.txt"));
        (text, log, transcript)
    };
    let assert_kept = |server: &mut Server| {
        for id in [&pipe, &pty] {
            let (text, log, _) = files(server, id);
            assert!(!text.contains("log evicted"), "{text}");
            let log = std::fs::read_to_string(log).unwrap();
            assert!(log.contains(marker), "{id}: {log}");
        }
        assert!(files(server, &pty).2.exists(), "the transcript is gone");
    };

    // Nothing can be evicted: the running writer's excess is dropped, and the kept logs stay.
    let (_, text) = server.ok(json!({"action": "start", "command": write(1_572_864), "loginShell": false, "waitMs": 60000}));
    assert!(
        text.contains("bytes past the log limit were not stored"),
        "{text}"
    );
    let full = server.id(3);
    assert_kept(&mut server);

    // A finished log goes in their place.
    let (_, text) = server.ok(
        json!({"action": "start", "command": write(614_400), "loginShell": false, "waitMs": 60000}),
    );
    assert!(!text.contains("not stored"), "{text}");
    let text = status(&mut server, &full);
    assert!(text.contains("log evicted"), "{text}");
    assert_kept(&mut server);

    // Once their processes end, they are finished too.
    for id in [&pipe, &pty] {
        server.ok(json!({"action": "kill", "taskId": id}));
    }
    server.ok(
        json!({"action": "start", "command": write(614_400), "loginShell": false, "waitMs": 60000}),
    );
    for id in [&pipe, &pty] {
        let (text, log, _) = files(&mut server, id);
        assert!(text.contains("log evicted") && !log.exists(), "{text}");
    }
    assert!(
        !files(&mut server, &pty).2.exists(),
        "the transcript remains"
    );
}

/// A task that keeps processes finishes when they end, so it is not the first to be retired
/// when they outlived later tasks.
#[test]
fn retirement_counts_kept_processes_ending_as_finishing() {
    let mut server = Server::start();
    let (out, text) = server.ok(json!({"action": "start", "command": "sleep 300 & echo kept", "keepTree": true, "waitMs": 10000}));
    assert_eq!(out["keptRunning"], true, "{text}");
    let kept = server.id(1);
    for _ in 0..64 {
        server.ok(json!({"action": "start", "command": "true"}));
    }
    server.ok(json!({"action": "kill", "taskId": kept}));
    server.ok(json!({"action": "start", "command": "true"}));
    // Retirement follows the result that shows the completion.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (out, _) = server.ok(json!({"action": "list"}));
        let tasks = out["tasks"].as_array().unwrap();
        if tasks.len() == 64 {
            let listed = |id: &str| tasks.iter().any(|task| task["taskId"] == id);
            assert!(
                listed(&kept) && !listed(&server.id(2)) && !listed(&server.id(3)),
                "the earliest finishers go first"
            );
            break;
        }
        assert!(Instant::now() < deadline, "no task was retired");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A long command runs from a script file, which stays while kept processes may run it as `$0`.
#[test]
fn kept_processes_can_run_the_command_script() {
    let [(gate, gate_bash), (child, child_bash)] = ["script-gate", "script-child"].map(scratch);
    let command = format!(
        "#{}\nif [ \"$1\" = child ]; then echo ran > '{child_bash}'; else (until [ -e '{gate_bash}' ]; do sleep 0.1; done; \"$BASH\" \"$0\" child; sleep 300) & echo parent; fi",
        "x".repeat(8_100)
    );
    let mut server = Server::start();
    let (out, text) = server.ok(json!({"action": "start", "command": command, "keepTree": true, "loginShell": false, "waitMs": 10000}));
    assert_eq!(out["keptRunning"], true, "{text}");
    // The result shows the root's end, so the child runs `$0` only after it.
    std::fs::write(&*gate, "").unwrap();
    wait_for_file(&child);
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::fs::read_to_string(&*child).unwrap() != "ran\n" {
        assert!(Instant::now() < deadline, "the script ran incompletely");
        std::thread::sleep(Duration::from_millis(50));
    }
    server.ok(json!({"action": "kill", "taskId": server.id(1)}));
}

#[test]
fn waits_on_kept_processes_last_until_they_end_or_a_condition_holds() {
    let markers = ["late-go", "short-done"].map(scratch);
    let [(go, go_bash), (done, done_bash)] = &markers;
    let mut server = Server::start();
    // Without returnWhen, the root's exit is the result, however long the kept processes run:
    // these wait for a file the test writes later.
    let command =
        format!("(until [ -e {go_bash} ]; do sleep 0.1; done; echo late; sleep 2) & echo early");
    let (out, text) = server
        .ok(json!({"action": "start", "command": command, "keepTree": true, "waitMs": 60000}));
    assert_eq!(out["waitEndedBy"]["condition"], "task_ended", "{text}");
    assert_eq!(out["keptRunning"], true, "{text}");
    let id = server.id(1);
    // Once a result showed the exit, waits outlast it: conditions on the kept processes' output
    // hold, and without them the wait ends with the tree.
    std::fs::write(&**go, "").unwrap();
    let (out, text) = server.ok(json!({"action": "poll", "taskId": id, "waitMs": 60000, "returnWhen": {"outputContains": ["late"]}}));
    assert_eq!(out["waitEndedBy"]["condition"], "output_contains", "{text}");
    assert!(text.starts_with("late"), "{text}");
    let (out, text) = server.ok(json!({"action": "poll", "taskId": id, "waitMs": 20000}));
    assert_eq!(out["waitEndedBy"]["condition"], "tree_ended", "{text}");
    assert_eq!(out["keptRunning"], false, "{text}");
    assert!(text.contains("kept processes ended"), "{text}");
    let (out, _) = server.ok(json!({"action": "poll", "taskId": id, "waitMs": 20000}));
    assert_eq!(
        out["waitEndedBy"]["condition"], "tree_ended",
        "an ended tree ends waits at once"
    );

    // returnWhen waits past a root exit 0 for what the kept processes print.
    let command = "(sleep 2; echo ready; sleep 300) & echo started";
    let (out, text) = server.ok(json!({"action": "start", "command": command, "keepTree": true, "waitMs": 20000, "returnWhen": {"outputContains": ["ready"]}}));
    assert_eq!(out["waitEndedBy"]["condition"], "output_contains", "{text}");
    assert!(text.contains("started\nready"), "{text}");
    let ready = server.id(2);
    // A root that fails ends the wait at once.
    let command = "sleep 300 & exit 3";
    let (out, text) = server.ok(json!({"action": "start", "command": command, "keepTree": true, "waitMs": 20000, "returnWhen": {"outputContains": ["never"]}}));
    assert_eq!(out["waitEndedBy"]["condition"], "task_ended", "{text}");
    assert_eq!(out["exitCode"], 3, "{text}");
    let failed = server.id(3);

    // The footer names the tasks that keep processes, and once the end of a tree it did not show.
    let (_, text) = server.ok(json!({"action": "start", "command": "true"}));
    assert!(text.contains(&format!("{ready} keeps ")), "{text}");
    server.ok(json!({"action": "kill", "taskId": failed}));
    let command = format!("(sleep 1; touch {done_bash}) & echo short");
    server.ok(json!({"action": "start", "command": command, "keepTree": true, "waitMs": 60000}));
    let short = server.id(5);
    wait_for_file(done);
    std::thread::sleep(Duration::from_millis(2000));
    let (_, text) = server.ok(json!({"action": "start", "command": "true"}));
    assert!(
        text.contains(&format!("{short} kept processes ended")),
        "{text}"
    );
    let (_, text) = server.ok(json!({"action": "start", "command": "true"}));
    assert!(
        !text.contains(&format!("{short} kept")),
        "shown once: {text}"
    );
    server.ok(json!({"action": "kill", "taskId": ready}));
}

/// A heartbeat in a new session through a double fork, as daemonizing servers (adb) start;
/// `program` runs bash's loop under another name.
#[cfg(unix)]
fn daemon(program: &str, bash_path: &str) -> String {
    format!(
        "setsid -f {program} -c 'while :; do echo x >> {bash_path}; sleep 0.1; done'; sleep 0.5;"
    )
}

#[cfg(unix)]
#[test]
fn processes_that_start_their_own_session_stay_in_the_tree() {
    let files = ["daemon", "group-term", "odd-name", "odd-name-bin"].map(scratch);
    let mut server = Server::start();
    let [
        (plain, plain_bash),
        (term, term_bash),
        (odd, odd_bash),
        (_, bin_bash),
    ] = &files;
    let command = format!("{} echo started", daemon("bash", plain_bash));
    let (out, text) = server.ok(json!({"action": "start", "command": command, "waitMs": 10000}));
    assert_eq!(out["state"], "exited", "{text}");
    assert!(text.contains("ended with bash: bash ("), "{text}");
    assert!(file_len(plain) > 0, "the daemon never started");
    assert_stops_growing(plain);

    // A command's `kill 0` reaches the task host too, which must outlive it.
    let command = format!("{} kill -TERM 0", daemon("bash", term_bash));
    let (out, text) = server.ok(json!({"action": "start", "command": command, "waitMs": 10000}));
    assert_eq!(out["exitCode"], 143, "{text}");
    assert!(text.contains("ended with bash: bash ("), "{text}");
    assert_stops_growing(term);

    // A process name that is not UTF-8 still counts the process in.
    let command = format!(
        "mkdir -p {bin_bash}; cp \"$(command -v bash)\" {bin_bash}/$'\\xff'beat; {}",
        daemon(&format!("{bin_bash}/$'\\xff'beat"), odd_bash)
    );
    let (out, text) = server.ok(json!({"action": "start", "command": command, "waitMs": 10000}));
    assert!(text.contains("ended with bash: \u{fffd}beat ("), "{out}");
    assert!(file_len(odd) > 0, "the daemon never started");
    assert_stops_growing(odd);
}

#[cfg(unix)]
#[test]
fn a_killed_server_takes_its_task_trees_with_it() {
    let files = ["orphaned", "orphaned-daemon"].map(scratch);
    let [(beat, beat_bash), (daemon_beat, daemon_bash)] = &files;
    let mut server = Server::start();
    let command = format!("{} sleep 300", heartbeat(beat_bash));
    server.ok(json!({"action": "start", "command": command, "keepTree": true, "waitMs": 1000}));
    let command = format!("{} echo started", daemon("bash", daemon_bash));
    let (out, text) = server.ok(json!({"action": "start", "command": command, "keepTree": true, "pty": true, "waitMs": 10000}));
    assert_eq!(out["keptRunning"], true, "{text}");
    // SIGKILL: no shutdown code runs, so only the hosts' lifelines can end the trees.
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    for beat in [beat, daemon_beat] {
        assert!(file_len(beat) > 0, "a heartbeat never started");
        assert_stops_growing(beat);
    }
}

/// A process the server may not signal, as one sudo runs as root, stays held and reported. The
/// fixture needs root and a C compiler, as in the Linux test container; elsewhere it is skipped.
#[cfg(target_os = "linux")]
#[test]
fn processes_the_server_may_not_kill_stay_held_and_reported() {
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::process::CommandExt;
    // SAFETY: plain syscall.
    if unsafe { libc::geteuid() } != 0 {
        eprintln!("skipped: a setuid-root fixture needs root");
        return;
    }
    let (dir, _) = scratch("root-daemon");
    std::fs::create_dir_all(&*dir).unwrap();
    let source = dir.join("daemon.c");
    std::fs::write(
        &source,
        // It ends by itself after 60 s, so a failing run leaves no immortal root process.
        "#define _GNU_SOURCE\n#include <stdio.h>\n#include <unistd.h>\nint main(int argc, char **argv) {\n  if (argc < 2 || setresuid(0, 0, 0) != 0) return 1;\n  for (int i = 0; i < 600; i++) { FILE *f = fopen(argv[1], \"a\"); if (f) { fputs(\"x\\n\", f); fclose(f); } usleep(100000); }\n  return 0;\n}\n",
    )
    .unwrap();
    let fixture = dir.join("root-daemon");
    match Command::new("cc")
        .arg(&source)
        .arg("-o")
        .arg(&fixture)
        .status()
    {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("skipped: no C compiler");
            return;
        }
        built => assert!(built.unwrap().success(), "the fixture did not compile"),
    }
    std::fs::set_permissions(&*dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::set_permissions(&fixture, std::fs::Permissions::from_mode(0o4755)).unwrap();
    let (fixture, beats) = (fixture.display(), [dir.join("exit"), dir.join("kill")]);
    let mut server = Server::start_with(|command| {
        command.uid(65534).gid(65534);
    });
    let mut pids = Vec::new();

    // At the root's exit.
    let command = format!("{fixture} {} & sleep 0.5; echo started", beats[0].display());
    let (out, text) = server.ok(json!({"action": "start", "command": command, "waitMs": 10000}));
    assert_eq!(out["keptRunning"], true, "{text}");
    assert!(
        text.contains("could not end root-daemon (") && text.contains("kill retries"),
        "{text}"
    );
    assert!(out["killError"].is_string(), "{out}");
    pids.extend(
        out["leftovers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|member| member["pid"].as_i64().unwrap()),
    );
    let (_, text, is_error) = server.call(json!({"action": "kill", "taskId": server.id(1)}));
    assert!(is_error && text.contains("Cannot terminate"), "{text}");

    // After a kill of the running root.
    let command = format!("{fixture} {} & sleep 300", beats[1].display());
    server.ok(json!({"action": "start", "command": command, "waitMs": 1000}));
    let (_, text, is_error) = server.call(json!({"action": "kill", "taskId": server.id(2)}));
    assert!(is_error && text.contains("Cannot terminate"), "{text}");
    let (out, text) = server.ok(json!({"action": "poll", "taskId": server.id(2), "waitMs": 10000}));
    assert_eq!(out["state"], "killed", "{text}");
    assert_eq!(out["keptRunning"], true, "{text}");
    assert!(text.contains("could not end root-daemon ("), "{text}");
    pids.extend(
        out["leftovers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|member| member["pid"].as_i64().unwrap()),
    );

    for beat in &beats {
        let before = file_len(beat);
        std::thread::sleep(Duration::from_millis(500));
        assert!(file_len(beat) > before, "the fixture should still run");
    }
    for pid in pids {
        // SAFETY: plain syscall, as root, on the fixtures' PIDs.
        unsafe { libc::kill(pid as i32, libc::SIGKILL) };
    }
    for beat in &beats {
        assert_stops_growing(beat);
    }
}

#[test]
fn trees_a_task_keeps_count_toward_the_task_limit() {
    let mut server = Server::start();
    for _ in 0..16 {
        let (out, text) = server.ok(json!({"action": "start", "command": "sleep 300 & echo started", "keepTree": true, "waitMs": 10000}));
        assert_eq!(out["keptRunning"], true, "{text}");
    }
    let (_, text, is_error) = server.call(json!({"action": "start", "command": "true"}));
    assert!(is_error && text.contains("TOO_MANY_TASKS"), "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(1)}));
    server.ok(json!({"action": "start", "command": "true"}));
}

#[test]
fn closing_the_server_ends_running_tasks_and_removes_logs() {
    let (beat, beat_bash) = scratch("shutdown");
    let mut server = Server::start();
    let (out, _) = server.ok(json!({"action": "start", "command": format!("{} sleep 300", heartbeat(&beat_bash)), "waitMs": 1000}));
    let log = PathBuf::from(out["logPath"].as_str().unwrap());
    assert!(log.exists());
    // An in-flight wait must not delay shutdown: hosts escalate to SIGKILL within ~2.5 s.
    let poll = json!({"name": "fastexec", "arguments": {"action": "poll", "taskId": server.id(1), "waitMs": 60000}});
    server.send(json!({"jsonrpc": "2.0", "id": 99, "method": "tools/call", "params": poll}));
    std::thread::sleep(Duration::from_millis(300));
    drop(server.stdin.take());
    let deadline = Instant::now() + Duration::from_secs(2);
    while server.child.try_wait().unwrap().is_none() {
        assert!(
            Instant::now() < deadline,
            "the server did not exit within 2 s of stdin closing"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_stops_growing(&beat);
    assert!(!log.parent().unwrap().exists(), "the log directory remains");
}

#[test]
fn finishing_after_many_short_tasks_keeps_its_own_output() {
    let mut server = Server::start();
    server.ok(json!({"action": "start", "command": "read -r x; echo final-$x", "waitMs": 0}));
    for _ in 0..65 {
        server.ok(json!({"action": "start", "command": "true"}));
    }
    let (out, text) = server
        .ok(json!({"action": "poll", "taskId": server.id(1), "input": "ok\n", "waitMs": 10000}));
    assert_eq!(out["state"], "exited");
    assert!(text.starts_with("final-ok\n"), "{text}");
    let (out, _) = server.ok(json!({"action": "list"}));
    let tasks = out["tasks"].as_array().unwrap();
    assert_eq!(tasks.len(), 64);
    let kept = |id: &str| tasks.iter().any(|task| task["taskId"] == id);
    assert!(
        kept(&server.id(1)) && !kept(&server.id(2)) && !kept(&server.id(3)),
        "the earliest finishers go first"
    );
}

#[test]
fn output_window_respects_max_bytes_and_points_at_the_log() {
    // A long log path makes the status line long; maxBytes still bounds the whole text.
    let root = Scratch::new(&"長".repeat(60));
    // Linux allows far longer paths than Windows' 260 characters: make the status alone
    // longer than maxBytes there.
    let temp = if cfg!(windows) {
        root.to_path_buf()
    } else {
        root.join("長".repeat(80)).join("長".repeat(80))
    };
    std::fs::create_dir_all(&temp).unwrap();
    let mut long = Server::start_with_temp(&temp);
    let (_, text) = long.ok(
        json!({"action": "start", "command": "printf '\\xff\\n'; seq 1 5000", "maxBytes": 1024}),
    );
    assert!(text.len() <= 1024, "{} bytes: {text}", text.len());
    drop(long);

    let mut server = Server::start();
    let (out, text) =
        server.ok(json!({"action": "start", "command": "seq 1 5000", "maxBytes": 2048}));
    assert!(text.len() <= 2048, "{} bytes", text.len());
    assert!(
        text.starts_with("1\n2\n") && text.contains("4999\n5000\n\n[exited 0]"),
        "{text}"
    );
    let [first, last] =
        [&out["omittedRange"][0], &out["omittedRange"][1]].map(|v| v.as_u64().unwrap());
    assert!(text.contains(&format!("log lines {first}-{last}")));
    let log = std::fs::read_to_string(out["logPath"].as_str().unwrap()).unwrap();
    assert_eq!(
        log.lines().nth(first as usize - 1),
        Some(first.to_string().as_str())
    );
    let (out, text) =
        server.ok(json!({"action": "start", "command": "seq 1 5000", "truncate": "none"}));
    assert_eq!(out["omittedLines"], 0);
    assert_eq!(
        text.lines()
            .filter(|line| line.parse::<u32>().is_ok())
            .count(),
        5000
    );

    // Bounded windows cut long lines and count them; truncate none returns whole lines.
    let line = |n: u32| format!("printf '%*s\\n' {n} '' | tr ' ' x");
    let (out, text) = server.ok(json!({"action": "start", "command": line(3000)}));
    assert_eq!(out["cutLines"], 1);
    assert!(text.contains("line cut at 2000 of 3000 chars"), "{text}");
    let (out, text) =
        server.ok(json!({"action": "start", "command": line(3000), "truncate": "none"}));
    assert_eq!(
        (text.split_once("\n\n").unwrap().0.len(), &out["cutLines"]),
        (3000, &json!(0))
    );
    let (out, text) = server
        .ok(json!({"action": "start", "command": line(70000), "truncate": "none", "raw": true}));
    assert_eq!(
        (text.split_once("\n\n").unwrap().0.len(), &out["cutLines"]),
        (70000, &json!(0))
    );

    // Small budgets leave out a placeholder that does not fit beside the status and footer.
    let (pty, _) = server.ok(json!({"action": "start", "command": "true", "pty": true}));
    let id = &pty["taskId"];
    server.ok(json!({"action": "transcript", "taskId": id}));
    server.ok(json!({"action": "start", "command": "sleep 30", "waitMs": 0}));
    for max in [100, 128, 144, 160] {
        for call in [
            json!({"action": "transcript", "taskId": id, "maxBytes": max}),
            json!({"action": "poll", "taskId": id, "waitMs": 0, "maxBytes": max}),
            json!({"action": "poll", "taskId": id, "waitMs": 0, "screen": true, "maxBytes": max}),
        ] {
            let (_, text) = server.ok(call);
            assert!(text.len() <= max, "{} bytes: {text}", text.len());
        }
    }
}

#[test]
fn the_log_keeps_a_contiguous_prefix_past_its_limit() {
    // 10 bytes below the 64 MiB limit, then a chunk that crosses it, then one that would fit.
    let command = "head -c 67108854 /dev/zero | tr '\\0' x; sleep 0.3; printf '%100s' '' | tr ' ' A; sleep 0.3; printf B";
    let mut server = Server::start();
    let (out, text) = server.ok(json!({"action": "start", "command": command, "loginShell": false, "waitMs": 60000, "maxBytes": 1024}));
    assert_eq!(out["state"], "exited", "{text}");
    let mut log = std::fs::File::open(out["logPath"].as_str().unwrap()).unwrap();
    let mut tail = Vec::new();
    log.seek(SeekFrom::End(-11)).unwrap();
    log.read_to_end(&mut tail).unwrap();
    assert_eq!(log.stream_position().unwrap(), 64 << 20);
    assert_eq!(tail, b"xAAAAAAAAAA");
    assert!(
        text.contains("91 bytes past the log limit were not stored"),
        "{text}"
    );
}

#[test]
fn an_exited_result_holds_output_written_while_its_window_was_read() {
    // Reading a 16 MB backlog takes long enough for the task to print a marker and exit
    // meanwhile; the delays move that exit across the read.
    let mut server = Server::start();
    for delay in ["0", "0.005", "0.01", "0.02", "0.04"] {
        let command = format!(
            "head -c 16000000 /dev/zero | tr '\\0' x; read -r go; sleep {delay}; printf '\\nFINAL_MARKER\\n'"
        );
        let (out, _) = server.ok(json!({"action": "start", "command": command, "loginShell": false, "waitMs": 0, "maxBytes": 1024}));
        let id = out["taskId"].clone();
        let log = PathBuf::from(out["logPath"].as_str().unwrap());
        let deadline = Instant::now() + Duration::from_secs(30);
        while file_len(&log) < 16_000_000 {
            assert!(
                Instant::now() < deadline,
                "the backlog did not reach the log"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let mut seen = String::new();
        let mut poll =
            json!({"action": "poll", "taskId": id, "input": "go\n", "waitMs": 0, "maxBytes": 1024});
        loop {
            let (out, text) = server.ok(poll);
            seen.push_str(&text);
            if out["state"] != "running" {
                break;
            }
            poll = json!({"action": "poll", "taskId": id, "waitMs": 10000, "maxBytes": 1024});
        }
        assert_eq!(
            seen.matches("FINAL_MARKER").count(),
            1,
            "delay {delay}: {seen}"
        );
    }
}

#[test]
fn pty_and_pipe_tasks_inherit_the_server_path() {
    let dir = Scratch::new("path");
    std::fs::create_dir_all(&*dir).unwrap();
    let probe = dir.join("fastexec_env_probe");
    std::fs::write(&probe, "#!/usr/bin/env bash\nprintf 'ENV_PATH_OK\\n'\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let inherited = std::env::var_os("PATH").unwrap_or_default();
    let path = std::env::join_paths(
        std::iter::once(dir.to_path_buf()).chain(std::env::split_paths(&inherited)),
    )
    .unwrap();
    // Git Bash's login profile builds PATH from ORIGINAL_PATH when a Git Bash parent set it.
    let mut server = Server::start_with(|command| {
        command
            .env("PATH", &path)
            .env("MSYS2_PATH_TYPE", "inherit")
            .env_remove("ORIGINAL_PATH");
    });
    // Debian's /etc/profile resets PATH for login shells; Git Bash's keeps it with inherit.
    let login = cfg!(windows);
    for pty in [false, true] {
        let (out, text) = server.ok(json!({"action": "start", "command": "fastexec_env_probe", "pty": pty, "loginShell": login, "waitMs": 15000}));
        assert_eq!(out["exitCode"], 0, "pty={pty}: {text}");
        assert!(text.contains("ENV_PATH_OK"), "pty={pty}: {text}");
    }
}

#[test]
fn encoding_decodes_legacy_output_and_long_commands_run_from_a_script() {
    let mut server = Server::start();
    let big5 = "printf '\\xa4\\xa4\\xa4\\xe5\\n'";
    let (_, text) = server.ok(json!({"action": "start", "command": big5}));
    assert!(text.contains("invalid in UTF-8; pass encoding"), "{text}");
    let (_, text) = server.ok(json!({"action": "start", "command": big5, "encoding": "big5"}));
    assert!(text.starts_with("中文\n"), "{text}");
    // A character split across polls decodes whole once its last byte arrives, also after
    // more than 4 KiB without a newline.
    for (encoding, prefix, lead, trail) in [
        ("big5", "", "\\xa4", "\\xa4"),
        ("gbk", "", "\\xd6", "\\xd0"),
        ("big5", "printf '%5000s' '' | tr ' ' a; ", "\\xa4", "\\xa4"),
    ] {
        // The marker shows when the lead byte is out, so the poll after it reads up to it.
        let (marker, marker_bash) = scratch(&format!("lead-{encoding}-{}", prefix.len()));
        let command =
            format!("{prefix}printf '{lead}'; touch {marker_bash}; read -r x; printf '{trail}\\n'");
        let (out, _) = server.ok(json!({"action": "start", "command": command, "encoding": encoding, "loginShell": false, "waitMs": 0}));
        wait_for_file(&marker);
        std::thread::sleep(Duration::from_millis(300));
        let (_, text) = server.ok(json!({"action": "poll", "taskId": out["taskId"], "waitMs": 0}));
        assert!(!text.contains('\u{fffd}'), "{encoding}: {text}");
        let (_, text) = server.ok(
            json!({"action": "poll", "taskId": out["taskId"], "input": "go\n", "waitMs": 10000}),
        );
        assert!(text.starts_with("中\n"), "{encoding}: {text}");
    }
    // Past 8,190 characters an argument would be cut on Windows; the script carries it whole.
    for length in [9_000, 100_000] {
        let long = format!("x='{}'; echo ${{#x}}", "a".repeat(length));
        let (_, text) = server.ok(json!({"action": "start", "command": long}));
        assert!(text.starts_with(&format!("{length}\n")), "{text}");
    }
}

#[test]
fn invalid_parameters_are_rejected_and_null_means_omitted() {
    let mut server = Server::start();
    for (arguments, expected) in [
        (
            json!({"action": "start", "command": "true", "taskId": "t1"}),
            "`taskId` does not apply",
        ),
        (json!({"action": "poll"}), "needs `taskId`"),
        (
            json!({"action": "poll", "taskId": "t1", "killAfterMs": 5}),
            "`killAfterMs` does not apply",
        ),
        (
            json!({"action": "start", "command": "true", "waitMs": 240001}),
            "out of range",
        ),
        (
            json!({"action": "start", "command": "true", "maxBytes": 99}),
            "out of range",
        ),
        (
            json!({"action": "start", "command": "true", "truncate": "none", "maxBytes": 4096}),
            "does not apply with truncate",
        ),
        (
            json!({"action": "start", "command": "true", "encoding": "nope"}),
            "Unknown or unsupported encoding",
        ),
        (json!({"action": "poll", "taskId": "t9"}), "Unknown taskId"),
        (
            json!({"action": "start", "command": "true", "encoding": "utf-16le"}),
            "unsupported encoding",
        ),
        (
            json!({"action": "start", "command": "true", "keys": ["Enter"]}),
            "`keys` does not apply",
        ),
        (
            json!({"action": "start", "command": "true", "screen": true}),
            "screen needs a PTY",
        ),
        (
            json!({"action": "start", "command": "true", "pty": true, "screen": true, "raw": true}),
            "does not apply with screen",
        ),
        (json!({"action": "transcript"}), "needs `taskId`"),
        (
            json!({"action": "transcript", "taskId": "t1", "waitMs": 0}),
            "`waitMs` does not apply",
        ),
        (
            json!({"action": "start", "command": "true", "returnWhen": {}}),
            "returnWhen needs",
        ),
        (
            json!({"action": "start", "command": "true", "returnWhen": {"outputContains": ["a\nb"]}}),
            "without line breaks",
        ),
        (
            json!({"action": "start", "command": "true", "returnWhen": {"outputContains": []}}),
            "takes 1-16 texts",
        ),
        (
            json!({"action": "start", "command": "true", "returnWhen": {"screenGone": ["x"]}}),
            "screenGone needs a PTY",
        ),
        (
            json!({"action": "poll", "taskId": "t1", "keyDelayMs": 100}),
            "keyDelayMs applies only with keys",
        ),
        (
            json!({"action": "poll", "taskId": "t1", "input": "x", "keys": vec!["a"; 16], "keyDelayMs": 2000}),
            "over the 30000 ms limit",
        ),
    ] {
        let (_, text, is_error) = server.call(arguments.clone());
        assert!(is_error && text.contains(expected), "{arguments} -> {text}");
    }
    let message = server.request(
        "tools/call",
        json!({"name": "fastexec", "arguments": {"action": "list", "bogus": 1}}),
    );
    assert!(
        message.get("error").is_some() || message["result"]["isError"] == json!(true),
        "{message}"
    );
    let (out, _) = server.ok(
        json!({"action": "start", "command": "echo ok", "cwd": null, "pty": null, "taskId": null}),
    );
    assert_eq!(out["state"], "exited");
}

#[test]
fn long_wait_sends_progress_and_cancellation_leaves_the_task_running() {
    let mut server = Server::start();
    server.ok(json!({"action": "start", "command": "sleep 120", "waitMs": 0}));
    server.next_id += 1;
    let id = server.next_id;
    let call = json!({"name": "fastexec", "arguments": {"action": "poll", "taskId": server.id(1), "waitMs": 60000}, "_meta": {"progressToken": "p1"}});
    server.send(json!({"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": call}));
    let progress = server
        .lines
        .recv_timeout(Duration::from_secs(25))
        .expect("progress within 25 s");
    assert_eq!(progress["method"], "notifications/progress");
    assert_eq!(progress["params"]["progressToken"], "p1");
    server.send(
        json!({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": id}}),
    );
    std::thread::sleep(Duration::from_millis(300));
    let (out, text) = server.ok(json!({"action": "list"}));
    assert_eq!(out["tasks"][0]["state"], "running", "{text}");
    let (out, _) = server.ok(json!({"action": "kill", "taskId": server.id(1)}));
    assert_eq!(out["state"], "killed");
}
