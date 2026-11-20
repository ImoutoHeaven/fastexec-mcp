//! Contract tests through the real MCP stdio boundary: each test drives the built binary.

use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, Instant};

mod support;
#[path = "support/transfer_http.rs"]
mod transfer_http;

struct Server {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<Value>,
    next_id: u64,
    pending_messages: Vec<Value>,
    /// The IDs of the tasks `start` calls began, in order.
    started: Vec<String>,
}

fn body_of(text: &str) -> &str {
    text.split_once("\n\n[").map_or(text, |(body, _)| body)
}

fn bare_id(handle: &str) -> &str {
    handle.split_once('.').expect("a reader handle").0
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
        Server::start_bin(
            std::path::Path::new(env!("CARGO_BIN_EXE_fastexec")),
            configure,
        )
    }

    /// Starts the server from `executable`, such as a world-reachable copy for dropped-uid tests.
    fn start_bin(executable: &std::path::Path, configure: impl FnOnce(&mut Command)) -> Server {
        let mut command = Command::new(executable);
        support::scrub_fastexec_env(&mut command);
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
            // Why stdout ended is the last message sent: a client waiting for an answer that
            // will never come can then say what happened to the server's output.
            let mut reader = BufReader::new(stdout);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => {
                        let _ = tx.send(json!({"__stdout_ended": "end of file"}));
                        return;
                    }
                    Ok(_) => match serde_json::from_str(&line) {
                        Ok(value) => {
                            let _ = tx.send(value);
                        }
                        Err(error) => {
                            let _ = tx.send(json!({"__stdout_ended": format!("invalid JSON: {error}: {line:?}")}));
                            return;
                        }
                    },
                    Err(error) => {
                        let _ = tx.send(json!({"__stdout_ended": format!("{error}")}));
                        return;
                    }
                }
            }
        });
        let stdin = child.stdin.take();
        let mut server = Server {
            child,
            stdin,
            lines,
            next_id: 0,
            pending_messages: Vec::new(),
            started: Vec::new(),
        };
        server.request(
            "initialize",
            json!({"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "test", "version": "0"}}),
        );
        server.send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        server
    }

    /// Like [`Server::start_with`], returning `Err(stderr)` if the process exits before answering
    /// `initialize` (for example, when a probed `FASTEXEC_TRANSFER_ADDR` raced with another test).
    fn try_start_with(configure: impl FnOnce(&mut Command)) -> Result<Server, String> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        support::scrub_fastexec_env(&mut command);
        configure(&mut command);
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn fastexec");
        let mut stdin = child.stdin.take().unwrap();
        let _ = writeln!(
            stdin,
            "{}",
            json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "test", "version": "0"}}})
        );
        let _ = stdin.flush();
        let mut reader = BufReader::new(child.stdout.take().unwrap());
        let mut first_line = String::new();
        match reader.read_line(&mut first_line) {
            Ok(0) => {
                drop(stdin);
                let output = child.wait_with_output().expect("wait failed fastexec");
                return Err(String::from_utf8_lossy(&output.stderr).into_owned());
            }
            Ok(_) => {
                let init: Value = serde_json::from_str(&first_line).unwrap_or_else(|error| {
                    panic!("invalid initialize JSON {error}: {first_line:?}")
                });
                assert!(init.get("error").is_none(), "{init}");
            }
            Err(error) => {
                drop(stdin);
                let _ = child.kill();
                let _ = child.wait();
                return Err(error.to_string());
            }
        }
        if let Some(mut stderr) = child.stderr.take() {
            std::thread::spawn(move || {
                let _ = std::io::copy(&mut stderr, &mut std::io::stderr());
            });
        }
        let (tx, lines) = channel();
        std::thread::spawn(move || {
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => {
                        let _ = tx.send(json!({"__stdout_ended": "end of file"}));
                        return;
                    }
                    Ok(_) => match serde_json::from_str(&line) {
                        Ok(value) => {
                            let _ = tx.send(value);
                        }
                        Err(error) => {
                            let _ = tx.send(json!({"__stdout_ended": format!("invalid JSON: {error}: {line:?}")}));
                            return;
                        }
                    },
                    Err(error) => {
                        let _ = tx.send(json!({"__stdout_ended": format!("{error}")}));
                        return;
                    }
                }
            }
        });
        let mut server = Server {
            child,
            stdin: Some(stdin),
            lines,
            next_id: 1,
            pending_messages: Vec::new(),
            started: Vec::new(),
        };
        server.send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        Ok(server)
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
        if let Some(index) = self
            .pending_messages
            .iter()
            .position(|message| message["id"].as_u64() == Some(id))
        {
            return self.pending_messages.remove(index);
        }
        // A failure bound, not a wait: it stays well above the largest wait a request asks the
        // server for, so a slow machine is a slow answer rather than a client timeout.
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let message = match self.lines.recv_timeout(deadline - Instant::now()) {
                Ok(message) => message,
                // A channel that closed means the server is gone, and how it ended is the one
                // thing that explains the failure.
                Err(error) => panic!(
                    "response in time: {error}; the server {}",
                    self.exit_state()
                ),
            };
            if let Some(reason) = message["__stdout_ended"].as_str() {
                // The reader says why it stopped, so a missing answer names its cause.
                panic!(
                    "response in time: the server's stdout ended ({reason}); the server {}",
                    self.exit_state()
                );
            }
            if message["id"] == json!(id) {
                return message;
            }
            self.pending_messages.push(message);
        }
    }

    /// Calls the tool and returns (structuredContent, text, isError).
    fn call(&mut self, arguments: Value) -> (Value, String, bool) {
        let start = arguments["action"] == "start";
        let message = self.request(
            "tools/call",
            json!({"name": "exec", "arguments": arguments}),
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

    /// How the server child ended, for a failure that has no answer to explain it.
    fn exit_state(&mut self) -> String {
        match self.child.try_wait() {
            Ok(Some(status)) => format!("exited with {status}"),
            Ok(None) => "is still running".to_string(),
            Err(error) => format!("could not be checked: {error}"),
        }
    }

    /// The ID of the `n`th task this server started, counting from 1.
    fn id(&self, n: usize) -> String {
        self.started[n - 1].clone()
    }

    fn bare_id(&self, n: usize) -> String {
        bare_id(&self.started[n - 1]).to_owned()
    }

    fn ok(&mut self, arguments: Value) -> (Value, String) {
        let (structured, text, is_error) = self.call(arguments);
        assert!(!is_error, "unexpected error: {text}");
        (structured, text)
    }

    /// Calls one of the file tools: (text, isError).
    fn file(&mut self, name: &str, arguments: Value) -> (String, bool) {
        let message = self.request("tools/call", json!({"name": name, "arguments": arguments}));
        let result = &message["result"];
        assert!(result.is_object(), "protocol error: {message}");
        let text = result["content"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        (text, result["isError"] == json!(true))
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

/// Readiness authorizes an immediate kill, so the host must have completed its only spawn.
#[cfg(target_os = "linux")]
#[test]
fn a_task_host_reports_readiness_after_its_command_exists() {
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    for _ in 0..32 {
        let (mut status, status_writer) = std::io::pipe().unwrap();
        let (lifeline_reader, lifeline) = std::io::pipe().unwrap();
        let descriptors = [status_writer.as_raw_fd(), lifeline_reader.as_raw_fd()];
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        support::scrub_fastexec_env(&mut command);
        command
            .arg("--fastexec-task-host")
            .args(descriptors.iter().map(|fd| fd.to_string()))
            .args(["/bin/sh", "-c", "exec sleep 300"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        // SAFETY: only async-signal-safe syscalls execute between fork and exec.
        unsafe {
            command.pre_exec(move || {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                for fd in descriptors {
                    if libc::fcntl(fd, libc::F_SETFD, 0) == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                Ok(())
            });
        }
        let mut host = command.spawn().unwrap();
        drop((status_writer, lifeline_reader));
        let mut ready = [0; 4];
        status.read_exact(&mut ready).unwrap();
        assert_eq!(ready, [0; 4], "the host failed startup");
        let children =
            std::fs::read(format!("/proc/{}/task/{}/children", host.id(), host.id())).unwrap();
        // Dropping the lifeline also exercises cleanup before any user-visible task exists.
        drop(lifeline);
        host.wait().unwrap();
        assert!(
            children.iter().any(|byte| !byte.is_ascii_whitespace()),
            "the host reported READY before creating its command"
        );
    }
}

/// An inherited automatic-reaping policy must not release a task host's PID before its Tree.
#[cfg(unix)]
#[test]
fn a_server_restores_waitable_child_statuses_before_starting_tasks() {
    use std::os::unix::process::CommandExt;
    for (disposition, flags) in [(libc::SIG_IGN, 0), (libc::SIG_DFL, libc::SA_NOCLDWAIT)] {
        let mut server = Server::start_with(|command| unsafe {
            command.pre_exec(move || {
                let mut action: libc::sigaction = std::mem::zeroed();
                action.sa_sigaction = disposition;
                action.sa_flags = flags;
                libc::sigemptyset(&mut action.sa_mask);
                if libc::sigaction(libc::SIGCHLD, &action, std::ptr::null_mut()) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        });
        for pty in [false, true] {
            let (out, text) = server.ok(json!({
                "action": "start", "command": "printf child-status; exit 17",
                "pty": pty, "waitMs": 10000
            }));
            assert_eq!(out["state"], "exited", "{text}");
            assert_eq!(out["exitCode"], 17, "{text}");
            assert!(text.contains("child-status"), "{text}");
        }
        let (out, _) = server.ok(json!({
            "action": "start", "command": "sleep 30", "waitMs": 0
        }));
        let (ended, text) = server.ok(json!({"action": "kill", "taskId": out["taskId"]}));
        assert_eq!(ended["state"], "killed", "{text}");
        assert!(ended.get("leftovers").is_none(), "{text}");
    }
}

#[test]
fn short_command_returns_its_final_result_in_one_call() {
    let mut server = Server::start();
    let (out, text) =
        server.ok(json!({"action": "start", "command": "echo hello; echo oops >&2; exit 3"}));
    assert_eq!(out["state"], "exited");
    assert_eq!(out["exitCode"], 3);
    assert!(out.get("leftovers").is_none(), "{text}");
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
            "action": "start", "eof": false, "command": "read -r line; printf '%s\\n' \"$line\"", "waitMs": 0
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

    let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
    support::scrub_fastexec_env(&mut command);
    let result = command
        .env("FASTEXEC_OUTPUT_MODE", "bogus")
        .output()
        .unwrap();
    assert!(
        !result.status.success(),
        "an invalid mode must fail at startup"
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr
            .contains("invalid FASTEXEC_OUTPUT_MODE \"bogus\"; expected text, both, or structured")
            && !stderr.contains("Ok(")
            && !stderr.contains("Err("),
        "{stderr:?}"
    );
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
            json!({"action": "start", "eof": false, "command": "read -r line; printf '%s\\n' \"$line\"", "waitMs": 0}),
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
            &format!("{} [exited 0]", bare_id(&id)),
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

    let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
    support::scrub_fastexec_env(&mut command);
    let result = command
        .env("FASTEXEC_OUTPUT_MODE", "text")
        .env("FASTEXEC_STRUCTURED_CONTENT", "bogus")
        .output()
        .unwrap();
    assert!(
        !result.status.success(),
        "an invalid switch must fail at startup"
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("invalid FASTEXEC_STRUCTURED_CONTENT \"bogus\"; expected true or false")
            && !stderr.contains("Ok(")
            && !stderr.contains("Err("),
        "{stderr:?}"
    );
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
                out.get("lifetimeExpired")
            ),
            (Some("exited"), Some(4), None),
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
        server.bare_id(3),
        server.bare_id(2)
    );
    assert_eq!(footer, expected, "{text}");

    // Reported completions leave the footer; running tasks stay in it.
    let (_, text) = server.ok(json!({"action": "start", "command": "true"}));
    let footer = text.lines().last().unwrap();
    assert!(
        footer.starts_with(&format!("(Background: {} running ", server.bare_id(1)))
            && !footer.contains(&server.bare_id(2)),
        "{text}"
    );

    // Many tasks: three IDs, then counts, within maxBytes.
    for _ in 0..5 {
        server.ok(json!({"action": "start", "command": "sleep 60", "waitMs": 0}));
    }
    let (_, text) =
        server.ok(json!({"action": "start", "command": "seq 1 5000", "maxBytes": 1024}));
    assert!(
        body_of(&text).len() <= 1024,
        "{} bytes",
        body_of(&text).len()
    );
    let footer = text.lines().last().unwrap();
    assert!(
        footer.starts_with(&format!("(Background: {} running ", server.bare_id(9)))
            && footer.ends_with("; 3 more; 6 running; use list.)"),
        "{text}"
    );
    let (_, text) = server.ok(json!({"action": "start", "command": "seq 1 5000", "maxBytes": 100}));
    assert!(
        body_of(&text).len() <= 100,
        "{} bytes: {text}",
        body_of(&text).len()
    );
}

#[test]
fn list_fits_max_bytes_and_leaves_the_tasks_it_omits_due() {
    let mut server = Server::start();
    // The tasks end together, after the footers of the starts showed them running; long
    // commands make lines that do not all fit.
    let (go, go_bash) = scratch("list-go");
    let command = format!(
        "until [ -e {go_bash} ]; do sleep 0.1; done; exit 3 # {}",
        "x".repeat(100)
    );
    for _ in 0..8 {
        server.ok(json!({"action": "start", "command": command, "loginShell": false, "waitMs": 0}));
    }
    std::fs::write(&*go, "").unwrap();
    // Wait for every task to end by asking the list, whose structured rows carry every task.
    // The 100-byte window shows only the omission marker, so it settles no task's footer state.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let (out, _) = server.ok(json!({"action": "list", "truncate": "head", "maxBytes": 100}));
        let ended = out["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|task| task["state"] == json!("exited"))
            .count();
        if ended == 8 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the tasks never finished: {ended} of 8 ended"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let (out, text) = server.ok(json!({"action": "list", "truncate": "head", "maxBytes": 400}));
    assert!(text.len() <= 400, "{} bytes: {text}", text.len());
    assert_eq!(
        out["tasks"].as_array().unwrap().len(),
        8,
        "structuredContent lists every task"
    );
    let owner = out["tasks"][0]["owner"].as_str().unwrap();
    assert!(
        owner.starts_with("test-") && owner.len() > "test-".len(),
        "the list names the session that started a task: {owner}"
    );
    let (first, last) = (text.lines().next().unwrap(), text.lines().last().unwrap());
    assert!(
        first.starts_with(&format!("{} [exited 3]", server.bare_id(8))),
        "newest first: {text}"
    );
    assert!(
        last.starts_with("... [")
            && last.ends_with(" tasks omitted; raise maxBytes to see them] ..."),
        "{text}"
    );
    assert!(!text.contains(&server.bare_id(1)), "{text}");
    // The footer still names the tasks the list left out, newest first, and none it showed.
    let listed: Vec<String> = text
        .lines()
        .filter(|line| !line.starts_with("... ["))
        .map(|line| line.split(' ').next().unwrap_or_default().to_string())
        .collect();
    let (_, text) = server.ok(json!({"action": "start", "command": "true"}));
    let footer = text.lines().last().unwrap();
    let newest_omitted = (1..=8)
        .rev()
        .map(|n| server.bare_id(n))
        .find(|id| !listed.contains(id))
        .unwrap();
    assert!(
        footer.starts_with(&format!("(Background: {newest_omitted} exited 3")),
        "{text}"
    );
    assert!(
        listed.iter().all(|id| !footer.contains(id)),
        "the footer names a task the list showed: {text}"
    );

    let (_, text) = server.ok(json!({"action": "list", "maxBytes": 400}));
    assert!(text.len() <= 400, "{} bytes: {text}", text.len());
    let lines: Vec<&str> = text.lines().collect();
    assert!(
        lines[0].starts_with(&server.bare_id(9))
            && lines.last().unwrap().starts_with(&server.bare_id(1)),
        "head_tail keeps both ends: {text}"
    );
    assert_eq!(
        lines
            .iter()
            .filter(|line| line.starts_with("... ["))
            .count(),
        1,
        "{text}"
    );
    let (_, text, is_error) =
        server.call(json!({"action": "list", "truncate": "none", "maxBytes": 400}));
    assert!(is_error && text.contains("unknown variant"), "{text}");
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
fn a_hundred_byte_result_keeps_the_whole_task_handle() {
    let mut server = Server::start_with(|command| {
        command.env("FASTEXEC_STRUCTURED_CONTENT", "false");
    });
    // A text-only client reads the task ID from the status line, so a small window must not cut
    // through it even when the root has already exited and its processes are still held.
    let (_, text) = server.ok(json!({
        "action": "start",
        "command": "sleep 300 & exit 137",
        "keepTree": true,
        "waitMs": 60000,
        "maxBytes": 100,
    }));
    let window = body_of(text.split("\n\nWarning:").next().unwrap());
    assert!(window.len() <= 100, "the output window fits: {text:?}");
    if text.len() > 100 {
        assert!(
            text.contains(
                "Warning: processes it keeps hold the output open; later output may follow"
            ),
            "only the complete warning exceeds the window budget: {text:?}"
        );
    }
    let status = text
        .lines()
        .find(|line| line.starts_with("[exited 137] "))
        .unwrap_or_else(|| panic!("no status line in {text:?}"));
    let id = status
        .strip_prefix("[exited 137] ")
        .and_then(|rest| rest.split(' ').next())
        .unwrap_or_else(|| panic!("no task ID in {text:?}"))
        .to_string();
    let (task, reader) = id.split_once('.').expect("a reader handle");
    assert_eq!(task.len(), 10, "the whole task ID survives: {text:?}");
    assert_eq!(reader.len(), 4, "the whole reader key survives: {text:?}");
    let (_, listed) = server.ok(json!({"action": "list"}));
    assert!(
        listed.contains(task) && !listed.contains(&id),
        "list names the bare task: {listed:?}"
    );
    server.ok(json!({"action": "kill", "taskId": id}));
}

#[test]
fn tiny_stream_and_transcript_windows_keep_requested_ends_and_truthful_omissions() {
    let mut server = Server::start_with(|command| {
        command.env("FASTEXEC_OUTPUT_MODE", "both");
    });
    for mode in ["head", "tail", "head_tail"] {
        let (started, text) = server.ok(json!({"action": "start",
            "command": "for i in {1..50}; do printf 'line%03d\\n' \"$i\"; done",
            "pty": true, "loginShell": false, "waitMs": 10000,
            "truncate": mode, "maxBytes": 100}));
        let (transcript, transcript_text) = server.ok(json!({"action": "transcript",
            "taskId": started["taskId"], "truncate": mode, "maxBytes": 100}));
        for (out, text) in [(&started, &text), (&transcript, &transcript_text)] {
            let body = out["output"].as_str().unwrap();
            assert!(body.len() <= 100, "{body}");
            if mode != "tail" {
                assert!(
                    body.starts_with("line001"),
                    "{mode}: output={body:?}; result={out}; text={text:?}"
                );
            }
            if mode != "head" {
                assert!(
                    body.ends_with("line050"),
                    "{mode}: output={body:?}; result={out}; text={text:?}"
                );
            }
            let shown: Vec<u64> = body
                .lines()
                .filter_map(|line| line.strip_prefix("line").and_then(|n| n.parse().ok()))
                .collect();
            let range = out["omittedRange"].as_array().unwrap();
            let (first, last) = (range[0].as_u64().unwrap(), range[1].as_u64().unwrap());
            assert_eq!(out["omittedLines"], 50 - shown.len() as u64, "{out}");
            assert_eq!(last - first + 1, 50 - shown.len() as u64, "{out}");
            assert!(shown.iter().all(|n| *n < first || *n > last), "{out}");
        }
    }
}

#[test]
fn head_truncate_stops_at_max_bytes_and_names_the_log() {
    let long = "y".repeat(2000);
    let mut server = Server::start();
    let (out, text) = server.ok(json!({
        "action": "start",
        "command": format!("printf '%s\n' {long}; seq 1 40"),
        "truncate": "head",
        "maxBytes": 1000,
        "waitMs": 30000,
    }));
    let log_path = out["logPath"].as_str().unwrap();
    assert!(
        text.contains(log_path),
        "the status line names the log: {text}"
    );
    assert!(
        text.contains("line cut at "),
        "the oversized first line is cut, not left out: {text}"
    );
    assert_eq!(out["cutLines"], 1, "{out}");
    assert_eq!(out["omittedRange"], json!([2, 41]), "{out}");
    let body = body_of(&text);
    assert!(body.len() <= 1000, "{} bytes of body: {body:?}", body.len());

    // A valid multibyte line crossing the cap is clipped on a UTF-8 boundary without encodingErrors.
    let (out, text) = server.ok(json!({
        "action": "start",
        "command": "printf '中%.0s' $(seq 1 400); echo",
        "truncate": "head",
        "maxBytes": 1000,
        "waitMs": 30000,
    }));
    assert!(out.get("encodingErrors").is_none(), "{out}");
    assert!(body_of(&text).len() <= 1000, "{text}");
}

#[test]
fn list_head_stops_at_max_bytes_and_reports_the_rows_it_omits() {
    let mut server = Server::start();
    let command = format!("sleep 60 # {}", "z".repeat(300));
    for _ in 0..8 {
        server.ok(json!({"action": "start", "command": command, "waitMs": 0}));
    }
    let (out, text) = server.ok(json!({"action": "list", "truncate": "head", "maxBytes": 1000}));
    assert!(text.len() <= 1000, "{} bytes: {text}", text.len());
    assert!(
        text.contains("tasks omitted"),
        "the rows that did not fit are reported: {text}"
    );
    assert_eq!(
        out["tasks"].as_array().unwrap().len(),
        8,
        "structuredContent still lists every task"
    );
    for n in 1..=8 {
        server.ok(json!({"action": "kill", "taskId": server.id(n)}));
    }
}

#[test]
fn tasks_inherit_the_launch_context_the_server_captured_at_startup() {
    let dir = Scratch::new("launch-context");
    std::fs::create_dir_all(&*dir).unwrap();
    let name = dir.file_name().unwrap().to_string_lossy().into_owned();
    let mut server = Server::start_with(|command| {
        command.current_dir(&*dir).env("FASTEXEC_CTX_PROBE", "xyz");
    });
    let (_, text) = server.ok(json!({
        "action": "start",
        "command": "printf '%s|%s' \"$FASTEXEC_CTX_PROBE\" \"$(pwd -P)\"",
        "waitMs": 30000,
    }));
    assert!(
        text.contains("xyz|"),
        "the captured environment reached the task: {text}"
    );
    assert!(
        text.contains(&name),
        "a start without cwd ran in the context's directory: {text}"
    );
}

#[test]
fn the_listed_tool_says_which_host_it_runs_on() {
    let dir = Scratch::new("exec-description");
    std::fs::create_dir_all(&*dir).unwrap();
    let mut server = Server::start_with(|command| {
        command.current_dir(&*dir);
    });
    let listed = server.request("tools/list", json!({}));
    let tools = listed["result"]["tools"].as_array().unwrap();
    let mut names: Vec<&str> = tools
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["edit", "exec", "read", "write"], "{listed}");
    // `transfer` is off unless `FASTEXEC_TRANSFER=true`, so it is not listed by default.
    let refused = server.request(
        "tools/call",
        json!({"name": "transfer", "arguments": {"path": "notes/a.txt"}}),
    );
    assert_eq!(
        refused["error"]["message"],
        json!("tool not found"),
        "{refused}"
    );
    let exec = tools
        .iter()
        .find(|tool| tool["name"] == json!("exec"))
        .unwrap_or_else(|| panic!("no exec in {listed}"));
    let description = exec["description"].as_str().unwrap();
    // The host line is the last one, so a client that shows the tail still says where it runs.
    let last = description.lines().last().unwrap();
    assert!(last.starts_with("Host: "), "{last:?}");
    let cwd = last
        .split_once("default cwd ")
        .unwrap_or_else(|| panic!("no default cwd in {last:?}"))
        .1;
    // macOS reports `/private/var` for a $TMPDIR under `/var`, so both sides are compared as the
    // filesystem resolves them.
    let expected = std::fs::canonicalize(&*dir).unwrap_or_else(|_| PathBuf::from(&*dir));
    let cwd = std::fs::canonicalize(cwd).unwrap_or_else(|_| PathBuf::from(cwd));
    let same = if cfg!(windows) {
        cwd.to_string_lossy()
            .eq_ignore_ascii_case(&expected.to_string_lossy())
    } else {
        cwd == expected
    };
    assert!(same, "cwd {cwd:?}, expected {expected:?} in {last:?}");
    // The Windows paragraph is there exactly where it applies.
    assert_eq!(
        description.contains("MSYS_NO_PATHCONV"),
        cfg!(windows),
        "{description}"
    );
}

#[test]
fn edit_refuses_ambiguity_hidden_by_normalization() {
    let dir = Scratch::new("edit-ambiguity");
    std::fs::create_dir_all(&*dir).unwrap();
    let mut server = Server::start_with(|command| {
        command.current_dir(&*dir);
    });
    for (content, old, new, batch) in [
        ("cafe\u{301} cafe", "cafe", "coffee", false),
        ("cafe\u{301} cafe\u{301}", "cafe", "coffee", false),
        ("a\u{301} e\u{301}", "\u{301}", "", false),
        (
            "header\nJose\u{301} Martin\nJose\u{301} Perez\n",
            "Jose",
            "Joe",
            true,
        ),
        ("cafe cafe", "cafe", "coffee", false),
        ("e\u{301}e\u{301}e\u{301}", "e\u{301}e", "X", false),
        ("\u{ff41}aa\u{301}", "aa", "X", false),
        ("it's it\u{2019}s", "it's", "it is", false),
        (
            "cafe\u{301} \u{ff43}\u{ff41}\u{ff46}\u{ff45}",
            "cafe",
            "coffee",
            false,
        ),
        (
            "\u{ff43}\u{ff41}\u{ff46}\u{ff45} cafe\u{301}",
            "cafe",
            "coffee",
            false,
        ),
    ] {
        let path = dir.join("case.txt");
        std::fs::write(&path, content).unwrap();
        let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
        let mut edits = Vec::new();
        if batch {
            edits.push(json!({"oldText": "header", "newText": "HEADER"}));
        }
        edits.push(json!({"oldText": old, "newText": new}));
        let (text, error) = server.file("edit", json!({"path": "case.txt", "edits": edits}));
        assert!(error, "{content:?} / {old:?}: {text}");
        #[cfg(unix)]
        let shown = std::fs::canonicalize(&*dir)
            .unwrap()
            .join("case.txt")
            .display()
            .to_string();
        #[cfg(windows)]
        let shown = path.to_string_lossy().replace('\\', "/");
        let target = if batch { "edits[1]" } else { "edits[0]" };
        assert!(
            text.starts_with(&format!("Cannot edit {shown}; nothing was changed:\n- {target}: oldText occurs 2 times, starting at lines ")),
            "{text}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), content.as_bytes());
        assert_eq!(
            std::fs::metadata(&path).unwrap().modified().unwrap(),
            modified
        );
    }
}

#[test]
fn edit_allows_a_unique_exact_match_changed_by_normalization() {
    let dir = Scratch::new("edit-unique");
    std::fs::create_dir_all(&*dir).unwrap();
    let mut server = Server::start_with(|command| {
        command.current_dir(&*dir);
    });
    for (content, old, new, expected) in [
        ("cafe\u{301}", "cafe", "coffee", "coffee\u{301}"),
        ("a\u{301}", "\u{301}", "", "a"),
        ("a ", "a ", "X", "X"),
        ("a \n", "a \n", "X\n", "X\n"),
        (" \na \n", " \na \n", "X\n", "X\n"),
    ] {
        let path = dir.join("case.txt");
        std::fs::write(&path, content).unwrap();
        let (text, error) = server.file(
            "edit",
            json!({"path": "case.txt", "edits": [{"oldText": old, "newText": new}]}),
        );
        #[cfg(unix)]
        let shown = std::fs::canonicalize(&*dir)
            .unwrap()
            .join("case.txt")
            .display()
            .to_string();
        #[cfg(windows)]
        let shown = path.to_string_lossy().replace('\\', "/");
        assert!(!error, "{content:?} / {old:?}: {text}");
        assert_eq!(
            text,
            format!("Successfully replaced 1 block(s) in {shown}.\nedits[0]: line 1")
        );
        assert_eq!(std::fs::read(&path).unwrap(), expected.as_bytes());
    }
}

#[test]
fn file_tools_round_trip_through_the_servers_directory() {
    let dir = Scratch::new("files");
    std::fs::create_dir_all(&*dir).unwrap();
    let mut server = Server::start_with(|command| {
        command.current_dir(&*dir);
    });
    #[cfg(unix)]
    let base_dir = std::fs::canonicalize(&*dir).unwrap();
    #[cfg(windows)]
    let base_dir = dir.to_path_buf();
    let shown_note = base_dir
        .join("notes/a.txt")
        .to_string_lossy()
        .replace('\\', "/");
    let shown_missing = base_dir
        .join("notes/missing.txt")
        .to_string_lossy()
        .replace('\\', "/");
    let (text, error) = server.file(
        "write",
        json!({"path": "notes/a.txt", "content": "one\ntwo\n"}),
    );
    assert!(!error, "{text}");
    assert_eq!(text, format!("Successfully wrote to {shown_note}"));
    let (text, error) = server.file("read", json!({"path": "notes/a.txt"}));
    assert!(!error, "{text}");
    assert_eq!(text, "one\ntwo\n");
    let (text, error) = server.file(
        "edit",
        json!({"path": "notes/a.txt", "edits": [{"oldText": "two", "newText": "TWO"}]}),
    );
    assert!(!error, "{text}");
    assert_eq!(
        text,
        format!("Successfully replaced 1 block(s) in {shown_note}.\nedits[0]: line 2")
    );
    let (text, _) = server.file("read", json!({"path": "notes/a.txt"}));
    assert_eq!(text, "one\nTWO\n");
    // The path resolved against the server's own directory, which is where the file now is.
    assert_eq!(
        std::fs::read(dir.join("notes/a.txt")).unwrap(),
        b"one\nTWO\n"
    );
    // A missing file is an error result, not a protocol failure.
    let (text, error) = server.file("read", json!({"path": "notes/missing.txt"}));
    assert!(error, "{text}");
    assert!(
        text.starts_with(&format!("Cannot read {shown_missing}: ")),
        "{text}"
    );
}

#[test]
fn read_answers_a_png_with_a_text_block_and_an_image_block() {
    let dir = Scratch::new("image");
    std::fs::create_dir_all(&*dir).unwrap();
    let image =
        image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(8, 6, image::Rgb([7, 8, 9])));
    let mut png = Vec::new();
    image
        .write_to(&mut std::io::Cursor::new(&mut png), image::ImageFormat::Png)
        .unwrap();
    std::fs::write(dir.join("shot.png"), &png).unwrap();
    let mut server = Server::start_with(|command| {
        command.current_dir(&*dir);
    });
    let message = server.request(
        "tools/call",
        json!({"name": "read", "arguments": {"path": "shot.png"}}),
    );
    let result = &message["result"];
    assert_eq!(result["isError"], json!(false), "{message}");
    let content = result["content"].as_array().expect("content blocks");
    assert_eq!(content.len(), 2, "{message}");
    assert_eq!(content[0]["type"], json!("text"));
    assert_eq!(content[0]["text"], json!("Read image file [image/png]"));
    assert_eq!(content[1]["type"], json!("image"));
    assert_eq!(content[1]["mimeType"], json!("image/png"));
    assert_eq!(
        content[1]["data"],
        json!(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            &png
        ))
    );
}

#[test]
fn read_omits_an_image_past_the_encoded_ceiling_without_loading_it() {
    let dir = Scratch::new("image-oversized");
    std::fs::create_dir_all(&*dir).unwrap();
    // Sparse: a PNG signature and the IHDR chunk it is recognized by at the front, and a length
    // past the ceiling a read loads, so creating it costs nothing and reading it whole would
    // cost the ceiling.
    let path = dir.join("huge.png");
    let mut file = std::fs::File::create(&path).unwrap();
    std::io::Write::write_all(&mut file, b"\x89PNG\r\n\x1a\n\x00\x00\x00\x0dIHDR").unwrap();
    file.set_len((512 << 20) + 1).unwrap();
    drop(file);
    let mut server = Server::start_with(|command| {
        command.current_dir(&*dir);
    });
    let message = server.request(
        "tools/call",
        json!({"name": "read", "arguments": {"path": "huge.png"}}),
    );
    let result = &message["result"];
    assert_eq!(result["isError"], json!(false), "{message}");
    let content = result["content"].as_array().expect("content blocks");
    assert_eq!(content.len(), 1, "no image is attached: {message}");
    assert_eq!(
        content[0]["text"],
        json!("Read image file [image/png]\n[Image omitted: the file is larger than 512 MiB.]")
    );
}

#[test]
fn pipe_input_arrives_verbatim_and_eof_closes_stdin() {
    // Git for Windows' bin/bash.exe launcher retains an inherited stdin handle.
    // Use discovery's real usr/bin/bash.exe so closing fd 0 closes the pipe reader.
    let mut server = Server::start_with(|command| {
        command.env_remove("FASTEXEC_BASH");
    });
    server.ok(json!({"action": "start", "eof": false, "command": "while IFS= read -r l || [ -n \"$l\" ]; do echo \"got:$l\"; done; echo end", "waitMs": 0}));
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
    let (_, text) = server.ok(json!({"action": "start", "eof": false, "command": "exec 0<&-; echo stdin-closed; sleep 30", "waitMs": 30000, "returnWhen": {"outputContains": ["stdin-closed"]}}));
    assert!(text.starts_with("stdin-closed\n"), "{text}");
    let (_, text, is_error) = server
        .call(json!({"action": "poll", "taskId": server.id(2), "input": "x\n", "waitMs": 50}));
    assert!(is_error && text.contains("stdin is closed"), "{text}");
    let (out, text, is_error) = server.call(json!({"action": "kill", "taskId": server.id(2)}));
    assert!(is_error, "the task must retain its input failure: {text}");
    assert_eq!(out["state"], "killed", "{text}");
    assert!(out["inputError"].as_str().is_some(), "{text}");
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

/// The bytes `od -An -tx1` printed, one space between them: BSD `od` pads its columns to the
/// width of a full line and GNU `od` does not.
fn od(text: &str) -> String {
    let mut normalized = String::new();
    for line in text.split_inclusive('\n') {
        let words: Vec<&str> = line.split_whitespace().collect();
        let bytes = !words.is_empty()
            && words
                .iter()
                .all(|word| word.len() == 2 && word.bytes().all(|b| b.is_ascii_hexdigit()));
        if bytes {
            normalized.push(' ');
            normalized.push_str(&words.join(" "));
            if line.ends_with('\n') {
                normalized.push('\n');
            }
        } else {
            normalized.push_str(line);
        }
    }
    normalized
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
    let text = od(&text);
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
        let text = od(&text);
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
    let text = od(&text);
    assert!(
        text.contains("1b 5b 32 37 75 1b 5b 31 30 35 3b 35 75 09") && text.contains("popped"),
        "Escape and C-i as CSI u, Tab as itself: {text}"
    );
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "keys": ["Escape", "Tab"], "waitMs": 10000
    }));
    assert_eq!(out["state"], "exited", "{text}");
    let text = od(&text);
    assert!(text.contains("1b 09"), "legacy bytes once popped: {text}");
}

#[test]
fn keyboard_stack_overflow_keeps_screen_keys_and_output_working() {
    let mut server = Server::start();
    // This used to trigger alacritty_terminal 0.26's 4097th-push panic. The bounded wrapper
    // must leave both raw output and the emulator's screen and keyboard modes usable.
    let command = "printf '\\033[>1u%.0s' $(seq 1 4097); printf 'alive\\r\\n'; read -r x; printf 'done\\r\\n'";
    let (_, text) = server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 30000, "returnWhen": {"outputContains": ["alive"]}}));
    assert!(text.contains("alive"), "{text}");
    let (out, screen) = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "screen": true, "waitMs": 0
    }));
    assert_eq!(out["state"], "running", "{screen}");
    assert!(
        screen.split("\n\n[").next().unwrap().contains("alive"),
        "{screen}"
    );
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "keys": ["Enter"], "waitMs": 10000
    }));
    assert_eq!(out["state"], "exited", "{text}");
    assert!(text.contains("done"), "{text}");
    let (_, transcript) = server.ok(json!({
        "action": "transcript", "taskId": server.id(1), "maxBytes": 4194304
    }));
    assert!(
        transcript.contains("alive") && transcript.contains("done"),
        "{transcript}"
    );
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
    // inserts, a combining mark joins its base, a wide character fills two columns, excessive
    // combining marks stop at the per-cell budget (DEL between them moves nothing), a tab
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
        format!("cap e{}", "\u{301}".repeat(32)),
        "tab a   b".to_string(),
        "dch  a".to_string(),
    ]
    .join("\n");
    // The wait targets the full final state, cursor included. A screen condition can match before
    // the trailing CRLF moves the cursor to [7, 1], and output that paused just before it looks
    // quiet while the cursor is still elsewhere, so the wait observes the whole state.
    server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 0
    }));
    let task = server.id(1);
    let (out, text) = wait_for_screen(&mut server, &task, &expected, json!([7, 1]));
    assert_eq!(out["state"], "running", "{text}");
    assert_eq!(
        text.split("\n\n[").next().unwrap(),
        expected,
        "screen: {text}"
    );
    assert_eq!(out["cursor"], json!([7, 1]), "{text}");
    #[cfg(unix)]
    {
        // The rendering cap must not alter the bytes captured from the Unix PTY.
        let raw = std::fs::read(out["logPath"].as_str().unwrap()).unwrap();
        let full_run = format!("cap e{}", "\u{301}\u{7f}".repeat(40));
        assert!(
            raw.windows(full_run.len())
                .any(|bytes| bytes == full_run.as_bytes()),
            "all 40 original marks and DEL bytes must remain in the raw log"
        );
    }
    let (_, text) =
        server.ok(json!({"action": "transcript", "taskId": server.id(1), "maxBytes": 4194304}));
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
    // What the program waits for is a terminal answer, not a profile: a login shell spends seconds
    // sourcing one under load, for nothing this test reads.
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "pty": true, "loginShell": false, "waitMs": 10000
    }));
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
        "printf 'kept %s\\n' {1..40}; printf '\\033[2J\\033[H\\033[3J'; printf 'kept %s\\n' {1..45}; ",
        "printf '\\033[2J\\033[H\\033[3J'; printf 'plain %s\\n' {1..40}; ",
        "printf '\\033[30;1H> composer'; ",
        "for k in $(seq 1 40); do ",
        "printf '\\033[1;29r\\033[29;1H\\r\\ninsert %d\\033[r\\033[30;1H> composer\\033[K' $k; done"
    );
    let (out, text) =
        server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 30000}));
    assert_eq!(out["state"], "exited", "{text}");
    let (out, text) =
        server.ok(json!({"action": "transcript", "taskId": server.id(1), "maxBytes": 4194304}));
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
    // from its frames. Each page is held back by a gate file the test creates only after the
    // screen shows the page before it, so a page is replaced only once it was drawn: a frame the
    // recorder missed fails here rather than being papered over by a longer sleep. Each page is
    // a synchronized update, one frame however its writes arrive: the MSYS runtime writes a page
    // a line or escape sequence at a time, and a loaded host pausing between them would show
    // half-drawn pages. Drawn as lines, a page ends well inside the update's timeout.
    let gate = std::env::temp_dir().join(format!("fastexec-pty-gate-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&gate);
    std::fs::create_dir_all(&gate).unwrap();
    let first_gate = gate.join("second");
    let second_gate = gate.join("scrolled");
    let command = format!(
        concat!(
            "printf 'normal %s\\n' {{1..40}}; printf 'a\\tb\\n\\033[?1049h'; ",
            "printf '\\033[?2026h\\033[H%s\\033[?2026l' \"$(printf 'first %s\\n' {{1..30}})\"; ",
            "while [ ! -f '{}' ]; do sleep 0.01; done; ",
            "printf '\\033[?2026h\\033[H%s\\033[?2026l' \"$(printf 'second %s\\n' {{1..30}})\"; ",
            "while [ ! -f '{}' ]; do sleep 0.01; done; ",
            "printf '\\r\\nscrolled %d' $(seq 1 40); read -r x"
        ),
        first_gate.display(),
        second_gate.display()
    );
    // The first page is on screen before anything replaces it.
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 60000,
        "returnWhen": {"screenContains": ["first 30"]}
    }));
    assert_eq!(
        out["waitEndedBy"]["condition"], "screen_contains",
        "the program drew its first page: {text}"
    );
    // Then the second page, and it is on screen before the next one is drawn.
    std::fs::write(&first_gate, b"").unwrap();
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": server.id(2), "waitMs": 60000,
        "returnWhen": {"screenContains": ["second 30"]}
    }));
    assert_eq!(
        out["waitEndedBy"]["condition"], "screen_contains",
        "the program drew its second page: {text}"
    );
    // Wait for the last line to be on screen before allowing a quiet interval to end the wait:
    // a quiet window that elapses early must not stand in for the drawing being done. A quiet
    // read after it then settles the frames.
    std::fs::write(&second_gate, b"").unwrap();
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": server.id(2), "waitMs": 60000,
        "returnWhen": {"screenContains": ["scrolled 40"]}
    }));
    assert_eq!(
        out["waitEndedBy"]["condition"], "screen_contains",
        "the program drew its last line: {text}"
    );
    server.ok(json!({
        "action": "poll", "taskId": server.id(2), "waitMs": 15000,
        "returnWhen": {"quietMs": 3000}
    }));
    let (out, text) =
        server.ok(json!({"action": "transcript", "taskId": server.id(2), "maxBytes": 4194304}));
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
        server.ok(json!({"action": "transcript", "taskId": server.id(2), "maxBytes": 4194304}));
    assert_eq!(out["firstLine"], json!(first), "{text}");
    assert!(!text.contains("normal 1\n"), "{text}");
    assert!(text.contains("scrolled 40typed"), "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(2)}));
    let _ = std::fs::remove_dir_all(&gate);

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
        assert!(
            body_of(&text).len() <= 100,
            "{} bytes: {text}",
            body_of(&text).len()
        );

        let command = format!("{} echo started", heartbeat(leaked_bash));
        let (mut out, mut text) =
            server.ok(json!({"action": "start", "command": command, "pty": pty, "waitMs": 60000}));
        assert_eq!(out["state"], "exited", "pty={pty}");
        assert!(!out["leftovers"].as_array().unwrap().is_empty(), "{text}");
        if out["keptRunning"] == json!(true) {
            // A signalled process may outlast the bounded kill check on a loaded machine.
            // Poll for automatic cleanup; a second kill would hide a leaked descendant.
            (out, text) = server.ok(json!({
                "action": "poll", "taskId": out["taskId"], "waitMs": 60000,
            }));
            assert_eq!(out["waitEndedBy"]["condition"], "tree_ended", "{text}");
        }
        assert!(
            (text.contains("ended with bash: ") && text.contains("keepTree: true"))
                || text.contains("kept processes ended"),
            "the result names what bash left or confirms its later cleanup (pty={pty}): {text}"
        );
        assert_eq!(out["state"], "exited", "{text}");
        assert_eq!(out["exitCode"], 0, "{text}");
        assert!(out.get("keptRunning").is_none(), "{text}");
        assert!(out.get("held").is_none(), "{text}");
        assert!(out.get("killError").is_none(), "{text}");
        assert_stops_growing(leaked);
    }
}

#[test]
fn kept_pipe_stdin_stays_open_after_root_exit() {
    kept_stdin_stays_open_after_root_exit(false);
}

#[cfg(windows)]
#[test]
fn a_finished_script_ends_a_root_that_does_not_exit() {
    // The script's status is read as the script ends, so a complete record and an empty file
    // both end the task within the grace, with the recorded status or 1.
    finished_script_case("printf '%s\n' 7 > \"$FASTEXEC_EXIT_STATUS\"", 7);
    finished_script_case(": > \"$FASTEXEC_EXIT_STATUS\"", 1);
}

/// Starts a task whose script records its status, leaves a heartbeat in an independent
/// subshell and then blocks, so only the grace after the script's end can end it, and only the
/// tree kill can end the heartbeat.
#[cfg(windows)]
fn finished_script_case(write_status: &str, expected: i32) {
    let beats = std::env::temp_dir().join(format!(
        "fastexec-heartbeat-{}-{expected}",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&beats);
    let mut server = Server::start_with(|command| {
        command.env("FASTEXEC_TEST_HEARTBEAT", &beats);
    });
    let command = format!(
        "( while :; do printf x >> \"$FASTEXEC_TEST_HEARTBEAT\"; sleep 0.1; done ) & {}; sleep 120",
        write_status
    );
    let started = Instant::now();
    let (out, text) =
        server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 30000}));
    let elapsed = started.elapsed();
    assert_eq!(out["state"], "exited", "{text}");
    assert_eq!(out["exitCode"], expected, "{text}");
    assert_eq!(out["forcedEnd"], true, "{text}");
    let (listed, _) = server.ok(json!({"action": "list", "limit": 1}));
    assert_eq!(
        listed["tasks"][0]["taskId"],
        bare_id(out["taskId"].as_str().unwrap())
    );
    assert_eq!(listed["tasks"][0]["forcedEnd"], true, "{listed}");
    assert!(
        elapsed < Duration::from_secs(12),
        "the grace ended the root within its bound: {elapsed:?}"
    );
    assert!(
        text.contains("root forced after 5 s exit grace"),
        "the result names the forced end: {text}"
    );
    assert!(
        !out["leftovers"].as_array().unwrap().is_empty(),
        "the leftover heartbeat is named: {text}"
    );
    let written = std::fs::read(&beats).expect("the heartbeat file").len();
    assert!(written > 0, "the heartbeat ran: {beats:?}");
    std::thread::sleep(Duration::from_millis(700));
    let after = std::fs::read(&beats).map(|bytes| bytes.len()).unwrap_or(0);
    assert_eq!(after, written, "the forced end ended the heartbeat too");
}

#[cfg(windows)]
#[test]
fn a_child_process_does_not_record_the_roots_status() {
    root_only_case("bash \"$0\" child");
}

#[cfg(windows)]
#[test]
fn a_child_subshell_does_not_record_the_roots_status() {
    root_only_case("( trap - EXIT; . \"$0\" child )");
}

/// Runs a command over the script threshold from its file, with a child that runs that file
/// again, and requires the root to finish its own work with its own status.
#[cfg(windows)]
fn root_only_case(invoker: &str) {
    let mut server = Server::start();
    let padding = "echo padding-line\n".repeat(700);
    let command = format!(
        "if [ \"${{1-}}\" = child ]; then sleep 1; exit 7; fi\n{padding}{invoker}\nsleep 12\necho root-done"
    );
    let started = Instant::now();
    let (out, text) =
        server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 30000}));
    let elapsed = started.elapsed();
    assert_eq!(out["state"], "exited", "{text}");
    assert_eq!(out["exitCode"], 0, "{text}");
    assert!(out.get("forcedEnd").is_none(), "{text}");
    assert!(
        elapsed >= Duration::from_secs(12),
        "the root was not ended early: {elapsed:?}"
    );
}

#[cfg(windows)]
#[test]
fn a_startup_exit_trap_keeps_its_own_cleanup() {
    // A profile-like startup file sets its own EXIT cleanup before the command runs; the status
    // marker rides on the command line and never replaces that cleanup.
    let dir = std::env::temp_dir().join(format!("fastexec-startup-trap-{}", std::process::id()));
    // A stale directory from an earlier run must not answer for this one.
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("a scratch directory");
    let marker = dir.join("startup-cleanup");
    let startup = dir.join("bash_env.sh");
    std::fs::write(
        &startup,
        format!(
            "trap 'printf ran > \"{}\"' EXIT\n",
            marker.to_string_lossy().replace('\\', "/")
        ),
    )
    .expect("the startup file");
    let mut server = Server::start_with(|command| {
        command.env("BASH_ENV", &startup);
    });
    let (out, text) = server.ok(json!({"action": "start", "command": "true", "pty": true,
        "loginShell": false, "waitMs": 10000}));
    assert_eq!(out["state"], "exited", "{text}");
    assert_eq!(out["exitCode"], 0, "{text}");
    assert!(out.get("forcedEnd").is_none(), "{text}");
    assert!(
        marker.exists(),
        "the startup file's own EXIT cleanup ran: {marker:?} {text}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn kept_pty_stdin_stays_open_after_root_exit() {
    kept_stdin_stays_open_after_root_exit(true);
}

fn kept_stdin_stays_open_after_root_exit(pty: bool) {
    let mut server = Server::start_with(|command| {
        command.env_remove("FASTEXEC_BASH");
    });
    let source = if pty { "/dev/tty" } else { "&0" };
    let command = format!(
        "(while IFS= read -r line; do printf 'got:%s\\n' \"$line\"; done; echo input-ended) <{source} & echo ready"
    );
    let mut start_args = json!({"action": "start", "command": command,
        "pty": pty, "keepTree": true, "loginShell": false, "waitMs": 10000});
    if !pty {
        start_args["eof"] = json!(false);
    }
    let (out, text) = server.ok(start_args);
    assert_eq!(out["state"], "exited", "{text}");
    assert_eq!(out["keptRunning"], true, "{text}");
    let id = out["taskId"].clone();
    let (_, text) = server.ok(json!({"action": "poll", "taskId": id,
        "input": "after-root\n", "waitMs": 10000,
        "returnWhen": {"outputContains": ["got:after-root"]}}));
    assert!(text.contains("got:after-root"), "{text}");
    let finish = if pty {
        json!({"action": "poll", "taskId": id, "keys": ["C-d"], "waitMs": 10000})
    } else {
        json!({"action": "poll", "taskId": id, "eof": true, "waitMs": 10000})
    };
    let (out, text) = server.ok(finish);
    assert!(text.contains("input-ended"), "{text}");
    assert!(out.get("keptRunning").is_none(), "{text}");
    let (_, text, failed) = server.call(json!({"action": "poll", "taskId": id,
        "input": "late\n", "waitMs": 0}));
    assert!(failed && text.contains("exited"), "{text}");
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
        let (out, text) = server.ok(json!({"action": "start", "command": command, "pty": pty, "keepTree": true, "waitMs": 60000}));
        assert_eq!(out["state"], "exited", "pty={pty}: {text}");
        assert_eq!(out["keptRunning"], true, "pty={pty}: {text}");
        assert!(
            text.contains("started") && text.contains(" running; kill ends them"),
            "{text}"
        );
        let id = out["taskId"].clone();
        wait_for_heartbeat(kept);
        let before = file_len(kept);
        let deadline = Instant::now() + Duration::from_secs(30);
        while file_len(kept) <= before {
            if Instant::now() >= deadline {
                panic!("a kept process stopped (pty={pty})");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let (list, _) = server.ok(json!({"action": "list"}));
        let entry = list["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|task| task["taskId"] == bare_id(id.as_str().unwrap()))
            .cloned()
            .unwrap();
        assert_eq!(entry["keptRunning"], true);
        assert!(!entry["held"].as_array().unwrap().is_empty(), "{entry}");
        let (out, text) = server.ok(json!({"action": "kill", "taskId": id}));
        assert_eq!(
            out["state"], "exited",
            "kill keeps the root's result: {text}"
        );
        assert!(out.get("keptRunning").is_none(), "{text}");
        assert!(out.get("held").is_none(), "{text}");
        assert!(text.contains("kept processes ended"), "{text}");
        assert_stops_growing(kept);

        // On a loaded machine the deadline can fire before this start even answers, so the
        // first result may already be the expired one. Either way the deadline, not a kill,
        // ends the kept processes.
        let command = format!("{} echo started", heartbeat(expiring_bash));
        let (out, text) = server.ok(json!({"action": "start", "command": command, "pty": pty, "keepTree": true, "killAfterMs": 8000, "waitMs": 60000}));
        let id = out["taskId"].clone();
        if out["keptRunning"] == json!(true) {
            // The root finished first: the deadline then reaches the kept processes only.
            assert_eq!(out["state"], "exited", "pty={pty}: {text}");
            assert_eq!(out["exitCode"], 0, "pty={pty}: {text}");
            let (out, text) = server.ok(json!({"action": "poll", "taskId": id, "waitMs": 60000}));
            assert_eq!(
                out["waitEndedBy"]["condition"], "tree_ended",
                "killAfterMs ends the kept processes: {text}"
            );
            assert_eq!(out["exitCode"], 0, "and keeps the root's result: {text}");
            assert!(out.get("keptRunning").is_none(), "{text}");
            assert!(out.get("held").is_none(), "{text}");
        } else {
            // Two valid outcomes: the root had already exited 0 and the deadline then reached
            // the kept processes (the flag stays false, as `expire` only records a running
            // root), or the deadline killed the root itself.
            assert!(out.get("held").is_none(), "{text}");
            if out["lifetimeExpired"] == json!(true) {
                assert_eq!(out["state"], "killed", "pty={pty}: {text}");
            } else {
                assert_eq!(out["state"], "exited", "pty={pty}: {text}");
                assert_eq!(out["exitCode"], 0, "pty={pty}: {text}");
            }
        }
        assert_stops_growing(expiring);
    }
    let (out, text) = server.ok(json!({"action": "start", "command": "true", "keepTree": true}));
    assert!(
        out.get("keptRunning").is_none(),
        "nothing left, nothing kept: {text}"
    );
    assert!(out.get("leftovers").is_none());
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
        let (out, text) = server.ok(json!({"action": "start", "command": command, "pty": pty, "keepTree": true, "loginShell": false, "waitMs": 60000}));
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
        let transcript = log.with_extension("transcript.txt");
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

    // Once their processes end, they are finished too. Their end is the newest one, so the log
    // that ended before them goes first and the next writer's log pays for reaching them.
    for id in [&pipe, &pty] {
        server.ok(json!({"action": "kill", "taskId": id}));
    }
    for _ in 0..2 {
        server.ok(json!({"action": "start", "command": write(614_400), "loginShell": false, "waitMs": 60000}));
    }
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
    // The kill ends the tree, so the task is finished now and the cap applies to it: it stays,
    // and the earliest finisher goes.
    let (out, _) = server.ok(json!({"action": "list", "limit": 100}));
    let tasks = out["tasks"].as_array().unwrap();
    assert_eq!(tasks.len(), 64, "{out}");
    let listed = |id: &str| tasks.iter().any(|task| task["taskId"] == bare_id(id));
    assert!(
        listed(&kept),
        "the task that just became finished stays: {out}"
    );
    assert!(
        !listed(&server.id(2)),
        "the earliest finisher goes first: {out}"
    );
    server.ok(json!({"action": "start", "command": "true"}));
    // Retirement follows the result that shows the completion.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (out, _) = server.ok(json!({"action": "list", "limit": 100}));
        let tasks = out["tasks"].as_array().unwrap();
        if tasks.len() == 64 {
            let listed = |id: &str| tasks.iter().any(|task| task["taskId"] == bare_id(id));
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
    assert!(out.get("keptRunning").is_none(), "{text}");
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
    // The footer names the processes a task keeps once.
    assert!(
        text.contains(&format!("{} keeps ", bare_id(&ready))),
        "{text}"
    );

    // The footer leaves out held processes it showed, and names the end of a tree it did not
    // show once.
    let (_, text) = server.ok(json!({"action": "start", "command": "true"}));
    assert!(
        !text.contains(&format!("{} keeps ", bare_id(&ready))),
        "shown once: {text}"
    );
    server.ok(json!({"action": "kill", "taskId": failed}));
    let command = format!("(sleep 1; touch {done_bash}) & echo short");
    server.ok(json!({"action": "start", "command": command, "keepTree": true, "waitMs": 60000}));
    let short = server.id(5);
    wait_for_file(done);
    std::thread::sleep(Duration::from_millis(2000));
    let (_, text) = server.ok(json!({"action": "start", "command": "true"}));
    assert!(
        text.contains(&format!("{} kept processes ended", bare_id(&short))),
        "{text}"
    );
    let (_, text) = server.ok(json!({"action": "start", "command": "true"}));
    assert!(
        !text.contains(&format!("{} kept", bare_id(&short))),
        "shown once: {text}"
    );
    // The footer names held processes once, and again once they change; a task's own result,
    // here too small to show them, counts for nothing.
    // The held set stays the same until the test changes it: two `sleep 300` children, the
    // second one's PID recorded so a separate task can end exactly it. Nothing else in the task
    // starts a process, so the footer's held list cannot churn on its own.
    let (short, short_bash) = scratch("short-held");
    let command = format!("sleep 300 & sleep 300 & echo \"$!\" > '{short_bash}'; echo two");
    let (out, text) = server.ok(json!({"action": "start", "command": command, "keepTree": true, "waitMs": 10000, "maxBytes": 100}));
    assert_eq!(out["keptRunning"], true, "{text}");
    let two = server.id(8);
    wait_for_file(&short);
    // The held list is named once while it stays the same.
    let (_, text) = server.ok(json!({"action": "start", "command": "true"}));
    assert!(
        text.contains(&format!("{} keeps ", bare_id(&two))),
        "{text}"
    );
    let (_, text) = server.ok(json!({"action": "start", "command": "true"}));
    assert!(
        !text.contains(&format!("{} keeps ", bare_id(&two))),
        "named once: {text}"
    );
    // Ending one held process changes the list, so the footer names it again; then it falls
    // quiet. The kill task's own sleeps belong to it, not to the held task.
    let command = format!(
        "pid=$(cat '{short_bash}'); kill \"$pid\"; while kill -0 \"$pid\" 2>/dev/null; do sleep 0.05; done"
    );
    let (_, text) = server.ok(json!({"action": "start", "command": command, "waitMs": 60000}));
    assert!(
        text.contains(&format!("{} keeps ", bare_id(&two))),
        "{text}"
    );
    let (_, text) = server.ok(json!({"action": "start", "command": "true"}));
    assert!(
        !text.contains(&format!("{} keeps ", bare_id(&two))),
        "named once: {text}"
    );
    server.ok(json!({"action": "kill", "taskId": two}));
    server.ok(json!({"action": "kill", "taskId": ready}));
}

/// A heartbeat in a new session through a double fork, as daemonizing servers (adb) start;
/// `program` runs bash's loop under another name. Perl's fork and `setsid` stand in for util-linux
/// `setsid -f`, which macOS does not carry; a failed detach says so instead of running the
/// heartbeat in the task's own session.
#[cfg(all(unix, not(target_os = "macos")))]
fn daemon(program: &str, bash_path: &str) -> String {
    format!(
        "perl -MPOSIX -e 'exit if fork; POSIX::setsid() or die \"setsid: $!\"; exec @ARGV' {program} -c 'while :; do echo x >> {bash_path}; sleep 0.1; done'; sleep 0.5;"
    )
}

/// Linux keeps the stronger assertion: its subreaper follows a process that detaches at once. On
/// macOS the host can only own what it observed first, and `a_detached_descendant_stays_owned_once_observed`
/// proves that half.
#[cfg(all(unix, not(target_os = "macos")))]
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

/// The daemon writes its pid, then waits for a release file before it detaches, so the host
/// reports it — by that pid — while it is still in the task's session: the acknowledgement the
/// macOS guarantee rests on.
#[cfg(target_os = "macos")]
fn daemon_until_released(
    program: &str,
    bash_path: &str,
    mark: &std::path::Path,
    go: &std::path::Path,
) -> String {
    let (mark, go) = (mark.display(), go.display());
    // Both phases are bounded, so a failing run leaves nothing behind: the wait for the go file
    // gives up after a minute, and the heartbeat after a minute of its own.
    format!(
        "perl -MPOSIX -e 'exit if fork; open my $m, \">\", \"{mark}\"; print $m $$; close $m; my $n = 0; while (!-e \"{go}\") {{ exit 3 if ++$n > 6000; select undef, undef, undef, 0.01 }} POSIX::setsid() or die \"setsid: $!\"; exec @ARGV' {program} -c 'for i in $(seq 600); do echo x >> {bash_path}; sleep 0.1; done'; sleep 0.5;"
    )
}

/// The pid the daemon wrote before it blocked.
#[cfg(target_os = "macos")]
fn wait_for_mark(path: &std::path::Path) -> u32 {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(text) = std::fs::read_to_string(path)
            && let Ok(pid) = text.trim().parse()
        {
            return pid;
        }
        assert!(Instant::now() < deadline, "the daemon never wrote its pid");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Waits until the named task itself reports that pid among the processes it holds: the host's
/// membership acknowledgement for that exact process, not a sleep and not another task's report.
#[cfg(target_os = "macos")]
fn wait_for_held_pid(server: &mut Server, task: &str, pid: u32) -> u32 {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (list, text) = server.ok(json!({"action": "list"}));
        for entry in list["tasks"].as_array().into_iter().flatten() {
            if entry["taskId"] != json!(bare_id(task)) {
                continue;
            }
            for member in entry["held"].as_array().into_iter().flatten() {
                if member["pid"] == json!(pid) {
                    return pid;
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "the task never reported the process {pid} as held: {list} {text}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The kernel's own identity for a live pid: the start time libproc reports, in microseconds, the
/// same pair the product compares.
#[cfg(target_os = "macos")]
fn birth_of(pid: u32) -> Option<u64> {
    let info = bsd_info(pid)?;
    Some(info.pbi_start_tvsec * 1_000_000 + info.pbi_start_tvusec)
}

/// What libproc reports about a live process: its parent, its group, and its start time.
#[cfg(target_os = "macos")]
fn bsd_info(pid: u32) -> Option<libc::proc_bsdinfo> {
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    // SAFETY: the buffer is exactly the size given, and libproc writes at most that many bytes.
    let read = unsafe {
        libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDTBSDINFO,
            0,
            &mut info as *mut libc::proc_bsdinfo as *mut libc::c_void,
            size,
        )
    };
    (read >= size).then_some(info)
}

/// The session a live process leads, from `getsid`.
#[cfg(target_os = "macos")]
fn session_of(pid: u32) -> Option<i32> {
    // SAFETY: plain syscall; a process that ended reports none.
    let session = unsafe { libc::getsid(pid as i32) };
    (session != -1).then_some(session)
}

/// The state character `ps` reports, where `T` means stopped.
#[cfg(target_os = "macos")]
fn state_of(pid: u32) -> String {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "stat="])
        .output();
    output
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .unwrap_or_default()
}

/// Waits, bounded, for a probe to answer.
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
fn wait_until<T>(what: &str, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(value) = probe() {
            return value;
        }
        assert!(Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A process a test started outside the tree, killed when the test ends whatever path it takes.
/// The signal goes out only while the birth time that names it is unchanged, so a reused pid is
/// never the target; the helper itself is bounded, so a leaked one ends on its own.
#[cfg(target_os = "macos")]
struct Escaped {
    pid: i32,
    birth: u64,
}

#[cfg(target_os = "macos")]
impl Escaped {
    /// Takes the pid the helper wrote, once the file holds a whole number.
    fn new(pid_file: &std::path::Path) -> Escaped {
        let pid = wait_until("the escape wrote its pid", || {
            std::fs::read_to_string(pid_file)
                .ok()
                .and_then(|text| text.trim().parse::<i32>().ok())
        });
        let birth = birth_of(pid as u32).expect("the escape is a live process");
        Escaped { pid, birth }
    }

    /// Whether the pid still names the process this guard took.
    fn is_mine(&self) -> bool {
        birth_of(self.pid as u32).is_some_and(|birth| birth == self.birth)
    }
}

#[cfg(target_os = "macos")]
impl Drop for Escaped {
    fn drop(&mut self) {
        if self.is_mine() {
            // SAFETY: plain syscall on a pid this guard just re-identified by birth time.
            unsafe { libc::kill(self.pid, libc::SIGKILL) };
        }
    }
}

/// The files one escape test needs, removed with it.
#[cfg(target_os = "macos")]
struct Files {
    dir: PathBuf,
}

#[cfg(target_os = "macos")]
impl Files {
    fn new(beat: &std::path::Path) -> Files {
        let dir = beat.with_extension("files");
        std::fs::create_dir_all(&dir).expect("a directory for the escape's files");
        Files { dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

#[cfg(target_os = "macos")]
impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Keeps a stopped host stopped no longer than the test needs: the signal is resumed on every path
/// out, and only while the pid is still the host this test stopped.
#[cfg(target_os = "macos")]
struct Stopped {
    pid: i32,
    birth: u64,
    resumed: bool,
}

#[cfg(target_os = "macos")]
impl Stopped {
    /// Stops the host and waits until the kernel says it is stopped, before anything relies on it.
    fn new(pid: i32) -> Stopped {
        let birth = birth_of(pid as u32).expect("the host is a live process");
        // The guard exists before the signal goes out, so a panic on any step below still resumes
        // the host on the way out.
        let stopped = Stopped {
            pid,
            birth,
            resumed: false,
        };
        // SAFETY: plain syscall on the pid the root named as its parent.
        assert_eq!(
            unsafe { libc::kill(pid, libc::SIGSTOP) },
            0,
            "the host stopped"
        );
        wait_until("the host is stopped", || {
            state_of(pid as u32).starts_with('T').then_some(())
        });
        stopped
    }

    fn resume(&mut self) {
        if self.resumed {
            return;
        }
        self.resumed = true;
        if birth_of(self.pid as u32).is_some_and(|birth| birth == self.birth) {
            // SAFETY: plain syscall on the pid this guard re-identified by birth time.
            unsafe { libc::kill(self.pid, libc::SIGCONT) };
        }
    }
}

#[cfg(target_os = "macos")]
impl Drop for Stopped {
    fn drop(&mut self) {
        self.resume();
    }
}

/// The documented limit, proven without racing a scan: the root names its parent — the task host —
/// and the test stops that host, waits until the kernel reports it stopped, and only then lets the
/// root create the escapee. The escapee detaches, orphans itself, and the host is resumed into a
/// tree that never held it. The root ends there, so the task reports what it left behind.
#[cfg(target_os = "macos")]
#[test]
fn an_unobserved_escape_is_never_claimed() {
    let (beat, beat_bash) = scratch("escaped-daemon");
    let files = Files::new(&beat);
    let (pid_file, go) = (files.path("pid"), files.path("go"));
    let mut server = Server::start();
    // The helper is bounded: it ends by itself after twenty seconds, whatever a failing run does.
    let command = format!(
        "printf 'host %s\n' \"$PPID\"; while [ ! -e \"{go}\" ]; do sleep 0.02; done; perl -MPOSIX -e 'exit if fork; POSIX::setsid() or die \"setsid: $!\"; exit if fork; open my $p, \">\", \"{pid}\"; print $p $$; close $p; exec @ARGV' bash -c 'for i in $(seq 200); do echo x >> {beat}; sleep 0.1; done'",
        go = go.display(),
        pid = pid_file.display(),
        beat = beat_bash
    );
    let (_, started) = server.ok(json!({
        "action": "start", "command": command, "keepTree": true, "waitMs": 1000
    }));
    let host: i32 = started
        .lines()
        .find_map(|line| line.trim().strip_prefix("host "))
        .and_then(|pid| pid.trim().parse().ok())
        .unwrap_or_else(|| panic!("the root named its parent: {started}"));
    let task = server.id(1);
    // The host cannot scan while it is stopped, so the escapee is created unobserved.
    let mut stopped = Stopped::new(host);
    std::fs::write(&go, b"go").unwrap();
    let escaped = Escaped::new(&pid_file);
    // Detached and orphaned, as the contract describes: its own session and group, and a parent
    // that is neither the root nor the host.
    let info = bsd_info(escaped.pid as u32).expect("the escape runs");
    let host_session = session_of(host as u32).expect("the host is a live process");
    assert_ne!(
        session_of(escaped.pid as u32),
        Some(host_session),
        "the escape left the task's session"
    );
    assert_ne!(info.pbi_pgid as i32, host, "and the task's group");
    assert_ne!(
        info.pbi_ppid as i32, host,
        "its parent is not the task host"
    );
    // Orphaned for real, not merely parented elsewhere: the intermediate setsid parent can still
    // be alive, and a live ancestry trace would be scanned once the host resumes.
    wait_until("the escape is orphaned", || {
        let info = bsd_info(escaped.pid as u32)?;
        let parent = info.pbi_ppid as i32;
        (parent == 1 || birth_of(parent as u32).is_none()).then_some(())
    });
    stopped.resume();
    // The root ends, so the task reports what it left behind.
    wait_until("the root ended", || {
        let (list, _) = server.ok(json!({"action": "list"}));
        let state = list["tasks"][0]["state"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        (state == "exited").then_some(())
    });
    let (list, listing) = server.ok(json!({"action": "list"}));
    let mut claimed = false;
    for entry in list["tasks"].as_array().into_iter().flatten() {
        if entry["taskId"] != json!(bare_id(&task)) {
            continue;
        }
        for field in ["held", "leftovers"] {
            for member in entry[field].as_array().into_iter().flatten() {
                claimed |= member["pid"] == json!(escaped.pid);
            }
        }
    }
    assert!(
        !claimed,
        "a descendant created while the host could not scan is never claimed: {listing}"
    );
    // An explicit kill reaches the tree the task owns, and not the process that left it.
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
    let before = file_len(&beat);
    std::thread::sleep(Duration::from_millis(600));
    assert!(
        file_len(&beat) > before,
        "the escaped descendant outlives the task's kill, which is the documented limit"
    );
}

/// A host stopped by its own tree runs nothing: it cannot answer a list or run a kill. The server
/// still owns it and its number is reserved, so a request wakes it with a signal, and both calls
/// finish instead of waiting on a process that will never run on its own.
#[cfg(target_os = "macos")]
#[test]
fn a_stopped_host_is_woken_for_a_list_and_a_kill() {
    // The list half: a kept tree whose root has exited. Only a kept tree asks the host for its
    // members, so this is the case where a stopped host would block a list.
    let (beat, beat_bash) = scratch("stopped-list");
    let files = Files::new(&beat);
    let (mark, go) = (files.path("mark"), files.path("go"));
    let mut server = Server::start();
    let command = format!(
        "printf 'host %s\n' \"$PPID\"; {} echo started",
        daemon_until_released("bash", &beat_bash, &mark, &go)
    );
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "keepTree": true, "waitMs": 10000
    }));
    assert_eq!(out["state"], "exited", "{text}");
    let host: i32 = text
        .lines()
        .find_map(|line| line.trim().strip_prefix("host "))
        .and_then(|pid| pid.trim().parse().ok())
        .unwrap_or_else(|| panic!("the root named its parent: {text}"));
    let daemon = wait_for_mark(&mark);
    let task = server.id(1);
    // The daemon is the task's own member, observed before the host is stopped.
    wait_for_held_pid(&mut server, &task, daemon);
    std::fs::write(&go, b"go").unwrap();
    wait_for_heartbeat(&beat);
    // The host is stopped from outside: the reserved, unreaped process the server still owns.
    let mut stopped = Stopped::new(host);
    // A list asks the stopped host which processes the task holds: it answers because the request
    // woke it, and the answer names the daemon rather than blocking on a host that never runs.
    let begun = Instant::now();
    let (list, listing) = server.ok(json!({"action": "list"}));
    assert!(
        begun.elapsed() < Duration::from_secs(10),
        "the list waited on a stopped host: {listing}"
    );
    let held = list["tasks"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|entry| entry["held"].as_array().into_iter().flatten())
        .filter_map(|member| member["pid"].as_i64())
        .collect::<Vec<_>>();
    assert!(
        held.contains(&(daemon as i64)),
        "the list names the held daemon {daemon}: {listing}"
    );
    stopped.resume();
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 10000}));

    // The kill half: a root that stops its own group, so nothing in the tree runs until the server
    // wakes the host for the kill.
    let mut server = Server::start();
    server.ok(json!({
        "action": "start", "command": "kill -STOP 0; sleep 60", "keepTree": true, "waitMs": 500
    }));
    let task = server.id(1);
    let begun = Instant::now();
    let (_, text) = server.ok(json!({"action": "kill", "taskId": task, "waitMs": 20000}));
    assert!(
        begun.elapsed() < Duration::from_secs(15),
        "the kill waited on a stopped host: {text}"
    );
    let (list, listing) = server.ok(json!({"action": "list"}));
    assert!(
        list["tasks"][0].get("held").is_none(),
        "the stopped tree ended: {listing}"
    );
}

/// Losing only the host does not end its children, before or after the root's exit.
#[cfg(target_os = "macos")]
#[test]
fn a_crashed_host_leaves_cleanup_unconfirmed() {
    for root_exited in [false, true] {
        let (beat, beat_bash) = scratch("crashed-host");
        let files = Files::new(&beat);
        let pid_file = files.path("pid");
        let root_pid_file = files.path("root-pid");
        let host_pid_file = files.path("host-pid");
        let command = format!(
            "printf '%s' \"$$\" > '{}'; printf '%s' \"$PPID\" > '{}'; (for i in $(seq 200); do echo x >> '{}'; sleep 0.1; done) & printf '%s' \"$!\" > '{}'; {}",
            root_pid_file.display(),
            host_pid_file.display(),
            beat_bash,
            pid_file.display(),
            if root_exited { "echo started" } else { "wait" }
        );
        let mut server = Server::start();
        server.ok(json!({
            "action": "start", "command": command, "keepTree": true, "waitMs": 1000,
        }));
        let host = wait_for_mark(&host_pid_file) as i32;
        let child = Escaped::new(&pid_file);
        let _root = (!root_exited).then(|| Escaped::new(&root_pid_file));
        wait_for_heartbeat(&beat);
        let task = server.id(1);
        if root_exited {
            wait_for_held_pid(&mut server, &task, child.pid as u32);
        }
        // The server stays alive; SIGKILL reaches only its task host.
        assert_eq!(unsafe { libc::kill(host, libc::SIGKILL) }, 0);
        wait_until("the lost host is reported", || {
            let (list, _) = server.ok(json!({"action": "list"}));
            (list["tasks"][0]["killError"].is_string() && list["tasks"][0]["keptRunning"] == true)
                .then_some(())
        });
        for _ in 0..2 {
            let (_, text, is_error) = server.call(json!({"action": "kill", "taskId": task}));
            assert!(
                is_error,
                "a disconnected host cannot confirm cleanup: {text}"
            );
            let (list, text) = server.ok(json!({"action": "list"}));
            assert_eq!(list["tasks"][0]["keptRunning"], true, "{text}");
            assert!(list["tasks"][0]["killError"].is_string(), "{text}");
        }
        let before = file_len(&beat);
        wait_until("the child still runs after host loss", || {
            (file_len(&beat) > before).then_some(())
        });
        // Escaped rechecks the child's birth before killing it on every exit path; the loop is
        // bounded too, so failed setup cannot leave a permanent writer.
        drop(child);
    }
}

/// A descendant the host observed stays in the tree after it leaves the session, the group, and
/// its parent chain, and a kill reaches it.
#[cfg(target_os = "macos")]
#[test]
fn a_detached_descendant_stays_owned_once_observed() {
    let (beat, beat_bash) = scratch("observed-daemon");
    let files = Files::new(&beat);
    let (mark, go) = (files.path("mark"), files.path("go"));
    let mut server = Server::start();
    let command = format!(
        "{} echo started",
        daemon_until_released("bash", &beat_bash, &mark, &go)
    );
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "keepTree": true, "waitMs": 10000
    }));
    assert_eq!(out["state"], "exited", "{text}");
    let pid = wait_for_mark(&mark);
    let task = server.id(1);
    // The acknowledgement names this process, under this task, before it detaches.
    wait_for_held_pid(&mut server, &task, pid);
    std::fs::write(&go, b"go").unwrap();
    wait_for_heartbeat(&beat);
    // It has detached by now, and is still the task's: the explicit kill reaches it.
    std::thread::sleep(Duration::from_millis(300));
    let (list, listing) = server.ok(json!({"action": "list"}));
    let held = list["tasks"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|entry| entry["taskId"] == json!(bare_id(&task)))
        .flat_map(|entry| entry["held"].as_array().into_iter().flatten())
        .filter_map(|member| member["pid"].as_i64())
        .collect::<Vec<_>>();
    assert!(
        held.contains(&(pid as i64)),
        "the detached descendant is still held: {listing}"
    );
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
    assert_stops_growing(&beat);
}

/// Server death ends the trees the host holds, a detached descendant included once it was seen.
#[cfg(target_os = "macos")]
#[test]
fn a_killed_server_takes_its_task_trees_with_it() {
    let files = ["orphaned", "orphaned-daemon"].map(scratch);
    let [(beat, beat_bash), (daemon_beat, daemon_bash)] = &files;
    let daemon_files = Files::new(daemon_beat);
    let (mark, go) = (daemon_files.path("mark"), daemon_files.path("go"));
    let mut server = Server::start();
    let command = format!("{} sleep 300", heartbeat(beat_bash));
    server.ok(json!({"action": "start", "command": command, "keepTree": true, "waitMs": 1000}));
    let command = format!(
        "{} echo started",
        daemon_until_released("bash", daemon_bash, &mark, &go)
    );
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "keepTree": true, "pty": true, "waitMs": 10000
    }));
    assert_eq!(out["keptRunning"], true, "{text}");
    let daemon = wait_for_mark(&mark);
    let second = server.id(2);
    wait_for_held_pid(&mut server, &second, daemon);
    std::fs::write(&go, b"go").unwrap();
    wait_for_heartbeat(daemon_beat);
    // SIGKILL: no shutdown code runs, so only the hosts' lifelines can end the trees.
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    for beat in [beat, daemon_beat] {
        assert!(file_len(beat) > 0, "a heartbeat never started");
        assert_stops_growing(beat);
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
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

/// A process the server may not signal — one sudo runs as root — is a member the host can see and
/// must not end: the kill fails, the task says so, and the tree stays held, as on Linux. The host
/// reads it from the kernel's own table, because libproc answers EPERM for another user's process.
#[cfg(target_os = "macos")]
#[test]
fn a_sudo_child_the_server_may_not_kill_is_held_and_reported() {
    let required = std::env::var("FASTEXEC_REQUIRE_ROOT_TEST").is_ok_and(|value| value == "1");
    let sudo = Command::new("sudo").args(["-n", "true"]).status();
    if !matches!(sudo, Ok(status) if status.success()) {
        assert!(
            !required,
            "FASTEXEC_REQUIRE_ROOT_TEST=1, and a sudo child needs passwordless sudo"
        );
        eprintln!("skipped: a sudo child needs passwordless sudo");
        return;
    }
    let (beat, beat_bash) = scratch("sudo-daemon");
    let files = Files::new(&beat);
    let pid_file = files.path("pid");
    let mut server = Server::start();
    // The daemon runs as root, so this user cannot signal it; it writes its own pid first, and it
    // is bounded, so a failing run leaves nothing behind for long.
    let command = format!(
        "sudo -n bash -c 'echo $$ > {pid}; for i in $(seq 600); do echo x >> {beat_bash}; sleep 0.1; done' & echo started",
        pid = pid_file.display()
    );
    server.ok(json!({
        "action": "start", "command": command, "keepTree": true, "waitMs": 2000
    }));
    wait_for_heartbeat(&beat);
    let rooted = Rooted::new(&pid_file);
    let task = server.id(1);
    // The kill fails and the tree stays held, as on Linux.
    let (_, text, is_error) =
        server.call(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
    assert!(
        is_error && text.contains("Cannot terminate"),
        "a member this user may not signal fails the kill: {text}"
    );
    // The refusal is reported, and the task still holds that exact root process.
    let (list, listing) = server.ok(json!({"action": "list"}));
    let entry = list["tasks"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|entry| entry["taskId"] == json!(bare_id(&task)))
        .unwrap_or_else(|| panic!("the task is listed: {listing}"));
    assert_eq!(
        entry["keptRunning"],
        json!(true),
        "the task keeps the tree it could not end: {listing}"
    );
    assert!(
        entry["killError"]
            .as_str()
            .is_some_and(|error| !error.is_empty()),
        "the refusal is reported: {listing}"
    );
    // Held only: `leftovers` is cached from the root's exit, so an old pid there would satisfy the
    // assertion even after membership lost it.
    let held = entry["held"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|member| member["pid"].as_i64())
        .collect::<Vec<_>>();
    assert!(
        held.contains(&(rooted.pid as i64)),
        "the held tree names the root process {}: {listing}",
        rooted.pid
    );
    let before = file_len(&beat);
    std::thread::sleep(Duration::from_millis(600));
    assert!(
        file_len(&beat) > before,
        "the daemon the server may not signal keeps running"
    );
}

/// A root process a test started, ended when the test ends: the pid it wrote itself and the birth
/// time the kernel reports for it, read through `sysctl`, which answers for every uid. The signal
/// goes out only while that birth time is unchanged, so a reused root pid is never the target; the
/// helper is bounded as well, so a skipped guard still ends.
#[cfg(target_os = "macos")]
struct Rooted {
    pid: i32,
    birth: u64,
}

#[cfg(target_os = "macos")]
impl Rooted {
    fn new(pid_file: &std::path::Path) -> Rooted {
        let pid = wait_until("the root daemon wrote its pid", || {
            std::fs::read_to_string(pid_file)
                .ok()
                .and_then(|text| text.trim().parse::<i32>().ok())
        });
        let birth = native_birth(pid).expect("the kernel describes a root process");
        Rooted { pid, birth }
    }

    fn is_mine(&self) -> bool {
        native_birth(self.pid).is_some_and(|birth| birth == self.birth)
    }
}

#[cfg(target_os = "macos")]
impl Drop for Rooted {
    fn drop(&mut self) {
        if self.is_mine() {
            let _ = Command::new("sudo")
                .args(["-n", "kill", "-9", &self.pid.to_string()])
                .status();
        }
    }
}

/// The start time the kernel reports for a pid, in microseconds, read from the `KERN_PROC_PID` row
/// — the one source that answers for another uid's process.
#[cfg(target_os = "macos")]
fn native_birth(pid: i32) -> Option<u64> {
    let mut buffer = [0_u8; 648];
    let mut size = buffer.len();
    let mut mib = [
        libc::CTL_KERN,
        libc::KERN_PROC,
        libc::KERN_PROC_PID,
        pid as libc::c_int,
    ];
    // SAFETY: the mib is four integers and the buffer is as long as the size passed.
    let result = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            mib.len() as libc::c_uint,
            buffer.as_mut_ptr() as *mut libc::c_void,
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if result != 0 || size != buffer.len() {
        return None;
    }
    let seconds = u64::from_le_bytes(buffer[0..8].try_into().ok()?);
    let micros = u32::from_le_bytes(buffer[8..12].try_into().ok()?) as u64;
    Some(seconds * 1_000_000 + micros)
}

/// A kept writer must not make terminal draining delay the root's independent exit status.
#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_a_continuous_pty_writer_does_not_delay_the_kept_roots_status() {
    let mut server = Server::start();
    let (out, text) = server.ok(json!({
        "action": "start", "pty": true, "keepTree": true, "loginShell": false,
        "waitMs": 3000, "killAfterMs": 15000, "maxBytes": 1024,
        "command": "(end=$((SECONDS + 20)); while (( SECONDS < end )); do printf 'background-output\\n'; done) & printf 'root-ended\\n'; exit 23",
    }));
    assert_eq!(out["state"], "exited", "{text}");
    assert_eq!(out["exitCode"], 23, "{text}");
    assert_eq!(out["keptRunning"], true, "{text}");
    let (out, text) = server.ok(json!({
        "action": "kill", "taskId": out["taskId"], "waitMs": 5000,
    }));
    assert_eq!(out["exitCode"], 23, "{text}");
    assert!(out.get("keptRunning").is_none(), "{text}");
    assert!(out.get("held").is_none(), "{text}");
    assert!(out.get("killError").is_none(), "{text}");
}

/// A host killed externally no longer has a reaper, but its unreaped PID still reserves its group.
#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_a_dead_host_still_cleans_up_its_reserved_process_group() {
    let (beat, beat_bash) = scratch("dead-freebsd-host");
    let mut server = Server::start();
    let command = format!(
        "(for i in {{1..200}}; do printf x >> '{beat_bash}'; sleep 0.1; done) & while [ ! -s '{beat_bash}' ]; do sleep 0.01; done; read -r gate; kill -KILL \"$PPID\"; sleep 20"
    );
    let (out, _) = server.ok(json!({
        "action": "start", "eof": false, "command": command, "waitMs": 0,
    }));
    // Start has observed READY before the command is allowed to kill its host.
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": out["taskId"], "input": "kill\n", "waitMs": 10000,
    }));
    assert_eq!(out["exitCode"], 137, "{text}");
    assert_stops_growing(&beat);
}

/// A trap in bash must survive the process-group signal without losing its task reaper.
#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_realtime_group_signals_preserve_the_host_and_tree_cleanup() {
    for signal in [65, 126] {
        let (beat, beat_bash) = scratch(&format!("realtime-{signal}"));
        let mut server = Server::start();
        let command = format!(
            "trap ':' {signal}; perl -MPOSIX -e 'exit if fork; POSIX::setsid() or die; exec @ARGV' /bin/sh -c 'for i in $(seq 300); do printf x >> \"{beat_bash}\"; sleep 0.1; done'; while [ ! -s '{beat_bash}' ]; do sleep 0.01; done; kill -{signal} 0; printf 'survived\\n'"
        );
        let (out, text) = server.ok(json!({
            "action": "start", "command": command, "waitMs": 10000,
        }));
        assert_eq!(out["exitCode"], 0, "signal {signal}: {text}");
        assert!(text.contains("survived"), "{text}");
        assert_stops_growing(&beat);
    }
}

/// FreeBSD's descendant query stops at nested reapers; both their membership and kill must recurse.
#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_nested_reapers_keep_detached_descendants_owned() {
    let dir = Scratch::new("nested-reaper");
    std::fs::create_dir(&*dir).unwrap();
    let source = dir.join("reaper.c");
    let fixture = dir.join("reaper");
    std::fs::write(&source, include_str!("fixtures/freebsd-reaper.c")).unwrap();
    assert!(
        Command::new("cc")
            .arg(&source)
            .arg("-o")
            .arg(&fixture)
            .status()
            .unwrap()
            .success(),
        "the nested reaper fixture did not compile"
    );
    for handover in [false, true] {
        let beat = dir.join(format!("beat-{handover}"));
        let mark = dir.join(format!("pids-{handover}"));
        let release = dir.join("release");
        let release_arg = if handover {
            format!(" '{}'", release.display())
        } else {
            String::new()
        };
        let mut server = Server::start();
        let (out, text) = server.ok(json!({
            "action": "start", "keepTree": true, "waitMs": 10000,
            "command": format!("'{}' '{}' '{}'{release_arg} & echo started",
                fixture.display(), beat.display(), mark.display()),
        }));
        assert_eq!(out["keptRunning"], true, "{text}");
        wait_for_heartbeat(&beat);
        let pids: Vec<u64> = std::fs::read_to_string(mark)
            .unwrap()
            .split_whitespace()
            .map(|pid| pid.parse().unwrap())
            .collect();
        assert_eq!(pids.len(), 2);
        let (list, text) = server.ok(json!({"action": "list"}));
        let held = list["tasks"][0]["held"].as_array().unwrap();
        for &pid in &pids {
            assert!(
                held.iter().any(|member| member["pid"] == pid),
                "the nested reaper member {pid} was lost: {text}"
            );
        }
        if handover {
            std::fs::write(release, b"exit").unwrap();
            wait_until(
                "the nested reaper naturally hands its child to the host",
                || {
                    let (list, _) = server.ok(json!({"action": "list"}));
                    let held = list["tasks"][0]["held"].as_array().unwrap();
                    (list["tasks"][0]["keptRunning"] == true
                        && held.len() == 1
                        && held[0]["pid"] == pids[1])
                        .then_some(())
                },
            );
            let before = file_len(&beat);
            wait_until("the handed-over child still writes", || {
                (file_len(&beat) > before).then_some(())
            });
        }
        server.ok(json!({"action": "kill", "taskId": server.id(1), "waitMs": 5000}));
        assert_stops_growing(&beat);
    }
}

/// A process the server may not signal, as one sudo runs as root, stays held and reported. The
/// fixture needs root and a C compiler; CI requires both on Linux and FreeBSD.
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
#[test]
fn processes_the_server_may_not_kill_stay_held_and_reported() {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::os::unix::process::CommandExt;
    // CI and fx-test set this, so the case cannot pass by skipping when a prerequisite is missing.
    let required = std::env::var("FASTEXEC_REQUIRE_ROOT_TEST").is_ok_and(|value| value == "1");
    // SAFETY: plain syscall.
    if unsafe { libc::geteuid() } != 0 {
        assert!(
            !required,
            "FASTEXEC_REQUIRE_ROOT_TEST=1, and a setuid-root fixture needs root"
        );
        eprintln!("skipped: a setuid-root fixture needs root");
        return;
    }
    // The setuid binary may only touch this instance's root-owned directory. Create it
    // exclusively, keep it private while compiling, and bind the helper to its original inode.
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).unwrap();
    let nonce: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
    let path = std::env::temp_dir().join(format!("fastexec-root-fixture-{nonce}"));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&path)
        .unwrap();
    let mut scratch = Scratch(path);
    scratch.0 = scratch.canonicalize().unwrap();
    let dir = scratch.join("instance");
    std::fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
    let metadata = std::fs::symlink_metadata(&dir).unwrap();
    assert!(metadata.is_dir());
    assert_eq!(metadata.uid(), 0);
    assert_eq!(metadata.mode() & 0o777, 0o700);
    #[cfg(target_os = "freebsd")]
    support::create_private_runtime(&dir);

    let beats = [dir.join("exit"), dir.join("kill")];
    let stops = [dir.join("exit.stop"), dir.join("kill.stop")];
    // Every byte gets its own escape, for both C literals and Bash's ANSI-C quoting.
    let escaped = |path: &std::path::Path| -> String {
        path.as_os_str()
            .as_bytes()
            .iter()
            .map(|byte| format!("\\x{byte:02x}"))
            .collect()
    };
    let source = dir.join("daemon.c");
    let declarations = format!(
        "#define _GNU_SOURCE\n\
         #include <errno.h>\n\
         #include <fcntl.h>\n\
         #include <stdint.h>\n\
         #include <stdio.h>\n\
         #include <string.h>\n\
         #include <sys/stat.h>\n\
         #include <unistd.h>\n\
         static const char directory[] = \"{}\";\n\
         static const uintmax_t directory_device = UINTMAX_C({});\n\
         static const uintmax_t directory_inode = UINTMAX_C({});\n\
         static const char *beats[] = {{\"exit\", \"kill\"}};\n\
         static const char *stops[] = {{\"exit.stop\", \"kill.stop\"}};\n",
        escaped(&dir),
        metadata.dev(),
        metadata.ino()
    );
    std::fs::write(
        &source,
        declarations
            // It ends by itself after 60 s, so a failing run leaves no immortal root process.
            + r#"static int open_directory(void) {
  int fd = open(directory, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
  if (fd < 0) { perror("fixture directory"); return -1; }
  struct stat st;
  if (fstat(fd, &st) != 0) { perror("fixture directory"); close(fd); return -1; }
  if (!S_ISDIR(st.st_mode) || st.st_uid != 0 || (st.st_mode & 07777) != 0755 ||
      (uintmax_t)st.st_dev != directory_device || (uintmax_t)st.st_ino != directory_inode) {
    fprintf(stderr, "fixture directory identity or permissions changed\n");
    close(fd);
    return -1;
  }
  return fd;
}
static int stopped(int directory_fd, const char *name) {
  struct stat st;
  if (fstatat(directory_fd, name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
    if (errno == ENOENT) return 0;
    perror("fixture stop");
    return -1;
  }
  if (!S_ISREG(st.st_mode) || st.st_uid != 0 || st.st_nlink != 1 ||
      (st.st_mode & 07777) != 0600) {
    fprintf(stderr, "fixture stop identity or permissions changed\n");
    return -1;
  }
  return 1;
}
static int heartbeat(int fd, int pid, int parent) {
  if (dprintf(fd, "%d %d\n", pid, parent) < 0) { perror("fixture heartbeat"); return 1; }
  return 0;
}
int main(int argc, char **argv) {
  if (argc != 2) return 2;
  int which;
  if (strcmp(argv[1], "--check-root") == 0) which = -1;
  else if (strcmp(argv[1], "exit") == 0) which = 0;
  else if (strcmp(argv[1], "kill") == 0) which = 1;
  else return 2;
  if (geteuid() != 0) {
    fprintf(stderr, "setuid-root fixture: uid=%u euid=%u; check TMPDIR mount flags and inherited privilege restrictions\n",
            (unsigned)getuid(), (unsigned)geteuid());
    return 1;
  }
  int directory_fd = open_directory();
  if (directory_fd < 0) return 1;
  if (setresuid(0, 0, 0) != 0) { perror("fixture setresuid"); return 1; }
  if (which < 0) return close(directory_fd) != 0;
  umask(077);
  int fd = openat(directory_fd, beats[which],
                  O_WRONLY | O_CREAT | O_EXCL | O_APPEND | O_NOFOLLOW | O_CLOEXEC, 0600);
  if (fd < 0) { perror("fixture heartbeat"); return 1; }
  for (int i = 0; i < 600; i++) {
    int stop = stopped(directory_fd, stops[which]);
    if (stop < 0) return 1;
    if (stop > 0) break;
    if (heartbeat(fd, (int)getpid(), (int)getppid()) != 0) return 1;
    usleep(100000);
  }
  int failed = heartbeat(fd, 0, 0);
  if (close(fd) != 0 || close(directory_fd) != 0) failed = 1;
  return failed;
}
"#,
    )
    .unwrap();
    std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o600)).unwrap();
    let fixture = dir.join("root-daemon");
    match Command::new("cc")
        .arg(&source)
        .arg("-o")
        .arg(&fixture)
        .status()
    {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            assert!(
                !required,
                "FASTEXEC_REQUIRE_ROOT_TEST=1, and the fixture needs a C compiler"
            );
            eprintln!("skipped: no C compiler");
            return;
        }
        built => assert!(built.unwrap().success(), "the fixture did not compile"),
    }
    std::fs::set_permissions(&fixture, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::set_permissions(&*dir, std::fs::Permissions::from_mode(0o755)).unwrap();

    // The private outer directory keeps these ordinary-binary checks under one identity.
    let check_plain = |executable: &std::path::Path, argument: &str| {
        Command::new(executable)
            .arg(argument)
            .stdin(Stdio::null())
            .output()
            .unwrap()
    };
    assert!(check_plain(&fixture, "--check-root").status.success());
    std::fs::set_permissions(&*dir, std::fs::Permissions::from_mode(0o775)).unwrap();
    let rejected = check_plain(&fixture, "--check-root");
    assert_eq!(rejected.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("fixture directory"));
    std::fs::set_permissions(&*dir, std::fs::Permissions::from_mode(0o755)).unwrap();

    let moved = Scratch(dir.with_extension("original"));
    std::fs::rename(&*dir, &*moved).unwrap();
    std::fs::DirBuilder::new()
        .mode(0o755)
        .create(&*dir)
        .unwrap();
    let plain = moved.join("root-daemon");
    assert_eq!(std::fs::metadata(&plain).unwrap().mode() & 0o7777, 0o755);
    assert_ne!(std::fs::metadata(&*dir).unwrap().ino(), metadata.ino());
    let rejected = check_plain(&plain, "exit");
    assert_eq!(rejected.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("fixture directory"));
    assert!(beats.iter().chain(&stops).all(|path| !path.exists()));
    std::fs::remove_dir(&*dir).unwrap();
    std::os::unix::fs::symlink(&*moved, &*dir).unwrap();
    let rejected = check_plain(&plain, "exit");
    assert_eq!(rejected.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("fixture directory"));
    assert!(!moved.join("exit").exists());
    std::fs::remove_file(&*dir).unwrap();
    std::fs::rename(&*moved, &*dir).unwrap();

    std::fs::write(&beats[0], b"original").unwrap();
    assert_eq!(check_plain(&fixture, "exit").status.code(), Some(1));
    assert_eq!(std::fs::read(&beats[0]).unwrap(), b"original");
    std::fs::remove_file(&beats[0]).unwrap();
    std::fs::set_permissions(&*scratch, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::set_permissions(&fixture, std::fs::Permissions::from_mode(0o4755)).unwrap();
    let check = Command::new(&fixture)
        .arg("--check-root")
        .uid(65534)
        .gid(65534)
        .current_dir(&*dir)
        .stdin(Stdio::null())
        .output()
        .expect("run the setuid fixture as uid 65534");
    assert!(
        check.status.success(),
        "the fixture must become root before testing protected descendants ({}): {}",
        dir.display(),
        String::from_utf8_lossy(&check.stderr)
    );
    // Invalid arguments never select a file, even though the executable becomes root.
    let sentinel = dir.join("not-a-fixture-path");
    std::fs::write(&sentinel, b"original").unwrap();
    std::fs::set_permissions(&sentinel, std::fs::Permissions::from_mode(0o600)).unwrap();
    for arguments in [
        vec![sentinel.as_os_str()],
        vec![std::ffi::OsStr::new("exit"), sentinel.as_os_str()],
        vec![beats[0].as_os_str(), stops[1].as_os_str()],
        vec![std::ffi::OsStr::new("--check-root"), sentinel.as_os_str()],
    ] {
        let rejected = Command::new(&fixture)
            .args(arguments)
            .uid(65534)
            .gid(65534)
            .current_dir(&*dir)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(rejected.status.code(), Some(2));
        assert_eq!(std::fs::read(&sentinel).unwrap(), b"original");
        assert!(beats.iter().chain(&stops).all(|path| !path.exists()));
    }
    let fixture = format!("$'{}'", escaped(&fixture));
    // Copy fastexec into the 0755 scratch directory so uid 65534 can execute it even when
    // the repository checkout resides under a mode-0700 home directory such as /root.
    let unprivileged_bin = scratch.join("fastexec-bin");
    std::fs::copy(env!("CARGO_BIN_EXE_fastexec"), &unprivileged_bin).unwrap();
    std::fs::set_permissions(&unprivileged_bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut server = Server::start_bin(&unprivileged_bin, |command| {
        command
            .uid(65534)
            .gid(65534)
            .current_dir(&*dir)
            .env("HOME", &*dir);
    });
    // Stop through private files, never old numeric PIDs: the bounded fixture may already
    // have exited and released its PID on a slow machine. This guard also runs after a panic.
    struct StopFixtures {
        beats: [PathBuf; 2],
        stops: [PathBuf; 2],
    }
    impl Drop for StopFixtures {
        fn drop(&mut self) {
            for path in &self.stops {
                let _ = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(path)
                    .and_then(|mut file| file.write_all(b"stop"));
            }
            let deadline = Instant::now() + Duration::from_secs(5);
            while self.beats.iter().any(|path| {
                path.exists()
                    && !std::fs::read_to_string(path).is_ok_and(|text| text.ends_with("0 0\n"))
            }) && Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
    let stopping = StopFixtures {
        beats: beats.clone(),
        stops,
    };
    let identity = |beat: &PathBuf| -> (i64, i64) {
        let text = std::fs::read_to_string(beat).unwrap();
        let (pid, parent) = text.lines().last().unwrap().split_once(' ').unwrap();
        (pid.parse().unwrap(), parent.parse().unwrap())
    };

    // At the root's exit. The heartbeat is written only after becoming root: a fixed delay
    // could let bash end while its scheduled child was still an unprivileged exec candidate.
    let command = format!("{fixture} exit & read -r ready; echo started");
    server.ok(json!({"action": "start", "eof": false, "command": command, "waitMs": 0}));
    wait_for_heartbeat(&beats[0]);
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "input": "ready\n", "waitMs": 10000
    }));
    assert_eq!(out["keptRunning"], true, "{text}");
    assert!(
        text.contains("could not end root-daemon (") && text.contains("kill retries"),
        "{text}"
    );
    assert!(out["killError"].is_string(), "{out}");
    assert!(
        out["leftovers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|member| member["pid"] == identity(&beats[0]).0),
        "the root fixture is reported among the protected descendants: {out}"
    );
    let (_, text, is_error) = server.call(json!({"action": "kill", "taskId": server.id(1)}));
    assert!(is_error && text.contains("Cannot terminate"), "{text}");

    // After a kill of the running root.
    let command = format!("{fixture} kill & sleep 300");
    server.ok(json!({"action": "start", "command": command, "waitMs": 0}));
    wait_for_heartbeat(&beats[1]);
    let (_, text, is_error) = server.call(json!({"action": "kill", "taskId": server.id(2)}));
    assert!(is_error && text.contains("Cannot terminate"), "{text}");
    let (out, text) = server.ok(json!({"action": "poll", "taskId": server.id(2), "waitMs": 10000}));
    assert_eq!(out["state"], "killed", "{text}");
    assert_eq!(out["keptRunning"], true, "{text}");
    assert!(text.contains("could not end root-daemon ("), "{text}");
    assert!(
        out["leftovers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|member| member["pid"] == identity(&beats[1]).0),
        "the root fixture is reported among the protected descendants: {out}"
    );

    for beat in &beats {
        let before = file_len(beat);
        std::thread::sleep(Duration::from_millis(500));
        assert!(file_len(beat) > before, "the fixture should still run");
        let metadata = std::fs::metadata(beat).unwrap();
        assert_eq!(metadata.uid(), 0);
        assert_eq!(metadata.mode() & 0o777, 0o600);
    }
    let parents: Vec<_> = beats.iter().map(|beat| identity(beat).1).collect();
    assert!(parents.iter().all(|pid| *pid > 1));
    // A dead server cannot retry a denied kill. Its hosts must retain their reaper ownership
    // until the privileged descendants end, instead of abandoning them to the system init.
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    std::thread::sleep(Duration::from_millis(500));
    for (beat, host) in beats.iter().zip(&parents) {
        assert_eq!(
            identity(beat).1,
            *host,
            "fixture {beat:?} lost its task host"
        );
    }
    drop(stopping);
    for beat in &beats {
        assert_eq!(identity(beat), (0, 0), "the fixture acknowledged its stop");
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
    let poll = json!({"name": "exec", "arguments": {"action": "poll", "taskId": server.id(1), "waitMs": 60000}});
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
    server.ok(json!({"action": "start", "eof": false, "command": "read -r x; echo final-$x", "waitMs": 0}));
    for _ in 0..65 {
        server.ok(json!({"action": "start", "command": "true"}));
    }
    let (out, text) = server
        .ok(json!({"action": "poll", "taskId": server.id(1), "input": "ok\n", "waitMs": 10000}));
    assert_eq!(out["state"], "exited");
    assert!(text.starts_with("final-ok\n"), "{text}");
    let (out, _) = server.ok(json!({"action": "list", "limit": 100}));
    let tasks = out["tasks"].as_array().unwrap();
    assert_eq!(tasks.len(), 64, "{out}");
    let kept = |id: &str| tasks.iter().any(|task| task["taskId"] == bare_id(id));
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
    assert!(
        body_of(&text).len() <= 1024,
        "{} bytes: {text}",
        body_of(&text).len()
    );
    assert!(text.contains("Warning:"), "{text}");
    drop(long);

    let mut server = Server::start();
    let (out, text) =
        server.ok(json!({"action": "start", "command": "seq 1 5000", "maxBytes": 2048}));
    assert!(
        body_of(&text).len() <= 2048,
        "{} bytes",
        body_of(&text).len()
    );
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
        server.ok(json!({"action": "start", "command": "seq 1 5000", "maxBytes": 65536}));
    assert!(out.get("omittedLines").is_none());
    assert_eq!(
        text.lines()
            .filter(|line| line.parse::<u32>().is_ok())
            .count(),
        5000
    );

    // No fixed per-line cap: 3000-byte line fits default 16384 whole; 70000-byte line cuts at
    // 16384 by default and stays whole when maxBytes is raised above it.
    let line = |n: u32| format!("printf '%*s\\n' {n} '' | tr ' ' x");
    let (out, text) = server.ok(json!({"action": "start", "command": line(3000)}));
    assert_eq!((body_of(&text).len(), out.get("cutLines")), (3000, None));
    let (out, text) = server.ok(json!({"action": "start", "command": line(70000)}));
    assert_eq!(out["cutLines"], 1);
    assert!(text.contains("line cut at 16 KiB;"), "{text}");
    assert!(body_of(&text).len() <= 16384, "{text}");
    let (out, text) = server
        .ok(json!({"action": "start", "command": line(70000), "maxBytes": 100000, "raw": true}));
    assert_eq!((body_of(&text).len(), out.get("cutLines")), (70000, None));

    // Small body budgets still preserve status and background footer outside maxBytes.
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
            assert!(
                body_of(&text).len() <= max,
                "{} bytes: {text}",
                body_of(&text).len()
            );
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
        let (out, _) = server.ok(json!({"action": "start", "eof": false, "command": command, "loginShell": false, "waitMs": 0, "maxBytes": 1024}));
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
    let home = Scratch::new("server-path-home");
    std::fs::create_dir_all(&*home).unwrap();
    #[cfg(unix)]
    {
        // Debian's /etc/profile overwrites PATH before sourcing ~/.bash_profile; exporting the
        // probe directory from ~/.bash_profile exercises the session's login-environment snapshot.
        std::fs::write(
            home.join(".bash_profile"),
            format!("export PATH=\"{}:$PATH\"\n", dir.display()),
        )
        .unwrap();
    }
    // Git Bash's login profile builds PATH from ORIGINAL_PATH when a Git Bash parent set it.
    let mut server = Server::start_with(|command| {
        command
            .env("HOME", &*home)
            .env("USERPROFILE", &*home)
            .env("PATH", &path)
            .env("FASTEXEC_HOST_ENV_CHECK", "HOST_OK")
            .env("MSYS2_PATH_TYPE", "inherit")
            .env_remove("ORIGINAL_PATH");
    });
    for pty in [false, true] {
        let (out, text) = server.ok(json!({
            "action": "start",
            "command": "printf '%s:' \"$FASTEXEC_HOST_ENV_CHECK\"; fastexec_env_probe",
            "pty": pty,
            "waitMs": 15000
        }));
        assert_eq!(out["exitCode"], 0, "pty={pty}: {text}");
        assert!(text.contains("HOST_OK:ENV_PATH_OK"), "pty={pty}: {text}");
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
        let (out, _) = server.ok(json!({"action": "start", "eof": false, "command": command, "encoding": encoding, "loginShell": false, "waitMs": 0}));
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
    for length in [7_999, 8_000, 8_001, 9_000, 131_073, 900_000] {
        let long = format!("x='{}'; echo ${{#x}}", "a".repeat(length));
        let (_, text) = server.ok(json!({"action": "start", "command": long}));
        assert!(text.starts_with(&format!("{length}\n")), "{text}");
    }
}

#[test]
fn nul_commands_are_rejected_before_short_or_script_execution() {
    let mut server = Server::start();
    for pty in [false, true] {
        for length in [512, 8_000, 8_001, 131_073] {
            let (marker, marker_bash) = scratch(&format!("nul-command-{pty}-{length}"));
            let prefix = format!("printf ran > '{marker_bash}'; printf 'A\0B';\n#");
            let command = format!("{prefix}{}", "x".repeat(length - prefix.len()));
            assert_eq!(command.len(), length);
            let (out, text, is_error) = server.call(json!({
                "action": "start", "command": command, "pty": pty, "loginShell": false
            }));
            assert!(is_error, "{text}");
            assert_eq!(out["ok"], false, "{text}");
            assert!(text.contains("NUL byte"), "{text}");
            assert!(!marker.exists(), "an invalid command ran before rejection");
        }
    }
    let (out, _) = server.ok(json!({"action": "list"}));
    assert_eq!(out["tasks"], json!([]), "{out}");
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
            json!({"action": "start", "command": "true", "maxBytes": 4194305}),
            "out of range",
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
            json!({"action": "start", "command": "true", "returnWhen": {}}),
            "returnWhen needs",
        ),
        (
            json!({"action": "start", "command": "true", "returnWhen": {"outputContains": ["a\nb"]}}),
            "without line breaks (outputContains matches within one output line; to avoid matching an echoed command, print a marker",
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
        json!({"name": "exec", "arguments": {"action": "list", "bogus": 1}}),
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
    let call = json!({"name": "exec", "arguments": {"action": "poll", "taskId": server.id(1), "waitMs": 60000}, "_meta": {"progressToken": "p1"}});
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

/// A task cannot start a daemon: its Job kills what it holds when it ends, and the CLI says so
/// instead of leaving a daemon that would die with its starter.
#[cfg(windows)]
#[test]
fn a_task_cannot_start_a_daemon_and_says_so() {
    let runtime = Scratch::new("daemon-in-task");
    support::create_private_runtime(&runtime);
    let namespace = format!("mcp{}", std::process::id());
    let exe = env!("CARGO_BIN_EXE_fastexec");
    // Armed before the start, so a daemon that was started by accident is stopped even when an
    // assertion below fails and the runtime directory is about to go.
    let _stop = StopOnDrop(daemon_command(exe, &runtime, &namespace, "stop"));
    let mut server = Server::start_with(|command| {
        command
            .env("FASTEXEC_TEST_RUNTIME_DIR", &*runtime)
            .env("FASTEXEC_NAMESPACE", &namespace);
    });
    let command = format!("\"{exe}\" daemon start");
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "waitMs": 60000
    }));
    assert_eq!(out["state"], "exited", "{text}");
    assert_eq!(out["exitCode"], 1, "the start did not fail: {text}");
    assert!(
        text.contains("could not leave this host's Job"),
        "the start did not explain itself: {text}"
    );

    // Nothing is left serving the namespace.
    let status = daemon_command(exe, &runtime, &namespace, "status")
        .output()
        .unwrap();
    let shown = String::from_utf8_lossy(&status.stdout).into_owned();
    assert!(
        shown.contains("No fastexec daemon for namespace"),
        "a daemon was left behind: {shown:?}"
    );
}

/// A CLI command for this namespace, in the test's own runtime directory.
#[cfg(windows)]
fn daemon_command(exe: &str, runtime: &std::path::Path, namespace: &str, action: &str) -> Command {
    let mut command = Command::new(exe);
    support::scrub_fastexec_env(&mut command);
    command
        .args(["daemon", action])
        .env("FASTEXEC_TEST_RUNTIME_DIR", runtime)
        .env("FASTEXEC_NAMESPACE", namespace)
        .stdin(Stdio::null());
    command
}

/// Runs its command when it is dropped, so a daemon started by accident does not outlive a test.
#[cfg(windows)]
struct StopOnDrop(Command);

#[cfg(windows)]
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _ = self.0.output();
    }
}

/// Polls a task's screen until it and the cursor are exactly `rows` and `cursor`, with a bounded
/// wait: the state the test means, not merely text that appears on the way there.
fn wait_for_screen(server: &mut Server, task: &str, rows: &str, cursor: Value) -> (Value, String) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let (out, text) = server.ok(json!({
            "action": "poll", "taskId": task, "screen": true, "waitMs": 100
        }));
        if text.split("\n\n[").next().unwrap_or_default() == rows && out["cursor"] == cursor {
            return (out, text);
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the screen never reached {rows:?} at {cursor}: {text}"
        );
    }
}

/// The transcript of a PTY task, read in windows until a call adds nothing: a task that ended
/// hands out its last window, then an empty one.
fn transcript_body(server: &mut Server, id: &str) -> String {
    let mut body = String::new();
    for _ in 0..20 {
        let (out, text) = server.ok(json!({
            "action": "transcript", "taskId": id, "maxBytes": 4194304
        }));
        let part = text
            .split(
                "

[",
            )
            .next()
            .unwrap_or_default();
        // The window's own note for an empty one is not transcript text.
        let part = match part.trim() {
            "(no transcript lines since the last read)" => "",
            other if other.starts_with("... [") && other.ends_with("] ...") => "",
            _ => part,
        };
        let ended = ["exited", "killed"].contains(&out["state"].as_str().unwrap_or_default());
        if part.is_empty() {
            if ended {
                break;
            }
            continue;
        }
        body.push_str(part);
        if !body.ends_with('\n') {
            body.push('\n');
        }
    }
    body
}

/// The size a PTY task starts at, as the program itself sees it: `stty size` prints rows then
/// columns.
#[test]
fn a_pty_task_starts_at_the_default_size_and_the_program_sees_it() {
    let mut server = Server::start();
    let (_, text) = server.ok(json!({
        "action": "start", "pty": true, "waitMs": 2000,
        "command": "stty size; sleep 30"
    }));
    assert!(
        text.contains("30 120"),
        "the program's terminal is 120x30: {text}"
    );
    let task = server.id(1);
    let (list, text) = server.ok(json!({"action": "list"}));
    assert!(text.contains(" pty 120x30"), "{text}");
    assert_eq!(list["tasks"][0]["cols"], json!(120), "{list}");
    assert_eq!(list["tasks"][0]["rows"], json!(30), "{list}");
    // The screen is read as it stands: the command sleeps, so a poll that waited would wait out
    // its own timeout for output that cannot arrive.
    let (_, text) = server.ok(json!(
        {"action": "poll", "taskId": task, "screen": true, "waitMs": 0}
    ));
    assert!(
        text.contains("screen 120x30, cursor row 2 col 1"),
        "the screen names its size: {text}"
    );
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
}

/// A resize in the same poll as a zero-delay key applies a synchronized update's held modes
/// before encoding the burst, including application-cursor mode.
#[test]
fn a_zero_delay_burst_carries_the_declared_keyboard_mode() {
    let (ready, ready_bash) = scratch("keyboard-mode-ready");
    let (received, received_bash) = scratch("keyboard-mode-received");
    let (done, done_bash) = scratch("keyboard-mode-done");
    let (stderr, stderr_bash) = scratch("keyboard-mode-stderr");
    let mut server = Server::start();
    let (out, text) = server.ok(json!({
        "action": "start", "pty": true, "loginShell": false,
        "cols": 100, "rows": 40, "waitMs": 0,
        "command": format!("set -e; stty raw -echo; printf '\\033[?2026h\\033[?1h'; : > '{ready_bash}'; dd bs=1 count=3 of='{received_bash}' 2> '{stderr_bash}'; : > '{done_bash}'")
    }));
    assert!(!text.contains("Cannot"), "{text}");
    let task = server.id(1);
    let log = PathBuf::from(out["logPath"].as_str().unwrap());
    // ConPTY can query CPR during startup. The ready file follows stty; the exact
    // declarations in the raw log prove capture has already fed the emulator.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let read = std::fs::read_to_string(&log).unwrap_or_default();
        if ready.exists() && read.contains("\x1b[?2026h\x1b[?1h") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the program's keyboard modes were not captured (ready={}): {read:?}",
            ready.exists()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let (poll_out, poll_text) = server.ok(json!({
        "action": "poll", "taskId": task, "cols": 80, "rows": 20,
        "keys": ["Up"], "keyDelayMs": 0, "waitMs": 1000
    }));
    // Keep the byte assertion independent of Bash's resize-sensitive read builtin.
    // The shell publishes done only after dd closes the file.
    let deadline = Instant::now() + Duration::from_secs(30);
    while !done.exists() {
        if Instant::now() >= deadline {
            let (state, state_text, state_error) = server.call(json!({
                "action": "poll", "taskId": task, "waitMs": 0, "raw": true
            }));
            panic!(
                "the program never finished reading the key: raw={:?}; received={:?}; reader stderr={:?}; start={out} {text:?}; poll={poll_out} {poll_text:?}; failure state={state} {state_text:?} isError={state_error}",
                std::fs::read_to_string(&log).unwrap_or_default(),
                std::fs::read(&*received).ok(),
                std::fs::read_to_string(&*stderr).ok()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let bytes = std::fs::read(&*received).unwrap();
    assert_eq!(
        bytes, b"\x1bOA",
        "the burst carries application-cursor mode: {bytes:?}"
    );
    assert_ne!(
        bytes, b"\x1b[A",
        "the burst used the modes the validation pass saw: {bytes:?}"
    );
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
}

#[test]
fn a_poll_resizes_the_terminal_before_its_input() {
    let mut server = Server::start();
    let (_, text) = server.ok(json!({
        "action": "start", "pty": true, "cols": 100, "rows": 40, "waitMs": 5000,
        "command": "stty size; while read -r line; do stty size; done"
    }));
    // The size `start` was given, read by the program itself before any poll runs.
    assert!(
        text.contains("40 100"),
        "the program's terminal is 100x40: {text}"
    );
    let task = server.id(1);
    let (_, text) = server.ok(json!({
        "action": "poll", "taskId": task, "cols": 60, "rows": 20, "input": "x
    ", "waitMs": 500
    }));
    assert!(
        text.contains("20 60"),
        "the program read the new size, after the resize: {text}"
    );
    // One axis alone keeps the other.
    let (_, text) = server.ok(json!({
        "action": "poll", "taskId": task, "rows": 25, "input": "y
    ", "waitMs": 500
    }));
    assert!(
        text.contains("25 60"),
        "rows changed and cols stayed: {text}"
    );
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": task, "screen": true, "waitMs": 100
    }));
    assert_eq!(out["cols"], json!(60), "{text}");
    assert_eq!(out["rows"], json!(25), "{text}");
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
}

/// The program is told the terminal changed size: on Unix it gets SIGWINCH and reads the new size
/// in its handler. The program says `ready` after the trap is armed, and that is asserted before
/// the one resize is sent: a resize to the size the terminal already has is a no-op, so a
/// notification lost before the trap existed could never be recovered by sending it again.
#[cfg(not(target_os = "windows"))]
#[test]
fn a_resize_sends_sigwinch_and_the_program_reads_the_new_size() {
    let mut server = Server::start();
    let (_, started) = server.ok(json!({
        "action": "start", "pty": true, "waitMs": 5000,
        "command": "trap 'printf \"winch %s\n\" \"$(stty size)\"' WINCH; printf 'ready\n'; while :; do sleep 0.02; done"
    }));
    assert!(
        started.contains("ready"),
        "the program armed its trap before the resize: {started}"
    );
    let task = server.id(1);
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut text = String::new();
    while !text.contains("winch 22 70") {
        let (_, polled) = server.ok(json!({
            "action": "poll", "taskId": task, "cols": 70, "rows": 22, "waitMs": 2000,
            "returnWhen": {"outputContains": ["winch 22 70"]}
        }));
        text.push_str(&polled);
        assert!(
            Instant::now() < deadline,
            "the program saw the new size on SIGWINCH: {text}"
        );
    }
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
}

/// On Windows the promise is the native console resize, which the program reads as its own size.
/// SIGWINCH delivery to bash is the emulation layer's business and is not asserted: the reader runs
/// `stty size` after the input that wakes it, and that answer must be the new size.
#[cfg(target_os = "windows")]
#[test]
fn a_resize_is_seen_by_the_program_as_its_new_size() {
    let mut server = Server::start();
    let (_, started) = server.ok(json!({
        "action": "start", "pty": true, "waitMs": 5000,
        "command": "printf 'ready\n'; while IFS= read -r line; do printf 'native %s\n' \"$(stty size)\"; done"
    }));
    assert!(started.contains("ready"), "the reader is ready: {started}");
    let task = server.id(1);
    // One resize, and nothing that resizes again.
    server.ok(json!({
        "action": "poll", "taskId": task, "cols": 70, "rows": 22, "waitMs": 2000
    }));
    let (_, text) = server.ok(json!({
        "action": "poll", "taskId": task, "input": "measure
    ", "waitMs": 5000,
        "returnWhen": {"outputContains": ["native 22 70"]}
    }));
    assert!(
        text.contains("native 22 70"),
        "the program reads the new size as its own: {text}"
    );
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
}

/// Bounds are checked before anything happens, and a pipe task has no size to change.
#[test]
fn sizes_out_of_range_or_on_a_pipe_task_are_refused() {
    let mut server = Server::start();
    let (_, text, is_error) = server.call(json!({
        "action": "start", "pty": true, "cols": 19, "command": "printf hi"
    }));
    assert!(
        is_error && text.contains("cols 19 is out of range 20-500"),
        "{text}"
    );
    let (_, text, is_error) = server.call(json!({
        "action": "start", "pty": true, "rows": 201, "command": "printf hi"
    }));
    assert!(
        is_error && text.contains("rows 201 is out of range 5-200"),
        "{text}"
    );
    let (_, text, is_error) = server.call(json!({
        "action": "start", "pty": false, "cols": 100, "command": "printf hi"
    }));
    assert!(
        is_error && text.contains("`cols` applies to PTY tasks only"),
        "{text}"
    );
    server.ok(json!({"action": "start", "pty": false, "waitMs": 5000, "command": "printf hi"}));
    let pipe = server.id(1);
    let (_, text, is_error) = server.call(json!({"action": "poll", "taskId": pipe, "rows": 10}));
    assert!(
        is_error && text.contains("applies to PTY tasks only"),
        "{text}"
    );
}

/// A poll that fails validation changes nothing: no resize, and no input delivered.
#[test]
fn an_invalid_combined_poll_neither_resizes_nor_delivers_input() {
    let mut server = Server::start();
    server.ok(json!({
        "action": "start", "pty": true, "waitMs": 5000,
        "command": "stty size; while read -r line; do printf 'got %s\\n' \"$line\"; done"
    }));
    let task = server.id(1);
    let (_, text, is_error) = server.call(json!({
        "action": "poll", "taskId": task, "cols": 100, "rows": 40, "input": "hello\n",
        "keys": ["NotAKey"], "waitMs": 200
    }));
    assert!(
        is_error && text.contains("NotAKey"),
        "the key name is refused: {text}"
    );
    let (_, text) = server.ok(json!({
        "action": "poll", "taskId": task, "input": "second\n", "waitMs": 500
    }));
    assert!(
        !text.contains("got hello"),
        "the refused poll's input was not delivered: {text}"
    );
    assert!(
        text.contains("got second"),
        "the next poll's input was: {text}"
    );
    let (out, text) =
        server.ok(json!({"action": "poll", "taskId": task, "screen": true, "waitMs": 200}));
    assert!(
        text.contains("screen 120x30"),
        "the refused poll's size was not applied: {text}"
    );
    assert_eq!(out["cols"], json!(120), "{text}");
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
}

/// A poll that names the size the task already has changes nothing: no SIGWINCH, no repaint.
#[test]
fn an_unchanged_size_sends_nothing() {
    let mut server = Server::start();
    server.ok(json!({
        "action": "start", "pty": true, "waitMs": 5000,
        "command": "n=0; trap 'n=$((n+1)); printf \"winch %s\\n\" \"$n\"' WINCH; printf 'ready\\n'; while :; do sleep 0.02; done"
    }));
    let task = server.id(1);
    // A loaded machine runs the trap late, so the size keeps changing until the program answers:
    // each change is another SIGWINCH, and the first answer is the one the test wants.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut text = String::new();
    let (mut cols, mut sent) = (121u16, 0u16);
    while !text.contains("winch 1") {
        sent = cols;
        let (_, polled) = server.ok(json!({
            "action": "poll", "taskId": task, "cols": cols, "waitMs": 2000,
            "returnWhen": {"outputContains": ["winch 1"]}
        }));
        text.push_str(&polled);
        cols = if cols == 121 { 122 } else { 121 };
        assert!(Instant::now() < deadline, "a changed size does: {text}");
    }
    // The size it now has, named again: the program has already answered one change, so a second
    // answer could only come from a resize that should not have been sent.
    let (_, unchanged) = server.ok(json!({
        "action": "poll", "taskId": task, "cols": sent, "waitMs": 1500
    }));
    assert!(
        !unchanged.contains("winch 2"),
        "an unchanged size sends no SIGWINCH: {unchanged}"
    );
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
}

/// A resize after the task's tree ended is refused before anything changes.
#[test]
fn a_resize_after_exit_is_refused() {
    let mut server = Server::start();
    server.ok(json!({"action": "start", "pty": true, "waitMs": 30000, "command": "printf hi"}));
    let task = server.id(1);
    let (_, text, is_error) = server.call(json!({"action": "poll", "taskId": task, "cols": 100}));
    assert!(
        is_error && (text.contains("terminal is closed") || text.contains("tree has ended")),
        "the resize is refused: {text}"
    );
    // The task's own record is unchanged, and it still renders its screen.
    let (out, text) =
        server.ok(json!({"action": "poll", "taskId": task, "screen": true, "waitMs": 100}));
    assert_eq!(out["cols"], json!(120), "{text}");
    assert!(text.contains("screen 120x30"), "{text}");
}

/// Sustained output through fifty resizes: the settled contract, on the product's own path. The
/// producer prints a fixed payload of 20 000 lines, is still printing for every resize, and its
/// transcript then holds every one of those lines, whole and in order, in over a megabyte.
#[test]
#[ignore = "heavy: opt in with --ignored"]
fn sustained_output_through_resizes_keeps_every_line() {
    const LINES: usize = 20_000;
    let started = std::time::Instant::now();
    let deadline = started + std::time::Duration::from_secs(180);
    let mut server = Server::start();
    let temp = std::env::temp_dir();
    let tag = std::process::id();
    let done = temp.join(format!("fastexec-b3-done-{tag}"));
    let _ = std::fs::remove_file(&done);
    // Every fifty lines the producer waits, so its output spans the whole resize loop instead of
    // landing in one burst, and the payload is fixed whether or not the loop keeps up.
    let command = format!(
        "i=0; while [ $i -lt {LINES} ]; do i=$((i+1)); \
         printf 'line %05d %s\\n' $i '------------------------------------------------------------'; \
         if [ $((i % 50)) -eq 0 ]; then sleep 0.02; fi; done; : > '{done}'; sleep 60",
        done = done.display().to_string().replace('\\', "/")
    );
    server.ok(json!({
        "action": "start", "pty": true, "cols": 68, "rows": 30, "waitMs": 500, "command": command
    }));
    let task = server.id(1);
    let sizes = [(100u16, 30u16), (40, 12), (20, 5), (500, 200), (68, 30)];
    for k in 0..50 {
        assert!(
            !done.exists(),
            "the producer finished before resize {k} of 50"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "the resizes passed their bound at {k}"
        );
        let (cols, rows) = sizes[k % sizes.len()];
        let (_, text) = server.ok(json!({
            "action": "poll", "taskId": task, "cols": cols, "rows": rows, "waitMs": 50
        }));
        assert!(!text.contains("Cannot resize"), "a resize failed: {text}");
    }
    assert!(!done.exists(), "the producer printed through every resize");
    // The payload's own end, within the run's bound: the producer's 20 000 lines take as long as
    // they take on this platform.
    while !done.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "the producer never finished its payload"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 10000}));
    let body = transcript_body(&mut server, &task);
    let written: Vec<&str> = body.lines().collect();
    let payload: Vec<String> = (1..=LINES)
        .map(|k| format!("line {k:05} {}", "-".repeat(60)))
        .collect();
    // The fixed payload, independently written: every line is there, whole, in order.
    let mut at = 0;
    for want in &payload {
        match written[at..].iter().position(|line| line == want) {
            Some(found) => at += found + 1,
            None => panic!(
                "payload line {want:?} is not in the transcript in order after {at} lines of {}",
                written.len()
            ),
        }
    }
    assert!(
        body.len() > 1_048_576,
        "the transcript holds over a megabyte of sustained output: {} bytes",
        body.len()
    );
    // Every other line is a payload line or a row visible at a resize, which a width change can
    // cut: it is a part of a payload line. A blank extra line counts, and fails this check.
    for line in &written {
        assert!(
            !line.is_empty() && payload.iter().any(|want| want.contains(line)),
            "a transcript line is neither a payload line nor part of one: {line:?}"
        );
    }
    let extras = written.len().saturating_sub(payload.len());
    assert!(
        extras <= 50 * 200,
        "{extras} extra lines, more than fifty resizes of the largest screen"
    );
    assert!(
        std::time::Instant::now() < deadline,
        "the whole run took {:?}",
        started.elapsed()
    );
    let _ = std::fs::remove_file(&done);
}

#[cfg(unix)]
#[test]
fn bash_discovery_still_works_after_the_session_directory_is_removed() {
    let gone = Scratch::new("gone-launch-cwd");
    std::fs::create_dir(&*gone).unwrap();
    let home = Scratch::new("gone-cwd-profile");
    std::fs::create_dir(&*home).unwrap();
    std::fs::write(home.join(".bash_profile"), "exit 7\n").unwrap();
    let mut server = Server::start_with(|command| {
        command.current_dir(&*gone).env("HOME", &*home);
    });
    std::fs::remove_dir(&*gone).unwrap();
    // The probe can run from bash's directory, but the actual start with the deleted cwd fails.
    let (_, text, failed) = server.call(json!({"action": "start", "command": "true"}));
    assert!(
        failed && text.contains("Cannot start the command"),
        "{text}"
    );
    let cwd = std::env::temp_dir();
    let (result, text) = server.ok(json!({
        "action": "start", "command": "printf discovered", "cwd": cwd,
        "loginShell": false, "waitMs": 10000
    }));
    assert_eq!(result["exitCode"], 0, "{text}");
    assert!(text.starts_with("discovered"), "{text}");
    assert!(text.contains("Login environment probe failed"), "{text}");
    // The successful discovery is cached, and later tasks still work from an explicit cwd.
    let (result, text) = server.ok(json!({
        "action": "start", "command": "printf cached", "cwd": cwd,
        "loginShell": false, "waitMs": 10000
    }));
    assert_eq!(result["exitCode"], 0, "{text}");
    assert!(text.starts_with("cached"), "{text}");
}

#[cfg(unix)]
#[test]
fn bash_version_probes_end_background_children_on_success_error_and_timeout() {
    let bash = if cfg!(target_os = "freebsd") {
        "/usr/local/bin/bash"
    } else {
        "/bin/bash"
    };
    use std::os::unix::fs::PermissionsExt;
    for (name, finish, should_succeed) in [
        ("success", "exit 0", true),
        ("failure", "exit 7", false),
        ("timeout", "sleep 30", false),
        ("large", "head -c 5000 /dev/zero; exit 0", false),
    ] {
        let dir = Scratch::new(&format!("bash-probe-{name}"));
        std::fs::create_dir(&*dir).unwrap();
        let wrapper = dir.join("bash");
        let beat = dir.join("beat");
        // A bounded heartbeat keeps stdout open after the version root exits. It is part of
        // the probe's tree even though no user-visible Task exists for a failed discovery.
        std::fs::write(&wrapper, format!(
            "#!/bin/sh\nif [ \"$1\" = --version ]; then\n (for i in $(seq 300); do printf x >> '{}'; sleep 0.1; done) &\n while [ ! -s '{}' ]; do sleep 0.01; done\n printf 'GNU bash, version 5.2\\n'\n {finish}\nfi\nexec {bash} \"$@\"\n",
            beat.display(), beat.display()
        )).unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut server = Server::start_with(|command| {
            command.env("FASTEXEC_BASH", &wrapper);
        });
        let began = Instant::now();
        let (_, text, is_error) = server.call(json!({
            "action": "start", "command": "printf tested", "loginShell": false, "waitMs": 10000
        }));
        assert_eq!(!is_error, should_succeed, "{name}: {text}");
        assert!(
            began.elapsed() < Duration::from_secs(15),
            "{name}: discovery was unbounded"
        );
        assert!(
            file_len(&beat) > 0,
            "{name}: the background child did not start"
        );
        assert_stops_growing(&beat);
    }
}

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
#[test]
fn bash_version_probe_owns_a_child_that_detaches_its_session() {
    let bash = if cfg!(target_os = "freebsd") {
        "/usr/local/bin/bash"
    } else {
        "/bin/bash"
    };
    use std::os::unix::fs::PermissionsExt;
    let dir = Scratch::new("bash-probe-detached");
    std::fs::create_dir(&*dir).unwrap();
    let wrapper = dir.join("bash");
    let beat = dir.join("beat");
    std::fs::write(&wrapper, format!(
        "#!/bin/sh\nif [ \"$1\" = --version ]; then\n perl -MPOSIX -e 'exit if fork; POSIX::setsid() or die \"setsid: $!\"; exec @ARGV' /bin/sh -c 'for i in $(seq 300); do printf x >> \"{}\"; sleep 0.1; done'\n while [ ! -s '{}' ]; do sleep 0.01; done\n printf 'GNU bash, version 5.2\\n'\n exit 0\nfi\nexec {bash} \"$@\"\n",
        beat.display(), beat.display()
    )).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut server = Server::start_with(|command| {
        command.env("FASTEXEC_BASH", &wrapper);
    });
    let (result, text) = server.ok(json!({
        "action": "start", "command": "printf tested", "loginShell": false, "waitMs": 10000
    }));
    assert_eq!(result["exitCode"], 0, "{text}");
    assert!(file_len(&beat) > 0, "the detached child did not start");
    assert_stops_growing(&beat);
}

#[cfg(unix)]
#[test]
fn file_tools_preserve_the_original_after_an_os_write_failure() {
    use std::os::unix::process::CommandExt;
    let directory = Scratch::new("atomic-save-mcp");
    std::fs::create_dir(&directory.0).unwrap();
    let target = directory.join("target.txt");
    let original = format!("{}needle\n", "original ".repeat(50));
    std::fs::write(&target, &original).unwrap();
    let mut server = Server::start_with(|command| {
        // This is a fresh, isolated server; no other test or daemon inherits the limit.
        unsafe {
            command.pre_exec(|| {
                libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
                let limit = libc::rlimit {
                    rlim_cur: 1024,
                    rlim_max: 1024,
                };
                if libc::setrlimit(libc::RLIMIT_FSIZE, &limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    });
    for (tool, arguments) in [
        (
            "write",
            json!({"path": target, "content": "N".repeat(2048)}),
        ),
        (
            "edit",
            json!({"path": target, "edits": [{"oldText": "needle", "newText": "N".repeat(2048)}]}),
        ),
    ] {
        let (text, error) = server.file(tool, arguments);
        assert!(error, "{tool} must report the OS failure: {text}");
        assert_eq!(
            std::fs::read(&target).unwrap(),
            original.as_bytes(),
            "{tool} corrupted the original"
        );
        assert_eq!(
            std::fs::read_dir(&directory.0).unwrap().count(),
            1,
            "failed staging files must be removed"
        );
    }
}

#[cfg(unix)]
#[test]
fn editing_a_fifo_returns_an_error_without_blocking_other_file_tools() {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::FileTypeExt;

    let directory = Scratch::new("fifo-edit-mcp");
    std::fs::create_dir(&directory.0).unwrap();
    let fifo = directory.join("pipe");
    let path = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
    // No writer opens this FIFO. A blocking read would hold the global mutation lock forever.
    assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
    let target = directory.join("regular.txt");
    let mut server = Server::start();
    for (id, tool, arguments) in [
        (
            1001,
            "edit",
            json!({"path": fifo, "edits": [{"oldText": "needle", "newText": "changed"}]}),
        ),
        (
            1002,
            "write",
            json!({"path": target, "content": "independent file"}),
        ),
        (1003, "read", json!({"path": fifo})),
    ] {
        server.send(json!({"jsonrpc": "2.0", "id": id, "method": "tools/call",
            "params": {"name": tool, "arguments": arguments}}));
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut replies = Vec::new();
    while replies.len() < 3 {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let message = server
            .lines
            .recv_timeout(remaining)
            .expect("a FIFO must not block its edit or an unrelated file mutation");
        if message["id"]
            .as_u64()
            .is_some_and(|id| (1001..=1003).contains(&id))
        {
            replies.push(message);
        } else {
            assert!(message["__stdout_ended"].is_null(), "{message}");
        }
    }
    for id in [1001, 1003] {
        let reply = replies.iter().find(|reply| reply["id"] == id).unwrap();
        assert_eq!(reply["result"]["isError"], true, "{reply}");
        assert!(
            reply["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("not a regular file"),
            "{reply}"
        );
    }
    let written = replies.iter().find(|reply| reply["id"] == 1002).unwrap();
    assert_ne!(written["result"]["isError"], true, "{written}");
    assert_eq!(std::fs::read(&target).unwrap(), b"independent file");
    assert!(
        std::fs::symlink_metadata(&fifo)
            .unwrap()
            .file_type()
            .is_fifo()
    );
}

#[test]
fn reader_handles_keep_independent_cursors_with_bare_id_attaches() {
    let mut server = Server::start();
    let (started, text) = server.ok(json!({
        "action": "start", "eof": false, "command": "printf 'reader-first\\n'; IFS= read -r line; printf 'reader-second:%s\\n' \"$line\"",
        "loginShell": false, "waitMs": 10000,
        "returnWhen": {"outputContains": ["reader-first"]}
    }));
    let first = started["taskId"].as_str().unwrap().to_owned();
    let task = bare_id(&first).to_owned();
    assert!(
        text.contains(&first) && !text.contains("new reader"),
        "{text}"
    );
    let (listed, listing) = server.ok(json!({"action": "list"}));
    assert_eq!(listed["tasks"][0]["taskId"], task);
    assert!(
        listing.starts_with(&task) && !listing.contains(&first),
        "{listing}"
    );

    let (attached, text) = server.ok(json!({"action": "poll", "taskId": task, "waitMs": 0}));
    let second = attached["taskId"].as_str().unwrap().to_owned();
    assert_ne!(first, second);
    assert_eq!(bare_id(&second), task);
    assert!(
        text.starts_with("reader-first\n") && text.contains(" · new reader"),
        "{text}"
    );
    let (_, text) = server.ok(json!({"action": "poll", "taskId": first, "waitMs": 0}));
    assert!(
        text.starts_with("(no new output)") && !text.contains("new reader"),
        "{text}"
    );

    let (_, text) =
        server.ok(json!({"action": "poll", "taskId": first, "input": "next\n", "waitMs": 10000}));
    assert!(
        text.starts_with("reader-second:next\n") && !text.contains("reader-first"),
        "{text}"
    );
    let (_, text) = server.ok(json!({"action": "poll", "taskId": second, "waitMs": 10000}));
    assert!(
        text.starts_with("reader-second:next\n") && !text.contains("new reader"),
        "{text}"
    );
    let (again, text) = server.ok(json!({"action": "poll", "taskId": task, "waitMs": 0}));
    assert_ne!(again["taskId"], first);
    assert_ne!(again["taskId"], second);
    assert!(
        text.starts_with("reader-first\nreader-second:next\n") && text.contains("new reader"),
        "{text}"
    );
}

#[test]
fn fresh_reader_notice_survives_kill_and_fits_every_result_mode() {
    let mut server = Server::start();
    server.ok(json!({"action": "start", "command": "sleep 60", "loginShell": false, "waitMs": 0}));
    for mode in ["stream", "screen", "transcript"] {
        let (started, text) = server.ok(json!({
            "action": "start", "command": "printf 'reader-ready\\n'; sleep 60", "loginShell": false,
            "pty": mode != "stream", "waitMs": 10000,
            "returnWhen": {"outputContains": ["reader-ready"]}
        }));
        assert!(!text.contains("new reader"), "{mode}: {text}");
        let bare = bare_id(started["taskId"].as_str().unwrap()).to_owned();
        let (killed, text) = server.ok(json!({
            "action": "kill", "taskId": bare, "maxBytes": 100
        }));
        let kill_handle = killed["taskId"].as_str().unwrap().to_owned();
        // P1-4: kill on a bare taskId delivers unseen output and carries the new-reader notice.
        assert!(
            body_of(&text).len() <= 100
                && text.contains(&kill_handle)
                && text.contains(" · new reader"),
            "{mode}: {text}"
        );
        assert!(
            text.lines().last().unwrap().starts_with("(Background: "),
            "{mode}: {text}"
        );
        let (_, after_kill) = server.ok(json!({
            "action": "poll", "taskId": kill_handle, "maxBytes": 100, "waitMs": 0
        }));
        assert!(
            body_of(&after_kill).len() <= 100 && !after_kill.contains("new reader"),
            "{mode}: {after_kill}"
        );
        // A fresh bare attach into poll/screen/transcript also carries new reader once within 100B body.
        let mut request = if mode == "transcript" {
            json!({"action": "transcript", "taskId": bare, "maxBytes": 100})
        } else {
            json!({"action": "poll", "taskId": bare, "maxBytes": 100, "waitMs": 0})
        };
        if mode == "screen" {
            request["screen"] = json!(true);
        }
        let (result, text) = server.ok(request.clone());
        let handle = result["taskId"].as_str().unwrap().to_owned();
        assert!(
            body_of(&text).len() <= 100 && text.contains(&handle) && text.contains(" · new reader"),
            "{mode}: {text}"
        );
        request["taskId"] = json!(handle);
        assert!(
            text.lines().last().unwrap().starts_with("(Background: "),
            "{mode}: {text}"
        );
        let (_, text) = server.ok(request);
        assert!(
            body_of(&text).len() <= 100 && !text.contains("new reader"),
            "{mode}: {text}"
        );
    }
}

#[test]
fn yolo_overrides_stdio_tool_annotations_and_rejects_invalid_values() {
    let defaults = [
        (
            "edit",
            json!({"title": "Edit file", "destructiveHint": true, "idempotentHint": false}),
        ),
        (
            "exec",
            json!({"title": "Run bash command", "readOnlyHint": false, "destructiveHint": true, "openWorldHint": true}),
        ),
        ("read", json!({"title": "Read file", "readOnlyHint": true})),
        (
            "write",
            json!({"title": "Write file", "destructiveHint": true, "idempotentHint": false}),
        ),
    ];
    for setting in [None, Some("false")] {
        let mut server = Server::start_with(|command| {
            if let Some(value) = setting {
                command.env("FASTEXEC_YOLO", value);
            }
        });
        let listed = server.request("tools/list", json!({}));
        let tools = listed["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 4, "{listed}");
        for (name, expected) in &defaults {
            let tool = tools
                .iter()
                .find(|tool| tool["name"] == *name)
                .unwrap_or_else(|| panic!("missing {name} in {listed}"));
            assert_eq!(&tool["annotations"], expected, "{setting:?} {name}: {tool}");
        }
    }

    let mut yolo = Server::start_with(|command| {
        command.env("FASTEXEC_YOLO", "true");
    });
    let listed = yolo.request("tools/list", json!({}));
    let tools = listed["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 4, "{listed}");
    for (name, expected) in &defaults {
        let tool = tools
            .iter()
            .find(|tool| tool["name"] == *name)
            .unwrap_or_else(|| panic!("missing {name} in {listed}"));
        assert_eq!(
            tool["annotations"],
            json!({
                "title": expected["title"],
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false,
            }),
            "yolo=true {name}: {tool}"
        );
    }

    let mut invalid: Vec<std::ffi::OsString> = ["", "on", "off", "1", "0", "bogus"]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        invalid.push(std::ffi::OsString::from_vec(vec![0xff]));
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt;
        invalid.push(std::ffi::OsString::from_wide(&[0xD800]));
    }
    for bad in invalid {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        support::scrub_fastexec_env(&mut command);
        let output = command
            .env("FASTEXEC_YOLO", &bad)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{bad:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("fastexec: invalid FASTEXEC_YOLO ")
                && stderr.contains("; expected true or false")
                && !stderr.contains("Ok(")
                && !stderr.contains("Err("),
            "{bad:?}: {stderr:?}"
        );
    }
}

fn codemode_server() -> Server {
    Server::start_with(|command| {
        command
            .env("FASTEXEC_CODEMODE", "true")
            .env("FASTEXEC_OUTPUT_MODE", "both");
    })
}

fn code_result(server: &mut Server, arguments: Value) -> Value {
    let reply = server.request(
        "tools/call",
        json!({"name":"codemode", "arguments":arguments}),
    );
    assert!(reply["result"].is_object(), "{reply}");
    reply["result"].clone()
}

fn code_text(result: &Value) -> &str {
    result["content"][0]["text"].as_str().unwrap_or_default()
}

fn image_count(result: &Value) -> usize {
    result["content"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|block| block["type"] == "image")
        .count()
}

fn image_data_url(width: u32, height: u32) -> String {
    let image = image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(
        width,
        height,
        image::Rgb([7, 8, 9]),
    ));
    let mut png = Vec::new();
    image
        .write_to(&mut std::io::Cursor::new(&mut png), image::ImageFormat::Png)
        .unwrap();
    format!(
        "data:image/png;base64,{}",
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, png)
    )
}

#[test]
fn codemode_requires_explicit_enable_and_respects_yolo() {
    for setting in [None, Some("false")] {
        let mut server = Server::start_with(|command| {
            if let Some(value) = setting {
                command.env("FASTEXEC_CODEMODE", value);
            }
        });
        let listed = server.request("tools/list", json!({}));
        assert!(
            !listed["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|tool| tool["name"] == "codemode"),
            "{listed}"
        );
        let reply = server.request(
            "tools/call",
            json!({"name":"codemode", "arguments":{"code":"return 1"}}),
        );
        assert!(
            reply["error"].is_object() || reply["result"]["isError"] == true,
            "{reply}"
        );
    }
    for yolo in [false, true] {
        let mut server = Server::start_with(|command| {
            command
                .env("FASTEXEC_CODEMODE", "true")
                .env("FASTEXEC_YOLO", yolo.to_string());
        });
        let listed = server.request("tools/list", json!({}));
        let tools = listed["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 5, "{listed}");
        let tool = tools
            .iter()
            .find(|tool| tool["name"] == "codemode")
            .unwrap();
        assert_eq!(tool["annotations"]["readOnlyHint"], yolo, "{tool}");
        assert_eq!(tool["annotations"]["destructiveHint"], !yolo, "{tool}");
        assert_eq!(tool["annotations"]["openWorldHint"], !yolo, "{tool}");
        assert_eq!(tool["inputSchema"]["additionalProperties"], false, "{tool}");
        assert_eq!(
            tool["inputSchema"]["properties"]["code"]["type"], "string",
            "{tool}"
        );
        assert_eq!(
            tool["inputSchema"]["properties"]["cwd"]["type"],
            json!("string"),
            "{tool}"
        );
        assert!(
            tool["description"]
                .as_str()
                .unwrap()
                .contains("Prefer this over multi-turn exec or read calls"),
            "{tool}"
        );
        assert!(
            tool["inputSchema"]["required"]
                .as_array()
                .unwrap()
                .contains(&json!("code")),
            "{tool}"
        );
        let exec = tools.iter().find(|tool| tool["name"] == "exec").unwrap();
        let description = exec["description"].as_str().unwrap();
        assert!(
            description.contains("poll, kill, and list also take codemode tasks"),
            "{description}"
        );
        assert!(
            description.lines().last().unwrap().starts_with("Host: "),
            "{description}"
        );
    }
    for bad in ["", "on", "off", "1", "0", "bogus"] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        support::scrub_fastexec_env(&mut command);
        let output = command
            .env("FASTEXEC_CODEMODE", bad)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{bad:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("invalid FASTEXEC_CODEMODE")
                && stderr.contains("expected true or false"),
            "{stderr}"
        );
    }
}

#[test]
fn codemode_invalid_arguments_do_not_create_tasks() {
    let mut server = codemode_server();
    // Schema-decoding failures have no structuredContent.
    for args in [
        json!({}),
        json!({"code":42}),
        json!({"code":"return 1", "pty":true}),
        json!({"code":"return 1", "returnWhen":{"screenContains":["x"]}}),
    ] {
        let result = code_result(&mut server, args);
        assert_eq!(result["isError"], true, "{result}");
        assert!(result.get("structuredContent").is_none(), "{result}");
        let (listed, _) = server.ok(json!({"action":"list"}));
        assert!(listed["tasks"].as_array().unwrap().is_empty(), "{listed}");
    }
    // Parameter and business validation failures carry structuredContent with action: "codemode".
    for args in [
        json!({"code":"  "}),
        json!({"code":"x\0y"}),
        json!({"code":"return 1", "cwd":"definitely-missing-relative-path"}),
        json!({"code":"return 1", "cwd":"/nonexistent-fastexec-dir-xyz"}),
        json!({"code":"return 1", "waitMs":240001}),
        json!({"code":"return 1", "returnWhen":{}}),
        json!({"code":"return 1", "returnWhen":{"quietMs":5}}),
        json!({"code":"return 1", "maxBytes":4194305}),
        json!({"code":"x".repeat(256 * 1024 + 1)}),
    ] {
        let result = code_result(&mut server, args);
        assert_eq!(result["isError"], true, "{result}");
        assert_eq!(result["structuredContent"]["ok"], false, "{result}");
        assert_eq!(
            result["structuredContent"]["action"], "codemode",
            "{result}"
        );
        assert!(
            result["structuredContent"]["error"]
                .as_str()
                .is_some_and(|error| !error.is_empty()),
            "{result}"
        );
        let (listed, _) = server.ok(json!({"action":"list"}));
        assert!(listed["tasks"].as_array().unwrap().is_empty(), "{listed}");
    }
}

#[test]
fn codemode_streams_script_results_and_preserves_error_lines() {
    let mut server = codemode_server();
    let result = code_result(
        &mut server,
        json!({"code":"text('first'); await Promise.resolve(); console.log('second', {ok:true}); return {last:3};"}),
    );
    assert_eq!(result["isError"], false, "{result}");
    let task = &result["structuredContent"];
    assert_eq!(task["action"], "codemode", "{result}");
    assert_eq!(task["kind"], "codemode", "{result}");
    assert_eq!(task["exitCode"], 0, "{result}");
    assert_eq!(
        task["output"], "first\nsecond {\"ok\":true}\n{\"last\":3}",
        "{result}"
    );
    for code in [
        "try { exit(); } catch { text('caught'); } text('after');",
        "return undefined;",
    ] {
        let result = code_result(&mut server, json!({"code":code}));
        assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
        assert_eq!(result["structuredContent"]["output"], "", "{result}");
    }
    for (code, expected) in [
        ("text('before');\nthrow new Error('boom');", "codemode.js:2"),
        (
            "await new Promise(() => {});",
            "promise that can never settle",
        ),
        ("const = ;", "SyntaxError"),
        ("Promise.reject(new Error('unhandled'));", "unhandled"),
    ] {
        let result = code_result(&mut server, json!({"code":code}));
        assert_eq!(result["isError"], false, "{result}");
        assert_eq!(result["structuredContent"]["exitCode"], 1, "{result}");
        assert!(
            code_text(&result).contains("Script error:") && code_text(&result).contains(expected),
            "{result}"
        );
    }
}

#[test]
fn codemode_nested_results_keep_their_shape_under_outer_render_settings() {
    let dir = Scratch::new("codemode-files");
    std::fs::create_dir_all(&*dir).unwrap();
    for structured in ["true", "false"] {
        let mut server = Server::start_with(|command| {
            command
                .current_dir(&*dir)
                .env("FASTEXEC_CODEMODE", "true")
                .env("FASTEXEC_OUTPUT_MODE", "structured")
                .env("FASTEXEC_STRUCTURED_CONTENT", structured);
        });
        let code = r#"
const written = await tools.write({path:'note.txt', content:'alpha\nbeta\n'});
const edited = await tools.edit({path:'note.txt', edits:[{oldText:'beta', newText:'BETA'}]});
const [command, file, absentOutcome] = await Promise.all([
  tools.exec({action:'start', command:'printf nested-output', loginShell:false}),
  tools.read({path:'note.txt'}),
  tools.read({path:'absent.txt'}).then(() => null, e => e)
]);
if ('ok' in written || 'structuredContent' in written || written.bytes !== 11) throw new Error('write shape');
if ('ok' in edited || edited.replacements !== 1 || edited.edits[0].line !== 2) throw new Error('edit shape');
if ('ok' in command || 'content' in command || command.output !== 'nested-output') throw new Error('exec shape');
if ('ok' in file || file.text !== 'alpha\nBETA\n' || file.truncated !== false || file.nextOffset !== null || file.totalLines !== 2) throw new Error('file shape');
if (!absentOutcome || absentOutcome.name !== 'ToolError' || absentOutcome.tool !== 'read' || typeof absentOutcome.result?.error !== 'string') throw new Error('absent shape');
const reads = await Promise.all(Array.from({length:20}, () => tools.read({path:'note.txt'})));
if (reads.length !== 20 || reads.some(r => 'ok' in r || r.text !== 'alpha\nBETA\n')) throw new Error('queued reads');
text('nested-ok');
"#;
        let result = code_result(&mut server, json!({"code":code}));
        assert_eq!(result["isError"], false, "{result}");
        let bad = code_result(&mut server, json!({"code":"   "}));
        assert_eq!(bad["isError"], true, "{bad}");
        if structured == "true" {
            assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
            assert_eq!(
                result["structuredContent"]["output"], "nested-ok",
                "{result}"
            );
            assert!(!code_text(&result).contains("nested-ok"), "{result}");
            assert_eq!(bad["structuredContent"]["action"], "codemode", "{bad}");
        } else {
            assert!(result.get("structuredContent").is_none(), "{result}");
            assert!(bad.get("structuredContent").is_none(), "{bad}");
            assert!(
                code_text(&result).contains("nested-ok")
                    && !code_text(&result).contains("Script error:"),
                "{result}"
            );
        }
    }
}

#[test]
fn codemode_rejects_bad_nested_calls_and_survives_changed_prototypes() {
    let mut server = codemode_server();
    let code = r#"
let rejected = 0;
for (const call of [
  () => tools.read(null), () => tools.read({path:'x', bogus:true}),
  () => tools.exec({action:'bogus'}), () => tools.exec({action:'list', bogus:true}),
  () => tools.codemode({code:'return 1'})
]) { try { await call(); } catch (e) { if (e instanceof TypeError) rejected++; } }
if (rejected !== 5) throw new Error('invalid calls resolved: ' + rejected);
for (const call of [
  () => tools.exec({action:'start'}),
  () => tools.exec({action:'start', command:''}),
  () => tools.exec({action:'start', command:'true', maxBytes:99}),
  () => tools.edit({path:'x', edits:[]})
]) {
  let err = null;
  try { await call(); } catch (e) { err = e; }
  if (!err || err.name !== 'ToolError' || typeof err.result?.error !== 'string') throw new Error('validation failure did not reject with ToolError');
}
const longWait = await tools.exec({action:'start', command:'true', waitMs:600000, loginShell:false});
if (longWait.exitCode !== 0) throw new Error('nested waitMs > 240000 must succeed');
const pending = tools.read({path:'a-file-that-does-not-exist'});
Map.prototype.get = Map.prototype.set = Map.prototype.delete = () => { throw new Error('changed Map'); };
Promise.prototype.then = () => { throw new Error('changed Promise'); };
JSON.parse = JSON.stringify = () => { throw new Error('changed JSON'); };
let missingErr = null;
try { await pending; } catch (e) { missingErr = e; }
if (!missingErr || missingErr.name !== 'ToolError') throw new Error('missing file did not reject with ToolError');
text('settled');
"#;
    let result = code_result(&mut server, json!({"code":code}));
    assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
    assert_eq!(result["structuredContent"]["output"], "settled", "{result}");
}

#[test]
fn codemode_kill_reports_pending_calls_and_leaves_started_tasks_running() {
    let mut server = codemode_server();
    let result = code_result(
        &mut server,
        json!({
            "code":"const child = await tools.exec({action:'start',command:'sleep 30',waitMs:0,loginShell:false}); text('child=' + child.taskId); const waiting = tools.exec({action:'poll',taskId:child.taskId}); text('waiting'); await waiting;",
            "waitMs":10000, "returnWhen":{"outputContains":["waiting"]}
        }),
    );
    assert_eq!(result["isError"], false, "{result}");
    let script = result["structuredContent"]["taskId"]
        .as_str()
        .unwrap()
        .to_owned();
    let child = code_text(&result)
        .lines()
        .find_map(|line| line.strip_prefix("child="))
        .unwrap()
        .to_owned();
    let (_, ok_text, ok_err) = server.call(json!({"action":"poll", "taskId":script, "waitMs":0, "eof":false, "screen":false, "raw":false}));
    assert!(!ok_err, "{ok_text}");
    for extra in [
        json!({"input":"x"}),
        json!({"eof":true}),
        json!({"keys":["Enter"]}),
        json!({"keyDelayMs":0}),
        json!({"cols":80}),
        json!({"rows":24}),
        json!({"screen":true}),
        json!({"raw":true}),
        json!({"encoding":"utf-8"}),
        json!({"returnWhen":{"screenContains":["x"]}}),
        json!({"returnWhen":{"screenAppears":["x"]}}),
        json!({"returnWhen":{"screenGone":["x"]}}),
    ] {
        let mut args = json!({"action":"poll", "taskId":script, "waitMs":0});
        args.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let (_, text, error) = server.call(args);
        assert!(
            error && text.contains("A codemode task takes no input"),
            "{text}"
        );
    }
    let (_, text, error) = server.call(json!({"action":"transcript", "taskId":script}));
    assert!(error && text.to_lowercase().contains("terminal"), "{text}");
    let (stopped, text) = server.ok(json!({"action":"kill", "taskId":script}));
    assert_eq!(stopped["state"], "killed", "{stopped}: {text}");
    assert!(stopped["exitCode"].is_null(), "{stopped}");
    assert!(
        text.contains("tasks it started") && text.contains("keep running"),
        "{text}"
    );
    assert!(
        stopped["codemode"]["cancelledCalls"]
            .as_array()
            .unwrap()
            .iter()
            .any(|call| call["tool"] == "exec" && call["action"] == "poll"),
        "{stopped}"
    );
    assert!(
        stopped["codemode"]["startedTasks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|task| task["taskId"] == child && task["state"] == "running"),
        "{stopped}"
    );
    let (listed, _) = server.ok(json!({"action":"list"}));
    assert!(
        listed["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|task| task["taskId"] == bare_id(&child)
                && task["state"] == "running"
                && task["kind"] == "bash"),
        "{listed}"
    );
    server.ok(json!({"action":"kill", "taskId":child}));
}

#[test]
fn codemode_deadlines_stop_bytecode_and_microtasks() {
    let mut server = codemode_server();
    for code in ["while (true) {}", "while (true) await null;"] {
        let result = code_result(
            &mut server,
            json!({"code":code,"killAfterMs":300,"waitMs":10000}),
        );
        assert_eq!(result["isError"], false, "{result}");
        assert_eq!(result["structuredContent"]["state"], "killed", "{result}");
        assert_eq!(
            result["structuredContent"]["lifetimeExpired"], true,
            "{result}"
        );
        assert!(
            result["structuredContent"]["exitCode"].is_null(),
            "{result}"
        );
    }
}

#[test]
fn codemode_outer_request_cancellation_only_ends_its_wait() {
    let marker = Scratch::new("codemode-cancel-marker");
    let mut server = codemode_server();
    let code = format!(
        "await tools.write({{path:{},content:'started'}}); while (true) {{}}",
        json!(marker.to_string_lossy())
    );
    server.next_id += 1;
    let request_id = server.next_id;
    server.send(json!({"jsonrpc":"2.0","id":request_id,"method":"tools/call","params":{"name":"codemode","arguments":{"code":code,"waitMs":60000}}}));
    let deadline = Instant::now() + Duration::from_secs(30);
    while !marker.exists() {
        assert!(
            Instant::now() < deadline,
            "script did not create its marker"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    server.send(json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":request_id,"reason":"test cancellation"}}));
    // rmcp may drop a cancelled response; list observes the task after cancellation.
    let (listed, _) = server.ok(json!({"action":"list"}));
    let script = listed["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|task| task["kind"] == "codemode")
        .unwrap();
    assert_eq!(script["state"], "running", "{listed}");
    server.ok(json!({"action":"kill","taskId":script["taskId"]}));
}

#[test]
fn codemode_script_admission_limit_is_released_after_kill() {
    let mut server = codemode_server();
    let mut ids = Vec::new();
    for _ in 0..4 {
        let result = code_result(&mut server, json!({"code":"while (true) {}", "waitMs":0}));
        assert_eq!(result["isError"], false, "{result}");
        ids.push(
            result["structuredContent"]["taskId"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    let rejected = code_result(&mut server, json!({"code":"return 1", "waitMs":0}));
    assert_eq!(rejected["isError"], true, "{rejected}");
    let (listed, _) = server.ok(json!({"action":"list"}));
    assert_eq!(listed["tasks"].as_array().unwrap().len(), 4, "{listed}");
    for id in ids {
        server.ok(json!({"action":"kill", "taskId":id}));
    }
    let next = code_result(&mut server, json!({"code":"return 'room again'"}));
    assert_eq!(next["structuredContent"]["exitCode"], 0, "{next}");
}

#[test]
fn codemode_images_follow_each_readers_cursor_even_when_text_is_truncated() {
    let mut server = codemode_server();
    let code = format!(
        "for(let i=0;i<6;i++) image({}); text('tail');",
        json!(image_data_url(8, 6))
    );
    let result = code_result(
        &mut server,
        json!({"code":code,"truncate":"tail","maxBytes":100}),
    );
    assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
    assert_eq!(image_count(&result), 4, "{result}");
    assert_eq!(
        result["structuredContent"]["codemode"]["imagesAttached"], 4,
        "{result}"
    );
    let omitted = result["structuredContent"]["codemode"]["imagesNotAttached"]
        .as_array()
        .unwrap();
    assert_eq!(omitted.len(), 2, "{result}");
    assert_eq!(omitted[0]["reason"], "budget", "{result}");
    let path = omitted[0]["path"].as_str().unwrap();
    let read = server.request(
        "tools/call",
        json!({"name":"read","arguments":{"path":path}}),
    );
    assert_eq!(image_count(&read["result"]), 1, "{read}");
    let handle = result["structuredContent"]["taskId"].as_str().unwrap();
    let poll = server.request(
        "tools/call",
        json!({"name":"exec","arguments":{"action":"poll","taskId":handle,"waitMs":0}}),
    );
    assert_eq!(image_count(&poll["result"]), 0, "{poll}");
    let fresh = server.request("tools/call", json!({"name":"exec","arguments":{"action":"poll","taskId":bare_id(handle),"waitMs":0,"truncate":"head","maxBytes":4194304}}));
    assert_eq!(image_count(&fresh["result"]), 4, "{fresh}");
}

#[test]
fn codemode_images_resize_and_survive_structured_only_output() {
    let mut server = Server::start_with(|command| {
        command
            .env("FASTEXEC_CODEMODE", "true")
            .env("FASTEXEC_OUTPUT_MODE", "structured");
    });
    let code = format!("image({});", json!(image_data_url(2400, 6)));
    let result = code_result(&mut server, json!({"code":code}));
    assert_eq!(image_count(&result), 1, "{result}");
    let block = result["content"]
        .as_array()
        .unwrap()
        .iter()
        .find(|block| block["type"] == "image")
        .unwrap();
    let data = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        block["data"].as_str().unwrap(),
    )
    .unwrap();
    let image = image::load_from_memory(&data).unwrap();
    assert!(
        image.width() <= 2000 && image.height() <= 2000,
        "{}x{}",
        image.width(),
        image.height()
    );
    assert!(!code_text(&result).contains("[image 1:"), "{result}");
    assert!(
        result["structuredContent"]["output"]
            .as_str()
            .unwrap()
            .contains("[image 1:"),
        "{result}"
    );
}

#[test]
fn codemode_nested_poll_and_kill_do_not_consume_other_tasks_background_notifications() {
    let mut server = codemode_server();
    let (task, _) = server.ok(json!({
        "action":"start", "eof":false, "command":"read -r reply", "loginShell":false, "waitMs":0
    }));
    let other = task["taskId"].as_str().unwrap().to_owned();
    let code = format!(
        "await tools.exec({{action:'poll',taskId:{},input:'done\\n',waitMs:10000}}); await tools.exec({{action:'kill',taskId:{}}}); text('done');",
        json!(other),
        json!(other)
    );
    let result = code_result(&mut server, json!({"code":code}));
    assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
    assert!(
        code_text(&result).contains(bare_id(&other)) && code_text(&result).contains("Background:"),
        "{result}"
    );
}

#[test]
fn codemode_awaited_helper_tasks_do_not_pollute_outer_background_footer() {
    let mut server = codemode_server();
    let code = r#"
for (let i = 0; i < 6; i++) {
  const res = await tools.exec({
    action: 'start',
    command: i % 2 === 0 ? `printf ok-${i}` : `exit ${i}`,
    loginShell: false,
  });
  if (res.state !== 'exited') throw new Error('helper failed');
}
const delayed = await tools.exec({
  action: 'start',
  command: 'sleep 0.05; exit 3',
  loginShell: false,
  waitMs: 0,
});
const polled = await tools.exec({
  action: 'poll',
  taskId: delayed.taskId,
});
if (polled.exitCode !== 3) throw new Error('delayed helper exit');
return 'clean';
"#;
    let result = code_result(&mut server, json!({"code":code}));
    assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
    assert!(
        !code_text(&result).contains("Background:"),
        "awaited helper tasks leaked into outer Background footer: {}",
        code_text(&result)
    );
    let (_, next_text) = server.ok(json!({
        "action":"start", "command":"printf outer", "loginShell":false
    }));
    assert!(
        !next_text.contains("Background:"),
        "later outer call received stale helper notifications: {next_text}"
    );
}

#[test]
fn codemode_script_cwd_applies_to_file_tools_and_nested_exec() {
    let outer = Scratch::new("codemode-outer-cwd");
    let inner = Scratch::new("codemode-inner-cwd");
    std::fs::create_dir_all(&*outer).unwrap();
    std::fs::create_dir_all(&*inner).unwrap();
    let mut server = Server::start_with(|command| {
        command
            .current_dir(&*outer)
            .env("FASTEXEC_CODEMODE", "true")
            .env("FASTEXEC_OUTPUT_MODE", "both");
    });
    let code = format!(
        r#"
await tools.write({{path:'sub/item.txt', content:'one\ntwo\n'}});
await tools.edit({{path:'sub/item.txt', edits:[{{oldText:'two', newText:'TWO'}}]}});
const [readBack, runInner, runOuter] = await Promise.all([
  tools.read({{path:'sub/item.txt'}}),
  tools.exec({{action:'start', command:'printf inner-ok > from-exec.txt', loginShell:false}}),
  tools.exec({{action:'start', command:'printf outer-ok > from-override.txt', cwd:{}, loginShell:false}}),
]);
if (runInner.exitCode !== 0 || runOuter.exitCode !== 0) throw new Error('nested exec failed');
return {{ file: readBack.text }};
"#,
        json!(outer.to_string_lossy())
    );
    let result = code_result(&mut server, json!({"code":code, "cwd": &*inner}));
    assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
    assert!(inner.join("sub/item.txt").is_file());
    assert!(!outer.join("sub/item.txt").exists());
    assert_eq!(
        std::fs::read_to_string(inner.join("sub/item.txt")).unwrap(),
        "one\nTWO\n"
    );
    assert_eq!(
        std::fs::read_to_string(inner.join("from-exec.txt")).unwrap(),
        "inner-ok"
    );
    assert!(!outer.join("from-exec.txt").exists());
    assert_eq!(
        std::fs::read_to_string(outer.join("from-override.txt")).unwrap(),
        "outer-ok"
    );
    assert!(!inner.join("from-override.txt").exists());
    let parsed: Value =
        serde_json::from_str(result["structuredContent"]["output"].as_str().unwrap()).unwrap();
    assert_eq!(parsed["file"], "one\nTWO\n");
}

#[test]
fn file_tools_paginate_via_structured_content_and_honor_output_modes() {
    let dir = Scratch::new("file-structured");
    std::fs::create_dir_all(&*dir).unwrap();
    let big: String = (1..=2500)
        .map(|line| format!("line-{line:04}-payload-text\n"))
        .collect();
    std::fs::write(dir.join("big.txt"), &big).unwrap();
    let huge_multi: String = (1..=48_000)
        .map(|line| format!("row-{line:05}-{}\n", "x".repeat(90)))
        .collect();
    std::fs::write(dir.join("huge-multi.txt"), &huge_multi).unwrap();
    let long_single = format!("{{\"blob\":\"{}\"}}\n", "y".repeat(80 * 1024));
    std::fs::write(dir.join("long-single.jsonl"), &long_single).unwrap();

    let mut server = Server::start_with(|command| {
        command
            .current_dir(&*dir)
            .env("FASTEXEC_CODEMODE", "true")
            .env("FASTEXEC_OUTPUT_MODE", "both");
    });
    let code = r#"
const single = await tools.read({path:'big.txt'});
if (single.truncated !== false || single.shownLines !== 2500 || single.totalLines !== 2500) {
  throw new Error('2500-line file must fit in one nested read window');
}
const jsonl = await tools.read({path:'long-single.jsonl'});
if (jsonl.lineTooLong !== false || JSON.parse(jsonl.text).blob.length !== 80 * 1024) {
  throw new Error('nested read must accept >50 KiB single lines up to 1 MiB');
}
const utf8 = "你好，fastexec 🚀";
const encoded = new TextEncoder().encode(utf8);
const roundtrip = new TextDecoder().decode(encoded);
if (roundtrip !== utf8) throw new Error('TextEncoder/TextDecoder roundtrip failed');

let offset = 1;
let pages = 0;
const parts = [];
while (offset !== null) {
  const page = await tools.read({path:'huge-multi.txt', offset});
  if (page.text.includes('[Showing lines')) {
    throw new Error('truncation notice leaked into text');
  }
  parts.push(page.text);
  pages++;
  offset = page.nextOffset;
}
const joined = parts.join('');
return {singleBytes: single.text.length, pages, bytes: joined.length};
"#;
    let result = code_result(&mut server, json!({"code":code}));
    assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
    let summary: Value =
        serde_json::from_str(result["structuredContent"]["output"].as_str().unwrap()).unwrap();
    assert_eq!(summary["singleBytes"].as_u64().unwrap(), big.len() as u64);
    assert!(summary["pages"].as_u64().unwrap() >= 2, "{summary}");
    assert_eq!(summary["bytes"].as_u64().unwrap(), huge_multi.len() as u64);

    // Direct MCP structured-only mode keeps the 2000-line / 50 KiB cap and places text in structuredContent.text only.
    let mut structured_only = Server::start_with(|command| {
        command
            .current_dir(&*dir)
            .env("FASTEXEC_OUTPUT_MODE", "structured");
    });
    let reply = structured_only.request(
        "tools/call",
        json!({"name":"read", "arguments":{"path":"big.txt"}}),
    );
    let read_res = &reply["result"];
    assert_eq!(read_res["isError"], false, "{reply}");
    assert_eq!(read_res["structuredContent"]["truncated"], true, "{reply}");
    assert!(
        read_res["structuredContent"]["nextOffset"].is_u64(),
        "{reply}"
    );
    assert!(
        !read_res["structuredContent"]["text"]
            .as_str()
            .unwrap()
            .contains("[Showing lines"),
        "{reply}"
    );
    assert!(
        read_res["content"][0]["text"]
            .as_str()
            .unwrap()
            .starts_with("[Showing lines"),
        "{reply}"
    );
}

#[test]
fn codemode_waits_match_verbatim_output_on_start_and_poll() {
    let mut server = codemode_server();
    let marker = "\x1b[31mready";
    let started = code_result(
        &mut server,
        json!({
            "code": format!("text({}); while (true) {{}}", json!(marker)),
            "waitMs": 1000,
            "returnWhen": {"outputContains": [marker]},
        }),
    );
    let start_data = &started["structuredContent"];
    server.ok(json!({"action": "kill", "taskId": start_data["taskId"]}));
    assert_eq!(
        start_data["waitEndedBy"]["condition"], "output_contains",
        "{started}"
    );
    assert_eq!(start_data["waitEndedBy"]["text"], marker, "{started}");
    assert_eq!(start_data["output"], marker, "{started}");

    let (release, release_bash) = scratch("codemode-matcher-release");
    let wait_command = format!("while [ ! -f '{release_bash}' ]; do sleep 0.02; done");
    let started = code_result(
        &mut server,
        json!({
            "code": format!(
                "text('boot'); await tools.exec({{action:'start', command:{}, loginShell:false}}); text({}); while (true) {{}}",
                json!(wait_command), json!(marker)
            ),
            "waitMs": 10000,
            "returnWhen": {"outputContains": ["boot"]},
        }),
    );
    let handle = &started["structuredContent"]["taskId"];
    std::fs::write(&*release, "ready").unwrap();
    let (polled, text) = server.ok(json!({
        "action": "poll", "taskId": handle, "waitMs": 10000,
        "returnWhen": {"outputContains": [marker]},
    }));
    server.ok(json!({"action": "kill", "taskId": handle}));
    assert_eq!(
        polled["waitEndedBy"]["condition"], "output_contains",
        "{text}"
    );
    assert_eq!(polled["waitEndedBy"]["text"], marker, "{text}");
    assert!(
        polled["output"].as_str().unwrap().contains(marker),
        "{text}"
    );
}

#[test]
fn nested_list_marks_tasks_started_by_an_outer_call() {
    let mut server = codemode_server();
    let (started, _) = server.ok(json!({
        "action": "start", "command": "sleep 0.1; exit 4",
        "loginShell": false, "waitMs": 0,
    }));
    let handle = started["taskId"].as_str().unwrap();
    let listed = code_result(
        &mut server,
        json!({
            "code": format!(
                "await tools.exec({{action:'poll', taskId:{}}}); const list = await tools.exec({{action:'list', state:'finished'}}); return list.tasks.find(task => task.taskId === {}).exitCode;",
                json!(handle), json!(bare_id(handle)),
            ),
        }),
    );
    assert_eq!(listed["structuredContent"]["output"], "4", "{listed}");
    assert!(
        !code_text(&listed).contains("(Background:"),
        "the delivered nested list already reported the finished outer task: {listed}",
    );
}

#[test]
fn codemode_drains_inflight_spawns_before_publishing_started_tasks() {
    let mut server = codemode_server();
    // Fire unawaited starts and return immediately; any child that actually spawned must already
    // be present in the initial codemode result's startedTasks.
    let code = r#"
for (let i = 0; i < 4; i++) {
  tools.exec({action:'start', command:'sleep 30', loginShell:false, waitMs:60000});
}
await Promise.resolve();
return 'returned-early';
"#;
    let result = code_result(&mut server, json!({"code":code}));
    assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
    let reported: std::collections::HashSet<String> =
        result["structuredContent"]["codemode"]["startedTasks"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|entry| bare_id(entry["taskId"].as_str().unwrap()).to_owned())
            .collect();
    let (listed, _) = server.ok(json!({"action":"list"}));
    let running_bash: Vec<String> = listed["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|task| task["kind"] == "bash")
        .map(|task| task["taskId"].as_str().unwrap().to_owned())
        .collect();
    for id in &running_bash {
        assert!(
            reported.contains(id),
            "spawned bash task {id} missed initial startedTasks report {reported:?}: {result}"
        );
        server.ok(json!({"action":"kill", "taskId":id}));
    }
}

fn schema_description_words(node: &Value) -> usize {
    match node {
        Value::Object(fields) => fields
            .iter()
            .map(|(key, value)| {
                if key == "description" {
                    value.as_str().unwrap().split_whitespace().count()
                } else {
                    schema_description_words(value)
                }
            })
            .sum(),
        Value::Array(items) => items.iter().map(schema_description_words).sum(),
        _ => 0,
    }
}

#[test]
fn server_instructions_and_tool_descriptions_stay_within_their_word_budget() {
    let mut disabled = Server::start();
    let init_off = disabled.request(
        "initialize",
        json!({"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"probe","version":"0"}}),
    );
    let instructions_off = init_off["result"]["instructions"].as_str().unwrap();
    assert!(!instructions_off.contains("codemode"), "{instructions_off}");

    let mut enabled = codemode_server();
    let init_on = enabled.request(
        "initialize",
        json!({"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"probe","version":"0"}}),
    );
    let instructions_on = init_on["result"]["instructions"].as_str().unwrap();
    assert!(
        instructions_on.contains("codemode") && instructions_on.contains("Prefer codemode"),
        "{instructions_on}"
    );

    // v0.3.0 stdio totals, excluding the host line, whose paths may contain spaces.
    let platform_words = if cfg!(windows) {
        59
    } else if cfg!(target_os = "macos") {
        69
    } else {
        0
    };
    for (server, instructions, baseline) in [
        (&mut disabled, instructions_off, 2233),
        (&mut enabled, instructions_on, 2987),
    ] {
        let listed = server.request("tools/list", json!({}));
        let mut words = instructions.split_whitespace().count();
        for tool in listed["result"]["tools"].as_array().unwrap() {
            let text = tool["description"].as_str().unwrap();
            let text = text.split_once("\n\nHost: ").map_or(text, |(body, _)| body);
            words +=
                text.split_whitespace().count() + schema_description_words(&tool["inputSchema"]);
        }
        let limit = baseline + platform_words;
        assert!(words <= limit, "{words} advertised words exceed {limit}");
    }
}

#[test]
fn codemode_preserves_long_json_raw_text_nested_windows_waits_and_sh_quote() {
    let mut server = codemode_server();

    // P0-1: 10,000-char JSON line stays whole and parses cleanly.
    let big_json = code_result(
        &mut server,
        json!({
            "code": "return { payload: 'x'.repeat(10000), ok: 1 };",
            "maxBytes": 16384
        }),
    );
    assert_eq!(big_json["structuredContent"]["exitCode"], 0, "{big_json}");
    assert!(
        big_json["structuredContent"].get("cutLines").is_none(),
        "{big_json}"
    );
    let body = big_json["structuredContent"]["output"].as_str().unwrap();
    let parsed: Value = serde_json::from_str(body.trim()).expect("whole JSON line parses");
    assert_eq!(parsed["payload"].as_str().unwrap().len(), 10_000);

    // P0-1 + P2-2: An 800-byte JSON line fits whole inside maxBytes: 1000 without NOTES_RESERVE
    // shaving it down or forcing ` · log ` into the status line; tight budgets refit cleanly.
    let near_cap = code_result(
        &mut server,
        json!({
            "code": "return { nearCap: 'q'.repeat(780) };",
            "maxBytes": 1000
        }),
    );
    let near_text = code_text(&near_cap);
    assert!(
        body_of(near_text).len() <= 1000,
        "len={} {near_text}",
        body_of(near_text).len()
    );
    assert!(
        near_cap["structuredContent"].get("cutLines").is_none(),
        "{near_cap}"
    );
    assert!(!near_text.contains(" · log "), "{near_text}");
    let parsed_near: Value = serde_json::from_str(
        near_cap["structuredContent"]["output"]
            .as_str()
            .unwrap()
            .trim(),
    )
    .expect("near-cap JSON parses whole");
    assert_eq!(parsed_near["nearCap"].as_str().unwrap().len(), 780);

    for tight in [100_u64, 150, 220, 320] {
        let tight_res = code_result(
            &mut server,
            json!({
                "code": "return 'T'.repeat(2000);",
                "maxBytes": tight
            }),
        );
        let tight_text = code_text(&tight_res);
        let tight_body = tight_res["structuredContent"]["output"].as_str().unwrap();
        assert!(
            tight_body.len() <= tight as usize,
            "tight={tight} len={}: {tight_text}",
            tight_body.len()
        );
        assert!(
            !tight_body.contains(" … [cut]") && tight_body.matches("line cut at ").count() <= 1,
            "double-cut marker at tight={tight}: {tight_body}"
        );
    }

    // P0-1: Over-maxBytes single line cuts within budget, sets cutLines: 1, and names log for recovery.
    let over_budget = code_result(
        &mut server,
        json!({
            "code": "return 'z'.repeat(10000);",
            "maxBytes": 2048
        }),
    );
    let over_text = code_text(&over_budget);
    assert!(
        body_of(over_text).len() <= 2048,
        "len={} {over_text}",
        body_of(over_text).len()
    );
    assert!(over_text.contains(" … [line cut at "), "{over_text}");
    assert!(over_text.contains(" · log "), "{over_text}");
    assert_eq!(
        over_budget["structuredContent"]["cutLines"], 1,
        "{over_budget}"
    );
    let log_path = over_budget["structuredContent"]["logPath"]
        .as_str()
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(log_path).unwrap().trim().len(),
        10_000
    );

    // P0-1: CR and trailing spaces + ANSI sequences are preserved verbatim in codemode output.
    let raw_kept = code_result(
        &mut server,
        json!({
            "code": "text('first\\rsecond  '); text('\\x1b[31mred\\x1b[0m  ');"
        }),
    );
    let raw_out = raw_kept["structuredContent"]["output"].as_str().unwrap();
    assert_eq!(raw_out, "first\rsecond  \n\x1b[31mred\x1b[0m  ");

    // P0-2, P0-3, P2-8: nested uncut lines, >240s waitMs, input 2s poll default, >4 MiB log recovery,
    // nested transcript maxBytes-only tail default, and sh.quote bash round-trip.
    let nested = code_result(
        &mut server,
        json!({
            "code": r#"
                const wide = await tools.exec({
                    action: "start",
                    loginShell: false,
                    command: "awk 'BEGIN { s = sprintf(\"%5000s\", \"\"); gsub(/ /, \"w\", s); print s; for (i = 1; i <= 3000; i++) print i }'"
                });
                if ("cutLines" in wide || "omittedRange" in wide) throw new Error("unexpected cut/omit: " + JSON.stringify(wide));
                const wLines = wide.output.trimEnd().split("\n");
                if (wLines.length !== 3001 || wLines[0].length !== 5000 || wLines[3000] !== "3000") {
                    throw new Error("wide mismatch: len=" + wLines.length + " first=" + wLines[0].length);
                }

                const hiWait = await tools.exec({
                    action: "start",
                    loginShell: false,
                    waitMs: 600000,
                    command: "printf fast"
                });
                if (hiWait.output !== "fast") throw new Error("hiWait: " + JSON.stringify(hiWait));

                const openCat = await tools.exec({
                    action: "start",
                    loginShell: false,
                    eof: false,
                    waitMs: 50,
                    command: "cat"
                });
                const t0 = Date.now();
                const polled = await tools.exec({
                    action: "poll",
                    taskId: openCat.taskId,
                    input: "echoed\n"
                });
                const dt = Date.now() - t0;
                await tools.exec({ action: "kill", taskId: openCat.taskId });
                if (polled.state !== "running" || polled.output !== "echoed" || dt < 1500 || dt > 12000) {
                    throw new Error("poll timing/output: dt=" + dt + " " + JSON.stringify(polled));
                }

                const single4m = await tools.exec({
                    action: "start",
                    loginShell: false,
                    command: "awk 'BEGIN { s = sprintf(\"%1000s\", \"\"); gsub(/ /, \"x\", s); for (i = 0; i < 4200; i++) printf \"%s\", s; printf \"\\n\" }'"
                });
                if (single4m.cutLines !== 1) throw new Error("single4m: " + JSON.stringify(single4m));
                const afterSingle = await tools.exec({ action: "poll", taskId: single4m.taskId, waitMs: 0 });
                if (afterSingle.output !== "") throw new Error("expected empty follow-up poll");

                const many4m = await tools.exec({
                    action: "start",
                    loginShell: false,
                    command: "awk 'BEGIN { s = sprintf(\"%1023s\", \"\"); gsub(/ /, \"y\", s); for (i = 1; i <= 4100; i++) print i \":\" s }'"
                });
                if (!Array.isArray(many4m.omittedRange) || many4m.omittedRange[1] !== 4100) {
                    throw new Error("many4m omittedRange: " + JSON.stringify(many4m.omittedRange));
                }
                const recovered = await tools.read({
                    path: many4m.logPath,
                    offset: many4m.omittedRange[0],
                    limit: 50
                });
                if (!recovered.text.includes("4100:")) throw new Error("log recovery missed 4100");

                const ptyRun = await tools.exec({
                    action: "start",
                    pty: true,
                    loginShell: false,
                    command: "awk 'BEGIN { for (i = 1; i <= 200; i++) printf \"line-%03d-padding-text\\n\", i }'"
                });
                const tr = await tools.exec({
                    action: "transcript",
                    taskId: ptyRun.taskId,
                    maxBytes: 400
                });
                if (!tr.output.includes("line-200-padding-text") || tr.output.includes("line-001-padding-text")) {
                    throw new Error("transcript did not tail: " + tr.output);
                }

                const tricky = "space 'quote' $HOME `uname` \\ \n end";
                const rt = await tools.exec({
                    action: "start",
                    loginShell: false,
                    raw: true,
                    command: "printf '%s' " + sh.quote(tricky)
                });
                if (rt.output !== tricky) throw new Error("sh.quote mismatch: " + JSON.stringify(rt.output));
                return "all-nested-checks-ok";
            "#
        }),
    );
    assert_eq!(nested["structuredContent"]["exitCode"], 0, "{nested}");
    assert!(
        nested["structuredContent"]["output"]
            .as_str()
            .unwrap()
            .contains("all-nested-checks-ok"),
        "{nested}"
    );

    // P0-2: Unbounded nested wait over 30s resolves without nested poll while outer waitMs returns a handle.
    let started_long = code_result(
        &mut server,
        json!({
            "code": r#"
                const r = await tools.exec({
                    action: "start",
                    loginShell: false,
                    command: "sleep 31; printf after-thirty-seconds"
                });
                if (r.state !== "exited" || r.output !== "after-thirty-seconds") {
                    throw new Error("unexpected nested wait end: " + JSON.stringify(r));
                }
                return "long-wait-resolved";
            "#,
            "waitMs": 300
        }),
    );
    assert_eq!(
        started_long["structuredContent"]["state"], "running",
        "{started_long}"
    );
    let script_handle = started_long["structuredContent"]["taskId"]
        .as_str()
        .unwrap()
        .to_owned();
    let (polled_long, polled_text) = server.ok(json!({
        "action": "poll",
        "taskId": script_handle,
        "waitMs": 45000
    }));
    assert_eq!(polled_long["state"], "exited", "{polled_text}");
    assert_eq!(polled_long["exitCode"], 0, "{polled_text}");
    assert!(polled_text.contains("long-wait-resolved"), "{polled_text}");
}

#[test]
fn pipe_start_stdin_defaults_to_eof_delivers_1mib_and_warns_on_early_close() {
    let (dir_guard, bash_dir) = scratch("pipe-stdin-p04");
    std::fs::create_dir_all(&*dir_guard).unwrap();
    std::fs::write(dir_guard.join("sample.txt"), "needle_token\nother\n").unwrap();
    let mut server = codemode_server();

    let (cat_out, cat_text) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "cat",
        "waitMs": 5000
    }));
    assert_eq!(cat_out["state"], "exited", "{cat_text}");
    assert_eq!(cat_out["exitCode"], 0, "{cat_text}");
    assert!(
        !cat_text.contains(" · log "),
        "clean exit status omits log segment: {cat_text}"
    );
    assert!(
        cat_out["logPath"].is_string(),
        "structuredContent.logPath stays present: {cat_out}"
    );

    let (grep_out, grep_text) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "cwd": bash_dir,
        "command": "if command -v rg >/dev/null 2>&1; then rg --no-filename needle_token; else [[ ! -p /dev/stdin ]] && grep needle_token sample.txt; fi",
        "waitMs": 5000
    }));
    assert_eq!(grep_out["exitCode"], 0, "{grep_text}");
    assert_eq!(grep_out["output"], "needle_token", "{grep_out}");

    let (sorted, sorted_text) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "sort -r",
        "input": "a\nb\n"
    }));
    assert_eq!(sorted["exitCode"], 0, "{sorted_text}");
    assert_eq!(sorted["output"], "b\na", "{sorted}");

    let (empty_in, _) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "cat",
        "input": ""
    }));
    assert_eq!(empty_in["exitCode"], 0, "{empty_in}");

    let (sleep_null, _) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "sleep 5",
        "waitMs": 50
    }));
    let null_id = sleep_null["taskId"].as_str().unwrap().to_owned();
    server.ok(json!({"action": "poll", "taskId": &null_id, "eof": true, "waitMs": 0}));
    let (_, err_text, is_err) = server.call(json!({
        "action": "poll",
        "taskId": &null_id,
        "input": "hello\n",
        "waitMs": 0
    }));
    assert!(is_err && err_text.contains("eof: false"), "{err_text}");
    server.ok(json!({"action": "kill", "taskId": &null_id}));

    for payload in [
        "x".repeat(1 << 20),
        "\n".repeat(1 << 20),
        "é".repeat(512 * 1024),
        "\0".repeat(1 << 20),
    ] {
        assert_eq!(payload.len(), 1 << 20);
        let (wc, wc_text) = server.ok(json!({
            "action": "start",
            "loginShell": false,
            "command": "wc -c",
            "input": payload
        }));
        assert_eq!(wc["exitCode"], 0, "{wc_text}");
        assert_eq!(wc["output"].as_str().unwrap().trim(), "1048576", "{wc}");
    }
    let (_, over_text, over_err) = server.call(json!({
        "action": "start",
        "loginShell": false,
        "command": "cat",
        "input": "a".repeat((1 << 20) + 1)
    }));
    assert!(over_err && over_text.contains("1 MiB"), "{over_text}");

    let nested_1m = code_result(
        &mut server,
        json!({
            "code": r#"
                for (const unit of ["x", "\n", "é", "\0"]) {
                    const ok = await tools.exec({
                        action: "start",
                        loginShell: false,
                        command: "wc -c",
                        input: unit.repeat(unit === "é" ? 524288 : 1048576)
                    });
                    if (ok.exitCode !== 0 || ok.output.trim() !== "1048576")
                        throw new Error("wc: " + ok.output);
                }
                const before = (await tools.exec({action: "list"})).totalCount;
                let refused = false;
                try {
                    await tools.exec({
                        action: "start",
                        loginShell: false,
                        command: "cat",
                        input: "a".repeat(1048577)
                    });
                } catch (e) {
                    if (e.name === "ToolError") refused = true;
                }
                if (!refused) throw new Error("expected ToolError for >1 MiB");
                const after = (await tools.exec({action: "list"})).totalCount;
                if (after !== before) throw new Error("refused start created a task");
                return "nested-1mib-ok";
            "#
        }),
    );
    assert_eq!(nested_1m["structuredContent"]["exitCode"], 0, "{nested_1m}");

    let (early_close, early_text) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "head -c 10",
        "input": "abcdefghij".repeat(104_858)[..(1 << 20)].to_owned()
    }));
    assert_eq!(early_close["ok"], true, "{early_text}");
    assert_eq!(early_close["output"], "abcdefghij", "{early_close}");
    let warn = early_close["inputWarning"]
        .as_str()
        .expect("inputWarning set on broken pipe");
    assert!(
        warn.starts_with("stdin closed after ") && warn.ends_with(" of 1048576 bytes"),
        "{warn}"
    );
    let (early_open, _) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "head -c 10",
        "input": "z".repeat(1 << 20),
        "eof": false,
        "waitMs": 0
    }));
    let early_open_id = early_open["taskId"].as_str().unwrap().to_owned();
    let (eof_after_bp, _) = server.ok(json!({
        "action": "poll",
        "taskId": early_open_id,
        "eof": true,
        "waitMs": 5000
    }));
    assert!(eof_after_bp.get("inputError").is_none(), "{eof_after_bp}");

    let unread_t0 = Instant::now();
    let (unread, _) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "sleep 30",
        "input": "q".repeat(1 << 20),
        "eof": false,
        "waitMs": 150
    }));
    assert_eq!(unread["state"], "running", "{unread}");
    assert!(unread_t0.elapsed() < Duration::from_secs(5));
    let unread_id = unread["taskId"].as_str().unwrap().to_owned();
    let (killed_unread, _) = server.ok(json!({"action": "kill", "taskId": unread_id}));
    assert_eq!(killed_unread["state"], "killed", "{killed_unread}");

    let (ord_start, _) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "cat",
        "input": "first-line\n",
        "eof": false,
        "waitMs": 0
    }));
    let ord_id = ord_start["taskId"].as_str().unwrap().to_owned();
    let mut parts = Vec::new();
    if let Some(s) = ord_start["output"].as_str().filter(|s| !s.is_empty()) {
        parts.push(s.to_owned());
    }
    let (ord_poll, _) = server.ok(json!({
        "action": "poll",
        "taskId": ord_id,
        "input": "second-line\n",
        "eof": true,
        "waitMs": 5000
    }));
    if let Some(s) = ord_poll["output"].as_str().filter(|s| !s.is_empty()) {
        parts.push(s.to_owned());
    }
    assert_eq!(parts.join("\n"), "first-line\nsecond-line");
}

#[test]
fn completed_screen_and_empty_codemode_lines_follow_the_rendered_body() {
    let mut server = codemode_server();
    let (screen, text) = server.ok(json!({
        "action":"start", "command":"printf %0220d 0", "loginShell":false,
        "pty":true, "cols":90, "rows":5, "screen":true, "maxBytes":360
    }));
    assert!(screen.get("omittedRows").is_none(), "{text}");
    assert_eq!(
        screen["output"].as_str().unwrap().matches('0').count(),
        220,
        "{text}"
    );
    assert!(!text.contains(" · log "), "{text}");
    assert!(body_of(&text).len() <= 360, "{text}");

    let blank = code_result(&mut server, json!({"code":"text('')"}));
    assert!(
        code_text(&blank).starts_with("\n\n["),
        "{}",
        code_text(&blank)
    );
    let (_, next) =
        server.ok(json!({"action":"poll","taskId":blank["structuredContent"]["taskId"]}));
    assert!(next.starts_with("(no new output)"), "{next}");
}

#[cfg(unix)]
#[test]
fn nested_stream_and_transcript_fit_the_status_after_a_long_log_path_is_shortened() {
    let scratch = Scratch::new("long-window-path");
    let mut dir = scratch.to_path_buf();
    for _ in 0..6 {
        dir.push("x".repeat(110));
    }
    std::fs::create_dir_all(&dir).unwrap();
    let mut server = Server::start_with(|command| {
        command
            .env("TMPDIR", &dir)
            .env("FASTEXEC_OUTPUT_MODE", "both")
            .env("FASTEXEC_CODEMODE", "true");
    });
    let result = code_result(
        &mut server,
        json!({
            "code": r#"
            const task = await tools.exec({
                action: "start", command: "printf '%0400d\\n' 1; sleep 30", loginShell: false,
                pty: true, cols: 500, rows: 5, maxBytes: 1000, returnWhen: {outputContains: ["0000"]}
            });
            const transcript = await tools.exec({action: "transcript", taskId: task.taskId, maxBytes: 1000});
            await tools.exec({action: "kill", taskId: task.taskId});
            return [task.output, transcript.output];
        "#
        }),
    );
    let bodies: Vec<String> =
        serde_json::from_str(result["structuredContent"]["output"].as_str().unwrap())
            .unwrap_or_else(|_| panic!("{result}"));
    let expected = format!("{:0400}", 1);
    assert_eq!(bodies[0], expected);
    assert!(bodies[1].lines().any(|line| line == expected), "{bodies:?}");
}

#[cfg(target_os = "linux")]
#[test]
fn kill_waits_for_output_closure_and_reports_unconfirmed_capture() {
    let mut server = codemode_server();
    let (start, _) = server.ok(json!({
        "action":"start", "loginShell":false, "eof":false,
        "command":"printf 'ready %s\\n' \"$BASHPID\"; read -r done",
        "returnWhen":{"outputContains":["ready "]}
    }));
    let pid: u32 = start["output"]
        .as_str()
        .unwrap()
        .trim()
        .strip_prefix("ready ")
        .unwrap()
        .parse()
        .unwrap();
    // Hold one writer independently of the completed tree to control when capture reaches EOF.
    let mut writer = std::fs::OpenOptions::new()
        .write(true)
        .open(format!("/proc/{pid}/fd/1"))
        .unwrap();
    let id = &start["taskId"];
    let (exited, _) = server
        .ok(json!({"action":"poll", "taskId":id, "input":"done\n", "eof":true, "waitMs":10000}));
    assert_eq!(exited["state"], "exited", "{exited}");
    assert_eq!(exited["outputOpen"], true, "{exited}");

    let (pending, text) = server.ok(json!({"action":"kill", "taskId":id, "maxBytes":100}));
    assert_eq!(pending["outputOpen"], true, "{pending}");
    assert!(text.contains("output did not close within 5 s"), "{text}");
    let closer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(150));
        writeln!(writer, "late-output").unwrap();
    });
    let (ended, text) = server.ok(json!({"action":"kill", "taskId":id}));
    closer.join().unwrap();
    assert!(ended.get("outputOpen").is_none(), "{text}");
    assert_eq!(ended["output"], "late-output", "{text}");
    let (after, _) = server.ok(json!({"action":"poll", "taskId":id}));
    assert_eq!(after["output"], "");
}

#[test]
fn kill_delivers_unseen_output_gates_concurrent_polls_and_attaches_codemode_images() {
    let mut server = codemode_server();

    let (started, _) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "printf 'first\\n'; sleep 0.3; printf 'after-start-before-kill\\n'; sleep 60",
        "waitMs": 100
    }));
    let handle = started["taskId"].as_str().unwrap().to_owned();
    let bare = bare_id(&handle).to_owned();
    std::thread::sleep(Duration::from_millis(500));
    let (killed, kill_text) = server.ok(json!({"action": "kill", "taskId": &handle}));
    assert_eq!(killed["state"], "killed", "{kill_text}");
    assert!(killed.get("waitEndedBy").is_none(), "{killed}");
    let combined = format!(
        "{}{}",
        started["output"].as_str().unwrap_or_default(),
        killed["output"].as_str().unwrap_or_default()
    );
    assert!(combined.contains("after-start-before-kill"), "{combined}");
    let (after_kill, _) = server.ok(json!({"action": "poll", "taskId": &handle, "waitMs": 0}));
    assert_eq!(after_kill["output"], "", "{after_kill}");

    let (bare_kill, bare_kill_text) = server.ok(json!({"action": "kill", "taskId": &bare}));
    assert!(
        bare_kill["output"].as_str().unwrap().contains("first"),
        "{bare_kill}"
    );
    assert!(bare_kill_text.contains("new reader"), "{bare_kill_text}");

    let (conc_start, _) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "printf 'ready\\n'; sleep 0.2; printf 'before-concurrent-kill\\n'; sleep 60",
        "returnWhen": {"outputContains": ["ready"]},
        "waitMs": 5000
    }));
    let conc_id = conc_start["taskId"].as_str().unwrap().to_owned();
    std::thread::sleep(Duration::from_millis(400));
    server.next_id += 1;
    let poll_req = server.next_id;
    server.send(json!({
        "jsonrpc": "2.0",
        "id": poll_req,
        "method": "tools/call",
        "params": {"name": "exec", "arguments": {"action": "poll", "taskId": &conc_id, "waitMs": 30000}}
    }));
    std::thread::sleep(Duration::from_millis(100));
    server.next_id += 1;
    let kill_req = server.next_id;
    server.send(json!({
        "jsonrpc": "2.0",
        "id": kill_req,
        "method": "tools/call",
        "params": {"name": "exec", "arguments": {"action": "kill", "taskId": &conc_id}}
    }));
    // A JSON-RPC client may await either reply first.
    let r2 = server.response(kill_req);
    let r1 = server.response(poll_req);
    assert_eq!(r1["result"]["isError"], false, "{r1}");
    assert_eq!(r2["result"]["isError"], false, "{r2}");
    let out1 = r1["result"]["structuredContent"]["output"]
        .as_str()
        .unwrap_or_default();
    let out2 = r2["result"]["structuredContent"]["output"]
        .as_str()
        .unwrap_or_default();
    let merged = format!("{out1}{out2}");
    assert_eq!(
        merged.matches("before-concurrent-kill").count(),
        1,
        "expected output conserved once across poll+kill: out1={out1:?} out2={out2:?}"
    );

    let url = image_data_url(4, 4);
    let img_script = code_result(
        &mut server,
        json!({
            "code": format!(
                "image({url:?}); await tools.exec({{action:'start', loginShell:false, command:'sleep 30'}});"
            ),
            "waitMs": 200
        }),
    );
    let cm_handle = img_script["structuredContent"]["taskId"]
        .as_str()
        .unwrap()
        .to_owned();
    let child_tasks: Vec<String> = img_script["structuredContent"]["codemode"]["startedTasks"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|t| t["taskId"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    let already_attached = image_count(&img_script);
    let kill_reply = server.request(
        "tools/call",
        json!({"name": "exec", "arguments": {"action": "kill", "taskId": cm_handle}}),
    );
    let kill_attached = image_count(&kill_reply["result"]);
    assert_eq!(
        already_attached + kill_attached,
        1,
        "{img_script} / {kill_reply}"
    );
    for child in child_tasks {
        let _ = server.call(json!({"action": "kill", "taskId": child}));
    }
}

#[test]
fn paths_read_edit_list_false_null_and_cli_contracts_hold_end_to_end() {
    let (root, _) = scratch("p1-p2-contracts");
    let home_dir = root.join("home");
    let work_dir = root.join("work");
    std::fs::create_dir_all(&home_dir).unwrap();
    std::fs::create_dir_all(work_dir.join("sub")).unwrap();

    let mut server = Server::start_with(|cmd| {
        cmd.current_dir(&work_dir)
            .env("FASTEXEC_CODEMODE", "true")
            .env("FASTEXEC_OUTPUT_MODE", "both")
            .env("HOME", &home_dir)
            .env("USERPROFILE", &home_dir);
    });

    let w_rel = server.request(
        "tools/call",
        json!({"name": "write", "arguments": {"path": "sub/empty.txt", "content": ""}}),
    );
    let w_path = w_rel["result"]["structuredContent"]["path"]
        .as_str()
        .unwrap();
    assert!(
        !w_path.contains('\\') && w_path.ends_with("/sub/empty.txt"),
        "{w_path}"
    );

    let r_empty = server.request(
        "tools/call",
        json!({"name": "read", "arguments": {"path": "sub/empty.txt", "offset": 1}}),
    );
    let r_sc = &r_empty["result"]["structuredContent"];
    assert_eq!(r_sc["totalLines"], 0, "{r_empty}");
    assert_eq!(r_sc["startLine"], 1, "{r_empty}");
    assert!(r_sc["endLine"].is_null(), "{r_empty}");
    assert_eq!(r_sc["shownLines"], 0, "{r_empty}");
    assert_eq!(r_sc["text"], "", "{r_empty}");

    server.request(
        "tools/call",
        json!({"name": "write", "arguments": {"path": "~/tilde.txt", "content": "alpha\r\nbeta"}}),
    );
    assert_eq!(
        std::fs::read_to_string(home_dir.join("tilde.txt")).unwrap(),
        "alpha\r\nbeta"
    );
    let r_tilde = server.request(
        "tools/call",
        json!({"name": "read", "arguments": {"path": "~/tilde.txt"}}),
    );
    assert_eq!(
        r_tilde["result"]["structuredContent"]["totalLines"], 2,
        "{r_tilde}"
    );
    assert_eq!(
        r_tilde["result"]["structuredContent"]["text"], "alpha\r\nbeta",
        "{r_tilde}"
    );

    let r_lim0 = server.request(
        "tools/call",
        json!({"name": "read", "arguments": {"path": "~/tilde.txt", "limit": 0}}),
    );
    assert_eq!(r_lim0["result"]["isError"], true, "{r_lim0}");
    let r_past = server.request(
        "tools/call",
        json!({"name": "read", "arguments": {"path": "~/tilde.txt", "offset": 99, "limit": 1}}),
    );
    let past_text = r_past["result"]["content"][0]["text"].as_str().unwrap();
    assert!(
        !past_text.starts_with('\n'),
        "no leading blank lines on empty body: {past_text:?}"
    );

    #[cfg(unix)]
    {
        let real_a = root.join("real_a");
        let real_b = root.join("real_b");
        std::fs::create_dir_all(real_a.join("inner")).unwrap();
        std::fs::create_dir_all(&real_b).unwrap();
        std::fs::write(real_a.join("sibling.txt"), "via-symlink-parent\n").unwrap();
        std::fs::write(real_b.join("sibling.txt"), "lexical-wrong-target\n").unwrap();
        std::os::unix::fs::symlink(real_a.join("inner"), real_b.join("link")).unwrap();
        let sym_file = real_b
            .join("link/../sibling.txt")
            .to_string_lossy()
            .into_owned();
        let sym_dir = real_b.join("link/..").to_string_lossy().into_owned();

        let via_read = server.request(
            "tools/call",
            json!({"name": "read", "arguments": {"path": &sym_file}}),
        );
        assert_eq!(
            via_read["result"]["structuredContent"]["text"], "via-symlink-parent\n",
            "{via_read}"
        );

        let via_edit = server.request(
            "tools/call",
            json!({
                "name": "edit",
                "arguments": {
                    "path": &sym_file,
                    "edits": [{"oldText": "via-symlink-parent", "newText": "edited-via-symlink"}]
                }
            }),
        );
        assert_eq!(via_edit["result"]["isError"], false, "{via_edit}");
        assert_eq!(
            std::fs::read_to_string(real_a.join("sibling.txt")).unwrap(),
            "edited-via-symlink\n"
        );
        assert_eq!(
            std::fs::read_to_string(real_b.join("sibling.txt")).unwrap(),
            "lexical-wrong-target\n"
        );

        let sym_written = real_b
            .join("link/../created.txt")
            .to_string_lossy()
            .into_owned();
        let via_write = server.request(
            "tools/call",
            json!({"name": "write", "arguments": {"path": &sym_written, "content": "created-in-real-a\n"}}),
        );
        assert_eq!(via_write["result"]["isError"], false, "{via_write}");
        assert_eq!(
            std::fs::read_to_string(real_a.join("created.txt")).unwrap(),
            "created-in-real-a\n"
        );
        assert!(!real_b.join("created.txt").exists());

        let (via_cwd, _) = server.ok(json!({
            "action": "start",
            "loginShell": false,
            "cwd": &sym_dir,
            "command": "cat sibling.txt"
        }));
        assert_eq!(via_cwd["output"], "edited-via-symlink", "{via_cwd}");

        let via_cm_cwd = code_result(
            &mut server,
            json!({
                "cwd": &sym_dir,
                "code": "const r = await tools.read({path: 'sibling.txt'}); return r.text.trim();"
            }),
        );
        assert_eq!(
            via_cm_cwd["structuredContent"]["output"], "edited-via-symlink",
            "{via_cm_cwd}"
        );
    }

    server.request(
        "tools/call",
        json!({"name": "write", "arguments": {"path": "sub/edit.txt", "content": "L1 start\nL2 mid\nL3 end\ndup\ndup\nkeep\n"}}),
    );
    let bad_edit = server.request(
        "tools/call",
        json!({
            "name": "edit",
            "arguments": {
                "path": "sub/edit.txt",
                "edits": [
                    {"oldText": "L1 start\nL2 mid\nL3 end\n", "newText": "SPAN0\n"},
                    {"oldText": "L1 start", "newText": "SPAN1"},
                    {"oldText": "L3 end", "newText": "SPAN2"},
                    {"oldText": "missing-token", "newText": "x"},
                    {"oldText": "dup", "newText": "DUP"},
                    {"oldText": "keep", "newText": "keep"}
                ]
            }
        }),
    );
    assert_eq!(bad_edit["result"]["isError"], true, "{bad_edit}");
    let failures = bad_edit["result"]["structuredContent"]["failures"]
        .as_array()
        .unwrap();
    let problems: Vec<&str> = failures
        .iter()
        .filter_map(|f| f["problem"].as_str())
        .collect();
    assert!(problems.contains(&"not_found"), "{failures:?}");
    assert!(problems.contains(&"ambiguous"), "{failures:?}");
    assert!(
        failures
            .iter()
            .any(|f| f["problem"] == "overlap" && f["index"] == 0 && f["with"] == json!([1, 2])),
        "expected containment overlap [1, 2] on entry 0: {failures:?}"
    );
    let noop_edit = server.request(
        "tools/call",
        json!({
            "name": "edit",
            "arguments": {
                "path": "sub/edit.txt",
                "edits": [{"oldText": "keep", "newText": "keep"}]
            }
        }),
    );
    assert_eq!(noop_edit["result"]["isError"], true, "{noop_edit}");
    assert_eq!(
        noop_edit["result"]["structuredContent"]["failures"],
        json!([]),
        "{noop_edit}"
    );

    let (t_ok, _) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "true",
        "eof": false,
        "raw": false,
        "screen": false,
        "waitMs": null
    }));
    let bare_t = bare_id(t_ok["taskId"].as_str().unwrap());
    let (epoch, _seq) = bare_t.split_once('-').expect("epoch-seq");
    assert_eq!(epoch.len(), 8, "8-char Crockford base32 epoch: {bare_t}");

    let (t_run, _) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "sleep 30",
        "waitMs": 50
    }));
    let run_id = t_run["taskId"].as_str().unwrap().to_owned();
    let (t_kill, _) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "sleep 30",
        "waitMs": 50
    }));
    server.ok(json!({"action": "kill", "taskId": t_kill["taskId"]}));

    let (list_active, _) = server.ok(json!({
        "action": "list",
        "state": "active",
        "raw": false,
        "eof": null
    }));
    assert_eq!(list_active["totalCount"], 1, "{list_active}");
    let (list_finished, _) = server.ok(json!({
        "action": "list",
        "state": "finished"
    }));
    assert!(
        list_finished["totalCount"].as_u64().unwrap() >= 2,
        "{list_finished}"
    );

    let (list_lim, list_lim_text) = server.ok(json!({"action": "list", "limit": 2}));
    assert_eq!(list_lim["tasks"].as_array().unwrap().len(), 2, "{list_lim}");
    assert!(list_lim["totalCount"].as_u64().unwrap() >= 3, "{list_lim}");
    assert!(list_lim_text.contains("Showing 2 of "), "{list_lim_text}");

    // P1-6: keepTree task after root exit counts as state "active" until killed.
    let (kt_out, _) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "keepTree": true,
        "command": "sleep 30 </dev/null >/dev/null 2>&1 &"
    }));
    let kt_handle = kt_out["taskId"].as_str().unwrap().to_owned();
    let kt_bare = bare_id(&kt_handle).to_owned();
    let (active_with_kt, _) = server.ok(json!({"action": "list", "state": "active"}));
    assert!(
        active_with_kt["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["taskId"] == kt_bare && t["keptRunning"] == true),
        "{active_with_kt}"
    );
    server.ok(json!({"action": "kill", "taskId": &kt_handle}));

    // P1-6: A warning on an older unselected task stays out when limit excludes it.
    let _ = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "head -c 5",
        "input": "w".repeat(1 << 20)
    }));
    let (with_warning, text) = server.ok(json!({"action": "list", "limit": 1}));
    assert!(text.contains("Warning:"), "{text}");
    assert!(
        text.ends_with(&format!(
            "Showing 1 of {} tasks; raise limit to see more.",
            with_warning["totalCount"]
        )),
        "{text}"
    );
    let (_, bounded) = server.ok(json!({"action":"list", "limit":1, "maxBytes":100}));
    let bounded_body = bounded.split("\n\nWarning:").next().unwrap();
    assert!(
        bounded_body.len() <= 100 && bounded.contains("Warning:"),
        "{bounded}"
    );
    assert!(bounded.ends_with("raise limit to see more."), "{bounded}");
    let _ = server.ok(json!({"action": "start", "loginShell": false, "command": "true"}));
    let (latest_only, latest_only_text) = server.ok(json!({"action": "list", "limit": 1}));
    assert!(
        latest_only.get("warnings").is_none() && !latest_only_text.contains("Warning:"),
        "{latest_only} / {latest_only_text}"
    );

    // P1-6: Tiny maxBytes on direct list leaves undelivered background notices due; nested list
    // marks script-owned tasks it delivers.
    let (due_bg, _) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "sleep 0.15; exit 4",
        "waitMs": 10
    }));
    let due_bare = bare_id(due_bg["taskId"].as_str().unwrap()).to_owned();
    std::thread::sleep(Duration::from_millis(300));
    let _ = server.ok(json!({"action": "list", "maxBytes": 100}));
    let (_, after_tiny_list) =
        server.ok(json!({"action": "start", "loginShell": false, "command": "true"}));
    assert!(
        after_tiny_list.contains(&due_bare),
        "tiny maxBytes list should leave {due_bare} due: {after_tiny_list}"
    );

    let nested_marks = code_result(
        &mut server,
        json!({
            "code": r#"
                const bg = await tools.exec({
                    action: "start",
                    loginShell: false,
                    command: "sleep 0.1; exit 5",
                    waitMs: 0
                });
                const waiter = await tools.exec({
                    action: "start",
                    loginShell: false,
                    command: "sleep 0.25"
                });
                const listed = await tools.exec({ action: "list", limit: 5 });
                const bare = bg.taskId.split(".")[0];
                if (!listed.tasks.some(t => t.taskId === bare && t.exitCode === 5)) {
                    throw new Error("nested list missed bg: " + JSON.stringify(listed));
                }
                return bare;
            "#
        }),
    );
    let nested_bare = nested_marks["structuredContent"]["output"]
        .as_str()
        .unwrap()
        .trim()
        .to_owned();
    assert!(
        !code_text(&nested_marks)
            .lines()
            .last()
            .unwrap()
            .contains(&nested_bare),
        "nested list should mark delivered script-owned task {nested_bare}: {}",
        code_text(&nested_marks)
    );

    server.ok(json!({"action": "kill", "taskId": run_id}));

    let (_, bad_id_msg, bad_id_err) = server.call(json!({"action": "poll", "taskId": "BAD_ID!"}));
    assert!(
        bad_id_err && bad_id_msg.contains("Unknown taskId"),
        "{bad_id_msg}"
    );

    let (_, true_msg, true_err) = server.call(json!({"action": "list", "eof": true}));
    assert!(
        true_err && true_msg.contains("action list; remove it."),
        "{true_msg}"
    );

    let (pty_scr, _) = server.ok(json!({
        "action": "start",
        "pty": true,
        "loginShell": false,
        "command": "printf hi",
        "screen": true,
        "raw": false
    }));
    assert!(pty_scr["cursor"].is_array(), "{pty_scr}");

    let cm_done = code_result(&mut server, json!({"code": "return 'cm-ok';"}));
    let cm_id = cm_done["structuredContent"]["taskId"].as_str().unwrap();
    server.ok(json!({
        "action": "poll",
        "taskId": cm_id,
        "eof": false,
        "screen": false,
        "raw": false,
        "input": null
    }));

    let bin_cmd = || {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        support::scrub_fastexec_env(&mut command);
        command
    };
    for flag in ["--version", "-V"] {
        let out = bin_cmd().arg(flag).output().unwrap();
        assert_eq!(out.status.code(), Some(0), "{flag}: {out:?}");
        assert!(
            String::from_utf8_lossy(&out.stdout).starts_with("fastexec "),
            "{out:?}"
        );
    }
    for flag in ["--help", "-h"] {
        let out = bin_cmd().arg(flag).output().unwrap();
        assert_eq!(out.status.code(), Some(0), "{flag}: {out:?}");
        assert!(
            String::from_utf8_lossy(&out.stdout).contains("daemon"),
            "{out:?}"
        );
    }
    for bad_args in [
        &["bogus"][..],
        &["--version", "extra"][..],
        &["--help", "extra"][..],
    ] {
        let out = bin_cmd().args(bad_args).output().unwrap();
        assert_eq!(out.status.code(), Some(2), "{bad_args:?}: {out:?}");
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("unknown argument"),
            "{out:?}"
        );
    }
    let eof_stdio = bin_cmd().stdin(Stdio::null()).output().unwrap();
    assert_eq!(eof_stdio.status.code(), Some(1), "{eof_stdio:?}");
}

#[cfg(windows)]
#[test]
fn windows_git_bash_drive_paths_and_forward_slash_reports() {
    let (root, _) = scratch("win-paths-p15");
    let spaced = root.join("dir with spaces");
    std::fs::create_dir_all(&spaced).unwrap();
    let native = spaced.to_string_lossy().replace('\\', "/");
    let (drive, rest) = native.split_once(":/").expect("Windows drive letter");
    let git_bash_dir = format!("/{}/{rest}", drive.to_ascii_lowercase());
    let git_bash_file = format!("{git_bash_dir}/hello.txt");

    let mut server = codemode_server();
    let listed = server.request("tools/list", json!({}));
    let exec_desc = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "exec")
        .unwrap()["description"]
        .as_str()
        .unwrap();
    let host_line = exec_desc.lines().last().unwrap();
    assert!(
        !host_line.contains('\\'),
        "Host line has backslash: {host_line}"
    );

    let w = server.request(
        "tools/call",
        json!({"name": "write", "arguments": {"path": &git_bash_file, "content": "win-ok\n"}}),
    );
    let reported = w["result"]["structuredContent"]["path"].as_str().unwrap();
    assert!(
        !reported.contains('\\') && reported.ends_with("/dir with spaces/hello.txt"),
        "{reported}"
    );

    let (ran, _) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "cwd": &git_bash_dir,
        "command": "cat hello.txt"
    }));
    assert_eq!(ran["output"], "win-ok", "{ran}");
    assert!(!ran["logPath"].as_str().unwrap().contains('\\'), "{ran}");

    let (root_drive, _) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "cwd": format!("/{}", drive.to_ascii_lowercase()),
        "command": "pwd"
    }));
    assert_eq!(root_drive["exitCode"], 0, "{root_drive}");

    let (tmp_msg, tmp_err) = server.file("read", json!({"path": "/tmp/x"}));
    assert!(tmp_err && tmp_msg.contains("cygpath -m"), "{tmp_msg}");
    let (rel_msg, rel_err) = server.file("read", json!({"path": format!("{drive}:relative.txt")}));
    assert!(rel_err, "{rel_msg}");

    // P1-1 Windows: default start PATH matches loginShell: true order, git & ls resolve, and
    // non-login snapshot execution runs at non-login speed.
    let path_cmd = "git --version >/dev/null && ls >/dev/null && IFS=: && for p in $PATH; do cygpath -m \"$p\"; done";
    let (snap_path, _) = server.ok(json!({
        "action": "start",
        "command": path_cmd
    }));
    let (login_path, _) = server.ok(json!({
        "action": "start",
        "loginShell": true,
        "command": path_cmd
    }));
    let snap_entries: Vec<&str> = snap_path["output"]
        .as_str()
        .unwrap()
        .lines()
        .take(5)
        .collect();
    let login_entries: Vec<&str> = login_path["output"]
        .as_str()
        .unwrap()
        .lines()
        .take(5)
        .collect();
    assert_eq!(
        snap_entries, login_entries,
        "Windows snapshot PATH order must match login shell"
    );
    let (fast_true, _) = server.ok(json!({"action": "start", "command": "true"}));
    assert!(
        fast_true["elapsedMs"].as_u64().unwrap() < 800,
        "expected fast non-login start on Windows, got {fast_true}"
    );
}

#[test]
fn p0_p1_p2_schemas_windows_reads_edits_and_tolerances_hold_end_to_end() {
    let dir = Scratch::new("ergonomics-e2e");
    std::fs::create_dir_all(&*dir).unwrap();
    let mut server = Server::start_with(|command| {
        command
            .current_dir(&*dir)
            .env("FASTEXEC_CODEMODE", "true")
            .env("FASTEXEC_OUTPUT_MODE", "both");
    });

    // Schema assertions (P0-1, P0-2, P1-2, P1-4).
    let listed = server.request("tools/list", json!({}));
    let tools_arr = listed["result"]["tools"].as_array().unwrap();
    let schema_of = |name: &str| -> &Value {
        &tools_arr.iter().find(|t| t["name"] == name).unwrap()["inputSchema"]
    };
    for name in ["exec", "codemode"] {
        let s = schema_of(name);
        assert_eq!(
            s["properties"]["truncate"]["enum"],
            json!(["head_tail", "head", "tail"]),
            "{name} truncate enum"
        );
        assert_eq!(s["properties"]["maxBytes"]["minimum"], 100);
        assert_eq!(s["properties"]["maxBytes"]["maximum"], 4_194_304);
    }
    let read_schema = schema_of("read");
    assert_eq!(read_schema["properties"]["offset"]["type"], "integer");
    assert!(read_schema["properties"]["offset"].get("minimum").is_none());
    assert_eq!(read_schema["properties"]["limit"]["minimum"], 1);
    let edit_item = &schema_of("edit")["properties"]["edits"]["items"];
    assert!(edit_item.get("prefixItems").is_none());
    assert_eq!(edit_item["properties"]["startLine"]["minimum"], 1);
    assert_eq!(edit_item["properties"]["endLine"]["minimum"], 1);

    // P0-1: maxBytes 4194304 accepted with all 3 placements; 4194305 and truncate "none" rejected.
    for placement in ["head_tail", "head", "tail"] {
        let (out, _) = server.ok(json!({
            "action": "start",
            "command": "printf ok",
            "truncate": placement,
            "maxBytes": 4_194_304
        }));
        assert_eq!(out["output"], "ok");
        let cm = code_result(
            &mut server,
            json!({"code": "return 'ok';", "truncate": placement, "maxBytes": 4_194_304}),
        );
        assert_eq!(cm["structuredContent"]["output"], "ok");
    }
    let bad_cm_none = code_result(
        &mut server,
        json!({"code": "return 1;", "truncate": "none"}),
    );
    assert_eq!(bad_cm_none["isError"], true);

    // P0-2 & P1-2: directory message, offset: 0 / limit: 0 errors, whole-file vs slice reads, negative offset.
    std::fs::create_dir_all(dir.join("adir")).unwrap();
    let (dir_msg, dir_err) = server.file("read", json!({"path": "adir"}));
    assert!(
        dir_err && dir_msg.contains("it is a directory; list it with exec."),
        "{dir_msg}"
    );
    let (_, off0_err) = server.file("read", json!({"path": "adir", "offset": 0}));
    assert!(off0_err);
    let (_, lim0_err) = server.file("read", json!({"path": "adir", "limit": 0}));
    assert!(lim0_err);

    std::fs::write(dir.join("lines50k.txt"), "x\n".repeat(50_003)).unwrap();
    let mut over_1m_line = "a".repeat(1_048_577);
    over_1m_line.push_str("\ny\n");
    std::fs::write(dir.join("line1m.txt"), &over_1m_line).unwrap();
    std::fs::write(dir.join("over4m.txt"), vec![b'z'; (4 << 20) + 1]).unwrap();
    std::fs::write(dir.join("badutf8.txt"), [0x6f_u8, 0xff, 0x6b, 0x0a]).unwrap();
    let five_m: String = (1..=100_000)
        .map(|i| format!("row-{i:06}-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n"))
        .collect();
    assert!(five_m.len() > 5_000_000);
    std::fs::write(dir.join("five_m.txt"), &five_m).unwrap();

    let p02 = code_result(
        &mut server,
        json!({
            "code": r#"
                const full50k = await tools.read({ path: "lines50k.txt" });
                if (full50k.totalLines !== 50003 || full50k.text.length !== 100006 || full50k.truncated !== false || full50k.nextOffset !== null) {
                    throw new Error("50k mismatch: " + JSON.stringify({ ...full50k, text: full50k.text.length }));
                }
                const full1m = await tools.read({ path: "line1m.txt" });
                if (full1m.text.length !== 1048580 || full1m.lineTooLong !== false) {
                    throw new Error("1m line mismatch: " + full1m.text.length);
                }
                let overErr = null;
                try {
                    await tools.read({ path: "over4m.txt" });
                } catch (e) {
                    overErr = e;
                }
                if (!overErr || overErr.name !== "ToolError" || !overErr.message.includes("offset") || !overErr.message.includes("limit") || ("text" in (overErr.result || {}))) {
                    throw new Error("over4m error mismatch: " + (overErr && overErr.message));
                }
                let utfErr = null;
                try {
                    await tools.read({ path: "badutf8.txt" });
                } catch (e) {
                    utfErr = e;
                }
                if (!utfErr || utfErr.name !== "ToolError") {
                    throw new Error("badutf8 whole-file must reject");
                }
                const lossySlice = await tools.read({ path: "badutf8.txt", offset: 1 });
                if (lossySlice.lossy !== true || lossySlice.text !== "o\uFFFDk\n") {
                    throw new Error("badutf8 slice mismatch: " + JSON.stringify(lossySlice));
                }
                const slice5m = await tools.read({ path: "five_m.txt", offset: 1, limit: 100 });
                if (slice5m.truncated !== true || slice5m.nextOffset !== 101 || slice5m.shownLines !== 100) {
                    throw new Error("slice5m mismatch: " + JSON.stringify({ ...slice5m, text: slice5m.text.length }));
                }
                const wholeKeys = Object.keys(full50k).sort().join(",");
                const sliceKeys = Object.keys(slice5m).sort().join(",");
                if (wholeKeys !== sliceKeys) {
                    throw new Error("key set mismatch: " + wholeKeys + " vs " + sliceKeys);
                }
                const negTail = await tools.read({ path: "lines50k.txt", offset: -50 });
                if (negTail.startLine !== 49954 || negTail.endLine !== 50003 || negTail.nextOffset !== null) {
                    throw new Error("negTail mismatch: " + JSON.stringify(negTail));
                }
                return "p02-ok";
            "#
        }),
    );
    assert_eq!(p02["structuredContent"]["exitCode"], 0, "{p02}");
    assert_eq!(p02["structuredContent"]["output"], "p02-ok");

    // Direct negative offset & P1-4 edit startLine/endLine over MCP.
    let direct_neg = server.request(
        "tools/call",
        json!({"name": "read", "arguments": {"path": "lines50k.txt", "offset": -5}}),
    )["result"]
        .clone();
    assert_eq!(direct_neg["structuredContent"]["startLine"], 49_999);
    assert_eq!(direct_neg["structuredContent"]["endLine"], 50_003);
    assert_eq!(direct_neg["structuredContent"]["nextOffset"], Value::Null);

    std::fs::write(dir.join("dup.txt"), "v = 1;\nkeep\nv = 1;\n").unwrap();
    let scoped_edit = server.request(
        "tools/call",
        json!({
            "name": "edit",
            "arguments": {
                "path": "dup.txt",
                "edits": [{"oldText": "v = 1;", "newText": "v = 9;", "startLine": 3, "endLine": 3}]
            }
        }),
    )["result"]
        .clone();
    assert_ne!(scoped_edit["isError"], true, "{scoped_edit}");
    assert_eq!(
        std::fs::read_to_string(dir.join("dup.txt")).unwrap(),
        "v = 1;\nkeep\nv = 9;\n"
    );

    // P2-1: keys: [] (with and without keyDelayMs) on pipe, PTY, and codemode; transcript ignores waitMs.
    let (pipe_task, _) = server.ok(json!({
        "action": "start",
        "command": "printf pipe-done"
    }));
    let (pipe_poll, _) = server.ok(json!({
        "action": "poll",
        "taskId": pipe_task["taskId"],
        "keys": [],
        "keyDelayMs": 500,
        "waitMs": 0
    }));
    assert_eq!(pipe_poll["state"], "exited");
    let (_, lone_delay_text, lone_delay_err) = server.call(json!({
        "action": "poll",
        "taskId": pipe_task["taskId"],
        "keyDelayMs": 500,
        "waitMs": 0
    }));
    assert!(
        lone_delay_err && lone_delay_text.contains("keyDelayMs applies only with keys"),
        "{lone_delay_text}"
    );

    let (pty_task, _) = server.ok(json!({
        "action": "start",
        "pty": true,
        "command": "printf 'pty-ready\\n'; sleep 30",
        "returnWhen": {"outputContains": ["pty-ready"]}
    }));
    let t0 = Instant::now();
    let (pty_poll, _) = server.ok(json!({
        "action": "poll",
        "taskId": pty_task["taskId"],
        "keys": [],
        "keyDelayMs": 500,
        "waitMs": 50
    }));
    assert_eq!(pty_poll["state"], "running");
    assert!(t0.elapsed() < Duration::from_millis(1500));

    let t_tr = Instant::now();
    let (tr_out, _) = server.ok(json!({
        "action": "transcript",
        "taskId": pty_task["taskId"],
        "waitMs": 5000
    }));
    assert!(
        t_tr.elapsed() < Duration::from_millis(1500),
        "transcript must ignore waitMs: {:?}",
        t_tr.elapsed()
    );
    assert!(tr_out["output"].as_str().unwrap().contains("pty-ready"));
    server.ok(json!({"action": "kill", "taskId": pty_task["taskId"]}));

    let cm_running = code_result(
        &mut server,
        json!({
            "code": "const p = sleep(30000); text('sleep-started'); await p;",
            "waitMs": 10000,
            "returnWhen": {"outputContains": ["sleep-started"]}
        }),
    );
    let cm_id = cm_running["structuredContent"]["taskId"]
        .as_str()
        .unwrap()
        .to_owned();
    let (cm_poll, _) = server.ok(json!({
        "action": "poll",
        "taskId": &cm_id,
        "keys": [],
        "keyDelayMs": 250,
        "waitMs": 20
    }));
    assert_eq!(cm_poll["state"], "running");
    let (cm_killed, cm_killed_text) = server.ok(json!({"action": "kill", "taskId": &cm_id}));
    assert_eq!(cm_killed["state"], "killed", "{cm_killed_text}");
    assert_eq!(
        cm_killed["codemode"]["cancelledCalls"],
        json!([{"tool": "sleep"}])
    );
}

#[test]
fn p0_3_p1_3_p1_5_codemode_permits_sleep_stacks_and_serializable_returns() {
    let dir = Scratch::new("codemode-permits");
    std::fs::create_dir_all(&*dir).unwrap();
    std::fs::write(dir.join("hello.txt"), "hello-world\n").unwrap();
    let mut server = Server::start_with(|command| {
        command
            .current_dir(&*dir)
            .env("FASTEXEC_CODEMODE", "true")
            .env("FASTEXEC_OUTPUT_MODE", "both");
    });

    let concurrency = code_result(
        &mut server,
        json!({
            "code": r#"
                const t0 = Date.now();
                const eightStarts = Promise.all(
                    Array.from({ length: 8 }, () => tools.exec({ action: "start", command: "sleep 60" }))
                );
                let active = [];
                while (active.length < 8) {
                    await sleep(20);
                    const listed = await tools.exec({ action: "list", state: "active" });
                    active = listed.tasks.filter(t => t.kind === "bash");
                }
                const readDuringStarts = await tools.read({ path: "hello.txt" });
                if (readDuringStarts.text !== "hello-world\n") {
                    throw new Error("read failed while starts saturated");
                }
                await Promise.all(active.map(t => tools.exec({ action: "kill", taskId: t.taskId })));
                const endedEight = await eightStarts;
                const dtEight = Date.now() - t0;
                if (dtEight > 20000 || endedEight.some(r => r.state !== "killed")) {
                    throw new Error("8 starts kill mismatch: dt=" + dtEight + " states=" + endedEight.map(r => r.state));
                }

                // 8 concurrent observation polls on distinct readers do not block read.
                const holder = await tools.exec({
                    action: "start",
                    command: "printf 'holder-ready\\n'; sleep 30",
                    returnWhen: { outputContains: ["holder-ready"] }
                });
                const bare = holder.taskId.split(".")[0];
                const polls = Array.from({ length: 8 }, (_, idx) =>
                    tools.exec({
                        action: "poll",
                        taskId: bare + ".r00" + idx,
                        waitMs: 2500
                    }).then(() => "polls-ended")
                );
                await sleep(20);
                const readWinner = await Promise.race([
                    tools.read({ path: "hello.txt" }).then(r => r.text),
                    Promise.all(polls).then(() => "polls-won")
                ]);
                await tools.exec({ action: "kill", taskId: holder.taskId });
                await Promise.allSettled(polls);
                if (readWinner !== "hello-world\n") {
                    throw new Error("read was blocked by observation polls: " + readWinner);
                }

                // 9 sleep 0.3 starts succeed via Promise.all.
                const nine = await Promise.all(
                    Array.from({ length: 9 }, () => tools.exec({ action: "start", command: "sleep 0.3" }))
                );
                if (nine.some(r => r.exitCode !== 0)) {
                    throw new Error("9 starts failed");
                }

                // Promise.race with sleep(100).
                const raceHolder = tools.exec({ action: "start", command: "sleep 10" });
                const winner = await Promise.race([
                    raceHolder,
                    sleep(100).then(() => "timeout")
                ]);
                if (winner !== "timeout") throw new Error("expected timeout winner");
                const remaining = (await tools.exec({ action: "list", state: "active" }))
                    .tasks.filter(t => t.kind === "bash");
                await Promise.all(remaining.map(t => tools.exec({ action: "kill", taskId: t.taskId })));
                await raceHolder;

                // sleep(-3) resolves; sleep(NaN) and sleep("1") reject with TypeError.
                await sleep(-3);
                for (const bad of [NaN, "1"]) {
                    let caught = null;
                    try {
                        await sleep(bad);
                    } catch (e) {
                        caught = e;
                    }
                    if (!caught || caught.name !== "TypeError") {
                        throw new Error("expected TypeError for sleep(" + bad + ")");
                    }
                }

                // 1025th pending call rejects with RangeError and caller stack.
                const sleepers = Array.from({ length: 1024 }, () => sleep(200));
                let capErr = null;
                try {
                    await sleep(200);
                } catch (e) {
                    capErr = e;
                }
                await Promise.all(sleepers);
                if (!capErr || capErr.name !== "RangeError" || !String(capErr.stack).includes("codemode.js:")) {
                    throw new Error("1025th call mismatch: " + (capErr && capErr.stack));
                }
                return "concurrency-ok";
            "#
        }),
    );
    assert_eq!(
        concurrency["structuredContent"]["exitCode"], 0,
        "{concurrency}"
    );
    assert_eq!(concurrency["structuredContent"]["output"], "concurrency-ok");

    // P1-3: Line 2 uncaught async ToolError names codemode.js:2; caught stack also names it;
    // unserializable returns exit 1 while null and "a\nb" succeed.
    let uncaught_line2 = code_result(
        &mut server,
        json!({"code": "const x = 1;\nawait tools.read({ path: 'missing-on-line-2.txt' });\nreturn x;"}),
    );
    assert_eq!(uncaught_line2["structuredContent"]["exitCode"], 1);
    assert!(
        uncaught_line2["structuredContent"]["output"]
            .as_str()
            .unwrap()
            .contains("codemode.js:2"),
        "{uncaught_line2}"
    );

    let caught_stacks = code_result(
        &mut server,
        json!({
            "code": "let s1 = '';\ntry { await tools.read({ path: 'nope.txt' }); } catch (e) { s1 = e.stack; }\nlet s2 = '';\ntry { await tools.exec({ action: 123 }); } catch (e) { s2 = e.stack; }\nreturn [s1.includes('codemode.js:2'), s2.includes('codemode.js:4')];"
        }),
    );
    assert_eq!(caught_stacks["structuredContent"]["output"], "[true,true]");

    for bad_ret in ["return Symbol('x');", "return () => 1;", "return 1n;"] {
        let bad = code_result(&mut server, json!({"code": bad_ret}));
        assert_eq!(bad["structuredContent"]["exitCode"], 1, "{bad_ret}: {bad}");
        assert!(
            bad["structuredContent"]["output"]
                .as_str()
                .unwrap()
                .contains("Script error: TypeError: the return value is not JSON-serializable"),
            "{bad_ret}: {bad}"
        );
    }
    let ret_null = code_result(&mut server, json!({"code": "return null;"}));
    assert_eq!(ret_null["structuredContent"]["exitCode"], 0);
    assert_eq!(ret_null["structuredContent"]["output"], "null");
    let ret_lines = code_result(&mut server, json!({"code": "return 'a\\nb';"}));
    assert_eq!(ret_lines["structuredContent"]["exitCode"], 0);
    assert_eq!(ret_lines["structuredContent"]["output"], "a\nb");
}

#[test]
fn cancelled_start_leaves_the_login_probe_warning_for_the_next_delivery() {
    let home = Scratch::new("login-warning-cancel");
    std::fs::create_dir_all(&*home).unwrap();
    std::fs::write(home.join(".bash_profile"), "exit 7\n").unwrap();
    let mut server = Server::start_with(|cmd| {
        cmd.env("HOME", &*home)
            .env("USERPROFILE", &*home)
            .env("FASTEXEC_CODEMODE", "true")
            .env("FASTEXEC_OUTPUT_MODE", "both");
    });
    let code = r#"
        const command = "read -r keep_open";
        const waiting = tools.exec({action: "start", command, eof: false});
        while (!(await tools.exec({action: "list"})).tasks.some(t => t.command === command)) {
            await sleep(10);
        }
        const other = await tools.exec({action: "start", command: "true"});
        if (other.warnings?.some(w => w.includes("Login environment probe failed"))) {
            throw Error("another start reported the first start's reserved warning");
        }
        return "registered";
    "#;
    let ended = code_result(&mut server, json!({"code": code}));
    assert_eq!(ended["structuredContent"]["exitCode"], 0, "{ended}");
    assert_eq!(
        ended["structuredContent"]["output"], "registered",
        "{ended}"
    );
    let (next, text) =
        server.ok(json!({"action": "start", "command": "true", "pty": true, "screen": true}));
    assert!(text.contains("Login environment probe failed"), "{text}");
    assert!(
        next["warnings"].as_array().unwrap().iter().any(|w| w
            .as_str()
            .unwrap()
            .contains("Login environment probe failed")),
        "{next}"
    );
    let (_, once) = server.ok(json!({"action": "start", "command": "true"}));
    assert!(!once.contains("Login environment probe failed"), "{once}");
}

#[test]
fn p1_1_login_environment_snapshot_bash_cache_and_fallbacks() {
    // 1. Profile exports FX_PROFILE=1, prints BANNER, cd /, and appends to $HOME/count.
    let home1 = Scratch::new("p1-1-home1");
    let work1 = Scratch::new("p1-1-work1");
    let sub1 = work1.join("sub1");
    let sub2 = work1.join("sub2");
    let sub3 = work1.join("sub3");
    std::fs::create_dir_all(&*home1).unwrap();
    for d in [&*work1, &sub1, &sub2, &sub3] {
        std::fs::create_dir_all(d).unwrap();
    }
    std::fs::write(
        home1.join(".bash_profile"),
        "export FX_PROFILE=1\necho BANNER\ncd /\necho x >> \"$HOME/count\"\n",
    )
    .unwrap();

    let mut s1 = Server::start_with(|cmd| {
        cmd.current_dir(&*work1)
            .env("HOME", &*home1)
            .env("USERPROFILE", &*home1)
            .env("FASTEXEC_CODEMODE", "true")
            .env("FASTEXEC_OUTPUT_MODE", "both");
    });
    for _ in 0..3 {
        let (out, text) = s1.ok(json!({
            "action": "start",
            "command": "printf '%s|%s' \"$FX_PROFILE\" \"$(pwd -P)\""
        }));
        let body = out["output"].as_str().unwrap();
        assert!(body.starts_with("1|"), "{text}");
        assert!(
            body.contains(work1.file_name().unwrap().to_str().unwrap()),
            "cwd must be requested directory, not /: {body}"
        );
        assert!(!text.contains("BANNER"), "{text}");
    }
    for sub in [&sub1, &sub2, &sub3] {
        let cwd_str = sub.to_string_lossy().replace('\\', "/");
        let cm = code_result(
            &mut s1,
            json!({
                "cwd": cwd_str,
                "code": "const r = await tools.exec({ action: 'start', command: 'printf \"%s|%s\" \"$FX_PROFILE\" \"$(pwd -P)\"' }); return r.output;"
            }),
        );
        let out_str = cm["structuredContent"]["output"].as_str().unwrap();
        assert!(
            out_str.starts_with("1|") && !out_str.contains("BANNER"),
            "{out_str}"
        );
    }
    let count_lines = std::fs::read_to_string(home1.join("count")).unwrap();
    assert_eq!(
        count_lines.lines().count(),
        1,
        "expected 1 probe: {count_lines:?}"
    );

    // loginShell: true runs the profile again and includes BANNER.
    let (login_out, _) = s1.ok(json!({
        "action": "start",
        "loginShell": true,
        "command": "printf after"
    }));
    assert!(
        login_out["output"].as_str().unwrap().contains("BANNER"),
        "{login_out}"
    );
    drop(s1);

    // 2. Profile exporting SHELLOPTS (errexit) does not force errexit into default non-login tasks,
    // and default TERM is dumb for pipe and xterm-256color for PTY.
    let home2 = Scratch::new("p1-1-shellopts");
    std::fs::create_dir_all(&*home2).unwrap();
    std::fs::write(
        home2.join(".bash_profile"),
        "set -o errexit\nexport SHELLOPTS\nexport TERM=vt100\n",
    )
    .unwrap();
    let mut s2 = Server::start_with(|cmd| {
        cmd.env("HOME", &*home2)
            .env("USERPROFILE", &*home2)
            .env("FASTEXEC_OUTPUT_MODE", "both");
    });
    let (ok_errexit, _) = s2.ok(json!({
        "action": "start",
        "command": "false; echo ok"
    }));
    assert_eq!(ok_errexit["exitCode"], 0);
    assert_eq!(ok_errexit["output"], "ok");

    let (pipe_term, _) = s2.ok(json!({"action": "start", "command": "printf '%s' \"$TERM\""}));
    assert_eq!(pipe_term["output"], "dumb");
    let (pty_term, _) = s2.ok(json!({
        "action": "start",
        "pty": true,
        "command": "printf '%s\\n' \"$TERM\""
    }));
    assert_eq!(
        pty_term["output"].as_str().unwrap().trim(),
        "xterm-256color"
    );
    drop(s2);

    // 3. Profile hanging (`sleep 30`) times out in ~5 s with a one-time warning, and subsequent starts
    // return immediately without repeating the warning.
    let home3 = Scratch::new("p1-1-timeout");
    std::fs::create_dir_all(&*home3).unwrap();
    std::fs::write(home3.join(".bash_profile"), "sleep 30\n").unwrap();
    let mut s3 = Server::start_with(|cmd| {
        cmd.env("HOME", &*home3)
            .env("USERPROFILE", &*home3)
            .env("FASTEXEC_OUTPUT_MODE", "both");
    });
    let t_probe = Instant::now();
    let (first_hang, first_text) = s3.ok(json!({
        "action": "start",
        "command": "printf fallback-1",
        "waitMs": 20000
    }));
    let dt_first = t_probe.elapsed();
    assert!(
        dt_first >= Duration::from_secs(4) && dt_first < Duration::from_secs(15),
        "probe timeout took {dt_first:?}"
    );
    assert_eq!(first_hang["output"], "fallback-1");
    assert!(
        first_text.contains("Warning: Login environment probe failed")
            && first_text.contains("Pass loginShell: true to run the profile."),
        "{first_text}"
    );

    let t_second = Instant::now();
    let (second_hang, second_text) = s3.ok(json!({
        "action": "start",
        "command": "printf fallback-2",
        "waitMs": 20000
    }));
    assert!(
        t_second.elapsed() < Duration::from_secs(3),
        "second start must not re-probe: {:?}",
        t_second.elapsed()
    );
    assert_eq!(second_hang["output"], "fallback-2");
    assert!(
        !second_text.contains("Login environment probe failed"),
        "{second_text}"
    );
    drop(s3);

    // 4. Counting FASTEXEC_BASH wrapper on Unix: `--version` runs only once across direct starts
    // and cwd-overridden codemode tasks in the same session.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let wrap_dir = Scratch::new("p1-1-bash-wrap");
        let wrap_sub = wrap_dir.join("sub");
        std::fs::create_dir_all(&wrap_sub).unwrap();
        let real_bash = ["/bin/bash", "/usr/bin/bash", "/usr/local/bin/bash"]
            .into_iter()
            .find(|p| std::path::Path::new(p).exists())
            .expect("system bash");
        let ver_count = wrap_dir.join("version_count");
        let wrapper = wrap_dir.join("bash-wrapper");
        std::fs::write(
            &wrapper,
            format!(
                "#!/bin/sh\nif [ \"${{1:-}}\" = \"--version\" ]; then echo v >> '{}'; fi\nexec '{}' \"$@\"\n",
                ver_count.display(),
                real_bash
            ),
        )
        .unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mut s4 = Server::start_with(|cmd| {
            cmd.current_dir(&*wrap_dir)
                .env("FASTEXEC_BASH", &wrapper)
                .env("FASTEXEC_CODEMODE", "true");
        });
        for _ in 0..3 {
            s4.ok(json!({"action": "start", "command": "true"}));
        }
        let sub_str = wrap_sub.to_string_lossy().into_owned();
        for _ in 0..3 {
            let cm = code_result(
                &mut s4,
                json!({
                    "cwd": &sub_str,
                    "code": "await tools.exec({ action: 'start', command: 'true' }); return 'ok';"
                }),
            );
            assert_eq!(cm["structuredContent"]["exitCode"], 0, "{cm}");
        }
        let v_lines = std::fs::read_to_string(&ver_count).unwrap();
        assert_eq!(
            v_lines.lines().count(),
            1,
            "bash --version must run once per session: {v_lines:?}"
        );
    }
}

#[test]
fn transfer_over_local_stdio_serves_put_head_get_and_validates_settings() {
    let dir = Scratch::new("stdio-transfer");
    std::fs::create_dir_all(&*dir).unwrap();
    let blob = dir.join("blob.bin");
    std::fs::write(&blob, [0u8, 1, 2, 3]).unwrap();

    // Off by default and when explicitly false; unused transfer env vars are not read.
    for setting in [None, Some("false")] {
        let mut server = Server::start_with(|command| {
            command
                .current_dir(&*dir)
                .env("FASTEXEC_PUBLIC_URL", "not-a-url")
                .env("FASTEXEC_TRANSFER_ADDR", "not-an-addr");
            if let Some(value) = setting {
                command.env("FASTEXEC_TRANSFER", value);
            }
        });
        let listed = server.request("tools/list", json!({}));
        let tools = listed["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 4, "{listed}");
        assert!(
            !tools.iter().any(|tool| tool["name"] == "transfer"),
            "{listed}"
        );
        let refused = server.request(
            "tools/call",
            json!({"name": "transfer", "arguments": {"path": "blob.bin"}}),
        );
        assert_eq!(refused["error"]["message"], "tool not found", "{refused}");
        let read_bin = server.request(
            "tools/call",
            json!({"name": "read", "arguments": {"path": "blob.bin"}}),
        )["result"]
            .clone();
        assert_eq!(read_bin["isError"], true, "{read_bin}");
        let msg = read_bin["content"][0]["text"].as_str().unwrap();
        assert!(msg.ends_with("Inspect it with exec."), "{msg}");
        assert!(!msg.contains("download it with transfer"), "{msg}");
    }

    // Enabled on local stdio with default and YOLO annotations, binary hint linkage, and PUT/HEAD/GET.
    for yolo in [false, true] {
        let mut server = Server::start_with(|command| {
            command
                .current_dir(&*dir)
                .env("FASTEXEC_TRANSFER", "true")
                .env("FASTEXEC_CODEMODE", "true")
                .env("FASTEXEC_YOLO", yolo.to_string());
        });
        let listed = server.request("tools/list", json!({}));
        let tools = listed["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 6, "{listed}");
        let transfer = tools
            .iter()
            .find(|tool| tool["name"] == "transfer")
            .unwrap();
        if yolo {
            assert_eq!(
                transfer["annotations"],
                json!({
                    "title": "Transfer file",
                    "readOnlyHint": true,
                    "destructiveHint": false,
                    "idempotentHint": false,
                    "openWorldHint": false,
                }),
                "{transfer}"
            );
        } else {
            assert_eq!(
                transfer["annotations"],
                json!({
                    "title": "Transfer file",
                    "destructiveHint": true,
                    "openWorldHint": true,
                }),
                "{transfer}"
            );
        }

        let read_bin = server.request(
            "tools/call",
            json!({"name": "read", "arguments": {"path": "blob.bin"}}),
        )["result"]
            .clone();
        assert!(
            read_bin["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("Inspect it with exec or download it with transfer."),
            "{read_bin}"
        );
        let cm_bin = code_result(
            &mut server,
            json!({"code": "try { await tools.read({path: 'blob.bin'}); return 'ok'; } catch (e) { return e.message; }"}),
        );
        assert!(
            code_text(&cm_bin).contains("download it with transfer"),
            "{cm_bin}"
        );

        let target_name = format!("roundtrip-{yolo}.bin");
        let minted = server.request(
            "tools/call",
            json!({"name": "transfer", "arguments": {"path": &target_name}}),
        )["result"]
            .clone();
        assert_eq!(minted["isError"], false, "{minted}");
        let url = minted["structuredContent"]["url"].as_str().unwrap();
        let rest = url.strip_prefix("http://").expect("http:// URL");
        let (authority, path_part) = rest.split_once('/').unwrap();
        let addr: std::net::SocketAddr = authority.parse().unwrap();
        let target = format!("/{path_part}");

        let (status, headers, _) = transfer_http::send(addr, "HEAD", &target, b"");
        assert_eq!(status, 200);
        assert!(
            headers
                .iter()
                .any(|(k, v)| k == "content-length" && v == "0")
        );

        let (status, _, body) =
            transfer_http::send(addr, "PUT", &format!("{target}?offset=0"), b"");
        assert_eq!(status, 200);
        assert_eq!(body, b"0");

        let payload = b"stdio-transfer-payload";
        let (status, _, body) = transfer_http::send(addr, "PUT", &target, payload);
        assert_eq!(status, 200);
        assert_eq!(body, payload.len().to_string().as_bytes());

        let (status, headers, _) = transfer_http::send(addr, "HEAD", &target, b"");
        assert_eq!(status, 200);
        assert!(
            headers
                .iter()
                .any(|(k, v)| k == "content-length" && v == &payload.len().to_string())
        );

        let (status, _, downloaded) = transfer_http::send(addr, "GET", &target, b"");
        assert_eq!(status, 200);
        assert_eq!(downloaded, payload);
        assert_eq!(std::fs::read(dir.join(&target_name)).unwrap(), payload);

        let (not_found, _, _) = transfer_http::send(addr, "GET", "/mcp", b"");
        assert_eq!(not_found, 404);
    }

    // FASTEXEC_TRANSFER_ADDR + FASTEXEC_PUBLIC_URL bind the fixed port while minting under the public root.
    let (fixed_addr, mut proxied) = (0..8)
        .find_map(|attempt| {
            let candidate = transfer_http::free_loopback_addr();
            match Server::try_start_with(|command| {
                command
                    .current_dir(&*dir)
                    .env("FASTEXEC_TRANSFER", "true")
                    .env("FASTEXEC_TRANSFER_ADDR", candidate.to_string())
                    .env("FASTEXEC_PUBLIC_URL", "https://files.example/stdio");
            }) {
                Ok(server) => Some((candidate, server)),
                Err(stderr) if attempt < 7 && stderr.contains("cannot listen on") => None,
                Err(stderr) => panic!("failed to start proxied stdio server: {stderr}"),
            }
        })
        .expect("bound fixed loopback port for stdio transfer");
    let minted = proxied.request(
        "tools/call",
        json!({"name": "transfer", "arguments": {"path": "proxied.bin"}}),
    )["result"]
        .clone();
    assert_eq!(minted["isError"], false, "{minted}");
    let url = minted["structuredContent"]["url"].as_str().unwrap();
    let id = url
        .strip_prefix("https://files.example/stdio/t/")
        .unwrap_or_else(|| panic!("unexpected proxied URL: {url}"));
    let target = format!("/t/{id}");
    assert_eq!(
        transfer_http::send(fixed_addr, "PUT", &format!("{target}?offset=0"), b"").0,
        200
    );
    assert_eq!(
        transfer_http::send(fixed_addr, "PUT", &target, b"via-proxy").0,
        200
    );
    assert_eq!(
        transfer_http::send(fixed_addr, "GET", &target, b"").2,
        b"via-proxy"
    );
    drop(proxied);

    // Wildcard FASTEXEC_TRANSFER_ADDR=0.0.0.0:0 with FASTEXEC_HTTP_ALLOW_REMOTE=1 requires FASTEXEC_PUBLIC_URL.
    let mut wildcard_cmd = Command::new(env!("CARGO_BIN_EXE_fastexec"));
    support::scrub_fastexec_env(&mut wildcard_cmd);
    let wildcard_without_root = wildcard_cmd
        .env("FASTEXEC_TRANSFER", "true")
        .env("FASTEXEC_HTTP_ALLOW_REMOTE", "1")
        .env("FASTEXEC_TRANSFER_ADDR", "0.0.0.0:0")
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(wildcard_without_root.status.code(), Some(1));
    let wildcard_err = String::from_utf8_lossy(&wildcard_without_root.stderr);
    assert!(
        wildcard_err.contains(
            "FASTEXEC_TRANSFER_ADDR 0.0.0.0:0 is an unspecified address; set FASTEXEC_PUBLIC_URL"
        ),
        "{wildcard_err:?}"
    );

    let mut wildcard_with_root = Server::start_with(|command| {
        command
            .current_dir(&*dir)
            .env("FASTEXEC_TRANSFER", "true")
            .env("FASTEXEC_HTTP_ALLOW_REMOTE", "1")
            .env("FASTEXEC_TRANSFER_ADDR", "0.0.0.0:0")
            .env("FASTEXEC_PUBLIC_URL", "https://files.example/");
    });
    let wildcard_minted = wildcard_with_root.request(
        "tools/call",
        json!({"name": "transfer", "arguments": {"path": "wildcard.bin"}}),
    )["result"]
        .clone();
    assert_eq!(wildcard_minted["isError"], false, "{wildcard_minted}");
    assert!(
        wildcard_minted["structuredContent"]["url"]
            .as_str()
            .unwrap()
            .starts_with("https://files.example/t/"),
        "{wildcard_minted}"
    );
    drop(wildcard_with_root);

    // Invalid FASTEXEC_TRANSFER, FASTEXEC_PUBLIC_URL, and FASTEXEC_TRANSFER_ADDR fail startup with exit 1.
    let mut invalid: Vec<std::ffi::OsString> = ["", "on", "off", "1", "0", "bogus"]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        invalid.push(std::ffi::OsString::from_vec(vec![0xff]));
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt;
        invalid.push(std::ffi::OsString::from_wide(&[0xD800]));
    }
    for bad in invalid {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        support::scrub_fastexec_env(&mut command);
        let output = command
            .env("FASTEXEC_TRANSFER", &bad)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{bad:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("fastexec: invalid FASTEXEC_TRANSFER ")
                && stderr.contains("; expected true or false"),
            "{bad:?}: {stderr:?}"
        );
    }
    for (var, bad, expected) in [
        (
            "FASTEXEC_PUBLIC_URL",
            "ftp://bad.example/",
            "FASTEXEC_PUBLIC_URL",
        ),
        (
            "FASTEXEC_TRANSFER_ADDR",
            "typo",
            "FASTEXEC_TRANSFER_ADDR typo is not an address",
        ),
        (
            "FASTEXEC_TRANSFER_ADDR",
            "0.0.0.0:0",
            "FASTEXEC_TRANSFER_ADDR 0.0.0.0:0 is not a loopback address",
        ),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        support::scrub_fastexec_env(&mut command);
        let output = command
            .env("FASTEXEC_TRANSFER", "true")
            .env(var, bad)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{var}={bad}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(expected), "{var}={bad}: {stderr:?}");
    }
}

#[test]
fn instructions_file_appends_to_stdio_initialize_and_rejects_invalid_files() {
    let dir = Scratch::new("instructions-file");
    std::fs::create_dir_all(&*dir).unwrap();
    let file = dir.join("notes.txt");
    std::fs::write(&file, "\u{feff}    indented line\r\nsecond line\r\n").unwrap();

    let init_params = json!({
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "probe", "version": "0"},
    });

    let mut plain = Server::start();
    let base = plain.request("initialize", init_params.clone())["result"]["instructions"]
        .as_str()
        .unwrap()
        .to_owned();
    drop(plain);

    let mut with_file = Server::start_with(|command| {
        command
            .current_dir(&*dir)
            .env("FASTEXEC_INSTRUCTIONS_FILE", "notes.txt");
    });
    let actual = with_file.request("initialize", init_params)["result"]["instructions"]
        .as_str()
        .unwrap()
        .to_owned();
    drop(with_file);

    assert_eq!(
        actual,
        format!("{base}\n\n    indented line\nsecond line\n")
    );

    let oversized = dir.join("oversized.txt");
    std::fs::write(&oversized, vec![b'a'; 16_385]).unwrap();
    for (case, value, expected) in [
        ("missing", "missing.txt", "FASTEXEC_INSTRUCTIONS_FILE"),
        ("oversized", "oversized.txt", "exceeds 16384 bytes"),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        support::scrub_fastexec_env(&mut command);
        let output = command
            .current_dir(&*dir)
            .env("FASTEXEC_INSTRUCTIONS_FILE", value)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{case}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("FASTEXEC_INSTRUCTIONS_FILE") && stderr.contains(expected),
            "{case}: {stderr:?}"
        );
    }
}

const MOVE_GUIDANCE_POINTER: &str = "See the instructions tool for host-specific guidance.";

fn stdio_init_params() -> Value {
    json!({
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "probe", "version": "0"},
    })
}

fn find_tool<'a>(listed: &'a Value, name: &str) -> Option<&'a Value> {
    listed["result"]["tools"]
        .as_array()
        .expect("tools/list array")
        .iter()
        .find(|tool| tool["name"] == name)
}

fn assert_instructions_call_ok(message: &Value, expected: &str, context: &str) {
    let result = &message["result"];
    assert!(result.is_object(), "{context}: {message}");
    assert_ne!(result["isError"], true, "{context}: {message}");
    assert!(
        result.get("structuredContent").is_none(),
        "{context}: unexpected structuredContent in {message}"
    );
    let content = result["content"]
        .as_array()
        .unwrap_or_else(|| panic!("{context}: missing content array in {message}"));
    assert_eq!(content.len(), 1, "{context}: {message}");
    assert_eq!(content[0]["type"], "text", "{context}: {message}");
    assert_eq!(content[0]["text"].as_str(), Some(expected), "{context}");
}

#[test]
fn instructions_tool_off_and_unset_preserve_v070_behavior() {
    let dir = Scratch::new("instructions-tool-off");
    std::fs::create_dir_all(&*dir).unwrap();
    let file = dir.join("extra.txt");
    std::fs::write(&file, "    host note\r\nsecond line  \r\n").unwrap();

    let mut plain = Server::start();
    let base = plain.request("initialize", stdio_init_params())["result"]["instructions"]
        .as_str()
        .unwrap()
        .to_owned();
    let listed = plain.request("tools/list", json!({}));
    assert!(
        find_tool(&listed, "instructions").is_none(),
        "unset should omit instructions tool: {listed}"
    );
    drop(plain);

    for mode in [None, Some("off")] {
        for with_file in [false, true] {
            let mut server = Server::start_with(|command| {
                command.current_dir(&*dir);
                if let Some(mode) = mode {
                    command.env("FASTEXEC_INSTRUCTIONS_TOOL", mode);
                }
                if with_file {
                    command.env("FASTEXEC_INSTRUCTIONS_FILE", "extra.txt");
                }
            });
            let init = server.request("initialize", stdio_init_params())["result"]["instructions"]
                .as_str()
                .unwrap()
                .to_owned();
            let expected = if with_file {
                format!("{base}\n\n    host note\nsecond line  \n")
            } else {
                base.clone()
            };
            assert_eq!(init, expected, "mode={mode:?} with_file={with_file}");

            let listed = server.request("tools/list", json!({}));
            assert!(
                find_tool(&listed, "instructions").is_none(),
                "mode={mode:?} with_file={with_file} should omit instructions tool: {listed}"
            );
        }
    }
}

#[test]
fn instructions_tool_move_mode_contract_and_annotations() {
    let dir = Scratch::new("instructions-tool-move");
    std::fs::create_dir_all(&*dir).unwrap();

    // Include leading indentation, UTF-8 text, >512 bytes before the unique marker, and trailing spaces.
    let marker = "MARKER-AFTER-512-BYTES-7c4e9b";
    let padding = "x".repeat(540);
    let raw =
        format!("\u{feff}    Opening directive — 日本語 café  \r\n{padding}\r\n{marker}   \r\n");
    let normalized = format!("    Opening directive — 日本語 café  \n{padding}\n{marker}   \n");
    assert!(normalized.find(marker).unwrap() > 512);
    std::fs::write(dir.join("host.txt"), raw).unwrap();

    let mut plain = Server::start();
    let base = plain.request("initialize", stdio_init_params())["result"]["instructions"]
        .as_str()
        .unwrap()
        .to_owned();
    drop(plain);

    let expected_proto = format!("{base}\n\n{MOVE_GUIDANCE_POINTER}");
    let expected_annotations = json!({
        "title": "Server instructions",
        "readOnlyHint": true,
        "destructiveHint": false,
        "idempotentHint": true,
        "openWorldHint": false,
    });
    let expected_schema = json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false,
    });

    for (yolo, output_mode) in [
        ("false", None),
        ("true", Some("structured")),
        ("false", Some("both")),
    ] {
        let mut server = Server::start_with(|command| {
            command
                .current_dir(&*dir)
                .env("FASTEXEC_INSTRUCTIONS_TOOL", "move")
                .env("FASTEXEC_INSTRUCTIONS_FILE", "host.txt")
                .env("FASTEXEC_YOLO", yolo);
            if let Some(mode) = output_mode {
                command.env("FASTEXEC_OUTPUT_MODE", mode);
            }
        });

        let init = server.request("initialize", stdio_init_params())["result"]["instructions"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(init, expected_proto, "yolo={yolo} output={output_mode:?}");
        assert!(!init.contains(marker), "file text leaked into initialize");

        let listed = server.request("tools/list", json!({}));
        let tool = find_tool(&listed, "instructions")
            .unwrap_or_else(|| panic!("missing instructions tool: {listed}"));
        assert_eq!(tool["description"].as_str(), Some(normalized.as_str()));
        assert_eq!(tool["inputSchema"], expected_schema, "{tool}");
        assert_eq!(
            tool["annotations"], expected_annotations,
            "yolo={yolo} output={output_mode:?}: {tool}"
        );

        for (label, params) in [
            (
                "empty object",
                json!({"name": "instructions", "arguments": {}}),
            ),
            ("omitted arguments", json!({"name": "instructions"})),
            (
                "null arguments",
                json!({"name": "instructions", "arguments": null}),
            ),
        ] {
            let reply = server.request("tools/call", params);
            assert_instructions_call_ok(
                &reply,
                &normalized,
                &format!("yolo={yolo} output={output_mode:?} {label}"),
            );
        }

        let extra_err = server.request(
            "tools/call",
            json!({"name": "instructions", "arguments": {"unexpected": true}}),
        );
        assert_eq!(
            extra_err["error"]["code"],
            json!(-32602),
            "extra args must fail with -32602: {extra_err}"
        );

        // Calling instructions must not allocate or leave any tasks.
        let (structured, text, is_error) = server.call(json!({"action": "list", "scope": "all"}));
        assert!(!is_error, "{text}");
        if output_mode.is_some() {
            assert_eq!(structured["tasks"], json!([]), "{structured}");
        } else {
            assert!(text.contains("No tasks."), "unexpected tasks: {text}");
        }
    }

    // move without FASTEXEC_INSTRUCTIONS_FILE returns built-in lines on both surfaces.
    let mut no_file = Server::start_with(|command| {
        command.env("FASTEXEC_INSTRUCTIONS_TOOL", "move");
    });
    let init = no_file.request("initialize", stdio_init_params())["result"]["instructions"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(init, base);
    let listed = no_file.request("tools/list", json!({}));
    let tool = find_tool(&listed, "instructions").expect("instructions tool");
    assert_eq!(tool["description"].as_str(), Some(base.as_str()));
    let reply = no_file.request(
        "tools/call",
        json!({"name": "instructions", "arguments": {}}),
    );
    assert_instructions_call_ok(&reply, &base, "move without file");
}

#[test]
fn instructions_tool_copy_mode_with_and_without_file() {
    let dir = Scratch::new("instructions-tool-copy");
    std::fs::create_dir_all(&*dir).unwrap();
    std::fs::write(dir.join("copy.txt"), "    copied guidance — naïve  \r\n").unwrap();

    let mut plain = Server::start();
    let base = plain.request("initialize", stdio_init_params())["result"]["instructions"]
        .as_str()
        .unwrap()
        .to_owned();
    drop(plain);

    for (with_file, expected) in [
        (false, base.clone()),
        (true, format!("{base}\n\n    copied guidance — naïve  \n")),
    ] {
        let mut server = Server::start_with(|command| {
            command
                .current_dir(&*dir)
                .env("FASTEXEC_INSTRUCTIONS_TOOL", "copy");
            if with_file {
                command.env("FASTEXEC_INSTRUCTIONS_FILE", "copy.txt");
            }
        });

        let init = server.request("initialize", stdio_init_params())["result"]["instructions"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(init, expected, "with_file={with_file}");

        let listed = server.request("tools/list", json!({}));
        let tool = find_tool(&listed, "instructions").expect("instructions tool");
        assert_eq!(
            tool["description"].as_str(),
            Some(expected.as_str()),
            "with_file={with_file}"
        );

        let reply = server.request(
            "tools/call",
            json!({"name": "instructions", "arguments": {}}),
        );
        assert_instructions_call_ok(&reply, &expected, &format!("copy with_file={with_file}"));
    }
}

#[test]
fn instructions_tool_16kib_boundary_and_snapshot_immutability() {
    let dir = Scratch::new("instructions-tool-16k");
    std::fs::create_dir_all(&*dir).unwrap();

    let mut plain = Server::start();
    let base = plain.request("initialize", stdio_init_params())["result"]["instructions"]
        .as_str()
        .unwrap()
        .to_owned();
    drop(plain);

    let prefix = "    exact-16k-head\n";
    let suffix = "\nexact-16k-tail   \n";
    let body = format!(
        "{prefix}{}{suffix}",
        "b".repeat(16_384 - prefix.len() - suffix.len())
    );
    assert_eq!(body.len(), 16_384);

    for mode in ["move", "copy"] {
        let file = dir.join(format!("{mode}.txt"));
        std::fs::write(&file, &body).unwrap();

        let mut server = Server::start_with(|command| {
            command
                .current_dir(&*dir)
                .env("FASTEXEC_INSTRUCTIONS_TOOL", mode)
                .env("FASTEXEC_INSTRUCTIONS_FILE", &file);
        });

        let expected_proto = if mode == "move" {
            format!("{base}\n\n{MOVE_GUIDANCE_POINTER}")
        } else {
            format!("{base}\n\n{body}")
        };
        let expected_tool = if mode == "move" {
            body.clone()
        } else {
            format!("{base}\n\n{body}")
        };

        let check = |server: &mut Server, stage: &str| {
            let init = server.request("initialize", stdio_init_params())["result"]["instructions"]
                .as_str()
                .unwrap()
                .to_owned();
            assert_eq!(init, expected_proto, "{mode} {stage} initialize");
            let listed = server.request("tools/list", json!({}));
            let tool = find_tool(&listed, "instructions").expect("instructions tool");
            assert_eq!(
                tool["description"].as_str(),
                Some(expected_tool.as_str()),
                "{mode} {stage} description"
            );
            let reply = server.request(
                "tools/call",
                json!({"name": "instructions", "arguments": {}}),
            );
            assert_instructions_call_ok(&reply, &expected_tool, &format!("{mode} {stage} call"));
        };

        check(&mut server, "initial");
        std::fs::write(&file, "edited after startup\n").unwrap();
        check(&mut server, "after edit");
        std::fs::remove_file(&file).unwrap();
        check(&mut server, "after delete");
    }
}

#[test]
fn instructions_tool_rejects_invalid_env_values() {
    for bad in ["", "true", "false", "MOVE", " off", "copy "] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        support::scrub_fastexec_env(&mut command);
        let output = command
            .env("FASTEXEC_INSTRUCTIONS_TOOL", bad)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "bad={bad:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("FASTEXEC_INSTRUCTIONS_TOOL"),
            "bad={bad:?}: {stderr:?}"
        );
    }

    #[cfg(unix)]
    {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        support::scrub_fastexec_env(&mut command);
        let output = command
            .env("FASTEXEC_INSTRUCTIONS_TOOL", OsStr::from_bytes(&[0xff]))
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "non-utf8");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("FASTEXEC_INSTRUCTIONS_TOOL"),
            "non-utf8: {stderr:?}"
        );
    }
}
