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
            .env_remove("FASTEXEC_STRUCTURED_CONTENT");
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
        (
            result["structuredContent"].clone(),
            text,
            result["isError"] == json!(true),
        )
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
    assert!(text.starts_with("hello\noops\n\n[exited 3] t1"), "{text}");
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
        assert!(text.contains("[exited 3] t1"), "{text}");
        assert_eq!(out["taskId"], "t1");
        assert_eq!(out["state"], "exited");
        assert_eq!(out["exitCode"], 3);
        assert!(std::path::Path::new(out["logPath"].as_str().unwrap()).exists());

        server.ok(json!({
            "action": "start", "command": "read -r line; printf '%s\\n' \"$line\"", "waitMs": 0
        }));
        let (out, text) = server.ok(json!({
            "action": "poll", "taskId": "t2", "input": "MODE_POLL\n", "eof": true, "waitMs": 10000
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
        let (out, text) = server.ok(json!({"action": "poll", "taskId": "t2", "waitMs": 0}));
        assert_eq!(text.contains("(no new output)"), text_body, "{text}");
        assert_eq!(
            out.get("output").and_then(Value::as_str),
            structured_body.then_some("")
        );
        assert!(text.contains("[exited 0] t2"), "{text}");
        assert_eq!(out["omittedRange"], Value::Null);

        let (out, text, is_error) = server.call(json!({"action": "poll", "taskId": "missing"}));
        assert!(is_error && text.contains("Unknown taskId"), "{text}");
        assert_eq!(out["ok"], false);
    }

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
        for (arguments, expected, is_error) in [
            (
                json!({"action": "start", "command": "cat <<'EOF'\nHello, world!\nEOF"}),
                "Hello, world!\n\n[exited 0] t1",
                false,
            ),
            (
                json!({"action": "start", "command": "read -r line; printf '%s\\n' \"$line\"", "waitMs": 0}),
                "[running] t2",
                false,
            ),
            (
                json!({"action": "poll", "taskId": "t2", "input": "POLL_BODY\n", "eof": true, "waitMs": 10000}),
                "POLL_BODY\n\n[exited 0] t2",
                false,
            ),
            (json!({"action": "list"}), "t2 [exited 0]", false),
            (
                json!({"action": "kill", "taskId": "t2"}),
                "[exited 0] t2",
                false,
            ),
            (
                json!({"action": "poll", "taskId": "missing"}),
                "Unknown taskId",
                true,
            ),
            (json!({"action": "start"}), "start needs a non-empty", true),
        ] {
            let message = server.request(
                "tools/call",
                json!({"name": "fastexec", "arguments": arguments}),
            );
            let result = &message["result"];
            assert!(result.is_object(), "protocol error: {message}");
            assert!(
                result.get("structuredContent").is_none(),
                "mode={mode}: {result}"
            );
            assert!(
                result["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains(expected),
                "mode={mode}: {result}"
            );
            assert_eq!(result["isError"], json!(is_error));
        }
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
    let (out, text) = server.ok(json!({"action": "poll", "taskId": "t1", "waitMs": 15000}));
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
    let mut server = Server::start();
    server.ok(json!({"action": "start", "command": "sleep 60", "waitMs": 0}));
    server.ok(json!({"action": "start", "command": "sleep 1; exit 0", "waitMs": 0}));
    server.ok(json!({"action": "start", "command": "sleep 1; exit 7", "waitMs": 0}));
    let (_, text) = server.ok(json!({"action": "poll", "taskId": "t1", "waitMs": 4000}));
    let footer = text.lines().last().unwrap();
    assert_eq!(footer, "(Background: t3 exited 7, t2 exited 0.)", "{text}");

    // Reported completions leave the footer; running tasks stay in it.
    let (_, text) = server.ok(json!({"action": "start", "command": "true"}));
    let footer = text.lines().last().unwrap();
    assert!(
        footer.starts_with("(Background: t1 running ") && !footer.contains("t2"),
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
        footer.starts_with("(Background: t9 running ")
            && footer.ends_with("; 3 more; 6 running; use list.)"),
        "{text}"
    );
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
    let (out, text) = server.ok(
        json!({"action": "start", "command": "echo first; sleep 4; echo second", "waitMs": 2000}),
    );
    assert_eq!(out["state"], "running");
    assert!(text.starts_with("first\n"), "{text}");
    let (out, text) = server.ok(json!({"action": "poll", "taskId": "t1", "waitMs": 10000}));
    assert_eq!(
        (out["state"].as_str(), out["exitCode"].as_i64()),
        (Some("exited"), Some(0))
    );
    assert!(
        text.starts_with("second\n") && !text.contains("first"),
        "{text}"
    );
}

#[test]
fn pipe_input_arrives_verbatim_and_eof_closes_stdin() {
    let mut server = Server::start();
    server.ok(json!({"action": "start", "command": "while IFS= read -r l || [ -n \"$l\" ]; do echo \"got:$l\"; done; echo end", "waitMs": 0}));
    let (_, text) = server.ok(json!({"action": "poll", "taskId": "t1", "input": "a b\n"}));
    assert!(text.starts_with("got:a b\n"), "{text}");
    let (out, text) = server
        .ok(json!({"action": "poll", "taskId": "t1", "input": "c", "eof": true, "waitMs": 10000}));
    assert_eq!(out["state"], "exited");
    assert!(text.starts_with("got:c\nend\n"), "{text}");
    let (_, text, is_error) =
        server.call(json!({"action": "poll", "taskId": "t1", "input": "late\n"}));
    assert!(is_error && text.contains("exited"), "{text}");
    let (out, _) = server.ok(json!({"action": "poll", "taskId": "t1", "waitMs": 0}));
    assert_eq!(
        out["state"], "exited",
        "a plain poll still reads a finished task"
    );

    // A program that closes its stdin refuses later input instead of queueing it.
    server.ok(json!({"action": "start", "command": "exec 0<&-; sleep 30", "waitMs": 500}));
    server.ok(json!({"action": "poll", "taskId": "t2", "input": "x\n", "waitMs": 500}));
    let (_, text, is_error) =
        server.call(json!({"action": "poll", "taskId": "t2", "input": "y\n", "waitMs": 0}));
    assert!(is_error && text.contains("stdin is closed"), "{text}");
    server.ok(json!({"action": "kill", "taskId": "t2"}));
}

#[test]
fn pty_task_sees_a_terminal_and_accepts_a_hidden_password() {
    let mut server = Server::start();
    let command = "[ -t 0 ] && echo tty; read -r -s -p 'Password: ' p; echo; echo \"len=${#p}\"";
    let (out, text) =
        server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 5000}));
    assert_eq!(out["state"], "running", "{text}");
    assert!(text.contains("tty") && text.contains("Password:"), "{text}");
    let (out, text) =
        server.ok(json!({"action": "poll", "taskId": "t1", "input": "secret\r", "waitMs": 10000}));
    assert_eq!(out["state"], "exited", "{text}");
    assert!(text.contains("len=6") && !text.contains("secret"), "{text}");
    let (_, text, is_error) = server.call(json!({"action": "poll", "taskId": "t1", "eof": true}));
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
    let (_, text) =
        server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 3000}));
    assert!(text.contains("ready"), "{text}");
    let (_, text, is_error) =
        server.call(json!({"action": "poll", "taskId": "t1", "keys": ["Up", "C-Enter"]}));
    assert!(is_error && text.contains("C-Enter"), "{text}");
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": "t1", "input": "ab", "keys": ["Enter", "Up", "C-c", "M-x"], "waitMs": 10000
    }));
    assert_eq!(out["state"], "exited", "{text}");
    assert!(
        text.contains("61 62 0d 1b 4f 41 03 1b 78"),
        "input, then keys in order, with Up in application cursor mode: {text}"
    );

    server.ok(json!({"action": "start", "command": "cat", "waitMs": 0}));
    let (_, text, is_error) =
        server.call(json!({"action": "poll", "taskId": "t2", "keys": ["Enter"]}));
    assert!(is_error && text.contains("PTY tasks only"), "{text}");
    server.ok(json!({"action": "kill", "taskId": "t2"}));
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
    let (_, text) =
        server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 3000}));
    assert!(text.contains("flags=[?1"), "{text}");
    let (_, text) = server.ok(json!({
        "action": "poll", "taskId": "t1", "keys": ["Escape", "C-i", "Tab"], "waitMs": 3000
    }));
    assert!(
        text.contains("1b 5b 32 37 75 1b 5b 31 30 35 3b 35 75 09") && text.contains("popped"),
        "Escape and C-i as CSI u, Tab as itself: {text}"
    );
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": "t1", "keys": ["Escape", "Tab"], "waitMs": 10000
    }));
    assert_eq!(out["state"], "exited", "{text}");
    assert!(text.contains("1b 09"), "legacy bytes once popped: {text}");
}

#[test]
fn a_terminal_emulator_failure_leaves_the_output_flowing() {
    let mut server = Server::start();
    // alacritty_terminal 0.26 panics past 4096 kitty keyboard pushes.
    let command = "printf '\\033[>1u%.0s' $(seq 1 4097); printf 'alive\\r\\n'; read -r x; printf 'done\\r\\n'";
    let (_, text) =
        server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 5000}));
    assert!(text.contains("alive"), "{text}");
    let (_, text, is_error) =
        server.call(json!({"action": "poll", "taskId": "t1", "keys": ["Enter"]}));
    assert!(is_error && text.contains("stopped"), "{text}");
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": "t1", "input": "\r", "waitMs": 10000
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
        "action": "start", "command": command, "pty": true, "screen": true, "waitMs": 3000
    }));
    assert_eq!(out["state"], "running", "{text}");
    let body = text.split("\n\n[").next().unwrap();
    assert_eq!(body.trim_end(), "\n  frame-2", "{text}");
    assert_eq!(out["cursor"], json!([2, 10]), "{text}");
    let (_, text) = server.ok(json!({"action": "poll", "taskId": "t1", "waitMs": 0}));
    assert!(text.starts_with("(no new output)"), "{text}");
    server.ok(json!({"action": "kill", "taskId": "t1"}));

    // A full first row leaves the cursor in the last column, pending the wrap.
    let command = "printf '%0120d' 0; read -r x";
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "pty": true, "screen": true, "waitMs": 3000
    }));
    assert_eq!(out["cursor"], json!([1, 120]), "{text}");
    server.ok(json!({"action": "kill", "taskId": "t2"}));

    // A synchronized update the program leaves open shows once it times out.
    let command = "printf 'shown\\r\\n\\033[?2026hheld'; read -r x";
    let (_, text) = server.ok(json!({
        "action": "start", "command": command, "pty": true, "screen": true, "waitMs": 2000
    }));
    assert_eq!(text.split("\n\n[").next().unwrap(), "shown\nheld", "{text}");
    server.ok(json!({"action": "kill", "taskId": "t3"}));
}

#[test]
fn return_when_ends_a_wait_on_unseen_output_text_or_quiet() {
    let mut server = Server::start();
    let command =
        "printf 'boot\\n'; sleep 0.5; printf '\\033[32mServer\\033[0m listening\\n'; sleep 30";
    let begun = Instant::now();
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "waitMs": 20000,
        "returnWhen": {"outputContains": {"text": "server LISTENING", "caseSensitive": false}}
    }));
    assert!(begun.elapsed() < Duration::from_secs(10), "{text}");
    assert_eq!(out["waitEndedBy"], "output_contains", "{text}");
    assert_eq!(out["state"], "running", "{text}");
    assert!(
        text.contains("Server listening") && text.contains("wait: output matched"),
        "{text}"
    );
    // Output an earlier result showed does not match again.
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": "t1", "waitMs": 500,
        "returnWhen": {"outputContains": {"text": "listening"}}
    }));
    assert_eq!(out["waitEndedBy"], "max_wait", "{text}");
    server.ok(json!({"action": "kill", "taskId": "t1"}));

    // Silence before any output does not count as quiet.
    let command =
        "sleep 1; echo first; sleep 1; echo stale; read -r x; sleep 2.5; echo \"got $x\"; sleep 30";
    let begun = Instant::now();
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "waitMs": 20000,
        "returnWhen": {"outputQuietForMs": 300}
    }));
    assert!(begun.elapsed() >= Duration::from_secs(1), "{text}");
    assert_eq!(out["waitEndedBy"], "output_quiet", "{text}");
    assert!(text.starts_with("first\n"), "{text}");
    // Unseen output, long quiet, does not end a wait after input; only the reply does. The
    // reply comes after the 2 s default of a plain poll with input.
    std::thread::sleep(Duration::from_millis(1500));
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": "t2", "input": "hi\n",
        "returnWhen": {"outputQuietForMs": 300}
    }));
    assert_eq!(out["waitEndedBy"], "output_quiet", "{text}");
    assert!(text.starts_with("stale\ngot hi\n"), "{text}");
    server.ok(json!({"action": "kill", "taskId": "t2"}));

    let (out, text) = server.ok(json!({
        "action": "start", "command": "echo hi",
        "returnWhen": {"outputContains": {"text": "nope"}}
    }));
    assert_eq!(out["waitEndedBy"], "task_ended", "{text}");
}

#[test]
fn screen_contains_and_quiet_see_only_committed_frames() {
    let mut server = Server::start();
    // The visible text is assembled by cursor moves; the earlier text is erased.
    let command = "printf 'GONE\\r\\n'; sleep 0.3; printf '\\033[2J\\033[H\\033[1;4Hdy\\033[1;1HRea'; read -r x";
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 20000,
        "returnWhen": {"screenContains": {"text": "Ready"}}
    }));
    assert_eq!(out["waitEndedBy"], "screen_contains", "{text}");
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": "t1", "waitMs": 500,
        "returnWhen": {"screenContains": {"text": "GONE"}}
    }));
    assert_eq!(out["waitEndedBy"], "max_wait", "{text}");
    server.ok(json!({"action": "kill", "taskId": "t1"}));

    // A synchronized update the program leaves open shows at its timeout, while the program
    // writes nothing more; until then the output is not quiet.
    let command = "printf 'shown\\r\\n\\033[?2026hheld'; read -r x";
    let begun = Instant::now();
    let (out, text) = server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 20000,
        "returnWhen": {"screenContains": {"text": "HELD", "caseSensitive": false}}
    }));
    assert!(begun.elapsed() < Duration::from_secs(10), "{text}");
    assert_eq!(out["waitEndedBy"], "screen_contains", "{text}");
    server.ok(json!({"action": "kill", "taskId": "t2"}));
    let command =
        "stty -echo; printf 'shown\\r\\n'; read -r x; printf '\\033[?2026hheld'; read -r x";
    server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 20000,
        "returnWhen": {"screenContains": {"text": "shown"}}
    }));
    let (out, text) = server.ok(json!({
        "action": "poll", "taskId": "t3", "keys": ["Enter"], "screen": true, "waitMs": 20000,
        "returnWhen": {"outputQuietForMs": 100}
    }));
    assert_eq!(out["waitEndedBy"], "output_quiet", "{text}");
    assert_eq!(text.split("\n\n[").next().unwrap(), "shown\nheld", "{text}");
    server.ok(json!({"action": "kill", "taskId": "t3"}));
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
        "action": "start", "command": command, "pty": true, "screen": true, "waitMs": 3000
    }));
    assert_eq!(out["state"], "running", "{text}");
    assert_eq!(
        text.split("\n\n[").next().unwrap(),
        expected,
        "screen: {text}"
    );
    assert_eq!(out["cursor"], json!([7, 1]), "{text}");
    let (_, text) = server.ok(json!({"action": "transcript", "taskId": "t1", "truncate": "none"}));
    assert_eq!(
        text.split("\n\n[").next().unwrap(),
        expected,
        "transcript: {text}"
    );
    server.ok(json!({"action": "kill", "taskId": "t1"}));
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
    // Like pi, a full redraw clears the screen and its history (ED2, ED3), then reprints.
    // Like Codex, history lines are then inserted through a scroll region above a fixed
    // composer row. Both push lines past the 30-row screen.
    let command = concat!(
        "seq -f 'stale %g' 1 40; printf '\\033[2J\\033[H\\033[3J'; seq -f 'plain %g' 1 40; ",
        "printf '\\033[30;1H> composer'; ",
        "for k in $(seq 1 40); do ",
        "printf '\\033[1;29r\\033[29;1H\\r\\ninsert %d\\033[r\\033[30;1H> composer\\033[K' $k; done"
    );
    let (out, text) =
        server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 30000}));
    assert_eq!(out["state"], "exited", "{text}");
    let (out, text) =
        server.ok(json!({"action": "transcript", "taskId": "t1", "truncate": "none"}));
    let body = text.split("\n\n[").next().unwrap();
    let lines: Vec<&str> = body.lines().collect();
    let expected: Vec<String> = (1..=40)
        .map(|k| format!("plain {k}"))
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
    assert!(!body.contains("stale"), "cleared history came back: {text}");
    let file = std::fs::read_to_string(out["transcriptPath"].as_str().unwrap()).unwrap();
    assert_eq!(file.trim_end(), body.trim_end());

    // A program on the alternate screen: the normal screen's history, then the full-screen view.
    let command =
        "seq -f 'normal %g' 1 40; printf 'a\\tb\\n\\033[?1049h\\033[Hfull-screen view'; read -r x";
    server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 3000}));
    let (out, text) =
        server.ok(json!({"action": "transcript", "taskId": "t2", "truncate": "none"}));
    assert_eq!(out["alternateScreen"], true, "{text}");
    let lines: Vec<&str> = text.lines().collect();
    let normal = lines.iter().position(|line| *line == "normal 1");
    let marker = lines
        .iter()
        .position(|line| *line == "--- alternate screen ---");
    assert!(normal.is_some_and(|normal| Some(normal) < marker), "{text}");
    assert_eq!(lines[marker.unwrap() + 1], "full-screen view", "{text}");
    assert!(
        lines.contains(&"a       b"),
        "a tab reads as spaces to its stop: {text}"
    );
    server.ok(json!({"action": "kill", "taskId": "t2"}));

    server.ok(json!({"action": "start", "command": "echo pipe"}));
    let (_, text, is_error) = server.call(json!({"action": "transcript", "taskId": "t3"}));
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
        server.ok(json!({"action": "start", "command": command, "pty": pty, "waitMs": 1000}));
        let id = format!("t{}", if pty { 3 } else { 1 });
        let (out, _) = server.ok(json!({"action": "kill", "taskId": id}));
        assert_eq!(out["state"], "killed", "pty={pty}");
        assert!(file_len(killed) > 0, "the heartbeat never started");
        assert_stops_growing(killed);
        let (again, _) = server.ok(json!({"action": "kill", "taskId": id}));
        assert_eq!(again["state"], "killed");

        let command = format!("{} echo started", heartbeat(leaked_bash));
        let (out, _) =
            server.ok(json!({"action": "start", "command": command, "pty": pty, "waitMs": 10000}));
        assert_eq!(out["state"], "exited", "pty={pty}");
        assert_stops_growing(leaked);
    }
}

#[test]
fn closing_the_server_ends_running_tasks_and_removes_logs() {
    let (beat, beat_bash) = scratch("shutdown");
    let mut server = Server::start();
    let (out, _) = server.ok(json!({"action": "start", "command": format!("{} sleep 300", heartbeat(&beat_bash)), "waitMs": 1000}));
    let log = PathBuf::from(out["logPath"].as_str().unwrap());
    assert!(log.exists());
    // An in-flight wait must not delay shutdown: hosts escalate to SIGKILL within ~2.5 s.
    let poll = json!({"name": "fastexec", "arguments": {"action": "poll", "taskId": "t1", "waitMs": 60000}});
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
    let (out, text) =
        server.ok(json!({"action": "poll", "taskId": "t1", "input": "ok\n", "waitMs": 10000}));
    assert_eq!(out["state"], "exited");
    assert!(text.starts_with("final-ok\n"), "{text}");
    let (out, _) = server.ok(json!({"action": "list"}));
    let tasks = out["tasks"].as_array().unwrap();
    assert_eq!(tasks.len(), 64);
    let kept = |id: &str| tasks.iter().any(|task| task["taskId"] == id);
    assert!(
        kept("t1") && !kept("t2") && !kept("t3"),
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
    for (index, delay) in ["0", "0.005", "0.01", "0.02", "0.04"].iter().enumerate() {
        let id = format!("t{}", index + 1);
        let command = format!(
            "head -c 16000000 /dev/zero | tr '\\0' x; read -r go; sleep {delay}; printf '\\nFINAL_MARKER\\n'"
        );
        let (out, _) = server.ok(json!({"action": "start", "command": command, "loginShell": false, "waitMs": 0, "maxBytes": 1024}));
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
    for (id, encoding, prefix, lead, trail) in [
        ("t3", "big5", "", "\\xa4", "\\xa4"),
        ("t4", "gbk", "", "\\xd6", "\\xd0"),
        (
            "t5",
            "big5",
            "printf '%5000s' '' | tr ' ' a; ",
            "\\xa4",
            "\\xa4",
        ),
    ] {
        let command = format!("{prefix}printf '{lead}'; read -r x; printf '{trail}\\n'");
        let (_, text) = server.ok(json!({"action": "start", "command": command, "encoding": encoding, "loginShell": false, "waitMs": 1000}));
        assert!(!text.contains('\u{fffd}'), "{encoding}: {text}");
        let (_, text) =
            server.ok(json!({"action": "poll", "taskId": id, "input": "go\n", "waitMs": 10000}));
        assert!(text.starts_with("中\n"), "{encoding}: {text}");
    }
    let long = format!("x='{}'; echo ${{#x}}", "a".repeat(13_000));
    let (_, text) = server.ok(json!({"action": "start", "command": long}));
    assert!(text.starts_with("13000\n"), "{text}");
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
            json!({"action": "start", "command": "true", "maxBytes": 1000}),
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
            json!({"action": "start", "command": "true", "returnWhen": {"outputContains": {"text": "a\nb"}}}),
            "without line breaks",
        ),
        (
            json!({"action": "start", "command": "true", "returnWhen": {"screenContains": {"text": "x"}}}),
            "screenContains needs a PTY",
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
    let call = json!({"name": "fastexec", "arguments": {"action": "poll", "taskId": "t1", "waitMs": 60000}, "_meta": {"progressToken": "p1"}});
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
    let (out, _) = server.ok(json!({"action": "kill", "taskId": "t1"}));
    assert_eq!(out["state"], "killed");
}
