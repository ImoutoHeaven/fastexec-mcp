//! Input delivery regressions through a private stdio MCP server.
mod support;

use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::time::Duration;

struct Server {
    child: Child,
    lines: Receiver<Value>,
    next: u64,
}

impl Server {
    fn start() -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        support::scrub_fastexec_env(&mut command);
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let (send, lines) = channel();
        let stdout = child.stdout.take().unwrap();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Ok(value) = serde_json::from_str(&line)
                    && send.send(value).is_err()
                {
                    break;
                }
            }
        });
        let mut server = Self {
            child,
            lines,
            next: 0,
        };
        server.request(
            "initialize",
            json!({
                "protocolVersion": "2025-06-18", "capabilities": {},
                "clientInfo": {"name": "input-correctness", "version": "1"}
            }),
        );
        server.send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        server
    }

    fn send(&mut self, value: Value) {
        let stdin = self.child.stdin.as_mut().unwrap();
        writeln!(stdin, "{value}").unwrap();
        stdin.flush().unwrap();
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next += 1;
        let id = self.next;
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        loop {
            let response = self.lines.recv_timeout(Duration::from_secs(30)).unwrap();
            if response["id"] == id {
                assert!(response["error"].is_null(), "{response}");
                return response["result"].clone();
            }
        }
    }

    fn exec(&mut self, arguments: Value) -> Value {
        let result = self.request(
            "tools/call",
            json!({"name": "exec", "arguments": arguments}),
        );
        assert!(result.get("structuredContent").is_none(), "{result}");
        result
    }

    fn task(&mut self, arguments: Value) -> String {
        let result = self.exec(arguments);
        assert_ne!(result["isError"], true, "{result}");
        task_id(&text(&result))
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        drop(self.child.stdin.take());
        let _ = self.child.wait();
    }
}

fn text(result: &Value) -> String {
    result["content"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|block| block["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

fn status_line(body: &str) -> &str {
    body.lines()
        .rev()
        .find(|line| line.starts_with('[') && line.contains("] "))
        .unwrap_or_else(|| panic!("no status line: {body}"))
}

fn task_id(body: &str) -> String {
    status_line(body)
        .split_once("] ")
        .unwrap()
        .1
        .split_whitespace()
        .next()
        .unwrap()
        .to_owned()
}

#[test]
fn pipe_input_is_confirmed_and_limit_rejection_delivers_nothing() {
    let mut server = Server::start();
    let task = server.task(json!({
        "action": "start", "command": "wc -c", "eof": false, "waitMs": 0
    }));
    let too_large = server.exec(json!({
        "action": "poll", "taskId": task, "input": "a".repeat(16_385),
        "eof": true, "waitMs": 0
    }));
    assert_eq!(too_large["isError"], true, "{too_large}");
    assert!(text(&too_large).contains("16 KiB"));
    let result = server.exec(json!({
        "action": "poll", "taskId": task, "input": "a".repeat(16_384),
        "eof": true, "waitMs": 10_000
    }));
    assert_ne!(result["isError"], true, "{result}");
    assert!(
        text(&result).lines().any(|line| line.trim() == "16384"),
        "{result}"
    );
}

#[test]
fn empty_poll_input_requires_an_open_pipe_and_eof_alone_is_idempotent() {
    let mut server = Server::start();
    let open = server.task(json!({
        "action": "start", "command": "wc -c", "eof": false, "waitMs": 0
    }));
    let empty = server.exec(json!({"action": "poll", "taskId": open, "input": "", "waitMs": 0}));
    assert_ne!(empty["isError"], true, "{empty}");
    let closed = server.exec(json!({
        "action": "poll", "taskId": open, "input": "", "eof": true, "waitMs": 10_000
    }));
    assert_ne!(closed["isError"], true, "{closed}");
    let closed_text = text(&closed);
    assert!(status_line(&closed_text).starts_with("[exited 0] "), "{closed}");
    assert!(
        closed_text.lines().any(|line| line.trim() == "0"),
        "{closed}"
    );

    let null = server.task(json!({"action": "start", "command": "cat", "waitMs": 10_000}));
    for id in [open, null] {
        for eof in [false, true] {
            let mut poll = json!({"action": "poll", "taskId": id, "waitMs": 0});
            if eof {
                poll["eof"] = json!(true);
            }
            let no_input = server.exec(poll.clone());
            assert_ne!(no_input["isError"], true, "{no_input}");
            poll["input"] = json!("");
            let refused = server.exec(poll);
            assert_eq!(refused["isError"], true, "{refused}");
            assert!(text(&refused).contains("eof: false"), "{refused}");
        }
    }
}

#[cfg(unix)]
#[test]
fn the_first_write_to_closed_stdin_reports_the_writer_error() {
    let mut server = Server::start();
    let task = server.task(json!({
        "action": "start", "command": "exec 0<&-; echo READY; sleep 30", "eof": false,
        "returnWhen": {"outputContains": ["READY"]}, "waitMs": 10_000
    }));
    let result = server.exec(json!({
        "action": "poll", "taskId": task, "input": "must-not-disappear",
        "waitMs": 0
    }));
    assert_eq!(result["isError"], true, "{result}");
    assert!(text(&result).contains("INPUT_WRITE_FAILED"), "{result}");
    let state = server.exec(json!({"action": "poll", "taskId": task, "waitMs": 0}));
    assert!(
        text(&state).contains("INPUT_WRITE_FAILED"),
        "{state}"
    );
}

#[cfg(windows)]
#[test]
fn windows_resize_input_risk_stays_explicit_for_short_keys_and_later_input() {
    let mut server = Server::start();
    let task = server.task(json!({
        "action": "start", "pty": true, "cols": 100, "rows": 40,
        "command": "printf 'READY\\n'; sleep 30",
        "returnWhen": {"outputContains": ["READY"]}, "waitMs": 10_000
    }));
    for arguments in [
        json!({"action": "poll", "taskId": task, "cols": 80, "rows": 20,
               "keys": ["Up"], "keyDelayMs": 0, "waitMs": 0, "maxBytes": 100}),
        json!({"action": "poll", "taskId": task, "input": "x",
               "waitMs": 0, "maxBytes": 100, "raw": true}),
    ] {
        let result = server.exec(arguments);
        assert_ne!(result["isError"], true, "{result}");
        let body = text(&result);
        assert!(body.contains("Warning:"), "full warning beyond maxBytes: {result}");
        for risk in [
            "leading input byte",
            "after a resize",
            "short keys",
            "does not confirm delivery to the program",
        ] {
            assert!(body.contains(risk), "{result}");
        }
    }
    let ended = server.exec(json!({"action": "kill", "taskId": task, "waitMs": 10_000}));
    assert_ne!(ended["isError"], true, "{ended}");
}

#[cfg(target_os = "linux")]
#[test]
fn canonical_input_keys_and_cross_poll_lines_always_report_the_risk() {
    let mut server = Server::start();
    for mode in 0..4 {
        let task = server.task(json!({
            "action": "start", "pty": true,
            "command": "stty -echo icanon; echo READY; IFS= read -r line; printf 'LEN=%s\\n' \"${#line}\"",
            "returnWhen": {"outputContains": ["READY"]}, "waitMs": 10_000
        }));
        let mut poll = json!({
            "action": "poll", "taskId": task, "waitMs": 10_000, "maxBytes": 100
        });
        match mode {
            0 | 3 => {
                poll["input"] = json!("a".repeat(8192));
                poll["keys"] = json!(["Enter"]);
                if mode == 3 {
                    poll["raw"] = json!(true);
                }
            }
            1 => {
                for _ in 0..4 {
                    let part = server.exec(json!({
                        "action": "poll", "taskId": task, "input": "a".repeat(2048),
                        "waitMs": 0, "maxBytes": 100
                    }));
                    assert_ne!(part["isError"], true, "{part}");
                    assert!(text(&part).contains("Canonical PTY"), "{part}");
                    assert!(text(&part).contains("Use pipe stdin or a file"), "{part}");
                }
                poll["keys"] = json!(["Enter"]);
            }
            2 => {
                let mut keys = vec!["a"; 8192];
                keys.push("Enter");
                poll["keys"] = json!(keys);
                poll["keyDelayMs"] = json!(0);
                poll["screen"] = json!(true);
            }
            _ => unreachable!(),
        }
        let result = server.exec(poll);
        assert_ne!(result["isError"], true, "{result}");
        let body = text(&result);
        assert!(
            body.contains("across multiple polls"),
            "the full warning survives maxBytes: {result}"
        );
        assert!(
            body.contains("Use pipe stdin or a file"),
            "the full warning survives maxBytes: {result}"
        );
        assert!(status_line(&body).starts_with("[exited 0] "), "{result}");
    }
}

#[cfg(target_os = "linux")]
#[test]
fn noncanonical_pty_delivers_a_full_keys_burst_without_a_canonical_warning() {
    let mut server = Server::start();
    let task = server.task(json!({
        "action": "start", "pty": true,
        "command": "stty -echo -icanon min 1 time 0; echo READY; head -c 16384 | sha256sum",
        "returnWhen": {"outputContains": ["READY"]}, "waitMs": 10_000
    }));
    let result = server.exec(json!({
        "action": "poll", "taskId": task, "keys": vec!["a"; 16_384],
        "keyDelayMs": 0, "waitMs": 10_000
    }));
    assert_ne!(result["isError"], true, "{result}");
    let body = text(&result);
    assert!(!body.contains("Canonical PTY"), "{result}");
    assert!(!body.contains("across multiple polls"), "{result}");
    assert!(
        body.contains("f3336bea752b5a28743033dd2c844a4a63fba08871aaee2586a2bf2d69be83a2"),
        "{result}"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn a_blocked_writer_reports_uncertain_delivery_and_warns_against_retry() {
    let mut server = Server::start();
    let task = server.task(json!({
        "action": "start", "command": "echo READY; sleep 30", "eof": false,
        "returnWhen": {"outputContains": ["READY"]}, "waitMs": 10_000
    }));
    for _ in 0..80 {
        let result = server.exec(json!({
            "action": "poll", "taskId": task, "input": "a".repeat(16_384), "waitMs": 0
        }));
        if result["isError"] == true {
            let body = text(&result);
            assert!(body.contains("INPUT_DELIVERY_UNCONFIRMED"), "{result}");
            assert!(body.contains("still queued"), "{result}");
            assert!(body.contains("Do not blindly retry"), "{result}");
            return;
        }
    }
    panic!("the unread pipe unexpectedly accepted more than 1 MiB");
}
