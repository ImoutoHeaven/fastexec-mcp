//! Input delivery regressions through a private stdio MCP server.
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
        for name in [
            "FASTEXEC_DAEMON",
            "FASTEXEC_NAMESPACE",
            "FASTEXEC_BASH",
            "FASTEXEC_OUTPUT_MODE",
            "FASTEXEC_TEST_RUNTIME_DIR",
            "FASTEXEC_TEST_TOTAL_LOG_LIMIT",
            "FASTEXEC_TEST_NONE_LIMIT",
        ] {
            command.env_remove(name);
        }
        let mut child = command
            .env("FASTEXEC_STRUCTURED_CONTENT", "true")
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
        self.request(
            "tools/call",
            json!({"name": "exec", "arguments": arguments}),
        )
    }

    fn task(&mut self, arguments: Value) -> String {
        let result = self.exec(arguments);
        assert_ne!(result["isError"], true, "{result}");
        result["structuredContent"]["taskId"]
            .as_str()
            .unwrap()
            .to_owned()
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

#[test]
fn pipe_input_is_confirmed_and_limit_rejection_delivers_nothing() {
    let mut server = Server::start();
    let task = server.task(json!({
        "action": "start", "command": "wc -c", "waitMs": 0
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

#[cfg(unix)]
#[test]
fn the_first_write_to_closed_stdin_reports_the_writer_error() {
    let mut server = Server::start();
    let task = server.task(json!({
        "action": "start", "command": "exec 0<&-; echo READY; sleep 30",
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
        state["structuredContent"]["inputError"]
            .as_str()
            .unwrap()
            .contains("INPUT_WRITE_FAILED"),
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
        let warning = result["structuredContent"]["inputWarning"]
            .as_str()
            .unwrap();
        for risk in [
            "leading input byte",
            "after a resize",
            "short keys",
            "does not confirm delivery to the program",
        ] {
            assert!(warning.contains(risk), "{result}");
        }
        assert!(
            text(&result).contains(warning),
            "full warning beyond maxBytes: {result}"
        );
        assert!(
            result["structuredContent"]["warnings"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry.as_str() == Some(warning)),
            "{result}"
        );
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
        let warning = result["structuredContent"]["inputWarning"]
            .as_str()
            .unwrap();
        assert!(warning.contains("across multiple polls"), "{result}");
        assert!(warning.contains("Use pipe stdin or a file"), "{result}");
        let body = text(&result);
        assert!(
            body.contains(warning),
            "the full warning survives maxBytes: {result}"
        );
        assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
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
    assert!(
        result["structuredContent"]["inputWarning"].is_null(),
        "{result}"
    );
    assert!(
        text(&result).contains("f3336bea752b5a28743033dd2c844a4a63fba08871aaee2586a2bf2d69be83a2"),
        "{result}"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn a_blocked_writer_reports_uncertain_delivery_and_warns_against_retry() {
    let mut server = Server::start();
    let task = server.task(json!({
        "action": "start", "command": "echo READY; sleep 30",
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
