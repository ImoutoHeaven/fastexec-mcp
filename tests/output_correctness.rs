//! Stored output reads must reject missing bytes without consuming the reader's position.
mod support;

use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
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
            .env("FASTEXEC_CODEMODE", "true")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let (send, lines) = channel();
        let stdout = child.stdout.take().unwrap();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let line = line.unwrap();
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
                "clientInfo": {"name": "output-correctness", "version": "1"}
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

    fn codemode(&mut self, arguments: Value) -> Value {
        let result = self.request(
            "tools/call",
            json!({"name": "codemode", "arguments": arguments}),
        );
        assert!(result.get("structuredContent").is_none(), "{result}");
        result
    }

    fn log_path(&mut self, bare_task_id: &str) -> PathBuf {
        let result = self.codemode(json!({
            "code": format!(
                "return (await tools.exec({{action: 'poll', taskId: {bare_task_id:?}, waitMs: 0}})).logPath;"
            )
        }));
        assert_ne!(result["isError"], true, "{}", text(&result));
        let rendered = text(&result);
        let (body, status) = split_output(&rendered);
        assert!(status.starts_with("[exited 0] "), "{status}");
        PathBuf::from(body)
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

fn split_output(rendered: &str) -> (&str, &str) {
    if let Some(pos) = rendered.rfind("\n\n[") {
        (&rendered[..pos], &rendered[pos + 2..])
    } else {
        ("", rendered)
    }
}

fn status_line(body: &str) -> &str {
    body.lines()
        .rev()
        .find(|line| line.starts_with('[') && line.contains("] "))
        .unwrap_or_else(|| panic!("no status line: {body}"))
}

fn task_handle(result: &Value) -> String {
    let body = text(result);
    status_line(&body)
        .split_once("] ")
        .unwrap()
        .1
        .split_whitespace()
        .next()
        .unwrap()
        .to_owned()
}

fn assert_log_recovers(command: &str, expected: &[u8], retained: usize, scan: bool) {
    let mut server = Server::start();
    let mut start = json!({"action": "start", "command": command, "waitMs": 10000});
    if scan {
        start["returnWhen"] = json!({"outputContains": ["A"]});
    }
    let result = server.exec(start);
    assert_ne!(result["isError"], true, "{}", text(&result));
    let task = task_handle(&result);
    if scan {
        // `start` returned as soon as `"A"` matched; wait for the task to finish so the full log
        // is flushed before reading and truncating it.
        let settled = server.exec(json!({"action": "poll", "taskId": task, "waitMs": 10000}));
        assert_ne!(settled["isError"], true, "{}", text(&settled));
    }
    let bare = task.split_once('.').unwrap().0;
    let path = server.log_path(bare);
    let original = std::fs::read(&path).unwrap();
    assert_eq!(original, expected);
    // A chosen new reader starts at byte zero, and can be retried after the error.
    let handle = format!("{bare}.abcd");
    std::fs::write(&path, &original[..retained]).unwrap();
    let mut poll = json!({"action": "poll", "taskId": handle, "waitMs": 0,
                          "truncate": "head", "maxBytes": 4194304, "raw": true});
    if scan {
        poll["returnWhen"] = json!({"outputContains": ["absent-marker"]});
    }
    let failed = server.exec(poll);
    assert_eq!(failed["isError"], true, "{}", text(&failed));
    assert!(
        text(&failed).contains("Cannot read the task log"),
        "{}",
        text(&failed)
    );
    std::fs::write(&path, &original).unwrap();
    let recovered = server.exec(json!({"action": "poll", "taskId": handle,
        "waitMs": 0, "truncate": "head", "maxBytes": 4194304, "raw": true}));
    assert_ne!(recovered["isError"], true, "{}", text(&recovered));
    let recovered_text = text(&recovered);
    let (output, _) = split_output(&recovered_text);
    assert_eq!(
        output.as_bytes(),
        expected.strip_suffix(b"\n").unwrap_or(expected)
    );
    let consumed = server.exec(json!({"action": "poll", "taskId": handle, "waitMs": 0}));
    let consumed_text = text(&consumed);
    let (consumed_body, _) = split_output(&consumed_text);
    assert!(
        consumed_body.is_empty() || consumed_body == "(no new output)",
        "{consumed_text}"
    );
}

#[test]
fn a_short_log_fails_and_the_same_reader_recovers_every_byte() {
    assert_log_recovers("printf 'A\\nB\\nC\\nD\\n'", b"A\nB\nC\nD\n", 1, false);
}

#[test]
fn a_short_later_chunk_does_not_consume_the_first_log_chunk() {
    assert_log_recovers(
        "head -c 65544 /dev/zero | tr '\\0' A",
        &vec![b'A'; 65544],
        65537,
        false,
    );
}

#[test]
fn return_when_scanning_rejects_a_short_log() {
    assert_log_recovers(
        "printf 'A\\nB\\nC\\nD\\n'; IFS= read -r line",
        b"A\nB\nC\nD\n",
        1,
        true,
    );
}

#[test]
fn a_short_transcript_fails_without_advancing_its_reader() {
    let mut server = Server::start();
    let result = server.exec(json!({"action": "start", "pty": true,
        "command": "printf 'A\\nB\\nC\\nD\\n'", "waitMs": 10000}));
    assert_ne!(result["isError"], true, "{}", text(&result));
    let handle = task_handle(&result);
    let bare = handle.split_once('.').unwrap().0;
    let path = server.log_path(bare).with_extension("transcript.txt");
    let original = std::fs::read(&path).unwrap();
    assert!(original.starts_with(b"A\nB\nC\nD\n"));
    std::fs::write(&path, &original[..1]).unwrap();
    let arguments = json!({"action": "transcript", "taskId": handle, "maxBytes": 4194304});
    let failed = server.exec(arguments.clone());
    assert_eq!(failed["isError"], true, "{}", text(&failed));
    assert!(
        text(&failed).contains("Cannot read the transcript file"),
        "{}",
        text(&failed)
    );
    std::fs::write(&path, &original).unwrap();
    let recovered = server.exec(arguments.clone());
    assert_ne!(recovered["isError"], true, "{}", text(&recovered));
    let recovered_text = text(&recovered);
    let (body, status) = split_output(&recovered_text);
    assert_eq!(body, "A\nB\nC\nD");
    assert!(status.contains("this read starts at line 1"), "{status}");
    let consumed = server.exec(arguments);
    assert!(
        text(&consumed).contains("(no transcript lines since the last read)"),
        "{}",
        text(&consumed)
    );
}

fn task_location(result: &Value) -> (String, PathBuf) {
    let body = text(result);
    let status = status_line(&body);
    (
        status
            .split_once("] ")
            .unwrap()
            .1
            .split_whitespace()
            .next()
            .unwrap()
            .to_owned(),
        PathBuf::from(
            status
                .split_once(" · log ")
                .unwrap()
                .1
                .split(" · ")
                .next()
                .unwrap(),
        ),
    )
}

fn assert_encoding_warning(result: &Value) {
    assert_ne!(result["isError"], true, "{}", text(result));
    assert!(result.get("structuredContent").is_none(), "{result}");
    let body = text(result);
    assert!(body.contains("Warning:"), "{body}");
    assert!(body.contains("invalid in UTF-8; pass encoding"), "{body}");
}

#[test]
fn encoding_warnings_survive_small_output_budget() {
    let mut server = Server::start();
    for raw in [false, true] {
        let arguments = json!({"action": "start", "command": "printf '\\377DATA\\n'",
                               "waitMs": 10000, "maxBytes": 100, "raw": raw});
        assert_encoding_warning(&server.exec(arguments));
    }

    // ConPTY can normalize invalid program bytes; corrupt the retained files instead.
    let started = server.exec(json!({"action": "start", "pty": true,
        "command": "printf 'ADATA\\n'; seq 1 200", "maxBytes": 300, "waitMs": 10000}));
    assert_ne!(started["isError"], true, "{}", text(&started));
    let (handle, path) = task_location(&started);
    let task_id = handle.split_once('.').unwrap().0;
    let mut log = std::fs::read(&path).unwrap();
    assert!(!log.is_empty());
    assert!(std::str::from_utf8(&log).is_ok());
    let payload = log
        .windows(b"ADATA".len())
        .rposition(|part| part == b"ADATA")
        .expect("the PTY log contains the displayed payload");
    log[payload] = 0xff;
    assert!(std::str::from_utf8(&log).is_err());
    std::fs::write(&path, log).unwrap();
    let screen = server.exec(json!({"action": "poll", "taskId": task_id,
        "screen": true, "waitMs": 0, "maxBytes": 100}));
    assert_encoding_warning(&screen);

    let path = path.with_extension("transcript.txt");
    let mut bytes = std::fs::read(&path).unwrap();
    assert!(bytes.starts_with(b"ADATA\n"));
    bytes[0] = 0xff;
    std::fs::write(&path, bytes).unwrap();
    let result = server.exec(json!({"action": "transcript", "taskId": task_id,
                                   "maxBytes": 100}));
    assert_encoding_warning(&result);
}

#[test]
fn head_and_tail_use_the_body_budget_and_report_omissions_in_status() {
    let mut server = Server::start();
    let endpoint = "x".repeat(90);
    for raw in [false, true] {
        for mode in ["head", "tail"] {
            let (first, last, omitted) = if mode == "head" {
                (endpoint.as_str(), "this line does not fit", [2, 2])
            } else {
                ("this line does not fit", endpoint.as_str(), [1, 1])
            };
            let arguments = json!({
                "action": "start",
                "command": format!("printf '%s\\n' '{first}' '{last}'"),
                "truncate": mode,
                "maxBytes": 100,
                "raw": raw,
            });
            let direct = server.exec(arguments.clone());
            let rendered = text(&direct);
            let (body, status) = split_output(&rendered);
            assert_eq!(body, endpoint);
            assert!(
                status.contains(&format!(
                    "1 lines omitted: log lines {}-{}",
                    omitted[0], omitted[1]
                )),
                "{status}"
            );
            assert!(status.contains(" · log "), "{status}");
            assert!(!status.contains("cut"), "{status}");

            let nested = server.codemode(json!({
                "code": format!("return await tools.exec({arguments});")
            }));
            assert_ne!(nested["isError"], true, "{}", text(&nested));
            let nested_text = text(&nested);
            let (nested_body, nested_status) = split_output(&nested_text);
            assert!(nested_status.starts_with("[exited 0] "), "{nested_status}");
            let data: Value = serde_json::from_str(nested_body).unwrap();
            assert_eq!(data["output"], endpoint);
            assert_eq!(data["omittedRange"], json!(omitted));
            assert_eq!(data["cutLines"].as_u64().unwrap_or(0), 0);
        }
    }
}

#[test]
fn p0_1_and_p1_6_windows_and_nul_preservation_hold_direct_and_nested() {
    let mut server = Server::start();

    // Direct 10,000 B JSON line under default parameters: whole, valid JSON, no cutLines.
    let payload_10k = "j".repeat(9980);
    let json_10k = server.exec(json!({
        "action": "start",
        "command": format!("printf '{{\"k\":\"%s\"}}\\n' '{payload_10k}'")
    }));
    assert_ne!(json_10k["isError"], true, "{}", text(&json_10k));
    let json_10k_text = text(&json_10k);
    let (body_10k, status_10k) = split_output(&json_10k_text);
    assert!(!status_10k.contains("cut"), "{status_10k}");
    let parsed_10k: Value = serde_json::from_str(body_10k).unwrap();
    assert_eq!(parsed_10k["k"].as_str().unwrap().len(), 9980);

    // Direct 20,000-char line followed by ERROR: final line: tail line whole, first line cut.
    let err_tail = server.exec(json!({
        "action": "start",
        "command": "(printf '%20000s\\n' '' | tr ' ' x; printf 'ERROR: final line\\n')"
    }));
    let err_tail_text = text(&err_tail);
    let (err_body, _) = split_output(&err_tail_text);
    assert!(err_body.len() <= 16_384, "len={}", err_body.len());
    assert!(err_body.ends_with("ERROR: final line"), "{err_body}");
    assert!(err_body.contains("line cut at"), "{err_body}");

    // Single 200 KiB multi-byte UTF-8 line under default budget: cut on char boundary, logPath in status.
    let multi_200k = server.exec(json!({
        "action": "start",
        "command": "awk 'BEGIN { for (i = 0; i < 70000; i++) printf \"中\"; printf \"\\n\" }'"
    }));
    let multi_200k_text = text(&multi_200k);
    let (body_200k, status_200k) = split_output(&multi_200k_text);
    assert!(body_200k.len() <= 16_384);
    assert!(body_200k.contains("line cut at"), "{body_200k}");
    assert!(status_200k.contains(" · log "), "{status_200k}");

    // Codemode script 1 & 2 from Spec P0-1.
    let cm1 = server.codemode(json!({
        "code": "text('x'.repeat(20000)); text('MIDDLE'); text('END-LINE-IMPORTANT'); return 'RET';"
    }));
    let cm1_text = text(&cm1);
    let (cm1_body, _) = split_output(&cm1_text);
    assert!(cm1_body.len() <= 16_384);
    assert!(cm1_body.contains("line cut at"), "{cm1_body}");
    assert!(
        cm1_body.ends_with("MIDDLE\nEND-LINE-IMPORTANT\nRET"),
        "{cm1_body}"
    );

    let cm1_many = server.codemode(json!({
        "code": "text('x'.repeat(20000)); for (let i = 0; i < 3000; i++) text('MIDDLE-' + i); text('END-LINE-IMPORTANT'); return 'RET';"
    }));
    let cm1_many_text = text(&cm1_many);
    let (cm1_many_body, _) = split_output(&cm1_many_text);
    assert!(cm1_many_body.len() <= 16_384);
    assert!(
        cm1_many_body.contains("lines omitted: log lines "),
        "{cm1_many_body}"
    );
    assert!(
        cm1_many_body.ends_with("END-LINE-IMPORTANT\nRET"),
        "{cm1_many_body}"
    );

    let cm2 = server.codemode(json!({
        "code": "text('H'.repeat(15000)); return {tail: 'V'.repeat(12000), done: 'END'};"
    }));
    let cm2_text = text(&cm2);
    let (cm2_body, _) = split_output(&cm2_text);
    assert!(cm2_body.len() <= 16_384);
    let last_line = cm2_body.lines().last().unwrap();
    let parsed_cm2: Value = serde_json::from_str(last_line).expect("12,000 B return line whole");
    assert_eq!(parsed_cm2["done"], "END");
    assert_eq!(parsed_cm2["tail"].as_str().unwrap().len(), 12_000);

    // Nested 150 lines with maxBytes: 180 (head default, no status deduction) + P1-6 NUL preservation.
    let dir = std::env::temp_dir().join(format!("fastexec-nul-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    for name in [
        "alpha beta.txt",
        "quote's file.txt",
        "third 'space' item.txt",
    ] {
        std::fs::write(dir.join(name), "ok").unwrap();
    }
    let dir_str = dir.to_string_lossy().replace('\\', "/");

    let direct_nul = server.exec(json!({
        "action": "start",
        "command": "printf 'a b\\0c\\nd\\0'"
    }));
    let direct_nul_text = text(&direct_nul);
    let (direct_nul_body, _) = split_output(&direct_nul_text);
    assert_eq!(direct_nul_body, "a bc\nd");

    let nested_checks = server.codemode(json!({
        "cwd": dir_str,
        "code": r#"
            const r150 = await tools.exec({
                action: "start",
                maxBytes: 180,
                command: "awk 'BEGIN { for (i = 1; i <= 150; i++) printf \"row-%03d\\n\", i }'"
            });
            if (r150.output.length < 150 || r150.output.length > 180) {
                throw new Error("bad r150 len=" + r150.output.length + " out=" + JSON.stringify(r150.output));
            }
            if (!Array.isArray(r150.omittedRange) || r150.omittedRange[1] !== 150 || r150.omittedRange[0] < 15) {
                throw new Error("expected tail omission, got " + JSON.stringify(r150.omittedRange));
            }

            const nulOut = await tools.exec({
                action: "start",
                command: "printf 'a b\\0c\\nd\\0'"
            });
            if (nulOut.output !== "a b\u0000c\nd\u0000") {
                throw new Error("nested NUL mismatch: " + JSON.stringify(nulOut.output));
            }

            const found = await tools.exec({
                action: "start",
                command: "find . -maxdepth 1 -type f -print0"
            });
            const names = found.output
                .split("\0")
                .filter(Boolean)
                .map(p => p.replace(/^\.\//, ""))
                .sort();
            return names;
        "#
    }));
    assert_ne!(nested_checks["isError"], true, "{}", text(&nested_checks));
    let nested_checks_text = text(&nested_checks);
    let (nested_checks_body, nested_checks_status) = split_output(&nested_checks_text);
    assert!(
        nested_checks_status.starts_with("[exited 0] "),
        "{nested_checks_status}"
    );
    let names: Vec<String> = serde_json::from_str(nested_checks_body).unwrap();
    assert_eq!(
        names,
        vec![
            "alpha beta.txt".to_string(),
            "quote's file.txt".to_string(),
            "third 'space' item.txt".to_string()
        ]
    );
    let _ = std::fs::remove_dir_all(&dir);
}
