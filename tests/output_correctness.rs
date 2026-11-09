//! Stored output reads must reject missing bytes without consuming the reader's position.
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
        Self::with_render("both", true)
    }

    fn with_render(mode: &str, structured: bool) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        for name in [
            "FASTEXEC_DAEMON",
            "FASTEXEC_NAMESPACE",
            "FASTEXEC_BASH",
            "FASTEXEC_TEST_RUNTIME_DIR",
            "FASTEXEC_TEST_TOTAL_LOG_LIMIT",
            "FASTEXEC_TEST_NONE_LIMIT",
        ] {
            command.env_remove(name);
        }
        let mut child = command
            .env(
                "FASTEXEC_STRUCTURED_CONTENT",
                if structured { "true" } else { "false" },
            )
            .env("FASTEXEC_OUTPUT_MODE", mode)
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
        self.request(
            "tools/call",
            json!({"name": "exec", "arguments": arguments}),
        )
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

fn assert_log_recovers(command: &str, expected: &[u8], retained: usize, scan: bool) {
    let mut server = Server::start();
    let mut start = json!({"action": "start", "command": command, "waitMs": 10000});
    if scan {
        start["returnWhen"] = json!({"outputContains": ["A"]});
    }
    let result = server.exec(start);
    assert_ne!(result["isError"], true, "{}", text(&result));
    let metadata = &result["structuredContent"];
    let path = PathBuf::from(metadata["logPath"].as_str().unwrap());
    let original = std::fs::read(&path).unwrap();
    assert_eq!(original, expected);
    let task = metadata["taskId"].as_str().unwrap();
    let bare = task.split_once('.').unwrap().0;
    // A chosen new reader starts at byte zero, and can be retried after the error.
    let handle = format!("{bare}.abcd");
    std::fs::write(&path, &original[..retained]).unwrap();
    let mut poll = json!({"action": "poll", "taskId": handle, "waitMs": 0,
                          "truncate": "none", "raw": true});
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
        "waitMs": 0, "truncate": "none", "raw": true}));
    assert_ne!(recovered["isError"], true, "{}", text(&recovered));
    let output = recovered["structuredContent"]["output"].as_str().unwrap();
    assert_eq!(
        output.as_bytes(),
        expected.strip_suffix(b"\n").unwrap_or(expected)
    );
    let consumed = server.exec(json!({"action": "poll", "taskId": handle, "waitMs": 0}));
    assert_eq!(consumed["structuredContent"]["output"], "");
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
    let metadata = &result["structuredContent"];
    let handle = metadata["taskId"].as_str().unwrap();
    let path =
        PathBuf::from(metadata["logPath"].as_str().unwrap()).with_extension("transcript.txt");
    let original = std::fs::read(&path).unwrap();
    assert!(original.starts_with(b"A\nB\nC\nD\n"));
    std::fs::write(&path, &original[..1]).unwrap();
    let arguments = json!({"action": "transcript", "taskId": handle, "truncate": "none"});
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
    assert_eq!(recovered["structuredContent"]["output"], "A\nB\nC\nD");
    assert_eq!(recovered["structuredContent"]["firstLine"], 1);
    let consumed = server.exec(arguments);
    assert_eq!(consumed["structuredContent"]["output"], "");
}

fn assert_encoding_warning(result: &Value, structured: bool) {
    assert_ne!(result["isError"], true, "{}", text(result));
    let body = text(result);
    assert!(body.contains("Warning:"), "{body}");
    assert!(body.contains("invalid in UTF-8; pass encoding"), "{body}");
    if structured {
        assert_eq!(result["structuredContent"]["encodingErrors"], 1);
        assert!(
            result["structuredContent"]["warnings"]
                .as_array()
                .unwrap()
                .iter()
                .any(|warning| warning.as_str().unwrap().contains("invalid in UTF-8"))
        );
    } else {
        assert!(result.get("structuredContent").is_none());
    }
}

#[test]
fn encoding_warnings_survive_every_mode_and_small_output_budget() {
    for mode in ["text", "both", "structured"] {
        for structured in [false, true] {
            let mut server = Server::with_render(mode, structured);
            for view in 0..3 {
                let mut arguments = json!({"action": "start", "command": "printf '\\377DATA\\n'",
                                           "waitMs": 10000, "maxBytes": 100});
                match view {
                    0 => {}
                    1 => arguments["raw"] = json!(true),
                    2 => {
                        arguments["pty"] = json!(true);
                        arguments["screen"] = json!(true);
                    }
                    _ => unreachable!(),
                }
                assert_encoding_warning(&server.exec(arguments), structured);
            }

            // A transcript has its own UTF-8 bytes; corrupt one without shortening the file.
            let started = server.exec(json!({"action": "start", "pty": true,
                "command": "printf 'ADATA\\n'", "waitMs": 10000}));
            assert_ne!(started["isError"], true, "{}", text(&started));
            let (handle, path) = if structured {
                (
                    started["structuredContent"]["taskId"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                    PathBuf::from(started["structuredContent"]["logPath"].as_str().unwrap()),
                )
            } else {
                let body = text(&started);
                let status = body
                    .lines()
                    .find(|line| line.starts_with("[exited 0] "))
                    .unwrap();
                (
                    status.split_whitespace().nth(2).unwrap().to_owned(),
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
            };
            let path = path.with_extension("transcript.txt");
            let mut bytes = std::fs::read(&path).unwrap();
            assert!(bytes.starts_with(b"ADATA\n"));
            bytes[0] = 0xff;
            std::fs::write(&path, bytes).unwrap();
            let result = server.exec(json!({"action": "transcript", "taskId": handle,
                                           "maxBytes": 100}));
            assert_encoding_warning(&result, structured);
        }
    }
}
