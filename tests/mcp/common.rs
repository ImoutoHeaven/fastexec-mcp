use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, Instant};

pub(super) struct Server {
    pub(super) child: Child,
    pub(super) stdin: Option<ChildStdin>,
    pub(super) lines: Receiver<Value>,
    pub(super) next_id: u64,
    pub(super) pending_messages: Vec<Value>,
    /// The IDs of the tasks `start` calls began, in order.
    pub(super) started: Vec<String>,
}

pub(super) fn body_of(text: &str) -> &str {
    text.split_once("\n\n[").map_or(text, |(body, _)| body)
}

pub(super) fn status_line(text: &str) -> &str {
    text.lines()
        .rev()
        .find(|line| line.starts_with('[') && line.contains("] "))
        .unwrap_or_else(|| panic!("no status line: {text}"))
}

pub(super) fn task_id_of(text: &str) -> &str {
    status_line(text)
        .split_once("] ")
        .and_then(|(_, rest)| rest.split(' ').next())
        .unwrap_or_else(|| panic!("no taskId in status line: {text}"))
}

pub(super) fn log_path_of(text: &str) -> Option<&str> {
    let after = status_line(text).split_once(" · log ")?.1;
    Some(after.split(" · ").next().unwrap_or(after))
}

pub(super) fn bare_id(handle: &str) -> &str {
    handle.split_once('.').expect("a reader handle").0
}

impl Server {
    pub(super) fn start() -> Server {
        Server::start_with(|_| {})
    }

    /// Starts the server with its temp directory (and so its log directory) under `temp`.
    pub(super) fn start_with_temp(temp: &std::path::Path) -> Server {
        Server::start_with(|command| {
            command
                .env("TEMP", temp)
                .env("TMP", temp)
                .env("TMPDIR", temp);
        })
    }

    /// Starts the server after `configure` adjusts its launch, such as its environment.
    pub(super) fn start_with(configure: impl FnOnce(&mut Command)) -> Server {
        Server::start_bin(
            std::path::Path::new(env!("CARGO_BIN_EXE_fastexec")),
            configure,
        )
    }

    /// Starts the server from `executable`, such as a world-reachable copy for dropped-uid tests.
    pub(super) fn start_bin(
        executable: &std::path::Path,
        configure: impl FnOnce(&mut Command),
    ) -> Server {
        let mut command = Command::new(executable);
        crate::support::scrub_fastexec_env(&mut command);
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
    pub(super) fn try_start_with(configure: impl FnOnce(&mut Command)) -> Result<Server, String> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        crate::support::scrub_fastexec_env(&mut command);
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

    pub(super) fn send(&mut self, message: Value) {
        let stdin = self.stdin.as_mut().unwrap();
        writeln!(stdin, "{message}").unwrap();
        stdin.flush().unwrap();
    }

    pub(super) fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        self.response(id)
    }

    pub(super) fn response(&mut self, id: u64) -> Value {
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

    /// Calls the exec tool and returns (text, isError), asserting no structuredContent.
    pub(super) fn call(&mut self, arguments: Value) -> (String, bool) {
        let start = arguments["action"] == "start";
        let message = self.request(
            "tools/call",
            json!({"name": "exec", "arguments": arguments}),
        );
        let result = &message["result"];
        assert!(result.is_object(), "protocol error: {message}");
        assert!(
            result.get("structuredContent").is_none(),
            "unexpected structuredContent: {result}"
        );
        let text = result["content"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let is_error = result["isError"] == json!(true);
        if start && !is_error {
            self.started.push(task_id_of(&text).to_string());
        }
        (text, is_error)
    }

    /// How the server child ended, for a failure that has no answer to explain it.
    pub(super) fn exit_state(&mut self) -> String {
        match self.child.try_wait() {
            Ok(Some(status)) => format!("exited with {status}"),
            Ok(None) => "is still running".to_string(),
            Err(error) => format!("could not be checked: {error}"),
        }
    }

    /// The ID of the `n`th task this server started, counting from 1.
    pub(super) fn id(&self, n: usize) -> String {
        self.started[n - 1].clone()
    }

    pub(super) fn bare_id(&self, n: usize) -> String {
        bare_id(&self.started[n - 1]).to_owned()
    }

    pub(super) fn ok(&mut self, arguments: Value) -> String {
        let (text, is_error) = self.call(arguments);
        assert!(!is_error, "unexpected error: {text}");
        text
    }

    /// Calls one of the file tools: (text, isError), asserting no structuredContent.
    pub(super) fn file(&mut self, name: &str, arguments: Value) -> (String, bool) {
        let message = self.request("tools/call", json!({"name": name, "arguments": arguments}));
        let result = &message["result"];
        assert!(result.is_object(), "protocol error: {message}");
        assert!(
            result.get("structuredContent").is_none(),
            "unexpected structuredContent: {result}"
        );
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
pub(super) struct Scratch(pub(super) PathBuf);

impl Scratch {
    pub(super) fn new(name: &str) -> Scratch {
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
pub(super) fn scratch(name: &str) -> (Scratch, String) {
    let path = Scratch::new(name);
    let bash_path = path.to_string_lossy().replace('\\', "/");
    (path, bash_path)
}

pub(super) fn file_len(path: &PathBuf) -> u64 {
    std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0)
}

/// Starts a background writer that appends to `bash_path` every 100 ms.
pub(super) fn heartbeat(bash_path: &str) -> String {
    format!("(while :; do echo x >> '{bash_path}'; sleep 0.1; done) &")
}

/// Waits up to 30 s for a heartbeat's first line; a loaded machine can start it late.
pub(super) fn wait_for_heartbeat(path: &PathBuf) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while file_len(path) == 0 {
        assert!(Instant::now() < deadline, "the heartbeat never started");
        std::thread::sleep(Duration::from_millis(50));
    }
}

pub(super) fn assert_stops_growing(path: &PathBuf) {
    std::thread::sleep(Duration::from_millis(800));
    let before = file_len(path);
    std::thread::sleep(Duration::from_millis(800));
    assert_eq!(file_len(path), before, "a descendant kept running");
    let _ = std::fs::remove_file(path);
}

pub(super) fn codemode_server() -> Server {
    Server::start_with(|command| {
        command.env("FASTEXEC_CODEMODE", "true");
    })
}

pub(super) fn code_result(server: &mut Server, arguments: Value) -> Value {
    let reply = server.request(
        "tools/call",
        json!({"name":"codemode", "arguments":arguments}),
    );
    assert!(reply["result"].is_object(), "{reply}");
    assert!(
        reply["result"].get("structuredContent").is_none(),
        "unexpected structuredContent: {reply}"
    );
    reply["result"].clone()
}

pub(super) fn code_text(result: &Value) -> &str {
    result["content"][0]["text"].as_str().unwrap_or_default()
}
