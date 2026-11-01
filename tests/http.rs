//! The daemon's Streamable HTTP front door, driven over a socket the way a client that speaks
//! MCP over HTTP does: one request per connection, and an answer read back as it arrives.

use serde_json::{Value, json};
use std::io::{BufRead, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

/// A token of this run's own: 32 hexadecimal characters, so no two cases share one, and a daemon
/// left listening by mistake admits nothing anyone else knows.
fn random_token() -> String {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).expect("random bytes");
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
/// The daemon a test talks to, in a runtime directory of its own, stopped however the test ends.
struct Fixture {
    runtime: PathBuf,
    namespace: String,
    addr: SocketAddr,
    /// The token this fixture's daemon admits, made for this run alone.
    token: String,
    /// The daemon this fixture saw running, so cleanup can end that one process by PID.
    pid: std::cell::Cell<Option<u32>>,
    /// The turn this fixture holds until it ends: held, never read.
    #[allow(dead_code)]
    starting: std::sync::MutexGuard<'static, ()>,
}

/// One fixture at a time in this binary: a stated test resource limit, not a fix and not a claim
/// about the daemon. Each fixture starts a daemon and copies its image; seven at once outran the
/// ten seconds a start is given, and spawns failed with STATUS_DLL_INIT_FAILED under host process
/// pressure.
static STARTING: std::sync::Mutex<()> = std::sync::Mutex::new(());

impl Fixture {
    /// Starts a daemon that serves HTTP on a port of the system's choosing.
    fn new(tag: &str) -> Fixture {
        let runtime =
            std::env::temp_dir().join(format!("fastexec-http-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&runtime);
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
        builder.create(&runtime).unwrap();
        // The turn is taken before the fixture exists, so a start that panics still cleans up with
        // it held.
        let starting = STARTING.lock().unwrap_or_else(|error| error.into_inner());
        let mut fixture = Fixture {
            runtime,
            namespace: format!("http{tag}-{}", std::process::id()),
            addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            token: random_token(),
            pid: std::cell::Cell::new(None),
            starting,
        };
        let (code, out, err) = fixture.run(&["daemon", "start", "--http", "127.0.0.1:0"]);
        assert_eq!(code, 0, "the daemon started: {out}{err}");
        let addr = out
            .split_whitespace()
            .skip_while(|word| *word != "http")
            .nth(1)
            .unwrap_or_else(|| panic!("no http address in {out:?}"));
        fixture.addr = addr.parse().expect("the daemon printed an address");
        fixture
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        command
            .args(args)
            .env("FASTEXEC_TEST_RUNTIME_DIR", &self.runtime)
            .env("FASTEXEC_NAMESPACE", &self.namespace)
            .env("FASTEXEC_HTTP_TOKEN", &self.token)
            // Nothing this process carries may change what a case sets up itself.
            .env_remove("FASTEXEC_DAEMON")
            .env_remove("FASTEXEC_HTTP_ALLOW_REMOTE")
            .env_remove("FASTEXEC_HTTP_ALLOWED_ORIGINS")
            .env_remove("FASTEXEC_OUTPUT_MODE")
            .env_remove("FASTEXEC_STRUCTURED_CONTENT")
            .env_remove("FASTEXEC_TEST_TOTAL_LOG_LIMIT")
            .env_remove("FASTEXEC_TEST_NONE_LIMIT")
            .stdin(std::process::Stdio::null());
        command
    }

    fn run(&self, args: &[&str]) -> (i32, String, String) {
        let output = self.command(args).output().unwrap();
        let out = String::from_utf8_lossy(&output.stdout).into_owned();
        if let Some(pid) = pid_of(&out) {
            self.pid.set(Some(pid));
        }
        (
            output.status.code().unwrap_or(-1),
            out,
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )
    }

    /// Whether `pid` is this fixture's daemon: the cached image under this fixture's own runtime
    /// directory. A number alone is not ownership.
    fn owns_process(&self, pid: u32) -> bool {
        let expected = self.runtime.join("bin");
        #[cfg(windows)]
        {
            let script = format!(
                "$p = Get-CimInstance Win32_Process -Filter \"ProcessId={pid}\"; if ($p) {{ $p.ExecutablePath }}"
            );
            let Ok(output) = Command::new("pwsh")
                .args(["-NoProfile", "-NonInteractive", "-Command", &script])
                .output()
            else {
                return false;
            };
            let path = String::from_utf8_lossy(&output.stdout)
                .trim()
                .to_lowercase();
            !path.is_empty() && path.starts_with(&expected.display().to_string().to_lowercase())
        }
        #[cfg(unix)]
        {
            std::fs::read_link(format!("/proc/{pid}/exe"))
                .map(|path| path.starts_with(&expected))
                .unwrap_or(false)
        }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Whatever the test did, its daemon goes. Nothing here goes through `run`, which unwraps a
        // process spawn: this can be the cleanup of a panic, and a second panic aborts the harness.
        let (stopped, why) = match self.command(&["daemon", "stop"]).output() {
            Ok(output) => {
                let out = String::from_utf8_lossy(&output.stdout).into_owned();
                let err = String::from_utf8_lossy(&output.stderr).into_owned();
                let stopped = output.status.code() == Some(0)
                    && (out.contains("Stopped") || out.starts_with("No fastexec daemon"));
                (stopped, format!("stop {out:?}{err:?}"))
            }
            // A CLI that cannot start says nothing about the daemon: what is left is unknown, not
            // stopped.
            Err(error) => (false, format!("stop could not run: {error}")),
        };
        let pid = if stopped { None } else { self.pid.get() };
        let killed = pid.is_some_and(|pid| self.owns_process(pid) && end_process(pid));
        let gone = stopped || killed;
        let _ = std::fs::remove_dir_all(&self.runtime);
        // A test that passes must not report success with its daemon still running. During a panic
        // this stays best effort, so the first panic is the one that is reported.
        if !gone && !std::thread::panicking() {
            panic!("the fixture could not stop its daemon (pid {pid:?}): {why}");
        }
    }
}

/// Ends one process by PID.
fn end_process(pid: u32) -> bool {
    let pid = pid.to_string();
    #[cfg(windows)]
    let killed = Command::new("taskkill").args(["/F", "/PID", &pid]).output();
    #[cfg(unix)]
    let killed = Command::new("kill").args(["-9", &pid]).output();
    matches!(killed, Ok(output) if output.status.success())
}

fn pid_of(text: &str) -> Option<u32> {
    let words: Vec<&str> = text.split_whitespace().collect();
    let at = words.iter().position(|word| *word == "pid")?;
    words.get(at + 1)?.parse().ok()
}

/// What came back: the status line, the headers, and the body with any chunking undone.
struct Response {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl Response {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// The JSON-RPC messages the answer carried, in the order they arrived.
    fn messages(&self) -> Vec<Value> {
        self.body
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .filter_map(|data| serde_json::from_str(data.trim()).ok())
            .collect()
    }

    /// The one answer to `id`.
    fn answer(&self, id: u64) -> Value {
        self.messages()
            .into_iter()
            .find(|message| message["id"] == json!(id))
            .unwrap_or_else(|| panic!("no answer to {id} in {:?}", self.body))
    }
}

/// Sends `request` and reads the whole answer, which `Connection: close` ends.
fn exchange(addr: SocketAddr, request: &[u8]) -> Response {
    let mut stream = TcpStream::connect(addr).expect("the daemon accepts a connection");
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    stream.write_all(request).unwrap();
    let mut bytes = Vec::new();
    // A read that fails or times out is a truncated answer, not a shorter one.
    stream
        .read_to_end(&mut bytes)
        .unwrap_or_else(|error| panic!("the answer ended early: {error}"));
    parse(&bytes)
}

fn parse(bytes: &[u8]) -> Response {
    let split = find(bytes, b"\r\n\r\n").unwrap_or_else(|| {
        panic!(
            "no header block in {:?}",
            String::from_utf8_lossy(&bytes[..bytes.len().min(400)])
        )
    });
    let head = String::from_utf8_lossy(&bytes[..split]).into_owned();
    let body = &bytes[split + 4..];
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status line in {head:?}"));
    let headers: Vec<(String, String)> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.trim().to_owned(), value.trim().to_owned()))
        .collect();
    let chunked = headers.iter().any(|(key, value)| {
        key.eq_ignore_ascii_case("transfer-encoding") && value.contains("chunked")
    });
    let body = if chunked {
        dechunk(body)
    } else {
        // A body that declares its length is that length, or the answer is incomplete.
        if let Some(length) = headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, value)| value.parse::<usize>().ok())
        {
            assert_eq!(
                body.len(),
                length,
                "the body is not the {length} bytes it declared"
            );
        }
        body.to_vec()
    };
    Response {
        status,
        headers,
        body: String::from_utf8_lossy(&body).into_owned(),
    }
}

/// The first place `needle` occurs in `haystack`.
fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Undoes chunked transfer coding, which is how an event stream arrives. A body that stops before
/// its terminal chunk is an incomplete answer, not a short one.
fn dechunk(body: &[u8]) -> Vec<u8> {
    let mut rest = body;
    let mut out = Vec::new();
    loop {
        let split = find(rest, b"\r\n").unwrap_or_else(|| {
            panic!(
                "a chunked body has no size line: {:?}",
                String::from_utf8_lossy(rest)
            )
        });
        let size = std::str::from_utf8(&rest[..split])
            .ok()
            .and_then(|size| usize::from_str_radix(size.trim(), 16).ok())
            .unwrap_or_else(|| {
                panic!(
                    "a chunk size {:?} is not hexadecimal",
                    String::from_utf8_lossy(&rest[..split])
                )
            });
        let tail = &rest[split + 2..];
        if size == 0 {
            // The last chunk is followed by an empty line, which ends the body.
            assert!(
                tail.starts_with(b"\r\n"),
                "the last chunk has no terminal line: {:?}",
                String::from_utf8_lossy(tail)
            );
            return out;
        }
        assert!(
            tail.len() >= size + 2,
            "a chunk of {size} bytes is cut short: {:?}",
            String::from_utf8_lossy(tail)
        );
        out.extend_from_slice(&tail[..size]);
        // Every chunk's data is followed by a line end of its own.
        assert!(
            tail[size..].starts_with(b"\r\n"),
            "a chunk of {size} bytes has no terminating line: {:?}",
            String::from_utf8_lossy(&tail[size..])
        );
        rest = &tail[size + 2..];
    }
}

/// One MCP session over HTTP: the session ID the daemon gave, and the next request ID.
struct Client {
    addr: SocketAddr,
    token: String,
    session: Option<String>,
    next_id: u64,
}

impl Client {
    fn new(addr: SocketAddr, token: String) -> Client {
        Client {
            addr,
            token,
            session: None,
            next_id: 0,
        }
    }

    /// Sends `body` as one POST to `/mcp` and returns the whole answer.
    fn post(&mut self, body: &str) -> Response {
        let mut request = String::new();
        request.push_str("POST /mcp HTTP/1.1\r\n");
        request.push_str(&format!("Host: {}\r\n", self.addr));
        request.push_str("Accept: application/json, text/event-stream\r\n");
        request.push_str("Content-Type: application/json\r\n");
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
        request.push_str("Connection: close\r\n");
        request.push_str(&format!("Authorization: Bearer {}\r\n", self.token));
        if let Some(session) = &self.session {
            request.push_str(&format!("Mcp-Session-Id: {session}\r\n"));
        }
        request.push_str("\r\n");
        request.push_str(body);
        exchange(self.addr, request.as_bytes())
    }

    fn send(&mut self, message: Value) -> Response {
        self.post(&message.to_string())
    }

    /// The MCP handshake, which names the session every later request carries.
    fn initialize(&mut self) {
        self.next_id += 1;
        let id = self.next_id;
        let response = self.send(json!({
            "jsonrpc": "2.0", "id": id, "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "http-test", "version": "0"},
            },
        }));
        assert_eq!(response.status, 200, "{:?}", response.body);
        self.session = Some(
            response
                .header("mcp-session-id")
                .unwrap_or_else(|| panic!("no session id in {:?}", response.headers))
                .to_owned(),
        );
        assert!(
            response.answer(id)["result"]["serverInfo"]["name"] == json!("fastexec"),
            "{:?}",
            response.body
        );
        let notified = self.send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        assert!(
            notified.status == 200 || notified.status == 202,
            "{}",
            notified.body
        );
    }

    /// The tools the server offers, by name.
    fn tools(&mut self) -> Vec<String> {
        self.next_id += 1;
        let id = self.next_id;
        let response = self.send(json!({"jsonrpc": "2.0", "id": id, "method": "tools/list"}));
        assert_eq!(response.status, 200, "{:?}", response.body);
        response.answer(id)["result"]["tools"]
            .as_array()
            .unwrap_or_else(|| panic!("no tools in {:?}", response.body))
            .iter()
            .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
            .collect()
    }

    /// One `tools/call`, and the tool's own result.
    fn call(&mut self, arguments: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        let response = self.send(json!({
            "jsonrpc": "2.0", "id": id, "method": "tools/call",
            "params": {"name": "fastexec", "arguments": arguments},
        }));
        assert_eq!(response.status, 200, "{:?}", response.body);
        response.answer(id)
    }

    /// The text a tool result carries, and its structured content.
    fn text(&mut self, arguments: Value) -> String {
        self.result(arguments).1
    }

    /// The structured content a tool result carries, and its text.
    fn result(&mut self, arguments: Value) -> (Value, String) {
        let answer = self.call(arguments);
        assert_ne!(answer["result"]["isError"], json!(true), "{answer}");
        let text = answer["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or_else(|| panic!("no text in {answer}"))
            .to_owned();
        (answer["result"]["structuredContent"].clone(), text)
    }
}

/// Sends one request with headers of the test's own choosing.
fn raw(addr: SocketAddr, token: &str, headers: &str, body: &str) -> Response {
    let mut request = String::new();
    request.push_str("POST /mcp HTTP/1.1\r\n");
    request.push_str(&format!("Host: {addr}\r\n"));
    request.push_str("Accept: application/json, text/event-stream\r\n");
    request.push_str("Content-Type: application/json\r\n");
    request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    request.push_str("Connection: close\r\n");
    if !token.is_empty() {
        request.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    request.push_str(headers);
    request.push_str("\r\n");
    request.push_str(body);
    exchange(addr, request.as_bytes())
}

/// One MCP endpoint in `on` mode over stdio, driven the way a host drives it.
struct Stdio {
    child: std::process::Child,
    stdin: std::process::ChildStdin,
    /// What the endpoint said, read by a thread of its own so a bridge that stops answering fails
    /// the test instead of holding it.
    lines: std::sync::mpsc::Receiver<Value>,
    next_id: u64,
}

impl Stdio {
    /// Starts an endpoint on `fixture`'s namespace, so it meets the daemon the fixture started.
    fn start(fixture: &Fixture) -> Stdio {
        let mut child = fixture
            .command(&[])
            .env("FASTEXEC_DAEMON", "on")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (sender, lines) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout)
                .lines()
                .map_while(Result::ok)
            {
                if let Ok(message) = serde_json::from_str(&line) {
                    let _ = sender.send(message);
                }
            }
        });
        let mut stdio = Stdio {
            child,
            stdin,
            lines,
            next_id: 0,
        };
        let answer = stdio.request(
            "initialize",
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "stdio-test", "version": "0"},
            }),
        );
        assert_eq!(
            answer["result"]["serverInfo"]["name"],
            json!("fastexec"),
            "{answer}"
        );
        stdio.notify(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        stdio
    }

    fn notify(&mut self, message: Value) {
        writeln!(self.stdin, "{message}").unwrap();
        self.stdin.flush().unwrap();
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        self.notify(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        loop {
            let message = self
                .lines
                .recv_timeout(Duration::from_secs(120))
                .unwrap_or_else(|error| panic!("the endpoint did not answer {method}: {error}"));
            if message["id"] == json!(id) {
                return message;
            }
        }
    }

    /// What the endpoint's own session sees, and its structured content.
    fn list(&mut self) -> (String, Value) {
        let answer = self.request(
            "tools/call",
            json!({"name": "fastexec", "arguments": {"action": "list"}}),
        );
        let result = &answer["result"];
        assert_ne!(result["isError"], json!(true), "{answer}");
        (
            result["content"][0]["text"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            result["structuredContent"].clone(),
        )
    }
}

impl Drop for Stdio {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[test]
fn a_session_runs_a_task_over_http() {
    let fixture = Fixture::new("run");
    let mut client = Client::new(fixture.addr, fixture.token.clone());
    client.initialize();
    let tools = client.tools();
    assert!(tools.iter().any(|name| name == "fastexec"), "{tools:?}");
    // The program reads a line and prints a marker, so the marker can only appear once this poll
    // delivered the input: a start that had already finished could not produce it. It also says
    // whether the token reached it, which it must not have.
    let (structured, started) = client.result(json!({
        "action": "start",
        "command": "read -r line; printf 'GOT %s\\n' \"$line\"; printf 'TOKEN=%s\\n' \"${FASTEXEC_HTTP_TOKEN:+SET}\"",
        "waitMs": 0,
    }));
    let id = structured["taskId"]
        .as_str()
        .unwrap_or_else(|| panic!("no task id in {started}"))
        .to_owned();
    let polled = client.text(json!({
        "action": "poll", "taskId": id, "input": "http-ok\n", "waitMs": 30000,
    }));
    assert!(polled.contains("GOT http-ok"), "{polled}");
    assert!(
        !polled.contains("TOKEN=SET"),
        "the token does not reach the tasks: {polled}"
    );
    assert!(
        polled.contains("TOKEN=\n") || polled.contains("TOKEN="),
        "{polled}"
    );
}

#[test]
fn a_stdio_endpoint_lists_the_task_an_http_session_started() {
    let fixture = Fixture::new("shared");
    let mut client = Client::new(fixture.addr, fixture.token.clone());
    client.initialize();
    let (structured, started) = client.result(json!({
        "action": "start", "command": "sleep 60", "waitMs": 0,
    }));
    let id = structured["taskId"]
        .as_str()
        .unwrap_or_else(|| panic!("no task id in {started}"))
        .to_owned();
    // An endpoint of its own, in `on` mode over the same namespace, sees that task and names the
    // HTTP session as its owner.
    let mut stdio = Stdio::start(&fixture);
    let (text, structured) = stdio.list();
    let tasks = structured["tasks"].as_array().cloned().unwrap_or_default();
    let task = tasks
        .iter()
        .find(|task| task["taskId"] == json!(id))
        .unwrap_or_else(|| panic!("the HTTP task is not listed: {text}"));
    assert!(
        task["owner"]
            .as_str()
            .unwrap_or_default()
            .starts_with("http-test-"),
        "the owner names the HTTP client: {task}"
    );
}

#[test]
fn a_missing_or_wrong_token_is_refused_and_no_session_is_returned() {
    let fixture = Fixture::new("token");
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
        "protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "x", "version": "0"},
    }})
    .to_string();
    let missing = raw(fixture.addr, "", "", &body);
    assert_eq!(missing.status, 401, "{:?}", missing.body);
    assert_eq!(missing.header("www-authenticate"), Some("Bearer"));
    assert!(
        missing.header("mcp-session-id").is_none(),
        "{:?}",
        missing.headers
    );
    let wrong = raw(
        fixture.addr,
        "",
        "Authorization: Bearer 0123456789abcdeX\r\n",
        &body,
    );
    assert_eq!(wrong.status, 401, "{:?}", wrong.body);
    assert!(
        wrong.header("mcp-session-id").is_none(),
        "{:?}",
        wrong.headers
    );
}

#[test]
fn a_declared_body_over_the_cap_is_refused_before_it_is_read() {
    let fixture = Fixture::new("large");
    let request = format!(
        "POST /mcp HTTP/1.1\r\nHost: {}\r\nAccept: application/json, text/event-stream\r\nContent-Type: application/json\r\nContent-Length: 5000000\r\nConnection: close\r\nAuthorization: Bearer {}\r\n\r\n",
        fixture.addr, fixture.token
    );
    let response = exchange(fixture.addr, request.as_bytes());
    assert_eq!(response.status, 413, "{:?}", response.body);
    // A body that declares no length is capped as it arrives, and the request fails instead of
    // being read to the end. The body is a valid `initialize` padded with whitespace past the cap,
    // so only the cap itself can make this request fail.
    let mut body = json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
        "protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "x", "version": "0"},
    }})
    .to_string();
    body.push_str(&" ".repeat((4 << 20) + 1 - body.len()));
    let mut stream = TcpStream::connect(fixture.addr).expect("the daemon accepts a connection");
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    // A server that stops reading must fail this test, not hold it in a write.
    stream
        .set_write_timeout(Some(Duration::from_secs(60)))
        .unwrap();
    let head = format!(
        "POST /mcp HTTP/1.1\r\nHost: {}\r\nAccept: application/json, text/event-stream\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\nAuthorization: Bearer {}\r\n\r\n",
        fixture.addr, fixture.token
    );
    let _ = stream.write_all(head.as_bytes());
    let bytes = body.as_bytes();
    let mut sent = 0;
    while sent < bytes.len() {
        let size = (bytes.len() - sent).min(64 * 1024);
        if write!(stream, "{size:x}\r\n").is_err()
            || stream.write_all(&bytes[sent..sent + size]).is_err()
        {
            break;
        }
        let _ = stream.write_all(b"\r\n");
        sent += size;
    }
    let _ = stream.write_all(b"0\r\n\r\n");
    let mut bytes = Vec::new();
    let _ = stream.read_to_end(&mut bytes);
    let response = parse(&bytes);
    // rmcp reads the body it was given and answers the read failure: a 401 or a truncated transfer
    // would not prove the cap did anything.
    assert_eq!(
        response.status, 500,
        "a body over the cap fails the read: {} {:?}",
        response.status, response.body
    );
    assert!(
        response.body.contains("Failed to read request body"),
        "the answer names the read: {:?}",
        response.body
    );
    assert!(
        response.body.contains("length limit exceeded"),
        "the answer names the cap: {:?}",
        response.body
    );
    assert!(
        response.header("mcp-session-id").is_none(),
        "the refused body started no session: {:?}",
        response.headers
    );
}

#[test]
fn a_browser_origin_from_elsewhere_is_refused_and_none_is_accepted() {
    let fixture = Fixture::new("origin");
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
        "protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "x", "version": "0"},
    }})
    .to_string();
    let evil = raw(
        fixture.addr,
        &fixture.token,
        "Origin: http://evil.example\r\n",
        &body,
    );
    assert!(
        evil.status == 403 || evil.status == 400,
        "the origin was refused: {} {:?}",
        evil.status,
        evil.body
    );
    assert!(
        evil.header("mcp-session-id").is_none(),
        "{:?}",
        evil.headers
    );
    let plain = raw(fixture.addr, &fixture.token, "", &body);
    assert_eq!(plain.status, 200, "{:?}", plain.body);
    assert!(
        plain.header("mcp-session-id").is_some(),
        "{:?}",
        plain.headers
    );
}

#[test]
fn a_start_outside_loopback_or_without_a_long_enough_token_is_refused() {
    let fixture = Fixture::new("refuse");
    let (code, out, err) = fixture.run(&["daemon", "stop"]);
    assert_eq!(code, 0, "{out}{err}");
    let (code, _, err) = fixture.run(&["daemon", "start", "--http", "0.0.0.0:0"]);
    assert_eq!(code, 1);
    assert!(err.contains("is not a loopback address"), "{err}");
    // The daemon never started, so nothing is listening on that address.
    let (code, out, _) = fixture.run(&["daemon", "status"]);
    assert_eq!(code, 0);
    assert!(out.contains("No fastexec daemon"), "{out}");
    let refused = |token: &str| {
        let output = fixture
            .command(&["daemon", "start", "--http", "127.0.0.1:0"])
            .env("FASTEXEC_HTTP_TOKEN", token)
            .output()
            .unwrap();
        (
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        )
    };
    let (code, err) = refused("short");
    assert_eq!(code, 1);
    assert!(
        err.contains("must be at least 16 visible ASCII characters"),
        "{err}"
    );
    // A token an Authorization header cannot carry exactly is refused as well.
    let (code, err) = refused("0123456789abcdef\u{e9}\u{e9}");
    assert_eq!(code, 1);
    assert!(
        err.contains("must be at least 16 visible ASCII characters"),
        "{err}"
    );
    let (code, err) = refused("0123456789 abcdef");
    assert_eq!(code, 1);
    assert!(
        err.contains("must be at least 16 visible ASCII characters"),
        "{err}"
    );
}

#[test]
fn status_reports_the_address_the_daemon_serves_on() {
    let fixture = Fixture::new("status");
    let (code, out, err) = fixture.run(&["daemon", "status"]);
    assert_eq!(code, 0, "{out}{err}");
    assert!(
        out.contains(&format!("http 127.0.0.1:{}", fixture.addr.port())),
        "{out}"
    );
    assert!(Path::new(&fixture.runtime).exists());
}
