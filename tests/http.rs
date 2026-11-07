//! The daemon's Streamable HTTP front door, driven over a socket the way a client that speaks
//! MCP over HTTP does: one request per connection, and an answer read back as it arrives.

#[cfg(any(windows, target_os = "freebsd"))]
mod support;

use serde_json::{Value, json};
use std::io::{BufRead, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

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
    /// The tokens this fixture's daemon admits, made for this run alone.
    tokens: Vec<String>,
    /// The daemon this fixture saw running, so cleanup can end that one process by PID.
    pid: std::cell::Cell<Option<u32>>,
    /// The public root this fixture's daemon is started with, when a case names one. It is set on
    /// this fixture's own commands only: no case changes the environment of the test process.
    public_url: Option<String>,
    /// An explicitly chosen launch directory for path-alias regression tests.
    cwd: Option<PathBuf>,
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
        let mut fixture = Fixture::bare(tag);
        fixture.start();
        fixture
    }

    /// Starts a daemon whose transfer URLs are minted from `root`, the way a proxy in front of the
    /// listener has it.
    fn with_public_url(mut self, root: &str) -> Fixture {
        self.public_url = Some(root.to_owned());
        self
    }

    /// A fixture with its runtime directory, and no daemon yet: the shape of a start that is
    /// expected to be refused.
    fn bare(tag: &str) -> Fixture {
        #[cfg(target_os = "freebsd")]
        support::freebsd::init();
        let runtime =
            std::env::temp_dir().join(format!("fastexec-http-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&runtime);
        #[cfg(unix)]
        {
            let mut builder = std::fs::DirBuilder::new();
            builder.recursive(true);
            std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
            builder.create(&runtime).unwrap();
        }
        #[cfg(windows)]
        support::create_private_runtime(&runtime);
        // The turn is taken before the fixture exists, so a start that panics still cleans up with
        // it held.
        let starting = STARTING.lock().unwrap_or_else(|error| error.into_inner());
        Fixture {
            runtime,
            namespace: format!("http{tag}-{}", std::process::id()),
            addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            tokens: vec![random_token()],
            pid: std::cell::Cell::new(None),
            public_url: None,
            cwd: None,
            starting,
        }
    }

    /// Starts the daemon, and reads back the address it bound.
    fn start(&mut self) {
        let (code, out, err) = self.run(&["daemon", "start", "--http", "127.0.0.1:0"]);
        assert_eq!(code, 0, "the daemon started: {out}{err}");
        let addr = out
            .split_whitespace()
            .skip_while(|word| *word != "http")
            .nth(1)
            .unwrap_or_else(|| panic!("no http address in {out:?}"));
        self.addr = addr.parse().expect("the daemon printed an address");
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        command
            .args(args)
            .env("FASTEXEC_TEST_RUNTIME_DIR", &self.runtime)
            .env("FASTEXEC_NAMESPACE", &self.namespace)
            .env("FASTEXEC_HTTP_TOKEN", self.tokens.join(" "))
            // Nothing this process carries may change what a case sets up itself.
            .env_remove("FASTEXEC_DAEMON")
            .env_remove("FASTEXEC_HTTP_ALLOW_REMOTE")
            .env_remove("FASTEXEC_HTTP_ALLOWED_ORIGINS")
            .env_remove("FASTEXEC_OUTPUT_MODE")
            .env_remove("FASTEXEC_STRUCTURED_CONTENT")
            .env_remove("FASTEXEC_TEST_TOTAL_LOG_LIMIT")
            .env_remove("FASTEXEC_TEST_NONE_LIMIT")
            .stdin(std::process::Stdio::null());
        if let Some(cwd) = &self.cwd {
            command.current_dir(cwd);
        }
        match &self.public_url {
            Some(root) => command.env("FASTEXEC_PUBLIC_URL", root),
            None => command.env_remove("FASTEXEC_PUBLIC_URL"),
        };
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
        #[cfg(target_os = "macos")]
        {
            // macOS has no /proc: libproc names the image, and $TMPDIR sits under /var, which the
            // kernel reports as /private/var.
            let mut buffer = vec![0u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
            // SAFETY: the buffer is as large as proc_pidpath writes, for this pid.
            let len = unsafe {
                libc::proc_pidpath(pid as i32, buffer.as_mut_ptr().cast(), buffer.len() as u32)
            };
            if len <= 0 {
                return false;
            }
            buffer.truncate(len as usize);
            let path = std::path::PathBuf::from(String::from_utf8_lossy(&buffer).into_owned());
            let resolved = |path: &std::path::Path| {
                std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
            };
            resolved(&path).starts_with(resolved(&expected))
        }
        #[cfg(target_os = "freebsd")]
        {
            support::freebsd::owns_process(pid, &expected)
        }
        #[cfg(all(unix, not(any(target_os = "macos", target_os = "freebsd"))))]
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
        #[cfg(target_os = "freebsd")]
        if stopped && let Some(pid) = self.pid.get() {
            let _ = support::freebsd::reap_stopped(pid);
        }
        let pid = if stopped { None } else { self.pid.get() };
        #[cfg(target_os = "freebsd")]
        let killed = pid
            .is_some_and(|pid| support::freebsd::end_owned_process(pid, &self.runtime.join("bin")));
        #[cfg(not(target_os = "freebsd"))]
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

/// Ends one process by PID after the caller checks its image.
#[cfg(not(target_os = "freebsd"))]
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
    /// The body as it arrived, for the bytes of a file.
    bytes: Vec<u8>,
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
    exchange_body(addr, request, true)
}

/// Sends `request` and reads the answer, which carries no body when `body` is false: a HEAD
/// answer declares the length of the body it would have sent and sends none of it.
fn exchange_body(addr: SocketAddr, request: &[u8], body: bool) -> Response {
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
    parse_body(&bytes, body)
}

fn parse(bytes: &[u8]) -> Response {
    parse_body(bytes, true)
}

fn parse_body(bytes: &[u8], expect_body: bool) -> Response {
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
        if expect_body
            && let Some(length) = headers
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
        bytes: body,
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
        self.post_with(body, "")
    }

    /// Sends `body` as one POST to `/mcp` with headers of the test's own choosing.
    fn post_with(&mut self, body: &str, headers: &str) -> Response {
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
        request.push_str(headers);
        request.push_str("\r\n");
        request.push_str(body);
        exchange(self.addr, request.as_bytes())
    }

    fn send(&mut self, message: Value) -> Response {
        self.post(&message.to_string())
    }

    /// A 2026 request carries its own metadata and never uses a protocol session.
    fn modern(&mut self, method: &str, params: Value) -> Value {
        self.inline("2026-07-28", method, params)
    }

    /// Inline lifecycle metadata selects a principal session even with an older version.
    fn inline(&mut self, version: &str, method: &str, mut params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        params["_meta"] = json!({
            "io.modelcontextprotocol/protocolVersion": version,
            "io.modelcontextprotocol/clientInfo": {"name": "http-modern-test", "version": "1"},
            "io.modelcontextprotocol/clientCapabilities": {},
        });
        let mut headers = format!("MCP-Protocol-Version: {version}\r\nMcp-Method: {method}\r\n");
        if let Some(name) = params["name"].as_str() {
            headers.push_str(&format!("Mcp-Name: {name}\r\n"));
        }
        let response = self.post_with(
            &json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}).to_string(),
            &headers,
        );
        assert_eq!(response.status, 200, "{}", response.body);
        assert!(response.header("mcp-session-id").is_none());
        let answer = response.answer(id);
        assert!(answer["result"].is_object(), "{answer}");
        if version == "2026-07-28" {
            assert_eq!(answer["result"]["resultType"], "complete", "{answer}");
        }
        answer["result"].clone()
    }

    /// A sessionless exec result, its structured content and its text.
    fn modern_result(&mut self, arguments: Value) -> (Value, String) {
        let result = self.modern(
            "tools/call",
            json!({"name": "exec", "arguments": arguments}),
        );
        assert_ne!(result["isError"], true, "{result}");
        let text = result["content"][0]["text"].as_str().unwrap().to_owned();
        (result["structuredContent"].clone(), text)
    }

    /// The MCP handshake, which names the session every later request carries.
    fn initialize(&mut self) {
        self.initialize_version("2025-06-18");
    }

    fn initialize_version(&mut self, version: &str) {
        self.next_id += 1;
        let id = self.next_id;
        let response = self.send(json!({
            "jsonrpc": "2.0", "id": id, "method": "initialize",
            "params": {
                "protocolVersion": version,
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

    /// One `tools/call` of any tool, and its own result.
    fn call_tool(&mut self, tool: &str, arguments: Value) -> Value {
        self.call_tool_with(tool, arguments, "")
    }

    /// One `tools/call` with headers of the test's own choosing.
    fn call_tool_with(&mut self, tool: &str, arguments: Value, headers: &str) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        let body = json!({
            "jsonrpc": "2.0", "id": id, "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        })
        .to_string();
        let response = self.post_with(&body, headers);
        assert_eq!(response.status, 200, "{:?}", response.body);
        let answer = response.answer(id);
        assert!(answer["result"].is_object(), "protocol error: {answer}");
        answer["result"].clone()
    }

    /// One `tools/call`, and the tool's own result.
    fn call(&mut self, arguments: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        let response = self.send(json!({
            "jsonrpc": "2.0", "id": id, "method": "tools/call",
            "params": {"name": "exec", "arguments": arguments},
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

/// Sends one request with a method, a target and headers of the test's own choosing.
/// Sends one request with a method, a target and headers of the test's own choosing.
fn send(addr: SocketAddr, method: &str, target: &str, headers: &str, body: &[u8]) -> Response {
    let mut request = Vec::new();
    request.extend_from_slice(format!("{method} {target} HTTP/1.1\r\n").as_bytes());
    request.extend_from_slice(format!("Host: {addr}\r\n").as_bytes());
    request.extend_from_slice(format!("Content-Length: {}\r\n", body.len()).as_bytes());
    request.extend_from_slice(b"Connection: close\r\n");
    request.extend_from_slice(headers.as_bytes());
    request.extend_from_slice(b"\r\n");
    request.extend_from_slice(body);
    exchange_body(addr, &request, method != "HEAD")
}
/// Sends one request with headers of the test's own choosing.
fn raw(addr: SocketAddr, token: &str, headers: &str, body: &str) -> Response {
    post_to(addr, token, "/mcp", headers, body)
}

/// Sends one authenticated POST to `target`, for the routes a client may and may not use.
fn post_to(addr: SocketAddr, token: &str, target: &str, headers: &str, body: &str) -> Response {
    let mut request = String::new();
    request.push_str(&format!("POST {target} HTTP/1.1\r\n"));
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
            json!({"name": "exec", "arguments": {"action": "list"}}),
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
fn modern_discovery_and_task_polls_work_without_a_handshake() {
    let fixture = Fixture::new("modern");
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    let discovery = client.modern("server/discover", json!({}));
    assert!(
        discovery["supportedVersions"]
            .as_array()
            .unwrap()
            .contains(&json!("2026-07-28"))
    );
    assert_eq!(
        discovery["_meta"]["io.modelcontextprotocol/serverInfo"]["name"],
        "fastexec"
    );
    let listed = client.modern("tools/list", json!({}));
    assert_eq!(listed["tools"].as_array().unwrap().len(), 5);
    let started = client.modern(
        "tools/call",
        json!({"name": "exec", "arguments": {
            "action": "start", "command": "printf MODERN_FIRST; read -r line; printf MODERN_SECOND",
            "waitMs": 30000, "returnWhen": {"outputContains": ["MODERN_FIRST"]},
        }}),
    );
    assert_ne!(started["isError"], true, "{started}");
    assert!(
        started["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("MODERN_FIRST")
    );
    let id = &started["structuredContent"]["taskId"];
    let polled = client.modern(
        "tools/call",
        json!({"name": "exec", "arguments": {
            "action": "poll", "taskId": id, "input": "go\n", "waitMs": 30000,
        }}),
    );
    let text = polled["content"][0]["text"].as_str().unwrap();
    assert!(text.contains("MODERN_SECOND"), "{polled}");
    assert!(
        !text.contains("MODERN_FIRST"),
        "a new HTTP request must keep its output cursor: {polled}"
    );
    // A legacy client still has an independent cursor over the shared daemon's task.
    let mut legacy = Client::new(fixture.addr, fixture.tokens[0].clone());
    legacy.initialize();
    let text = legacy.text(json!({"action": "poll", "taskId": id, "waitMs": 1000}));
    assert!(
        text.contains("MODERN_FIRST") && text.contains("MODERN_SECOND"),
        "{text}"
    );
}

#[test]
fn sessionless_tokens_scope_notices_and_handles_keep_independent_cursors() {
    let mut fixture = Fixture::bare("principals");
    fixture.tokens.push(random_token());
    fixture.start();
    let mut a = Client::new(fixture.addr, fixture.tokens[0].clone());
    let mut b = Client::new(fixture.addr, fixture.tokens[0].clone());
    let mut other = Client::new(fixture.addr, fixture.tokens[1].clone());

    let (started, text) = a.modern_result(json!({
        "action": "start",
        "command": "printf 'A_FIRST\\n'; read -r line; printf 'A_SECOND\\n'; read -r line",
        "waitMs": 30000, "returnWhen": {"outputContains": ["A_FIRST"]},
    }));
    let a_handle = started["taskId"].as_str().unwrap();
    let a_id = a_handle.split_once('.').unwrap().0;
    assert!(
        text.contains(a_handle) && text.contains("A_FIRST"),
        "{text}"
    );
    assert!(!text.contains("new reader"), "{text}");

    let (other_started, text) = other.modern_result(json!({
        "action": "start", "command": "printf 'OTHER_FIRST\\n'; read -r line",
        "waitMs": 30000, "returnWhen": {"outputContains": ["OTHER_FIRST"]},
    }));
    let other_handle = other_started["taskId"].as_str().unwrap();
    let other_id = other_handle.split_once('.').unwrap().0;
    assert!(text.contains("1 other running"), "{text}");
    assert!(
        !text.contains(a_id),
        "another principal has counts only: {text}"
    );

    let (b_started, text) = b.modern_result(json!({
        "action": "start", "command": "printf 'B_FIRST\\n'; read -r line",
        "waitMs": 30000, "returnWhen": {"outputContains": ["B_FIRST"]},
    }));
    let b_id = b_started["taskId"]
        .as_str()
        .unwrap()
        .split_once('.')
        .unwrap()
        .0;
    assert!(
        text.contains(a_id),
        "the same token sees its own tasks: {text}"
    );
    assert!(!text.contains(a_handle), "footer task IDs are bare: {text}");
    assert!(text.contains("1 other running"), "{text}");
    assert!(!text.contains(other_id), "{text}");

    let (attached, text) = b.modern_result(json!({
        "action": "poll", "taskId": a_id, "waitMs": 0,
    }));
    let b_handle = attached["taskId"].as_str().unwrap();
    assert_ne!(b_handle, a_handle);
    assert_eq!(b_handle.split_once('.').unwrap().0, a_id);
    assert!(
        text.contains("A_FIRST") && text.contains(" · new reader"),
        "{text}"
    );
    assert!(text.contains(b_handle), "{text}");

    let (_, text) = a.modern_result(json!({
        "action": "poll", "taskId": a_handle, "input": "go\n",
        "waitMs": 30000, "returnWhen": {"outputContains": ["A_SECOND"]},
    }));
    assert!(
        text.contains("A_SECOND") && !text.contains("A_FIRST"),
        "{text}"
    );
    assert!(!text.contains("new reader"), "{text}");

    let (_, text) = b.modern_result(json!({
        "action": "poll", "taskId": b_handle, "waitMs": 0,
    }));
    assert!(
        text.contains("A_SECOND") && !text.contains("A_FIRST"),
        "{text}"
    );
    assert!(!text.contains("new reader"), "{text}");

    let (listed, _) = other.modern_result(json!({"action": "list"}));
    let tasks = listed["tasks"].as_array().unwrap();
    assert_eq!(tasks.len(), 3, "{listed}");
    for (id, owner) in [(a_id, "http-t1"), (b_id, "http-t1"), (other_id, "http-t2")] {
        let task = tasks.iter().find(|task| task["taskId"] == id).unwrap();
        assert_eq!(task["owner"], owner, "{task}");
        assert!(!task["taskId"].as_str().unwrap().contains('.'), "{task}");
    }
    // Listing a tenant's task creates no entry and cannot hide its repeated count.
    for _ in 0..2 {
        let (_, text) = other.modern_result(json!({
            "action": "poll", "taskId": other_handle, "waitMs": 0,
        }));
        assert!(text.contains("2 others running"), "{text}");
        assert!(!text.contains(a_id) && !text.contains(b_id), "{text}");
    }
}

#[test]
fn sessionless_exit_reporting_is_local_to_each_reader() {
    let fixture = Fixture::new("reader-exit");
    let mut a = Client::new(fixture.addr, fixture.tokens[0].clone());
    let mut b = Client::new(fixture.addr, fixture.tokens[0].clone());
    let (started, text) = a.modern_result(json!({
        "action": "start", "command": "sleep 60 & echo ROOT_EXIT",
        "keepTree": true, "loginShell": false, "waitMs": 30000,
    }));
    assert_eq!(started["state"], "exited", "{text}");
    assert_eq!(started["keptRunning"], true, "{text}");
    assert_eq!(started["waitEndedBy"]["condition"], "task_ended", "{text}");
    let a_handle = started["taskId"].as_str().unwrap();
    let id = a_handle.split_once('.').unwrap().0;

    let (waited, text) = a.modern_result(json!({
        "action": "poll", "taskId": a_handle, "waitMs": 150,
    }));
    assert_eq!(waited["keptRunning"], true, "{text}");
    assert_eq!(waited["waitEndedBy"]["condition"], "max_wait", "{text}");

    let (attached, text) = b.modern_result(json!({
        "action": "poll", "taskId": id, "waitMs": 1000,
    }));
    assert_eq!(attached["keptRunning"], true, "{text}");
    assert_eq!(attached["waitEndedBy"]["condition"], "task_ended", "{text}");
    assert!(
        text.contains("ROOT_EXIT") && text.contains(" · new reader"),
        "{text}"
    );
    assert_ne!(attached["taskId"], started["taskId"]);

    let (killed, text) = a.modern_result(json!({"action": "kill", "taskId": a_handle}));
    assert_eq!(killed["keptRunning"], false, "{text}");
    assert_eq!(killed["taskId"], a_handle, "{text}");
}

#[test]
fn invalid_http_requests_create_no_reader_and_change_no_task_state() {
    let fixture = Fixture::new("invalid-reader");
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    for pty in [false, true] {
        let mut arguments = json!({
            "action": "start", "pty": pty, "loginShell": false,
            "command": "printf 'READY\\n'; while IFS= read -r line; do printf 'GOT_%s\\n' \"$line\"; done",
            "waitMs": 30000, "returnWhen": {"outputContains": ["READY"]},
        });
        if pty {
            arguments["cols"] = json!(80);
            arguments["rows"] = json!(24);
        }
        let (started, _) = client.modern_result(arguments);
        let handle = started["taskId"].as_str().unwrap();
        let id = handle.split_once('.').unwrap().0;
        // The start reader is now the oldest of 16 idle readers. Any invalid call that allocates
        // a reader would evict it; its final poll below would replay READY and say new reader.
        for _ in 0..15 {
            let (_, text) = client.modern_result(json!({
                "action": "poll", "taskId": id, "waitMs": 0,
            }));
            assert!(text.contains(" · new reader"), "{text}");
        }
        let mut invalid = vec![
            json!({"action": "poll", "taskId": format!("{id}.abc"), "input": "POISON\n"}),
            json!({"action": "kill", "taskId": format!("{id}.abcd.extra")}),
            json!({"action": "kill", "taskId": id, "input": "POISON\n"}),
            json!({"action": "poll", "taskId": id, "rows": 201, "input": "POISON\n"}),
        ];
        if pty {
            invalid.extend([
                json!({"action": "poll", "taskId": id, "keys": ["NotAKey"],
                    "cols": 100, "rows": 40, "input": "POISON\n"}),
                json!({"action": "poll", "taskId": id, "eof": true,
                    "cols": 100, "rows": 40, "input": "POISON\n"}),
            ]);
        } else {
            invalid.extend([
                json!({"action": "poll", "taskId": id, "screen": true, "input": "POISON\n"}),
                json!({"action": "poll", "taskId": id, "keys": ["Enter"], "input": "POISON\n"}),
                json!({"action": "poll", "taskId": id, "cols": 100, "input": "POISON\n"}),
                json!({"action": "poll", "taskId": id,
                    "returnWhen": {"screenContains": ["READY"]}, "input": "POISON\n"}),
                json!({"action": "transcript", "taskId": id}),
            ]);
        }
        for arguments in invalid {
            let result = client.modern(
                "tools/call",
                json!({"name": "exec", "arguments": arguments}),
            );
            assert_eq!(result["isError"], true, "pty={pty}: {result}");
        }

        let (listed, _) = client.modern_result(json!({"action": "list"}));
        let task = listed["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|task| task["taskId"] == id)
            .unwrap();
        assert_eq!(task["state"], "running", "{task}");
        if pty {
            assert_eq!(task["cols"], 80, "{task}");
            assert_eq!(task["rows"], 24, "{task}");
        }
        let (polled, text) = client.modern_result(json!({
            "action": "poll", "taskId": handle, "input": "SAFE\n",
            "waitMs": 30000, "returnWhen": {"outputContains": ["GOT_SAFE"]},
        }));
        assert_eq!(polled["taskId"], handle);
        assert!(text.contains("GOT_SAFE"), "pty={pty}: {text}");
        assert!(!text.contains("POISON"), "pty={pty}: {text}");
        assert!(
            !text.contains("READY"),
            "the reader was not evicted: pty={pty}: {text}"
        );
        assert!(!text.contains("new reader"), "pty={pty}: {text}");
        client.modern_result(json!({"action": "kill", "taskId": handle}));
    }
}

#[test]
fn inline_requests_with_an_older_version_use_the_principal_session() {
    let fixture = Fixture::new("inline-old");
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    let started = client.inline("2025-06-18", "tools/call", json!({
        "name": "exec", "arguments": {
            "action": "start", "command": "printf 'OLD_FIRST\\n'; read -r line; printf 'OLD_SECOND\\n'",
            "waitMs": 30000, "returnWhen": {"outputContains": ["OLD_FIRST"]},
        },
    }));
    assert_ne!(started["isError"], true, "{started}");
    let handle = started["structuredContent"]["taskId"].as_str().unwrap();
    let polled = client.inline(
        "2025-06-18",
        "tools/call",
        json!({
            "name": "exec", "arguments": {
                "action": "poll", "taskId": handle, "input": "go\n", "waitMs": 30000,
            },
        }),
    );
    let text = polled["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("OLD_SECOND") && !text.contains("OLD_FIRST"),
        "{polled}"
    );
    assert!(!text.contains("new reader"), "{polled}");
    assert_eq!(polled["structuredContent"]["taskId"], handle);

    let (listed, _) = client.modern_result(json!({"action": "list"}));
    let task = &listed["tasks"][0];
    assert_eq!(task["taskId"], handle.split_once('.').unwrap().0);
    assert_eq!(task["owner"], "http-t1");
}

#[test]
fn legacy_initialize_with_a_modern_version_keeps_its_client_session() {
    let fixture = Fixture::new("legacy-new");
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    client.initialize_version("2026-07-28");
    let (started, _) = client.result(json!({
        "action": "start", "command": "true", "waitMs": 30000,
    }));
    let (listed, _) = client.result(json!({"action": "list"}));
    let task = &listed["tasks"][0];
    assert_eq!(
        task["taskId"],
        started["taskId"]
            .as_str()
            .unwrap()
            .split_once('.')
            .unwrap()
            .0
    );
    assert!(
        task["owner"].as_str().unwrap().starts_with("http-test-"),
        "{task}"
    );
}

#[test]
fn minimum_budget_with_one_tenant_task_keeps_the_count_footer() {
    let mut fixture = Fixture::bare("tenant-budget");
    fixture.tokens.push(random_token());
    fixture.start();
    let mut tenant = Client::new(fixture.addr, fixture.tokens[0].clone());
    let mut caller = Client::new(fixture.addr, fixture.tokens[1].clone());
    let (started, _) = tenant.modern_result(json!({
        "action": "start", "command": "read -r line", "waitMs": 0,
    }));
    // A finished task this caller already saw is not counted.
    caller.modern_result(json!({"action": "start", "command": "true", "waitMs": 30000}));
    let (_, text) = caller.modern_result(json!({
        "action": "start", "command": "true", "waitMs": 30000, "maxBytes": 100,
    }));
    assert!(text.len() <= 100, "{text:?}");
    assert!(text.contains("(1 task; use list.)"), "{text}");
    assert!(
        !text.contains(
            started["taskId"]
                .as_str()
                .unwrap()
                .split_once('.')
                .unwrap()
                .0
        ),
        "{text}"
    );
}

#[test]
fn a_session_runs_a_task_over_http() {
    let fixture = Fixture::new("run");
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    client.initialize();
    let mut tools = client.tools();
    tools.sort_unstable();
    assert_eq!(tools, ["edit", "exec", "read", "transfer", "write"]);
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
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
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
        .find(|task| task["taskId"] == json!(id.split_once('.').unwrap().0))
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
        fixture.addr, fixture.tokens[0]
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
        fixture.addr, fixture.tokens[0]
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
        &fixture.tokens[0],
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
    let plain = raw(fixture.addr, &fixture.tokens[0], "", &body);
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
fn file_tools_round_trip_over_http_and_name_transfer_for_a_binary() {
    let fixture = Fixture::new("files");
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    client.initialize();
    let dir = std::env::temp_dir().join(format!("fastexec-http-files-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("notes.txt");
    let written = client.call_tool(
        "write",
        json!({"path": path.display().to_string(), "content": "one\ntwo\n"}),
    );
    assert_eq!(written["isError"], json!(false), "{written}");
    assert_eq!(
        written["content"][0]["text"],
        json!(format!("Successfully wrote to {}", path.display()))
    );
    let read = client.call_tool("read", json!({"path": path.display().to_string()}));
    assert_eq!(read["isError"], json!(false), "{read}");
    assert_eq!(read["content"][0]["text"], json!("one\ntwo\n"));
    // An HTTP session has a way to download a file, so a binary names it.
    let blob = dir.join("blob.bin");
    std::fs::write(&blob, [0u8, 1]).unwrap();
    let binary = client.call_tool("read", json!({"path": blob.display().to_string()}));
    assert_eq!(binary["isError"], json!(true), "{binary}");
    let text = binary["content"][0]["text"].as_str().unwrap_or_default();
    assert!(
        text.contains("Inspect it with exec or download it with transfer."),
        "{binary}"
    );
    let _ = std::fs::remove_dir_all(&dir);
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

/// A path under this fixture's own directory, which the daemon can serve.
fn published(fixture: &Fixture, name: &str) -> PathBuf {
    let dir = fixture.runtime.join("published");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

/// Publishes `path` through the tool and answers the URL it got.
fn publish(client: &mut Client, path: &Path) -> String {
    let result = client.call_tool("transfer", json!({"path": path.display().to_string()}));
    assert_ne!(result["isError"], json!(true), "{result}");
    let url = result["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("no url in {result}"))
        .to_owned();
    assert_eq!(result["structuredContent"]["url"], json!(url), "{result}");
    url
}

/// The target a transfer URL names.
fn target_of(url: &str) -> String {
    let id = url.rsplit('/').next().expect("an id");
    assert_eq!(id.len(), 26, "{url}");
    assert!(
        id.bytes()
            .all(|byte| b"0123456789abcdefghjkmnpqrstvwxyz".contains(&byte)),
        "{url}"
    );
    format!("/t/{id}")
}

/// Waits for the daemon to report `expected` bytes, and answers what it reports last.
fn wait_for_size(addr: SocketAddr, target: &str, expected: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let head = send(addr, "HEAD", target, "", b"");
        let size: u64 = head
            .header("content-length")
            .and_then(|value| value.parse().ok())
            .unwrap_or_else(|| panic!("no length in {:?}", head.headers));
        if size == expected || Instant::now() > deadline {
            return size;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn transfer_is_listed_over_http_and_names_the_scheme_the_client_saw() {
    let fixture = Fixture::new("transfer-url");
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    client.initialize();
    let tools = client.tools();
    assert!(tools.contains(&"transfer".to_owned()), "{tools:?}");
    let path = published(&fixture, "shot.bin");
    let url = publish(&mut client, &path);
    assert_eq!(url, format!("http://{}{}", fixture.addr, target_of(&url)));
    // A proxy that terminated TLS says so, and the first token of the list is the one it means.
    let result = client.call_tool_with(
        "transfer",
        json!({"path": path.display().to_string()}),
        "X-Forwarded-Proto: https, http\r\n",
    );
    let forwarded = result["content"][0]["text"].as_str().expect("a url");
    assert!(
        forwarded.starts_with(&format!("https://{}/t/", fixture.addr)),
        "{forwarded}"
    );
    // A scheme that is neither of the two is not believed.
    let result = client.call_tool_with(
        "transfer",
        json!({"path": path.display().to_string()}),
        "X-Forwarded-Proto: ftp\r\n",
    );
    let other = result["content"][0]["text"].as_str().expect("a url");
    assert!(
        other.starts_with(&format!("http://{}/t/", fixture.addr)),
        "{other}"
    );
    // Every response of an MCP call tells a proxy not to buffer it.
    let response = client.send(json!({"jsonrpc": "2.0", "id": 99, "method": "tools/list"}));
    assert_eq!(response.header("x-accel-buffering"), Some("no"));
}

#[test]
fn a_named_root_wins_over_the_host_and_keeps_its_prefix() {
    let root = "https://files.example/mac/";
    let mut fixture = Fixture::bare("public-root").with_public_url(root);
    fixture.start();
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    client.initialize();
    let path = published(&fixture, "rooted.bin");
    let url = publish(&mut client, &path);
    // The whole URL, not only its prefix: the root, then the id the listener checks itself.
    let target = target_of(&url);
    assert_eq!(url, format!("{root}{}", target.trim_start_matches('/')));
    // Neither the Host the request carries nor a forwarded scheme moves a named root.
    for forwarded in [
        "X-Forwarded-Proto: https\r\n",
        "X-Forwarded-Proto: http\r\n",
    ] {
        let result = client.call_tool_with(
            "transfer",
            json!({"path": path.display().to_string()}),
            forwarded,
        );
        let again = result["content"][0]["text"].as_str().expect("a url");
        let again_target = target_of(again);
        assert_eq!(
            again,
            format!("{root}{}", again_target.trim_start_matches('/')),
            "{again}"
        );
    }
    // The id is served by this listener: the prefix belongs to the proxy that strips it.
    let target = target_of(&url);
    let put = send(fixture.addr, "PUT", &format!("{target}?offset=0"), "", b"");
    assert_eq!(put.status, 200, "{}", put.body);
    let put = send(fixture.addr, "PUT", &target, "", b"rooted bytes");
    assert_eq!(put.body, "12");
    let get = send(fixture.addr, "GET", &target, "", b"");
    assert_eq!(get.status, 200, "{}", get.body);
    assert_eq!(get.bytes, b"rooted bytes");
}

#[test]
fn a_second_start_with_another_root_keeps_the_root_the_daemon_has() {
    let first = "https://first.example/mac/";
    let mut fixture = Fixture::bare("root-kept").with_public_url(first);
    fixture.start();
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    client.initialize();
    // A start that finds a running HTTP daemon keeps that daemon, and with it the root it minted
    // from when it started.
    fixture.public_url = Some("https://second.example/linux/".to_owned());
    let (code, out, err) = fixture.run(&["daemon", "start", "--http", "127.0.0.1:0"]);
    assert_eq!(code, 0, "{out}{err}");
    let path = published(&fixture, "kept.bin");
    let url = publish(&mut client, &path);
    assert!(url.starts_with(first), "{url}");
}

#[test]
fn a_path_that_is_not_mcp_is_not_found() {
    let fixture = Fixture::new("routes");
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    client.initialize();
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "prefix", "version": "1"}
        }
    })
    .to_string();
    // `initialize` answers on the one route a session uses, and on nothing that only looks like it:
    // a proxy that forwards its prefix unstripped fails here, at initialize, rather than later at a
    // download.
    for (target, status) in [
        ("/mcp", 200),
        ("/mcp/", 404),
        ("/mac/mcp", 404),
        ("/MCP", 404),
        ("/t/mcp", 404),
    ] {
        let answer = post_to(fixture.addr, &fixture.tokens[0], target, "", &body);
        assert_eq!(answer.status, status, "{target}: {:?}", answer.body);
    }
    // A path nobody may use is refused as unauthenticated before it is reported missing.
    let anonymous = post_to(fixture.addr, "", "/mac/mcp", "", &body);
    assert_eq!(anonymous.status, 401);
    assert_eq!(anonymous.header("www-authenticate"), Some("Bearer"));
}

#[test]
fn a_daemon_refuses_a_public_root_it_cannot_use() {
    for (value, reason) in [
        ("", "must start with http:// or https://"),
        ("http://h:0/", "port outside 1-65535"),
        ("https://user@h/mac/", "userinfo"),
    ] {
        let fixture = Fixture::bare("bad-root").with_public_url(value);
        // Both entry paths: the CLI refuses before it spawns, and `run` is the daemon itself, which
        // refuses before it binds.
        for action in [
            ["daemon", "start", "--http", "127.0.0.1:0"],
            ["daemon", "run", "--http", "127.0.0.1:0"],
        ] {
            let (code, out, err) = fixture.run(&action);
            assert_eq!(code, 1, "{value:?} ran {action:?}: {out}{err}");
            assert!(
                err.contains("FASTEXEC_PUBLIC_URL") && err.contains(reason),
                "{value:?} through {action:?} gave {err:?}"
            );
        }
        // Nothing was left running for a start that never happened.
        let (code, out, _) = fixture.run(&["daemon", "status"]);
        assert_eq!(code, 0, "{out}");
        assert!(
            out.starts_with("No fastexec daemon"),
            "{value:?} left a daemon: {out}"
        );
    }
}

#[test]
fn an_upload_and_a_download_over_t_need_no_token() {
    let fixture = Fixture::new("transfer-io");
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    client.initialize();
    let file = published(&fixture, "up.bin");
    let target = target_of(&publish(&mut client, &file));
    let addr = fixture.addr;
    // A file that does not exist yet reports no bytes, and the entry is ready for an upload.
    let head = send(addr, "HEAD", &target, "", b"");
    assert_eq!(head.status, 200, "{}", head.body);
    assert_eq!(head.header("content-length"), Some("0"));
    assert_eq!(head.header("cache-control"), Some("no-store"));
    assert_eq!(head.header("x-accel-buffering"), Some("no"));
    assert_eq!(head.header("accept-ranges"), Some("bytes"));
    // An empty upload creates the file and answers its size.
    let put = send(addr, "PUT", &format!("{target}?offset=0"), "", b"");
    assert_eq!(put.status, 200, "{}", put.body);
    assert_eq!(put.body, "0");
    // Two parts, the second from the size the first one left.
    let put = send(addr, "PUT", &target, "", b"hello ");
    assert_eq!(put.status, 200, "{}", put.body);
    assert_eq!(put.body, "6");
    let size = wait_for_size(addr, &target, 6);
    assert_eq!(size, 6);
    let put = send(
        addr,
        "PUT",
        &format!("{target}?offset={size}"),
        "",
        b"world",
    );
    assert_eq!(put.status, 200, "{}", put.body);
    assert_eq!(put.body, "11");
    // The download is the same bytes, and says what it is.
    let get = send(addr, "GET", &target, "", b"");
    assert_eq!(get.status, 200, "{}", get.body);
    assert_eq!(get.bytes, b"hello world");
    assert_eq!(get.header("content-type"), Some("application/octet-stream"));
    assert_eq!(get.header("accept-ranges"), Some("bytes"));
    assert_eq!(get.header("content-length"), Some("11"));
    // An empty upload resets a target that was larger.
    let put = send(addr, "PUT", &format!("{target}?offset=0"), "", b"");
    assert_eq!(put.body, "0");
    let head = send(addr, "HEAD", &target, "", b"");
    assert_eq!(head.header("content-length"), Some("0"));
    assert_eq!(std::fs::read(&file).unwrap(), b"");
}

#[test]
fn ranges_offsets_and_unknown_ids_answer_as_specified() {
    let fixture = Fixture::new("transfer-edges");
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    client.initialize();
    let file = published(&fixture, "edges.bin");
    std::fs::write(&file, b"hello world").unwrap();
    let target = target_of(&publish(&mut client, &file));
    let addr = fixture.addr;
    let get = send(addr, "GET", &target, "Range: bytes=2-5\r\n", b"");
    assert_eq!(get.status, 206, "{}", get.body);
    assert_eq!(get.header("content-range"), Some("bytes 2-5/11"));
    assert_eq!(get.header("content-length"), Some("4"));
    assert_eq!(get.bytes, b"llo ");
    let get = send(addr, "GET", &target, "Range: bytes=11-\r\n", b"");
    assert_eq!(get.status, 416);
    assert_eq!(get.header("content-range"), Some("bytes */11"));
    assert_eq!(get.header("content-type"), Some("application/octet-stream"));
    assert!(get.bytes.is_empty());
    // Everything the two forms do not describe is ignored, and the whole file is sent.
    for header in [
        "Range: bytes=5-2\r\n",
        "Range: bytes=-3\r\n",
        "Range: bytes=0-1,3-4\r\n",
        "Range: bytes=0-1\r\nRange: bytes=3-4\r\n",
        "Range: nonsense\r\n",
        "Range: bytes=0-18446744073709551616\r\n",
    ] {
        let get = send(addr, "GET", &target, header, b"");
        assert_eq!(get.status, 200, "{header}");
        assert_eq!(get.bytes, b"hello world", "{header}");
    }
    // An unknown id and a path that is not an id are both absent, whatever the method is: an
    // entry that is not there is absent before the method is even looked at.
    for path in ["/t/0123456789abcdefghjkmnpqrs", "/t/../mcp", "/t/"] {
        for method in ["GET", "HEAD", "PUT", "DELETE", "OPTIONS", "POST", "PATCH"] {
            let answer = send(addr, method, path, "", b"");
            assert_eq!(answer.status, 404, "{method} {path}");
            assert_eq!(answer.header("cache-control"), Some("no-store"));
            assert_eq!(
                answer.header("content-type"),
                Some("application/octet-stream"),
                "{method} {path}"
            );
        }
    }
    // A method the route does not serve says which ones it does.
    let delete = send(addr, "DELETE", &target, "", b"");
    assert_eq!(delete.status, 405);
    assert_eq!(delete.header("allow"), Some("GET, HEAD, PUT"));
    assert_eq!(
        delete.header("content-type"),
        Some("application/octet-stream")
    );
    // An offset past the end is refused, and the file is left as it was.
    let put = send(addr, "PUT", &format!("{target}?offset=99"), "", b"x");
    assert_eq!(put.status, 409);
    assert_eq!(std::fs::read(&file).unwrap(), b"hello world");
    // An offset that is not one decimal number is refused before anything is opened.
    for query in [
        "offset=",
        "offset",
        "offset=+1",
        "offset=1.5",
        "offset=1&offset=2",
        "offset=18446744073709551616",
    ] {
        let put = send(addr, "PUT", &format!("{target}?{query}"), "", b"x");
        assert_eq!(put.status, 400, "{query}");
        assert_eq!(std::fs::read(&file).unwrap(), b"hello world", "{query}");
    }
    // An unrelated query key is not an offset.
    let put = send(addr, "PUT", &format!("{target}?other=1"), "", b"!");
    assert_eq!(put.status, 200, "{}", put.body);
    // A file that does not exist cannot be uploaded into past its start.
    let absent = published(&fixture, "absent.bin");
    let absent_target = target_of(&publish(&mut client, &absent));
    let put = send(addr, "PUT", &format!("{absent_target}?offset=3"), "", b"x");
    assert_eq!(put.status, 409);
    assert!(!absent.exists());
}

#[test]
fn an_interrupted_upload_leaves_a_prefix_that_resumes() {
    let fixture = Fixture::new("transfer-resume");
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    client.initialize();
    let file = published(&fixture, "resume.bin");
    let target = target_of(&publish(&mut client, &file));
    let addr = fixture.addr;
    let source: Vec<u8> = (0..1000u32).map(|number| (number % 251) as u8).collect();
    // A part the client never finishes: it declares a thousand bytes and sends three hundred.
    let mut stream = TcpStream::connect(addr).expect("the daemon accepts a connection");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let head = format!(
        "PUT {target} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 1000\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(head.as_bytes()).unwrap();
    stream.write_all(&source[..300]).unwrap();
    stream.flush().unwrap();
    stream.shutdown(std::net::Shutdown::Write).unwrap();
    let mut answer = Vec::new();
    let _ = stream.read_to_end(&mut answer);
    // The bytes the daemon accepted are the file's size, and the rest resumes from there.
    let size = wait_for_size(addr, &target, 300);
    assert_eq!(size, 300, "the accepted prefix is kept");
    let rest = send(
        addr,
        "PUT",
        &format!("{target}?offset={size}"),
        "",
        &source[size as usize..],
    );
    assert_eq!(rest.status, 200, "{}", rest.body);
    assert_eq!(rest.body, "1000");
    let get = send(addr, "GET", &target, "", b"");
    assert_eq!(get.status, 200);
    assert_eq!(get.bytes, source);
}

/// Sends one PUT whose body is chunked, so the route is exercised without a declared length.
fn send_chunked(addr: SocketAddr, target: &str, body: &[u8]) -> Response {
    let mut request = Vec::new();
    request.extend_from_slice(format!("PUT {target} HTTP/1.1\r\n").as_bytes());
    request.extend_from_slice(format!("Host: {addr}\r\n").as_bytes());
    request.extend_from_slice(b"Transfer-Encoding: chunked\r\n");
    request.extend_from_slice(b"Connection: close\r\n");
    request.extend_from_slice(b"\r\n");
    request.extend_from_slice(format!("{:x}\r\n", body.len()).as_bytes());
    request.extend_from_slice(body);
    request.extend_from_slice(b"\r\n0\r\n\r\n");
    exchange(addr, &request)
}

#[test]
fn an_upload_larger_than_the_call_cap_arrives_whole() {
    let fixture = Fixture::new("transfer-large");
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    client.initialize();
    let file = published(&fixture, "large.bin");
    let target = target_of(&publish(&mut client, &file));
    let addr = fixture.addr;
    // Larger than the 4 MiB a call may carry, so the route's own limit is what admits it.
    let payload: Vec<u8> = (0..5 * 1024 * 1024 + 12_345u32)
        .map(|number| (number % 253) as u8)
        .collect();
    assert!(payload.len() > 4 << 20);
    let put = send(addr, "PUT", &format!("{target}?offset=0"), "", &payload);
    assert_eq!(put.status, 200, "{}", put.body);
    assert_eq!(put.body, payload.len().to_string());
    let get = send(addr, "GET", &target, "", b"");
    assert_eq!(get.status, 200);
    assert_eq!(get.bytes.len(), payload.len());
    assert!(get.bytes == payload, "the download is the upload");
    // The same size again through chunked framing, after the empty upload resets the file.
    let put = send(addr, "PUT", &format!("{target}?offset=0"), "", b"");
    assert_eq!(put.body, "0");
    let put = send_chunked(addr, &target, &payload);
    assert_eq!(put.status, 200, "{}", put.body);
    assert_eq!(put.body, payload.len().to_string());
    let get = send(addr, "GET", &target, "", b"");
    assert!(get.bytes == payload, "the chunked download is the upload");
}

/// A descriptor the daemon can open as a regular file, for a write the disk refuses.
#[cfg(target_os = "linux")]
struct MemoryFile(std::os::fd::OwnedFd);

#[cfg(target_os = "linux")]
impl MemoryFile {
    /// A memory file, sealed against writes when `sealed`: metadata, opening, `set_len` and
    /// `seek` still work, and every write fails with `EPERM`.
    fn new(sealed: bool) -> MemoryFile {
        use std::os::fd::{AsRawFd, FromRawFd};
        let name = std::ffi::CString::new("fastexec-transfer").unwrap();
        let raw = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_ALLOW_SEALING) };
        assert!(raw >= 0, "memfd_create failed");
        let file = MemoryFile(unsafe { std::os::fd::OwnedFd::from_raw_fd(raw) });
        if sealed {
            let added =
                unsafe { libc::fcntl(file.0.as_raw_fd(), libc::F_ADD_SEALS, libc::F_SEAL_WRITE) };
            assert_eq!(added, 0, "the seal was added");
        }
        file
    }

    /// The path the daemon opens, which names this descriptor of this process.
    fn path(&self) -> String {
        use std::os::fd::AsRawFd;
        format!("/proc/{}/fd/{}", std::process::id(), self.0.as_raw_fd())
    }
}

#[cfg(target_os = "linux")]
#[test]
fn a_refused_disk_write_is_not_acknowledged_as_success() {
    let fixture = Fixture::new("transfer-sealed");
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    client.initialize();
    let addr = fixture.addr;
    // The control: an unsealed memory file accepts the same upload and answers its size.
    let open = MemoryFile::new(false);
    let target = target_of(&publish(&mut client, std::path::Path::new(&open.path())));
    let put = send(
        addr,
        "PUT",
        &format!("{target}?offset=0"),
        "",
        b"fifteen bytes!!",
    );
    assert_eq!(put.status, 200, "{}", put.body);
    assert_eq!(put.body, "15");
    // The sealed one lets every step but the write through, and the write is what must be said.
    let sealed = MemoryFile::new(true);
    let target = target_of(&publish(&mut client, std::path::Path::new(&sealed.path())));
    let put = send(
        addr,
        "PUT",
        &format!("{target}?offset=0"),
        "",
        b"fifteen bytes!!",
    );
    assert_eq!(put.status, 500, "{}", put.body);
    assert!(put.bytes.is_empty());
}

/// Opens a PUT whose first three bytes have arrived but whose body is not yet complete.
fn begin_paused_upload(addr: SocketAddr, target: &str) -> TcpStream {
    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let headers = format!(
        "PUT {target} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: 6\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(headers.as_bytes()).unwrap();
    stream.write_all(b"AAA").unwrap();
    stream.flush().unwrap();
    stream
}

fn finish_paused_upload(mut stream: TcpStream) -> Response {
    stream.write_all(b"CCC").unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).unwrap();
    parse(&response)
}

/// Two capabilities may name the very same file. Neither upload may truncate or overwrite the
/// other while it is still streaming, even though the callers use different path spellings.
fn check_serial_uploads(fixture: &Fixture, first: &str, second: &str, file: &Path) {
    let paused = begin_paused_upload(fixture.addr, first);
    // Existing alias fixtures contain three bytes too ("old"). A HEAD length of three can
    // therefore be observed before this PUT acquires the lock. Wait for its actual write.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let contents = std::fs::read(file);
        if matches!(&contents, Ok(bytes) if bytes == b"AAA") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the first upload did not write its prefix to {}: {contents:?}",
            file.display(),
        );
        std::thread::sleep(Duration::from_millis(25));
    }
    let mut stream = TcpStream::connect(fixture.addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let request = format!(
        "PUT {second} HTTP/1.1\r\nHost: {}\r\nContent-Length: 3\r\nConnection: close\r\n\r\nBBB",
        fixture.addr,
    );
    stream.write_all(request.as_bytes()).unwrap();
    stream.flush().unwrap();
    let (send_result, result) = std::sync::mpsc::channel();
    let reading = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        let read = stream.read_to_end(&mut bytes);
        let _ = send_result.send((read, bytes));
    });
    let early = result.recv_timeout(Duration::from_millis(250));
    let waited = matches!(early, Err(std::sync::mpsc::RecvTimeoutError::Timeout));
    // Finish both requests before asserting, so a failed check never leaves a writer stranded.
    let first_reply = finish_paused_upload(paused);
    let (read, bytes) = match early {
        Ok(reply) => reply,
        Err(_) => result.recv_timeout(Duration::from_secs(10)).unwrap(),
    };
    reading.join().unwrap();
    read.unwrap();
    let second_reply = parse(&bytes);
    assert_eq!(first_reply.status, 200, "{}", first_reply.body);
    assert_eq!(second_reply.status, 200, "{}", second_reply.body);
    assert!(
        waited,
        "the second capability overwrote a still-active upload"
    );
    assert_eq!(std::fs::read(file).unwrap(), b"BBB");
}

#[test]
fn transfer_uploads_to_one_file_serialize_across_capabilities_and_aliases() {
    let mut fixture = Fixture::bare("transfer-file-identity");
    fixture.cwd = Some(fixture.runtime.clone());
    fixture.start();
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    client.initialize();
    // A previously missing file gets two IDs; opening it must not give each upload its own lock.
    let file = published(&fixture, "same.bin");
    let first = target_of(&publish(&mut client, &file));
    let second = target_of(&publish(&mut client, &file));
    assert_ne!(first, second);
    check_serial_uploads(&fixture, &first, &second, &file);

    let file = published(&fixture, "relative.bin");
    let first = target_of(&publish(&mut client, &file));
    let second = target_of(&publish(&mut client, Path::new("published/relative.bin")));
    check_serial_uploads(&fixture, &first, &second, &file);

    #[cfg(unix)]
    {
        // A symlinked directory also aliases a file that does not exist at publication time.
        let alias_dir = fixture.runtime.join("alias-dir");
        std::os::unix::fs::symlink(fixture.runtime.join("published"), &alias_dir).unwrap();
        let file = published(&fixture, "symlink-dir.bin");
        let first = target_of(&publish(&mut client, &file));
        let second = target_of(&publish(&mut client, &alias_dir.join("symlink-dir.bin")));
        check_serial_uploads(&fixture, &first, &second, &file);

        let file = published(&fixture, "symlink-file.bin");
        std::fs::write(&file, b"old").unwrap();
        let alias = fixture.runtime.join("file-link");
        std::os::unix::fs::symlink(&file, &alias).unwrap();
        let first = target_of(&publish(&mut client, &file));
        let second = target_of(&publish(&mut client, &alias));
        check_serial_uploads(&fixture, &first, &second, &file);
    }

    // Hard links share content even though canonicalizing their paths yields different names.
    let file = published(&fixture, "hardlink.bin");
    std::fs::write(&file, b"old").unwrap();
    let alias = fixture.runtime.join("hardlink-alias");
    std::fs::hard_link(&file, &alias).unwrap();
    let first = target_of(&publish(&mut client, &file));
    let second = target_of(&publish(&mut client, &alias));
    check_serial_uploads(&fixture, &first, &second, &file);
}

#[test]
fn transfer_uploads_to_different_files_do_not_wait_for_each_other() {
    let fixture = Fixture::new("transfer-independent");
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    client.initialize();
    let first_file = published(&fixture, "first.bin");
    let second_file = published(&fixture, "second.bin");
    let first = target_of(&publish(&mut client, &first_file));
    let second = target_of(&publish(&mut client, &second_file));
    let paused = begin_paused_upload(fixture.addr, &first);
    assert_eq!(wait_for_size(fixture.addr, &first, 3), 3);
    let response = send(fixture.addr, "PUT", &second, "", b"independent");
    let first_reply = finish_paused_upload(paused);
    assert_eq!(response.status, 200, "{}", response.body);
    assert_eq!(first_reply.status, 200, "{}", first_reply.body);
    assert_eq!(std::fs::read(&first_file).unwrap(), b"AAACCC");
    assert_eq!(std::fs::read(&second_file).unwrap(), b"independent");
}

#[test]
fn http_idle_connections_are_bounded_and_capacity_recovers() {
    let fixture = Fixture::new("http-connection-limit");
    let mut held = Vec::new();
    for _ in 0..128 {
        let mut stream = TcpStream::connect(fixture.addr).unwrap();
        stream.write_all(b"G").unwrap();
        held.push(stream);
    }
    let mut excess = TcpStream::connect(fixture.addr).unwrap();
    excess
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    // The limit is before authentication and before parsing, so this request is refused as a
    // connection instead of being served while all 128 existing connections remain admitted.
    let request = format!(
        "GET /mcp HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n",
        fixture.addr
    );
    let _ = excess.write_all(request.as_bytes());
    let mut bytes = Vec::new();
    let ended = excess.read_to_end(&mut bytes);
    assert!(
        matches!(ended, Ok(0))
            || ended.is_err_and(|error| matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
            )),
        "an excess unauthenticated connection was not closed: {:?}",
        String::from_utf8_lossy(&bytes),
    );
    // HTTP pressure must not prevent the local management connection from working.
    let (code, out, err) = fixture.run(&["daemon", "status"]);
    assert_eq!(code, 0, "{out}{err}");
    assert!(out.contains("http"), "{out}");
    drop(held);
    // A closed connection hands its slot back promptly, not at a sweep ten minutes later.
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let mut stream = TcpStream::connect(fixture.addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        let mut bytes = Vec::new();
        if stream.write_all(request.as_bytes()).is_ok()
            && stream.read_to_end(&mut bytes).is_ok()
            && bytes.starts_with(b"HTTP/1.1 401")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "released HTTP capacity never recovered"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn http_incomplete_headers_time_out() {
    let fixture = Fixture::new("http-header-timeout");
    let mut stream = TcpStream::connect(fixture.addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(14)))
        .unwrap();
    stream.write_all(b"POST /mcp HTTP/1.1\r\nHost: ").unwrap();
    let mut bytes = Vec::new();
    let ended = stream.read_to_end(&mut bytes);
    assert!(
        ended.is_ok()
            || ended.is_err_and(|error| matches!(
                error.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
            )),
        "an incomplete unauthenticated header held the connection past its deadline",
    );
    assert_eq!(send(fixture.addr, "GET", "/mcp", "", b"").status, 401);
}

#[test]
fn http_unauthorized_incomplete_bodies_do_not_hold_a_connection() {
    let fixture = Fixture::new("http-rejected-body");
    let mut stream = TcpStream::connect(fixture.addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let request = format!(
        "POST /mcp HTTP/1.1\r\nHost: {}\r\nContent-Length: 1000\r\n\r\n",
        fixture.addr,
    );
    stream.write_all(request.as_bytes()).unwrap();
    let mut bytes = Vec::new();
    stream
        .read_to_end(&mut bytes)
        .expect("an unauthorized body was not drained indefinitely");
    let response = parse(&bytes);
    assert_eq!(response.status, 401);
    assert_eq!(response.header("connection"), Some("close"));
}

#[test]
fn http_a_long_tool_call_outlives_the_header_deadline() {
    let fixture = Fixture::new("http-long-tool-call");
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    client.initialize();
    let result = client.call_tool(
        "exec",
        json!({
            "action": "start", "command": "sleep 12\nprintf after-header-deadline", "waitMs": 15000,
        }),
    );
    assert_ne!(result["isError"], json!(true), "{result}");
    assert_eq!(
        result["structuredContent"]["exitCode"],
        json!(0),
        "{result}"
    );
    assert!(
        result["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("after-header-deadline"),
        "{result}"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn disconnected_uploads_waiting_for_the_same_file_release_their_descriptors() {
    let fixture = Fixture::new("transfer-cancelled-queue");
    let pid = fixture.pid.get().expect("the fixture started a daemon");
    let descriptors = || {
        std::fs::read_dir(format!("/proc/{pid}/fd"))
            .unwrap()
            .count()
    };
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    client.initialize();
    let file = published(&fixture, "paused.bin");
    let target = target_of(&publish(&mut client, &file));
    let paused = begin_paused_upload(fixture.addr, &target);
    assert_eq!(wait_for_size(fixture.addr, &target, 3), 3);
    // Wait for the HEAD request's descriptor to be closed before taking the baseline.
    std::thread::sleep(Duration::from_millis(30));
    let active = descriptors();
    for _ in 0..16 {
        let before = descriptors();
        let mut stream = TcpStream::connect(fixture.addr).unwrap();
        let request = format!(
            "PUT {target} HTTP/1.1\r\nHost: {}\r\nContent-Length: 3\r\nConnection: close\r\n\r\nBBB",
            fixture.addr,
        );
        stream.write_all(request.as_bytes()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        // The new socket and file must both exist: closing a request before the server read its
        // headers would not exercise cancellation of an upload already queued for the lock.
        while descriptors() < before + 2 && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert!(
            descriptors() >= before + 2,
            "the upload did not reach the file lock"
        );
        drop(stream);
        std::thread::sleep(Duration::from_millis(10));
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while descriptors() > active + 2 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    let remaining = descriptors();
    let finished = finish_paused_upload(paused);
    assert_eq!(finished.status, 200);
    assert!(
        remaining <= active + 2,
        "closed requests retained file descriptors behind a slow upload: {active} -> {remaining}"
    );
    assert_eq!(std::fs::read(file).unwrap(), b"AAACCC");
}

#[test]
fn malformed_or_truncated_upload_bodies_keep_the_prefix_but_are_not_successful() {
    let fixture = Fixture::new("transfer-invalid-body");
    let mut client = Client::new(fixture.addr, fixture.tokens[0].clone());
    client.initialize();
    let file = published(&fixture, "partial.bin");
    let target = target_of(&publish(&mut client, &file));
    for (framing, body) in [
        ("Transfer-Encoding: chunked", &b"3\r\nABC\r\nBOGUS\r\n"[..]),
        ("Content-Length: 10", &b"ABC"[..]),
    ] {
        std::fs::write(&file, b"old contents").unwrap();
        let mut stream = TcpStream::connect(fixture.addr).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let headers = format!(
            "PUT {target} HTTP/1.1\r\nHost: {}\r\n{framing}\r\nConnection: close\r\n\r\n",
            fixture.addr,
        );
        stream.write_all(headers.as_bytes()).unwrap();
        stream.write_all(body).unwrap();
        stream.shutdown(std::net::Shutdown::Write).unwrap();
        let mut bytes = Vec::new();
        stream.read_to_end(&mut bytes).unwrap();
        let response = parse(&bytes);
        assert_eq!(response.status, 400, "{framing}: {}", response.body);
        assert_eq!(wait_for_size(fixture.addr, &target, 3), 3);
        assert_eq!(std::fs::read(&file).unwrap(), b"ABC");
        // The failed transfer is still resumable from its acknowledged prefix.
        let resumed = send(
            fixture.addr,
            "PUT",
            &format!("{target}?offset=3"),
            "",
            b"DEF",
        );
        assert_eq!(resumed.status, 200, "{}", resumed.body);
        assert_eq!(std::fs::read(&file).unwrap(), b"ABCDEF");
    }
}

#[test]
fn malformed_http_requests_do_not_grow_the_daemon_error_log() {
    let fixture = Fixture::new("http-malformed-log");
    let log = std::fs::read_dir(fixture.runtime.join("ns"))
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path().join("daemon.log"))
        .find(|path| path.is_file())
        .expect("the fixture owns one daemon log");
    let before = std::fs::metadata(&log).unwrap().len();
    for _ in 0..32 {
        let invalid = exchange(
            fixture.addr,
            b"G\0T /mcp HTTP/1.1\r\nHost: localhost\r\n\r\n",
        );
        assert_eq!(invalid.status, 400);
        let mut abandoned = TcpStream::connect(fixture.addr).unwrap();
        abandoned
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        abandoned.write_all(b"GET /mcp HTTP/1.1\r\nHost: ").unwrap();
        abandoned.shutdown(std::net::Shutdown::Write).unwrap();
        let mut bytes = Vec::new();
        let _ = abandoned.read_to_end(&mut bytes);
    }
    assert_eq!(send(fixture.addr, "GET", "/mcp", "", b"").status, 401);
    std::thread::sleep(Duration::from_millis(50));
    let after = std::fs::metadata(log).unwrap().len();
    assert_eq!(
        after, before,
        "unauthenticated parse failures amplified into persistent error logs"
    );
}

#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_fixture_fallback_cleanup_owns_its_daemon() {
    let fixture = Fixture::new("freebsd-fallback");
    let pid = fixture.pid.get().unwrap();
    assert!(fixture.owns_process(pid));
    assert!(!fixture.owns_process(std::process::id()));
    assert!(support::freebsd::end_owned_process(
        pid,
        &fixture.runtime.join("bin")
    ));
    assert!(!support::freebsd::is_alive(pid));
}
