//! The daemon as a process: started, inspected, and stopped through the CLI, in a runtime
//! directory of the test's own; and the MCP endpoint in `on` mode, which forwards every call to
//! that daemon.

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Output, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, Instant};

/// A namespace of its own for one test, and a runtime directory to hold it. The daemon it starts
/// is stopped however the test ends.
struct Fixture {
    runtime: PathBuf,
    namespace: String,
    /// Whether this fixture owns the runtime directory and removes it when it ends.
    owns_runtime: bool,
    /// A temporary directory for the child, when a test needs one of its own.
    tmpdir: Option<PathBuf>,
    /// The daemon this fixture saw running, so cleanup can end that one process by PID.
    pid: std::cell::Cell<Option<u32>>,
}

/// Creates a runtime directory the production code accepts: mode 0700 on Unix.
fn create_root(path: &Path) {
    let _ = std::fs::remove_dir_all(path);
    #[cfg_attr(not(unix), allow(unused_mut))]
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(path).unwrap();
}

impl Fixture {
    fn new(tag: &str) -> Fixture {
        let runtime =
            std::env::temp_dir().join(format!("fastexec-daemon-{}-{tag}", std::process::id()));
        create_root(&runtime);
        Fixture {
            runtime,
            namespace: format!("test{tag}-{}", std::process::id()),
            owns_runtime: true,
            tmpdir: None,
            pid: std::cell::Cell::new(None),
        }
    }

    /// A namespace that shares an existing runtime directory, so two of them meet in one cache.
    fn sharing(runtime: &Path, scope: &str) -> Fixture {
        Fixture {
            runtime: runtime.to_path_buf(),
            namespace: format!("{scope}-{}", std::process::id()),
            owns_runtime: false,
            tmpdir: None,
            pid: std::cell::Cell::new(None),
        }
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        command
            .args(args)
            .env("FASTEXEC_TEST_RUNTIME_DIR", &self.runtime)
            .env("FASTEXEC_NAMESPACE", &self.namespace)
            .stdin(std::process::Stdio::null());
        if let Some(tmpdir) = &self.tmpdir {
            command
                .env("TMPDIR", tmpdir)
                .env("TMP", tmpdir)
                .env("TEMP", tmpdir);
        }
        command
    }

    fn output(&self, args: &[&str]) -> Output {
        self.command(args).output().unwrap()
    }

    fn run(&self, args: &[&str]) -> (i32, String, String) {
        let output = self.output(args);
        let (code, out, err) = (
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stdout).into_owned(),
            String::from_utf8_lossy(&output.stderr).into_owned(),
        );
        // The daemon this fixture met, so its cleanup can end that one process if the CLI cannot.
        if let Some(pid) = pid_of(&out) {
            self.pid.set(Some(pid));
        }
        (code, out, err)
    }

    /// The daemon's PID, asked for once, when the fixture never saw one itself.
    fn learn_pid(&self) -> Option<u32> {
        let output = self.command(&["daemon", "status"]).output().ok()?;
        pid_of(&String::from_utf8_lossy(&output.stdout))
    }

    /// The epoch a `start` or `status` line names.
    fn epoch_of(&self, text: &str) -> String {
        let words: Vec<&str> = text.split_whitespace().collect();
        let at = words
            .iter()
            .position(|word| *word == "daemon" || *word == "epoch")
            .unwrap_or_else(|| panic!("no epoch in {text:?}"));
        words
            .get(at + 1)
            .unwrap_or_else(|| panic!("no epoch in {text:?}"))
            .to_string()
    }

    /// The value of a `key value` line in `status` output.
    fn field(text: &str, key: &str) -> String {
        text.lines()
            .find_map(|line| line.strip_prefix(&format!("{key} ")))
            .unwrap_or_else(|| panic!("no {key} line in {text:?}"))
            .trim()
            .to_string()
    }
}

/// The PID a `start` or `status` line names.
fn pid_of(text: &str) -> Option<u32> {
    let words: Vec<&str> = text.split_whitespace().collect();
    let at = words.iter().position(|word| *word == "pid")?;
    words.get(at + 1)?.parse().ok()
}

impl Fixture {
    /// Whether `pid` is this fixture's daemon: a process running the cached image under this
    /// fixture's own runtime directory. A number alone is not ownership, and a number from a
    /// daemon that already stopped can belong to anything by now.
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
            let expected = expected.display().to_string().to_lowercase();
            !path.is_empty() && path.starts_with(&expected)
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
        #[cfg(all(unix, not(target_os = "macos")))]
        {
            std::fs::read_link(format!("/proc/{pid}/exe"))
                .map(|path| path.starts_with(&expected))
                .unwrap_or(false)
        }
    }

    /// Ends one process by PID, and only when it is this fixture's own daemon.
    fn end_own_daemon(&self, pid: u32) -> bool {
        if !self.owns_process(pid) {
            return false;
        }
        let pid = pid.to_string();
        #[cfg(windows)]
        let output = Command::new("taskkill").args(["/F", "/PID", &pid]).output();
        #[cfg(unix)]
        let output = Command::new("kill").args(["-9", &pid]).output();
        matches!(output, Ok(output) if output.status.success())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Whatever the test did, its daemon goes: it would otherwise outlive the test binary,
        // and its tasks with it. Nothing here goes through `run`, which unwraps a process spawn:
        // this can be the cleanup of a panic, and a second panic aborts the harness.
        let (stopped, why) = match self.command(&["daemon", "stop"]).output() {
            Ok(output) => {
                let out = String::from_utf8_lossy(&output.stdout).into_owned();
                let err = String::from_utf8_lossy(&output.stderr).into_owned();
                let stopped = output.status.code() == Some(0)
                    && (out.contains("Stopped") || out.starts_with("No fastexec daemon"));
                (stopped, format!("stop {out:?}{err:?}"))
            }
            // A CLI that cannot start says nothing about the daemon: what is left is unknown,
            // not stopped.
            Err(error) => (false, format!("stop could not run: {error}")),
        };
        if stopped {
            // Nothing of this fixture is running, so the number it saw before is no longer its.
            self.pid.set(None);
        }
        // The daemon an endpoint started is still this fixture's daemon, and `status` names it.
        let pid = if stopped {
            None
        } else {
            self.pid.get().or_else(|| self.learn_pid())
        };
        let gone = stopped || pid.is_some_and(|pid| self.end_own_daemon(pid));
        if self.owns_runtime {
            let _ = std::fs::remove_dir_all(&self.runtime);
        }
        // A test that passes must not report success with its daemon still running. During a
        // panic this stays best effort, so the first panic is the one that is reported.
        if !gone && !std::thread::panicking() {
            panic!("the fixture could not stop its daemon (pid {pid:?}): {why}");
        }
    }
}

/// One MCP endpoint in `on` mode, driven over stdio the way a host drives it.
struct Endpoint {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<Value>,
    next_id: u64,
}

impl Endpoint {
    /// Starts an endpoint on `fixture`'s daemon namespace.
    fn start(fixture: &Fixture) -> Endpoint {
        Endpoint::start_with(fixture, |_| {})
    }

    /// Starts an endpoint after `configure` adjusts its launch, such as its environment.
    fn start_with(fixture: &Fixture, configure: impl FnOnce(&mut Command)) -> Endpoint {
        let mut command = fixture.command(&[]);
        configure(&mut command);
        command
            // The endpoint's own settings are the defaults here: a host's environment must not
            // change what these tests read.
            .env_remove("FASTEXEC_OUTPUT_MODE")
            .env_remove("FASTEXEC_STRUCTURED_CONTENT")
            .env_remove("FASTEXEC_TEST_TOTAL_LOG_LIMIT")
            .env_remove("FASTEXEC_TEST_NONE_LIMIT")
            .env("FASTEXEC_DAEMON", "on")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let mut child = command.spawn().expect("spawn fastexec");
        let stdout = child.stdout.take().unwrap();
        let (sender, lines) = channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if let Ok(value) = serde_json::from_str(&line) {
                    let _ = sender.send(value);
                }
            }
        });
        let stdin = child.stdin.take();
        let mut endpoint = Endpoint {
            child,
            stdin,
            lines,
            next_id: 0,
        };
        endpoint.request(
            "initialize",
            json!({"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "test", "version": "0"}}),
        );
        endpoint.send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));
        endpoint
    }

    fn send(&mut self, message: Value) {
        let stdin = self.stdin.as_mut().expect("stdin is open");
        writeln!(stdin, "{message}").unwrap();
        stdin.flush().unwrap();
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        self.response(id)
    }

    /// Sends a request without waiting for its answer, so a call can be in flight while the test
    /// does something else.
    fn request_later(&mut self, method: &str, params: Value) -> u64 {
        self.next_id += 1;
        let id = self.next_id;
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        id
    }

    fn response(&mut self, id: u64) -> Value {
        // A failure bound, not a wait: it stays well above the longest wait a call asks for.
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let message = self
                .lines
                .recv_timeout(deadline - Instant::now())
                .expect("response in time");
            if message["id"] == json!(id) {
                return message;
            }
        }
    }

    /// Calls the tool: (structured content, text, isError).
    fn call(&mut self, arguments: Value) -> (Value, String, bool) {
        let message = self.request(
            "tools/call",
            json!({"name": "exec", "arguments": arguments}),
        );
        Endpoint::answer(&message)
    }

    /// Calls a file tool: (structured content, text, isError).
    fn file(&mut self, tool: &str, arguments: Value) -> (Value, String, bool) {
        let message = self.request("tools/call", json!({"name": tool, "arguments": arguments}));
        Endpoint::answer(&message)
    }

    fn answer(message: &Value) -> (Value, String, bool) {
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

    /// Closes stdin and waits for the endpoint to exit, as a host does at shutdown.
    fn close(mut self) {
        drop(self.stdin.take());
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                assert!(status.success(), "the endpoint exited with {status}");
                return;
            }
            assert!(Instant::now() < deadline, "the endpoint did not exit");
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// The next message the endpoint sent that `matches`, such as a progress notification.
    fn next_matching(&mut self, what: &str, matches: impl Fn(&Value) -> bool) -> Value {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let message = self
                .lines
                .recv_timeout(deadline - Instant::now())
                .unwrap_or_else(|_| panic!("{what}"));
            if matches(&message) {
                return message;
            }
        }
    }

    /// The task ID a `start` answer named.
    fn task_id(structured: &Value, text: &str) -> String {
        if let Some(id) = structured["taskId"].as_str() {
            return id.to_string();
        }
        // The status line, the last line that opens with `[`, names the task after the state.
        text.lines()
            .rev()
            .find_map(|line| {
                line.strip_prefix('[')?
                    .split_once("] ")?
                    .1
                    .split(' ')
                    .next()
            })
            .unwrap_or_else(|| panic!("no task ID: {text}"))
            .to_string()
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn sha256(path: &Path) -> String {
    let bytes = std::fs::read(path).unwrap();
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let mut out = String::new();
    for byte in hasher.finalize() {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[test]
fn status_and_stop_with_no_daemon_say_so_and_succeed() {
    let fixture = Fixture::new("none");
    for action in ["status", "stop"] {
        let (code, out, err) = fixture.run(&["daemon", action]);
        assert_eq!(code, 0, "{action} failed: {err}");
        assert!(
            out.starts_with("No fastexec daemon for namespace ") && out.trim_end().ends_with('.'),
            "{action} printed {out:?}"
        );
    }
}

#[test]
fn a_daemon_starts_is_reported_and_stops() {
    let fixture = Fixture::new("life");
    let (code, out, err) = fixture.run(&["daemon", "start"]);
    assert_eq!(code, 0, "start failed: {err}");
    let epoch = fixture.epoch_of(&out);
    assert_eq!(epoch.len(), 26, "{epoch}");
    // The namespace is the 16 hex characters the identity hashes the scope into.
    let namespace = out
        .split_whitespace()
        .skip_while(|word| *word != "namespace")
        .nth(1)
        .unwrap_or_else(|| panic!("no namespace in {out:?}"))
        .to_string();
    assert_eq!(namespace.len(), 16, "{namespace}");
    assert!(
        namespace.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "{namespace}"
    );

    let (code, status, err) = fixture.run(&["daemon", "status"]);
    assert_eq!(code, 0, "status failed: {err}");
    assert_eq!(Fixture::field(&status, "epoch"), epoch, "{status}");
    let pid: u32 = Fixture::field(&status, "pid").parse().unwrap();
    assert_ne!(pid, 0, "{status}");
    assert_ne!(pid, std::process::id(), "the status CLI reported itself");
    assert_eq!(Fixture::field(&status, "sessions"), "0", "{status}");
    assert_eq!(Fixture::field(&status, "tasks"), "0", "{status}");

    // A second start joins the daemon that is already there.
    let (code, again, err) = fixture.run(&["daemon", "start"]);
    assert_eq!(code, 0, "the second start failed: {err}");
    assert_eq!(fixture.epoch_of(&again), epoch, "{again}");

    let (code, out, err) = fixture.run(&["daemon", "stop"]);
    assert_eq!(code, 0, "stop failed: {err}");
    assert_eq!(out.trim(), format!("Stopped fastexec daemon {epoch}."));

    let (code, out, err) = fixture.run(&["daemon", "status"]);
    assert_eq!(code, 0, "status failed: {err}");
    assert!(
        out.contains("No fastexec daemon for namespace"),
        "the daemon is still there: {out:?}"
    );
}

#[test]
fn four_starts_at_once_reach_one_daemon() {
    let fixture = Fixture::new("race");
    let children: Vec<_> = (0..4)
        .map(|_| {
            fixture
                .command(&["daemon", "start"])
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
                .unwrap()
        })
        .collect();
    let mut epochs = Vec::new();
    for child in children {
        let output = child.wait_with_output().unwrap();
        let out = String::from_utf8_lossy(&output.stdout).into_owned();
        let err = String::from_utf8_lossy(&output.stderr).into_owned();
        assert_eq!(output.status.code(), Some(0), "a start failed: {err}");
        epochs.push(fixture.epoch_of(&out));
    }
    assert!(
        epochs.windows(2).all(|pair| pair[0] == pair[1]),
        "the starts reached different daemons: {epochs:?}"
    );
}

#[test]
fn a_start_sweeps_what_another_namespace_left() {
    let fixture = Fixture::new("sweep");
    let stale = fixture.runtime.join("ns").join("0000000000000000");
    std::fs::create_dir_all(&stale).unwrap();
    std::fs::write(stale.join("startup.lock"), b"").unwrap();
    std::fs::write(stale.join("daemon.lock"), b"").unwrap();
    std::fs::write(stale.join("payload"), b"left behind").unwrap();
    std::fs::create_dir_all(stale.join("logs")).unwrap();
    std::fs::write(stale.join("logs").join("task.log"), b"left behind").unwrap();

    let (code, _, err) = fixture.run(&["daemon", "start"]);
    assert_eq!(code, 0, "start failed: {err}");
    assert!(
        !stale.join("payload").exists(),
        "the sweep left a payload file"
    );
    assert!(
        !stale.join("logs").exists(),
        "the sweep left a logs directory"
    );
    assert!(
        stale.join("startup.lock").exists() && stale.join("daemon.lock").exists(),
        "the sweep deleted a lock file"
    );
}

#[test]
fn a_start_caches_the_image_it_runs() {
    let fixture = Fixture::new("cache");
    let (code, _, err) = fixture.run(&["daemon", "start"]);
    assert_eq!(code, 0, "start failed: {err}");
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_fastexec"));
    let build = sha256(&exe);
    let cached = fixture
        .runtime
        .join("bin")
        .join(format!("{build}{}", std::env::consts::EXE_SUFFIX));
    assert!(cached.exists(), "{} is missing", cached.display());
    assert_eq!(
        std::fs::metadata(&cached).unwrap().len(),
        std::fs::metadata(&exe).unwrap().len(),
        "the cached image is not the binary"
    );
}

#[test]
fn a_daemon_of_another_build_refuses_to_serve() {
    let fixture = Fixture::new("build");
    let zeros = "0".repeat(64);
    let (code, _, err) = fixture.run(&["daemon", "serve", &fixture.namespace.clone(), &zeros]);
    assert_eq!(code, 1, "a mismatched build served: {err}");
    assert!(
        err.contains(&zeros),
        "the error does not name the build: {err}"
    );
    assert!(
        err.contains("not build"),
        "the error does not say what happened: {err}"
    );
}

#[test]
fn two_namespaces_share_one_cached_image() {
    let runtime =
        std::env::temp_dir().join(format!("fastexec-daemon-{}-shared", std::process::id()));
    create_root(&runtime);
    let first = Fixture::sharing(&runtime, "sharedone");
    let second = Fixture::sharing(&runtime, "sharedtwo");

    let (code, out, err) = first.run(&["daemon", "start"]);
    assert_eq!(code, 0, "start failed: {err}");
    let first_epoch = first.epoch_of(&out);

    // Age the cached image, so the next start has to refresh it: the first daemon may already
    // have it mapped, and a mapped image still takes a touch on both platforms.
    let exe = PathBuf::from(env!("CARGO_BIN_EXE_fastexec"));
    let build = sha256(&exe);
    let cached = runtime
        .join("bin")
        .join(format!("{build}{}", std::env::consts::EXE_SUFFIX));
    assert!(cached.exists(), "{} is missing", cached.display());
    let aged = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
    set_modified(&cached, aged);

    // The second namespace finds the image cached and mapped, and still starts on its own.
    let (code, out, err) = second.run(&["daemon", "start"]);
    assert_eq!(code, 0, "the second start failed: {err}");
    let second_epoch = second.epoch_of(&out);
    assert_ne!(first_epoch, second_epoch, "two namespaces share one daemon");

    let refreshed = std::fs::metadata(&cached).unwrap().modified().unwrap();
    assert!(
        refreshed > aged + std::time::Duration::from_secs(1800),
        "the cached image was not refreshed: {refreshed:?}"
    );

    for fixture in [&first, &second] {
        let (code, status, err) = fixture.run(&["daemon", "status"]);
        assert_eq!(code, 0, "status failed: {err}");
        assert_eq!(Fixture::field(&status, "sessions"), "0", "{status}");
    }
    assert_eq!(
        std::fs::metadata(&cached).unwrap().len(),
        std::fs::metadata(&exe).unwrap().len()
    );

    for fixture in [&first, &second] {
        let (code, _, err) = fixture.run(&["daemon", "stop"]);
        assert_eq!(code, 0, "stop failed: {err}");
    }
    // The fixtures only borrow the directory, so it is removed once both are gone.
    drop((first, second));
    let _ = std::fs::remove_dir_all(&runtime);
}

/// Sets a file's modification time. On Windows a read-only handle cannot, so only the attribute
/// is opened, which a mapped image allows.
fn set_modified(path: &Path, when: std::time::SystemTime) {
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;

        // FILE_WRITE_ATTRIBUTES
        const WRITE_ATTRIBUTES: u32 = 0x0100;
        let file = std::fs::OpenOptions::new()
            .access_mode(WRITE_ATTRIBUTES)
            .open(path)
            .unwrap();
        file.set_modified(when).unwrap();
    }
    #[cfg(unix)]
    {
        let file = std::fs::File::open(path).unwrap();
        file.set_modified(when).unwrap();
    }
}

/// A runtime path longer than a Unix socket path allows still starts a daemon, and a dead
/// namespace's fallback socket is swept with the rest of its state.
#[cfg(unix)]
#[test]
fn a_long_runtime_path_falls_back_to_a_short_socket() {
    let runtime = std::env::temp_dir().join(format!(
        "{}-{}",
        "fastexec-daemon-with-a-long-path".repeat(4),
        std::process::id()
    ));
    create_root(&runtime);
    let uid = unsafe { libc::geteuid() };
    let short = PathBuf::from("/tmp").join(format!("fastexec-{uid}"));

    // A long temporary directory of its own, so a fallback built from it could not fit in a
    // socket path either: only the literal /tmp works.
    let tmpdir = runtime.join("a-temporary-directory-that-is-also-very-long");
    create_root(&tmpdir);
    let mut probe = Fixture::sharing(&runtime, "longscope");
    probe.tmpdir = Some(tmpdir);

    // The scope is a name, not a namespace: the CLI hashes it, so ask the CLI what it uses.
    let (_, out, _) = probe.run(&["daemon", "status"]);
    let namespace = out
        .trim_end()
        .trim_end_matches('.')
        .rsplit(' ')
        .next()
        .unwrap_or_else(|| panic!("no namespace in {out:?}"))
        .to_string();
    assert_eq!(namespace.len(), 16, "{namespace}");
    let socket = short.join(format!("{namespace}.sock"));

    // A namespace that died leaves its state and its fallback socket behind, before this start.
    let dead = runtime.join("ns").join("0123456789abcdef");
    std::fs::create_dir_all(&dead).unwrap();
    std::fs::write(dead.join("startup.lock"), b"").unwrap();
    std::fs::write(dead.join("daemon.lock"), b"").unwrap();
    let dead_socket = short.join("0123456789abcdef.sock");
    let _ = std::fs::create_dir_all(&short);
    std::fs::write(&dead_socket, b"").unwrap();
    let _ = std::fs::remove_file(&socket);

    let (code, out, err) = probe.run(&["daemon", "start"]);
    assert_eq!(code, 0, "start failed: {err}");
    let epoch = probe.epoch_of(&out);
    assert_eq!(epoch.len(), 26, "{out:?}");
    // The daemon is reachable over the fallback socket, under the same long temporary directory.
    let (code, status, err) = probe.run(&["daemon", "status"]);
    assert_eq!(code, 0, "status failed: {err}");
    assert_eq!(Fixture::field(&status, "epoch"), epoch, "{status}");
    assert!(
        socket.exists(),
        "the daemon did not fall back to {}",
        socket.display()
    );
    assert!(
        !dead_socket.exists(),
        "the dead namespace's socket was left behind"
    );

    let (code, _, err) = probe.run(&["daemon", "stop"]);
    assert_eq!(code, 0, "stop failed: {err}");
    drop(probe);
    let _ = std::fs::remove_dir_all(&runtime);
}

#[test]
fn the_daemon_leaves_the_directory_it_was_started_from() {
    let fixture = Fixture::new("leave-cwd");
    let starter = fixture.runtime.join("starter");
    create_root(&starter);
    let output = fixture
        .command(&["daemon", "start"])
        .current_dir(&starter)
        .output()
        .expect("the start CLI runs");
    let out = String::from_utf8_lossy(&output.stdout).into_owned();
    let err = String::from_utf8_lossy(&output.stderr).into_owned();
    assert_eq!(output.status.code(), Some(0), "start failed: {err}");
    if let Some(pid) = pid_of(&out) {
        fixture.pid.set(Some(pid));
    }
    // A Windows process holds its current directory open, so this fails unless the daemon left
    // it; the daemon runs on while its starter's directory goes.
    std::fs::remove_dir_all(&starter).expect("the starting directory is removable");
    let (code, status, err) = fixture.run(&["daemon", "status"]);
    assert_eq!(code, 0, "status failed: {err}");
    assert_eq!(
        Fixture::field(&status, "epoch"),
        fixture.epoch_of(&out),
        "{status}"
    );
}

#[test]
fn two_sessions_send_their_input_whole() {
    let fixture = Fixture::new("e2e-input");
    let mut first = Endpoint::start(&fixture);
    // Each read gets one session's input and then that session's key, or the two would mix.
    let command = r#"stty -echo; IFS= read -r a; IFS= read -r b; printf '<%s><%s>
' "$a" "$b""#;
    let (started, text, error) = first.call(json!({
        "action": "start",
        "command": command,
        "pty": true,
        "loginShell": false,
        "waitMs": 0
    }));
    assert!(!error, "{text}");
    let id = Endpoint::task_id(&started, &text);
    // A second endpoint is a second session: each has its own read gate, so only the task's
    // input gate keeps one session's input and keys together. Its first call connects, which is
    // done here so the two polls below are what overlap.
    let mut second = Endpoint::start(&fixture);
    let (listed, text, error) = second.call(json!({"action": "list"}));
    assert!(!error, "{text}");
    assert!(listed["tasks"].is_array(), "{listed}");
    let poll = |input: &str, key: &str| {
        json!({
            "action": "poll", "taskId": id, "input": input,
            "keys": [key, "Enter"], "keyDelayMs": 150, "waitMs": 30000,
        })
    };
    let first_call = first.request_later(
        "tools/call",
        json!({"name": "exec", "arguments": poll("aaaa-", "1")}),
    );
    let second_call = second.request_later(
        "tools/call",
        json!({"name": "exec", "arguments": poll("bbbb-", "2")}),
    );
    for (endpoint, call) in [(&mut first, first_call), (&mut second, second_call)] {
        let message = endpoint.response(call);
        assert!(message["result"]["isError"] != json!(true), "{message}");
    }
    let (out, text, error) = first.call(json!({"action": "poll", "taskId": id, "waitMs": 30000}));
    assert!(!error, "{text}");
    assert_eq!(out["state"], json!("exited"), "{text}");
    let log = std::fs::read_to_string(out["logPath"].as_str().unwrap()).unwrap();
    let whole = log.contains("<aaaa-1><bbbb-2>") || log.contains("<bbbb-2><aaaa-1>");
    assert!(whole, "the two inputs arrived whole: {log:?}");
    first.close();
    second.close();
}

#[test]
fn a_pending_watch_sees_another_sessions_resize() {
    let fixture = Fixture::new("e2e-watch-resize");
    let mut first = Endpoint::start(&fixture);
    // At 20 columns the row wraps, so the token is split across rows and the program's "filler"
    // text is what the screen shows. Widening the terminal rewraps the row and puts the token back
    // together on one row, which is the change the pending watch is waiting for. Nothing else about
    // the program changes: it prints once and then blocks.
    let command = "stty -echo; printf 'prefix-padding-18SPLITTOKENfiller-filler-filler\r\n'; while IFS= read -r line; do :; done";
    let (started, text, error) = first.call(json!({
        "action": "start",
        "command": command,
        "pty": true,
        "cols": 20,
        "rows": 30,
        "loginShell": false,
        "waitMs": 0
    }));
    assert!(!error, "{text}");
    let id = Endpoint::task_id(&started, &text);
    // The filler text is on the screen before the watch starts: read as a screen, not as a wait a
    // timeout would also satisfy, and with no row holding the whole token at this width.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (_, screen, error) = first.call(json!({
            "action": "poll", "taskId": id, "screen": true, "waitMs": 100
        }));
        assert!(!error, "the screen never arrived: {screen}");
        if screen.lines().any(|line| line.contains("filler")) {
            assert!(
                !screen.lines().any(|line| line.contains("SPLITTOKEN")),
                "the narrow screen splits the token: {screen}"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the filler text never reached the screen: {screen}"
        );
    }
    // The watch is pending when the other session resizes: the request is sent, then the endpoint
    // is left parked in its wait while a second connection widens the same task.
    let watch = first.request_later(
        "tools/call",
        json!({"name": "exec", "arguments": {
            "action": "poll", "taskId": id, "screen": true, "waitMs": 30000,
            "returnWhen": {"screenContains": ["SPLITTOKEN"]}
        }}),
    );
    std::thread::sleep(Duration::from_millis(200));
    let mut second = Endpoint::start(&fixture);
    let (_, listed, error) = second.call(json!({"action": "list"}));
    assert!(!error, "{listed}");
    assert!(listed.contains(" pty 20x30"), "{listed}");
    let (_, text, error) = second.call(json!({
        "action": "poll", "taskId": id, "cols": 60, "rows": 30, "waitMs": 0
    }));
    assert!(!error, "the resize failed: {text}");
    // The pending watch ends on the condition, not on its timeout, and the screen it reads holds the
    // token on one row again: the resize woke the wait and rewrapped the terminal.
    let message = first.response(watch);
    let (out, text, error) = Endpoint::answer(&message);
    assert!(!error, "{text}");
    assert_eq!(
        out["waitEndedBy"],
        json!({"condition": "screen_contains", "text": "SPLITTOKEN"}),
        "{text}"
    );
    assert!(
        text.lines().any(|line| line.contains("SPLITTOKEN")),
        "the wider terminal holds the token on one row: {text}"
    );
    let (_, listed, error) = second.call(json!({"action": "list"}));
    assert!(!error, "{listed}");
    assert!(listed.contains(" pty 60x30"), "the task is 60x30: {listed}");
    first.close();
    second.close();
}

#[test]
fn a_closed_endpoint_leaves_no_session_behind() {
    let fixture = Fixture::new("bye");
    let mut endpoint = Endpoint::start(&fixture);
    // A call is what starts the daemon and attaches this session to it.
    let (listed, text, error) = endpoint.call(json!({"action": "list"}));
    assert!(!error, "{text}");
    assert!(listed["tasks"].is_array(), "{listed}");
    endpoint.close();
    // The endpoint exits after the daemon reads its Bye, so one status is enough.
    let (code, status, err) = fixture.run(&["daemon", "status"]);
    assert_eq!(code, 0, "status failed: {err}");
    assert_eq!(Fixture::field(&status, "sessions"), "0", "{status}");
}

#[test]
fn a_task_outlives_the_endpoint_that_started_it() {
    let fixture = Fixture::new("e2e-outlive");
    let mut first = Endpoint::start(&fixture);
    let (started, text, error) = first.call(json!({
        "action": "start",
        "command": "echo one; sleep 60",
        "waitMs": 0
    }));
    assert!(!error, "{text}");
    let id = Endpoint::task_id(&started, &text);
    // The session this endpoint runs as, and the call that started the task.
    let (listed, text, error) = first.call(json!({"action": "list"}));
    assert!(!error, "{text}");
    let entry = listed["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["taskId"] == json!(id))
        .unwrap_or_else(|| panic!("no task in {listed}"))
        .clone();
    assert_eq!(entry["callId"], json!(1), "{listed}");
    let session = entry["session"]
        .as_str()
        .expect("a started task has a session")
        .to_string();
    assert_eq!(session.len(), 26, "{session}");
    first.close();

    let mut second = Endpoint::start(&fixture);
    let (polled, text, error) =
        second.call(json!({"action": "poll", "taskId": id, "waitMs": 5000}));
    assert!(!error, "{text}");
    assert!(text.contains("one"), "{text}");
    assert_eq!(polled["state"], json!("running"), "{polled}");

    let (listed, text, error) = second.call(json!({"action": "list"}));
    assert!(!error, "{text}");
    let entry = listed["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["taskId"] == json!(id))
        .unwrap_or_else(|| panic!("the second endpoint does not see the task: {listed}"));
    assert_eq!(entry["session"], json!(session), "{listed}");
    assert_eq!(entry["callId"], json!(1), "{listed}");
    assert!(text.contains("call 1"), "{text}");

    let (killed, text, error) = second.call(json!({"action": "kill", "taskId": id}));
    assert!(!error, "{text}");
    assert_ne!(killed["state"], json!("running"), "{killed}");
    second.close();
}

#[test]
fn two_endpoints_read_one_task_through_independent_cursors() {
    let fixture = Fixture::new("e2e-cursors");
    let mut first = Endpoint::start(&fixture);
    let (started, text, error) = first.call(json!({
        "action": "start",
        "command": "IFS= read -r line; printf 'x%sy\\n' \"$line\"; sleep 60",
        "waitMs": 0
    }));
    assert!(!error, "{text}");
    let id = Endpoint::task_id(&started, &text);
    // The poll supplies the input that makes the marker, and waits for it: no output can come
    // before this call, and a slow start is a slow answer rather than a miss.
    let (_, seen_by_first, error) = first.call(json!({
        "action": "poll",
        "taskId": id,
        "waitMs": 30000,
        "input": "42\n",
        "returnWhen": {"outputContains": ["x42y"]}
    }));
    assert!(!error, "{seen_by_first}");
    assert!(seen_by_first.contains("x42y"), "{seen_by_first}");

    let mut second = Endpoint::start(&fixture);
    let (_, seen_by_second, error) =
        second.call(json!({"action": "poll", "taskId": id, "waitMs": 30000}));
    assert!(!error, "{seen_by_second}");
    assert!(
        seen_by_second.contains("x42y"),
        "the second endpoint missed output the first one read: {seen_by_second}"
    );

    // Each cursor is its own: what one endpoint read is gone for it, and only for it.
    let (_, again_by_first, error) =
        first.call(json!({"action": "poll", "taskId": id, "waitMs": 0}));
    assert!(!error, "{again_by_first}");
    assert!(!again_by_first.contains("x42y"), "{again_by_first}");
    let (_, again_by_second, error) =
        second.call(json!({"action": "poll", "taskId": id, "waitMs": 0}));
    assert!(!error, "{again_by_second}");
    assert!(!again_by_second.contains("x42y"), "{again_by_second}");

    second.call(json!({"action": "kill", "taskId": id}));
    second.close();
    first.close();
}

#[test]
fn a_kill_after_ms_survives_the_endpoint_that_started_the_task() {
    let fixture = Fixture::new("e2e-lifetime");
    let mut first = Endpoint::start(&fixture);
    let (started, text, error) = first.call(json!({
        "action": "start",
        "command": "sleep 60",
        "killAfterMs": 3000,
        "waitMs": 0
    }));
    assert!(!error, "{text}");
    let id = Endpoint::task_id(&started, &text);
    first.close();

    let mut second = Endpoint::start(&fixture);
    let (polled, text, error) =
        second.call(json!({"action": "poll", "taskId": id, "waitMs": 10000}));
    assert!(!error, "{text}");
    assert_ne!(polled["state"], json!("running"), "{polled}");
    assert_eq!(polled["lifetimeExpired"], json!(true), "{polled}");
    second.close();
}

#[test]
fn a_lost_call_after_a_daemon_stop_is_reported_and_the_next_call_starts_a_daemon() {
    let fixture = Fixture::new("e2e-restart");
    let mut endpoint = Endpoint::start(&fixture);
    let (started, text, error) = endpoint.call(json!({
        "action": "start",
        "command": "sleep 60",
        "waitMs": 0
    }));
    assert!(!error, "{text}");
    let old = Endpoint::task_id(&started, &text);

    // The poll is waiting on the daemon when the daemon goes away. Its progress notification is
    // what proves the call was dispatched, not merely sent.
    let waiting = endpoint.request_later(
        "tools/call",
        json!({
            "name": "exec",
            "arguments": {"action": "poll", "taskId": old, "waitMs": 30000},
            "_meta": {"progressToken": "p1"}
        }),
    );
    let progress = endpoint.next_matching("progress from the waiting poll", |message| {
        message["method"] == json!("notifications/progress")
    });
    assert_eq!(
        progress["params"]["progressToken"],
        json!("p1"),
        "{progress}"
    );

    let (code, out, err) = fixture.run(&["daemon", "stop"]);
    assert_eq!(code, 0, "stop failed: {err}");
    assert!(out.contains("Stopped"), "{out}");

    // The call that was in flight may have run, so it is reported, never replayed.
    let message = endpoint.response(waiting);
    let (_, text, error) = Endpoint::answer(&message);
    assert!(error, "{text}");
    assert!(text.contains("closed during poll call 2"), "{text}");
    assert!(text.contains("request 3"), "{text}");
    assert!(text.contains("session "), "{text}");
    assert!(text.contains("the operation may have run"), "{text}");

    // The next call on the same endpoint starts a daemon and succeeds, with no new initialize.
    let (listed, text, error) = endpoint.call(json!({"action": "list"}));
    assert!(!error, "{text}");
    assert_eq!(listed["tasks"], json!([]), "{listed}");

    // The old ID belongs to the daemon that is gone, and says so.
    let (_, text, error) = endpoint.call(json!({"action": "poll", "taskId": old, "waitMs": 0}));
    assert!(error, "{text}");
    assert!(
        text.contains("it comes from an earlier fastexec daemon"),
        "{text}"
    );
    assert!(text.contains("daemon restart"), "{text}");
    endpoint.close();
}

#[test]
fn a_call_over_one_mib_is_refused_before_it_reaches_a_daemon() {
    let fixture = Fixture::new("e2e-limit");
    let mut endpoint = Endpoint::start(&fixture);
    let command = "x".repeat(2 << 20);
    let (_, text, error) = endpoint.call(json!({
        "action": "start",
        "command": command,
        "waitMs": 0
    }));
    assert!(error, "{text}");
    assert!(text.contains("calls are limited to 1 MiB"), "{text}");
    // Nothing was sent, so nothing started a daemon.
    let (code, out, err) = fixture.run(&["daemon", "status"]);
    assert_eq!(code, 0, "status failed: {err}");
    assert!(out.starts_with("No fastexec daemon"), "{out}");
    endpoint.close();
}

#[test]
fn an_invalid_daemon_mode_is_refused_at_startup() {
    let fixture = Fixture::new("e2e-invalid");
    let output = fixture
        .command(&[])
        .env("FASTEXEC_DAEMON", "bogus")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("invalid FASTEXEC_DAEMON \"bogus\"; expected on or off"),
        "{stderr}"
    );
}

#[test]
fn initialize_and_tools_list_answer_with_no_daemon() {
    let fixture = Fixture::new("e2e-lazy");
    let mut endpoint = Endpoint::start(&fixture);
    let listed = endpoint.request("tools/list", json!({}));
    let tools = listed["result"]["tools"].as_array().unwrap();
    let mut names: Vec<&str> = tools
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["edit", "exec", "read", "write"], "{listed}");
    // A stdio endpoint has no route to serve a URL from, so `transfer` is not even a tool here.
    let refused = endpoint.request(
        "tools/call",
        json!({"name": "transfer", "arguments": {"path": "notes/a.txt"}}),
    );
    assert_eq!(
        refused["error"]["message"],
        json!("tool not found"),
        "{refused}"
    );
    let (code, out, err) = fixture.run(&["daemon", "status"]);
    assert_eq!(code, 0, "status failed: {err}");
    assert!(out.starts_with("No fastexec daemon"), "{out}");
    endpoint.close();
}

#[test]
fn file_tools_run_in_the_endpoints_directory_and_not_the_daemons() {
    let fixture = Fixture::new("files");
    let dir = std::env::temp_dir().join(format!("fastexec-daemon-files-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    // The daemon runs first, with the launch context of the test's own directory. A file call
    // must then resolve against the endpoint's directory, not against that captured one.
    let (code, out, err) = fixture.run(&["daemon", "start"]);
    assert_eq!(code, 0, "{out}{err}");
    let mut endpoint = Endpoint::start_with(&fixture, |command| {
        command.current_dir(&dir);
    });
    let (_, written, error) = endpoint.file(
        "write",
        json!({"path": "notes/a.txt", "content": "one\ntwo\n"}),
    );
    assert!(!error, "{written}");
    assert_eq!(written, "Successfully wrote to notes/a.txt");
    assert_eq!(
        std::fs::read(dir.join("notes/a.txt")).unwrap(),
        b"one\ntwo\n"
    );
    let (_, read, error) = endpoint.file("read", json!({"path": "notes/a.txt"}));
    assert!(!error, "{read}");
    assert_eq!(read, "one\ntwo\n");
    let (_, edited, error) = endpoint.file(
        "edit",
        json!({"path": "notes/a.txt", "edits": [{"oldText": "two", "newText": "TWO"}]}),
    );
    assert!(!error, "{edited}");
    assert_eq!(
        edited,
        "Successfully replaced 1 block(s) in notes/a.txt.\nedits[0]: line 2"
    );
    let (_, read, _) = endpoint.file("read", json!({"path": "notes/a.txt"}));
    assert_eq!(read, "one\nTWO\n");
    // A missing file is an error result, not a lost call.
    let (_, text, error) = endpoint.file("read", json!({"path": "notes/missing.txt"}));
    assert!(error, "{text}");
    assert!(
        text.starts_with("Cannot read notes/missing.txt: "),
        "{text}"
    );
    endpoint.close();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_image_read_crosses_the_daemon_link() {
    let fixture = Fixture::new("image");
    let dir = std::env::temp_dir().join(format!("fastexec-daemon-image-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let image =
        image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(8, 6, image::Rgb([7, 8, 9])));
    let mut png = Vec::new();
    image
        .write_to(&mut std::io::Cursor::new(&mut png), image::ImageFormat::Png)
        .unwrap();
    std::fs::write(dir.join("shot.png"), &png).unwrap();
    let (code, out, err) = fixture.run(&["daemon", "start"]);
    assert_eq!(code, 0, "{out}{err}");
    let mut endpoint = Endpoint::start_with(&fixture, |command| {
        command.current_dir(&dir);
    });
    let message = endpoint.request(
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
    endpoint.close();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_cancelled_call_does_not_disturb_another_call_on_the_same_endpoint() {
    let fixture = Fixture::new("e2e-cancel");
    let mut endpoint = Endpoint::start(&fixture);
    let (started, text, error) = endpoint.call(json!({
        "action": "start",
        "command": "sleep 60",
        "waitMs": 0
    }));
    assert!(!error, "{text}");
    let id = Endpoint::task_id(&started, &text);

    // One call waits on the daemon while another one runs beside it on the same connection.
    let waiting = endpoint.request_later(
        "tools/call",
        json!({
            "name": "exec",
            "arguments": {"action": "poll", "taskId": id, "waitMs": 30000},
            "_meta": {"progressToken": "p1"}
        }),
    );
    let progress = endpoint.next_matching("progress from the waiting poll", |message| {
        message["method"] == json!("notifications/progress")
    });
    assert_eq!(
        progress["params"]["progressToken"],
        json!("p1"),
        "{progress}"
    );

    // The poll is still pending, and another call answers beside it. A serial proxy that ran one
    // call at a time would have to answer the poll first, and that answer is what this fails on.
    let beside = endpoint.request_later(
        "tools/call",
        json!({"name": "exec", "arguments": {"action": "list"}}),
    );
    let listed = endpoint.next_matching("the list answer beside the waiting poll", |message| {
        if message["id"] == json!(waiting) {
            panic!("the waiting poll answered before the list: {message}");
        }
        message["id"] == json!(beside)
    });
    let (_, text, error) = Endpoint::answer(&listed);
    assert!(!error, "{text}");

    // The client cancels the wait; rmcp ends that call's token and drops its answer, so nothing
    // arrives for that ID, and the connection is free for the next call.
    endpoint.send(json!({
        "jsonrpc": "2.0",
        "method": "notifications/cancelled",
        "params": {"requestId": waiting, "reason": "test"}
    }));
    let listing = endpoint.request_later(
        "tools/call",
        json!({"name": "exec", "arguments": {"action": "list"}}),
    );
    let listed = endpoint.next_matching("the list answer beside the cancelled call", |message| {
        message["id"] == json!(listing)
    });
    let (_, text, error) = Endpoint::answer(&listed);
    assert!(!error, "{text}");

    // Cancelling the wait did not touch the task.
    let (polled, text, error) = endpoint.call(json!({"action": "poll", "taskId": id, "waitMs": 0}));
    assert!(!error, "{text}");
    assert_eq!(polled["state"], json!("running"), "{polled}");
    endpoint.call(json!({"action": "kill", "taskId": id}));
    endpoint.close();
}

#[test]
fn each_endpoint_starts_its_tasks_with_its_own_environment() {
    let fixture = Fixture::new("e2e-env");
    let mut first = Endpoint::start_with(&fixture, |command| {
        command.env("FASTEXEC_TEST_MARKER", "from-the-first");
    });
    let mut second = Endpoint::start_with(&fixture, |command| {
        command.env("FASTEXEC_TEST_MARKER", "from-the-second");
    });

    let (started, text, error) = first.call(json!({
        "action": "start",
        "command": "echo \"$FASTEXEC_TEST_MARKER\"; sleep 60",
        "waitMs": 10000
    }));
    assert!(!error, "{text}");
    assert!(text.contains("from-the-first"), "{text}");
    let id = Endpoint::task_id(&started, &text);

    // The task keeps the environment of the endpoint that started it, not of this one.
    let (_, seen, error) = second.call(json!({
        "action": "poll",
        "taskId": id,
        "waitMs": 0
    }));
    assert!(!error, "{seen}");
    assert!(!seen.contains("from-the-second"), "{seen}");

    let (_, text, error) = second.call(json!({
        "action": "start",
        "command": "echo \"$FASTEXEC_TEST_MARKER\"; sleep 60",
        "waitMs": 10000
    }));
    assert!(!error, "{text}");
    assert!(text.contains("from-the-second"), "{text}");

    let (_, text, error) = second.call(json!({"action": "kill", "taskId": id}));
    assert!(!error, "{text}");
    second.close();
    first.close();
}

/// The token a foreground daemon needs before it will serve HTTP at all.
const TOKEN: &str = "0123456789abcdef";

/// A `daemon run` child: the daemon in its own process, with the stderr its readiness line
/// arrives on. It is declared after the fixture in every test, so it ends its child before the
/// fixture removes the runtime directory. Its own child handle is the only authority over it: the
/// detached-process ownership check deliberately does not recognize this executable.
struct Foreground {
    child: Child,
    /// The line that says the daemon is ready, already read.
    ready: String,
}

impl Foreground {
    fn start(fixture: &Fixture, args: &[&str]) -> Foreground {
        let mut command = fixture.command(&["daemon", "run"]);
        command
            .args(args)
            .env("FASTEXEC_HTTP_TOKEN", TOKEN)
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let mut child = command.spawn().expect("spawn fastexec daemon run");
        let stderr = child.stderr.take().unwrap();
        // The guard owns the child from the moment it exists, so a startup that fails, or that
        // never reaches its readiness line, still ends it.
        let mut running = Foreground {
            child,
            ready: String::new(),
        };
        running.ready = readiness_line(stderr);
        running
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    /// The address the readiness line names, when the daemon serves HTTP.
    fn http(&self) -> Option<String> {
        let at = self.ready.find(" http ")?;
        Some(self.ready[at + " http ".len()..].trim().to_string())
    }

    /// Waits for the daemon to end, for at most `within`.
    fn wait_exit(&mut self, within: Duration) -> Option<std::process::ExitStatus> {
        let deadline = Instant::now() + within;
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return Some(status);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Foreground {
    fn drop(&mut self) {
        // The child handle ends whatever the test left running, so the fixture's runtime
        // directory is removable afterwards.
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

/// Reads a daemon's stderr until its readiness line. The readiness line is the barrier: nothing
/// else is claimed about that daemon before it arrives, and a daemon that never gets there is a
/// failure of the test that started it.
fn readiness_line(stderr: std::process::ChildStderr) -> String {
    let (sender, lines) = channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            if sender.send(line).is_err() {
                break;
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        match lines.recv_timeout(left) {
            Ok(line) if line.starts_with("fastexec daemon ") => return line,
            Ok(_) => {}
            Err(error) => panic!("no readiness line on stderr: {error:?}"),
        }
    }
}

/// Runs a `daemon run` that must be refused, and returns its exit code and stderr. The child is
/// owned and waited for with a bound, so a `run` that serves instead of refusing cannot hang the
/// test: it is ended and reported.
fn run_refused(fixture: &Fixture, args: &[&str]) -> (i32, String) {
    let mut command = fixture.command(&["daemon", "run"]);
    command
        .args(args)
        .env("FASTEXEC_HTTP_TOKEN", TOKEN)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn fastexec daemon run");
    let stderr = child.stderr.take().unwrap();
    let reading = std::thread::spawn(move || {
        BufReader::new(stderr)
            .lines()
            .map_while(Result::ok)
            .collect::<Vec<String>>()
            .join("\n")
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("daemon run {args:?} did not refuse: it was still running");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    (status.code().unwrap_or(-1), reading.join().unwrap())
}

/// Whether a process is still running. A zombie has ended, so only a live process counts.
fn is_alive(pid: u32) -> bool {
    #[cfg(target_os = "macos")]
    {
        // A zombie has ended. libproc reports its state; /proc does not exist here.
        let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
        // SAFETY: proc_pidinfo fills the struct for this pid; the size is the struct's.
        let size = unsafe {
            libc::proc_pidinfo(
                pid as i32,
                libc::PROC_PIDTBSDINFO,
                0,
                info.as_mut_ptr().cast(),
                std::mem::size_of::<libc::proc_bsdinfo>() as i32,
            )
        };
        if size as usize == std::mem::size_of::<libc::proc_bsdinfo>() {
            // SAFETY: proc_pidinfo filled the whole struct.
            return unsafe { info.assume_init() }.pbi_status != libc::SZOMB;
        }
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat"))
            && let Some(rest) = stat.rsplit(')').next()
            && let Some(state) = rest.split_whitespace().next()
        {
            return state != "Z";
        }
        unsafe { libc::kill(pid as i32, 0) == 0 }
    }
    #[cfg(windows)]
    {
        let script =
            format!("if (Get-Process -Id {pid} -ErrorAction SilentlyContinue) {{ 'alive' }}");
        let Ok(output) = Command::new("pwsh")
            .args(["-NoProfile", "-NonInteractive", "-Command", &script])
            .output()
        else {
            return false;
        };
        String::from_utf8_lossy(&output.stdout).contains("alive")
    }
}

/// A task that names its own process and then waits to be killed. Git Bash's `$$` is an MSYS
/// number, not a native PID, so on Windows the number comes from pwsh's own `$PID`.
#[cfg(unix)]
const NAMED_TASK: &str = "printf 'PID %s READY\\n' $$; exec sleep 60";
#[cfg(windows)]
const NAMED_TASK: &str =
    "pwsh -NoProfile -NonInteractive -Command 'Write-Output \"PID $PID READY\"; Start-Sleep 60'";

/// The value of a `key value` word in a line.
fn word_after(text: &str, key: &str) -> Option<String> {
    let words: Vec<&str> = text.split_whitespace().collect();
    let at = words.iter().position(|word| *word == key)?;
    words.get(at + 1).map(|word| (*word).to_string())
}

/// The number a task printed after `PID `.
fn printed_pid(text: &str) -> u32 {
    text.split_whitespace()
        .skip_while(|word| *word != "PID")
        .nth(1)
        .and_then(|number| number.trim().parse().ok())
        .unwrap_or_else(|| panic!("no PID in {text:?}"))
}

#[test]
fn a_foreground_daemon_announces_readiness_and_a_status_finds_it() {
    let fixture = Fixture::new("fg-ready");
    let mut running = Foreground::start(&fixture, &["--http", "127.0.0.1:0"]);
    // The readiness line names this process, this namespace, and the address actually bound.
    assert_eq!(
        pid_of(&running.ready),
        Some(running.pid()),
        "{}",
        running.ready
    );
    let namespace = word_after(&running.ready, "namespace").expect("a namespace");
    assert!(!namespace.is_empty(), "{}", running.ready);
    let http = running.http().expect("an http address");
    assert!(http.starts_with("127.0.0.1:"), "{http}");
    assert_ne!(http, "127.0.0.1:0", "the bound address, not the asked one");
    let epoch = fixture.epoch_of(&running.ready);

    let (code, status, err) = fixture.run(&["daemon", "status"]);
    assert_eq!(code, 0, "{status:?}{err}");
    assert_eq!(Fixture::field(&status, "epoch"), epoch);
    assert_eq!(Fixture::field(&status, "pid"), running.pid().to_string());
    assert_eq!(Fixture::field(&status, "http"), http);

    // A task runs on that daemon, through an endpoint that would otherwise start one of its own.
    let mut endpoint = Endpoint::start(&fixture);
    let (started, text, error) = endpoint.call(json!({
        "action": "start",
        "command": "sleep 60",
        "waitMs": 0
    }));
    assert!(!error, "{text}");
    assert_eq!(started["state"], json!("running"), "{started}");
    let id = Endpoint::task_id(&started, &text);
    // The same daemon served it: no second one was started behind the endpoint.
    let (_, after, err) = fixture.run(&["daemon", "status"]);
    assert_eq!(
        Fixture::field(&after, "pid"),
        running.pid().to_string(),
        "{err}"
    );
    assert_eq!(Fixture::field(&after, "tasks"), "1");
    endpoint.call(json!({"action": "kill", "taskId": id}));
    endpoint.close();

    // A second daemon in the same namespace is refused, not waited for, and it names the
    // namespace the ready line named: both were derived from this fixture's own environment.
    let (code, err) = run_refused(&fixture, &[]);
    assert_eq!(code, 1, "{err}");
    assert!(
        err.contains(&format!("another daemon owns namespace {namespace}")),
        "the running daemon is not reused and not replaced: {err}"
    );
    assert_eq!(running.wait_exit(Duration::from_secs(1)), None);
}

#[test]
fn daemon_run_refuses_an_idle_exit() {
    let fixture = Fixture::new("fg-idle");
    for value in ["on", "off"] {
        let (code, err) = run_refused(&fixture, &["--idle-exit", value]);
        assert_eq!(code, 1, "--idle-exit {value}: {err}");
        assert!(err.contains("usage: fastexec daemon"), "{err}");
        assert!(err.contains("run [--http <addr>]"), "{err}");
    }
    // Nothing was started by a refused command.
    let (_, status, _) = fixture.run(&["daemon", "status"]);
    assert!(status.starts_with("No fastexec daemon"), "{status}");
}

#[test]
fn a_stopped_foreground_daemon_ends_and_takes_its_task_with_it() {
    let fixture = Fixture::new("fg-stop");
    let mut running = Foreground::start(&fixture, &[]);
    assert_eq!(
        pid_of(&running.ready),
        Some(running.pid()),
        "{}",
        running.ready
    );
    assert_eq!(running.http(), None, "{}", running.ready);

    let mut endpoint = Endpoint::start(&fixture);
    // The wait belongs to the start call itself: its own reply may already carry the output, and
    // a later read would only see what this one had not shown. READY follows the whole number.
    let (started, text, error) = endpoint.call(json!({
        "action": "start",
        "command": NAMED_TASK,
        "waitMs": 30000,
        "returnWhen": {"outputContains": ["READY"]}
    }));
    assert!(!error, "{text}");
    assert_eq!(started["state"], json!("running"), "{started}");
    let _id = Endpoint::task_id(&started, &text);
    let task = printed_pid(&text);
    assert!(
        is_alive(task),
        "the task runs before the daemon stops: {text}"
    );
    endpoint.close();

    // A signal takes the same path as `daemon stop`. Sending a console control event to a child
    // needs a console of its own, which this harness has not, so Windows stops it the CLI way.
    #[cfg(unix)]
    {
        assert_eq!(
            unsafe { libc::kill(running.pid() as i32, libc::SIGTERM) },
            0
        );
    }
    #[cfg(windows)]
    {
        let (code, out, err) = fixture.run(&["daemon", "stop"]);
        assert_eq!(code, 0, "{out}{err}");
        assert!(out.contains("Stopped"), "{out}");
    }
    let status = running
        .wait_exit(Duration::from_secs(60))
        .expect("the daemon exits");
    assert_eq!(status.code(), Some(0), "a signalled daemon exits cleanly");
    // The task it was running went with it.
    let deadline = Instant::now() + Duration::from_secs(30);
    while is_alive(task) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(!is_alive(task), "the task outlived the daemon that ran it");
}
