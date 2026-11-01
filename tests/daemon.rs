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
        #[cfg(unix)]
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
            json!({"name": "fastexec", "arguments": arguments}),
        );
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
        json!({"name": "fastexec", "arguments": poll("aaaa-", "1")}),
    );
    let second_call = second.request_later(
        "tools/call",
        json!({"name": "fastexec", "arguments": poll("bbbb-", "2")}),
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
            "name": "fastexec",
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
    assert_eq!(tools.len(), 1, "{listed}");
    assert_eq!(tools[0]["name"], json!("fastexec"), "{listed}");
    let (code, out, err) = fixture.run(&["daemon", "status"]);
    assert_eq!(code, 0, "status failed: {err}");
    assert!(out.starts_with("No fastexec daemon"), "{out}");
    endpoint.close();
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
            "name": "fastexec",
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
        json!({"name": "fastexec", "arguments": {"action": "list"}}),
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
        json!({"name": "fastexec", "arguments": {"action": "list"}}),
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
