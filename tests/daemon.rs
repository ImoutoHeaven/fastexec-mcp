//! The daemon as a process: started, inspected, and stopped through the CLI, in a runtime
//! directory of the test's own; and the MCP endpoint in `on` mode, which forwards every call to
//! that daemon.

mod support;
#[path = "support/transfer_http.rs"]
mod transfer_http;

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
    #[cfg(unix)]
    {
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
        builder.create(path).unwrap();
    }
    #[cfg(windows)]
    support::create_private_runtime(path);
}

impl Fixture {
    fn new(tag: &str) -> Fixture {
        #[cfg(target_os = "freebsd")]
        support::freebsd::init();
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
        #[cfg(target_os = "freebsd")]
        support::freebsd::init();
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
        support::scrub_fastexec_env(&mut command);
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

    /// Ends one process by PID, and only when it is this fixture's own daemon.
    fn end_own_daemon(&self, pid: u32) -> bool {
        #[cfg(target_os = "freebsd")]
        {
            support::freebsd::end_owned_process(pid, &self.runtime.join("bin"))
        }
        #[cfg(not(target_os = "freebsd"))]
        {
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
            #[cfg(target_os = "freebsd")]
            if let Some(pid) = self.pid.get() {
                let _ = support::freebsd::reap_stopped(pid);
            }
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
            .env("FASTEXEC_DAEMON", "on")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let mut child = command.spawn().expect("spawn fastexec");
        let stdout = child.stdout.take().unwrap();
        let (sender, lines) = channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                match serde_json::from_str(&line) {
                    Ok(value) => {
                        let _ = sender.send(value);
                    }
                    Err(error) => {
                        let _ = sender.send(
                            json!({"__stdout_error": format!("invalid JSON: {error}: {line:?}")}),
                        );
                        return;
                    }
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
            assert!(
                message["__stdout_error"].is_null(),
                "{}",
                message["__stdout_error"]
            );
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
    assert_eq!(epoch.len(), 8, "{epoch}");
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
    #[cfg(target_os = "freebsd")]
    let socket = short.join(&namespace).join("sock");
    #[cfg(not(target_os = "freebsd"))]
    let socket = short.join(format!("{namespace}.sock"));

    // A namespace that died leaves its state and its fallback socket behind, before this start.
    let mut random = [0; 8];
    getrandom::fill(&mut random).unwrap();
    let dead_namespace = format!("{:016x}", u64::from_ne_bytes(random));
    let dead = runtime.join("ns").join(&dead_namespace);
    std::fs::create_dir_all(&dead).unwrap();
    std::fs::write(dead.join("startup.lock"), b"").unwrap();
    std::fs::write(dead.join("daemon.lock"), b"").unwrap();
    #[cfg(target_os = "freebsd")]
    let dead_socket = {
        use std::os::unix::fs::DirBuilderExt;
        let private = short.join(&dead_namespace);
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&private)
            .unwrap();
        private.join("sock")
    };
    #[cfg(not(target_os = "freebsd"))]
    let dead_socket = short.join(format!("{dead_namespace}.sock"));
    std::fs::File::create_new(&dead_socket).unwrap();
    let _ = std::fs::remove_file(&socket);

    let (code, out, err) = probe.run(&["daemon", "start"]);
    assert_eq!(code, 0, "start failed: {err}");
    let epoch = probe.epoch_of(&out);
    assert_eq!(epoch.len(), 8, "{out:?}");
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
    #[cfg(target_os = "freebsd")]
    assert!(
        !dead_socket.parent().unwrap().exists(),
        "the dead fallback namespace directory was left behind"
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
    let (_, listed, error) = second.call(json!({"action": "list", "scope": "all"}));
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
        .find(|entry| entry["taskId"] == json!(id.split_once('.').unwrap().0))
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
        .find(|entry| entry["taskId"] == json!(id.split_once('.').unwrap().0))
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
        "eof": false,
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
fn a_call_over_three_mib_is_refused_before_it_reaches_a_daemon() {
    let fixture = Fixture::new("e2e-limit");
    let mut endpoint = Endpoint::start(&fixture);
    let command = "x".repeat((3 << 20) + 1024);
    let (_, text, error) = endpoint.call(json!({
        "action": "start",
        "command": command,
        "waitMs": 0
    }));
    assert!(error, "{text}");
    assert!(text.contains("calls are limited to 3 MiB"), "{text}");
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
    // `transfer` is off unless `FASTEXEC_TRANSFER=true`, so it is not listed by default.
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
    #[cfg(unix)]
    let base_dir = std::fs::canonicalize(&dir).unwrap();
    #[cfg(windows)]
    let base_dir = dir.clone();
    let expected_note = base_dir
        .join("notes/a.txt")
        .to_string_lossy()
        .replace('\\', "/");
    let expected_missing = base_dir
        .join("notes/missing.txt")
        .to_string_lossy()
        .replace('\\', "/");
    let (_, written, error) = endpoint.file(
        "write",
        json!({"path": "notes/a.txt", "content": "one\ntwo\n"}),
    );
    assert!(!error, "{written}");
    assert_eq!(written, format!("Successfully wrote to {expected_note}"));
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
        format!("Successfully replaced 1 block(s) in {expected_note}.\nedits[0]: line 2")
    );
    let (_, read, _) = endpoint.file("read", json!({"path": "notes/a.txt"}));
    assert_eq!(read, "one\nTWO\n");
    // A missing file is an error result, not a lost call.
    let (_, text, error) = endpoint.file("read", json!({"path": "notes/missing.txt"}));
    assert!(error, "{text}");
    assert!(
        text.starts_with(&format!("Cannot read {expected_missing}: ")),
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
    #[cfg(target_os = "freebsd")]
    {
        support::freebsd::is_alive(pid)
    }
    #[cfg(all(unix, not(any(target_os = "macos", target_os = "freebsd"))))]
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
fn daemon_usage_errors_name_the_offending_argument() {
    let fixture = Fixture::new("cli-usage");
    for (args, expected) in [
        (&["daemon"][..], "expected a daemon subcommand"),
        (&["daemon", "extra"], "unknown argument extra"),
        (&["daemon", "status", "extra"], "unknown argument extra"),
        (
            &["daemon", "stop", "--http", "127.0.0.1:1"],
            "unknown argument --http",
        ),
        (
            &["daemon", "start", "--extra", "value"],
            "unknown argument --extra",
        ),
        (
            &["daemon", "start", "--http"],
            "--http needs an address (host:port)",
        ),
        (
            &["daemon", "start", "--http", "typo"],
            "--http typo is not an address (host:port)",
        ),
        (
            &["daemon", "start", "--idle-exit", "maybe"],
            "--idle-exit maybe must be on or off",
        ),
        (
            &["daemon", "start", "--idle-exit", "on", "--idle-exit", "off"],
            "unknown argument --idle-exit",
        ),
    ] {
        let (code, out, err) = fixture.run(args);
        assert_eq!(code, 2, "{args:?}: {err}");
        assert!(out.is_empty(), "{args:?}: {out}");
        let mut lines = err.lines();
        assert_eq!(
            lines.next().unwrap(),
            format!("fastexec: {expected}"),
            "{args:?}"
        );
        assert!(
            lines.next().unwrap().starts_with("usage: fastexec daemon"),
            "{err}"
        );
    }
}

#[test]
fn daemon_run_refuses_an_idle_exit() {
    let fixture = Fixture::new("fg-idle");
    for value in ["on", "off"] {
        let (code, err) = run_refused(&fixture, &["--idle-exit", value]);
        assert_eq!(code, 2, "--idle-exit {value}: {err}");
        assert_eq!(
            err.lines().next(),
            Some("fastexec: unknown argument --idle-exit"),
            "{err}"
        );
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

#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_fixture_fallback_cleanup_owns_its_daemon() {
    let fixture = Fixture::new("freebsd-fallback");
    let (code, out, err) = fixture.run(&["daemon", "start"]);
    assert_eq!(code, 0, "{out}{err}");
    let pid = pid_of(&out).unwrap();
    assert!(fixture.owns_process(pid));
    assert!(!fixture.owns_process(std::process::id()));
    assert!(fixture.end_own_daemon(pid));
    assert!(!is_alive(pid));
}

#[test]
fn two_endpoints_on_one_daemon_keep_independent_yolo_annotations() {
    let fixture = Fixture::new("yolo-endpoints");
    let mut yolo = Endpoint::start_with(&fixture, |command| {
        command.env("FASTEXEC_YOLO", "true");
    });
    let listed = yolo.request("tools/list", json!({}));
    let tools = listed["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 4, "{listed}");
    for (name, title) in [
        ("edit", "Edit file"),
        ("exec", "Run bash command"),
        ("read", "Read file"),
        ("write", "Write file"),
    ] {
        let tool = tools.iter().find(|tool| tool["name"] == name).unwrap();
        assert_eq!(
            tool["annotations"],
            json!({
                "title": title,
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false,
            }),
            "{tool}"
        );
    }

    // Auto-starting the daemon under FASTEXEC_YOLO=true succeeds and shares tasks with plain endpoints.
    let (started, text, error) =
        yolo.call(json!({"action": "start", "command": "true", "waitMs": 10000}));
    assert!(!error, "{text}");
    let bare = started["taskId"]
        .as_str()
        .unwrap()
        .split_once('.')
        .unwrap()
        .0
        .to_owned();

    let mut plain = Endpoint::start(&fixture);
    let (listed_tasks, text, error) = plain.call(json!({"action": "list", "scope": "all"}));
    assert!(!error, "{text}");
    assert!(
        listed_tasks["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|task| task["taskId"] == bare),
        "{listed_tasks}"
    );
    let listed = plain.request("tools/list", json!({}));
    let tools = listed["result"]["tools"].as_array().unwrap();
    for (name, expected) in [
        (
            "edit",
            json!({"title": "Edit file", "destructiveHint": true, "idempotentHint": false}),
        ),
        (
            "exec",
            json!({"title": "Run bash command", "readOnlyHint": false, "destructiveHint": true, "openWorldHint": true}),
        ),
        ("read", json!({"title": "Read file", "readOnlyHint": true})),
        (
            "write",
            json!({"title": "Write file", "destructiveHint": true, "idempotentHint": false}),
        ),
    ] {
        let tool = tools.iter().find(|tool| tool["name"] == name).unwrap();
        assert_eq!(tool["annotations"], expected, "{tool}");
    }
    plain.close();
    yolo.close();
}

#[test]
fn invalid_yolo_refuses_start_without_blocking_status_or_stop() {
    let fixture = Fixture::new("yolo-invalid");
    for (var, bad, expected) in [
        (
            "FASTEXEC_YOLO",
            "on",
            "invalid FASTEXEC_YOLO \"on\"; expected true or false",
        ),
        (
            "FASTEXEC_TRANSFER",
            "on",
            "invalid FASTEXEC_TRANSFER \"on\"; expected true or false",
        ),
        (
            "FASTEXEC_OUTPUT_MODE",
            "bogus",
            "invalid FASTEXEC_OUTPUT_MODE \"bogus\"; expected text, both, or structured",
        ),
        (
            "FASTEXEC_STRUCTURED_CONTENT",
            "bogus",
            "invalid FASTEXEC_STRUCTURED_CONTENT \"bogus\"; expected true or false",
        ),
    ] {
        let output = fixture
            .command(&["daemon", "start"])
            .env(var, bad)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{var}={bad}: {output:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(expected), "{var}={bad}: {stderr:?}");
    }
    let endpoint_output = fixture
        .command(&[])
        .env("FASTEXEC_DAEMON", "on")
        .env("FASTEXEC_YOLO", "on")
        .output()
        .unwrap();
    assert_eq!(endpoint_output.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&endpoint_output.stderr)
            .contains("invalid FASTEXEC_YOLO \"on\"; expected true or false")
    );
    let bad_xfer_ep = fixture
        .command(&[])
        .env("FASTEXEC_DAEMON", "on")
        .env("FASTEXEC_TRANSFER", "on")
        .output()
        .unwrap();
    assert_eq!(bad_xfer_ep.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&bad_xfer_ep.stderr)
            .contains("invalid FASTEXEC_TRANSFER \"on\"; expected true or false")
    );
    let bad_xfer_run = fixture
        .command(&["daemon", "run"])
        .env("FASTEXEC_TRANSFER", "on")
        .output()
        .unwrap();
    assert_eq!(bad_xfer_run.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&bad_xfer_run.stderr)
            .contains("invalid FASTEXEC_TRANSFER \"on\"; expected true or false")
    );

    for action in ["status", "stop"] {
        let output = fixture
            .command(&["daemon", action])
            .env("FASTEXEC_YOLO", "on")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(0), "{action}: {output:?}");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.starts_with("No fastexec daemon"),
            "{action} unexpectedly saw a daemon: {stdout:?}"
        );
    }

    let (code, out, err) = fixture.run(&["daemon", "start"]);
    assert_eq!(code, 0, "{out}{err}");
    let status = fixture
        .command(&["daemon", "status"])
        .env("FASTEXEC_YOLO", "bogus")
        .env("FASTEXEC_TRANSFER", "bogus")
        .output()
        .unwrap();
    assert_eq!(status.status.code(), Some(0));
    assert!(
        String::from_utf8_lossy(&status.stdout).contains("pid "),
        "{status:?}"
    );
    let stop = fixture
        .command(&["daemon", "stop"])
        .env("FASTEXEC_YOLO", "bogus")
        .env("FASTEXEC_TRANSFER", "bogus")
        .output()
        .unwrap();
    assert_eq!(stop.status.code(), Some(0));
    assert!(
        String::from_utf8_lossy(&stop.stdout).contains("Stopped"),
        "{stop:?}"
    );
    let (code, out, err) = fixture.run(&["daemon", "status"]);
    assert_eq!(code, 0, "{out}{err}");
    assert!(out.starts_with("No fastexec daemon"), "{out}");
}

#[test]
fn codemode_endpoint_enables_an_existing_daemon_and_another_reader_gets_images() {
    let fixture = Fixture::new("codemode");
    let image =
        image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(8, 6, image::Rgb([7, 8, 9])));
    let mut png = Vec::new();
    image
        .write_to(&mut std::io::Cursor::new(&mut png), image::ImageFormat::Png)
        .unwrap();
    std::fs::write(fixture.runtime.join("shot.png"), &png).unwrap();
    let (status, out, err) = fixture.run(&["daemon", "start"]);
    assert_eq!(status, 0, "{out}{err}");
    let mut enabled = Endpoint::start_with(&fixture, |command| {
        command
            .current_dir(&fixture.runtime)
            .env("FASTEXEC_CODEMODE", "true")
            .env("FASTEXEC_OUTPUT_MODE", "both");
    });
    let mut plain = Endpoint::start(&fixture);
    for (endpoint, expected) in [(&mut enabled, true), (&mut plain, false)] {
        let listed = endpoint.request("tools/list", json!({}));
        assert_eq!(
            listed["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|tool| tool["name"] == "codemode"),
            expected,
            "{listed}"
        );
    }
    let invalid = enabled.request(
        "tools/call",
        json!({"name":"codemode","arguments":{"code":"   "}}),
    )["result"]
        .clone();
    assert_eq!(invalid["isError"], true, "{invalid}");
    assert_eq!(
        invalid["structuredContent"]["action"], "codemode",
        "{invalid}"
    );
    let read_img = enabled.request(
        "tools/call",
        json!({"name":"read","arguments":{"path":"shot.png"}}),
    )["result"]
        .clone();
    assert_eq!(read_img["structuredContent"]["kind"], "image", "{read_img}");
    assert_eq!(read_img["structuredContent"]["width"], 8, "{read_img}");
    let result = enabled.request("tools/call", json!({"name":"codemode","arguments":{
        "code":"image(await tools.read({path:'shot.png'})); const child = await tools.exec({action:'start',command:'printf nested',loginShell:false}); text(child.taskId); return child.output;"
    }}))["result"].clone();
    assert_eq!(result["isError"], false, "{result}");
    assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
    assert_eq!(
        result["content"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|block| block["type"] == "image")
            .count(),
        1,
        "{result}"
    );
    let handle = result["structuredContent"]["taskId"].as_str().unwrap();
    let bare = handle.split_once('.').unwrap().0;
    let fresh = plain.request(
        "tools/call",
        json!({"name":"exec","arguments":{"action":"poll","taskId":bare,"waitMs":0}}),
    )["result"]
        .clone();
    assert_eq!(fresh["isError"], false, "{fresh}");
    assert!(
        fresh["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("nested"),
        "{fresh}"
    );
    let block = fresh["content"]
        .as_array()
        .unwrap()
        .iter()
        .find(|block| block["type"] == "image")
        .unwrap();
    assert_eq!(
        block["data"],
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &png),
        "{fresh}"
    );
    let (listed, text, error) = enabled.call(json!({"action":"list"}));
    assert!(!error, "{text}");
    let tasks = listed["tasks"].as_array().unwrap();
    let script = tasks
        .iter()
        .find(|task| task["kind"] == "codemode")
        .unwrap();
    let child = tasks.iter().find(|task| task["kind"] == "bash").unwrap();
    assert!(!script["callId"].is_null(), "{listed}");
    assert_eq!(script["callId"], child["callId"], "{listed}");
    assert_eq!(script["session"], child["session"], "{listed}");
}

#[test]
fn invalid_codemode_is_rejected_by_daemon_start_and_run() {
    let fixture = Fixture::new("codemode-invalid");
    for action in ["start", "run"] {
        let output = fixture
            .command(&["daemon", action])
            .env("FASTEXEC_CODEMODE", "on")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("invalid FASTEXEC_CODEMODE"),
            "{output:?}"
        );
    }
    let output = fixture
        .command(&["daemon", "status"])
        .env("FASTEXEC_CODEMODE", "on")
        .output()
        .unwrap();
    assert!(
        String::from_utf8_lossy(&output.stdout).starts_with("No fastexec daemon"),
        "{output:?}"
    );
}

#[test]
fn daemon_ipc_accepts_1mib_escaped_input_refuses_over_3mib_and_matches_stdio_schemas() {
    let fixture = Fixture::new("ipc-3mib-and-schemas");
    let mut daemon_ep = Endpoint::start_with(&fixture, |cmd| {
        cmd.env("FASTEXEC_CODEMODE", "true")
            .env("FASTEXEC_TRANSFER", "true");
    });

    let daemon_tools = daemon_ep.request("tools/list", json!({}))["result"]["tools"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(daemon_tools.len(), 6, "{daemon_tools:?}");
    let mut stdio_cmd = Command::new(env!("CARGO_BIN_EXE_fastexec"));
    support::scrub_fastexec_env(&mut stdio_cmd);
    let mut stdio_child = stdio_cmd
        .env("FASTEXEC_DAEMON", "off")
        .env("FASTEXEC_CODEMODE", "true")
        .env("FASTEXEC_TRANSFER", "true")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdio_reader = BufReader::new(stdio_child.stdout.take().unwrap());
    let mut read_reply = |want_id: u64| -> Value {
        loop {
            let mut line = String::new();
            assert!(
                stdio_reader.read_line(&mut line).unwrap() > 0,
                "stdio EOF before id {want_id}"
            );
            if let Ok(msg) = serde_json::from_str::<Value>(&line)
                && msg["id"] == want_id
            {
                return msg;
            }
        }
    };
    {
        let stdin = stdio_child.stdin.as_mut().unwrap();
        writeln!(
            stdin,
            "{}",
            json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}})
        )
        .unwrap();
        stdin.flush().unwrap();
    }
    let _ = read_reply(1);
    {
        let stdin = stdio_child.stdin.as_mut().unwrap();
        writeln!(
            stdin,
            "{}",
            json!({"jsonrpc":"2.0","method":"notifications/initialized"})
        )
        .unwrap();
        writeln!(
            stdin,
            "{}",
            json!({"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}})
        )
        .unwrap();
        stdin.flush().unwrap();
    }
    let list_msg = read_reply(2);
    drop(stdio_child.stdin.take());
    let _ = stdio_child.wait();
    let stdio_tools = list_msg["result"]["tools"].as_array().unwrap();
    assert_eq!(daemon_tools.len(), stdio_tools.len());
    for s_tool in stdio_tools {
        let name = s_tool["name"].as_str().unwrap();
        let d_tool = daemon_tools.iter().find(|t| t["name"] == name).unwrap();
        assert_eq!(
            d_tool["inputSchema"], s_tool["inputSchema"],
            "schema mismatch on {name}"
        );
    }

    for unit in ["x", "\n", "é"] {
        let (wc_sc, wc_text, wc_err) = daemon_ep.call(json!({
            "action": "start",
            "loginShell": false,
            "command": "wc -c",
            "input": unit.repeat((1 << 20) / unit.len())
        }));
        assert!(!wc_err, "{wc_text}");
        assert_eq!(wc_sc["exitCode"], 0, "{wc_text}");
        assert!(
            wc_text.lines().any(|line| line.trim() == "1048576"),
            "{wc_text}"
        );
    }

    let big_file = fixture.runtime.join("near-1mib.txt");
    let (_, w_text, w_err) = daemon_ep.file(
        "write",
        json!({"path": big_file.to_string_lossy(), "content": "\n".repeat(900_000)}),
    );
    assert!(!w_err, "{w_text}");
    assert_eq!(std::fs::metadata(&big_file).unwrap().len(), 900_000);

    let (_, nul_text, nul_err) = daemon_ep.call(json!({
        "action": "start",
        "loginShell": false,
        "command": "cat",
        "input": "\0".repeat(1 << 20)
    }));
    assert!(nul_err && nul_text.contains("3 MiB"), "{nul_text}");
    let (listed, text, err) = daemon_ep.call(json!({"action": "list"}));
    assert!(!err, "{text}");
    assert_eq!(listed["totalCount"], 3, "{listed}");
}

#[test]
fn outside_session_running_footer_is_delivered_once_and_preserved_across_tiny_budget() {
    let fixture = Fixture::new("outside-footer-once");
    let mut ep_a = Endpoint::start(&fixture);
    let mut ep_b = Endpoint::start(&fixture);

    let (t1, t1_text, e1) = ep_a
        .call(json!({"action": "start", "loginShell": false, "command": "sleep 30", "waitMs": 50}));
    let (t2, t2_text, e2) = ep_a
        .call(json!({"action": "start", "loginShell": false, "command": "sleep 30", "waitMs": 50}));
    assert!(!e1 && !e2);
    let id1 = Endpoint::task_id(&t1, &t1_text);
    let id2 = Endpoint::task_id(&t2, &t2_text);

    let mut ep_cm = Endpoint::start_with(&fixture, |cmd| {
        cmd.env("FASTEXEC_CODEMODE", "true");
    });
    let nested_first = ep_cm.request(
        "tools/call",
        json!({
            "name": "codemode",
            "arguments": {
                "code": "await tools.exec({action: 'start', loginShell: false, command: 'true'}); return 'ok';"
            }
        }),
    );
    let nested_outer_text = nested_first["result"]["content"][0]["text"]
        .as_str()
        .unwrap();
    assert!(
        nested_outer_text.contains(
            "(Background: 2 tasks running outside this session; use list with scope \"all\".)"
        ),
        "nested exec inside codemode must not swallow outer session's first outside-task notice: {nested_outer_text}"
    );

    // Footer lives outside the body maxBytes budget, so even maxBytes: 100 delivers the
    // outside-session notice once on the first call and omits it on subsequent calls.
    let (_, first_text, first_err) = ep_b.call(json!({
        "action": "start",
        "command": "true",
        "maxBytes": 100
    }));
    assert!(
        !first_err
            && first_text.contains(
                "(Background: 2 tasks running outside this session; use list with scope \"all\".)"
            ),
        "{first_text}"
    );

    let (_, second_text, second_err) = ep_b.call(json!({
        "action": "start",
        "command": "true"
    }));
    assert!(
        !second_err && !second_text.contains("outside this session"),
        "{second_text}"
    );

    let (sess_list, _, _) = ep_b.call(json!({"action": "list"}));
    assert_eq!(sess_list["totalCount"], 2, "{sess_list}");
    let (all_list, _, _) = ep_b.call(json!({"action": "list", "scope": "all"}));
    assert_eq!(all_list["totalCount"], 6, "{all_list}");

    // Stale 8-char base32 epoch in daemon mode reports an earlier daemon; malformed ID says Unknown taskId.
    let (_, stale_text, stale_err) = ep_b.call(json!({"action": "poll", "taskId": "01234567-9z"}));
    assert!(
        stale_err && stale_text.contains("earlier fastexec daemon"),
        "{stale_text}"
    );
    let (_, bad_text, bad_err) = ep_b.call(json!({"action": "poll", "taskId": "foo-bar"}));
    assert!(
        bad_err
            && bad_text.contains("Unknown taskId")
            && !bad_text.contains("earlier fastexec daemon"),
        "{bad_text}"
    );

    // Closing ep_b sends Bye (dropping its session); a newly connected session starts fresh and shows the count again.
    drop(ep_b);
    let mut ep_c = Endpoint::start(&fixture);
    let (_, fresh_text, fresh_err) = ep_c.call(json!({
        "action": "start",
        "loginShell": false,
        "command": "true"
    }));
    assert!(
        !fresh_err
            && fresh_text.contains(
                "(Background: 2 tasks running outside this session; use list with scope \"all\".)"
            ),
        "{fresh_text}"
    );
    drop(ep_c);

    ep_a.call(json!({"action": "kill", "taskId": id1}));
    ep_a.call(json!({"action": "kill", "taskId": id2}));
}

#[test]
fn daemon_delivers_4mib_both_mode_window_and_probes_login_profile_once_per_session() {
    let fixture = Fixture::new("four-mib-and-probe");
    let home = fixture.runtime.join("test-home");
    let sub_a = fixture.runtime.join("test-sub-a");
    let sub_b = fixture.runtime.join("test-sub-b");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&sub_a).unwrap();
    std::fs::create_dir_all(&sub_b).unwrap();
    std::fs::write(
        home.join(".bash_profile"),
        "export FX_DAEMON_SNAP=from_profile\necho BannerNoise\necho x >> \"$HOME/probe_count\"\n",
    )
    .unwrap();

    let sub_a_str = sub_a.to_string_lossy().replace('\\', "/");
    let sub_b_str = sub_b.to_string_lossy().replace('\\', "/");
    let mut ep1 = Endpoint::start_with(&fixture, |cmd| {
        cmd.env("HOME", &home)
            .env("USERPROFILE", &home)
            .env("FASTEXEC_OUTPUT_MODE", "both")
            .env("FASTEXEC_CODEMODE", "true");
    });

    for _ in 0..2 {
        let (out, text, err) = ep1.call(json!({
            "action": "start",
            "command": "printf '%s' \"$FX_DAEMON_SNAP\""
        }));
        assert!(!err, "{text}");
        assert_eq!(out["output"], "from_profile");
        assert!(!text.contains("BannerNoise"), "{text}");
    }

    for cwd in [&sub_a_str, &sub_b_str] {
        let cm = ep1.request(
            "tools/call",
            json!({
                "name": "codemode",
                "arguments": {
                    "cwd": cwd,
                    "code": "const r = await tools.exec({action: 'start', command: 'printf \"%s\" \"$FX_DAEMON_SNAP\"'}); return r.output;"
                }
            }),
        );
        assert_ne!(cm["result"]["isError"], true, "{cm}");
        assert_eq!(cm["result"]["structuredContent"]["output"], "from_profile");
    }

    let count_ep1 = std::fs::read_to_string(home.join("probe_count")).unwrap();
    assert_eq!(
        count_ep1.lines().count(),
        1,
        "one session plus with_cwd codemode derivations must probe login env once: {count_ep1:?}"
    );

    // A second endpoint session probes its own snapshot once.
    let mut ep2 = Endpoint::start_with(&fixture, |cmd| {
        cmd.env("HOME", &home).env("USERPROFILE", &home);
    });
    let (_, text2, err2) = ep2.call(json!({
        "action": "start",
        "command": "printf '%s' \"$FX_DAEMON_SNAP\""
    }));
    assert!(!err2 && text2.starts_with("from_profile"), "{text2}");
    let count_ep2 = std::fs::read_to_string(home.join("probe_count")).unwrap();
    assert_eq!(count_ep2.lines().count(), 2, "{count_ep2:?}");
    drop(ep2);

    // 4 MiB body in FASTEXEC_OUTPUT_MODE=both carries both text and structuredContent.output
    // comfortably within ipc::RESPONSE_LIMIT (64 MiB).
    let (four_mib, _, four_err) = ep1.call(json!({
        "action": "start",
        "maxBytes": 4194304,
        "truncate": "head",
        "command": "head -c 4194300 /dev/zero | tr '\\0' M"
    }));
    assert!(!four_err);
    assert_eq!(four_mib["output"].as_str().unwrap().len(), 4_194_300);
}

#[test]
fn transfer_endpoints_share_a_non_http_daemon_with_independent_hints_and_yolo() {
    let fixture = Fixture::new("daemon-transfer");
    let work = fixture.runtime.join("work");
    std::fs::create_dir_all(&work).unwrap();
    std::fs::write(work.join("blob.bin"), [0u8, 1, 2, 3]).unwrap();
    std::fs::write(work.join("note.txt"), "hello\n").unwrap();

    // When FASTEXEC_TRANSFER is unset or false without --http, invalid transfer env vars are
    // unread by both `daemon start` and lazy daemon startup on an endpoint's first exec/read.
    for (idx, setting) in [None, Some("false")].into_iter().enumerate() {
        let start_fixture = Fixture::sharing(&fixture.runtime, &format!("xfer-off-start-{idx}"));
        let mut start_cmd = start_fixture.command(&["daemon", "start"]);
        start_cmd
            .env("FASTEXEC_PUBLIC_URL", "not-a-url")
            .env("FASTEXEC_TRANSFER_ADDR", "not-an-addr");
        if let Some(value) = setting {
            start_cmd.env("FASTEXEC_TRANSFER", value);
        }
        let started = start_cmd.output().unwrap();
        assert_eq!(
            started.status.code(),
            Some(0),
            "{setting:?}: {}",
            String::from_utf8_lossy(&started.stderr)
        );
        let (_, status, _) = start_fixture.run(&["daemon", "status"]);
        assert!(status.lines().any(|line| line == "http off"), "{status}");
        drop(start_fixture);

        let lazy_fixture = Fixture::sharing(&fixture.runtime, &format!("xfer-off-lazy-{idx}"));
        let mut lazy_ep = Endpoint::start_with(&lazy_fixture, |cmd| {
            cmd.current_dir(&work)
                .env("FASTEXEC_PUBLIC_URL", "not-a-url")
                .env("FASTEXEC_TRANSFER_ADDR", "not-an-addr");
            if let Some(value) = setting {
                cmd.env("FASTEXEC_TRANSFER", value);
            }
        });
        let (exec_sc, exec_text, exec_err) = lazy_ep.call(json!({
            "action": "start",
            "command": "printf LAZY_OK",
            "loginShell": false,
        }));
        assert!(!exec_err && exec_sc["exitCode"] == 0, "{exec_text}");
        assert!(exec_text.contains("LAZY_OK"), "{exec_text}");
        let (_, read_text, read_err) = lazy_ep.file("read", json!({"path": "note.txt"}));
        assert!(!read_err && read_text == "hello\n", "{read_text}");
        lazy_ep.close();
        drop(lazy_fixture);
    }

    // Wildcard FASTEXEC_TRANSFER_ADDR=0.0.0.0:0 with FASTEXEC_HTTP_ALLOW_REMOTE=1 requires FASTEXEC_PUBLIC_URL.
    {
        let wildcard_fixture = Fixture::sharing(&fixture.runtime, "xfer-wildcard");
        for action in [&["daemon", "start"][..], &["daemon", "run"][..]] {
            let out = wildcard_fixture
                .command(action)
                .env("FASTEXEC_TRANSFER", "true")
                .env("FASTEXEC_HTTP_ALLOW_REMOTE", "1")
                .env("FASTEXEC_TRANSFER_ADDR", "0.0.0.0:0")
                .output()
                .unwrap();
            assert_eq!(out.status.code(), Some(1), "{action:?}: {out:?}");
            let stderr = String::from_utf8_lossy(&out.stderr);
            assert!(
                stderr.contains(
                    "FASTEXEC_TRANSFER_ADDR 0.0.0.0:0 is an unspecified address; set FASTEXEC_PUBLIC_URL"
                ),
                "{action:?}: {stderr:?}"
            );
        }
        let ok = wildcard_fixture
            .command(&["daemon", "start"])
            .env("FASTEXEC_TRANSFER", "true")
            .env("FASTEXEC_HTTP_ALLOW_REMOTE", "1")
            .env("FASTEXEC_TRANSFER_ADDR", "0.0.0.0:0")
            .env("FASTEXEC_PUBLIC_URL", "https://files.example/")
            .output()
            .unwrap();
        assert_eq!(
            ok.status.code(),
            Some(0),
            "{}",
            String::from_utf8_lossy(&ok.stderr)
        );
        let mut ep = Endpoint::start_with(&wildcard_fixture, |cmd| {
            cmd.current_dir(&work).env("FASTEXEC_TRANSFER", "true");
        });
        let minted = ep.request(
            "tools/call",
            json!({"name": "transfer", "arguments": {"path": "wild.bin"}}),
        )["result"]
            .clone();
        assert_eq!(minted["isError"], false, "{minted}");
        assert!(
            minted["structuredContent"]["url"]
                .as_str()
                .unwrap()
                .starts_with("https://files.example/t/"),
            "{minted}"
        );
        ep.close();
        drop(wildcard_fixture);
    }

    // Start a plain daemon without --http and without FASTEXEC_TRANSFER.
    let (code, out, err) = fixture.run(&["daemon", "start"]);
    assert_eq!(code, 0, "{out}{err}");
    let (_, status, _) = fixture.run(&["daemon", "status"]);
    assert!(status.lines().any(|line| line == "http off"), "{status}");

    let mut transfer_ep = Endpoint::start_with(&fixture, |cmd| {
        cmd.current_dir(&work)
            .env("FASTEXEC_TRANSFER", "true")
            .env("FASTEXEC_YOLO", "true");
    });
    let mut plain_ep = Endpoint::start_with(&fixture, |cmd| {
        cmd.current_dir(&work);
    });

    let xfer_tools = transfer_ep.request("tools/list", json!({}))["result"]["tools"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(xfer_tools.len(), 5, "{xfer_tools:?}");
    let xfer_tool = xfer_tools
        .iter()
        .find(|tool| tool["name"] == "transfer")
        .unwrap();
    assert_eq!(
        xfer_tool["annotations"],
        json!({
            "title": "Transfer file",
            "readOnlyHint": true,
            "destructiveHint": false,
            "idempotentHint": false,
            "openWorldHint": false,
        }),
        "{xfer_tool}"
    );
    let plain_tools = plain_ep.request("tools/list", json!({}))["result"]["tools"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(plain_tools.len(), 4, "{plain_tools:?}");
    assert!(!plain_tools.iter().any(|tool| tool["name"] == "transfer"));

    let (_, xfer_bin_text, xfer_bin_err) = transfer_ep.file("read", json!({"path": "blob.bin"}));
    assert!(xfer_bin_err, "{xfer_bin_text}");
    assert!(
        xfer_bin_text.contains("Inspect it with exec or download it with transfer."),
        "{xfer_bin_text}"
    );
    let (_, plain_bin_text, plain_bin_err) = plain_ep.file("read", json!({"path": "blob.bin"}));
    assert!(plain_bin_err, "{plain_bin_text}");
    assert!(
        plain_bin_text.ends_with("Inspect it with exec.")
            && !plain_bin_text.contains("download it with transfer"),
        "{plain_bin_text}"
    );

    let minted = transfer_ep.request(
        "tools/call",
        json!({"name": "transfer", "arguments": {"path": "uploaded.bin"}}),
    )["result"]
        .clone();
    assert_eq!(minted["isError"], false, "{minted}");
    let url = minted["structuredContent"]["url"].as_str().unwrap();
    let rest = url.strip_prefix("http://").expect("http:// URL");
    let (authority, path_part) = rest.split_once('/').unwrap();
    let addr: std::net::SocketAddr = authority.parse().unwrap();
    let target = format!("/{path_part}");

    assert_eq!(transfer_http::send(addr, "HEAD", &target, b"").0, 200);
    assert_eq!(
        transfer_http::send(addr, "PUT", &format!("{target}?offset=0"), b"").0,
        200
    );
    let payload = b"daemon-stdio-transfer";
    let (put_status, _, put_body) = transfer_http::send(addr, "PUT", &target, payload);
    assert_eq!(put_status, 200);
    assert_eq!(put_body, payload.len().to_string().as_bytes());
    let (head_status, head_headers, _) = transfer_http::send(addr, "HEAD", &target, b"");
    assert_eq!(head_status, 200);
    assert!(
        head_headers
            .iter()
            .any(|(k, v)| k == "content-length" && v == &payload.len().to_string())
    );
    let (get_status, _, get_bytes) = transfer_http::send(addr, "GET", &target, b"");
    assert_eq!(get_status, 200);
    assert_eq!(get_bytes, payload);
    assert_eq!(std::fs::read(work.join("uploaded.bin")).unwrap(), payload);
    assert_eq!(transfer_http::send(addr, "GET", "/mcp", b"").0, 404);

    // Requesting a different IP (`0.0.0.0:0` with port 0) on a daemon already serving `127.0.0.1:<port>` is refused.
    let mut conflict_ep = Endpoint::start_with(&fixture, |cmd| {
        cmd.current_dir(&work)
            .env("FASTEXEC_TRANSFER", "true")
            .env("FASTEXEC_HTTP_ALLOW_REMOTE", "1")
            .env("FASTEXEC_TRANSFER_ADDR", "0.0.0.0:0")
            .env("FASTEXEC_PUBLIC_URL", "https://files.example/");
    });
    let conflict = conflict_ep.request(
        "tools/call",
        json!({"name": "transfer", "arguments": {"path": "conflict.bin"}}),
    )["result"]
        .clone();
    assert_eq!(conflict["isError"], true, "{conflict}");
    assert_eq!(
        conflict["structuredContent"]["error"],
        json!(format!(
            "the daemon already serves /t/ on {addr}, not 0.0.0.0:0; stop it first"
        ))
    );
    conflict_ep.close();

    plain_ep.close();
    transfer_ep.close();

    // Even when FASTEXEC_PUBLIC_URL is set without --http, the daemon binds /t/ on FASTEXEC_TRANSFER_ADDR
    // (and an earlier directory refusal does not prematurely bind `127.0.0.1:0`).
    let (fixed_addr, proxied_fixture, proxied_ep, minted) = (0..8)
        .find_map(|attempt| {
            let proxied_fixture =
                Fixture::sharing(&fixture.runtime, &format!("xfer-proxied-{attempt}"));
            let mut dir_ep = Endpoint::start_with(&proxied_fixture, |cmd| {
                cmd.current_dir(&work).env("FASTEXEC_TRANSFER", "true");
            });
            let dir_refused = dir_ep.request(
                "tools/call",
                json!({"name": "transfer", "arguments": {"path": "."}}),
            )["result"]
                .clone();
            assert_eq!(dir_refused["isError"], true, "{dir_refused}");
            dir_ep.close();

            let candidate = transfer_http::free_loopback_addr();
            let mut proxied_ep = Endpoint::start_with(&proxied_fixture, |cmd| {
                cmd.current_dir(&work)
                    .env("FASTEXEC_TRANSFER", "true")
                    .env("FASTEXEC_TRANSFER_ADDR", candidate.to_string())
                    .env("FASTEXEC_PUBLIC_URL", "https://files.example/daemon");
            });
            let minted = proxied_ep.request(
                "tools/call",
                json!({"name": "transfer", "arguments": {"path": "proxied.bin"}}),
            )["result"]
                .clone();
            if minted["isError"] == true {
                let err_text = minted["structuredContent"]["error"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                proxied_ep.close();
                if attempt < 7 && err_text.contains("cannot listen on") {
                    return None;
                }
                panic!("unexpected daemon proxied transfer error: {minted}");
            }
            Some((candidate, proxied_fixture, proxied_ep, minted))
        })
        .expect("bound fixed loopback port for daemon transfer");
    let url = minted["structuredContent"]["url"].as_str().unwrap();
    let id = url
        .strip_prefix("https://files.example/daemon/t/")
        .unwrap_or_else(|| panic!("unexpected proxied URL: {url}"));
    let target = format!("/t/{id}");
    assert_eq!(
        transfer_http::send(fixed_addr, "PUT", &format!("{target}?offset=0"), b"").0,
        200
    );
    assert_eq!(
        transfer_http::send(fixed_addr, "PUT", &target, b"proxied-daemon").0,
        200
    );
    assert_eq!(
        transfer_http::send(fixed_addr, "GET", &target, b"").2,
        b"proxied-daemon"
    );
    proxied_ep.close();
    drop(proxied_fixture);
}

#[test]
fn cold_http_daemon_missing_instructions_file_exits_without_deleting_logs() {
    let fixture = Fixture::new("instructions-cold-http");
    // Start and stop once so the namespace directory exists under its derived hex name.
    let (code, _, err) = fixture.run(&["daemon", "start"]);
    assert_eq!(code, 0, "seed start failed: {err}");
    let (code, _, err) = fixture.run(&["daemon", "stop"]);
    assert_eq!(code, 0, "seed stop failed: {err}");

    let ns_dir = std::fs::read_dir(fixture.runtime.join("ns"))
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| path.is_dir())
        .expect("namespace directory");
    let logs = ns_dir.join("logs");
    std::fs::create_dir_all(&logs).unwrap();
    let sentinel = logs.join("preexisting.log");
    std::fs::write(&sentinel, b"keep this log").unwrap();

    let missing = fixture.runtime.join("does-not-exist.txt");
    for subcommand in ["start", "run"] {
        let output = fixture
            .command(&["daemon", subcommand, "--http", "127.0.0.1:0"])
            .env("FASTEXEC_HTTP_TOKEN", "0123456789abcdef")
            .env("FASTEXEC_INSTRUCTIONS_FILE", &missing)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "daemon {subcommand} --http");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("FASTEXEC_INSTRUCTIONS_FILE"),
            "daemon {subcommand} --http: {stderr:?}"
        );
        assert_eq!(
            std::fs::read(&sentinel).unwrap(),
            b"keep this log",
            "daemon {subcommand} --http deleted pre-existing logs"
        );
    }
}

#[test]
fn non_http_daemon_cli_ignores_missing_instructions_file() {
    let fixture = Fixture::new("instructions-non-http");
    let missing = fixture.runtime.join("does-not-exist.txt");

    for subcommand in ["start", "status", "stop"] {
        let output = fixture
            .command(&["daemon", subcommand])
            .env("FASTEXEC_INSTRUCTIONS_FILE", &missing)
            .output()
            .unwrap();
        let out = String::from_utf8_lossy(&output.stdout);
        let err = String::from_utf8_lossy(&output.stderr);
        if let Some(pid) = pid_of(&out) {
            fixture.pid.set(Some(pid));
        }
        assert_eq!(
            output.status.code(),
            Some(0),
            "daemon {subcommand} failed: {out}{err}"
        );
    }
}
