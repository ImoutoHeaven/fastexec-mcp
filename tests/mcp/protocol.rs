#[cfg(target_os = "macos")]
use super::common::bare_id;
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
use super::common::wait_for_heartbeat;
use super::common::{Scratch, Server, code_result, code_text, codemode_server};
#[cfg(unix)]
use super::common::{assert_stops_growing, file_len, heartbeat, scratch};
use serde_json::{Value, json};
use std::path::PathBuf;
use std::process::{Command, Stdio};
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
use std::time::{Duration, Instant};

#[test]
fn the_listed_tool_says_which_host_it_runs_on() {
    let dir = Scratch::new("exec-description");
    std::fs::create_dir_all(&*dir).unwrap();
    let mut server = Server::start_with(|command| {
        command.current_dir(&*dir);
    });
    let listed = server.request("tools/list", json!({}));
    let tools = listed["result"]["tools"].as_array().unwrap();
    let mut names: Vec<&str> = tools
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["edit", "exec", "read", "write"], "{listed}");
    // `transfer` is off unless `FASTEXEC_TRANSFER=true`, so it is not listed by default.
    let refused = server.request(
        "tools/call",
        json!({"name": "transfer", "arguments": {"path": "notes/a.txt"}}),
    );
    assert_eq!(
        refused["error"]["message"],
        json!("tool not found"),
        "{refused}"
    );
    let exec = tools
        .iter()
        .find(|tool| tool["name"] == json!("exec"))
        .unwrap_or_else(|| panic!("no exec in {listed}"));
    let description = exec["description"].as_str().unwrap();
    // The host line is the last one, so a client that shows the tail still says where it runs.
    let last = description.lines().last().unwrap();
    assert!(last.starts_with("Host: "), "{last:?}");
    let cwd = last
        .split_once("default cwd ")
        .unwrap_or_else(|| panic!("no default cwd in {last:?}"))
        .1;
    // macOS reports `/private/var` for a $TMPDIR under `/var`, so both sides are compared as the
    // filesystem resolves them.
    let expected = std::fs::canonicalize(&*dir).unwrap_or_else(|_| PathBuf::from(&*dir));
    let cwd = std::fs::canonicalize(cwd).unwrap_or_else(|_| PathBuf::from(cwd));
    let same = if cfg!(windows) {
        cwd.to_string_lossy()
            .eq_ignore_ascii_case(&expected.to_string_lossy())
    } else {
        cwd == expected
    };
    assert!(same, "cwd {cwd:?}, expected {expected:?} in {last:?}");
    // The Windows paragraph is there exactly where it applies.
    assert_eq!(
        description.contains("MSYS_NO_PATHCONV"),
        cfg!(windows),
        "{description}"
    );
}

/// A heartbeat in a new session through a double fork, as daemonizing servers (adb) start;
/// `program` runs bash's loop under another name. Perl's fork and `setsid` stand in for util-linux
/// `setsid -f`, which macOS does not carry; a failed detach says so instead of running the
/// heartbeat in the task's own session.
#[cfg(all(unix, not(target_os = "macos")))]
fn daemon(program: &str, bash_path: &str) -> String {
    format!(
        "perl -MPOSIX -e 'exit if fork; POSIX::setsid() or die \"setsid: $!\"; exec @ARGV' {program} -c 'while :; do echo x >> {bash_path}; sleep 0.1; done'; sleep 0.5;"
    )
}

/// Linux keeps the stronger assertion: its subreaper follows a process that detaches at once. On
/// macOS the host can only own what it observed first, and `a_detached_descendant_stays_owned_once_observed`
/// proves that half.
#[cfg(all(unix, not(target_os = "macos")))]
#[test]
fn processes_that_start_their_own_session_stay_in_the_tree() {
    let files = ["daemon", "group-term", "odd-name", "odd-name-bin"].map(scratch);
    let mut server = Server::start();
    let [
        (plain, plain_bash),
        (term, term_bash),
        (odd, odd_bash),
        (_, bin_bash),
    ] = &files;
    let command = format!("{} echo started", daemon("bash", plain_bash));
    let text = server.ok(json!({"action": "start", "command": command, "waitMs": 10000}));
    assert!(text.contains("[exited 0]"), "{text}");
    assert!(text.contains("ended with bash: bash ("), "{text}");
    assert!(file_len(plain) > 0, "the daemon never started");
    assert_stops_growing(plain);

    // A command's `kill 0` reaches the task host too, which must outlive it.
    let command = format!("{} kill -TERM 0", daemon("bash", term_bash));
    let text = server.ok(json!({"action": "start", "command": command, "waitMs": 10000}));
    assert!(text.contains("[exited 143]"), "{text}");
    assert!(text.contains("ended with bash: bash ("), "{text}");
    assert_stops_growing(term);

    // A process name that is not UTF-8 still counts the process in.
    let command = format!(
        "mkdir -p {bin_bash}; cp \"$(command -v bash)\" {bin_bash}/$'\\xff'beat; {}",
        daemon(&format!("{bin_bash}/$'\\xff'beat"), odd_bash)
    );
    let text = server.ok(json!({"action": "start", "command": command, "waitMs": 10000}));
    assert!(text.contains("ended with bash: \u{fffd}beat ("), "{text}");
    assert!(file_len(odd) > 0, "the daemon never started");
    assert_stops_growing(odd);
}

/// The daemon writes its pid, then waits for a release file before it detaches, so the host
/// reports it — by that pid — while it is still in the task's session: the acknowledgement the
/// macOS guarantee rests on.
#[cfg(target_os = "macos")]
fn daemon_until_released(
    program: &str,
    bash_path: &str,
    mark: &std::path::Path,
    go: &std::path::Path,
) -> String {
    let (mark, go) = (mark.display(), go.display());
    // Both phases are bounded, so a failing run leaves nothing behind: the wait for the go file
    // gives up after a minute, and the heartbeat after a minute of its own.
    format!(
        "perl -MPOSIX -e 'exit if fork; open my $m, \">\", \"{mark}\"; print $m $$; close $m; my $n = 0; while (!-e \"{go}\") {{ exit 3 if ++$n > 6000; select undef, undef, undef, 0.01 }} POSIX::setsid() or die \"setsid: $!\"; exec @ARGV' {program} -c 'for i in $(seq 600); do echo x >> {bash_path}; sleep 0.1; done'; sleep 0.5;"
    )
}

/// The pid the daemon wrote before it blocked.
#[cfg(target_os = "macos")]
fn wait_for_mark(path: &std::path::Path) -> u32 {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(text) = std::fs::read_to_string(path)
            && let Ok(pid) = text.trim().parse()
        {
            return pid;
        }
        assert!(Instant::now() < deadline, "the daemon never wrote its pid");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Waits until the named task itself reports that pid among the processes it holds: the host's
/// membership acknowledgement for that exact process, not a sleep and not another task's report.
#[cfg(target_os = "macos")]
fn wait_for_held_pid(server: &mut Server, task: &str, pid: u32) -> u32 {
    let deadline = Instant::now() + Duration::from_secs(30);
    let task_bare = bare_id(task);
    let pid_needle = format!("({pid})");
    loop {
        let text = server.ok(json!({"action": "list"}));
        if text
            .lines()
            .any(|line| line.starts_with(task_bare) && line.contains(&pid_needle))
        {
            return pid;
        }
        assert!(
            Instant::now() < deadline,
            "the task never reported the process {pid} as held: {text}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// The kernel's own identity for a live pid: the start time libproc reports, in microseconds, the
/// same pair the product compares.
#[cfg(target_os = "macos")]
fn birth_of(pid: u32) -> Option<u64> {
    let info = bsd_info(pid)?;
    Some(info.pbi_start_tvsec * 1_000_000 + info.pbi_start_tvusec)
}

/// What libproc reports about a live process: its parent, its group, and its start time.
#[cfg(target_os = "macos")]
fn bsd_info(pid: u32) -> Option<libc::proc_bsdinfo> {
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    // SAFETY: the buffer is exactly the size given, and libproc writes at most that many bytes.
    let read = unsafe {
        libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDTBSDINFO,
            0,
            &mut info as *mut libc::proc_bsdinfo as *mut libc::c_void,
            size,
        )
    };
    (read >= size).then_some(info)
}

/// The session a live process leads, from `getsid`.
#[cfg(target_os = "macos")]
fn session_of(pid: u32) -> Option<i32> {
    // SAFETY: plain syscall; a process that ended reports none.
    let session = unsafe { libc::getsid(pid as i32) };
    (session != -1).then_some(session)
}

/// The state character `ps` reports, where `T` means stopped.
#[cfg(target_os = "macos")]
fn state_of(pid: u32) -> String {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "stat="])
        .output();
    output
        .map(|output| String::from_utf8_lossy(&output.stdout).trim().to_string())
        .unwrap_or_default()
}

/// Waits, bounded, for a probe to answer.
#[cfg(any(target_os = "macos", target_os = "freebsd"))]
fn wait_until<T>(what: &str, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(value) = probe() {
            return value;
        }
        assert!(Instant::now() < deadline, "{what}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// A process a test started outside the tree, killed when the test ends whatever path it takes.
/// The signal goes out only while the birth time that names it is unchanged, so a reused pid is
/// never the target; the helper itself is bounded, so a leaked one ends on its own.
#[cfg(target_os = "macos")]
struct Escaped {
    pid: i32,
    birth: u64,
}

#[cfg(target_os = "macos")]
impl Escaped {
    /// Takes the pid the helper wrote, once the file holds a whole number.
    fn new(pid_file: &std::path::Path) -> Escaped {
        let pid = wait_until("the escape wrote its pid", || {
            std::fs::read_to_string(pid_file)
                .ok()
                .and_then(|text| text.trim().parse::<i32>().ok())
        });
        let birth = birth_of(pid as u32).expect("the escape is a live process");
        Escaped { pid, birth }
    }

    /// Whether the pid still names the process this guard took.
    fn is_mine(&self) -> bool {
        birth_of(self.pid as u32).is_some_and(|birth| birth == self.birth)
    }
}

#[cfg(target_os = "macos")]
impl Drop for Escaped {
    fn drop(&mut self) {
        if self.is_mine() {
            // SAFETY: plain syscall on a pid this guard just re-identified by birth time.
            unsafe { libc::kill(self.pid, libc::SIGKILL) };
        }
    }
}

/// The files one escape test needs, removed with it.
#[cfg(target_os = "macos")]
struct Files {
    dir: PathBuf,
}

#[cfg(target_os = "macos")]
impl Files {
    fn new(beat: &std::path::Path) -> Files {
        let dir = beat.with_extension("files");
        std::fs::create_dir_all(&dir).expect("a directory for the escape's files");
        Files { dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }
}

#[cfg(target_os = "macos")]
impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Keeps a stopped host stopped no longer than the test needs: the signal is resumed on every path
/// out, and only while the pid is still the host this test stopped.
#[cfg(target_os = "macos")]
struct Stopped {
    pid: i32,
    birth: u64,
    resumed: bool,
}

#[cfg(target_os = "macos")]
impl Stopped {
    /// Stops the host and waits until the kernel says it is stopped, before anything relies on it.
    fn new(pid: i32) -> Stopped {
        let birth = birth_of(pid as u32).expect("the host is a live process");
        // The guard exists before the signal goes out, so a panic on any step below still resumes
        // the host on the way out.
        let stopped = Stopped {
            pid,
            birth,
            resumed: false,
        };
        // SAFETY: plain syscall on the pid the root named as its parent.
        assert_eq!(
            unsafe { libc::kill(pid, libc::SIGSTOP) },
            0,
            "the host stopped"
        );
        wait_until("the host is stopped", || {
            state_of(pid as u32).starts_with('T').then_some(())
        });
        stopped
    }

    fn resume(&mut self) {
        if self.resumed {
            return;
        }
        self.resumed = true;
        if birth_of(self.pid as u32).is_some_and(|birth| birth == self.birth) {
            // SAFETY: plain syscall on the pid this guard re-identified by birth time.
            unsafe { libc::kill(self.pid, libc::SIGCONT) };
        }
    }
}

#[cfg(target_os = "macos")]
impl Drop for Stopped {
    fn drop(&mut self) {
        self.resume();
    }
}

/// The documented limit, proven without racing a scan: the root names its parent — the task host —
/// and the test stops that host, waits until the kernel reports it stopped, and only then lets the
/// root create the escapee. The escapee detaches, orphans itself, and the host is resumed into a
/// tree that never held it. The root ends there, so the task reports what it left behind.
#[cfg(target_os = "macos")]
#[test]
fn an_unobserved_escape_is_never_claimed() {
    let (beat, beat_bash) = scratch("escaped-daemon");
    let files = Files::new(&beat);
    let (pid_file, go) = (files.path("pid"), files.path("go"));
    let mut server = Server::start();
    // The helper is bounded: it ends by itself after twenty seconds, whatever a failing run does.
    let command = format!(
        "printf 'host %s\n' \"$PPID\"; while [ ! -e \"{go}\" ]; do sleep 0.02; done; perl -MPOSIX -e 'exit if fork; POSIX::setsid() or die \"setsid: $!\"; exit if fork; open my $p, \">\", \"{pid}\"; print $p $$; close $p; exec @ARGV' bash -c 'for i in $(seq 200); do echo x >> {beat}; sleep 0.1; done'",
        go = go.display(),
        pid = pid_file.display(),
        beat = beat_bash
    );
    let started = server.ok(json!({
        "action": "start", "command": command, "keepTree": true, "waitMs": 1000
    }));
    let host: i32 = started
        .lines()
        .find_map(|line| line.trim().strip_prefix("host "))
        .and_then(|pid| pid.trim().parse().ok())
        .unwrap_or_else(|| panic!("the root named its parent: {started}"));
    let task = server.id(1);
    // The host cannot scan while it is stopped, so the escapee is created unobserved.
    let mut stopped = Stopped::new(host);
    std::fs::write(&go, b"go").unwrap();
    let escaped = Escaped::new(&pid_file);
    // Detached and orphaned, as the contract describes: its own session and group, and a parent
    // that is neither the root nor the host.
    let info = bsd_info(escaped.pid as u32).expect("the escape runs");
    let host_session = session_of(host as u32).expect("the host is a live process");
    assert_ne!(
        session_of(escaped.pid as u32),
        Some(host_session),
        "the escape left the task's session"
    );
    assert_ne!(info.pbi_pgid as i32, host, "and the task's group");
    assert_ne!(
        info.pbi_ppid as i32, host,
        "its parent is not the task host"
    );
    // Orphaned for real, not merely parented elsewhere: the intermediate setsid parent can still
    // be alive, and a live ancestry trace would be scanned once the host resumes.
    wait_until("the escape is orphaned", || {
        let info = bsd_info(escaped.pid as u32)?;
        let parent = info.pbi_ppid as i32;
        (parent == 1 || birth_of(parent as u32).is_none()).then_some(())
    });
    stopped.resume();
    // The root ends, so the task reports what it left behind.
    wait_until("the root ended", || {
        let text = server.ok(json!({"action": "list"}));
        text.contains("[exited ").then_some(())
    });
    let listing = server.ok(json!({"action": "list"}));
    let pid_needle = format!("({})", escaped.pid);
    let claimed = listing
        .lines()
        .any(|line| line.starts_with(bare_id(&task)) && line.contains(&pid_needle));
    assert!(
        !claimed,
        "a descendant created while the host could not scan is never claimed: {listing}"
    );
    // An explicit kill reaches the tree the task owns, and not the process that left it.
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
    let before = file_len(&beat);
    std::thread::sleep(Duration::from_millis(600));
    assert!(
        file_len(&beat) > before,
        "the escaped descendant outlives the task's kill, which is the documented limit"
    );
}

/// A host stopped by its own tree runs nothing: it cannot answer a list or run a kill. The server
/// still owns it and its number is reserved, so a request wakes it with a signal, and both calls
/// finish instead of waiting on a process that will never run on its own.
#[cfg(target_os = "macos")]
#[test]
fn a_stopped_host_is_woken_for_a_list_and_a_kill() {
    // The list half: a kept tree whose root has exited. Only a kept tree asks the host for its
    // members, so this is the case where a stopped host would block a list.
    let (beat, beat_bash) = scratch("stopped-list");
    let files = Files::new(&beat);
    let (mark, go) = (files.path("mark"), files.path("go"));
    let mut server = Server::start();
    let command = format!(
        "printf 'host %s\n' \"$PPID\"; {} echo started",
        daemon_until_released("bash", &beat_bash, &mark, &go)
    );
    let text = server.ok(json!({
        "action": "start", "command": command, "keepTree": true, "waitMs": 10000
    }));
    assert!(text.contains("[exited "), "{text}");
    let host: i32 = text
        .lines()
        .find_map(|line| line.trim().strip_prefix("host "))
        .and_then(|pid| pid.trim().parse().ok())
        .unwrap_or_else(|| panic!("the root named its parent: {text}"));
    let daemon = wait_for_mark(&mark);
    let task = server.id(1);
    // The daemon is the task's own member, observed before the host is stopped.
    wait_for_held_pid(&mut server, &task, daemon);
    std::fs::write(&go, b"go").unwrap();
    wait_for_heartbeat(&beat);
    // The host is stopped from outside: the reserved, unreaped process the server still owns.
    let mut stopped = Stopped::new(host);
    // A list asks the stopped host which processes the task holds: it answers because the request
    // woke it, and the answer names the daemon rather than blocking on a host that never runs.
    let begun = Instant::now();
    let listing = server.ok(json!({"action": "list"}));
    assert!(
        begun.elapsed() < Duration::from_secs(10),
        "the list waited on a stopped host: {listing}"
    );
    assert!(
        listing.contains(&format!("({daemon})")),
        "the list names the held daemon {daemon}: {listing}"
    );
    stopped.resume();
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 10000}));

    // The kill half: a root that stops its own group, so nothing in the tree runs until the server
    // wakes the host for the kill.
    let mut server = Server::start();
    server.ok(json!({
        "action": "start", "command": "kill -STOP 0; sleep 60", "keepTree": true, "waitMs": 500
    }));
    let task = server.id(1);
    let begun = Instant::now();
    let text = server.ok(json!({"action": "kill", "taskId": task, "waitMs": 20000}));
    assert!(
        begun.elapsed() < Duration::from_secs(15),
        "the kill waited on a stopped host: {text}"
    );
    let listing = server.ok(json!({"action": "list"}));
    assert!(
        !listing.contains("keeps "),
        "the stopped tree ended: {listing}"
    );
}

/// Losing only the host does not end its children, before or after the root's exit.
#[cfg(target_os = "macos")]
#[test]
fn a_crashed_host_leaves_cleanup_unconfirmed() {
    for root_exited in [false, true] {
        let (beat, beat_bash) = scratch("crashed-host");
        let files = Files::new(&beat);
        let pid_file = files.path("pid");
        let root_pid_file = files.path("root-pid");
        let host_pid_file = files.path("host-pid");
        let command = format!(
            "printf '%s' \"$$\" > '{}'; printf '%s' \"$PPID\" > '{}'; (for i in $(seq 200); do echo x >> '{}'; sleep 0.1; done) & printf '%s' \"$!\" > '{}'; {}",
            root_pid_file.display(),
            host_pid_file.display(),
            beat_bash,
            pid_file.display(),
            if root_exited { "echo started" } else { "wait" }
        );
        let mut server = Server::start();
        server.ok(json!({
            "action": "start", "command": command, "keepTree": true, "waitMs": 1000,
        }));
        let host = wait_for_mark(&host_pid_file) as i32;
        let child = Escaped::new(&pid_file);
        let _root = (!root_exited).then(|| Escaped::new(&root_pid_file));
        wait_for_heartbeat(&beat);
        let task = server.id(1);
        if root_exited {
            wait_for_held_pid(&mut server, &task, child.pid as u32);
        }
        // The server stays alive; SIGKILL reaches only its task host.
        assert_eq!(unsafe { libc::kill(host, libc::SIGKILL) }, 0);
        wait_until("the lost host is reported", || {
            let text = server.ok(json!({"action": "list"}));
            (text.contains("Warning:") && text.contains("keeps ")).then_some(())
        });
        for _ in 0..2 {
            let (text, is_error) = server.call(json!({"action": "kill", "taskId": task}));
            assert!(
                is_error,
                "a disconnected host cannot confirm cleanup: {text}"
            );
            let text = server.ok(json!({"action": "list"}));
            assert!(text.contains("keeps "), "{text}");
            assert!(text.contains("Warning:"), "{text}");
        }
        let before = file_len(&beat);
        wait_until("the child still runs after host loss", || {
            (file_len(&beat) > before).then_some(())
        });
        // Escaped rechecks the child's birth before killing it on every exit path; the loop is
        // bounded too, so failed setup cannot leave a permanent writer.
        drop(child);
    }
}

/// A descendant the host observed stays in the tree after it leaves the session, the group, and
/// its parent chain, and a kill reaches it.
#[cfg(target_os = "macos")]
#[test]
fn a_detached_descendant_stays_owned_once_observed() {
    let (beat, beat_bash) = scratch("observed-daemon");
    let files = Files::new(&beat);
    let (mark, go) = (files.path("mark"), files.path("go"));
    let mut server = Server::start();
    let command = format!(
        "{} echo started",
        daemon_until_released("bash", &beat_bash, &mark, &go)
    );
    let text = server.ok(json!({
        "action": "start", "command": command, "keepTree": true, "waitMs": 10000
    }));
    assert!(text.contains("[exited "), "{text}");
    let pid = wait_for_mark(&mark);
    let task = server.id(1);
    // The acknowledgement names this process, under this task, before it detaches.
    wait_for_held_pid(&mut server, &task, pid);
    std::fs::write(&go, b"go").unwrap();
    wait_for_heartbeat(&beat);
    // It has detached by now, and is still the task's: the explicit kill reaches it.
    std::thread::sleep(Duration::from_millis(300));
    let listing = server.ok(json!({"action": "list"}));
    assert!(
        listing
            .lines()
            .any(|line| line.starts_with(bare_id(&task)) && line.contains(&format!("({pid})"))),
        "the detached descendant is still held: {listing}"
    );
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
    assert_stops_growing(&beat);
}

/// Server death ends the trees the host holds, a detached descendant included once it was seen.
#[cfg(target_os = "macos")]
#[test]
fn a_killed_server_takes_its_task_trees_with_it() {
    let files = ["orphaned", "orphaned-daemon"].map(scratch);
    let [(beat, beat_bash), (daemon_beat, daemon_bash)] = &files;
    let daemon_files = Files::new(daemon_beat);
    let (mark, go) = (daemon_files.path("mark"), daemon_files.path("go"));
    let mut server = Server::start();
    let command = format!("{} sleep 300", heartbeat(beat_bash));
    server.ok(json!({"action": "start", "command": command, "keepTree": true, "waitMs": 1000}));
    let command = format!(
        "{} echo started",
        daemon_until_released("bash", daemon_bash, &mark, &go)
    );
    let text = server.ok(json!({
        "action": "start", "command": command, "keepTree": true, "pty": true, "waitMs": 10000
    }));
    assert!(text.contains("keeps "), "{text}");
    let daemon = wait_for_mark(&mark);
    let second = server.id(2);
    wait_for_held_pid(&mut server, &second, daemon);
    std::fs::write(&go, b"go").unwrap();
    wait_for_heartbeat(daemon_beat);
    // SIGKILL: no shutdown code runs, so only the hosts' lifelines can end the trees.
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    for beat in [beat, daemon_beat] {
        assert!(file_len(beat) > 0, "a heartbeat never started");
        assert_stops_growing(beat);
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
#[test]
fn a_killed_server_takes_its_task_trees_with_it() {
    let files = ["orphaned", "orphaned-daemon"].map(scratch);
    let [(beat, beat_bash), (daemon_beat, daemon_bash)] = &files;
    let mut server = Server::start();
    let command = format!("{} sleep 300", heartbeat(beat_bash));
    server.ok(json!({"action": "start", "command": command, "keepTree": true, "waitMs": 1000}));
    let command = format!("{} echo started", daemon("bash", daemon_bash));
    let text = server.ok(json!({"action": "start", "command": command, "keepTree": true, "pty": true, "waitMs": 10000}));
    assert!(text.contains("keeps "), "{text}");
    // SIGKILL: no shutdown code runs, so only the hosts' lifelines can end the trees.
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    for beat in [beat, daemon_beat] {
        assert!(file_len(beat) > 0, "a heartbeat never started");
        assert_stops_growing(beat);
    }
}

/// A process the server may not signal — one sudo runs as root — is a member the host can see and
/// must not end: the kill fails, the task says so, and the tree stays held, as on Linux. The host
/// reads it from the kernel's own table, because libproc answers EPERM for another user's process.
#[cfg(target_os = "macos")]
#[test]
fn a_sudo_child_the_server_may_not_kill_is_held_and_reported() {
    let required = std::env::var("FASTEXEC_REQUIRE_ROOT_TEST").is_ok_and(|value| value == "1");
    let sudo = Command::new("sudo").args(["-n", "true"]).status();
    if !matches!(sudo, Ok(status) if status.success()) {
        assert!(
            !required,
            "FASTEXEC_REQUIRE_ROOT_TEST=1, and a sudo child needs passwordless sudo"
        );
        eprintln!("skipped: a sudo child needs passwordless sudo");
        return;
    }
    let (beat, beat_bash) = scratch("sudo-daemon");
    let files = Files::new(&beat);
    let pid_file = files.path("pid");
    let mut server = Server::start();
    // The daemon runs as root, so this user cannot signal it; it writes its own pid first, and it
    // is bounded, so a failing run leaves nothing behind for long.
    let command = format!(
        "sudo -n bash -c 'echo $$ > {pid}; for i in $(seq 600); do echo x >> {beat_bash}; sleep 0.1; done' & echo started",
        pid = pid_file.display()
    );
    server.ok(json!({
        "action": "start", "command": command, "keepTree": true, "waitMs": 2000
    }));
    wait_for_heartbeat(&beat);
    let rooted = Rooted::new(&pid_file);
    let task = server.id(1);
    // The kill fails and the tree stays held, as on Linux.
    let (text, is_error) = server.call(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
    assert!(
        is_error && text.contains("Cannot terminate"),
        "a member this user may not signal fails the kill: {text}"
    );
    // The refusal is reported, and the task still holds that exact root process.
    let listing = server.ok(json!({"action": "list"}));
    let line = listing
        .lines()
        .find(|line| line.starts_with(bare_id(&task)))
        .unwrap_or_else(|| panic!("the task is listed: {listing}"));
    assert!(
        line.contains("keeps "),
        "the task keeps the tree it could not end: {listing}"
    );
    assert!(
        line.contains(&format!("({})", rooted.pid))
            || line.contains(&format!("pid {}", rooted.pid)),
        "the task still holds the root process {}: {listing}",
        rooted.pid
    );
    assert!(
        listing.contains("Warning:"),
        "the refusal is reported: {listing}"
    );
    let before = file_len(&beat);
    std::thread::sleep(Duration::from_millis(600));
    assert!(
        file_len(&beat) > before,
        "the daemon the server may not signal keeps running"
    );
}

/// A root process a test started, ended when the test ends: the pid it wrote itself and the birth
/// time the kernel reports for it, read through `sysctl`, which answers for every uid. The signal
/// goes out only while that birth time is unchanged, so a reused root pid is never the target; the
/// helper is bounded as well, so a skipped guard still ends.
#[cfg(target_os = "macos")]
struct Rooted {
    pid: i32,
    birth: u64,
}

#[cfg(target_os = "macos")]
impl Rooted {
    fn new(pid_file: &std::path::Path) -> Rooted {
        let pid = wait_until("the root daemon wrote its pid", || {
            std::fs::read_to_string(pid_file)
                .ok()
                .and_then(|text| text.trim().parse::<i32>().ok())
        });
        let birth = native_birth(pid).expect("the kernel describes a root process");
        Rooted { pid, birth }
    }

    fn is_mine(&self) -> bool {
        native_birth(self.pid).is_some_and(|birth| birth == self.birth)
    }
}

#[cfg(target_os = "macos")]
impl Drop for Rooted {
    fn drop(&mut self) {
        if self.is_mine() {
            let _ = Command::new("sudo")
                .args(["-n", "kill", "-9", &self.pid.to_string()])
                .status();
        }
    }
}

/// The start time the kernel reports for a pid, in microseconds, read from the `KERN_PROC_PID` row
/// — the one source that answers for another uid's process.
#[cfg(target_os = "macos")]
fn native_birth(pid: i32) -> Option<u64> {
    let mut buffer = [0_u8; 648];
    let mut size = buffer.len();
    let mut mib = [
        libc::CTL_KERN,
        libc::KERN_PROC,
        libc::KERN_PROC_PID,
        pid as libc::c_int,
    ];
    // SAFETY: the mib is four integers and the buffer is as long as the size passed.
    let result = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            mib.len() as libc::c_uint,
            buffer.as_mut_ptr() as *mut libc::c_void,
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if result != 0 || size != buffer.len() {
        return None;
    }
    let seconds = u64::from_le_bytes(buffer[0..8].try_into().ok()?);
    let micros = u32::from_le_bytes(buffer[8..12].try_into().ok()?) as u64;
    Some(seconds * 1_000_000 + micros)
}

/// A kept writer must not make terminal draining delay the root's independent exit status.
#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_a_continuous_pty_writer_does_not_delay_the_kept_roots_status() {
    let mut server = Server::start();
    let text = server.ok(json!({
        "action": "start", "pty": true, "keepTree": true, "loginShell": false,
        "waitMs": 3000, "killAfterMs": 15000, "maxBytes": 1024,
        "command": "(end=$((SECONDS + 20)); while (( SECONDS < end )); do printf 'background-output\\n'; done) & printf 'root-ended\\n'; exit 23",
    }));
    assert!(text.contains("[exited 23]"), "{text}");
    assert!(text.contains("keeps "), "{text}");
    let task = server.id(1);
    let text = server.ok(json!({
        "action": "kill", "taskId": task, "waitMs": 5000,
    }));
    assert!(text.contains("[exited 23]"), "{text}");
    assert!(!text.contains("keeps "), "{text}");
    assert!(!text.contains("Warning:"), "{text}");
}

/// A host killed externally no longer has a reaper, but its unreaped PID still reserves its group.
#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_a_dead_host_still_cleans_up_its_reserved_process_group() {
    let (beat, beat_bash) = scratch("dead-freebsd-host");
    let mut server = Server::start();
    let command = format!(
        "(for i in {{1..200}}; do printf x >> '{beat_bash}'; sleep 0.1; done) & while [ ! -s '{beat_bash}' ]; do sleep 0.01; done; read -r gate; kill -KILL \"$PPID\"; sleep 20"
    );
    server.ok(json!({
        "action": "start", "eof": false, "command": command, "waitMs": 0,
    }));
    // Start has observed READY before the command is allowed to kill its host.
    let text = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "input": "kill\n", "waitMs": 10000,
    }));
    assert!(text.contains("[exited 137]"), "{text}");
    assert_stops_growing(&beat);
}

/// A trap in bash must survive the process-group signal without losing its task reaper.
#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_realtime_group_signals_preserve_the_host_and_tree_cleanup() {
    for signal in [65, 126] {
        let (beat, beat_bash) = scratch(&format!("realtime-{signal}"));
        let mut server = Server::start();
        let command = format!(
            "trap ':' {signal}; perl -MPOSIX -e 'exit if fork; POSIX::setsid() or die; exec @ARGV' /bin/sh -c 'for i in $(seq 300); do printf x >> \"{beat_bash}\"; sleep 0.1; done'; while [ ! -s '{beat_bash}' ]; do sleep 0.01; done; kill -{signal} 0; printf 'survived\\n'"
        );
        let text = server.ok(json!({
            "action": "start", "command": command, "waitMs": 10000,
        }));
        assert!(text.contains("[exited 0]"), "signal {signal}: {text}");
        assert!(text.contains("survived"), "{text}");
        assert_stops_growing(&beat);
    }
}

/// FreeBSD's descendant query stops at nested reapers; both their membership and kill must recurse.
#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_nested_reapers_keep_detached_descendants_owned() {
    let dir = Scratch::new("nested-reaper");
    std::fs::create_dir(&*dir).unwrap();
    let source = dir.join("reaper.c");
    let fixture = dir.join("reaper");
    std::fs::write(
        &source,
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/freebsd-reaper.c"
        )),
    )
    .unwrap();
    assert!(
        Command::new("cc")
            .arg(&source)
            .arg("-o")
            .arg(&fixture)
            .status()
            .unwrap()
            .success(),
        "the nested reaper fixture did not compile"
    );
    for handover in [false, true] {
        let beat = dir.join(format!("beat-{handover}"));
        let mark = dir.join(format!("pids-{handover}"));
        let release = dir.join("release");
        let release_arg = if handover {
            format!(" '{}'", release.display())
        } else {
            String::new()
        };
        let mut server = Server::start();
        let text = server.ok(json!({
            "action": "start", "keepTree": true, "waitMs": 10000,
            "command": format!("'{}' '{}' '{}'{release_arg} & echo started",
                fixture.display(), beat.display(), mark.display()),
        }));
        assert!(text.contains("keeps "), "{text}");
        wait_for_heartbeat(&beat);
        let pids: Vec<u64> = std::fs::read_to_string(mark)
            .unwrap()
            .split_whitespace()
            .map(|pid| pid.parse().unwrap())
            .collect();
        assert_eq!(pids.len(), 2);
        let text = server.ok(json!({"action": "list"}));
        for &pid in &pids {
            assert!(
                text.contains(&format!("({pid})")),
                "the nested reaper member {pid} was lost: {text}"
            );
        }
        if handover {
            std::fs::write(release, b"exit").unwrap();
            wait_until(
                "the nested reaper naturally hands its child to the host",
                || {
                    let text = server.ok(json!({"action": "list"}));
                    (text.contains("keeps ")
                        && !text.contains(&format!("({})", pids[0]))
                        && text.contains(&format!("({})", pids[1])))
                    .then_some(())
                },
            );
            let before = file_len(&beat);
            wait_until("the handed-over child still writes", || {
                (file_len(&beat) > before).then_some(())
            });
        }
        server.ok(json!({"action": "kill", "taskId": server.id(1), "waitMs": 5000}));
        assert_stops_growing(&beat);
    }
}

#[test]
fn yolo_overrides_stdio_tool_annotations_and_rejects_invalid_values() {
    let defaults = [
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
    ];
    for setting in [None, Some("false")] {
        let mut server = Server::start_with(|command| {
            if let Some(value) = setting {
                command.env("FASTEXEC_YOLO", value);
            }
        });
        let listed = server.request("tools/list", json!({}));
        let tools = listed["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 4, "{listed}");
        for (name, expected) in &defaults {
            let tool = tools
                .iter()
                .find(|tool| tool["name"] == *name)
                .unwrap_or_else(|| panic!("missing {name} in {listed}"));
            assert_eq!(&tool["annotations"], expected, "{setting:?} {name}: {tool}");
        }
    }

    let mut yolo = Server::start_with(|command| {
        command.env("FASTEXEC_YOLO", "true");
    });
    let listed = yolo.request("tools/list", json!({}));
    let tools = listed["result"]["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 4, "{listed}");
    for (name, expected) in &defaults {
        let tool = tools
            .iter()
            .find(|tool| tool["name"] == *name)
            .unwrap_or_else(|| panic!("missing {name} in {listed}"));
        assert_eq!(
            tool["annotations"],
            json!({
                "title": expected["title"],
                "readOnlyHint": true,
                "destructiveHint": false,
                "idempotentHint": false,
                "openWorldHint": false,
            }),
            "yolo=true {name}: {tool}"
        );
    }

    let mut invalid: Vec<std::ffi::OsString> = ["", "on", "off", "1", "0", "bogus"]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        invalid.push(std::ffi::OsString::from_vec(vec![0xff]));
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt;
        invalid.push(std::ffi::OsString::from_wide(&[0xD800]));
    }
    for bad in invalid {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        crate::support::scrub_fastexec_env(&mut command);
        let output = command
            .env("FASTEXEC_YOLO", &bad)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{bad:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("fastexec: invalid FASTEXEC_YOLO ")
                && stderr.contains("; expected true or false")
                && !stderr.contains("Ok(")
                && !stderr.contains("Err("),
            "{bad:?}: {stderr:?}"
        );
    }
}

fn schema_description_words(node: &Value) -> usize {
    match node {
        Value::Object(fields) => fields
            .iter()
            .map(|(key, value)| {
                if key == "description" {
                    value.as_str().unwrap().split_whitespace().count()
                } else {
                    schema_description_words(value)
                }
            })
            .sum(),
        Value::Array(items) => items.iter().map(schema_description_words).sum(),
        _ => 0,
    }
}

#[test]
fn server_instructions_and_tool_descriptions_stay_within_their_word_budget() {
    let mut disabled = Server::start();
    let init_off = disabled.request(
        "initialize",
        json!({"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"probe","version":"0"}}),
    );
    let instructions_off = init_off["result"]["instructions"].as_str().unwrap();
    assert!(!instructions_off.contains("codemode"), "{instructions_off}");

    let mut enabled = codemode_server();
    let init_on = enabled.request(
        "initialize",
        json!({"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"probe","version":"0"}}),
    );
    let instructions_on = init_on["result"]["instructions"].as_str().unwrap();
    assert!(
        instructions_on.contains("codemode") && instructions_on.contains("Prefer codemode"),
        "{instructions_on}"
    );

    // Include decoding guidance; exclude the host line, whose paths may contain spaces.
    let platform_words = if cfg!(windows) {
        59
    } else if cfg!(target_os = "macos") {
        69
    } else {
        0
    };
    for (server, instructions, baseline) in [
        (&mut disabled, instructions_off, 2300),
        (&mut enabled, instructions_on, 3100),
    ] {
        let listed = server.request("tools/list", json!({}));
        let mut words = instructions.split_whitespace().count();
        for tool in listed["result"]["tools"].as_array().unwrap() {
            let text = tool["description"].as_str().unwrap();
            let text = text.split_once("\n\nHost: ").map_or(text, |(body, _)| body);
            words +=
                text.split_whitespace().count() + schema_description_words(&tool["inputSchema"]);
        }
        let limit = baseline + platform_words;
        assert!(words <= limit, "{words} advertised words exceed {limit}");
    }
}

#[test]
fn transfer_over_local_stdio_serves_put_head_get_and_validates_settings() {
    let dir = Scratch::new("stdio-transfer");
    std::fs::create_dir_all(&*dir).unwrap();
    let blob = dir.join("blob.bin");
    std::fs::write(&blob, [0u8, 1, 2, 3]).unwrap();

    // Off by default and when explicitly false; unused transfer env vars are not read.
    for setting in [None, Some("false")] {
        let mut server = Server::start_with(|command| {
            command
                .current_dir(&*dir)
                .env("FASTEXEC_PUBLIC_URL", "not-a-url")
                .env("FASTEXEC_TRANSFER_ADDR", "not-an-addr");
            if let Some(value) = setting {
                command.env("FASTEXEC_TRANSFER", value);
            }
        });
        let listed = server.request("tools/list", json!({}));
        let tools = listed["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 4, "{listed}");
        assert!(
            !tools.iter().any(|tool| tool["name"] == "transfer"),
            "{listed}"
        );
        let refused = server.request(
            "tools/call",
            json!({"name": "transfer", "arguments": {"path": "blob.bin"}}),
        );
        assert_eq!(refused["error"]["message"], "tool not found", "{refused}");
        let read_bin = server.request(
            "tools/call",
            json!({"name": "read", "arguments": {"path": "blob.bin"}}),
        )["result"]
            .clone();
        assert_eq!(read_bin["isError"], true, "{read_bin}");
        let msg = read_bin["content"][0]["text"].as_str().unwrap();
        assert!(msg.ends_with("Inspect it with exec."), "{msg}");
        assert!(!msg.contains("download it with transfer"), "{msg}");
    }

    // Enabled on local stdio with default and YOLO annotations, binary hint linkage, and PUT/HEAD/GET.
    for yolo in [false, true] {
        let mut server = Server::start_with(|command| {
            command
                .current_dir(&*dir)
                .env("FASTEXEC_TRANSFER", "true")
                .env("FASTEXEC_CODEMODE", "true")
                .env("FASTEXEC_YOLO", yolo.to_string());
        });
        let listed = server.request("tools/list", json!({}));
        let tools = listed["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 6, "{listed}");
        let transfer = tools
            .iter()
            .find(|tool| tool["name"] == "transfer")
            .unwrap();
        if yolo {
            assert_eq!(
                transfer["annotations"],
                json!({
                    "title": "Transfer file",
                    "readOnlyHint": true,
                    "destructiveHint": false,
                    "idempotentHint": false,
                    "openWorldHint": false,
                }),
                "{transfer}"
            );
        } else {
            assert_eq!(
                transfer["annotations"],
                json!({
                    "title": "Transfer file",
                    "destructiveHint": true,
                    "openWorldHint": true,
                }),
                "{transfer}"
            );
        }

        let read_bin = server.request(
            "tools/call",
            json!({"name": "read", "arguments": {"path": "blob.bin"}}),
        )["result"]
            .clone();
        assert!(
            read_bin["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("Inspect it with exec or download it with transfer."),
            "{read_bin}"
        );
        let cm_bin = code_result(
            &mut server,
            json!({"code": "try { await tools.read({path: 'blob.bin'}); return 'ok'; } catch (e) { return e.message; }"}),
        );
        assert!(
            code_text(&cm_bin).contains("download it with transfer"),
            "{cm_bin}"
        );

        let target_name = format!("roundtrip-{yolo}.bin");
        let minted = server.request(
            "tools/call",
            json!({"name": "transfer", "arguments": {"path": &target_name}}),
        )["result"]
            .clone();
        assert_eq!(minted["isError"], false, "{minted}");
        assert!(minted.get("structuredContent").is_none(), "{minted}");
        let url = minted["content"][0]["text"]
            .as_str()
            .unwrap()
            .lines()
            .next()
            .unwrap();
        let rest = url.strip_prefix("http://").expect("http:// URL");
        let (authority, path_part) = rest.split_once('/').unwrap();
        let addr: std::net::SocketAddr = authority.parse().unwrap();
        let target = format!("/{path_part}");

        let (status, headers, _) = crate::transfer_http::send(addr, "HEAD", &target, b"");
        assert_eq!(status, 200);
        assert!(
            headers
                .iter()
                .any(|(k, v)| k == "content-length" && v == "0")
        );

        let (status, _, body) =
            crate::transfer_http::send(addr, "PUT", &format!("{target}?offset=0"), b"");
        assert_eq!(status, 200);
        assert_eq!(body, b"0");

        let payload = b"stdio-transfer-payload";
        let (status, _, body) = crate::transfer_http::send(addr, "PUT", &target, payload);
        assert_eq!(status, 200);
        assert_eq!(body, payload.len().to_string().as_bytes());

        let (status, headers, _) = crate::transfer_http::send(addr, "HEAD", &target, b"");
        assert_eq!(status, 200);
        assert!(
            headers
                .iter()
                .any(|(k, v)| k == "content-length" && v == &payload.len().to_string())
        );

        let (status, _, downloaded) = crate::transfer_http::send(addr, "GET", &target, b"");
        assert_eq!(status, 200);
        assert_eq!(downloaded, payload);
        assert_eq!(std::fs::read(dir.join(&target_name)).unwrap(), payload);

        let (not_found, _, _) = crate::transfer_http::send(addr, "GET", "/mcp", b"");
        assert_eq!(not_found, 404);
    }

    // FASTEXEC_TRANSFER_ADDR + FASTEXEC_PUBLIC_URL bind the fixed port while minting under the public root.
    let (fixed_addr, mut proxied) = (0..8)
        .find_map(|attempt| {
            let candidate = crate::transfer_http::free_loopback_addr();
            match Server::try_start_with(|command| {
                command
                    .current_dir(&*dir)
                    .env("FASTEXEC_TRANSFER", "true")
                    .env("FASTEXEC_TRANSFER_ADDR", candidate.to_string())
                    .env("FASTEXEC_PUBLIC_URL", "https://files.example/stdio");
            }) {
                Ok(server) => Some((candidate, server)),
                Err(stderr) if attempt < 7 && stderr.contains("cannot listen on") => None,
                Err(stderr) => panic!("failed to start proxied stdio server: {stderr}"),
            }
        })
        .expect("bound fixed loopback port for stdio transfer");
    let minted = proxied.request(
        "tools/call",
        json!({"name": "transfer", "arguments": {"path": "proxied.bin"}}),
    )["result"]
        .clone();
    assert_eq!(minted["isError"], false, "{minted}");
    assert!(minted.get("structuredContent").is_none(), "{minted}");
    let url = minted["content"][0]["text"]
        .as_str()
        .unwrap()
        .lines()
        .next()
        .unwrap();
    let id = url
        .strip_prefix("https://files.example/stdio/t/")
        .unwrap_or_else(|| panic!("unexpected proxied URL: {url}"));
    let target = format!("/t/{id}");
    assert_eq!(
        crate::transfer_http::send(fixed_addr, "PUT", &format!("{target}?offset=0"), b"").0,
        200
    );
    assert_eq!(
        crate::transfer_http::send(fixed_addr, "PUT", &target, b"via-proxy").0,
        200
    );
    assert_eq!(
        crate::transfer_http::send(fixed_addr, "GET", &target, b"").2,
        b"via-proxy"
    );
    drop(proxied);

    // Wildcard FASTEXEC_TRANSFER_ADDR=0.0.0.0:0 with FASTEXEC_HTTP_ALLOW_REMOTE=1 requires FASTEXEC_PUBLIC_URL.
    let mut wildcard_cmd = Command::new(env!("CARGO_BIN_EXE_fastexec"));
    crate::support::scrub_fastexec_env(&mut wildcard_cmd);
    let wildcard_without_root = wildcard_cmd
        .env("FASTEXEC_TRANSFER", "true")
        .env("FASTEXEC_HTTP_ALLOW_REMOTE", "1")
        .env("FASTEXEC_TRANSFER_ADDR", "0.0.0.0:0")
        .stdin(Stdio::null())
        .output()
        .unwrap();
    assert_eq!(wildcard_without_root.status.code(), Some(1));
    let wildcard_err = String::from_utf8_lossy(&wildcard_without_root.stderr);
    assert!(
        wildcard_err.contains(
            "FASTEXEC_TRANSFER_ADDR 0.0.0.0:0 is an unspecified address; set FASTEXEC_PUBLIC_URL"
        ),
        "{wildcard_err:?}"
    );

    let mut wildcard_with_root = Server::start_with(|command| {
        command
            .current_dir(&*dir)
            .env("FASTEXEC_TRANSFER", "true")
            .env("FASTEXEC_HTTP_ALLOW_REMOTE", "1")
            .env("FASTEXEC_TRANSFER_ADDR", "0.0.0.0:0")
            .env("FASTEXEC_PUBLIC_URL", "https://files.example/");
    });
    let wildcard_minted = wildcard_with_root.request(
        "tools/call",
        json!({"name": "transfer", "arguments": {"path": "wildcard.bin"}}),
    )["result"]
        .clone();
    assert_eq!(wildcard_minted["isError"], false, "{wildcard_minted}");
    assert!(
        wildcard_minted.get("structuredContent").is_none(),
        "{wildcard_minted}"
    );
    assert!(
        wildcard_minted["content"][0]["text"]
            .as_str()
            .unwrap()
            .lines()
            .next()
            .unwrap()
            .starts_with("https://files.example/t/"),
        "{wildcard_minted}"
    );
    drop(wildcard_with_root);

    // Invalid FASTEXEC_TRANSFER, FASTEXEC_PUBLIC_URL, and FASTEXEC_TRANSFER_ADDR fail startup with exit 1.
    let mut invalid: Vec<std::ffi::OsString> = ["", "on", "off", "1", "0", "bogus"]
        .into_iter()
        .map(std::ffi::OsString::from)
        .collect();
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        invalid.push(std::ffi::OsString::from_vec(vec![0xff]));
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt;
        invalid.push(std::ffi::OsString::from_wide(&[0xD800]));
    }
    for bad in invalid {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        crate::support::scrub_fastexec_env(&mut command);
        let output = command
            .env("FASTEXEC_TRANSFER", &bad)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{bad:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("fastexec: invalid FASTEXEC_TRANSFER ")
                && stderr.contains("; expected true or false"),
            "{bad:?}: {stderr:?}"
        );
    }
    for (var, bad, expected) in [
        (
            "FASTEXEC_PUBLIC_URL",
            "ftp://bad.example/",
            "FASTEXEC_PUBLIC_URL",
        ),
        (
            "FASTEXEC_TRANSFER_ADDR",
            "typo",
            "FASTEXEC_TRANSFER_ADDR typo is not an address",
        ),
        (
            "FASTEXEC_TRANSFER_ADDR",
            "0.0.0.0:0",
            "FASTEXEC_TRANSFER_ADDR 0.0.0.0:0 is not a loopback address",
        ),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        crate::support::scrub_fastexec_env(&mut command);
        let output = command
            .env("FASTEXEC_TRANSFER", "true")
            .env(var, bad)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{var}={bad}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains(expected), "{var}={bad}: {stderr:?}");
    }
}

#[test]
fn instructions_file_appends_to_stdio_initialize_and_rejects_invalid_files() {
    let dir = Scratch::new("instructions-file");
    std::fs::create_dir_all(&*dir).unwrap();
    let file = dir.join("notes.txt");
    std::fs::write(&file, "\u{feff}    indented line\r\nsecond line\r\n").unwrap();

    let init_params = json!({
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "probe", "version": "0"},
    });

    let mut plain = Server::start();
    let base = plain.request("initialize", init_params.clone())["result"]["instructions"]
        .as_str()
        .unwrap()
        .to_owned();
    drop(plain);

    let mut with_file = Server::start_with(|command| {
        command
            .current_dir(&*dir)
            .env("FASTEXEC_INSTRUCTIONS_FILE", "notes.txt");
    });
    let actual = with_file.request("initialize", init_params)["result"]["instructions"]
        .as_str()
        .unwrap()
        .to_owned();
    drop(with_file);

    assert_eq!(
        actual,
        format!("{base}\n\n    indented line\nsecond line\n")
    );

    let oversized = dir.join("oversized.txt");
    std::fs::write(&oversized, vec![b'a'; 16_385]).unwrap();
    for (case, value, expected) in [
        ("missing", "missing.txt", "FASTEXEC_INSTRUCTIONS_FILE"),
        ("oversized", "oversized.txt", "exceeds 16384 bytes"),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        crate::support::scrub_fastexec_env(&mut command);
        let output = command
            .current_dir(&*dir)
            .env("FASTEXEC_INSTRUCTIONS_FILE", value)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{case}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("FASTEXEC_INSTRUCTIONS_FILE") && stderr.contains(expected),
            "{case}: {stderr:?}"
        );
    }
}

const MOVE_GUIDANCE_POINTER: &str = "See the instructions tool for host-specific guidance.";

fn stdio_init_params() -> Value {
    json!({
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "probe", "version": "0"},
    })
}

fn find_tool<'a>(listed: &'a Value, name: &str) -> Option<&'a Value> {
    listed["result"]["tools"]
        .as_array()
        .expect("tools/list array")
        .iter()
        .find(|tool| tool["name"] == name)
}

fn assert_instructions_call_ok(message: &Value, expected: &str, context: &str) {
    let result = &message["result"];
    assert!(result.is_object(), "{context}: {message}");
    assert_ne!(result["isError"], true, "{context}: {message}");
    assert!(
        result.get("structuredContent").is_none(),
        "{context}: unexpected structuredContent in {message}"
    );
    let content = result["content"]
        .as_array()
        .unwrap_or_else(|| panic!("{context}: missing content array in {message}"));
    assert_eq!(content.len(), 1, "{context}: {message}");
    assert_eq!(content[0]["type"], "text", "{context}: {message}");
    assert_eq!(content[0]["text"].as_str(), Some(expected), "{context}");
}

#[test]
fn instructions_tool_off_and_unset_preserve_v070_behavior() {
    let dir = Scratch::new("instructions-tool-off");
    std::fs::create_dir_all(&*dir).unwrap();
    let file = dir.join("extra.txt");
    std::fs::write(&file, "    host note\r\nsecond line  \r\n").unwrap();

    let mut plain = Server::start();
    let base = plain.request("initialize", stdio_init_params())["result"]["instructions"]
        .as_str()
        .unwrap()
        .to_owned();
    let listed = plain.request("tools/list", json!({}));
    assert!(
        find_tool(&listed, "instructions").is_none(),
        "unset should omit instructions tool: {listed}"
    );
    drop(plain);

    for mode in [None, Some("off")] {
        for with_file in [false, true] {
            let mut server = Server::start_with(|command| {
                command.current_dir(&*dir);
                if let Some(mode) = mode {
                    command.env("FASTEXEC_INSTRUCTIONS_TOOL", mode);
                }
                if with_file {
                    command.env("FASTEXEC_INSTRUCTIONS_FILE", "extra.txt");
                }
            });
            let init = server.request("initialize", stdio_init_params())["result"]["instructions"]
                .as_str()
                .unwrap()
                .to_owned();
            let expected = if with_file {
                format!("{base}\n\n    host note\nsecond line  \n")
            } else {
                base.clone()
            };
            assert_eq!(init, expected, "mode={mode:?} with_file={with_file}");

            let listed = server.request("tools/list", json!({}));
            assert!(
                find_tool(&listed, "instructions").is_none(),
                "mode={mode:?} with_file={with_file} should omit instructions tool: {listed}"
            );
        }
    }
}

#[test]
fn instructions_tool_move_mode_contract_and_annotations() {
    let dir = Scratch::new("instructions-tool-move");
    std::fs::create_dir_all(&*dir).unwrap();

    // Include leading indentation, UTF-8 text, >512 bytes before the unique marker, and trailing spaces.
    let marker = "MARKER-AFTER-512-BYTES-7c4e9b";
    let padding = "x".repeat(540);
    let raw =
        format!("\u{feff}    Opening directive — 日本語 café  \r\n{padding}\r\n{marker}   \r\n");
    let normalized = format!("    Opening directive — 日本語 café  \n{padding}\n{marker}   \n");
    assert!(normalized.find(marker).unwrap() > 512);
    std::fs::write(dir.join("host.txt"), raw).unwrap();

    let mut plain = Server::start();
    let base = plain.request("initialize", stdio_init_params())["result"]["instructions"]
        .as_str()
        .unwrap()
        .to_owned();
    drop(plain);

    let expected_proto = format!("{base}\n\n{MOVE_GUIDANCE_POINTER}");
    let expected_annotations = json!({
        "title": "Server instructions",
        "readOnlyHint": true,
        "destructiveHint": false,
        "idempotentHint": true,
        "openWorldHint": false,
    });
    let expected_schema = json!({
        "type": "object",
        "properties": {},
        "additionalProperties": false,
    });

    for yolo in ["false", "true"] {
        let mut server = Server::start_with(|command| {
            command
                .current_dir(&*dir)
                .env("FASTEXEC_INSTRUCTIONS_TOOL", "move")
                .env("FASTEXEC_INSTRUCTIONS_FILE", "host.txt")
                .env("FASTEXEC_YOLO", yolo);
        });

        let init = server.request("initialize", stdio_init_params())["result"]["instructions"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(init, expected_proto, "yolo={yolo}");
        assert!(!init.contains(marker), "file text leaked into initialize");

        let listed = server.request("tools/list", json!({}));
        let tool = find_tool(&listed, "instructions")
            .unwrap_or_else(|| panic!("missing instructions tool: {listed}"));
        assert_eq!(tool["description"].as_str(), Some(normalized.as_str()));
        assert_eq!(tool["inputSchema"], expected_schema, "{tool}");
        let mut annotations = expected_annotations.clone();
        annotations["idempotentHint"] = json!(yolo == "false");
        assert_eq!(tool["annotations"], annotations, "yolo={yolo}: {tool}");

        for (label, params) in [
            (
                "empty object",
                json!({"name": "instructions", "arguments": {}}),
            ),
            ("omitted arguments", json!({"name": "instructions"})),
            (
                "null arguments",
                json!({"name": "instructions", "arguments": null}),
            ),
        ] {
            let reply = server.request("tools/call", params);
            assert_instructions_call_ok(&reply, &normalized, &format!("yolo={yolo} {label}"));
        }

        let extra_err = server.request(
            "tools/call",
            json!({"name": "instructions", "arguments": {"unexpected": true}}),
        );
        assert_eq!(
            extra_err["error"]["code"],
            json!(-32602),
            "extra args must fail with -32602: {extra_err}"
        );

        // Calling instructions must not allocate or leave any tasks.
        let (text, is_error) = server.call(json!({"action": "list", "scope": "all"}));
        assert!(!is_error, "{text}");
        assert!(text.contains("No tasks."), "unexpected tasks: {text}");
    }

    // move without FASTEXEC_INSTRUCTIONS_FILE returns built-in lines on both surfaces.
    let mut no_file = Server::start_with(|command| {
        command.env("FASTEXEC_INSTRUCTIONS_TOOL", "move");
    });
    let init = no_file.request("initialize", stdio_init_params())["result"]["instructions"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(init, base);
    let listed = no_file.request("tools/list", json!({}));
    let tool = find_tool(&listed, "instructions").expect("instructions tool");
    assert_eq!(tool["description"].as_str(), Some(base.as_str()));
    let reply = no_file.request(
        "tools/call",
        json!({"name": "instructions", "arguments": {}}),
    );
    assert_instructions_call_ok(&reply, &base, "move without file");
}

#[test]
fn instructions_tool_copy_mode_with_and_without_file() {
    let dir = Scratch::new("instructions-tool-copy");
    std::fs::create_dir_all(&*dir).unwrap();
    std::fs::write(dir.join("copy.txt"), "    copied guidance — naïve  \r\n").unwrap();

    let mut plain = Server::start();
    let base = plain.request("initialize", stdio_init_params())["result"]["instructions"]
        .as_str()
        .unwrap()
        .to_owned();
    drop(plain);

    for (with_file, expected) in [
        (false, base.clone()),
        (true, format!("{base}\n\n    copied guidance — naïve  \n")),
    ] {
        let mut server = Server::start_with(|command| {
            command
                .current_dir(&*dir)
                .env("FASTEXEC_INSTRUCTIONS_TOOL", "copy");
            if with_file {
                command.env("FASTEXEC_INSTRUCTIONS_FILE", "copy.txt");
            }
        });

        let init = server.request("initialize", stdio_init_params())["result"]["instructions"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(init, expected, "with_file={with_file}");

        let listed = server.request("tools/list", json!({}));
        let tool = find_tool(&listed, "instructions").expect("instructions tool");
        assert_eq!(
            tool["description"].as_str(),
            Some(expected.as_str()),
            "with_file={with_file}"
        );

        let reply = server.request(
            "tools/call",
            json!({"name": "instructions", "arguments": {}}),
        );
        assert_instructions_call_ok(&reply, &expected, &format!("copy with_file={with_file}"));
    }
}

#[test]
fn instructions_tool_16kib_boundary_and_snapshot_immutability() {
    let dir = Scratch::new("instructions-tool-16k");
    std::fs::create_dir_all(&*dir).unwrap();

    let mut plain = Server::start();
    let base = plain.request("initialize", stdio_init_params())["result"]["instructions"]
        .as_str()
        .unwrap()
        .to_owned();
    drop(plain);

    let prefix = "    exact-16k-head\n";
    let suffix = "\nexact-16k-tail   \n";
    let body = format!(
        "{prefix}{}{suffix}",
        "b".repeat(16_384 - prefix.len() - suffix.len())
    );
    assert_eq!(body.len(), 16_384);

    for mode in ["move", "copy"] {
        let file = dir.join(format!("{mode}.txt"));
        std::fs::write(&file, &body).unwrap();

        let mut server = Server::start_with(|command| {
            command
                .current_dir(&*dir)
                .env("FASTEXEC_INSTRUCTIONS_TOOL", mode)
                .env("FASTEXEC_INSTRUCTIONS_FILE", &file);
        });

        let expected_proto = if mode == "move" {
            format!("{base}\n\n{MOVE_GUIDANCE_POINTER}")
        } else {
            format!("{base}\n\n{body}")
        };
        let expected_tool = if mode == "move" {
            body.clone()
        } else {
            format!("{base}\n\n{body}")
        };

        let check = |server: &mut Server, stage: &str| {
            let init = server.request("initialize", stdio_init_params())["result"]["instructions"]
                .as_str()
                .unwrap()
                .to_owned();
            assert_eq!(init, expected_proto, "{mode} {stage} initialize");
            let listed = server.request("tools/list", json!({}));
            let tool = find_tool(&listed, "instructions").expect("instructions tool");
            assert_eq!(
                tool["description"].as_str(),
                Some(expected_tool.as_str()),
                "{mode} {stage} description"
            );
            let reply = server.request(
                "tools/call",
                json!({"name": "instructions", "arguments": {}}),
            );
            assert_instructions_call_ok(&reply, &expected_tool, &format!("{mode} {stage} call"));
        };

        check(&mut server, "initial");
        std::fs::write(&file, "edited after startup\n").unwrap();
        check(&mut server, "after edit");
        std::fs::remove_file(&file).unwrap();
        check(&mut server, "after delete");
    }
}

#[test]
fn instructions_tool_rejects_invalid_env_values() {
    for bad in ["", "true", "false", "MOVE", " off", "copy "] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        crate::support::scrub_fastexec_env(&mut command);
        let output = command
            .env("FASTEXEC_INSTRUCTIONS_TOOL", bad)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "bad={bad:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("FASTEXEC_INSTRUCTIONS_TOOL"),
            "bad={bad:?}: {stderr:?}"
        );
    }

    #[cfg(unix)]
    {
        use std::ffi::OsStr;
        use std::os::unix::ffi::OsStrExt;
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        crate::support::scrub_fastexec_env(&mut command);
        let output = command
            .env("FASTEXEC_INSTRUCTIONS_TOOL", OsStr::from_bytes(&[0xff]))
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "non-utf8");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("FASTEXEC_INSTRUCTIONS_TOOL"),
            "non-utf8: {stderr:?}"
        );
    }
}

#[test]
fn stdio_survives_pre_init_notifications_errors_and_budget_overload_keeping_tasks_alive() {
    use std::io::{BufRead, BufReader, Write};

    let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
    crate::support::scrub_fastexec_env(&mut command);
    let mut child = command
        .env("FASTEXEC_DAEMON", "off")
        .env("FASTEXEC_TEST_MEMORY_LIMIT", "131072")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap()).lines();

    let read_json = |stdout: &mut std::io::Lines<BufReader<std::process::ChildStdout>>| -> Value {
        let line = stdout.next().expect("Unexpected EOF").unwrap();
        serde_json::from_str(&line).unwrap_or_else(|e| panic!("bad json {e}: {line}"))
    };

    // 1. Pre-initialize notifications and malformed notification do not kill the stdio server.
    writeln!(
        stdin,
        "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/roots/list_changed\"}}"
    )
    .unwrap();
    writeln!(
        stdin,
        "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/custom\",\"params\":{{\"x\":1}}}}"
    )
    .unwrap();
    writeln!(
        stdin,
        "{{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\",\"params\":\"bad\"}}"
    )
    .unwrap();

    // 2. Syntax error returns -32700 (id: null) and invalid request/params preserve id.
    writeln!(stdin, "{{broken json").unwrap();
    let parse_err = read_json(&mut stdout);
    assert_eq!(parse_err["error"]["code"], -32700, "{parse_err}");
    assert!(parse_err["id"].is_null(), "{parse_err}");

    writeln!(
        stdin,
        "{{\"jsonrpc\":\"2.0\",\"id\":\"bad-init\",\"method\":\"initialize\",\"params\":123}}"
    )
    .unwrap();
    let invalid_params = read_json(&mut stdout);
    assert_eq!(invalid_params["error"]["code"], -32602, "{invalid_params}");
    assert_eq!(invalid_params["id"], "bad-init", "{invalid_params}");

    // 3. Valid initialize succeeds.
    writeln!(
        stdin,
        "{}",
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": {"name": "stdio-survival", "version": "1"}
            }
        })
    )
    .unwrap();
    let init_res = read_json(&mut stdout);
    assert_eq!(init_res["id"], 1, "{init_res}");
    assert!(init_res.get("result").is_some(), "{init_res}");

    // 4. Start a background local task, then send an over-budget (> 128 KiB limit) request.
    writeln!(
        stdin,
        "{}",
        json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "exec",
                "arguments": {
                    "action": "start",
                    "command": "sleep 30",
                    "waitMs": 50
                }
            }
        })
    )
    .unwrap();
    let start_res = read_json(&mut stdout);
    assert_eq!(start_res["id"], 2, "{start_res}");

    let huge_req = json!({
        "jsonrpc": "2.0",
        "id": "over-budget-req",
        "method": "tools/call",
        "params": {
            "name": "write",
            "arguments": {
                "path": "ignored.txt",
                "content": "z".repeat(80 * 1024)
            }
        }
    });
    writeln!(stdin, "{huge_req}").unwrap();
    let budget_err = read_json(&mut stdout);
    assert_eq!(budget_err["id"], "over-budget-req", "{budget_err}");
    assert_eq!(budget_err["error"]["code"], -32603, "{budget_err}");

    // 5. Session and local background task remain alive!
    writeln!(
        stdin,
        "{}",
        json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "exec",
                "arguments": {
                    "action": "list",
                    "state": "active"
                }
            }
        })
    )
    .unwrap();
    let list_res = read_json(&mut stdout);
    assert_eq!(list_res["id"], 3, "{list_res}");
    assert_eq!(list_res["result"]["isError"], false, "{list_res}");
    let list_text = list_res["result"]["content"][0]["text"].as_str().unwrap();
    assert!(list_text.contains("running"), "{list_text}");

    drop(stdin);
    let status = child.wait().unwrap();
    assert!(status.success(), "{status:?}");
}
