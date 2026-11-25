use super::common::{
    Scratch, Server, assert_stops_growing, bare_id, body_of, code_result, code_text,
    codemode_server, file_len, heartbeat, log_path_of, scratch, status_line, task_id_of,
    wait_for_heartbeat,
};
use serde_json::{Value, json};
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
use std::io::Write;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
#[cfg(not(target_os = "macos"))]
use std::process::Command;
#[cfg(not(target_os = "macos"))]
use std::process::Stdio;
use std::time::{Duration, Instant};

/// Waits up to 30 s for a file a command creates.
fn wait_for_file(path: &std::path::Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "{} never appeared",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Readiness authorizes an immediate kill, so the host must have completed its only spawn.
#[cfg(target_os = "linux")]
#[test]
fn a_task_host_reports_readiness_after_its_command_exists() {
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    for _ in 0..32 {
        let (mut status, status_writer) = std::io::pipe().unwrap();
        let (lifeline_reader, lifeline) = std::io::pipe().unwrap();
        let descriptors = [status_writer.as_raw_fd(), lifeline_reader.as_raw_fd()];
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        crate::support::scrub_fastexec_env(&mut command);
        command
            .arg("--fastexec-task-host")
            .args(descriptors.iter().map(|fd| fd.to_string()))
            .args(["/bin/sh", "-c", "exec sleep 300"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit());
        // SAFETY: only async-signal-safe syscalls execute between fork and exec.
        unsafe {
            command.pre_exec(move || {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                for fd in descriptors {
                    if libc::fcntl(fd, libc::F_SETFD, 0) == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                Ok(())
            });
        }
        let mut host = command.spawn().unwrap();
        drop((status_writer, lifeline_reader));
        let mut ready = [0; 4];
        status.read_exact(&mut ready).unwrap();
        assert_eq!(ready, [0; 4], "the host failed startup");
        let children =
            std::fs::read(format!("/proc/{}/task/{}/children", host.id(), host.id())).unwrap();
        // Dropping the lifeline also exercises cleanup before any user-visible task exists.
        drop(lifeline);
        host.wait().unwrap();
        assert!(
            children.iter().any(|byte| !byte.is_ascii_whitespace()),
            "the host reported READY before creating its command"
        );
    }
}

/// An inherited automatic-reaping policy must not release a task host's PID before its Tree.
#[cfg(unix)]
#[test]
fn a_server_restores_waitable_child_statuses_before_starting_tasks() {
    use std::os::unix::process::CommandExt;
    for (disposition, flags) in [(libc::SIG_IGN, 0), (libc::SIG_DFL, libc::SA_NOCLDWAIT)] {
        let mut server = Server::start_with(|command| unsafe {
            command.pre_exec(move || {
                let mut action: libc::sigaction = std::mem::zeroed();
                action.sa_sigaction = disposition;
                action.sa_flags = flags;
                libc::sigemptyset(&mut action.sa_mask);
                if libc::sigaction(libc::SIGCHLD, &action, std::ptr::null_mut()) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        });
        for pty in [false, true] {
            let text = server.ok(json!({
                "action": "start", "command": "printf child-status; exit 17",
                "pty": pty, "waitMs": 10000
            }));
            assert!(text.contains("[exited 17]"), "{text}");
            assert!(text.contains("child-status"), "{text}");
        }
        let started = server.ok(json!({
            "action": "start", "command": "sleep 30", "waitMs": 0
        }));
        let text = server.ok(json!({"action": "kill", "taskId": task_id_of(&started)}));
        assert!(text.contains("[killed"), "{text}");
        assert!(!text.contains("ended with bash:"), "{text}");
    }
}

#[test]
fn short_command_returns_its_final_result_in_one_call() {
    let mut server = Server::start();
    let text =
        server.ok(json!({"action": "start", "command": "echo hello; echo oops >&2; exit 3"}));
    assert!(!text.contains("ended with bash:"), "{text}");
    assert!(
        text.starts_with(&format!("hello\noops\n\n[exited 3] {}", server.id(1))),
        "{text}"
    );
    let text = server.ok(json!({"action": "start", "command": "true"}));
    assert!(text.starts_with("(no new output)"), "{text}");
    for pty in [false, true] {
        let text = server.ok(json!({"action": "start", "command": "kill -INT $$", "pty": pty}));
        assert!(
            text.contains("[exited 130]"),
            "a signal exit reports 128 + signal (pty={pty}): {text}"
        );
    }
}

#[test]
fn direct_mcp_exec_returns_pure_text_without_structured_content() {
    let mut first_ids = Vec::new();
    for _ in 0..3 {
        let mut server = Server::start();
        let text = server.ok(json!({
            "action": "start", "command": "printf 'MODE_START\\n'; exit 3"
        }));
        assert!(
            text.starts_with(&format!("MODE_START\n\n[exited 3] {}", server.id(1))),
            "{text}"
        );
        first_ids.push(server.id(1));

        let text = server.ok(json!({
            "action": "start", "eof": false, "command": "read -r line; printf '%s\\n' \"$line\"", "waitMs": 0
        }));
        assert!(text.contains("[running] "), "{text}");
        let id = server.id(2);
        let text = server.ok(json!({
            "action": "poll", "taskId": &id, "input": "MODE_POLL\n", "eof": true, "waitMs": 10000
        }));
        assert!(
            text.starts_with(&format!("MODE_POLL\n\n[exited 0] {id}")),
            "{text}"
        );
        let text = server.ok(json!({"action": "poll", "taskId": &id, "waitMs": 0}));
        assert!(text.starts_with("(no new output)"), "{text}");
        assert!(text.contains(&format!("[exited 0] {id}")), "{text}");

        let listed = server.ok(json!({"action": "list"}));
        assert!(
            listed.contains(&format!("{} [exited 0]", bare_id(&id))),
            "{listed}"
        );
        let killed = server.ok(json!({"action": "kill", "taskId": &id}));
        assert!(killed.contains(&format!("[exited 0] {id}")), "{killed}");

        let (text, is_error) = server.call(json!({"action": "poll", "taskId": "missing"}));
        assert!(is_error && text.contains("Unknown taskId"), "{text}");
        let (text, is_error) = server.call(json!({"action": "start"}));
        assert!(
            is_error && text.contains("start needs a non-empty"),
            "{text}"
        );
    }
    // Each server draws its own random IDs, so the servers' first tasks differ.
    let mut distinct = first_ids.clone();
    distinct.sort();
    distinct.dedup();
    assert_eq!(distinct.len(), first_ids.len(), "{first_ids:?}");
}

#[test]
fn kill_after_ms_bounds_task_lifetime_independently_of_waits() {
    let mut server = Server::start();
    let text = server
        .ok(json!({"action": "start", "command": "sleep 30", "killAfterMs": 1000, "waitMs": 0}));
    assert!(
        text.contains("[running]"),
        "the deadline outlives the ended wait: {text}"
    );
    let text = server.ok(json!({"action": "poll", "taskId": server.id(1), "waitMs": 15000}));
    assert!(
        text.contains("[killed") && text.contains("killAfterMs"),
        "{text}"
    );
    // Completion before the deadline and the disabled forms keep the natural result.
    for limit in [json!(60000), json!(0), Value::Null] {
        let text = server.ok(json!({"action": "start", "command": "sleep 1; exit 4", "killAfterMs": limit, "waitMs": 15000}));
        assert!(
            text.contains("[exited 4]") && !text.contains("killAfterMs"),
            "killAfterMs={limit}: {text}"
        );
    }
}

#[test]
fn background_footer_names_unreported_tasks_once_failures_first() {
    let markers = ["footer-0", "footer-7"].map(scratch);
    let [(_, zero), (_, seven)] = &markers;
    let mut server = Server::start();
    // The first task prints once both others have run their last command, and some time after.
    let first = format!(
        "until [ -e {zero} ] && [ -e {seven} ]; do sleep 0.1; done; sleep 2; echo both; sleep 60"
    );
    server.ok(json!({"action": "start", "command": first, "waitMs": 0}));
    server.ok(json!({"action": "start", "command": format!("sleep 1; touch {zero}; exit 0"), "waitMs": 0}));
    server.ok(json!({"action": "start", "command": format!("sleep 1; touch {seven}; exit 7"), "waitMs": 0}));
    let text = server.ok(json!({"action": "poll", "taskId": server.id(1), "waitMs": 60000, "returnWhen": {"outputContains": ["both"]}}));
    let footer = text.lines().last().unwrap();
    let expected = format!(
        "(Background: {} exited 7, {} exited 0.)",
        server.bare_id(3),
        server.bare_id(2)
    );
    assert_eq!(footer, expected, "{text}");

    // Reported completions leave the footer; running tasks stay in it.
    let text = server.ok(json!({"action": "start", "command": "true"}));
    let footer = text.lines().last().unwrap();
    assert!(
        footer.starts_with(&format!("(Background: {} running ", server.bare_id(1)))
            && !footer.contains(&server.bare_id(2)),
        "{text}"
    );

    // Many tasks: three IDs, then counts, within maxBytes.
    for _ in 0..5 {
        server.ok(json!({"action": "start", "command": "sleep 60", "waitMs": 0}));
    }
    let text = server.ok(json!({"action": "start", "command": "seq 1 5000", "maxBytes": 1024}));
    assert!(
        body_of(&text).len() <= 1024,
        "{} bytes",
        body_of(&text).len()
    );
    let footer = text.lines().last().unwrap();
    assert!(
        footer.starts_with(&format!("(Background: {} running ", server.bare_id(9)))
            && footer.ends_with("; 3 more; 6 running; use list.)"),
        "{text}"
    );
    let text = server.ok(json!({"action": "start", "command": "seq 1 5000", "maxBytes": 100}));
    assert!(
        body_of(&text).len() <= 100,
        "{} bytes: {text}",
        body_of(&text).len()
    );
}

#[test]
fn list_fits_max_bytes_and_leaves_the_tasks_it_omits_due() {
    let mut server = Server::start();
    // The tasks end together, after the footers of the starts showed them running; long
    // commands make lines that do not all fit.
    let (go, go_bash) = scratch("list-go");
    let command = format!(
        "until [ -e {go_bash} ]; do sleep 0.1; done; exit 3 # {}",
        "x".repeat(100)
    );
    for _ in 0..8 {
        server.ok(json!({"action": "start", "command": command, "loginShell": false, "waitMs": 0}));
    }
    std::fs::write(&*go, "").unwrap();
    // Wait for every task to end by listing only active tasks, so finished tasks stay unreported.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let active = server.ok(json!({"action": "list", "state": "active"}));
        if active.contains("No active tasks.") || active.contains("No tasks.") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the tasks never finished: {active}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let text = server.ok(json!({"action": "list", "truncate": "head", "maxBytes": 400}));
    assert!(text.len() <= 400, "{} bytes: {text}", text.len());
    let (first, last) = (text.lines().next().unwrap(), text.lines().last().unwrap());
    assert!(
        first.starts_with(&format!("{} [exited 3]", server.bare_id(8))),
        "newest first: {text}"
    );
    assert!(
        first.contains(" · test-"),
        "the list names the session that started a task: {first}"
    );
    assert!(
        last.starts_with("... [")
            && last.ends_with(" tasks omitted; raise maxBytes to see them] ..."),
        "{text}"
    );
    assert!(!text.contains(&server.bare_id(1)), "{text}");
    // The footer still names the tasks the list left out, newest first, and none it showed.
    let listed: Vec<String> = text
        .lines()
        .filter(|line| !line.starts_with("... ["))
        .map(|line| line.split(' ').next().unwrap_or_default().to_string())
        .collect();
    let text = server.ok(json!({"action": "start", "command": "true"}));
    let footer = text.lines().last().unwrap();
    let newest_omitted = (1..=8)
        .rev()
        .map(|n| server.bare_id(n))
        .find(|id| !listed.contains(id))
        .unwrap();
    assert!(
        footer.starts_with(&format!("(Background: {newest_omitted} exited 3")),
        "{text}"
    );
    assert!(
        listed.iter().all(|id| !footer.contains(id)),
        "the footer names a task the list showed: {text}"
    );

    let text = server.ok(json!({"action": "list", "maxBytes": 400}));
    assert!(text.len() <= 400, "{} bytes: {text}", text.len());
    let lines: Vec<&str> = text.lines().collect();
    assert!(
        lines[0].starts_with(&server.bare_id(9))
            && lines.last().unwrap().starts_with(&server.bare_id(1)),
        "head_tail keeps both ends: {text}"
    );
    assert_eq!(
        lines
            .iter()
            .filter(|line| line.starts_with("... ["))
            .count(),
        1,
        "{text}"
    );
    let (text, is_error) =
        server.call(json!({"action": "list", "truncate": "none", "maxBytes": 400}));
    assert!(is_error && text.contains("unknown variant"), "{text}");
}

#[cfg(unix)]
#[test]
fn bash_override_must_exit_successfully() {
    use std::os::unix::fs::PermissionsExt;
    let dir = Scratch::new("fake-bash");
    std::fs::create_dir_all(&*dir).unwrap();
    let fake = dir.join("bash");
    std::fs::write(&fake, "#!/bin/sh\necho 'GNU bash, version 5.2'\nexit 1\n").unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut server = Server::start_with(|command| {
        command.env("FASTEXEC_BASH", &fake);
    });
    let (text, is_error) = server.call(json!({"action": "start", "command": "true"}));
    assert!(is_error && text.contains("Invalid FASTEXEC_BASH"), "{text}");
}

#[test]
fn long_command_yields_then_poll_returns_only_unseen_output() {
    let mut server = Server::start();
    let text = server.ok(json!({"action": "start", "command": "echo first; sleep 4; echo second", "waitMs": 30000, "returnWhen": {"outputContains": ["first"]}}));
    assert!(text.contains("[running]"), "{text}");
    assert!(text.starts_with("first\n"), "{text}");
    let text = server.ok(json!({"action": "poll", "taskId": server.id(1), "waitMs": 30000}));
    assert!(text.contains("[exited 0]"), "{text}");
    assert!(
        text.starts_with("second\n") && !text.contains("first") && !text.contains("wait:"),
        "a wait without returnWhen adds no note: {text}"
    );
}

#[test]
fn a_hundred_byte_result_keeps_the_whole_task_handle() {
    let mut server = Server::start();
    // A text client reads the task ID from the status line, so a small window must not cut
    // through it even when the root has already exited and its processes are still held.
    let text = server.ok(json!({
        "action": "start",
        "command": "sleep 300 & exit 137",
        "keepTree": true,
        "waitMs": 60000,
        "maxBytes": 100,
    }));
    let window = body_of(text.split("\n\nWarning:").next().unwrap());
    assert!(window.len() <= 100, "the output window fits: {text:?}");
    if text.len() > 100 {
        assert!(
            text.contains(
                "Warning: processes it keeps hold the output open; later output may follow"
            ),
            "only the complete warning exceeds the window budget: {text:?}"
        );
    }
    let status = text
        .lines()
        .find(|line| line.starts_with("[exited 137] "))
        .unwrap_or_else(|| panic!("no status line in {text:?}"));
    let id = status
        .strip_prefix("[exited 137] ")
        .and_then(|rest| rest.split(' ').next())
        .unwrap_or_else(|| panic!("no task ID in {text:?}"))
        .to_string();
    let (task, reader) = id.split_once('.').expect("a reader handle");
    assert_eq!(task.len(), 10, "the whole task ID survives: {text:?}");
    assert_eq!(reader.len(), 4, "the whole reader key survives: {text:?}");
    let listed = server.ok(json!({"action": "list"}));
    assert!(
        listed.contains(task) && !listed.contains(&id),
        "list names the bare task: {listed:?}"
    );
    server.ok(json!({"action": "kill", "taskId": id}));
}

#[test]
fn tiny_stream_and_transcript_windows_keep_requested_ends_and_truthful_omissions() {
    let mut server = Server::start();
    for mode in ["head", "tail", "head_tail"] {
        let text = server.ok(json!({"action": "start",
            "command": "for i in {1..50}; do printf 'line%03d\\n' \"$i\"; done",
            "pty": true, "loginShell": false, "waitMs": 10000,
            "truncate": mode, "maxBytes": 100}));
        let id = task_id_of(&text).to_owned();
        let transcript_text = server.ok(json!({"action": "transcript",
            "taskId": id, "truncate": mode, "maxBytes": 100}));
        for text in [&text, &transcript_text] {
            let body = body_of(text);
            assert!(body.len() <= 100, "{body}");
            if mode != "tail" {
                assert!(body.starts_with("line001"), "{mode}: text={text:?}");
            }
            if mode != "head" {
                assert!(body.ends_with("line050"), "{mode}: text={text:?}");
            }
            let shown: Vec<u64> = body
                .lines()
                .filter_map(|line| line.strip_prefix("line").and_then(|n| n.parse().ok()))
                .collect();
            let (omitted, first, last) = if let Some(marker) = body
                .lines()
                .find(|line| line.starts_with("... [") && line.contains("lines omitted"))
            {
                let omitted: u64 = marker
                    .strip_prefix("... [")
                    .and_then(|s| s.split(' ').next())
                    .and_then(|n| n.parse().ok())
                    .unwrap_or_else(|| panic!("{mode}: bad marker {marker:?}"));
                let range_part = marker
                    .rsplit_once("lines ")
                    .and_then(|(_, rest)| rest.split(']').next())
                    .unwrap_or_else(|| panic!("{mode}: missing range in {marker:?}"));
                let (first, last): (u64, u64) = range_part
                    .split_once('-')
                    .map(|(a, b)| (a.parse().unwrap(), b.parse().unwrap()))
                    .unwrap_or_else(|| panic!("{mode}: bad range {range_part:?}"));
                (omitted, first, last)
            } else {
                let status = status_line(text);
                let seg = status
                    .split(" · ")
                    .find(|s| s.contains("lines omitted"))
                    .unwrap_or_else(|| panic!("{mode}: missing omission info in {text:?}"));
                let omitted: u64 = seg
                    .split(' ')
                    .next()
                    .and_then(|n| n.parse().ok())
                    .unwrap_or_else(|| panic!("{mode}: bad status segment {seg:?}"));
                let range_part = seg
                    .rsplit_once("lines ")
                    .map(|(_, rest)| rest)
                    .unwrap_or_else(|| panic!("{mode}: missing range in {seg:?}"));
                let (first, last): (u64, u64) = range_part
                    .split_once('-')
                    .map(|(a, b)| {
                        let b = b.split(|c: char| !c.is_ascii_digit()).next().unwrap();
                        (a.parse().unwrap(), b.parse().unwrap())
                    })
                    .unwrap_or_else(|| panic!("{mode}: bad range {range_part:?}"));
                (omitted, first, last)
            };
            assert_eq!(omitted, 50 - shown.len() as u64, "{text}");
            assert_eq!(last - first + 1, omitted, "{text}");
            assert!(shown.iter().all(|n| *n < first || *n > last), "{text}");
        }
    }
}

#[test]
fn head_truncate_stops_at_max_bytes_and_names_the_log() {
    let long = "y".repeat(2000);
    let mut server = Server::start();
    let text = server.ok(json!({
        "action": "start",
        "command": format!("printf '%s\n' {long}; seq 1 40"),
        "truncate": "head",
        "maxBytes": 1000,
        "waitMs": 30000,
    }));
    let log_path = log_path_of(&text).unwrap_or_else(|| panic!("missing log path: {text}"));
    assert!(
        std::path::Path::new(log_path).exists(),
        "the status line names an existing log: {text}"
    );
    assert!(
        text.contains("line cut at "),
        "the oversized first line is cut, not left out: {text}"
    );
    assert!(text.contains("log lines 2-41"), "{text}");
    let body = body_of(&text);
    assert!(body.len() <= 1000, "{} bytes of body: {body:?}", body.len());

    // A valid multibyte line crossing the cap is clipped on a UTF-8 boundary without encodingErrors.
    let text = server.ok(json!({
        "action": "start",
        "command": "printf '中%.0s' $(seq 1 400); echo",
        "truncate": "head",
        "maxBytes": 1000,
        "waitMs": 30000,
    }));
    assert!(!text.contains("invalid in UTF-8"), "{text}");
    assert!(body_of(&text).len() <= 1000, "{text}");
}

#[test]
fn list_head_stops_at_max_bytes_and_reports_the_rows_it_omits() {
    let mut server = Server::start();
    let command = format!("sleep 60 # {}", "z".repeat(300));
    for _ in 0..8 {
        server.ok(json!({"action": "start", "command": command, "waitMs": 0}));
    }
    let text = server.ok(json!({"action": "list", "truncate": "head", "maxBytes": 1000}));
    assert!(text.len() <= 1000, "{} bytes: {text}", text.len());
    assert!(
        text.contains("tasks omitted"),
        "the rows that did not fit are reported: {text}"
    );
    for n in 1..=8 {
        server.ok(json!({"action": "kill", "taskId": server.id(n)}));
    }
}

#[test]
fn tasks_inherit_the_launch_context_the_server_captured_at_startup() {
    let dir = Scratch::new("launch-context");
    std::fs::create_dir_all(&*dir).unwrap();
    let name = dir.file_name().unwrap().to_string_lossy().into_owned();
    let mut server = Server::start_with(|command| {
        command.current_dir(&*dir).env("FASTEXEC_CTX_PROBE", "xyz");
    });
    let text = server.ok(json!({
        "action": "start",
        "command": "printf '%s|%s' \"$FASTEXEC_CTX_PROBE\" \"$(pwd -P)\"",
        "waitMs": 30000,
    }));
    assert!(
        text.contains("xyz|"),
        "the captured environment reached the task: {text}"
    );
    assert!(
        text.contains(&name),
        "a start without cwd ran in the context's directory: {text}"
    );
}

#[cfg(debug_assertions)]
#[test]
fn output_matching_budget_refusal_precedes_poll_input() {
    // Small request/output frames fit; the matcher also reserves decoding and growth.
    let mut server = Server::start_with(|command| {
        command.env("FASTEXEC_TEST_MEMORY_LIMIT", (512 * 1024).to_string());
    });
    let started = server.ok(json!({
        "action": "start",
        "command": "IFS= read -r value; printf '%s\\n' \"$value\"",
        "eof": false,
        "waitMs": 0,
        "maxBytes": 100,
        "killAfterMs": 30000
    }));
    let task = task_id_of(&started).to_owned();
    let (text, failed) = server.call(json!({
        "action": "poll",
        "taskId": &task,
        "input": "refused\n",
        "returnWhen": {"outputContains": ["accepted"]},
        "waitMs": 1000,
        "maxBytes": 100
    }));
    assert!(failed, "{text}");
    assert!(text.contains("cannot fit output matching"), "{text}");
    assert!(text.contains(bare_id(&task)), "{text}");
    let text = server.ok(json!({
        "action": "poll",
        "taskId": &task,
        "input": "accepted\n",
        "eof": true,
        "waitMs": 10000,
        "maxBytes": 100
    }));
    assert_eq!(body_of(&text).trim(), "accepted", "{text}");
    assert!(text.contains("[exited 0]"), "{text}");
}

#[test]
fn pipe_input_arrives_verbatim_and_eof_closes_stdin() {
    // Git for Windows' bin/bash.exe launcher retains an inherited stdin handle.
    // Use discovery's real usr/bin/bash.exe so closing fd 0 closes the pipe reader.
    let mut server = Server::start_with(|command| {
        command.env_remove("FASTEXEC_BASH");
    });
    server.ok(json!({"action": "start", "eof": false, "command": "while IFS= read -r l || [ -n \"$l\" ]; do echo \"got:$l\"; done; echo end", "waitMs": 0}));
    let text = server.ok(json!({"action": "poll", "taskId": server.id(1), "input": "a b\n", "waitMs": 30000, "returnWhen": {"outputContains": ["got:a b"]}}));
    assert!(text.starts_with("got:a b\n"), "{text}");
    let text = server
        .ok(json!({"action": "poll", "taskId": server.id(1), "input": "c", "eof": true, "waitMs": 10000}));
    assert!(text.contains("[exited 0]"), "{text}");
    assert!(text.starts_with("got:c\nend\n"), "{text}");
    let (text, is_error) =
        server.call(json!({"action": "poll", "taskId": server.id(1), "input": "late\n"}));
    assert!(is_error && text.contains("exited"), "{text}");
    let text = server.ok(json!({"action": "poll", "taskId": server.id(1), "waitMs": 0}));
    assert!(
        text.contains("[exited 0]"),
        "a plain poll still reads a finished task: {text}"
    );

    // A program that closes its stdin refuses later input instead of queueing it.
    let text = server.ok(json!({"action": "start", "eof": false, "command": "exec 0<&-; echo stdin-closed; sleep 30", "waitMs": 30000, "returnWhen": {"outputContains": ["stdin-closed"]}}));
    assert!(text.starts_with("stdin-closed\n"), "{text}");
    let (text, is_error) = server
        .call(json!({"action": "poll", "taskId": server.id(2), "input": "x\n", "waitMs": 50}));
    assert!(is_error && text.contains("stdin is closed"), "{text}");
    let (text, is_error) = server.call(json!({"action": "kill", "taskId": server.id(2)}));
    assert!(is_error, "the task must retain its input failure: {text}");
    assert!(text.contains("[killed") && text.contains("stdin"), "{text}");
}

#[test]
fn return_when_ends_a_wait_on_unseen_output_text_or_quiet() {
    let mut server = Server::start();
    let command =
        "printf 'boot\\n'; sleep 0.5; printf '\\033[32mServer\\033[0m listening\\n'; sleep 30";
    let begun = Instant::now();
    let text = server.ok(json!({
        "action": "start", "command": command, "waitMs": 20000,
        "returnWhen": {"outputContains": ["error:", "Server listening"]}
    }));
    assert!(begun.elapsed() < Duration::from_secs(10), "{text}");
    assert!(text.contains("[running]"), "{text}");
    assert!(
        text.contains("wait: output matched \"Server listening\""),
        "{text}"
    );
    // Output an earlier result showed does not match again.
    let text = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "waitMs": 500,
        "returnWhen": {"outputContains": ["listening"]}
    }));
    assert!(text.contains("wait: waitMs elapsed"), "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(1)}));

    // Every output restarts the quiet time.
    let command = "echo a; sleep 0.5; echo b; sleep 0.5; echo c; sleep 30";
    let begun = Instant::now();
    let text = server.ok(json!({
        "action": "start", "command": command, "loginShell": false, "waitMs": 20000,
        "returnWhen": {"quietMs": 1500}
    }));
    assert!(begun.elapsed() >= Duration::from_millis(2400), "{text}");
    assert!(text.contains("wait: output quiet"), "{text}");
    assert!(text.starts_with("a\nb\nc\n"), "{text}");
    // The quiet time counts from the start of the wait, so a program already quiet ends a
    // wait that sees no output, after the full time.
    let begun = Instant::now();
    let text = server.ok(json!({
        "action": "poll", "taskId": server.id(2), "waitMs": 20000, "returnWhen": {"quietMs": 300}
    }));
    let waited = begun.elapsed();
    assert!(
        waited >= Duration::from_millis(300) && waited < Duration::from_secs(10),
        "{waited:?} {text}"
    );
    assert!(text.contains("wait: output quiet"), "{text}");
    assert!(text.starts_with("(no new output)"), "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(2)}));

    let text = server.ok(json!({
        "action": "start", "command": "echo hi",
        "returnWhen": {"outputContains": ["nope"]}
    }));
    assert!(text.contains("[exited 0]"), "{text}");
}

#[test]
fn kill_and_root_exit_end_the_whole_process_tree() {
    // Scratch guards are declared before the server so they outlive it, also when unwinding:
    // the server stops its writers before the files are removed.
    let files = [false, true].map(|pty| {
        (
            scratch(&format!("killed-{pty}")),
            scratch(&format!("leaked-{pty}")),
        )
    });
    let mut server = Server::start();
    for (pty, ((killed, killed_bash), (leaked, leaked_bash))) in
        [false, true].into_iter().zip(&files)
    {
        // Job control moves the heartbeat into its own process group.
        let command = format!("set -m; {} bash -c 'sleep 300'", heartbeat(killed_bash));
        let started =
            server.ok(json!({"action": "start", "command": command, "pty": pty, "waitMs": 1000}));
        let id = task_id_of(&started).to_owned();
        wait_for_heartbeat(killed);
        let killed_text = server.ok(json!({"action": "kill", "taskId": &id}));
        assert!(killed_text.contains("[killed"), "pty={pty}: {killed_text}");
        assert_stops_growing(killed);
        let text =
            server.ok(json!({"action": "kill", "taskId": &id, "waitMs": 0, "maxBytes": 100}));
        assert!(text.contains("[killed"), "{text}");
        assert!(
            body_of(&text).len() <= 100,
            "{} bytes: {text}",
            body_of(&text).len()
        );

        let command = format!("{} echo started", heartbeat(leaked_bash));
        let mut text =
            server.ok(json!({"action": "start", "command": command, "pty": pty, "waitMs": 60000}));
        assert!(text.contains("[exited 0]"), "pty={pty}: {text}");
        let leaked_id = task_id_of(&text).to_owned();
        if text.contains("keeps ") {
            // A signalled process may outlast the bounded kill check on a loaded machine.
            // Poll for automatic cleanup; a second kill would hide a leaked descendant.
            text = server.ok(json!({
                "action": "poll", "taskId": &leaked_id, "waitMs": 60000,
            }));
        }
        assert!(
            (text.contains("ended with bash: ") && text.contains("keepTree: true"))
                || text.contains("kept processes ended"),
            "the result names what bash left or confirms its later cleanup (pty={pty}): {text}"
        );
        assert!(text.contains("[exited 0]"), "{text}");
        assert!(!text.contains("keeps "), "{text}");
        assert!(!text.contains("Cannot terminate"), "{text}");
        assert_stops_growing(leaked);
    }
}

#[test]
fn kept_pipe_stdin_stays_open_after_root_exit() {
    kept_stdin_stays_open_after_root_exit(false);
}

#[cfg(windows)]
#[test]
fn a_finished_script_ends_a_root_that_does_not_exit() {
    // The script's status is read as the script ends, so a complete record and an empty file
    // both end the task within the grace, with the recorded status or 1.
    finished_script_case("printf '%s\n' 7 > \"$FASTEXEC_EXIT_STATUS\"", 7);
    finished_script_case(": > \"$FASTEXEC_EXIT_STATUS\"", 1);
}

/// Starts a task whose script records its status, leaves a heartbeat in an independent
/// subshell and then blocks, so only the grace after the script's end can end it, and only the
/// tree kill can end the heartbeat.
#[cfg(windows)]
fn finished_script_case(write_status: &str, expected: i32) {
    let beats = std::env::temp_dir().join(format!(
        "fastexec-heartbeat-{}-{expected}",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&beats);
    let mut server = Server::start_with(|command| {
        command.env("FASTEXEC_TEST_HEARTBEAT", &beats);
    });
    let command = format!(
        "( while :; do printf x >> \"$FASTEXEC_TEST_HEARTBEAT\"; sleep 0.1; done ) & {}; sleep 120",
        write_status
    );
    let started = Instant::now();
    let text =
        server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 30000}));
    let elapsed = started.elapsed();
    assert!(text.contains(&format!("[exited {expected}]")), "{text}");
    let listed = server.ok(json!({"action": "list", "limit": 1}));
    assert!(listed.contains(bare_id(task_id_of(&text))), "{listed}");
    assert!(
        elapsed < Duration::from_secs(12),
        "the grace ended the root within its bound: {elapsed:?}"
    );
    assert!(
        text.contains("root forced after 5 s exit grace"),
        "the result names the forced end: {text}"
    );
    assert!(
        text.contains("ended with bash: "),
        "the leftover heartbeat is named: {text}"
    );
    let written = std::fs::read(&beats).expect("the heartbeat file").len();
    assert!(written > 0, "the heartbeat ran: {beats:?}");
    std::thread::sleep(Duration::from_millis(700));
    let after = std::fs::read(&beats).map(|bytes| bytes.len()).unwrap_or(0);
    assert_eq!(after, written, "the forced end ended the heartbeat too");
}

#[cfg(windows)]
#[test]
fn a_child_process_does_not_record_the_roots_status() {
    root_only_case("bash \"$0\" child");
}

#[cfg(windows)]
#[test]
fn a_child_subshell_does_not_record_the_roots_status() {
    root_only_case("( trap - EXIT; . \"$0\" child )");
}

/// Runs a command over the script threshold from its file, with a child that runs that file
/// again, and requires the root to finish its own work with its own status.
#[cfg(windows)]
fn root_only_case(invoker: &str) {
    let mut server = Server::start();
    let padding = "echo padding-line\n".repeat(700);
    let command = format!(
        "if [ \"${{1-}}\" = child ]; then sleep 1; exit 7; fi\n{padding}{invoker}\nsleep 12\necho root-done"
    );
    let started = Instant::now();
    let text =
        server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 30000}));
    let elapsed = started.elapsed();
    assert!(text.contains("[exited 0]"), "{text}");
    assert!(!text.contains("root forced after 5 s exit grace"), "{text}");
    assert!(
        elapsed >= Duration::from_secs(12),
        "the root was not ended early: {elapsed:?}"
    );
}

#[cfg(windows)]
#[test]
fn a_startup_exit_trap_keeps_its_own_cleanup() {
    // A profile-like startup file sets its own EXIT cleanup before the command runs; the status
    // marker rides on the command line and never replaces that cleanup.
    let dir = std::env::temp_dir().join(format!("fastexec-startup-trap-{}", std::process::id()));
    // A stale directory from an earlier run must not answer for this one.
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("a scratch directory");
    let marker = dir.join("startup-cleanup");
    let startup = dir.join("bash_env.sh");
    std::fs::write(
        &startup,
        format!(
            "trap 'printf ran > \"{}\"' EXIT\n",
            marker.to_string_lossy().replace('\\', "/")
        ),
    )
    .expect("the startup file");
    let mut server = Server::start_with(|command| {
        command.env("BASH_ENV", &startup);
    });
    let text = server.ok(json!({"action": "start", "command": "true", "pty": true,
        "loginShell": false, "waitMs": 10000}));
    assert!(text.contains("[exited 0]"), "{text}");
    assert!(!text.contains("root forced after 5 s exit grace"), "{text}");
    assert!(
        marker.exists(),
        "the startup file's own EXIT cleanup ran: {marker:?} {text}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn kept_pty_stdin_stays_open_after_root_exit() {
    kept_stdin_stays_open_after_root_exit(true);
}

fn kept_stdin_stays_open_after_root_exit(pty: bool) {
    let mut server = Server::start_with(|command| {
        command.env_remove("FASTEXEC_BASH");
    });
    let source = if pty { "/dev/tty" } else { "&0" };
    let command = format!(
        "(while IFS= read -r line; do printf 'got:%s\\n' \"$line\"; done; echo input-ended) <{source} & echo ready"
    );
    let mut start_args = json!({"action": "start", "command": command,
        "pty": pty, "keepTree": true, "loginShell": false, "waitMs": 10000});
    if !pty {
        start_args["eof"] = json!(false);
    }
    let text = server.ok(start_args);
    assert!(
        text.contains("[exited 0]") && text.contains("keeps "),
        "{text}"
    );
    let id = task_id_of(&text).to_owned();
    let text = server.ok(json!({"action": "poll", "taskId": &id,
        "input": "after-root\n", "waitMs": 10000,
        "returnWhen": {"outputContains": ["got:after-root"]}}));
    assert!(text.contains("got:after-root"), "{text}");
    let finish = if pty {
        json!({"action": "poll", "taskId": &id, "keys": ["C-d"], "waitMs": 10000})
    } else {
        json!({"action": "poll", "taskId": &id, "eof": true, "waitMs": 10000})
    };
    let text = server.ok(finish);
    assert!(text.contains("input-ended"), "{text}");
    assert!(!text.contains("keeps "), "{text}");
    let (text, failed) = server.call(json!({"action": "poll", "taskId": &id,
        "input": "late\n", "waitMs": 0}));
    assert!(failed && text.contains("exited"), "{text}");
}

#[test]
fn keep_tree_keeps_what_bash_leaves_until_kill_or_deadline() {
    let files = [false, true].map(|pty| {
        (
            scratch(&format!("kept-{pty}")),
            scratch(&format!("expiring-{pty}")),
        )
    });
    let mut server = Server::start();
    for (pty, ((kept, kept_bash), (expiring, expiring_bash))) in
        [false, true].into_iter().zip(&files)
    {
        let command = format!("{} echo started", heartbeat(kept_bash));
        let text = server.ok(json!({"action": "start", "command": command, "pty": pty, "keepTree": true, "waitMs": 60000}));
        assert!(text.contains("[exited 0]"), "pty={pty}: {text}");
        assert!(
            text.contains("started") && text.contains(" running; kill ends them"),
            "pty={pty}: {text}"
        );
        let id = task_id_of(&text).to_owned();
        wait_for_heartbeat(kept);
        let before = file_len(kept);
        let deadline = Instant::now() + Duration::from_secs(30);
        while file_len(kept) <= before {
            if Instant::now() >= deadline {
                panic!("a kept process stopped (pty={pty})");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let list = server.ok(json!({"action": "list"}));
        let entry = list
            .lines()
            .find(|line| line.starts_with(bare_id(&id)))
            .unwrap_or_else(|| panic!("missing task in list: {list}"));
        assert!(entry.contains("keeps "), "{entry}");
        let text = server.ok(json!({"action": "kill", "taskId": &id}));
        assert!(
            text.contains("[exited 0]"),
            "kill keeps the root's result: {text}"
        );
        assert!(!text.contains("keeps "), "{text}");
        assert!(text.contains("kept processes ended"), "{text}");
        assert_stops_growing(kept);

        // On a loaded machine the deadline can fire before this start even answers, so the
        // first result may already be the expired one. Either way the deadline, not a kill,
        // ends the kept processes.
        let command = format!("{} echo started", heartbeat(expiring_bash));
        let text = server.ok(json!({"action": "start", "command": command, "pty": pty, "keepTree": true, "killAfterMs": 8000, "waitMs": 60000}));
        let id = task_id_of(&text).to_owned();
        if text.contains("keeps ") {
            // The root finished first: the deadline then reaches the kept processes only.
            assert!(text.contains("[exited 0]"), "pty={pty}: {text}");
            let text = server.ok(json!({"action": "poll", "taskId": &id, "waitMs": 60000}));
            assert!(
                text.contains("[exited 0]") && text.contains("kept processes ended"),
                "killAfterMs ends the kept processes and keeps the root's result: {text}"
            );
            assert!(!text.contains("keeps "), "{text}");
        } else {
            assert!(!text.contains("keeps "), "{text}");
            assert!(
                text.contains("[killed]") || text.contains("[exited 0]"),
                "pty={pty}: {text}"
            );
        }
        assert_stops_growing(expiring);
    }
    let text = server.ok(json!({"action": "start", "command": "true", "keepTree": true}));
    assert!(
        !text.contains("keeps ") && !text.contains("ended with bash:"),
        "nothing left, nothing kept: {text}"
    );
}

/// The total log limit evicts finished logs only: a task that keeps processes keeps its log and
/// transcript until they end. The debug build takes a 1 MiB limit for the test.
#[cfg(debug_assertions)]
#[test]
fn log_eviction_spares_tasks_that_keep_processes() {
    let mut server = Server::start_with(|command| {
        command.env("FASTEXEC_TEST_TOTAL_LOG_LIMIT", "1048576");
    });
    let marker = "KEPT-MARKER";
    for pty in [false, true] {
        let command = format!("sleep 300 & echo {marker}");
        let text = server.ok(json!({"action": "start", "command": command, "pty": pty, "keepTree": true, "loginShell": false, "waitMs": 60000}));
        assert!(text.contains("keeps "), "pty={pty}: {text}");
    }
    let (pipe, pty) = (server.id(1), server.id(2));
    let write = |bytes: u32| format!("head -c {bytes} /dev/zero | tr '\\0' x");
    let status = |server: &mut Server, id: &str| {
        server.ok(json!({"action": "poll", "taskId": id, "waitMs": 0}))
    };
    // Files are checked on disk: a transcript call would reserve log space itself.
    let files = |server: &mut Server, id: &str| {
        let text = server.ok(json!({"action": "poll", "taskId": id, "waitMs": 0}));
        let log = log_path_of(&text).map(PathBuf::from);
        let transcript = log.as_ref().map(|p| p.with_extension("transcript.txt"));
        (text, log, transcript)
    };
    let assert_kept = |server: &mut Server| {
        for id in [&pipe, &pty] {
            let (text, log, _) = files(server, id);
            assert!(!text.contains("log evicted"), "{text}");
            let log = std::fs::read_to_string(log.unwrap()).unwrap();
            assert!(log.contains(marker), "{id}: {log}");
        }
        assert!(
            files(server, &pty).2.unwrap().exists(),
            "the transcript is gone"
        );
    };

    // Nothing can be evicted: the running writer's excess is dropped, and the kept logs stay.
    let text = server.ok(json!({"action": "start", "command": write(1_572_864), "loginShell": false, "waitMs": 60000}));
    assert!(
        text.contains("bytes past the log limit were not stored"),
        "{text}"
    );
    let full = server.id(3);
    assert_kept(&mut server);

    // A finished log goes in their place.
    let text = server.ok(
        json!({"action": "start", "command": write(614_400), "loginShell": false, "waitMs": 60000}),
    );
    assert!(!text.contains("not stored"), "{text}");
    let text = status(&mut server, &full);
    assert!(text.contains("log evicted"), "{text}");
    assert_kept(&mut server);

    // Once their processes end, they are finished too. Their end is the newest one, so the log
    // that ended before them goes first and the next writer's log pays for reaching them.
    let pipe_log = files(&mut server, &pipe).1.unwrap();
    let pty_log = files(&mut server, &pty).1.unwrap();
    let pty_transcript = pty_log.with_extension("transcript.txt");
    for id in [&pipe, &pty] {
        server.ok(json!({"action": "kill", "taskId": id}));
    }
    for _ in 0..2 {
        server.ok(json!({"action": "start", "command": write(614_400), "loginShell": false, "waitMs": 60000}));
    }
    for (id, log) in [(&pipe, &pipe_log), (&pty, &pty_log)] {
        let text = status(&mut server, id);
        assert!(text.contains("log evicted") && !log.exists(), "{text}");
    }
    assert!(!pty_transcript.exists(), "the transcript remains");
}

/// A task that keeps processes finishes when they end, so it is not the first to be retired
/// when they outlived later tasks.
#[test]
fn retirement_counts_kept_processes_ending_as_finishing() {
    let mut server = Server::start();
    let text = server.ok(json!({"action": "start", "command": "sleep 300 & echo kept", "keepTree": true, "waitMs": 10000}));
    assert!(text.contains("keeps "), "{text}");
    let kept = server.id(1);
    for _ in 0..64 {
        server.ok(json!({"action": "start", "command": "true"}));
    }
    server.ok(json!({"action": "kill", "taskId": kept}));
    // The kill ends the tree, so the task is finished now and the cap applies to it: it stays,
    // and the earliest finisher goes.
    let list = server.ok(json!({"action": "list", "limit": 100}));
    assert_eq!(list.lines().count(), 64, "{list}");
    let listed = |text: &str, id: &str| {
        let want = bare_id(id);
        text.lines()
            .any(|l| l.split_whitespace().next() == Some(want))
    };
    assert!(
        listed(&list, &kept),
        "the task that just became finished stays: {list}"
    );
    assert!(
        !listed(&list, &server.id(2)),
        "the earliest finisher goes first: {list}"
    );
    server.ok(json!({"action": "start", "command": "true"}));
    // Retirement follows the result that shows the completion.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let list = server.ok(json!({"action": "list", "limit": 100}));
        if list.lines().count() == 64 {
            assert!(
                listed(&list, &kept)
                    && !listed(&list, &server.id(2))
                    && !listed(&list, &server.id(3)),
                "the earliest finishers go first"
            );
            break;
        }
        assert!(Instant::now() < deadline, "no task was retired");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// A long command runs from a script file, which stays while kept processes may run it as `$0`.
#[test]
fn kept_processes_can_run_the_command_script() {
    let [(gate, gate_bash), (child, child_bash)] = ["script-gate", "script-child"].map(scratch);
    let command = format!(
        "#{}\nif [ \"$1\" = child ]; then echo ran > '{child_bash}'; else (until [ -e '{gate_bash}' ]; do sleep 0.1; done; \"$BASH\" \"$0\" child; sleep 300) & echo parent; fi",
        "x".repeat(8_100)
    );
    let mut server = Server::start();
    let text = server.ok(json!({"action": "start", "command": command, "keepTree": true, "loginShell": false, "waitMs": 10000}));
    assert!(text.contains("keeps "), "{text}");
    // The result shows the root's end, so the child runs `$0` only after it.
    std::fs::write(&*gate, "").unwrap();
    wait_for_file(&child);
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::fs::read_to_string(&*child).unwrap() != "ran\n" {
        assert!(Instant::now() < deadline, "the script ran incompletely");
        std::thread::sleep(Duration::from_millis(50));
    }
    server.ok(json!({"action": "kill", "taskId": server.id(1)}));
}

#[test]
fn waits_on_kept_processes_last_until_they_end_or_a_condition_holds() {
    let markers = ["late-go", "short-done"].map(scratch);
    let [(go, go_bash), (done, done_bash)] = &markers;
    let mut server = Server::start();
    // Without returnWhen, the root's exit is the result, however long the kept processes run:
    // these wait for a file the test writes later.
    let command =
        format!("(until [ -e {go_bash} ]; do sleep 0.1; done; echo late; sleep 2) & echo early");
    let text = server
        .ok(json!({"action": "start", "command": command, "keepTree": true, "waitMs": 60000}));
    assert!(
        text.contains("[exited 0]") && text.contains("keeps "),
        "{text}"
    );
    let id = server.id(1);
    // Once a result showed the exit, waits outlast it: conditions on the kept processes' output
    // hold, and without them the wait ends with the tree.
    std::fs::write(&**go, "").unwrap();
    let text = server.ok(json!({"action": "poll", "taskId": id, "waitMs": 60000, "returnWhen": {"outputContains": ["late"]}}));
    assert!(text.contains("wait: output matched \"late\""), "{text}");
    assert!(text.starts_with("late"), "{text}");
    let text = server.ok(json!({"action": "poll", "taskId": id, "waitMs": 20000}));
    assert!(!text.contains("keeps "), "{text}");
    assert!(text.contains("kept processes ended"), "{text}");
    let begun = Instant::now();
    server.ok(json!({"action": "poll", "taskId": id, "waitMs": 20000}));
    assert!(
        begun.elapsed() < Duration::from_secs(5),
        "an ended tree ends waits at once"
    );

    // returnWhen waits past a root exit 0 for what the kept processes print.
    let command = "(sleep 2; echo ready; sleep 300) & echo started";
    let text = server.ok(json!({"action": "start", "command": command, "keepTree": true, "waitMs": 20000, "returnWhen": {"outputContains": ["ready"]}}));
    assert!(text.contains("wait: output matched \"ready\""), "{text}");
    assert!(text.contains("started\nready"), "{text}");
    let ready = server.id(2);
    // A root that fails ends the wait at once.
    let command = "sleep 300 & exit 3";
    let text = server.ok(json!({"action": "start", "command": command, "keepTree": true, "waitMs": 20000, "returnWhen": {"outputContains": ["never"]}}));
    assert!(text.contains("[exited 3]"), "{text}");
    let failed = server.id(3);
    // The footer names the processes a task keeps once.
    assert!(
        text.contains(&format!("{} keeps ", bare_id(&ready))),
        "{text}"
    );

    // The footer leaves out held processes it showed, and names the end of a tree it did not
    // show once.
    let text = server.ok(json!({"action": "start", "command": "true"}));
    assert!(
        !text.contains(&format!("{} keeps ", bare_id(&ready))),
        "shown once: {text}"
    );
    server.ok(json!({"action": "kill", "taskId": failed}));
    let command = format!("(sleep 1; touch {done_bash}) & echo short");
    server.ok(json!({"action": "start", "command": command, "keepTree": true, "waitMs": 60000}));
    let short = server.id(5);
    wait_for_file(done);
    std::thread::sleep(Duration::from_millis(2000));
    let text = server.ok(json!({"action": "start", "command": "true"}));
    assert!(
        text.contains(&format!("{} kept processes ended", bare_id(&short))),
        "{text}"
    );
    let text = server.ok(json!({"action": "start", "command": "true"}));
    assert!(
        !text.contains(&format!("{} kept", bare_id(&short))),
        "shown once: {text}"
    );
    // The footer names held processes once, and again once they change; a task's own result,
    // here too small to show them, counts for nothing.
    // The held set stays the same until the test changes it: two `sleep 300` children, the
    // second one's PID recorded so a separate task can end exactly it. Nothing else in the task
    // starts a process, so the footer's held list cannot churn on its own.
    let (short, short_bash) = scratch("short-held");
    let command = format!("sleep 300 & sleep 300 & echo \"$!\" > '{short_bash}'; echo two");
    let text = server.ok(json!({"action": "start", "command": command, "keepTree": true, "waitMs": 10000, "maxBytes": 100}));
    assert!(text.contains("keeps "), "{text}");
    let two = server.id(8);
    wait_for_file(&short);
    // The held list is named once while it stays the same.
    let text = server.ok(json!({"action": "start", "command": "true"}));
    assert!(
        text.contains(&format!("{} keeps ", bare_id(&two))),
        "{text}"
    );
    let text = server.ok(json!({"action": "start", "command": "true"}));
    assert!(
        !text.contains(&format!("{} keeps ", bare_id(&two))),
        "named once: {text}"
    );
    // Ending one held process changes the list, so the footer names it again; then it falls
    // quiet. The kill task's own sleeps belong to it, not to the held task.
    let command = format!(
        "pid=$(cat '{short_bash}'); kill \"$pid\"; while kill -0 \"$pid\" 2>/dev/null; do sleep 0.05; done"
    );
    let text = server.ok(json!({"action": "start", "command": command, "waitMs": 60000}));
    assert!(
        text.contains(&format!("{} keeps ", bare_id(&two))),
        "{text}"
    );
    let text = server.ok(json!({"action": "start", "command": "true"}));
    assert!(
        !text.contains(&format!("{} keeps ", bare_id(&two))),
        "named once: {text}"
    );
    server.ok(json!({"action": "kill", "taskId": two}));
    server.ok(json!({"action": "kill", "taskId": ready}));
}

/// A process the server may not signal, as one sudo runs as root, stays held and reported. The
/// fixture needs root and a C compiler; CI requires both on Linux and FreeBSD.
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
#[test]
fn processes_the_server_may_not_kill_stay_held_and_reported() {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
    use std::os::unix::process::CommandExt;
    // CI and fx-test set this, so the case cannot pass by skipping when a prerequisite is missing.
    let required = std::env::var("FASTEXEC_REQUIRE_ROOT_TEST").is_ok_and(|value| value == "1");
    // SAFETY: plain syscall.
    if unsafe { libc::geteuid() } != 0 {
        assert!(
            !required,
            "FASTEXEC_REQUIRE_ROOT_TEST=1, and a setuid-root fixture needs root"
        );
        eprintln!("skipped: a setuid-root fixture needs root");
        return;
    }
    // The setuid binary may only touch this instance's root-owned directory. Create it
    // exclusively, keep it private while compiling, and bind the helper to its original inode.
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).unwrap();
    let nonce: String = random.iter().map(|byte| format!("{byte:02x}")).collect();
    let path = std::env::temp_dir().join(format!("fastexec-root-fixture-{nonce}"));
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&path)
        .unwrap();
    let mut scratch = Scratch(path);
    scratch.0 = scratch.canonicalize().unwrap();
    let dir = scratch.join("instance");
    std::fs::DirBuilder::new().mode(0o700).create(&dir).unwrap();
    let metadata = std::fs::symlink_metadata(&dir).unwrap();
    assert!(metadata.is_dir());
    assert_eq!(metadata.uid(), 0);
    assert_eq!(metadata.mode() & 0o777, 0o700);
    #[cfg(target_os = "freebsd")]
    crate::support::create_private_runtime(&dir);

    let beats = [dir.join("exit"), dir.join("kill")];
    let stops = [dir.join("exit.stop"), dir.join("kill.stop")];
    // Every byte gets its own escape, for both C literals and Bash's ANSI-C quoting.
    let escaped = |path: &std::path::Path| -> String {
        path.as_os_str()
            .as_bytes()
            .iter()
            .map(|byte| format!("\\x{byte:02x}"))
            .collect()
    };
    let source = dir.join("daemon.c");
    let declarations = format!(
        "#define _GNU_SOURCE\n\
         #include <errno.h>\n\
         #include <fcntl.h>\n\
         #include <stdint.h>\n\
         #include <stdio.h>\n\
         #include <string.h>\n\
         #include <sys/stat.h>\n\
         #include <unistd.h>\n\
         static const char directory[] = \"{}\";\n\
         static const uintmax_t directory_device = UINTMAX_C({});\n\
         static const uintmax_t directory_inode = UINTMAX_C({});\n\
         static const char *beats[] = {{\"exit\", \"kill\"}};\n\
         static const char *stops[] = {{\"exit.stop\", \"kill.stop\"}};\n",
        escaped(&dir),
        metadata.dev(),
        metadata.ino()
    );
    std::fs::write(
        &source,
        declarations
            // It ends by itself after 60 s, so a failing run leaves no immortal root process.
            + r#"static int open_directory(void) {
  int fd = open(directory, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
  if (fd < 0) { perror("fixture directory"); return -1; }
  struct stat st;
  if (fstat(fd, &st) != 0) { perror("fixture directory"); close(fd); return -1; }
  if (!S_ISDIR(st.st_mode) || st.st_uid != 0 || (st.st_mode & 07777) != 0755 ||
      (uintmax_t)st.st_dev != directory_device || (uintmax_t)st.st_ino != directory_inode) {
    fprintf(stderr, "fixture directory identity or permissions changed\n");
    close(fd);
    return -1;
  }
  return fd;
}
static int stopped(int directory_fd, const char *name) {
  struct stat st;
  if (fstatat(directory_fd, name, &st, AT_SYMLINK_NOFOLLOW) != 0) {
    if (errno == ENOENT) return 0;
    perror("fixture stop");
    return -1;
  }
  if (!S_ISREG(st.st_mode) || st.st_uid != 0 || st.st_nlink != 1 ||
      (st.st_mode & 07777) != 0600) {
    fprintf(stderr, "fixture stop identity or permissions changed\n");
    return -1;
  }
  return 1;
}
static int heartbeat(int fd, int pid, int parent) {
  if (dprintf(fd, "%d %d\n", pid, parent) < 0) { perror("fixture heartbeat"); return 1; }
  return 0;
}
int main(int argc, char **argv) {
  if (argc != 2) return 2;
  int which;
  if (strcmp(argv[1], "--check-root") == 0) which = -1;
  else if (strcmp(argv[1], "exit") == 0) which = 0;
  else if (strcmp(argv[1], "kill") == 0) which = 1;
  else return 2;
  if (geteuid() != 0) {
    fprintf(stderr, "setuid-root fixture: uid=%u euid=%u; check TMPDIR mount flags and inherited privilege restrictions\n",
            (unsigned)getuid(), (unsigned)geteuid());
    return 1;
  }
  int directory_fd = open_directory();
  if (directory_fd < 0) return 1;
  if (setresuid(0, 0, 0) != 0) { perror("fixture setresuid"); return 1; }
  if (which < 0) return close(directory_fd) != 0;
  umask(077);
  int fd = openat(directory_fd, beats[which],
                  O_WRONLY | O_CREAT | O_EXCL | O_APPEND | O_NOFOLLOW | O_CLOEXEC, 0600);
  if (fd < 0) { perror("fixture heartbeat"); return 1; }
  for (int i = 0; i < 600; i++) {
    int stop = stopped(directory_fd, stops[which]);
    if (stop < 0) return 1;
    if (stop > 0) break;
    if (heartbeat(fd, (int)getpid(), (int)getppid()) != 0) return 1;
    usleep(100000);
  }
  int failed = heartbeat(fd, 0, 0);
  if (close(fd) != 0 || close(directory_fd) != 0) failed = 1;
  return failed;
}
"#,
    )
    .unwrap();
    std::fs::set_permissions(&source, std::fs::Permissions::from_mode(0o600)).unwrap();
    let fixture = dir.join("root-daemon");
    match Command::new("cc")
        .arg(&source)
        .arg("-o")
        .arg(&fixture)
        .status()
    {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            assert!(
                !required,
                "FASTEXEC_REQUIRE_ROOT_TEST=1, and the fixture needs a C compiler"
            );
            eprintln!("skipped: no C compiler");
            return;
        }
        built => assert!(built.unwrap().success(), "the fixture did not compile"),
    }
    std::fs::set_permissions(&fixture, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::set_permissions(&*dir, std::fs::Permissions::from_mode(0o755)).unwrap();

    // The private outer directory keeps these ordinary-binary checks under one identity.
    let check_plain = |executable: &std::path::Path, argument: &str| {
        Command::new(executable)
            .arg(argument)
            .stdin(Stdio::null())
            .output()
            .unwrap()
    };
    assert!(check_plain(&fixture, "--check-root").status.success());
    std::fs::set_permissions(&*dir, std::fs::Permissions::from_mode(0o775)).unwrap();
    let rejected = check_plain(&fixture, "--check-root");
    assert_eq!(rejected.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("fixture directory"));
    std::fs::set_permissions(&*dir, std::fs::Permissions::from_mode(0o755)).unwrap();

    let moved = Scratch(dir.with_extension("original"));
    std::fs::rename(&*dir, &*moved).unwrap();
    std::fs::DirBuilder::new()
        .mode(0o755)
        .create(&*dir)
        .unwrap();
    let plain = moved.join("root-daemon");
    assert_eq!(std::fs::metadata(&plain).unwrap().mode() & 0o7777, 0o755);
    assert_ne!(std::fs::metadata(&*dir).unwrap().ino(), metadata.ino());
    let rejected = check_plain(&plain, "exit");
    assert_eq!(rejected.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("fixture directory"));
    assert!(beats.iter().chain(&stops).all(|path| !path.exists()));
    std::fs::remove_dir(&*dir).unwrap();
    std::os::unix::fs::symlink(&*moved, &*dir).unwrap();
    let rejected = check_plain(&plain, "exit");
    assert_eq!(rejected.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("fixture directory"));
    assert!(!moved.join("exit").exists());
    std::fs::remove_file(&*dir).unwrap();
    std::fs::rename(&*moved, &*dir).unwrap();

    std::fs::write(&beats[0], b"original").unwrap();
    assert_eq!(check_plain(&fixture, "exit").status.code(), Some(1));
    assert_eq!(std::fs::read(&beats[0]).unwrap(), b"original");
    std::fs::remove_file(&beats[0]).unwrap();
    std::fs::set_permissions(&*scratch, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::set_permissions(&fixture, std::fs::Permissions::from_mode(0o4755)).unwrap();
    let check = Command::new(&fixture)
        .arg("--check-root")
        .uid(65534)
        .gid(65534)
        .current_dir(&*dir)
        .stdin(Stdio::null())
        .output()
        .expect("run the setuid fixture as uid 65534");
    assert!(
        check.status.success(),
        "the fixture must become root before testing protected descendants ({}): {}",
        dir.display(),
        String::from_utf8_lossy(&check.stderr)
    );
    // Invalid arguments never select a file, even though the executable becomes root.
    let sentinel = dir.join("not-a-fixture-path");
    std::fs::write(&sentinel, b"original").unwrap();
    std::fs::set_permissions(&sentinel, std::fs::Permissions::from_mode(0o600)).unwrap();
    for arguments in [
        vec![sentinel.as_os_str()],
        vec![std::ffi::OsStr::new("exit"), sentinel.as_os_str()],
        vec![beats[0].as_os_str(), stops[1].as_os_str()],
        vec![std::ffi::OsStr::new("--check-root"), sentinel.as_os_str()],
    ] {
        let rejected = Command::new(&fixture)
            .args(arguments)
            .uid(65534)
            .gid(65534)
            .current_dir(&*dir)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(rejected.status.code(), Some(2));
        assert_eq!(std::fs::read(&sentinel).unwrap(), b"original");
        assert!(beats.iter().chain(&stops).all(|path| !path.exists()));
    }
    let fixture = format!("$'{}'", escaped(&fixture));
    // Copy fastexec into the 0755 scratch directory so uid 65534 can execute it even when
    // the repository checkout resides under a mode-0700 home directory such as /root.
    let unprivileged_bin = scratch.join("fastexec-bin");
    std::fs::copy(env!("CARGO_BIN_EXE_fastexec"), &unprivileged_bin).unwrap();
    std::fs::set_permissions(&unprivileged_bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut server = Server::start_bin(&unprivileged_bin, |command| {
        command
            .uid(65534)
            .gid(65534)
            .current_dir(&*dir)
            .env("HOME", &*dir);
    });
    // Stop through private files, never old numeric PIDs: the bounded fixture may already
    // have exited and released its PID on a slow machine. This guard also runs after a panic.
    struct StopFixtures {
        beats: [PathBuf; 2],
        stops: [PathBuf; 2],
    }
    impl Drop for StopFixtures {
        fn drop(&mut self) {
            for path in &self.stops {
                let _ = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(0o600)
                    .open(path)
                    .and_then(|mut file| file.write_all(b"stop"));
            }
            let deadline = Instant::now() + Duration::from_secs(5);
            while self.beats.iter().any(|path| {
                path.exists()
                    && !std::fs::read_to_string(path).is_ok_and(|text| text.ends_with("0 0\n"))
            }) && Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
    let stopping = StopFixtures {
        beats: beats.clone(),
        stops,
    };
    let identity = |beat: &PathBuf| -> (i64, i64) {
        let text = std::fs::read_to_string(beat).unwrap();
        let (pid, parent) = text.lines().last().unwrap().split_once(' ').unwrap();
        (pid.parse().unwrap(), parent.parse().unwrap())
    };

    // At the root's exit. The heartbeat is written only after becoming root: a fixed delay
    // could let bash end while its scheduled child was still an unprivileged exec candidate.
    let command = format!("{fixture} exit & read -r ready; echo started");
    server.ok(json!({"action": "start", "eof": false, "command": command, "waitMs": 0}));
    wait_for_heartbeat(&beats[0]);
    let text = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "input": "ready\n", "waitMs": 10000
    }));
    assert!(
        text.contains(&format!(
            "could not end root-daemon ({})",
            identity(&beats[0]).0
        )) && text.contains("kill retries"),
        "{text}"
    );
    let (text, is_error) = server.call(json!({"action": "kill", "taskId": server.id(1)}));
    assert!(is_error && text.contains("Cannot terminate"), "{text}");

    // After a kill of the running root.
    let command = format!("{fixture} kill & sleep 300");
    server.ok(json!({"action": "start", "command": command, "waitMs": 0}));
    wait_for_heartbeat(&beats[1]);
    let (text, is_error) = server.call(json!({"action": "kill", "taskId": server.id(2)}));
    assert!(is_error && text.contains("Cannot terminate"), "{text}");
    let text = server.ok(json!({"action": "poll", "taskId": server.id(2), "waitMs": 10000}));
    assert!(text.contains("[killed"), "{text}");
    assert!(
        text.contains(&format!(
            "could not end root-daemon ({})",
            identity(&beats[1]).0
        )),
        "{text}"
    );

    for beat in &beats {
        let before = file_len(beat);
        std::thread::sleep(Duration::from_millis(500));
        assert!(file_len(beat) > before, "the fixture should still run");
        let metadata = std::fs::metadata(beat).unwrap();
        assert_eq!(metadata.uid(), 0);
        assert_eq!(metadata.mode() & 0o777, 0o600);
    }
    let parents: Vec<_> = beats.iter().map(|beat| identity(beat).1).collect();
    assert!(parents.iter().all(|pid| *pid > 1));
    // A dead server cannot retry a denied kill. Its hosts must retain their reaper ownership
    // until the privileged descendants end, instead of abandoning them to the system init.
    server.child.kill().unwrap();
    server.child.wait().unwrap();
    std::thread::sleep(Duration::from_millis(500));
    for (beat, host) in beats.iter().zip(&parents) {
        assert_eq!(
            identity(beat).1,
            *host,
            "fixture {beat:?} lost its task host"
        );
    }
    drop(stopping);
    for beat in &beats {
        assert_eq!(identity(beat), (0, 0), "the fixture acknowledged its stop");
        assert_stops_growing(beat);
    }
}

#[test]
fn trees_a_task_keeps_count_toward_the_task_limit() {
    let mut server = Server::start();
    for _ in 0..16 {
        let text = server.ok(json!({"action": "start", "command": "sleep 300 & echo started", "keepTree": true, "waitMs": 10000}));
        assert!(text.contains("keeps "), "{text}");
    }
    let (text, is_error) = server.call(json!({"action": "start", "command": "true"}));
    assert!(is_error && text.contains("TOO_MANY_TASKS"), "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(1)}));
    server.ok(json!({"action": "start", "command": "true"}));
}

#[test]
fn closing_the_server_ends_running_tasks_and_removes_logs() {
    let (beat, beat_bash) = scratch("shutdown");
    let mut server = Server::start();
    let text = server.ok(json!({"action": "start", "command": format!("{} sleep 300", heartbeat(&beat_bash)), "waitMs": 1000}));
    let log = PathBuf::from(log_path_of(&text).unwrap());
    assert!(log.exists());
    // An in-flight wait must not delay shutdown: hosts escalate to SIGKILL within ~2.5 s.
    let poll = json!({"name": "exec", "arguments": {"action": "poll", "taskId": server.id(1), "waitMs": 60000}});
    server.send(json!({"jsonrpc": "2.0", "id": 99, "method": "tools/call", "params": poll}));
    std::thread::sleep(Duration::from_millis(300));
    drop(server.stdin.take());
    let deadline = Instant::now() + Duration::from_secs(2);
    while server.child.try_wait().unwrap().is_none() {
        assert!(
            Instant::now() < deadline,
            "the server did not exit within 2 s of stdin closing"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_stops_growing(&beat);
    assert!(!log.parent().unwrap().exists(), "the log directory remains");
}

#[test]
fn finishing_after_many_short_tasks_keeps_its_own_output() {
    let mut server = Server::start();
    server.ok(json!({"action": "start", "eof": false, "command": "read -r x; echo final-$x", "keepTree": true, "waitMs": 0}));
    // A root exit is not a retention barrier. keepTree reports a pending tree as
    // keptRunning until the ender confirms completion, even for these childless commands.
    let await_tree = |server: &mut Server, mut text: String| {
        let id = task_id_of(&text).to_owned();
        let deadline = Instant::now() + Duration::from_secs(30);
        while !status_line(&text).starts_with("[exited ") || status_line(&text).contains("keeps ") {
            assert!(Instant::now() < deadline, "tree did not finish: {text}");
            text = server.ok(json!({
                "action": "poll", "taskId": &id, "waitMs": 1000
            }));
        }
    };
    for _ in 0..65 {
        let text = server.ok(json!({"action": "start", "command": "true", "keepTree": true}));
        await_tree(&mut server, text);
    }
    let text = server
        .ok(json!({"action": "poll", "taskId": server.id(1), "input": "ok\n", "waitMs": 10000}));
    assert!(text.contains("[exited 0]"), "{text}");
    assert!(text.starts_with("final-ok\n"), "{text}");
    await_tree(&mut server, text);
    let list = server.ok(json!({"action": "list", "limit": 100}));
    assert_eq!(list.lines().count(), 64, "{list}");
    let kept = |id: &str| {
        let want = bare_id(id);
        list.lines()
            .any(|l| l.split_whitespace().next() == Some(want))
    };
    assert!(
        kept(&server.id(1)) && !kept(&server.id(2)) && !kept(&server.id(3)),
        "the earliest finishers go first"
    );
}

#[test]
fn output_window_respects_max_bytes_and_points_at_the_log() {
    // A long log path makes the status line long; maxBytes still bounds the whole text.
    let root = Scratch::new(&"長".repeat(60));
    // Linux allows far longer paths than Windows' 260 characters: make the status alone
    // longer than maxBytes there.
    let temp = if cfg!(windows) {
        root.to_path_buf()
    } else {
        root.join("長".repeat(80)).join("長".repeat(80))
    };
    std::fs::create_dir_all(&temp).unwrap();
    let mut long = Server::start_with_temp(&temp);
    let text = long.ok(
        json!({"action": "start", "command": "printf '\\xff\\n'; seq 1 5000", "maxBytes": 1024}),
    );
    assert!(
        body_of(&text).len() <= 1024,
        "{} bytes: {text}",
        body_of(&text).len()
    );
    assert!(text.contains("Warning:"), "{text}");
    drop(long);

    let mut server = Server::start();
    let text = server.ok(json!({"action": "start", "command": "seq 1 5000", "maxBytes": 2048}));
    assert!(
        body_of(&text).len() <= 2048,
        "{} bytes",
        body_of(&text).len()
    );
    assert!(
        text.starts_with("1\n2\n") && text.contains("4999\n5000\n\n[exited 0]"),
        "{text}"
    );
    let marker = body_of(&text)
        .lines()
        .find(|line| line.starts_with("... [") && line.contains("log lines "))
        .unwrap_or_else(|| panic!("missing omission marker: {text}"));
    let range = marker
        .split_once("log lines ")
        .and_then(|(_, rest)| rest.split(']').next())
        .unwrap();
    let (first, _last): (u64, u64) = range
        .split_once('-')
        .map(|(a, b)| (a.parse().unwrap(), b.parse().unwrap()))
        .unwrap();
    let log = std::fs::read_to_string(log_path_of(&text).unwrap()).unwrap();
    assert_eq!(
        log.lines().nth(first as usize - 1),
        Some(first.to_string().as_str())
    );
    let text = server.ok(json!({"action": "start", "command": "seq 1 5000", "maxBytes": 65536}));
    assert!(!text.contains("lines omitted"), "{text}");
    assert_eq!(
        text.lines()
            .filter(|line| line.parse::<u32>().is_ok())
            .count(),
        5000
    );

    // No fixed per-line cap: 3000-byte line fits default 16384 whole; 70000-byte line cuts at
    // 16384 by default and stays whole when maxBytes is raised above it.
    let line = |n: u32| format!("printf '%*s\\n' {n} '' | tr ' ' x");
    let text = server.ok(json!({"action": "start", "command": line(3000)}));
    assert_eq!(body_of(&text).len(), 3000);
    assert!(!text.contains("line cut at "), "{text}");
    let text = server.ok(json!({"action": "start", "command": line(70000)}));
    assert!(text.contains("line cut at 16 KiB;"), "{text}");
    assert!(body_of(&text).len() <= 16384, "{text}");
    let text = server
        .ok(json!({"action": "start", "command": line(70000), "maxBytes": 100000, "raw": true}));
    assert_eq!(body_of(&text).len(), 70000);
    assert!(!text.contains("line cut at "), "{text}");

    // Small body budgets still preserve status and background footer outside maxBytes.
    let pty = server.ok(json!({"action": "start", "command": "true", "pty": true}));
    let id = task_id_of(&pty).to_owned();
    server.ok(json!({"action": "transcript", "taskId": &id}));
    server.ok(json!({"action": "start", "command": "sleep 30", "waitMs": 0}));
    for max in [100, 128, 144, 160] {
        for call in [
            json!({"action": "transcript", "taskId": &id, "maxBytes": max}),
            json!({"action": "poll", "taskId": &id, "waitMs": 0, "maxBytes": max}),
            json!({"action": "poll", "taskId": &id, "waitMs": 0, "screen": true, "maxBytes": max}),
        ] {
            let text = server.ok(call);
            assert!(
                body_of(&text).len() <= max,
                "{} bytes: {text}",
                body_of(&text).len()
            );
        }
    }
}

#[test]
fn the_log_keeps_a_contiguous_prefix_past_its_limit() {
    // 10 bytes below the 64 MiB limit, then a chunk that crosses it, then one that would fit.
    let command = "head -c 67108854 /dev/zero | tr '\\0' x; sleep 0.3; printf '%100s' '' | tr ' ' A; sleep 0.3; printf B";
    let mut server = Server::start();
    let text = server.ok(json!({"action": "start", "command": command, "loginShell": false, "waitMs": 60000, "maxBytes": 1024}));
    assert!(text.contains("[exited 0]"), "{text}");
    let mut log = std::fs::File::open(log_path_of(&text).unwrap()).unwrap();
    let mut tail = Vec::new();
    log.seek(SeekFrom::End(-11)).unwrap();
    log.read_to_end(&mut tail).unwrap();
    assert_eq!(log.stream_position().unwrap(), 64 << 20);
    assert_eq!(tail, b"xAAAAAAAAAA");
    assert!(
        text.contains("91 bytes past the log limit were not stored"),
        "{text}"
    );
}

#[test]
fn an_exited_result_holds_output_written_while_its_window_was_read() {
    // Reading a 16 MB backlog takes long enough for the task to print a marker and exit
    // meanwhile; the delays move that exit across the read.
    let mut server = Server::start();
    for delay in ["0", "0.005", "0.01", "0.02", "0.04"] {
        let command = format!(
            "head -c 16000000 /dev/zero | tr '\\0' x; read -r go; sleep {delay}; printf '\\nFINAL_MARKER\\n'"
        );
        let started = server.ok(json!({"action": "start", "eof": false, "command": command, "loginShell": false, "waitMs": 0, "maxBytes": 1024}));
        let id = task_id_of(&started).to_owned();
        let log = PathBuf::from(log_path_of(&started).unwrap());
        let deadline = Instant::now() + Duration::from_secs(30);
        while file_len(&log) < 16_000_000 {
            assert!(
                Instant::now() < deadline,
                "the backlog did not reach the log"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let mut seen = String::new();
        let mut poll = json!({"action": "poll", "taskId": &id, "input": "go\n", "waitMs": 0, "maxBytes": 1024});
        loop {
            let text = server.ok(poll);
            seen.push_str(&text);
            if !status_line(&text).starts_with("[running]") {
                break;
            }
            poll = json!({"action": "poll", "taskId": &id, "waitMs": 10000, "maxBytes": 1024});
        }
        assert_eq!(
            seen.matches("FINAL_MARKER").count(),
            1,
            "delay {delay}: {seen}"
        );
    }
}

#[test]
fn pty_and_pipe_tasks_inherit_the_server_path() {
    let dir = Scratch::new("path");
    std::fs::create_dir_all(&*dir).unwrap();
    let probe = dir.join("fastexec_env_probe");
    std::fs::write(&probe, "#!/usr/bin/env bash\nprintf 'ENV_PATH_OK\\n'\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let inherited = std::env::var_os("PATH").unwrap_or_default();
    let path = std::env::join_paths(
        std::iter::once(dir.to_path_buf()).chain(std::env::split_paths(&inherited)),
    )
    .unwrap();
    let home = Scratch::new("server-path-home");
    std::fs::create_dir_all(&*home).unwrap();
    #[cfg(unix)]
    {
        // Debian's /etc/profile overwrites PATH before sourcing ~/.bash_profile; exporting the
        // probe directory from ~/.bash_profile exercises the session's login-environment snapshot.
        std::fs::write(
            home.join(".bash_profile"),
            format!("export PATH=\"{}:$PATH\"\n", dir.display()),
        )
        .unwrap();
    }
    // Git Bash's login profile builds PATH from ORIGINAL_PATH when a Git Bash parent set it.
    let mut server = Server::start_with(|command| {
        command
            .env("HOME", &*home)
            .env("USERPROFILE", &*home)
            .env("PATH", &path)
            .env("FASTEXEC_HOST_ENV_CHECK", "HOST_OK")
            .env("MSYS2_PATH_TYPE", "inherit")
            .env_remove("ORIGINAL_PATH");
    });
    for pty in [false, true] {
        let text = server.ok(json!({
            "action": "start",
            "command": "printf '%s:' \"$FASTEXEC_HOST_ENV_CHECK\"; fastexec_env_probe",
            "pty": pty,
            "waitMs": 15000
        }));
        assert!(text.contains("[exited 0]"), "pty={pty}: {text}");
        assert!(text.contains("HOST_OK:ENV_PATH_OK"), "pty={pty}: {text}");
    }
}

#[test]
fn encoding_decodes_legacy_output_and_long_commands_run_from_a_script() {
    let mut server = Server::start();
    let big5 = "printf '\\xa4\\xa4\\xa4\\xe5\\n'";
    let text = server.ok(json!({"action": "start", "command": big5}));
    assert!(text.contains("invalid in UTF-8; pass encoding"), "{text}");
    let text = server.ok(json!({"action": "start", "command": big5, "encoding": "big5"}));
    assert!(text.starts_with("中文\n"), "{text}");
    // A character split across polls decodes whole once its last byte arrives, also after
    // more than 4 KiB without a newline.
    for (encoding, prefix, lead, trail) in [
        ("big5", "", "\\xa4", "\\xa4"),
        ("gbk", "", "\\xd6", "\\xd0"),
        ("big5", "printf '%5000s' '' | tr ' ' a; ", "\\xa4", "\\xa4"),
    ] {
        // The marker shows when the lead byte is out, so the poll after it reads up to it.
        let (marker, marker_bash) = scratch(&format!("lead-{encoding}-{}", prefix.len()));
        let command =
            format!("{prefix}printf '{lead}'; touch {marker_bash}; read -r x; printf '{trail}\\n'");
        let started = server.ok(json!({"action": "start", "eof": false, "command": command, "encoding": encoding, "loginShell": false, "waitMs": 0}));
        let id = task_id_of(&started).to_owned();
        wait_for_file(&marker);
        std::thread::sleep(Duration::from_millis(300));
        let text = server.ok(json!({"action": "poll", "taskId": &id, "waitMs": 0}));
        assert!(!text.contains('\u{fffd}'), "{encoding}: {text}");
        let text =
            server.ok(json!({"action": "poll", "taskId": &id, "input": "go\n", "waitMs": 10000}));
        assert!(text.starts_with("中\n"), "{encoding}: {text}");
    }
    // Past 8,190 characters an argument would be cut on Windows; the script carries it whole.
    for length in [7_999, 8_000, 8_001, 9_000, 131_073, 900_000] {
        let long = format!("x='{}'; echo ${{#x}}", "a".repeat(length));
        let text = server.ok(json!({"action": "start", "command": long}));
        assert!(text.starts_with(&format!("{length}\n")), "{text}");
    }
}

#[test]
fn nul_commands_are_rejected_before_short_or_script_execution() {
    let mut server = Server::start();
    for pty in [false, true] {
        for length in [512, 8_000, 8_001, 131_073] {
            let (marker, marker_bash) = scratch(&format!("nul-command-{pty}-{length}"));
            let prefix = format!("printf ran > '{marker_bash}'; printf 'A\0B';\n#");
            let command = format!("{prefix}{}", "x".repeat(length - prefix.len()));
            assert_eq!(command.len(), length);
            let (text, is_error) = server.call(json!({
                "action": "start", "command": command, "pty": pty, "loginShell": false
            }));
            assert!(is_error, "{text}");
            assert!(text.contains("NUL byte"), "{text}");
            assert!(!marker.exists(), "an invalid command ran before rejection");
        }
    }
    let list = server.ok(json!({"action": "list"}));
    assert!(list.contains("No tasks."), "{list}");
}

#[test]
fn invalid_parameters_are_rejected_and_null_means_omitted() {
    let mut server = Server::start();
    for (arguments, expected) in [
        (
            json!({"action": "start", "command": "true", "taskId": "t1"}),
            "`taskId` does not apply",
        ),
        (json!({"action": "poll"}), "needs `taskId`"),
        (
            json!({"action": "poll", "taskId": "t1", "killAfterMs": 5}),
            "`killAfterMs` does not apply",
        ),
        (
            json!({"action": "start", "command": "true", "waitMs": 240001}),
            "out of range",
        ),
        (
            json!({"action": "start", "command": "true", "maxBytes": 99}),
            "out of range",
        ),
        (
            json!({"action": "start", "command": "true", "maxBytes": 4194305}),
            "out of range",
        ),
        (
            json!({"action": "start", "command": "true", "encoding": "nope"}),
            "Unknown or unsupported encoding",
        ),
        (json!({"action": "poll", "taskId": "t9"}), "Unknown taskId"),
        (
            json!({"action": "start", "command": "true", "encoding": "utf-16le"}),
            "unsupported encoding",
        ),
        (
            json!({"action": "start", "command": "true", "keys": ["Enter"]}),
            "`keys` does not apply",
        ),
        (
            json!({"action": "start", "command": "true", "screen": true}),
            "screen needs a PTY",
        ),
        (
            json!({"action": "start", "command": "true", "pty": true, "screen": true, "raw": true}),
            "does not apply with screen",
        ),
        (json!({"action": "transcript"}), "needs `taskId`"),
        (
            json!({"action": "start", "command": "true", "returnWhen": {}}),
            "returnWhen needs",
        ),
        (
            json!({"action": "start", "command": "true", "returnWhen": {"outputContains": ["a\nb"]}}),
            "without line breaks (outputContains matches within one output line; to avoid matching an echoed command, print a marker",
        ),
        (
            json!({"action": "start", "command": "true", "returnWhen": {"outputContains": []}}),
            "takes 1-16 texts",
        ),
        (
            json!({"action": "start", "command": "true", "returnWhen": {"screenGone": ["x"]}}),
            "screenGone needs a PTY",
        ),
        (
            json!({"action": "poll", "taskId": "t1", "keyDelayMs": 100}),
            "keyDelayMs applies only with keys",
        ),
        (
            json!({"action": "poll", "taskId": "t1", "input": "x", "keys": vec!["a"; 16], "keyDelayMs": 2000}),
            "over the 30000 ms limit",
        ),
    ] {
        let (text, is_error) = server.call(arguments.clone());
        assert!(is_error && text.contains(expected), "{arguments} -> {text}");
    }
    let message = server.request(
        "tools/call",
        json!({"name": "exec", "arguments": {"action": "list", "bogus": 1}}),
    );
    assert!(
        message.get("error").is_some() || message["result"]["isError"] == json!(true),
        "{message}"
    );
    let text = server.ok(
        json!({"action": "start", "command": "echo ok", "cwd": null, "pty": null, "taskId": null}),
    );
    assert!(text.contains("[exited 0]"), "{text}");
}

#[test]
fn long_wait_sends_progress_and_cancellation_leaves_the_task_running() {
    let mut server = Server::start();
    server.ok(json!({"action": "start", "command": "sleep 120", "waitMs": 0}));
    server.next_id += 1;
    let id = server.next_id;
    let call = json!({"name": "exec", "arguments": {"action": "poll", "taskId": server.id(1), "waitMs": 60000}, "_meta": {"progressToken": "p1"}});
    server.send(json!({"jsonrpc": "2.0", "id": id, "method": "tools/call", "params": call}));
    let progress = server
        .lines
        .recv_timeout(Duration::from_secs(25))
        .expect("progress within 25 s");
    assert_eq!(progress["method"], "notifications/progress");
    assert_eq!(progress["params"]["progressToken"], "p1");
    server.send(
        json!({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": id}}),
    );
    std::thread::sleep(Duration::from_millis(300));
    let text = server.ok(json!({"action": "list"}));
    assert!(text.contains("[running]"), "{text}");
    let text = server.ok(json!({"action": "kill", "taskId": server.id(1)}));
    assert!(text.contains("[killed"), "{text}");
}

/// A task cannot start a daemon: its Job kills what it holds when it ends, and the CLI says so
/// instead of leaving a daemon that would die with its starter.
#[cfg(windows)]
#[test]
fn a_task_cannot_start_a_daemon_and_says_so() {
    let runtime = Scratch::new("daemon-in-task");
    crate::support::create_private_runtime(&runtime);
    let namespace = format!("mcp{}", std::process::id());
    let exe = env!("CARGO_BIN_EXE_fastexec");
    // Armed before the start, so a daemon that was started by accident is stopped even when an
    // assertion below fails and the runtime directory is about to go.
    let _stop = StopOnDrop(daemon_command(exe, &runtime, &namespace, "stop"));
    let mut server = Server::start_with(|command| {
        command
            .env("FASTEXEC_TEST_RUNTIME_DIR", &*runtime)
            .env("FASTEXEC_NAMESPACE", &namespace);
    });
    let command = format!("\"{exe}\" daemon start");
    let text = server.ok(json!({
        "action": "start", "command": command, "waitMs": 60000
    }));
    assert!(
        text.contains("[exited 1]"),
        "the start did not fail: {text}"
    );
    assert!(
        text.contains("could not leave this host's Job"),
        "the start did not explain itself: {text}"
    );

    // Nothing is left serving the namespace.
    let status = daemon_command(exe, &runtime, &namespace, "status")
        .output()
        .unwrap();
    let shown = String::from_utf8_lossy(&status.stdout).into_owned();
    assert!(
        shown.contains("No fastexec daemon for namespace"),
        "a daemon was left behind: {shown:?}"
    );
}

/// A CLI command for this namespace, in the test's own runtime directory.
#[cfg(windows)]
fn daemon_command(exe: &str, runtime: &std::path::Path, namespace: &str, action: &str) -> Command {
    let mut command = Command::new(exe);
    crate::support::scrub_fastexec_env(&mut command);
    command
        .args(["daemon", action])
        .env("FASTEXEC_TEST_RUNTIME_DIR", runtime)
        .env("FASTEXEC_NAMESPACE", namespace)
        .stdin(Stdio::null());
    command
}

/// Runs its command when it is dropped, so a daemon started by accident does not outlive a test.
#[cfg(windows)]
struct StopOnDrop(Command);

#[cfg(windows)]
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let _ = self.0.output();
    }
}

#[cfg(unix)]
#[test]
fn bash_discovery_still_works_after_the_session_directory_is_removed() {
    let gone = Scratch::new("gone-launch-cwd");
    std::fs::create_dir(&*gone).unwrap();
    let home = Scratch::new("gone-cwd-profile");
    std::fs::create_dir(&*home).unwrap();
    std::fs::write(home.join(".bash_profile"), "exit 7\n").unwrap();
    let mut server = Server::start_with(|command| {
        command.current_dir(&*gone).env("HOME", &*home);
    });
    std::fs::remove_dir(&*gone).unwrap();
    // The probe can run from bash's directory, but the actual start with the deleted cwd fails.
    let (text, failed) = server.call(json!({"action": "start", "command": "true"}));
    assert!(
        failed && text.contains("Cannot start the command"),
        "{text}"
    );
    let cwd = std::env::temp_dir();
    let text = server.ok(json!({
        "action": "start", "command": "printf discovered", "cwd": cwd,
        "loginShell": false, "waitMs": 10000
    }));
    assert!(text.contains("[exited 0]"), "{text}");
    assert!(text.starts_with("discovered"), "{text}");
    assert!(text.contains("Login environment probe failed"), "{text}");
    // The successful discovery is cached, and later tasks still work from an explicit cwd.
    let text = server.ok(json!({
        "action": "start", "command": "printf cached", "cwd": cwd,
        "loginShell": false, "waitMs": 10000
    }));
    assert!(text.contains("[exited 0]"), "{text}");
    assert!(text.starts_with("cached"), "{text}");
}

#[cfg(unix)]
#[test]
fn bash_version_probes_end_background_children_on_success_error_and_timeout() {
    let bash = if cfg!(target_os = "freebsd") {
        "/usr/local/bin/bash"
    } else {
        "/bin/bash"
    };
    use std::os::unix::fs::PermissionsExt;
    for (name, finish, should_succeed) in [
        ("success", "exit 0", true),
        ("failure", "exit 7", false),
        ("timeout", "sleep 30", false),
        ("large", "head -c 5000 /dev/zero; exit 0", false),
    ] {
        let dir = Scratch::new(&format!("bash-probe-{name}"));
        std::fs::create_dir(&*dir).unwrap();
        let wrapper = dir.join("bash");
        let beat = dir.join("beat");
        // A bounded heartbeat keeps stdout open after the version root exits. It is part of
        // the probe's tree even though no user-visible Task exists for a failed discovery.
        std::fs::write(&wrapper, format!(
            "#!/bin/sh\nif [ \"$1\" = --version ]; then\n (for i in $(seq 300); do printf x >> '{}'; sleep 0.1; done) &\n while [ ! -s '{}' ]; do sleep 0.01; done\n printf 'GNU bash, version 5.2\\n'\n {finish}\nfi\nexec {bash} \"$@\"\n",
            beat.display(), beat.display()
        )).unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut server = Server::start_with(|command| {
            command.env("FASTEXEC_BASH", &wrapper);
        });
        let began = Instant::now();
        let (text, is_error) = server.call(json!({
            "action": "start", "command": "printf tested", "loginShell": false, "waitMs": 10000
        }));
        assert_eq!(!is_error, should_succeed, "{name}: {text}");
        assert!(
            began.elapsed() < Duration::from_secs(15),
            "{name}: discovery was unbounded"
        );
        assert!(
            file_len(&beat) > 0,
            "{name}: the background child did not start"
        );
        assert_stops_growing(&beat);
    }
}

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
#[test]
fn bash_version_probe_owns_a_child_that_detaches_its_session() {
    let bash = if cfg!(target_os = "freebsd") {
        "/usr/local/bin/bash"
    } else {
        "/bin/bash"
    };
    use std::os::unix::fs::PermissionsExt;
    let dir = Scratch::new("bash-probe-detached");
    std::fs::create_dir(&*dir).unwrap();
    let wrapper = dir.join("bash");
    let beat = dir.join("beat");
    std::fs::write(&wrapper, format!(
        "#!/bin/sh\nif [ \"$1\" = --version ]; then\n perl -MPOSIX -e 'exit if fork; POSIX::setsid() or die \"setsid: $!\"; exec @ARGV' /bin/sh -c 'for i in $(seq 300); do printf x >> \"{}\"; sleep 0.1; done'\n while [ ! -s '{}' ]; do sleep 0.01; done\n printf 'GNU bash, version 5.2\\n'\n exit 0\nfi\nexec {bash} \"$@\"\n",
        beat.display(), beat.display()
    )).unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut server = Server::start_with(|command| {
        command.env("FASTEXEC_BASH", &wrapper);
    });
    let text = server.ok(json!({
        "action": "start", "command": "printf tested", "loginShell": false, "waitMs": 10000
    }));
    assert!(text.contains("[exited 0]"), "{text}");
    assert!(file_len(&beat) > 0, "the detached child did not start");
    assert_stops_growing(&beat);
}

#[test]
fn reader_handles_keep_independent_cursors_with_bare_id_attaches() {
    let mut server = Server::start();
    let text = server.ok(json!({
        "action": "start", "eof": false, "command": "printf 'reader-first\\n'; IFS= read -r line; printf 'reader-second:%s\\n' \"$line\"",
        "loginShell": false, "waitMs": 10000,
        "returnWhen": {"outputContains": ["reader-first"]}
    }));
    let first = task_id_of(&text).to_owned();
    let task = bare_id(&first).to_owned();
    assert!(
        text.contains(&first) && !text.contains("new reader"),
        "{text}"
    );
    let listing = server.ok(json!({"action": "list"}));
    assert!(
        listing.starts_with(&task) && !listing.contains(&first),
        "{listing}"
    );

    let text = server.ok(json!({"action": "poll", "taskId": &task, "waitMs": 0}));
    let second = task_id_of(&text).to_owned();
    assert_ne!(first, second);
    assert_eq!(bare_id(&second), task);
    assert!(
        text.starts_with("reader-first\n") && text.contains(" · new reader"),
        "{text}"
    );
    let text = server.ok(json!({"action": "poll", "taskId": &first, "waitMs": 0}));
    assert!(
        text.starts_with("(no new output)") && !text.contains("new reader"),
        "{text}"
    );

    let text =
        server.ok(json!({"action": "poll", "taskId": &first, "input": "next\n", "waitMs": 10000}));
    assert!(
        text.starts_with("reader-second:next\n") && !text.contains("reader-first"),
        "{text}"
    );
    let text = server.ok(json!({"action": "poll", "taskId": &second, "waitMs": 10000}));
    assert!(
        text.starts_with("reader-second:next\n") && !text.contains("new reader"),
        "{text}"
    );
    let text = server.ok(json!({"action": "poll", "taskId": &task, "waitMs": 0}));
    let third = task_id_of(&text).to_owned();
    assert_ne!(third, first);
    assert_ne!(third, second);
    assert!(
        text.starts_with("reader-first\nreader-second:next\n") && text.contains("new reader"),
        "{text}"
    );
}

#[test]
fn fresh_reader_notice_survives_kill_and_fits_every_result_mode() {
    let mut server = Server::start();
    server.ok(json!({"action": "start", "command": "sleep 60", "loginShell": false, "waitMs": 0}));
    for mode in ["stream", "screen", "transcript"] {
        let text = server.ok(json!({
            "action": "start", "command": "printf 'reader-ready\\n'; sleep 60", "loginShell": false,
            "pty": mode != "stream", "waitMs": 10000,
            "returnWhen": {"outputContains": ["reader-ready"]}
        }));
        assert!(!text.contains("new reader"), "{mode}: {text}");
        let bare = bare_id(task_id_of(&text)).to_owned();
        let text = server.ok(json!({
            "action": "kill", "taskId": &bare, "maxBytes": 100
        }));
        let kill_handle = task_id_of(&text).to_owned();
        // P1-4: kill on a bare taskId delivers unseen output and carries the new-reader notice.
        assert!(
            body_of(&text).len() <= 100
                && text.contains(&kill_handle)
                && text.contains(" · new reader"),
            "{mode}: {text}"
        );
        assert!(
            text.lines().last().unwrap().starts_with("(Background: "),
            "{mode}: {text}"
        );
        let after_kill = server.ok(json!({
            "action": "poll", "taskId": &kill_handle, "maxBytes": 100, "waitMs": 0
        }));
        assert!(
            body_of(&after_kill).len() <= 100 && !after_kill.contains("new reader"),
            "{mode}: {after_kill}"
        );
        // A fresh bare attach into poll/screen/transcript also carries new reader once within 100B body.
        let mut request = if mode == "transcript" {
            json!({"action": "transcript", "taskId": &bare, "maxBytes": 100})
        } else {
            json!({"action": "poll", "taskId": &bare, "maxBytes": 100, "waitMs": 0})
        };
        if mode == "screen" {
            request["screen"] = json!(true);
        }
        let text = server.ok(request.clone());
        let handle = task_id_of(&text).to_owned();
        assert!(
            body_of(&text).len() <= 100 && text.contains(&handle) && text.contains(" · new reader"),
            "{mode}: {text}"
        );
        request["taskId"] = json!(handle);
        assert!(
            text.lines().last().unwrap().starts_with("(Background: "),
            "{mode}: {text}"
        );
        let text = server.ok(request);
        assert!(
            body_of(&text).len() <= 100 && !text.contains("new reader"),
            "{mode}: {text}"
        );
    }
}

#[test]
fn pipe_start_stdin_defaults_to_eof_delivers_1mib_and_warns_on_early_close() {
    let (dir_guard, bash_dir) = scratch("pipe-stdin-p04");
    std::fs::create_dir_all(&*dir_guard).unwrap();
    std::fs::write(dir_guard.join("sample.txt"), "needle_token\nother\n").unwrap();
    let mut server = codemode_server();

    let cat_text = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "cat",
        "waitMs": 5000
    }));
    assert!(cat_text.contains("[exited 0]"), "{cat_text}");
    assert!(
        !cat_text.contains(" · log "),
        "clean exit status omits log segment: {cat_text}"
    );

    let grep_text = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "cwd": bash_dir,
        "command": "if command -v rg >/dev/null 2>&1; then rg --no-filename needle_token; else [[ ! -p /dev/stdin ]] && grep needle_token sample.txt; fi",
        "waitMs": 5000
    }));
    assert!(grep_text.contains("[exited 0]"), "{grep_text}");
    assert_eq!(body_of(&grep_text), "needle_token", "{grep_text}");

    let sorted_text = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "sort -r",
        "input": "a\nb\n"
    }));
    assert!(sorted_text.contains("[exited 0]"), "{sorted_text}");
    assert_eq!(body_of(&sorted_text), "b\na", "{sorted_text}");

    let empty_in = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "cat",
        "input": ""
    }));
    assert!(empty_in.contains("[exited 0]"), "{empty_in}");

    let sleep_null = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "sleep 5",
        "waitMs": 50
    }));
    let null_id = task_id_of(&sleep_null).to_owned();
    server.ok(json!({"action": "poll", "taskId": &null_id, "eof": true, "waitMs": 0}));
    let (err_text, is_err) = server.call(json!({
        "action": "poll",
        "taskId": &null_id,
        "input": "hello\n",
        "waitMs": 0
    }));
    assert!(is_err && err_text.contains("eof: false"), "{err_text}");
    server.ok(json!({"action": "kill", "taskId": &null_id}));

    for payload in [
        "x".repeat(1 << 20),
        "\n".repeat(1 << 20),
        "é".repeat(512 * 1024),
        "\0".repeat(1 << 20),
    ] {
        assert_eq!(payload.len(), 1 << 20);
        let wc_text = server.ok(json!({
            "action": "start",
            "loginShell": false,
            "command": "wc -c",
            "input": payload
        }));
        assert!(wc_text.contains("[exited 0]"), "{wc_text}");
        assert_eq!(body_of(&wc_text).trim(), "1048576", "{wc_text}");
    }
    let (over_text, over_err) = server.call(json!({
        "action": "start",
        "loginShell": false,
        "command": "cat",
        "input": "a".repeat((1 << 20) + 1)
    }));
    assert!(over_err && over_text.contains("1 MiB"), "{over_text}");

    let nested_1m = code_result(
        &mut server,
        json!({
            "code": r#"
                for (const unit of ["x", "\n", "é", "\0"]) {
                    const ok = await tools.exec({
                        action: "start",
                        loginShell: false,
                        command: "wc -c",
                        input: unit.repeat(unit === "é" ? 524288 : 1048576)
                    });
                    if (ok.exitCode !== 0 || ok.output.trim() !== "1048576")
                        throw new Error("wc: " + ok.output);
                }
                const before = (await tools.exec({action: "list"})).totalCount;
                let refused = false;
                try {
                    await tools.exec({
                        action: "start",
                        loginShell: false,
                        command: "cat",
                        input: "a".repeat(1048577)
                    });
                } catch (e) {
                    if (e.name === "ToolError") refused = true;
                }
                if (!refused) throw new Error("expected ToolError for >1 MiB");
                const after = (await tools.exec({action: "list"})).totalCount;
                if (after !== before) throw new Error("refused start created a task");
                return "nested-1mib-ok";
            "#
        }),
    );
    assert!(code_text(&nested_1m).contains("[exited 0]"), "{nested_1m}");

    let early_text = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "head -c 10",
        "input": "abcdefghij".repeat(104_858)[..(1 << 20)].to_owned()
    }));
    assert_eq!(body_of(&early_text), "abcdefghij", "{early_text}");
    assert!(
        early_text.contains("stdin closed after ") && early_text.contains(" of 1048576 bytes"),
        "{early_text}"
    );
    let early_open = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "head -c 10",
        "input": "z".repeat(1 << 20),
        "eof": false,
        "waitMs": 0
    }));
    let early_open_id = task_id_of(&early_open).to_owned();
    let eof_after_bp = server.ok(json!({
        "action": "poll",
        "taskId": early_open_id,
        "eof": true,
        "waitMs": 5000
    }));
    assert!(
        !eof_after_bp.contains("stdin write failed"),
        "{eof_after_bp}"
    );

    let unread_t0 = Instant::now();
    let unread = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "sleep 30",
        "input": "q".repeat(1 << 20),
        "eof": false,
        "waitMs": 150
    }));
    assert!(unread.contains("[running]"), "{unread}");
    assert!(unread_t0.elapsed() < Duration::from_secs(5));
    let unread_id = task_id_of(&unread).to_owned();
    let killed_unread = server.ok(json!({"action": "kill", "taskId": unread_id}));
    assert!(killed_unread.contains("[killed"), "{killed_unread}");

    let ord_start = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "cat",
        "input": "first-line\n",
        "eof": false,
        "waitMs": 0
    }));
    let ord_id = task_id_of(&ord_start).to_owned();
    let mut parts = Vec::new();
    let start_body = body_of(&ord_start);
    if !start_body.is_empty() && start_body != "(no new output)" {
        parts.push(start_body.to_owned());
    }
    let ord_poll = server.ok(json!({
        "action": "poll",
        "taskId": ord_id,
        "input": "second-line\n",
        "eof": true,
        "waitMs": 5000
    }));
    let poll_body = body_of(&ord_poll);
    if !poll_body.is_empty() && poll_body != "(no new output)" {
        parts.push(poll_body.to_owned());
    }
    assert_eq!(parts.join("\n"), "first-line\nsecond-line");
}

#[cfg(target_os = "linux")]
#[test]
fn kill_waits_for_output_closure_and_reports_unconfirmed_capture() {
    let mut server = codemode_server();
    let start = server.ok(json!({
        "action":"start", "loginShell":false, "eof":false,
        "command":"printf 'ready %s\\n' \"$BASHPID\"; read -r done",
        "returnWhen":{"outputContains":["ready "]}
    }));
    let pid: u32 = body_of(&start)
        .trim()
        .strip_prefix("ready ")
        .unwrap()
        .parse()
        .unwrap();
    // Hold one writer independently of the completed tree to control when capture reaches EOF.
    let mut writer = std::fs::OpenOptions::new()
        .write(true)
        .open(format!("/proc/{pid}/fd/1"))
        .unwrap();
    let id = task_id_of(&start).to_owned();
    let exited = server
        .ok(json!({"action":"poll", "taskId": &id, "input":"done\n", "eof":true, "waitMs":10000}));
    assert!(exited.contains("[exited 0]"), "{exited}");
    assert!(exited.contains("output stayed open after exit"), "{exited}");

    let text = server.ok(json!({"action":"kill", "taskId": &id, "maxBytes":100}));
    assert!(text.contains("output did not close within 5 s"), "{text}");
    let closer = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(150));
        writeln!(writer, "late-output").unwrap();
    });
    let text = server.ok(json!({"action":"kill", "taskId": &id}));
    closer.join().unwrap();
    assert!(!text.contains("output did not close"), "{text}");
    assert_eq!(body_of(&text), "late-output", "{text}");
    let after = server.ok(json!({"action":"poll", "taskId": &id}));
    assert_eq!(body_of(&after), "(no new output)", "{after}");
}

#[test]
fn cancelled_start_leaves_the_login_probe_warning_for_the_next_delivery() {
    let home = Scratch::new("login-warning-cancel");
    std::fs::create_dir_all(&*home).unwrap();
    std::fs::write(home.join(".bash_profile"), "exit 7\n").unwrap();
    let mut server = Server::start_with(|cmd| {
        cmd.env("HOME", &*home)
            .env("USERPROFILE", &*home)
            .env("FASTEXEC_CODEMODE", "true");
    });
    let code = r#"
        const command = "read -r keep_open";
        const waiting = tools.exec({action: "start", command, eof: false});
        while (!(await tools.exec({action: "list"})).tasks.some(t => t.command === command || t.children?.activeTasks?.some(c => c.command === command))) {
            await sleep(10);
        }
        const other = await tools.exec({action: "start", command: "true"});
        if (other.warnings?.some(w => w.includes("Login environment probe failed"))) {
            throw Error("another start reported the first start's reserved warning");
        }
        return "registered";
    "#;
    let ended = code_result(&mut server, json!({"code": code}));
    assert!(code_text(&ended).contains("[exited 0]"), "{ended}");
    assert_eq!(body_of(code_text(&ended)), "registered", "{ended}");
    let text =
        server.ok(json!({"action": "start", "command": "true", "pty": true, "screen": true}));
    assert!(text.contains("Login environment probe failed"), "{text}");
    let once = server.ok(json!({"action": "start", "command": "true"}));
    assert!(!once.contains("Login environment probe failed"), "{once}");
}

#[test]
fn p1_1_login_environment_snapshot_bash_cache_and_fallbacks() {
    // 1. Profile exports FX_PROFILE=1, prints BANNER, cd /, and appends to $HOME/count.
    let home1 = Scratch::new("p1-1-home1");
    let work1 = Scratch::new("p1-1-work1");
    let sub1 = work1.join("sub1");
    let sub2 = work1.join("sub2");
    let sub3 = work1.join("sub3");
    std::fs::create_dir_all(&*home1).unwrap();
    for d in [&*work1, &sub1, &sub2, &sub3] {
        std::fs::create_dir_all(d).unwrap();
    }
    std::fs::write(
        home1.join(".bash_profile"),
        "export FX_PROFILE=1\necho BANNER\ncd /\necho x >> \"$HOME/count\"\n",
    )
    .unwrap();

    let mut s1 = Server::start_with(|cmd| {
        cmd.current_dir(&*work1)
            .env("HOME", &*home1)
            .env("USERPROFILE", &*home1)
            .env("FASTEXEC_CODEMODE", "true");
    });
    for _ in 0..3 {
        let text = s1.ok(json!({
            "action": "start",
            "command": "printf '%s|%s' \"$FX_PROFILE\" \"$(pwd -P)\""
        }));
        let body = body_of(&text);
        assert!(body.starts_with("1|"), "{text}");
        assert!(
            body.contains(work1.file_name().unwrap().to_str().unwrap()),
            "cwd must be requested directory, not /: {body}"
        );
        assert!(!text.contains("BANNER"), "{text}");
    }
    for sub in [&sub1, &sub2, &sub3] {
        let cwd_str = sub.to_string_lossy().replace('\\', "/");
        let cm = code_result(
            &mut s1,
            json!({
                "cwd": cwd_str,
                "code": "const r = await tools.exec({ action: 'start', command: 'printf \"%s|%s\" \"$FX_PROFILE\" \"$(pwd -P)\"' }); return r.output;"
            }),
        );
        let out_str = body_of(code_text(&cm));
        assert!(
            out_str.starts_with("1|") && !out_str.contains("BANNER"),
            "{out_str}"
        );
    }
    let count_lines = std::fs::read_to_string(home1.join("count")).unwrap();
    assert_eq!(
        count_lines.lines().count(),
        1,
        "expected 1 probe: {count_lines:?}"
    );

    // loginShell: true runs the profile again and includes BANNER.
    let login_out = s1.ok(json!({
        "action": "start",
        "loginShell": true,
        "command": "printf after"
    }));
    assert!(body_of(&login_out).contains("BANNER"), "{login_out}");
    drop(s1);

    // 2. Profile exporting SHELLOPTS (errexit) does not force errexit into default non-login tasks,
    // and default TERM is dumb for pipe and xterm-256color for PTY.
    let home2 = Scratch::new("p1-1-shellopts");
    std::fs::create_dir_all(&*home2).unwrap();
    std::fs::write(
        home2.join(".bash_profile"),
        "set -o errexit\nexport SHELLOPTS\nexport TERM=vt100\n",
    )
    .unwrap();
    let mut s2 = Server::start_with(|cmd| {
        cmd.env("HOME", &*home2).env("USERPROFILE", &*home2);
    });
    let ok_errexit = s2.ok(json!({
        "action": "start",
        "command": "false; echo ok"
    }));
    assert!(ok_errexit.contains("[exited 0]"), "{ok_errexit}");
    assert_eq!(body_of(&ok_errexit), "ok", "{ok_errexit}");

    let pipe_term = s2.ok(json!({"action": "start", "command": "printf '%s' \"$TERM\""}));
    assert_eq!(body_of(&pipe_term), "dumb", "{pipe_term}");
    let pty_term = s2.ok(json!({
        "action": "start",
        "pty": true,
        "command": "printf '%s\\n' \"$TERM\""
    }));
    assert_eq!(body_of(&pty_term).trim(), "xterm-256color", "{pty_term}");
    drop(s2);

    // 3. Profile hanging (`sleep 30`) times out in ~5 s with a one-time warning, and subsequent starts
    // return immediately without repeating the warning.
    let home3 = Scratch::new("p1-1-timeout");
    std::fs::create_dir_all(&*home3).unwrap();
    std::fs::write(home3.join(".bash_profile"), "sleep 30\n").unwrap();
    let mut s3 = Server::start_with(|cmd| {
        cmd.env("HOME", &*home3).env("USERPROFILE", &*home3);
    });
    let t_probe = Instant::now();
    let first_text = s3.ok(json!({
        "action": "start",
        "command": "printf fallback-1",
        "waitMs": 20000
    }));
    let dt_first = t_probe.elapsed();
    assert!(
        dt_first >= Duration::from_secs(4) && dt_first < Duration::from_secs(15),
        "probe timeout took {dt_first:?}"
    );
    assert_eq!(body_of(&first_text), "fallback-1", "{first_text}");
    assert!(
        first_text.contains("Warning: Login environment probe failed")
            && first_text.contains("Pass loginShell: true to run the profile."),
        "{first_text}"
    );

    let t_second = Instant::now();
    let second_text = s3.ok(json!({
        "action": "start",
        "command": "printf fallback-2",
        "waitMs": 20000
    }));
    assert!(
        t_second.elapsed() < Duration::from_secs(3),
        "second start must not re-probe: {:?}",
        t_second.elapsed()
    );
    assert_eq!(body_of(&second_text), "fallback-2", "{second_text}");
    assert!(
        !second_text.contains("Login environment probe failed"),
        "{second_text}"
    );
    drop(s3);

    // 4. Counting FASTEXEC_BASH wrapper on Unix: `--version` runs only once across direct starts
    // and cwd-overridden codemode tasks in the same session.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let wrap_dir = Scratch::new("p1-1-bash-wrap");
        let wrap_sub = wrap_dir.join("sub");
        std::fs::create_dir_all(&wrap_sub).unwrap();
        let real_bash = ["/bin/bash", "/usr/bin/bash", "/usr/local/bin/bash"]
            .into_iter()
            .find(|p| std::path::Path::new(p).exists())
            .expect("system bash");
        let ver_count = wrap_dir.join("version_count");
        let wrapper = wrap_dir.join("bash-wrapper");
        std::fs::write(
            &wrapper,
            format!(
                "#!/bin/sh\nif [ \"${{1:-}}\" = \"--version\" ]; then echo v >> '{}'; fi\nexec '{}' \"$@\"\n",
                ver_count.display(),
                real_bash
            ),
        )
        .unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o755)).unwrap();

        let mut s4 = Server::start_with(|cmd| {
            cmd.current_dir(&*wrap_dir)
                .env("FASTEXEC_BASH", &wrapper)
                .env("FASTEXEC_CODEMODE", "true");
        });
        for _ in 0..3 {
            s4.ok(json!({"action": "start", "command": "true"}));
        }
        let sub_str = wrap_sub.to_string_lossy().into_owned();
        for _ in 0..3 {
            let cm = code_result(
                &mut s4,
                json!({
                    "cwd": &sub_str,
                    "code": "await tools.exec({ action: 'start', command: 'true' }); return 'ok';"
                }),
            );
            assert!(code_text(&cm).contains("[exited 0]"), "{cm}");
        }
        let v_lines = std::fs::read_to_string(&ver_count).unwrap();
        assert_eq!(
            v_lines.lines().count(),
            1,
            "bash --version must run once per session: {v_lines:?}"
        );
    }
}

#[test]
fn task_exit_events_count_kept_children_until_the_tree_ends() {
    let mut server = Server::start_with(|command| {
        command.env("FASTEXEC_DAEMON", "off");
    });
    let meta = json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": {"name": "events-test", "version": "1"},
        "io.modelcontextprotocol/clientCapabilities": {},
    });
    let initial = server.request("events/poll", json!({"name": "task.exited", "_meta": meta}));
    let mut cursor = initial["result"]["cursor"].clone();
    assert!(cursor.is_string(), "{initial}");
    let text = server.ok(json!({
        "action": "start", "command": "sleep 300 &", "keepTree": true,
        "loginShell": false, "waitMs": 10000
    }));
    let handle = task_id_of(&text).to_owned();
    let task_id = bare_id(&handle);
    let next_event = |server: &mut Server, cursor: &mut Value| {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let response = server.request(
                "events/poll",
                json!({"name": "task.exited", "cursor": cursor, "_meta": meta}),
            );
            assert!(response.get("error").is_none(), "{response}");
            *cursor = response["result"]["cursor"].clone();
            if let Some(event) = response["result"]["events"].as_array().unwrap().first() {
                return event["data"].clone();
            }
            assert!(Instant::now() < deadline, "missing task exit event");
            std::thread::sleep(Duration::from_millis(10));
        }
    };
    let root = next_event(&mut server, &mut cursor);
    server.ok(json!({"action": "kill", "taskId": handle}));
    let ended = next_event(&mut server, &mut cursor);
    assert_eq!(root["taskId"], task_id);
    assert!(root["keptProcesses"].as_u64().unwrap() > 0, "{root}");
    assert_eq!(root["treeEnded"], false);
    assert_eq!(ended["taskId"], task_id);
    assert_eq!(ended["keptProcesses"], 0);
    assert_eq!(ended["treeEnded"], true);
}

#[test]
fn rejected_start_result_preserves_the_login_probe_warning() {
    for screen in [false, true] {
        let home = Scratch::new(if screen {
            "login-warning-screen-refused"
        } else {
            "login-warning-window-refused"
        });
        std::fs::create_dir_all(&*home).unwrap();
        std::fs::write(home.join(".bash_profile"), "exit 7\n").unwrap();
        let mut server = Server::start_with(|cmd| {
            cmd.env("HOME", &*home)
                .env("USERPROFILE", &*home)
                .env("FASTEXEC_TEST_OUTBOUND_LIMIT", "4096");
        });
        let mut request = json!({
            "action":"start", "command":"printf '%06000d' 0", "maxBytes":8192
        });
        if screen {
            request["pty"] = json!(true);
            request["screen"] = json!(true);
            request["cols"] = json!(120);
            request["rows"] = json!(100);
        }
        let refused = server.request("tools/call", json!({"name":"exec","arguments":request}));
        assert_eq!(refused["result"]["isError"], true, "{refused}");
        assert!(
            refused["result"].get("structuredContent").is_none(),
            "{refused}"
        );
        assert!(
            refused["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("frame limit"),
            "{refused}"
        );
        let next = server.ok(json!({"action":"start","command":"true"}));
        assert!(next.contains("Login environment probe failed"), "{next}");
        let once = server.ok(json!({"action":"start","command":"true"}));
        assert!(!once.contains("Login environment probe failed"), "{once}");
    }
}

#[test]
fn keeptree_list_lifecycle_idempotence_narrow_cut_and_ordered_events() {
    let mut server = Server::start_with(|command| {
        command.env("FASTEXEC_DAEMON", "off");
    });
    let meta = json!({
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientInfo": {"name": "keeptree-e2e", "version": "1"},
        "io.modelcontextprotocol/clientCapabilities": {},
    });
    let initial = server.request("events/poll", json!({"name": "task.exited", "_meta": meta}));
    let mut cursor = initial["result"]["cursor"].clone();
    assert!(cursor.is_string(), "{initial}");
    let drain_events =
        |server: &mut Server, cursor: &mut Value, target_id: &str, min_count: usize| {
            let mut collected = Vec::new();
            let deadline = Instant::now() + Duration::from_secs(20);
            while collected.len() < min_count && Instant::now() < deadline {
                let response = server.request(
                    "events/poll",
                    json!({"name": "task.exited", "cursor": cursor, "_meta": meta}),
                );
                assert!(response.get("error").is_none(), "{response}");
                *cursor = response["result"]["cursor"].clone();
                for event in response["result"]["events"].as_array().unwrap() {
                    if event["data"]["taskId"].as_str() == Some(target_id) {
                        collected.push(event["data"].clone());
                    }
                }
                if collected.len() < min_count {
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
            collected
        };

    // 1. keepTree running root shows `[running] · keepTree` in list; killing while running
    // emits a single `task.exited` event with `treeEnded: false`.
    let running_res = server.ok(json!({
        "action": "start",
        "command": "sleep 30",
        "keepTree": true,
        "waitMs": 0
    }));
    let running_handle = task_id_of(&running_res).to_owned();
    let running_id = bare_id(&running_handle).to_owned();
    let list_running = server.ok(json!({"action": "list", "limit": 5}));
    assert!(
        list_running.contains(&format!("{running_id} [running] · keepTree · ")),
        "expected running keepTree marker:\n{list_running}"
    );
    server.ok(json!({"action": "kill", "taskId": running_handle}));
    std::thread::sleep(Duration::from_millis(150));
    let running_events = drain_events(&mut server, &mut cursor, &running_id, 1);
    assert_eq!(running_events.len(), 1, "{running_events:?}");
    assert_eq!(running_events[0]["treeEnded"], false, "{running_events:?}");

    // 2. keepTree root exiting cleanly with no leftovers shows `[exited 0] · keepTree`.
    let clean_res = server.ok(json!({
        "action": "start",
        "command": "echo clean",
        "keepTree": true,
        "waitMs": 10000
    }));
    let clean_id = bare_id(task_id_of(&clean_res)).to_owned();
    let list_clean = server.ok(json!({"action": "list", "limit": 5}));
    assert!(
        list_clean.contains(&format!("{clean_id} [exited 0] · keepTree · ")),
        "expected clean keepTree marker:\n{list_clean}"
    );

    let sleep = if cfg!(windows) { "sleep.exe" } else { "sleep" };

    // 3. keepTree non-zero exit holding a child shows `[exited 3] · keeps {sleep} (`, then
    // after child ends shows `[exited 3] · kept processes ended`, remains idempotent across
    // repeated `exec: list` calls after footer acknowledgement, and emits ordered events.
    let held_res = server.ok(json!({
        "action": "start",
        "command": "sleep 300 & exit 3",
        "keepTree": true,
        "waitMs": 10000
    }));
    let held_handle = task_id_of(&held_res).to_owned();
    let held_id = bare_id(&held_handle).to_owned();
    let list_held = server.ok(json!({"action": "list", "limit": 5}));
    assert!(
        list_held.contains(&format!("{held_id} [exited 3] · keeps {sleep} (")),
        "expected non-zero kept marker:\n{list_held}"
    );
    server.ok(json!({"action": "kill", "taskId": held_handle}));
    let held_events = drain_events(&mut server, &mut cursor, &held_id, 2);
    assert_eq!(held_events.len(), 2, "{held_events:?}");
    assert_eq!(held_events[0]["treeEnded"], false, "{held_events:?}");
    assert!(
        held_events[0]["keptProcesses"].as_u64().unwrap() > 0,
        "{held_events:?}"
    );
    assert_eq!(held_events[1]["treeEnded"], true, "{held_events:?}");
    assert_eq!(held_events[1]["keptProcesses"], 0, "{held_events:?}");

    // Consume any footer notices, then verify repeated `exec: list` stays idempotent.
    let _ = server.ok(json!({"action": "start", "command": "true"}));
    let _ = server.ok(json!({"action": "start", "command": "true"}));
    let list_ended_1 = server.ok(json!({"action": "list", "limit": 10}));
    let list_ended_2 = server.ok(json!({"action": "list", "limit": 10}));
    assert!(
        list_ended_1.contains(&format!("{held_id} [exited 3] · kept processes ended · ")),
        "expected kept processes ended:\n{list_ended_1}"
    );
    assert!(
        list_ended_2.contains(&format!("{held_id} [exited 3] · kept processes ended · ")),
        "repeated list must stay idempotent:\n{list_ended_2}"
    );

    // 4. 12 background processes with `limit: 1, maxBytes: 100` triggers Tier 4 brief cut
    // (`keeps sleep +11` + ` … [cut]`) and conserves keeps acknowledgement in the footer.
    let twelve_res = server.ok(json!({
        "action": "start",
        "command": "for i in 1 2 3 4 5 6 7 8 9 10 11 12; do sleep 300 & done; echo twelve",
        "keepTree": true,
        "waitMs": 10000
    }));
    let twelve_handle = task_id_of(&twelve_res).to_owned();
    let twelve_id = bare_id(&twelve_handle).to_owned();
    let narrow = server.ok(json!({
        "action": "list",
        "limit": 1,
        "maxBytes": 100
    }));
    let first_line = narrow.lines().next().unwrap_or("");
    assert!(
        first_line.len() <= 100,
        "first line exceeded 100 bytes ({}): {first_line}",
        first_line.len()
    );
    assert!(
        first_line.contains(&format!("{twelve_id} [exited 0] · keeps {sleep} +11")),
        "expected Tier 4 brief for 12 held processes:\n{narrow}"
    );
    assert!(
        first_line.ends_with(" … [cut]"),
        "expected inline cut marker:\n{narrow}"
    );
    // Tier 4 confirmed keeps, so the next direct start must not repeat `twelve_id keeps` in footer.
    let after_narrow = server.ok(json!({"action": "start", "command": "true"}));
    assert!(
        !after_narrow.contains(&format!("{twelve_id} keeps ")),
        "Tier 4 ack should suppress duplicate footer keeps notice:\n{after_narrow}"
    );
    server.ok(json!({"action": "kill", "taskId": twelve_handle}));
}
