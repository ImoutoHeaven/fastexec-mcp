use super::common::{Server, log_path_of, scratch, status_line, task_id_of};
use serde_json::json;
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[test]
fn pty_task_sees_a_terminal_and_accepts_a_hidden_password() {
    let mut server = Server::start();
    let command = "[ -t 0 ] && echo tty; read -r -s -p 'Password: ' p; echo; echo \"len=${#p}\"";
    let text = server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 30000, "returnWhen": {"outputContains": ["Password:"]}}));
    assert!(text.contains("[running]"), "{text}");
    assert!(text.contains("tty") && text.contains("Password:"), "{text}");
    let text = server.ok(
        json!({"action": "poll", "taskId": server.id(1), "input": "secret\r", "waitMs": 10000}),
    );
    assert!(text.contains("[exited 0]"), "{text}");
    assert!(text.contains("len=6") && !text.contains("secret"), "{text}");
    let (text, is_error) =
        server.call(json!({"action": "poll", "taskId": server.id(1), "eof": true}));
    assert!(
        is_error && text.contains("EOF_UNSUPPORTED_IN_PTY"),
        "{text}"
    );
}

/// The bytes `od -An -tx1` printed, one space between them: BSD `od` pads its columns to the
/// width of a full line and GNU `od` does not.
fn od(text: &str) -> String {
    let mut normalized = String::new();
    for line in text.split_inclusive('\n') {
        let words: Vec<&str> = line.split_whitespace().collect();
        let bytes = !words.is_empty()
            && words
                .iter()
                .all(|word| word.len() == 2 && word.bytes().all(|b| b.is_ascii_hexdigit()));
        if bytes {
            normalized.push(' ');
            normalized.push_str(&words.join(" "));
            if line.ends_with('\n') {
                normalized.push('\n');
            }
        } else {
            normalized.push_str(line);
        }
    }
    normalized
}

#[test]
fn pty_keys_follow_input_and_the_programs_cursor_mode() {
    let mut server = Server::start();
    // The program switches to application cursor mode (DECCKM), then dumps what it reads.
    let command = "stty raw -echo; printf '\\033[?1hready\\r\\n'; head -c 9 | od -An -tx1";
    let text = server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 30000, "returnWhen": {"outputContains": ["ready"]}}));
    assert!(text.contains("ready"), "{text}");
    let (text, is_error) =
        server.call(json!({"action": "poll", "taskId": server.id(1), "keys": ["Up", "C-Enter"]}));
    assert!(is_error && text.contains("C-Enter"), "{text}");
    let text = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "input": "ab", "keys": ["Enter", "Up", "C-c", "M-x"], "waitMs": 10000
    }));
    assert!(text.contains("[exited 0]"), "{text}");
    let text = od(&text);
    assert!(
        text.contains("61 62 0d 1b 4f 41 03 1b 78"),
        "input, then keys in order, with Up in application cursor mode: {text}"
    );

    server.ok(json!({"action": "start", "command": "cat", "waitMs": 0}));
    let (text, is_error) =
        server.call(json!({"action": "poll", "taskId": server.id(2), "keys": ["Enter"]}));
    assert!(is_error && text.contains("PTY tasks only"), "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(2)}));
}

/// The task host shares the terminal's foreground group with bash and must leave Ctrl-C to it.
// ConPTY delivers Ctrl-C to msys programs after a variable delay, so Windows is left out.
#[cfg(unix)]
#[test]
fn ctrl_c_interrupts_the_program_of_a_pty_task() {
    use std::os::unix::process::CommandExt;
    // Also from a server that inherited SIGINT ignored and blocked, as a background job can.
    let hostile = Server::start_with(|command| unsafe {
        command.pre_exec(|| {
            let mut set: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut set);
            libc::sigaddset(&mut set, libc::SIGINT);
            libc::sigprocmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
            libc::signal(libc::SIGINT, libc::SIG_IGN);
            Ok(())
        });
    });
    for mut server in [Server::start(), hostile] {
        let text = server.ok(json!({"action": "start", "command": "echo ready; sleep 30; echo after", "pty": true, "waitMs": 10000, "returnWhen": {"outputContains": ["ready"]}}));
        assert!(text.contains("[running]"), "{text}");
        let text = server.ok(
            json!({"action": "poll", "taskId": server.id(1), "keys": ["C-c"], "waitMs": 10000}),
        );
        assert!(text.contains("[exited 130]"), "{text}");
        assert!(!text.contains("after"), "{text}");
    }
}

#[test]
fn keys_pause_after_input_and_follow_the_modes_set_meanwhile() {
    let mut server = Server::start();
    // The program dumps each read, which returns the bytes that have arrived, and switches to
    // application cursor mode after the first one.
    let command = concat!(
        "stty raw -echo; printf 'ready\\r\\n'; dd bs=64 count=1 2>/dev/null | od -An -tx1; ",
        "printf '\\033[?1h'; for i in 1 2; do dd bs=64 count=1 2>/dev/null | od -An -tx1; done"
    );
    // With a pause the keys come after the text, Up after the mode switch; without one, the
    // text and keys go out in one write, read at once, before it. The pause is longer than the
    // default, so a slow machine starts the program's next read in time; the default pause is
    // measured below.
    for (delay, first, up) in [
        (Some(1000), " 61 62\n", "1b 4f 41"),
        (Some(0), " 61 62 0d 1b 5b 41\n", "1b 5b 41"),
        (None, "", ""),
    ] {
        let started = server.ok(json!({
            "action": "start", "command": command, "pty": true, "waitMs": 20000,
            "returnWhen": {"outputContains": ["ready"]}
        }));
        let task = task_id_of(&started).to_owned();
        let mut poll = json!({
            "action": "poll", "taskId": task, "input": "ab", "keys": ["Enter", "Up"],
            "waitMs": 10000, "returnWhen": {"outputContains": [" 41"]}
        });
        if let Some(delay) = delay {
            poll["keyDelayMs"] = json!(delay);
        }
        let begun = Instant::now();
        let text = server.ok(poll);
        let text = od(&text);
        if delay.is_none() {
            // 150 ms before each of the two keys.
            assert!(begun.elapsed() >= Duration::from_millis(300), "{text}");
        }
        assert!(
            text.starts_with(first) && text.contains(up),
            "{first:?}, {up}: {text}"
        );
        server.ok(json!({"action": "kill", "taskId": task}));
    }
}

#[test]
fn pty_keys_follow_the_kitty_keyboard_protocol_the_program_pushes() {
    let mut server = Server::start();
    // The program pushes the disambiguation flag, queries the flags, and dumps what it reads;
    // then it pops the flag and dumps again.
    let command = concat!(
        "stty raw -echo; printf '\\033[>1u\\033[?u'; IFS= read -r -d u flags; ",
        "printf 'flags=%s\\r\\n' \"${flags#?}\"; head -c 14 | od -An -tx1; ",
        "printf '\\033[<upopped\\r\\n'; head -c 2 | od -An -tx1"
    );
    let text = server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 30000, "returnWhen": {"outputContains": ["flags=[?1"]}}));
    assert!(text.contains("flags=[?1"), "{text}");
    let text = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "keys": ["Escape", "C-i", "Tab"], "waitMs": 30000,
        "returnWhen": {"outputContains": ["popped"]}
    }));
    let text = od(&text);
    assert!(
        text.contains("1b 5b 32 37 75 1b 5b 31 30 35 3b 35 75 09") && text.contains("popped"),
        "Escape and C-i as CSI u, Tab as itself: {text}"
    );
    let text = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "keys": ["Escape", "Tab"], "waitMs": 10000
    }));
    assert!(text.contains("[exited 0]"), "{text}");
    let text = od(&text);
    assert!(text.contains("1b 09"), "legacy bytes once popped: {text}");
}

#[test]
fn keyboard_stack_overflow_keeps_screen_keys_and_output_working() {
    let mut server = Server::start();
    // This used to trigger alacritty_terminal 0.26's 4097th-push panic. The bounded wrapper
    // must leave both raw output and the emulator's screen and keyboard modes usable.
    let command = "printf '\\033[>1u%.0s' $(seq 1 4097); printf 'alive\\r\\n'; read -r x; printf 'done\\r\\n'";
    let text = server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 30000, "returnWhen": {"outputContains": ["alive"]}}));
    assert!(text.contains("alive"), "{text}");
    let screen = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "screen": true, "waitMs": 0
    }));
    assert!(screen.contains("[running]"), "{screen}");
    assert!(
        screen.split("\n\n[").next().unwrap().contains("alive"),
        "{screen}"
    );
    let text = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "keys": ["Enter"], "waitMs": 10000
    }));
    assert!(text.contains("[exited 0]"), "{text}");
    assert!(text.contains("done"), "{text}");
    let transcript = server.ok(json!({
        "action": "transcript", "taskId": server.id(1), "maxBytes": 4194304
    }));
    assert!(
        transcript.contains("alive") && transcript.contains("done"),
        "{transcript}"
    );
}

#[test]
fn screen_shows_the_rendered_terminal_and_marks_the_stream_read() {
    let mut server = Server::start();
    // Frame 1 is printed, then cleared and overwritten in place at row 2.
    let command =
        "printf 'frame-1\\r\\n'; sleep 0.5; printf '\\033[H\\033[2J\\033[2;3Hframe-2'; read -r x";
    let text = server.ok(json!({
        "action": "start", "command": command, "pty": true, "screen": true, "waitMs": 30000,
        "returnWhen": {"screenContains": ["frame-2"]}
    }));
    assert!(text.contains("[running]"), "{text}");
    let body = text.split("\n\n[").next().unwrap();
    assert_eq!(body.trim_end(), "\n  frame-2", "{text}");
    assert!(text.contains("cursor row 2 col 10"), "{text}");
    let text = server.ok(json!({"action": "poll", "taskId": server.id(1), "waitMs": 0}));
    assert!(text.starts_with("(no new output)"), "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(1)}));

    // A full first row leaves the cursor in the last column, pending the wrap.
    let command = "printf '%0120d' 0; read -r x";
    let text = server.ok(json!({
        "action": "start", "command": command, "pty": true, "screen": true, "waitMs": 30000,
        "returnWhen": {"screenContains": ["0000000000"]}
    }));
    assert!(text.contains("cursor row 1 col 120"), "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(2)}));

    // A synchronized update the program leaves open shows once it times out.
    let command = "printf 'shown\\r\\n\\033[?2026hheld'; read -r x";
    let text = server.ok(json!({
        "action": "start", "command": command, "pty": true, "screen": true, "waitMs": 30000,
        "returnWhen": {"screenContains": ["held"]}
    }));
    assert_eq!(text.split("\n\n[").next().unwrap(), "shown\nheld", "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(3)}));
}

#[test]
fn screen_contains_and_quiet_see_only_committed_frames() {
    let mut server = Server::start();
    // The visible text is assembled by cursor moves; the earlier text is erased.
    let command = "printf 'GONE\\r\\n'; sleep 0.3; printf '\\033[2J\\033[H\\033[1;4Hdy\\033[1;1HRea'; read -r x";
    let text = server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 20000,
        "returnWhen": {"screenContains": ["Ready"]}
    }));
    assert!(text.contains("wait: screen matched \"Ready\""), "{text}");
    let text = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "waitMs": 500,
        "returnWhen": {"screenContains": ["GONE"]}
    }));
    assert!(text.contains("wait: waitMs elapsed"), "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(1)}));

    // A synchronized update the program leaves open shows at its timeout, while the program
    // writes nothing more; until then the output is not quiet.
    let command = "printf 'shown\\r\\n\\033[?2026hheld'; read -r x";
    let begun = Instant::now();
    let text = server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 20000,
        "returnWhen": {"screenContains": ["held"]}
    }));
    assert!(begun.elapsed() < Duration::from_secs(10), "{text}");
    assert!(text.contains("wait: screen matched \"held\""), "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(2)}));
    let command =
        "stty -echo; printf 'shown\\r\\n'; read -r x; printf '\\033[?2026hheld'; read -r x";
    server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 20000,
        "returnWhen": {"screenContains": ["shown"]}
    }));
    let text = server.ok(json!({
        "action": "poll", "taskId": server.id(3), "keys": ["Enter"], "screen": true, "waitMs": 20000,
        "returnWhen": {"quietMs": 100}
    }));
    assert!(text.contains("wait: output quiet"), "{text}");
    assert_eq!(text.split("\n\n[").next().unwrap(), "shown\nheld", "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(3)}));
}

#[test]
fn screen_appears_and_gone_compare_with_the_screen_before_the_input() {
    let mut server = Server::start();
    // Enter shows a busy line, then replaces it with a done line like the old one.
    let command = concat!(
        "stty -echo; printf 'Done 1\\r\\n'; read -r x; printf 'Busy'; sleep 0.5; ",
        "printf '\\r\\033[KDone 1\\r\\n'; read -r x"
    );
    server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 20000,
        "returnWhen": {"screenContains": ["Done 1"]}
    }));
    // The old done line shows throughout; only the second one is new.
    let text = server.ok(json!({
        "action": "poll", "taskId": server.id(1), "keys": ["Enter"], "screen": true, "waitMs": 20000,
        "returnWhen": {"screenAppears": ["Done 1"]}
    }));
    assert!(text.contains("wait: screen text appeared \"Done 1\""), "{text}");
    assert_eq!(
        text.split("\n\n[").next().unwrap(),
        "Done 1\nDone 1",
        "{text}"
    );
    server.ok(json!({"action": "kill", "taskId": server.id(1)}));

    // The busy line is absent when the poll begins; the wait ends only once it has shown, for
    // half a second, and gone.
    server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 20000,
        "returnWhen": {"screenContains": ["Done 1"]}
    }));
    let begun = Instant::now();
    let text = server.ok(json!({
        "action": "poll", "taskId": server.id(2), "keys": ["Enter"], "screen": true, "waitMs": 20000,
        "returnWhen": {"screenGone": ["Busy"]}
    }));
    assert!(text.contains("wait: screen text gone \"Busy\""), "{text}");
    assert!(begun.elapsed() >= Duration::from_millis(400), "{text}");
    assert!(
        !text.split("\n\n[").next().unwrap().contains("Busy"),
        "{text}"
    );
    server.ok(json!({"action": "kill", "taskId": server.id(2)}));
}

#[test]
fn screen_and_transcript_render_the_same_terminal() {
    let mut server = Server::start();
    // Each row needs the terminal to act on a sequence: REP repeats the last character, IRM
    // inserts, a combining mark joins its base, a wide character fills two columns, excessive
    // combining marks stop at the per-cell budget (DEL between them moves nothing), a tab
    // moves to its stop, and deleting the first half of a wide character leaves a blank.
    let command = concat!(
        "printf 'rep x\\033[4b\\r\\n'; ",
        "printf 'irm abc\\r\\033[5C\\033[4hZ\\033[4l\\r\\n'; ",
        "printf 'mix e\\314\\201 \\344\\270\\255\\346\\226\\207!\\r\\n'; ",
        "printf 'cap e'; for i in $(seq 40); do printf '\\314\\201\\177'; done; printf '\\r\\n'; ",
        "printf 'tab a\\tb\\r\\n'; ",
        "printf 'dch \\344\\270\\255a\\r\\033[4C\\033[P\\r\\n'; read -r x"
    );
    let expected = [
        "rep xxxxx".to_string(),
        "irm aZbc".to_string(),
        "mix e\u{301} 中文!".to_string(),
        format!("cap e{}", "\u{301}".repeat(32)),
        "tab a   b".to_string(),
        "dch  a".to_string(),
    ]
    .join("\n");
    // The wait targets the full final state, cursor included. A screen condition can match before
    // the trailing CRLF moves the cursor to [7, 1], and output that paused just before it looks
    // quiet while the cursor is still elsewhere, so the wait observes the whole state.
    server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 0
    }));
    let task = server.id(1);
    let text = wait_for_screen(&mut server, &task, &expected, "cursor row 7 col 1");
    assert!(text.contains("[running]"), "{text}");
    assert_eq!(
        text.split("\n\n[").next().unwrap(),
        expected,
        "screen: {text}"
    );
    assert!(text.contains("cursor row 7 col 1"), "{text}");
    #[cfg(unix)]
    {
        // The rendering cap must not alter the bytes captured from the Unix PTY.
        let raw = std::fs::read(log_path_of(&text).unwrap()).unwrap();
        let full_run = format!("cap e{}", "\u{301}\u{7f}".repeat(40));
        assert!(
            raw.windows(full_run.len())
                .any(|bytes| bytes == full_run.as_bytes()),
            "all 40 original marks and DEL bytes must remain in the raw log"
        );
    }
    let text =
        server.ok(json!({"action": "transcript", "taskId": server.id(1), "maxBytes": 4194304}));
    assert_eq!(
        text.split("\n\n[").next().unwrap(),
        expected,
        "transcript: {text}"
    );
    server.ok(json!({"action": "kill", "taskId": server.id(1)}));
}

#[test]
fn pty_programs_get_answers_to_terminal_queries() {
    let mut server = Server::start();
    // A cursor-position report (DSR 6) and primary device attributes (DA1), read raw.
    let command = concat!(
        "stty raw -echo; printf 'ab\\033[6n'; IFS= read -r -d R cpr; ",
        "printf '\\033[c'; IFS= read -r -d c da; ",
        "printf '\\r\\ncpr=%s da=%s\\r\\n' \"${cpr#?}\" \"${da#?}\""
    );
    // What the program waits for is a terminal answer, not a profile: a login shell spends seconds
    // sourcing one under load, for nothing this test reads.
    let text = server.ok(json!({
        "action": "start", "command": command, "pty": true, "loginShell": false, "waitMs": 10000
    }));
    assert!(text.contains("[exited 0]"), "{text}");
    assert!(text.contains("cpr=[1;3 da=[?"), "{text}");
}

#[test]
fn transcript_recovers_every_line_a_program_pushed_off_the_screen() {
    let mut server = Server::start();
    // Like pi, a full redraw clears the screen and its history (ED2, ED3), then reprints the
    // lines it showed and new ones. Like `clear`, a clear before new lines keeps what showed.
    // Like Codex, history lines are then inserted through a scroll region above a fixed
    // composer row. All push lines past the 30-row screen.
    let command = concat!(
        "printf 'kept %s\\n' {1..40}; printf '\\033[2J\\033[H\\033[3J'; printf 'kept %s\\n' {1..45}; ",
        "printf '\\033[2J\\033[H\\033[3J'; printf 'plain %s\\n' {1..40}; ",
        "printf '\\033[30;1H> composer'; ",
        "for k in $(seq 1 40); do ",
        "printf '\\033[1;29r\\033[29;1H\\r\\ninsert %d\\033[r\\033[30;1H> composer\\033[K' $k; done"
    );
    let text =
        server.ok(json!({"action": "start", "command": command, "pty": true, "waitMs": 30000}));
    assert!(text.contains("[exited 0]"), "{text}");
    let text =
        server.ok(json!({"action": "transcript", "taskId": server.id(1), "maxBytes": 4194304}));
    let body = text.split("\n\n[").next().unwrap();
    let lines: Vec<&str> = body.lines().collect();
    let expected: Vec<String> = (1..=45)
        .map(|k| format!("kept {k}"))
        .chain((1..=40).map(|k| format!("plain {k}")))
        .chain((1..=40).map(|k| format!("insert {k}")))
        .chain(["> composer".to_string()])
        .collect();
    let positions: Vec<usize> = expected
        .iter()
        .map(|line| {
            let found: Vec<usize> = (0..lines.len()).filter(|&i| lines[i] == line).collect();
            assert_eq!(found.len(), 1, "{line:?} must appear once: {text}");
            found[0]
        })
        .collect();
    assert!(positions.is_sorted(), "lines out of order: {text}");
    let transcript_path = status_line(&text)
        .rsplit_once(" · ")
        .map(|(_, path)| path)
        .unwrap_or_else(|| panic!("no transcript path: {text}"));
    let file = std::fs::read_to_string(transcript_path).unwrap();
    assert_eq!(file.trim_end(), body.trim_end());
    // Output ended, so every line is final, and a second read has none left.
    assert!(
        text.contains(&format!(
            "transcript {} lines, final through {}",
            lines.len(),
            lines.len()
        )),
        "{text}"
    );
    let text = server.ok(json!({"action": "transcript", "taskId": server.id(1)}));
    assert!(
        text.starts_with("(no transcript lines since the last read)"),
        "{text}"
    );
    assert!(
        text.contains(&format!("this read starts at line {}", lines.len() + 1)),
        "{text}"
    );

    // A program on the alternate screen: the normal screen's history, then the session merged
    // from its frames. Each page is held back by a gate file the test creates only after the
    // screen shows the page before it, so a page is replaced only once it was drawn: a frame the
    // recorder missed fails here rather than being papered over by a longer sleep. Each page is
    // a synchronized update, one frame however its writes arrive: the MSYS runtime writes a page
    // a line or escape sequence at a time, and a loaded host pausing between them would show
    // half-drawn pages. Drawn as lines, a page ends well inside the update's timeout.
    let gate = std::env::temp_dir().join(format!("fastexec-pty-gate-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&gate);
    std::fs::create_dir_all(&gate).unwrap();
    let first_gate = gate.join("second");
    let second_gate = gate.join("scrolled");
    let command = format!(
        concat!(
            "printf 'normal %s\\n' {{1..40}}; printf 'a\\tb\\n\\033[?1049h'; ",
            "printf '\\033[?2026h\\033[H%s\\033[?2026l' \"$(printf 'first %s\\n' {{1..30}})\"; ",
            "while [ ! -f '{}' ]; do sleep 0.01; done; ",
            "printf '\\033[?2026h\\033[H%s\\033[?2026l' \"$(printf 'second %s\\n' {{1..30}})\"; ",
            "while [ ! -f '{}' ]; do sleep 0.01; done; ",
            "printf '\\r\\nscrolled %d' $(seq 1 40); read -r x"
        ),
        first_gate.display(),
        second_gate.display()
    );
    // The first page is on screen before anything replaces it.
    let text = server.ok(json!({
        "action": "start", "command": command, "pty": true, "waitMs": 60000,
        "returnWhen": {"screenContains": ["first 30"]}
    }));
    assert!(
        text.contains("wait: screen matched \"first 30\""),
        "the program drew its first page: {text}"
    );
    // Then the second page, and it is on screen before the next one is drawn.
    std::fs::write(&first_gate, b"").unwrap();
    let text = server.ok(json!({
        "action": "poll", "taskId": server.id(2), "waitMs": 60000,
        "returnWhen": {"screenContains": ["second 30"]}
    }));
    assert!(
        text.contains("wait: screen matched \"second 30\""),
        "the program drew its second page: {text}"
    );
    // Wait for the last line to be on screen before allowing a quiet interval to end the wait:
    // a quiet window that elapses early must not stand in for the drawing being done. A quiet
    // read after it then settles the frames.
    std::fs::write(&second_gate, b"").unwrap();
    let text = server.ok(json!({
        "action": "poll", "taskId": server.id(2), "waitMs": 60000,
        "returnWhen": {"screenContains": ["scrolled 40"]}
    }));
    assert!(
        text.contains("wait: screen matched \"scrolled 40\""),
        "the program drew its last line: {text}"
    );
    server.ok(json!({
        "action": "poll", "taskId": server.id(2), "waitMs": 15000,
        "returnWhen": {"quietMs": 3000}
    }));
    let text =
        server.ok(json!({"action": "transcript", "taskId": server.id(2), "maxBytes": 4194304}));
    let lines: Vec<&str> = text.lines().collect();
    let normal = lines.iter().position(|line| *line == "normal 1");
    let marker = lines
        .iter()
        .position(|line| *line == "--- alternate screen ---");
    assert!(normal.is_some_and(|normal| Some(normal) < marker), "{text}");
    let session: Vec<&str> = lines[marker.unwrap() + 1..]
        .iter()
        .copied()
        .take_while(|line| !line.is_empty())
        .collect();
    let expected: Vec<String> = (1..=30)
        .map(|k| format!("first {k}"))
        .chain((1..=30).map(|k| format!("second {k}")))
        .chain((1..=40).map(|k| format!("scrolled {k}")))
        .collect();
    assert_eq!(session, expected, "{text}");
    assert!(
        lines.contains(&"a       b"),
        "a tab reads as spaces to its stop: {text}"
    );
    // The screen's rows are pending; the next read starts after the final lines and shows
    // the pending rows again, with the line typed since.
    let status = status_line(&text);
    let total_lines: u64 = status
        .split_once("transcript ")
        .and_then(|(_, rest)| rest.split(' ').next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("missing transcript line count: {text}"));
    let final_lines: u64 = status
        .split_once("final through ")
        .and_then(|(_, rest)| rest.split(';').next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("missing finalLines: {text}"));
    let first = final_lines + 1;
    assert!(total_lines >= first, "{text}");
    server.ok(json!({"action": "poll", "taskId": server.id(2), "input": "typed", "waitMs": 30000, "returnWhen": {"screenContains": ["40typed"]}}));
    let text =
        server.ok(json!({"action": "transcript", "taskId": server.id(2), "maxBytes": 4194304}));
    assert!(
        text.contains(&format!("this read starts at line {first}")),
        "{text}"
    );
    assert!(!text.contains("normal 1\n"), "{text}");
    assert!(text.contains("scrolled 40typed"), "{text}");
    server.ok(json!({"action": "kill", "taskId": server.id(2)}));
    let _ = std::fs::remove_dir_all(&gate);

    server.ok(json!({"action": "start", "command": "echo pipe"}));
    let (text, is_error) = server.call(json!({"action": "transcript", "taskId": server.id(3)}));
    assert!(is_error && text.contains("PTY tasks only"), "{text}");
}

/// Polls a task's screen until it and the cursor are exactly `rows` and `cursor`, with a bounded
/// wait: the state the test means, not merely text that appears on the way there.
fn wait_for_screen(server: &mut Server, task: &str, rows: &str, cursor: &str) -> String {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let text = server.ok(json!({
            "action": "poll", "taskId": task, "screen": true, "waitMs": 100
        }));
        if text.split("\n\n[").next().unwrap_or_default() == rows && text.contains(cursor) {
            return text;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the screen never reached {rows:?} at {cursor}: {text}"
        );
    }
}

/// The transcript of a PTY task, read in windows until a call adds nothing: a task that ended
/// hands out its last window, then an empty one.
fn transcript_body(server: &mut Server, id: &str) -> String {
    let mut body = String::new();
    for _ in 0..20 {
        let text = server.ok(json!({
            "action": "transcript", "taskId": id, "maxBytes": 4194304
        }));
        let part = text
            .split(
                "

[",
            )
            .next()
            .unwrap_or_default();
        // The window's own note for an empty one is not transcript text.
        let part = match part.trim() {
            "(no transcript lines since the last read)" => "",
            other if other.starts_with("... [") && other.ends_with("] ...") => "",
            _ => part,
        };
        let status = status_line(&text);
        let ended = status.starts_with("[exited ") || status.starts_with("[killed]");
        if part.is_empty() {
            if ended {
                break;
            }
            continue;
        }
        body.push_str(part);
        if !body.ends_with('\n') {
            body.push('\n');
        }
    }
    body
}

/// The size a PTY task starts at, as the program itself sees it: `stty size` prints rows then
/// columns.
#[test]
fn a_pty_task_starts_at_the_default_size_and_the_program_sees_it() {
    let mut server = Server::start();
    let text = server.ok(json!({
        "action": "start", "pty": true, "waitMs": 2000,
        "command": "stty size; sleep 30"
    }));
    assert!(
        text.contains("30 120"),
        "the program's terminal is 120x30: {text}"
    );
    let task = server.id(1);
    let text = server.ok(json!({"action": "list"}));
    assert!(text.contains(" pty 120x30"), "{text}");
    // The screen is read as it stands: the command sleeps, so a poll that waited would wait out
    // its own timeout for output that cannot arrive.
    let text = server.ok(json!(
        {"action": "poll", "taskId": task, "screen": true, "waitMs": 0}
    ));
    assert!(
        text.contains("screen 120x30, cursor row 2 col 1"),
        "the screen names its size: {text}"
    );
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
}

/// A resize in the same poll as a zero-delay key applies a synchronized update's held modes
/// before encoding the burst, including application-cursor mode.
#[test]
fn a_zero_delay_burst_carries_the_declared_keyboard_mode() {
    let (ready, ready_bash) = scratch("keyboard-mode-ready");
    let (received, received_bash) = scratch("keyboard-mode-received");
    let (done, done_bash) = scratch("keyboard-mode-done");
    let (stderr, stderr_bash) = scratch("keyboard-mode-stderr");
    let mut server = Server::start();
    let text = server.ok(json!({
        "action": "start", "pty": true, "loginShell": false,
        "cols": 100, "rows": 40, "waitMs": 0,
        "command": format!("set -e; stty raw -echo; printf '\\033[?2026h\\033[?1h'; : > '{ready_bash}'; dd bs=1 count=3 of='{received_bash}' 2> '{stderr_bash}'; : > '{done_bash}'")
    }));
    assert!(!text.contains("Cannot"), "{text}");
    let task = server.id(1);
    let log = PathBuf::from(log_path_of(&text).unwrap());
    // ConPTY can query CPR during startup. The ready file follows stty; the exact
    // declarations in the raw log prove capture has already fed the emulator.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let read = std::fs::read_to_string(&log).unwrap_or_default();
        if ready.exists() && read.contains("\x1b[?2026h\x1b[?1h") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the program's keyboard modes were not captured (ready={}): {read:?}",
            ready.exists()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let poll_text = server.ok(json!({
        "action": "poll", "taskId": task, "cols": 80, "rows": 20,
        "keys": ["Up"], "keyDelayMs": 0, "waitMs": 1000
    }));
    // Keep the byte assertion independent of Bash's resize-sensitive read builtin.
    // The shell publishes done only after dd closes the file.
    let deadline = Instant::now() + Duration::from_secs(30);
    while !done.exists() {
        if Instant::now() >= deadline {
            let (state_text, state_error) = server.call(json!({
                "action": "poll", "taskId": task, "waitMs": 0, "raw": true
            }));
            panic!(
                "the program never finished reading the key: raw={:?}; received={:?}; reader stderr={:?}; start={text:?}; poll={poll_text:?}; failure state={state_text:?} isError={state_error}",
                std::fs::read_to_string(&log).unwrap_or_default(),
                std::fs::read(&*received).ok(),
                std::fs::read_to_string(&*stderr).ok()
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let bytes = std::fs::read(&*received).unwrap();
    assert_eq!(
        bytes, b"\x1bOA",
        "the burst carries application-cursor mode: {bytes:?}"
    );
    assert_ne!(
        bytes, b"\x1b[A",
        "the burst used the modes the validation pass saw: {bytes:?}"
    );
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
}

#[test]
fn a_poll_resizes_the_terminal_before_its_input() {
    let mut server = Server::start();
    let text = server.ok(json!({
        "action": "start", "pty": true, "cols": 100, "rows": 40, "waitMs": 5000,
        "command": "stty size; while read -r line; do stty size; done"
    }));
    // The size `start` was given, read by the program itself before any poll runs.
    assert!(
        text.contains("40 100"),
        "the program's terminal is 100x40: {text}"
    );
    let task = server.id(1);
    let text = server.ok(json!({
        "action": "poll", "taskId": task, "cols": 60, "rows": 20, "input": "x
    ", "waitMs": 500
    }));
    assert!(
        text.contains("20 60"),
        "the program read the new size, after the resize: {text}"
    );
    // One axis alone keeps the other.
    let text = server.ok(json!({
        "action": "poll", "taskId": task, "rows": 25, "input": "y
    ", "waitMs": 500
    }));
    assert!(
        text.contains("25 60"),
        "rows changed and cols stayed: {text}"
    );
    let text = server.ok(json!({
        "action": "poll", "taskId": task, "screen": true, "waitMs": 100
    }));
    assert!(text.contains("screen 60x25"), "{text}");
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
}

/// The program is told the terminal changed size: on Unix it gets SIGWINCH and reads the new size
/// in its handler. The program says `ready` after the trap is armed, and that is asserted before
/// the one resize is sent: a resize to the size the terminal already has is a no-op, so a
/// notification lost before the trap existed could never be recovered by sending it again.
#[cfg(not(target_os = "windows"))]
#[test]
fn a_resize_sends_sigwinch_and_the_program_reads_the_new_size() {
    let mut server = Server::start();
    let started = server.ok(json!({
        "action": "start", "pty": true, "waitMs": 5000,
        "command": "trap 'printf \"winch %s\n\" \"$(stty size)\"' WINCH; printf 'ready\n'; while :; do sleep 0.02; done"
    }));
    assert!(
        started.contains("ready"),
        "the program armed its trap before the resize: {started}"
    );
    let task = server.id(1);
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut text = String::new();
    while !text.contains("winch 22 70") {
        let polled = server.ok(json!({
            "action": "poll", "taskId": task, "cols": 70, "rows": 22, "waitMs": 2000,
            "returnWhen": {"outputContains": ["winch 22 70"]}
        }));
        text.push_str(&polled);
        assert!(
            Instant::now() < deadline,
            "the program saw the new size on SIGWINCH: {text}"
        );
    }
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
}

/// On Windows the promise is the native console resize, which the program reads as its own size.
/// SIGWINCH delivery to bash is the emulation layer's business and is not asserted: the reader runs
/// `stty size` after the input that wakes it, and that answer must be the new size.
#[cfg(target_os = "windows")]
#[test]
fn a_resize_is_seen_by_the_program_as_its_new_size() {
    let mut server = Server::start();
    let started = server.ok(json!({
        "action": "start", "pty": true, "waitMs": 5000,
        "command": "printf 'ready\n'; while IFS= read -r line; do printf 'native %s\n' \"$(stty size)\"; done"
    }));
    assert!(started.contains("ready"), "the reader is ready: {started}");
    let task = server.id(1);
    // One resize, and nothing that resizes again.
    server.ok(json!({
        "action": "poll", "taskId": task, "cols": 70, "rows": 22, "waitMs": 2000
    }));
    let text = server.ok(json!({
        "action": "poll", "taskId": task, "input": "measure
    ", "waitMs": 5000,
        "returnWhen": {"outputContains": ["native 22 70"]}
    }));
    assert!(
        text.contains("native 22 70"),
        "the program reads the new size as its own: {text}"
    );
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
}

/// Bounds are checked before anything happens, and a pipe task has no size to change.
#[test]
fn sizes_out_of_range_or_on_a_pipe_task_are_refused() {
    let mut server = Server::start();
    let (text, is_error) = server.call(json!({
        "action": "start", "pty": true, "cols": 19, "command": "printf hi"
    }));
    assert!(
        is_error && text.contains("cols 19 is out of range 20-500"),
        "{text}"
    );
    let (text, is_error) = server.call(json!({
        "action": "start", "pty": true, "rows": 201, "command": "printf hi"
    }));
    assert!(
        is_error && text.contains("rows 201 is out of range 5-200"),
        "{text}"
    );
    let (text, is_error) = server.call(json!({
        "action": "start", "pty": false, "cols": 100, "command": "printf hi"
    }));
    assert!(
        is_error && text.contains("`cols` applies to PTY tasks only"),
        "{text}"
    );
    server.ok(json!({"action": "start", "pty": false, "waitMs": 5000, "command": "printf hi"}));
    let pipe = server.id(1);
    let (text, is_error) = server.call(json!({"action": "poll", "taskId": pipe, "rows": 10}));
    assert!(
        is_error && text.contains("applies to PTY tasks only"),
        "{text}"
    );
}

/// A poll that fails validation changes nothing: no resize, and no input delivered.
#[test]
fn an_invalid_combined_poll_neither_resizes_nor_delivers_input() {
    let mut server = Server::start();
    server.ok(json!({
        "action": "start", "pty": true, "waitMs": 5000,
        "command": "stty size; while read -r line; do printf 'got %s\\n' \"$line\"; done"
    }));
    let task = server.id(1);
    let (text, is_error) = server.call(json!({
        "action": "poll", "taskId": task, "cols": 100, "rows": 40, "input": "hello\n",
        "keys": ["NotAKey"], "waitMs": 200
    }));
    assert!(
        is_error && text.contains("NotAKey"),
        "the key name is refused: {text}"
    );
    let text = server.ok(json!({
        "action": "poll", "taskId": task, "input": "second\n", "waitMs": 500
    }));
    assert!(
        !text.contains("got hello"),
        "the refused poll's input was not delivered: {text}"
    );
    assert!(
        text.contains("got second"),
        "the next poll's input was: {text}"
    );
    let text =
        server.ok(json!({"action": "poll", "taskId": task, "screen": true, "waitMs": 200}));
    assert!(
        text.contains("screen 120x30"),
        "the refused poll's size was not applied: {text}"
    );
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
}

/// A poll that names the size the task already has changes nothing: no SIGWINCH, no repaint.
#[test]
fn an_unchanged_size_sends_nothing() {
    let mut server = Server::start();
    server.ok(json!({
        "action": "start", "pty": true, "waitMs": 5000,
        "command": "n=0; trap 'n=$((n+1)); printf \"winch %s\\n\" \"$n\"' WINCH; printf 'ready\\n'; while :; do sleep 0.02; done"
    }));
    let task = server.id(1);
    // A loaded machine runs the trap late, so the size keeps changing until the program answers:
    // each change is another SIGWINCH, and the first answer is the one the test wants.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut text = String::new();
    let (mut cols, mut sent) = (121u16, 0u16);
    while !text.contains("winch 1") {
        sent = cols;
        let polled = server.ok(json!({
            "action": "poll", "taskId": task, "cols": cols, "waitMs": 2000,
            "returnWhen": {"outputContains": ["winch 1"]}
        }));
        text.push_str(&polled);
        cols = if cols == 121 { 122 } else { 121 };
        assert!(Instant::now() < deadline, "a changed size does: {text}");
    }
    // The size it now has, named again: the program has already answered one change, so a second
    // answer could only come from a resize that should not have been sent.
    let unchanged = server.ok(json!({
        "action": "poll", "taskId": task, "cols": sent, "waitMs": 1500
    }));
    assert!(
        !unchanged.contains("winch 2"),
        "an unchanged size sends no SIGWINCH: {unchanged}"
    );
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 5000}));
}

/// A resize after the task's tree ended is refused before anything changes.
#[test]
fn a_resize_after_exit_is_refused() {
    let mut server = Server::start();
    server.ok(json!({"action": "start", "pty": true, "waitMs": 30000, "command": "printf hi"}));
    let task = server.id(1);
    let (text, is_error) = server.call(json!({"action": "poll", "taskId": task, "cols": 100}));
    assert!(
        is_error && (text.contains("terminal is closed") || text.contains("tree has ended")),
        "the resize is refused: {text}"
    );
    // The task's own record is unchanged, and it still renders its screen.
    let text =
        server.ok(json!({"action": "poll", "taskId": task, "screen": true, "waitMs": 100}));
    assert!(text.contains("screen 120x30"), "{text}");
}

/// Sustained output through fifty resizes: the settled contract, on the product's own path. The
/// producer prints a fixed payload of 20 000 lines, is still printing for every resize, and its
/// transcript then holds every one of those lines, whole and in order, in over a megabyte.
#[test]
#[ignore = "heavy: opt in with --ignored"]
fn sustained_output_through_resizes_keeps_every_line() {
    const LINES: usize = 20_000;
    let started = std::time::Instant::now();
    let deadline = started + std::time::Duration::from_secs(180);
    let mut server = Server::start();
    let temp = std::env::temp_dir();
    let tag = std::process::id();
    let done = temp.join(format!("fastexec-b3-done-{tag}"));
    let _ = std::fs::remove_file(&done);
    // Every fifty lines the producer waits, so its output spans the whole resize loop instead of
    // landing in one burst, and the payload is fixed whether or not the loop keeps up.
    let command = format!(
        "i=0; while [ $i -lt {LINES} ]; do i=$((i+1)); \
         printf 'line %05d %s\\n' $i '------------------------------------------------------------'; \
         if [ $((i % 50)) -eq 0 ]; then sleep 0.02; fi; done; : > '{done}'; sleep 60",
        done = done.display().to_string().replace('\\', "/")
    );
    server.ok(json!({
        "action": "start", "pty": true, "cols": 68, "rows": 30, "waitMs": 500, "command": command
    }));
    let task = server.id(1);
    let sizes = [(100u16, 30u16), (40, 12), (20, 5), (500, 200), (68, 30)];
    for k in 0..50 {
        assert!(
            !done.exists(),
            "the producer finished before resize {k} of 50"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "the resizes passed their bound at {k}"
        );
        let (cols, rows) = sizes[k % sizes.len()];
        let text = server.ok(json!({
            "action": "poll", "taskId": task, "cols": cols, "rows": rows, "waitMs": 50
        }));
        assert!(!text.contains("Cannot resize"), "a resize failed: {text}");
    }
    assert!(!done.exists(), "the producer printed through every resize");
    // The payload's own end, within the run's bound: the producer's 20 000 lines take as long as
    // they take on this platform.
    while !done.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "the producer never finished its payload"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    server.ok(json!({"action": "kill", "taskId": task, "waitMs": 10000}));
    let body = transcript_body(&mut server, &task);
    let written: Vec<&str> = body.lines().collect();
    let payload: Vec<String> = (1..=LINES)
        .map(|k| format!("line {k:05} {}", "-".repeat(60)))
        .collect();
    // The fixed payload, independently written: every line is there, whole, in order.
    let mut at = 0;
    for want in &payload {
        match written[at..].iter().position(|line| line == want) {
            Some(found) => at += found + 1,
            None => panic!(
                "payload line {want:?} is not in the transcript in order after {at} lines of {}",
                written.len()
            ),
        }
    }
    assert!(
        body.len() > 1_048_576,
        "the transcript holds over a megabyte of sustained output: {} bytes",
        body.len()
    );
    // Every other line is a payload line or a row visible at a resize, which a width change can
    // cut: it is a part of a payload line. A blank extra line counts, and fails this check.
    for line in &written {
        assert!(
            !line.is_empty() && payload.iter().any(|want| want.contains(line)),
            "a transcript line is neither a payload line nor part of one: {line:?}"
        );
    }
    let extras = written.len().saturating_sub(payload.len());
    assert!(
        extras <= 50 * 200,
        "{extras} extra lines, more than fifty resizes of the largest screen"
    );
    assert!(
        std::time::Instant::now() < deadline,
        "the whole run took {:?}",
        started.elapsed()
    );
    let _ = std::fs::remove_file(&done);
}
