use super::state::*;
use super::workers::*;
use super::*;
use crate::lease::BytePool;
use crate::session::ReadState;
use std::io::Read;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

#[cfg(unix)]
mod private_directory_tests {
    use super::*;
    use std::os::unix::fs::{PermissionsExt, symlink};

    #[cfg(target_os = "linux")]
    #[test]
    fn stale_sweep_refuses_untrusted_entries_and_unsafe_parent() {
        let root = std::env::temp_dir().join(format!(
            "fastexec-sweep-test-{}-{}",
            std::process::id(),
            random_chars(12).unwrap()
        ));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let prefix = dir_prefix();
        let valid = root.join(format!("{prefix}2147483647-0123456789ab"));
        let shared = root.join(format!("{prefix}2147483646-0123456789ab"));
        let target = root.join("target");
        let link = root.join(format!("{prefix}2147483645-0123456789ab"));
        let foreign = root.join(format!("{prefix}2147483644-0123456789ab"));
        let live = root.join(format!("{prefix}{}-0123456789ab", std::process::id()));
        let old_format = root.join(format!("{prefix}2147483643"));
        for dir in [&valid, &shared, &target, &foreign, &live, &old_format] {
            std::fs::create_dir(dir).unwrap();
        }
        std::fs::set_permissions(&valid, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o755)).unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(&foreign, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(&live, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::set_permissions(&old_format, std::fs::Permissions::from_mode(0o700)).unwrap();
        // Exercise another owner's otherwise private directory when run as root.
        if unsafe { libc::geteuid() } == 0 {
            use std::os::unix::ffi::OsStrExt;
            let name = std::ffi::CString::new(foreign.as_os_str().as_bytes()).unwrap();
            assert_eq!(unsafe { libc::chown(name.as_ptr(), 65534, 65534) }, 0);
        }
        symlink(&target, &link).unwrap();
        let sentinel = shared.join("sentinel");
        std::fs::write(&sentinel, "keep").unwrap();
        // An untrusted non-sticky shared parent is never swept, even for a valid leaf.
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o777)).unwrap();
        remove_stale_dirs(&root);
        assert!(valid.exists());
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        remove_stale_dirs(&root);
        assert!(!valid.exists(), "private stale directory should be retired");
        assert!(sentinel.exists(), "shared directory must not be removed");
        assert!(
            std::fs::symlink_metadata(&link).is_ok(),
            "symlink must not be removed"
        );
        assert!(target.exists());
        assert!(live.exists(), "live process logs must not be removed");
        assert!(
            old_format.exists(),
            "names without a nonce must be retained"
        );
        if unsafe { libc::geteuid() } == 0 {
            assert!(
                foreign.exists(),
                "another user's private directory must not be removed"
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn stale_sweep_checks_ancestors_sticky_owner_and_new_names() {
        let root = std::env::temp_dir().join(format!(
            "fastexec-sweep-coverage-{}-{}",
            std::process::id(),
            random_chars(12).unwrap()
        ));
        std::fs::create_dir(&root).unwrap();
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
        let prefix = dir_prefix();
        let unsafe_ancestor = root.join("unsafe-ancestor");
        let inner = unsafe_ancestor.join("inner");
        std::fs::create_dir_all(&inner).unwrap();
        std::fs::set_permissions(&unsafe_ancestor, std::fs::Permissions::from_mode(0o777)).unwrap();
        std::fs::set_permissions(&inner, std::fs::Permissions::from_mode(0o700)).unwrap();
        let candidate = inner.join(format!("{prefix}2147483647-0123456789ab"));
        std::fs::create_dir(&candidate).unwrap();
        std::fs::set_permissions(&candidate, std::fs::Permissions::from_mode(0o700)).unwrap();
        remove_stale_dirs(&inner);
        assert!(candidate.exists(), "unsafe ancestor must prevent sweeping");

        let sticky = root.join("sticky");
        std::fs::create_dir(&sticky).unwrap();
        std::fs::set_permissions(&sticky, std::fs::Permissions::from_mode(0o1777)).unwrap();
        let good = sticky.join(format!("{prefix}2147483647-0123456789ab"));
        let short = sticky.join(format!("{prefix}2147483646-short"));
        let invalid = sticky.join(format!("{prefix}2147483645-0123456789a!"));
        for dir in [&good, &short, &invalid] {
            std::fs::create_dir(dir).unwrap();
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        remove_stale_dirs(&sticky);
        assert!(
            !good.exists(),
            "trusted sticky parent should allow safe cleanup"
        );
        assert!(
            short.exists() && invalid.exists(),
            "invalid suffixes must be retained"
        );

        if unsafe { libc::geteuid() } == 0 {
            use std::os::unix::ffi::OsStrExt;
            let foreign_sticky = root.join("foreign-sticky");
            std::fs::create_dir(&foreign_sticky).unwrap();
            std::fs::set_permissions(&foreign_sticky, std::fs::Permissions::from_mode(0o1777))
                .unwrap();
            let candidate = foreign_sticky.join(format!("{prefix}2147483647"));
            std::fs::create_dir(&candidate).unwrap();
            std::fs::set_permissions(&candidate, std::fs::Permissions::from_mode(0o700)).unwrap();
            let name = std::ffi::CString::new(foreign_sticky.as_os_str().as_bytes()).unwrap();
            assert_eq!(unsafe { libc::chown(name.as_ptr(), 65534, 65534) }, 0);
            remove_stale_dirs(&foreign_sticky);
            assert!(
                candidate.exists(),
                "untrusted sticky owner must prevent sweeping"
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn task_log_directories_refuse_shared_directories_and_symlinks() {
        let root = std::env::temp_dir().join(format!(
            "fastexec-private-test-{}-{}",
            std::process::id(),
            random_chars(12).unwrap()
        ));
        std::fs::create_dir(&root).unwrap();
        let shared = root.join("shared");
        std::fs::create_dir(&shared).unwrap();
        std::fs::set_permissions(&shared, std::fs::Permissions::from_mode(0o755)).unwrap();
        let target = root.join("target");
        std::fs::create_dir(&target).unwrap();
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o700)).unwrap();
        let link = root.join("link");
        symlink(&target, &link).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let _entered = runtime.enter();
        let shared_refused = Tasks::new_in(Limits::OFF, shared).is_err();
        let symlink_refused = Tasks::new_in(Limits::OFF, link).is_err();
        std::fs::remove_dir_all(root).unwrap();
        assert!(
            shared_refused,
            "a directory open to other users was accepted"
        );
        assert!(
            symlink_refused,
            "a symlink was accepted as a private log directory"
        );
    }
}

mod output_reads {
    use super::*;

    struct InterruptedThenFailed(u8);

    impl Read for InterruptedThenFailed {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            self.0 += 1;
            match self.0 {
                1 => Err(std::io::ErrorKind::Interrupted.into()),
                2 => {
                    buffer[..6].copy_from_slice(b"prefix");
                    Ok(6)
                }
                3 => Err(std::io::Error::other("output read failed")),
                _ => panic!("an unexpected error must end capture"),
            }
        }
    }

    #[test]
    fn unexpected_output_read_errors_follow_the_captured_prefix() {
        for pty in [false, true] {
            let (sender, received) = mpsc::sync_channel(2);
            read_chunks(Box::new(InterruptedThenFailed(0)), sender, pty);
            let mut chunks = received.into_iter();
            assert_eq!(chunks.next().unwrap().unwrap().1, b"prefix");
            assert_eq!(
                chunks.next().unwrap().unwrap_err().to_string(),
                "output read failed"
            );
            assert!(chunks.next().is_none());
        }
    }

    #[cfg(unix)]
    #[test]
    fn only_a_pty_treats_eio_as_end_of_output() {
        struct Eio;
        impl Read for Eio {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::from_raw_os_error(libc::EIO))
            }
        }
        for pty in [false, true] {
            let (sender, received) = mpsc::sync_channel(1);
            read_chunks(Box::new(Eio), sender, pty);
            let chunks: Vec<_> = received.into_iter().collect();
            if pty {
                assert!(chunks.is_empty());
            } else {
                assert_eq!(chunks.len(), 1);
                assert_eq!(
                    chunks[0].as_ref().unwrap_err().raw_os_error(),
                    Some(libc::EIO)
                );
            }
        }
    }
}

// Pure ID logic, so it runs on every platform; the tests below launch commands.
mod ids {
    use super::*;

    #[test]
    fn epoch_is_8_crockford_characters_and_differs_between_servers() {
        let epoch = new_epoch().unwrap();
        assert_eq!(epoch.len(), 8, "{epoch}");
        assert!(epoch.bytes().all(|b| ALPHABET.contains(&b)), "{epoch}");
        assert_ne!(epoch, new_epoch().unwrap());
        assert!(is_valid_task_id(&task_id(&epoch, 1)));
        assert!(is_valid_task_id(&task_id(&epoch, u64::MAX)));
        for malformed in [
            "",
            "t1",
            "foo-bar",
            "7yfh1xz-1",
            "7yfh1xz30-1",
            "7yfh1xz3-",
            "7yfh1xz3-1.abcd",
        ] {
            assert!(!is_valid_task_id(malformed), "{malformed}");
        }
    }

    #[test]
    fn uncertain_membership_never_kills_a_kept_tree_or_shortens_a_successful_cleanup() {
        for listed in [Some(Vec::new()), None] {
            let unknown = listed.is_none();
            let (leftovers, kept, error) = root_leftovers(true, listed, || panic!("keepTree"));
            assert!(leftovers.is_empty());
            assert_eq!(kept, unknown);
            assert!(error.is_none());
        }
        let (_, kept, error) = root_leftovers(false, None, || Ok(()));
        assert!(!kept);
        assert!(error.is_none());
    }

    #[test]
    fn base32_is_minimal_and_most_significant_first() {
        assert_eq!(base32(1, 1), "1");
        assert_eq!(base32(31, 1), "z");
        assert_eq!(base32(32, 1), "10");
        assert_eq!(base32(0, 1), "0");
        // A zero source still fills the whole epoch width.
        assert_eq!(base32(0, 26), "0".repeat(26));
    }

    #[test]
    fn short_random_keys_use_the_full_alphabet_at_every_position() {
        let mut high = [false; 4];
        for _ in 0..128 {
            let key = random_chars(4).unwrap();
            assert_eq!(key.len(), 4);
            for (i, byte) in key.bytes().enumerate() {
                assert!(ALPHABET.contains(&byte));
                high[i] |= ALPHABET[8..].contains(&byte);
            }
        }
        assert!(high.into_iter().all(|seen| seen));
    }

    #[test]
    fn task_ids_carry_the_epoch_and_never_repeat() {
        let epoch = new_epoch().unwrap();
        let first = task_id(&epoch, 1);
        let second = task_id(&epoch, 2);
        assert_eq!(first, format!("{epoch}-1"));
        assert_eq!(second, format!("{epoch}-2"));
        assert_ne!(first, second);
        assert_ne!(task_id(&epoch, 1), task_id(&new_epoch().unwrap(), 1));
    }
}

// Unix tasks run under the fastexec binary as their host, which a test binary is not.
#[cfg(windows)]
mod tests {
    use super::*;
    use crate::service::render::{OutputMode, Render};
    use crate::service::schema::Request;
    use crate::service::{Call, Service};
    use crate::term::output::{Cleaner, Matcher, Truncate};

    /// A `Tasks` for a test: it needs a runtime for its `killAfterMs` timer, which these tests
    /// do not arm.
    fn tasks() -> (tokio::runtime::Runtime, Arc<Tasks>) {
        tasks_with(Limits::OFF)
    }

    fn tasks_with(limits: Limits) -> (tokio::runtime::Runtime, Arc<Tasks>) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let tasks = {
            let _entered = runtime.enter();
            Tasks::new(limits).unwrap()
        };
        (runtime, tasks)
    }

    /// This process's environment, as a session's launch context.
    fn launch() -> Arc<LaunchContext> {
        Arc::new(LaunchContext::new(
            std::env::temp_dir(),
            std::env::vars_os().collect(),
        ))
    }

    fn session(label: &str) -> Arc<Session> {
        Session::new(label.into(), label.into(), launch())
    }

    fn start(tasks: &Arc<Tasks>, args: StartArgs) -> Result<Arc<Task>, String> {
        tasks
            .start(args, &session("test-client"), random_chars(4).unwrap())
            .map(|((task, _, _), _)| task)
    }

    fn start_args() -> StartArgs {
        StartArgs {
            command: "printf abc".into(),
            cwd: std::env::temp_dir(),
            pty: false,
            cols: process::PTY_COLS,
            rows: process::PTY_ROWS,
            login: false,
            keep: false,
            input: None,
            eof: None,
            encoding: encoding_rs::UTF_8,
            kill_after_ms: None,
            owner: "test-client".into(),
            started_by: None,
            launch: launch(),
            input_owner: Default::default(),
        }
    }

    #[test]
    fn a_chunk_whose_reservation_evicts_its_own_log_gives_it_back() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (_runtime, tasks) = tasks();
        let task = start(&tasks, start_args()).unwrap();
        // Its own capture is done too, so nothing else touches the accounting below.
        let deadline = Instant::now() + Duration::from_secs(30);
        while task.is_running() || !task.output_closed() {
            assert!(Instant::now() < deadline, "the task never finished");
            std::thread::sleep(Duration::from_millis(10));
        }
        // Reserving the 99-byte chunk crosses the limit and evicts the finished task's 3 bytes.
        let before = tasks.log_limit - 97;
        tasks.log_bytes.store(before, Ordering::SeqCst);
        let log = File::create(tasks.dir.join("extra.log")).unwrap();
        let (chunks, received) = mpsc::sync_channel(1);
        chunks.send(Ok((Instant::now(), vec![b'x'; 99]))).unwrap();
        drop(chunks);
        capture(&tasks, &task, received, log, None);
        let out = lock(&task.out);
        assert!(out.evicted);
        assert_eq!((out.written, out.dropped), (3, 99));
        assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), before - 3);
        drop(out);
        tasks.shutdown();
    }

    #[test]
    fn a_list_call_drops_the_entries_of_tasks_that_have_retired() {
        let _one_at_a_time = lock(&ONE_TASKS);
        // One finished task is kept, so the earlier ones retire as the loop goes on.
        let (runtime, tasks) = tasks_with(Limits {
            max_running: 16,
            max_scripts: 4,
            max_finished: 1,
            finished_ttl: None,
        });
        let session = session("prune");
        let mut started = Vec::new();
        for _ in 0..3 {
            let ((task, _, _), _) = tasks
                .start(start_args(), &session, random_chars(4).unwrap())
                .unwrap();
            finish(&task);
            started.push(task);
        }
        let retired = &started[0];
        assert!(
            tasks.find(&retired.id).is_none(),
            "the registry kept the retired task"
        );
        assert!(
            session.existing(retired).is_some(),
            "the entry was gone before the call"
        );
        // A list call is all it takes; it never reaches the background footer.
        let service = Service::new(Arc::clone(&tasks), false);
        let request: Request =
            serde_json::from_value(serde_json::json!({"action": "list"})).unwrap();
        let call = Call {
            session: &session,
            launch: None,
            render: Render {
                output_mode: OutputMode::Text,
                structured_content: true,
            },
            cancel: tokio_util::sync::CancellationToken::new(),
            progress: None,
            call_id: None,
            nested: false,
            http_response: false,
            rpc_id_bytes: 0,
            started: None,
            render_slot: None,
            input_owner: Default::default(),
        };
        runtime.block_on(service.call_tracked(request, call));
        assert!(
            session.existing(retired).is_none(),
            "a retired task's entry survived a list call"
        );
        tasks.shutdown();
    }

    #[test]
    fn expire_finished_drops_a_task_past_its_ttl_and_keeps_a_running_one() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (_runtime, tasks) = tasks_with(Limits::ON);
        let finished = start(&tasks, start_args()).unwrap();
        finish(&finished);
        let running = start(
            &tasks,
            StartArgs {
                command: "sleep 30".into(),
                ..start_args()
            },
        )
        .unwrap();
        let ttl = Limits::ON.finished_ttl.unwrap();
        // Nothing is old enough yet, so nothing goes.
        tasks.expire_finished(Instant::now());
        assert!(tasks.find(&finished.id).is_some(), "a fresh finish expired");
        // A day later the finished task goes, its log with it, and the running one stays.
        tasks.expire_finished(Instant::now() + ttl);
        assert!(
            tasks.find(&finished.id).is_none(),
            "the expired task stays in the registry"
        );
        assert!(
            !tasks.dir.join(format!("{}.log", finished.id)).exists(),
            "the expired task's log stays"
        );
        assert!(
            tasks.find(&running.id).is_some(),
            "a running task expired with the finished ones"
        );
        tasks.shutdown();
    }

    #[test]
    fn idle_and_pending_report_an_empty_registry() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (_runtime, tasks) = tasks_with(Limits::OFF);
        assert!(tasks.idle(), "a fresh registry is not idle");
        assert_eq!(tasks.pending(), 0);
        // A start in flight is not idle, even before its task is inserted.
        lock(&tasks.registry).pending = 1;
        assert_eq!(tasks.pending(), 1);
        assert!(!tasks.idle(), "a start in flight counts as idle");
        lock(&tasks.registry).pending = 0;
        tasks.shutdown();
    }

    #[test]
    fn the_running_limit_is_the_configured_one_and_the_refusal_names_it() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (_runtime, tasks) = tasks_with(Limits {
            max_running: 2,
            ..Limits::OFF
        });
        let long = || StartArgs {
            command: "sleep 30".into(),
            ..start_args()
        };
        let first = start(&tasks, long()).unwrap();
        let second = start(&tasks, long()).unwrap();
        let Err(refused) = start(&tasks, start_args()) else {
            panic!("a third task was admitted under a limit of 2");
        };
        assert!(refused.starts_with("TOO_MANY_TASKS"), "{refused}");
        assert!(refused.contains('2'), "{refused}");
        // The limit is the configured one, not a constant.
        assert_eq!(
            tasks.all().iter().filter(|task| task.is_running()).count(),
            2
        );
        drop((first, second));
        tasks.shutdown();
    }

    #[test]
    fn admission_counts_pending_starts_and_closes_while_stopping() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (_runtime, tasks) = tasks();
        // A start in flight counts against the limit as if its task were already running.
        lock(&tasks.registry).pending = Limits::OFF.max_running;
        let Err(refused) = start(&tasks, start_args()) else {
            panic!(
                "a start was admitted with {} starts in flight",
                Limits::OFF.max_running
            );
        };
        assert!(refused.starts_with("TOO_MANY_TASKS"), "{refused}");
        lock(&tasks.registry).pending = 0;
        // A stopping server admits nothing.
        lock(&tasks.registry).stopping = true;
        let Err(refused) = start(&tasks, start_args()) else {
            panic!("a start was admitted while the server was stopping");
        };
        assert!(refused.contains("shutting down"), "{refused}");
        lock(&tasks.registry).stopping = false;
        // A start that fails after admission gives its place back, so the limit is not eaten.
        let failed = StartArgs {
            cwd: std::env::temp_dir().join("fastexec-no-such-directory"),
            ..start_args()
        };
        assert!(
            start(&tasks, failed).is_err(),
            "a start with a missing cwd ran"
        );
        assert_eq!(lock(&tasks.registry).pending, 0);
        tasks.shutdown();
    }

    /// The text of a result, whatever its content blocks.
    fn result_text(result: &rmcp::model::CallToolResult) -> String {
        result
            .content
            .iter()
            .filter_map(|block| block.as_text().map(|text| text.text.clone()))
            .collect()
    }

    /// Waits for `task` to finish: its root ended, its output closed, and its tree done, since
    /// `ended_at` takes the later of the root's end and the tree's.
    fn finish(task: &Arc<Task>) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while task.is_running() || !task.output_closed() || !*task.tree_done().borrow() {
            assert!(Instant::now() < deadline, "the task never finished");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn eviction_takes_the_log_that_ended_earliest_not_the_one_that_started_first() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (runtime, tasks) = tasks();
        // The first task cannot finish until it gets a line: no timing guess.
        let slow = start(
            &tasks,
            StartArgs {
                command: "read -r line; printf '%s' \"$line\"".into(),
                eof: Some(false),
                ..start_args()
            },
        )
        .unwrap();
        let quick = start(&tasks, start_args()).unwrap();
        finish(&quick);
        runtime
            .block_on(slow.send(
                Some(b"done\n"),
                false,
                &CancellationToken::new(),
                Default::default(),
            ))
            .unwrap();
        finish(&slow);
        // Registry order is start order here, and the end times are the other way round.
        let (slow_end, quick_end) = (slow.ended_at().unwrap(), quick.ended_at().unwrap());
        assert!(
            quick_end < slow_end,
            "the fixture did not end in the wrong order"
        );
        // One byte over the limit: exactly one eviction is needed, and it must be the earliest end.
        tasks.log_bytes.store(tasks.log_limit - 1, Ordering::SeqCst);
        assert!(tasks.reserve(2), "the reservation failed");
        assert!(
            lock(&quick.out).evicted,
            "the log that ended earliest was not evicted first"
        );
        assert!(
            !lock(&slow.out).evicted,
            "the log that ended last was evicted first"
        );
        assert!(!tasks.dir.join(format!("{}.log", quick.id)).exists());
        assert!(tasks.dir.join(format!("{}.log", slow.id)).exists());
        tasks.shutdown();
    }

    #[test]
    fn a_read_of_an_evicted_log_is_an_outcome_not_an_error() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (_runtime, tasks) = tasks();
        let task = start(&tasks, start_args()).unwrap();
        finish(&task);
        let written = lock(&task.out).written;
        assert!(written > 0, "the task wrote nothing");
        tasks.evict_log(&task);
        let mut state = ReadState::default();
        // Every read path that opens the deleted file reports the eviction instead of an error.
        let window = task
            .read_window(
                &mut state,
                Truncate::Head,
                4096,
                false,
                true,
                false,
                encoding_rs::UTF_8,
                None,
            )
            .expect("an evicted log is not an I/O error");
        // Empty, with no invented line range: the snapshot reports the eviction.
        assert_eq!(window.text, "");
        assert_eq!(window.omitted, None);
        assert_eq!(state.cursor, written, "the cursor moved past the output");
        // A second read has nothing more to move past and stays empty.
        let again = task
            .read_window(
                &mut state,
                Truncate::Head,
                4096,
                false,
                true,
                false,
                encoding_rs::UTF_8,
                None,
            )
            .unwrap();
        assert_eq!(again.text, "");
        assert_eq!(state.cursor, written);
        // A legacy encoding takes its own path through the same read.
        assert!(
            task.read_window(
                &mut ReadState::default(),
                Truncate::Tail,
                1024,
                false,
                true,
                false,
                encoding_rs::BIG5,
                None,
            )
            .is_ok()
        );
        // So does the return_when scan.
        let needles = ["abc".to_string()];
        let mut matcher = Matcher::new(
            &needles,
            encoding_rs::UTF_8,
            Some(Cleaner::default()),
            Matcher::reserve(&needles, crate::lease::process_pool()).unwrap(),
        );
        let mut from = 0;
        assert_eq!(
            task.scan(&mut matcher, &mut from, &CancellationToken::new())
                .unwrap(),
            None
        );
        // A log that is gone while the task is not evicted stays an error.
        let live = start(&tasks, start_args()).unwrap();
        finish(&live);
        std::fs::remove_file(tasks.dir.join(format!("{}.log", live.id))).unwrap();
        let needles = ["abc".to_string()];
        let mut matcher = Matcher::new(
            &needles,
            encoding_rs::UTF_8,
            Some(Cleaner::default()),
            Matcher::reserve(&needles, crate::lease::process_pool()).unwrap(),
        );
        let mut from = 0;
        assert!(
            live.scan(&mut matcher, &mut from, &CancellationToken::new())
                .is_err(),
            "a missing log of a task that was not evicted is a real error"
        );
        tasks.shutdown();
    }

    #[test]
    fn a_session_launch_context_supplies_the_environment_and_default_cwd() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (runtime, tasks) = tasks();
        let dir = std::env::temp_dir().join(format!("fastexec-ctx-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // A variable of this process that the context leaves out: a task must not inherit it.
        let leaked = std::env::vars_os()
            .find(|(name, _)| name.to_string_lossy().starts_with("CARGO_"))
            .map(|(name, _)| name.to_string_lossy().into_owned());
        let mut env: Vec<(std::ffi::OsString, std::ffi::OsString)> = std::env::vars_os()
            .filter(|(name, _)| Some(name.to_string_lossy()) != leaked.as_deref().map(Into::into))
            .collect();
        env.push(("FASTEXEC_CTX_PROBE".into(), "xyz".into()));
        let launch = Arc::new(LaunchContext::new(dir.clone(), env));
        let probe = leaked
            .as_ref()
            .map(|name| format!("${{{name}-NONE}}"))
            .unwrap_or_default();
        let command =
            format!("printf '%s|%s|%s' \"$FASTEXEC_CTX_PROBE\" \"$(pwd -P)\" \"{probe}\"");
        let service = crate::service::Service::new(Arc::clone(&tasks), false);
        let session = Session::new("ctx-1".into(), "ctx-1".into(), Arc::clone(&launch));
        let call = || crate::service::Call {
            session: &session,
            launch: None,
            render: crate::service::render::Render {
                output_mode: crate::service::render::OutputMode::Text,
                structured_content: true,
            },
            cancel: tokio_util::sync::CancellationToken::new(),
            progress: None,
            call_id: None,
            nested: false,
            http_response: false,
            rpc_id_bytes: 0,
            started: None,
            render_slot: None,
            input_owner: Default::default(),
        };
        let name = dir.file_name().unwrap().to_string_lossy().into_owned();
        // A pipe task and a PTY task see the same environment and the same default directory.
        for pty in [false, true] {
            let result = runtime.block_on(service.call_json(
                serde_json::json!({
                    "action": "start",
                    "command": command,
                    "pty": pty,
                    "waitMs": 30000,
                }),
                call(),
            ));
            let text = result_text(&result);
            assert!(
                text.contains("xyz|"),
                "the context's environment reached the pty={pty} task: {text}"
            );
            assert!(
                text.contains(&name),
                "the default cwd came from the context in the pty={pty} task: {text}"
            );
            if leaked.is_some() {
                assert!(
                    text.contains("|NONE"),
                    "the variable the context left out is unset in the pty={pty} task: {text}"
                );
            }
        }
        tasks.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A native resize that fails leaves the terminal's state unusable while raw output capture
    /// continues, and the size the program's terminal kept is the one the task reports. A console
    /// A finished PTY task is released: no sender, thread, or registry entry holds it once it
    /// ended, was retired, and every owner dropped. A stored answer sender kept the writer
    /// thread's receiver connected, and the writer thread held the task: the weak reference
    /// never expired.
    #[test]
    fn a_finished_pty_task_is_released() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (runtime, tasks) = tasks_with(Limits {
            max_finished: 0,
            ..Limits::OFF
        });
        let mut args = start_args();
        args.command = "read -r line".into();
        args.pty = true;
        let task = start(&tasks, args).unwrap();
        let weak = Arc::downgrade(&task);
        task.kill().unwrap();
        runtime.block_on(async {
            let mut tree_done = task.tree_done();
            let _ = tree_done.wait_for(|done| *done).await;
        });
        // The ender retires the task just after it publishes the end: the registry lets go.
        let id = task.id.clone();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while tasks.all().iter().any(|task| task.id == id) {
            assert!(
                std::time::Instant::now() < deadline,
                "the finished task stays in the registry"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        drop(task);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while weak.upgrade().is_some() {
            assert!(
                std::time::Instant::now() < deadline,
                "the finished task is still held"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        tasks.shutdown();
    }

    /// The permanent state a failed native resize leaves: the size is unknown from then on.
    fn size_unknown(error: &str) -> bool {
        error.contains("size is unknown")
    }

    /// A resize racing a kill: the two run concurrently, every resize either applies or is
    /// refused with the reason the terminal gives, and a resize after the end always refuses.
    #[test]
    fn a_resize_racing_a_kill_applies_or_refuses_and_never_breaks_the_task() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (runtime, tasks) = tasks();
        let mut args = start_args();
        args.command = "read -r line".into();
        args.pty = true;
        let task = start(&tasks, args).unwrap();
        let racer = Arc::clone(&task);
        let shared = Arc::clone(&tasks);
        let ended = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let first = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop = Arc::clone(&ended);
        let ran = Arc::clone(&first);
        let resizes = std::thread::spawn(move || {
            let (mut applied, mut refused) = (0, 0);
            let mut step = 0u16;
            while !stop.load(Ordering::SeqCst) {
                step = step.wrapping_add(1);
                match racer.resize(&shared, 100 + step % 100, 30) {
                    Ok(()) => applied += 1,
                    Err(error) => {
                        assert!(
                            error.contains("tree has ended")
                                || error.contains("terminal is closed")
                                || error.contains("Cannot resize the terminal")
                                || size_unknown(&error),
                            "a resize before the end applies, fails natively, or reports the state \
                             that failure left: {error}"
                        );
                        refused += 1;
                    }
                }
                ran.store(true, Ordering::SeqCst);
            }
            // The ender clears the control before it publishes the tree's end, so the first
            // resize after that end refuses at once. A native failure earlier in the race left a
            // permanent state error, which also refuses: a refusal either way, never a success.
            let error = racer
                .resize(&shared, 90, 20)
                .expect_err("a resize after the end refuses at once");
            assert!(
                error.contains("tree has ended") || size_unknown(&error),
                "{error}"
            );
            (applied, refused)
        });
        // The kill goes out once the worker has made a resize, so its first outcome is real.
        let ack = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !first.load(Ordering::SeqCst) {
            assert!(
                std::time::Instant::now() < ack,
                "the resize worker never ran"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        task.kill().unwrap();
        runtime.block_on(async {
            let mut tree_done = task.tree_done();
            let _ = tree_done.wait_for(|done| *done).await;
        });
        ended.store(true, Ordering::SeqCst);
        let (applied, refused) = resizes.join().unwrap();
        assert!(applied + refused > 0, "the race has outcomes");
        // The size it already has is refused too: the closed terminal is checked before the no-op.
        let (cols, rows) = task.size();
        let error = task
            .resize(&tasks, cols, rows)
            .expect_err("a closed terminal refuses its own size");
        assert!(
            error.contains("tree has ended") || size_unknown(&error),
            "{error}"
        );
        let error = task
            .resize(&tasks, 90, 20)
            .expect_err("a closed terminal refuses a resize");
        assert!(
            error.contains("tree has ended") || size_unknown(&error),
            "{error}"
        );
        tasks.shutdown();
    }

    /// A resize is a change a pending wait can see. The task prints rows and then is silent, so
    /// only the resize can wake it: the wake carries the reflowed screen, where a shrunk terminal
    /// no longer shows the rows that scrolled out. Dropping the resize's own wake leaves the wait
    /// asleep and this fails on the deadline.
    #[test]
    fn a_resize_wakes_a_wait_with_the_reflowed_screen() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (runtime, tasks) = tasks();
        let mut args = start_args();
        args.pty = true;
        args.cols = 60;
        args.rows = 30;
        args.command = "for i in $(seq 1 20); do printf 'row %02d\\n' $i; done; \
                        printf 'tail-marker\\n'; read -r line"
            .into();
        let task = start(&tasks, args).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !task
            .screen(crate::lease::process_pool())
            .map(|screen| screen.rows.iter().any(|row| row.contains("tail-marker")))
            .unwrap_or(false)
        {
            assert!(
                std::time::Instant::now() < deadline,
                "the program never printed its rows"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        // Nothing else changes from here: the program is waiting for input.
        std::thread::sleep(std::time::Duration::from_millis(200));
        let before = task.screen(crate::lease::process_pool()).unwrap();
        let mut activity = task.activity();
        while activity.has_changed().unwrap_or(false) {
            activity.borrow_and_update();
        }
        let racer = Arc::clone(&task);
        let shared = Arc::clone(&tasks);
        let resizer = std::thread::spawn(move || racer.resize(&shared, 60, 10));
        let woke = runtime.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(10), activity.changed()).await
        });
        assert!(woke.is_ok(), "the resize woke the pending wait");
        resizer.join().unwrap().expect("the resize applies");
        let screen = task.screen(crate::lease::process_pool()).unwrap();
        assert_eq!(
            (screen.cols, screen.rows_count),
            (60, 10),
            "the screen is the new size"
        );
        // Reflow keeps the side the cursor is on, so which rows survive is the emulator's call;
        // that the screen the wait woke with is no longer the old one is the contract here.
        assert!(
            screen.rows != before.rows && screen.rows.len() <= 10,
            "the woken screen is the reflowed one: {:?} then {:?}",
            before.rows,
            screen.rows
        );
        assert_eq!(task.size(), (60, 10), "the task reports the new size");
        task.kill().unwrap();
        tasks.shutdown();
    }

    #[test]
    fn two_contexts_do_not_share_a_bash_result() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (_runtime, tasks) = tasks();
        let bad = Arc::new(LaunchContext::new(
            std::env::temp_dir(),
            vec![("FASTEXEC_BASH".into(), "/nonexistent/bash".into())],
        ));
        let Err(refused) = start(
            &tasks,
            StartArgs {
                launch: Arc::clone(&bad),
                ..start_args()
            },
        ) else {
            panic!("a task started with an invalid bash override");
        };
        assert!(refused.contains("FASTEXEC_BASH"), "{refused}");
        // A context that finds bash still starts a task, and its start got its place back.
        let task = start(&tasks, start_args()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while task.is_running() || !task.output_closed() {
            assert!(Instant::now() < deadline, "the task never finished");
            std::thread::sleep(Duration::from_millis(10));
        }
        tasks.shutdown();
    }

    #[test]
    fn two_sessions_read_one_task_independently() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (_runtime, tasks) = tasks();
        let task = start(
            &tasks,
            StartArgs {
                command: "printf 'a\\nb\\n'".into(),
                ..start_args()
            },
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while task.is_running() || !task.output_closed() {
            assert!(Instant::now() < deadline, "the task never finished");
            std::thread::sleep(Duration::from_millis(10));
        }
        let (first, second) = (session("a-1"), session("b-2"));
        let read = |session: &Arc<Session>| -> String {
            let reader = session.entry(&task).reader(Some("test")).unwrap();
            let mut state = reader.read.try_lock().expect("the read gate is free");
            let window = task
                .read_window(
                    &mut state,
                    Truncate::Tail,
                    1024,
                    false,
                    true,
                    false,
                    encoding_rs::UTF_8,
                    None,
                )
                .unwrap();
            window.text
        };
        let (from_first, from_second) = (read(&first), read(&second));
        assert!(
            from_first.contains('a') && from_first.contains('b'),
            "{from_first:?}"
        );
        assert_eq!(
            from_second, from_first,
            "each session reads the whole output"
        );
        assert_eq!(
            read(&first),
            "",
            "a second read of one session sees nothing new"
        );
        tasks.shutdown();
    }
}

mod script_tests {
    use super::*;

    fn fixture(limits: Limits) -> (tokio::runtime::Runtime, Arc<Tasks>, Arc<Session>) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let tasks = {
            let _entered = runtime.enter();
            Tasks::new(limits).unwrap()
        };
        let session = Session::new(
            "script".into(),
            "script".into(),
            Arc::new(LaunchContext::new(std::env::temp_dir(), Vec::new())),
        );
        (runtime, tasks, session)
    }

    #[test]
    fn script_admission_counts_pending_starts_and_releases_both_limits() {
        let (_runtime, tasks, _session) = fixture(Limits {
            max_running: 2,
            max_scripts: 1,
            ..Limits::OFF
        });
        let (_, script) = tasks.admit(true).unwrap();
        assert!(
            tasks
                .admit(true)
                .err()
                .unwrap()
                .contains("TOO_MANY_SCRIPTS")
        );
        let (_, process) = tasks.admit(false).unwrap();
        assert!(tasks.admit(false).err().unwrap().contains("TOO_MANY_TASKS"));
        drop(script);
        let (_, next) = tasks.admit(true).unwrap();
        drop((next, process));
        let registry = lock(&tasks.registry);
        assert_eq!((registry.pending, registry.pending_scripts), (0, 0));
        drop(registry);
        tasks.shutdown();
    }

    fn script_args(tasks: &Arc<Tasks>) -> ScriptArgs {
        ScriptArgs {
            code: "return undefined".into(),
            launch: Arc::new(LaunchContext::new(std::env::temp_dir(), Vec::new())),
            kill_after_ms: None,
            transfer: false,
            call_id: None,
            service: Arc::new(crate::service::Service::new(Arc::clone(tasks), false)),
            input_owner: Default::default(),
        }
    }

    #[test]
    fn ipc_ingress_script_code_keeps_owner_until_vm_exits() {
        let (runtime, tasks, session) = fixture(Limits::OFF);
        let pool = crate::lease::BytePool::new(64);
        let mut args = script_args(&tasks);
        args.code = "await sleep(60000);".into();
        args.input_owner = crate::lease::Owner::new(pool.try_acquire(64).unwrap());
        let (task, _, _) = tasks.start_script(args, &session, "ownr".into()).unwrap();
        assert_eq!(
            pool.used(),
            64,
            "VM still owns the admitted code after start returns"
        );
        task.kill().unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                task.done().wait_for(|done| *done).await.unwrap();
                while pool.used() != 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        });
        tasks.shutdown();
    }

    #[test]
    fn ipc_ingress_stdin_queue_keeps_owner_after_wait_is_dropped() {
        use std::future::Future;
        let (runtime, tasks, session) = fixture(Limits::OFF);
        let (task, _, _) = tasks
            .start_script(script_args(&tasks), &session, "ownr".into())
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
                .await
                .unwrap()
                .unwrap();
            let pool = crate::lease::BytePool::new(64);
            let owner = crate::lease::Owner::new(pool.try_acquire(64).unwrap());
            let (sender, receiver) = mpsc::channel();
            *lock(&task.input) = Some(sender);
            let cancel = CancellationToken::new();
            let mut sending = Box::pin(task.send(Some(b"hello"), false, &cancel, owner));
            assert!(
                std::future::poll_fn(|cx| std::task::Poll::Ready(
                    sending.as_mut().poll(cx).is_pending()
                ))
                .await
            );
            drop(sending);
            assert_eq!(pool.used(), 64);
            let message = receiver.recv().unwrap();
            assert_eq!(message.bytes, b"hello");
            task.queued.fetch_sub(message.bytes.len(), Ordering::SeqCst);
            drop(message);
            assert_eq!(pool.used(), 0);
        });
        tasks.shutdown();
    }

    #[test]
    fn failed_script_start_releases_admission_and_prepared_workers() {
        let (runtime, tasks, session) = fixture(Limits::OFF);
        std::fs::remove_dir_all(&tasks.dir).unwrap();
        assert!(
            tasks
                .start_script(script_args(&tasks), &session, "1234".into())
                .is_err()
        );
        assert_eq!(tasks.pending(), 0);
        assert_eq!(lock(&tasks.registry).pending_scripts, 0);
        assert!(tasks.all().is_empty());
        std::fs::create_dir(&tasks.dir).unwrap();
        let (task, _, _) = tasks
            .start_script(script_args(&tasks), &session, "1234".into())
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
                .await
                .unwrap()
                .unwrap();
        });
        assert_eq!(task.snapshot().exit_code, Some(0));
        tasks.shutdown();
    }

    #[test]
    fn background_metadata_admits_only_notice_fields_and_leaves_claims_due_on_refusal() {
        let (runtime, tasks, session) = fixture(Limits::OFF);
        let (task, entry, _) = tasks
            .start_script(script_args(&tasks), &session, "meta".into())
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
                .await
                .unwrap()
                .unwrap();
        });
        let mut note = String::with_capacity(256);
        note.push_str("ordinary diagnostic");
        lock(&task.out).log_error = Some(note.clone());
        lock(&task.out).capture_error = Some(note.clone());
        *lock(&task.input_warning) = Some(note.clone());
        *lock(&task.input_error) = Some(note.clone());
        *lock(&task.kill_error) = Some(note);
        lock(&task.end).as_mut().unwrap().leftovers.push(Member {
            pid: 7,
            name: "worker".into(),
        });
        let snapshot = task.notice_snapshot();
        assert_eq!((snapshot.state, snapshot.exit_code), ("exited", Some(0)));
        assert!(snapshot.leftovers.is_empty());
        assert!(snapshot.log_error.is_none() && snapshot.capture_error.is_none());
        assert!(snapshot.input_warning.is_none() && snapshot.input_error.is_none());
        let charges = [256, task.id.capacity() + 128];
        let required = 32 * charges.iter().sum::<usize>();
        for limit in [required - 1, required, required + 1] {
            let memory = BytePool::new(limit);
            let mut lease = crate::lease::Lease::empty(&memory);
            let mut requested = Vec::new();
            let notice = tasks.background(
                &session,
                "another-task",
                512,
                &mut |bytes| {
                    requested.push(bytes);
                    lease
                        .try_grow(32 * bytes)
                        .then_some(())
                        .ok_or_else(|| "metadata full".into())
                },
                &mut |_| panic!("a script has no native members"),
            );
            assert_eq!(requested, charges);
            assert!(!entry.is_reported());
            if limit < required {
                assert!(notice.is_err());
                drop(notice);
                drop(lease);
            } else {
                let (value, claim) = notice.unwrap();
                let (line, shown) = value.as_ref().unwrap();
                assert_eq!(line, &format!("(Background: {} exited 0.)", task.id));
                assert_eq!(shown.len(), 1);
                assert!(shown[0].1.leftovers.is_empty());
                let owned = crate::ipc::Owned::new((value, claim), crate::lease::Owner::new(lease));
                assert_eq!(memory.used(), required);
                drop(owned);
            }
            assert_eq!(memory.used(), 0);
            assert!(!entry.is_reported());
            let claim = session.claim_outside();
            assert!(
                claim.is_claimed(),
                "an uncommitted notice leaves the outside count due"
            );
            drop(claim);
        }
        tasks.shutdown();
    }

    #[test]
    fn response_snapshot_admits_source_capacity_before_cloning() {
        let (runtime, tasks, session) = fixture(Limits::OFF);
        let (task, _, _) = tasks
            .start_script(script_args(&tasks), &session, "meta".into())
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
                .await
                .unwrap()
                .unwrap();
        });
        let mut name = String::with_capacity(48);
        name.push_str("worker");
        let mut error = String::with_capacity(64);
        error.push_str("write note");
        let mut warning = String::with_capacity(96);
        warning.push_str("input note");
        let charges = [
            name.capacity() + 32,
            error.capacity() + 128,
            warning.capacity() + 128,
        ];
        lock(&task.end)
            .as_mut()
            .unwrap()
            .leftovers
            .push(Member { pid: 7, name });
        lock(&task.out).log_error = Some(error);
        *lock(&task.input_warning) = Some(warning);
        let required = charges.iter().sum::<usize>();
        for limit in [required - 1, required, required + 1] {
            let pool = BytePool::new(limit);
            let mut lease = crate::lease::Lease::empty(&pool);
            let mut requested = Vec::new();
            let snapshot = task.snapshot_held_reserved(
                &mut |bytes| {
                    requested.push(bytes);
                    lease
                        .try_grow(bytes)
                        .then_some(())
                        .ok_or_else(|| "metadata full".into())
                },
                &mut |_| panic!("a script has no native process members"),
            );
            assert_eq!(requested, charges);
            if limit < required {
                assert!(snapshot.is_err());
                assert_eq!(pool.used(), charges[0] + charges[1]);
                drop(snapshot);
                drop(lease);
            } else {
                let snapshot = snapshot.unwrap();
                assert_eq!(snapshot.leftovers[0].name, "worker");
                assert_eq!(snapshot.log_error.as_deref(), Some("write note"));
                assert_eq!(snapshot.input_warning.as_deref(), Some("input note"));
                let owned = crate::ipc::Owned::new(snapshot, crate::lease::Owner::new(lease));
                assert_eq!(pool.used(), required);
                drop(owned);
            }
            assert_eq!(pool.used(), 0);
        }
        tasks.shutdown();
    }

    #[test]
    fn response_snapshot_refresh_admits_diagnostics_without_recloning_final_state() {
        let (runtime, tasks, session) = fixture(Limits::OFF);
        let (task, _, _) = tasks
            .start_script(script_args(&tasks), &session, "meta".into())
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
                .await
                .unwrap()
                .unwrap();
        });
        let mut snapshot = task.snapshot();
        snapshot.leftovers.push(Member {
            pid: 7,
            name: "worker".into(),
        });
        snapshot.kill_error = Some("previous tree note".into());
        let member_ptr = snapshot.leftovers[0].name.as_ptr();
        let kill_ptr = snapshot.kill_error.as_ref().unwrap().as_ptr();
        let old_lines = snapshot.lines;
        let diagnostic = |text: &str| {
            let mut value = String::with_capacity(64);
            value.push_str(text);
            value
        };
        {
            let mut out = lock(&task.out);
            out.lines = 9;
            out.dropped = 2;
            out.log_error = Some(diagnostic("new log note"));
            out.capture_error = Some(diagnostic("new capture note"));
        }
        *lock(&task.input_warning) = Some(diagnostic("new input warning"));
        *lock(&task.input_error) = Some(diagnostic("new input note"));
        let required = 4 * (64 + 128);
        for limit in [required - 1, required] {
            let pool = BytePool::new(limit);
            let mut lease = crate::lease::Lease::empty(&pool);
            let mut copies = 0;
            let refreshed = task.refresh_snapshot_output(&mut snapshot, &mut |bytes| {
                copies += 1;
                assert_eq!(bytes, 64 + 128);
                lease
                    .try_grow(bytes)
                    .then_some(())
                    .ok_or_else(|| "metadata full".into())
            });
            assert_eq!(copies, 4);
            if limit < required {
                assert!(refreshed.is_err());
                assert_eq!(snapshot.lines, old_lines);
                assert!(snapshot.log_error.is_none());
                assert!(snapshot.capture_error.is_none());
                assert!(snapshot.input_warning.is_none());
                assert!(snapshot.input_error.is_none());
                drop(lease);
            } else {
                refreshed.unwrap();
                assert_eq!((snapshot.lines, snapshot.dropped), (9, 2));
                assert_eq!(snapshot.log_error.as_deref(), Some("new log note"));
                assert_eq!(snapshot.capture_error.as_deref(), Some("new capture note"));
                assert_eq!(snapshot.input_warning.as_deref(), Some("new input warning"));
                assert_eq!(snapshot.input_error.as_deref(), Some("new input note"));
                assert_eq!(snapshot.leftovers[0].name.as_ptr(), member_ptr);
                assert_eq!(snapshot.kill_error.as_ref().unwrap().as_ptr(), kill_ptr);
                // Keep the new diagnostic copies owned until the snapshot is destroyed below.
                let owned = crate::ipc::Owned::new(snapshot, crate::lease::Owner::new(lease));
                assert_eq!(pool.used(), required);
                drop(owned);
                assert_eq!(pool.used(), 0);
                tasks.shutdown();
                return;
            }
            assert_eq!(pool.used(), 0);
        }
        unreachable!();
    }

    #[test]
    fn snapshots_hide_cleanup_errors_only_after_confirmed_tree_end() {
        let (runtime, tasks, session) = fixture(Limits::OFF);
        let (task, _, _) = tasks
            .start_script(
                ScriptArgs {
                    code: "while (true) {}".into(),
                    ..script_args(&tasks)
                },
                &session,
                "1234".into(),
            )
            .unwrap();
        for error in [
            "descendants remain after SIGKILL; the task host stays alive",
            "task host disconnected; tree cleanup is unconfirmed",
        ] {
            *lock(&task.kill_error) = Some(error.into());
            assert_eq!(task.snapshot().kill_error.as_deref(), Some(error));
        }
        // End the root without clearing the recorded error through Task::kill.
        task.root.kill().unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
                .await
                .unwrap()
                .unwrap();
        });
        assert!(lock(&task.kill_error).is_some());
        assert!(task.snapshot().kill_error.is_none());
        // A concurrent kill may store its earlier failure after completion was confirmed.
        *lock(&task.kill_error) = Some("late cleanup error".into());
        assert!(task.snapshot().kill_error.is_none());
        tasks.shutdown();
    }

    #[test]
    fn host_loss_holds_the_task_before_or_after_the_root_result() {
        let (runtime, tasks, session) = fixture(Limits {
            max_running: 1,
            ..Limits::OFF
        });
        let (task, _, _) = tasks
            .start_script(script_args(&tasks), &session, "1234".into())
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
                .await
                .unwrap()
                .unwrap();
        });
        for before_root_result in [true, false] {
            *lock(&task.end) = None;
            *lock(&task.kill_error) = None;
            task.tree_done.send_replace(false);
            if before_root_result {
                task.hold_unconfirmed_tree();
                task.kill().unwrap();
            }
            task.publish_end(Final {
                exit_code: Some(0),
                killed: false,
                expired: false,
                forced_end: false,
                elapsed: Duration::ZERO,
                output_open: false,
                leftovers: Vec::new(),
                kept: false,
            });
            if !before_root_result {
                task.hold_unconfirmed_tree();
                task.kill().unwrap();
            }
            let snapshot = task.snapshot();
            assert!(snapshot.kept);
            assert!(!snapshot.tree_ended);
            assert!(snapshot.kill_error.unwrap().contains("unconfirmed"));
            assert_eq!(task.root_end(), Some((Some(0), true)));
            assert!(task.keeps_processes());
            assert!(tasks.admit(false).err().unwrap().contains("TOO_MANY_TASKS"));
        }
        tasks.shutdown();
    }

    #[test]
    fn pending_tree_confirmation_blocks_retirement_expiry_eviction_and_admission() {
        let (runtime, tasks, session) = fixture(Limits {
            max_running: 2,
            max_finished: 1,
            finished_ttl: Some(Duration::ZERO),
            ..Limits::OFF
        });
        let wait = |task: &Task| {
            runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
                    .await
                    .unwrap()
                    .unwrap();
            });
        };
        let (pending, _, _) = tasks
            .start_script(script_args(&tasks), &session, "1234".into())
            .unwrap();
        wait(&pending);
        pending.tree_done.send_replace(false);
        assert!(!pending.is_running() && !pending.keeps_processes());
        assert!(!pending.is_finished());

        let (finished, _, _) = tasks
            .start_script(script_args(&tasks), &session, "5678".into())
            .unwrap();
        wait(&finished);
        assert!(
            tasks.find(&pending.id).is_some(),
            "pending trees do not retire"
        );
        assert!(!pending.evicted());

        let (_, admitted) = tasks.admit(false).unwrap();
        assert!(tasks.admit(false).err().unwrap().contains("TOO_MANY_TASKS"));
        drop(admitted);

        tasks.expire_finished(Instant::now());
        assert!(tasks.find(&finished.id).is_none());
        assert!(
            tasks.find(&pending.id).is_some(),
            "pending trees do not expire"
        );
        assert!(!tasks.reserve(tasks.log_limit + 1));
        assert!(
            !pending.evicted(),
            "log pressure must preserve a pending tree"
        );
        assert!(pending.log_path.exists());

        pending.tree_done.send_replace(true);
        assert!(pending.is_finished());
        tasks.expire_finished(Instant::now());
        assert!(tasks.find(&pending.id).is_none());
        assert!(pending.evicted());
        tasks.shutdown();
    }

    #[test]
    fn stored_attachments_track_seen_watermarks_and_release_quota_on_eviction() {
        let (runtime, tasks, session) = fixture(Limits::OFF);
        let (task, _, _) = tasks
            .start_script(script_args(&tasks), &session, "1234".into())
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
                .await
                .unwrap()
                .unwrap();
        });
        let before = tasks.log_bytes.load(Ordering::SeqCst);
        let first = tasks
            .store_attachment(&task, MediaKind::Image, "image/png", b"one")
            .unwrap();
        assert_eq!(first.index, 1);
        assert_eq!(std::fs::read(&first.path).unwrap(), b"one");
        let second = tasks
            .store_attachment(&task, MediaKind::Document, "application/pdf", b"two")
            .unwrap();
        assert_eq!(second.index, 2);
        assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), before + 6);
        assert_eq!(task.unseen_attachment_count(0), 2);
        assert_eq!(task.unseen_attachments(0)[0].meta().index, 1);
        assert_eq!(task.unseen_attachments(1)[0].meta().index, 2);
        assert!(task.unseen_attachments(2).is_empty());

        let pinned = task.unseen_attachments(1).pop().unwrap();
        tasks.evict_log(&task);
        assert!(!first.path.exists());
        assert!(
            second.path.exists(),
            "pinned attachment must survive eviction until lease drops"
        );
        assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), 3);
        drop(pinned);
        assert!(!second.path.exists());
        assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), 0);
        tasks.shutdown();
    }

    #[test]
    fn failed_unlink_retains_disk_quota_across_task_retirement_and_refunds_once_on_retry() {
        let (runtime, mut tasks, session) = fixture(Limits {
            max_running: 16,
            max_scripts: 4,
            max_finished: 1,
            finished_ttl: Some(Duration::from_millis(1)),
        });
        Arc::get_mut(&mut tasks).unwrap().set_log_limit(1024);
        let (task1, _, _) = tasks
            .start_script(script_args(&tasks), &session, "0001".into())
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task1.done().wait_for(|done| *done))
                .await
                .unwrap()
                .unwrap();
        });
        let baseline = tasks.log_bytes.load(Ordering::SeqCst);
        assert!(baseline < 900, "unexpected baseline={baseline}");
        let att_len = (1024 - baseline - 30) as usize;
        let att1 = tasks
            .store_attachment(
                &task1,
                MediaKind::Document,
                "application/pdf",
                &vec![b'x'; att_len],
            )
            .unwrap();
        assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), 994);

        let pin = task1.unseen_attachments(0).pop().expect("leased");
        let fail_deletes = Arc::new(AtomicBool::new(true));
        let flag = Arc::clone(&fail_deletes);
        tasks.set_remove_hook(Some(Arc::new(move |path| {
            if flag.load(Ordering::SeqCst) {
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "sharing violation",
                ))
            } else {
                std::fs::remove_file(path)
            }
        })));

        std::thread::sleep(Duration::from_millis(5));
        tasks.expire_finished(Instant::now());
        assert!(
            tasks.find(&task1.id).is_none(),
            "task1 removed from registry"
        );
        assert!(att1.path.exists(), "read lease kept attachment on disk");
        assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), 994);

        drop(pin);
        assert!(att1.path.exists());
        assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), 994);

        assert!(!tasks.reserve(40), "must not admit past 1024-byte limit");
        assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), 994);

        fail_deletes.store(false, Ordering::SeqCst);
        assert!(tasks.reserve(40), "freed pending delete makes room");
        assert!(!att1.path.exists());
        assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), 40);

        tasks.retry_pending_deletes();
        assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), 40);
        tasks.log_bytes.fetch_sub(40, Ordering::SeqCst);
        tasks.set_remove_hook(None);
        tasks.shutdown();
    }

    #[test]
    fn per_task_attachment_cap_stops_at_1024_before_creating_extra_files() {
        let (runtime, tasks, session) = fixture(Limits::OFF);
        let (task, _, _) = tasks
            .start_script(script_args(&tasks), &session, "cap1".into())
            .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
                .await
                .unwrap()
                .unwrap();
        });
        for i in 1..=MAX_TASK_ATTACHMENTS {
            let meta = tasks
                .store_attachment(&task, MediaKind::Image, "image/png", b"x")
                .unwrap();
            assert_eq!(meta.index, i);
        }
        let err = tasks
            .store_attachment(&task, MediaKind::Image, "image/png", b"x")
            .unwrap_err();
        assert!(err.contains("1024"), "{err}");
        assert!(
            !task
                .attachment_path(MAX_TASK_ATTACHMENTS + 1, "image/png")
                .exists()
        );
        tasks.shutdown();
    }

    fn finished_pty_task(tasks: &Arc<Tasks>, cols: u16, rows: u16) -> Arc<Task> {
        let id = tasks.next_id();
        let log_path = tasks.dir.join(format!("{id}.log"));
        let _log = private_file(&log_path).unwrap();
        let transcript_path = tasks.dir.join(format!("{id}.transcript.txt"));
        let tfile = private_file(&transcript_path).unwrap();
        let ended_at = OnceLock::new();
        let _ = ended_at.set(Instant::now());
        let (script, _worker) = codemode::prepare(tasks.handle.clone()).unwrap();
        let task = Arc::new(Task {
            id,
            command: "pty-sim".into(),
            pty: true,
            log_path,
            encoding: encoding_rs::UTF_8,
            keep: false,
            owner: "pty-test".into(),
            session_id: None,
            started_by: None,
            started: Instant::now(),
            root: Root::Script(script),
            tree_done: watch::Sender::new(true),
            tree_ended_at: ended_at,
            kill_error: Mutex::new(None),
            kill_requested: AtomicBool::new(false),
            expired: AtomicBool::new(false),
            out: Mutex::default(),
            end: Mutex::new(Some(Final {
                exit_code: Some(0),
                killed: false,
                expired: false,
                forced_end: false,
                elapsed: Duration::ZERO,
                output_open: true,
                leftovers: Vec::new(),
                kept: false,
            })),
            done: watch::Sender::new(true),
            activity: watch::Sender::new(()),
            input: Mutex::new(None),
            writer_done: WriterDone::new(true),
            input_warning: Mutex::new(None),
            input_error: Mutex::new(None),
            queued: AtomicUsize::new(0),
            input_gate: tokio::sync::Mutex::new(()),
            terminal: Some(Mutex::new(Pty {
                live: Live::at(cols, rows),
                spool: Spool {
                    bytes: 0,
                    lines: 0,
                    pending: 0,
                    stopped: None,
                },
            })),
            transcript_file: Mutex::new(Some(tfile)),
            size: Mutex::new((cols, rows)),
            pty_control: Mutex::new(None),
            size_error: Mutex::new(None),
        });
        lock(&tasks.registry).tasks.push(Arc::clone(&task));
        task
    }

    fn partial_log_write(tasks: &Tasks, rollback_fails: bool) {
        *lock(&tasks.log_write_hook) = Some(Arc::new(|file, bytes| {
            file.write_all(&bytes[..bytes.len().min(2)])?;
            Err(std::io::Error::other("injected partial write"))
        }));
        *lock(&tasks.log_truncate_hook) = rollback_fails.then(|| {
            Arc::new(|_: &File, _: u64| Err(std::io::Error::other("injected truncate failure")))
                as LogTruncateHook
        });
    }

    fn delay_log_delete(tasks: &Tasks) -> Arc<AtomicBool> {
        let delayed = Arc::new(AtomicBool::new(true));
        let flag = Arc::clone(&delayed);
        tasks.set_remove_hook(Some(Arc::new(move |path| {
            if flag.load(Ordering::SeqCst) {
                Err(std::io::Error::other("injected delete delay"))
            } else {
                std::fs::remove_file(path)
            }
        })));
        delayed
    }

    fn seed_capture_prefix(tasks: &Tasks, task: &Task) -> File {
        let mut file = File::options().write(true).open(&task.log_path).unwrap();
        file.write_all(b"ok\n").unwrap();
        assert!(tasks.reserve(3));
        let mut out = lock(&task.out);
        out.written = 3;
        out.readable = 3;
        out.lines = 1;
        file
    }

    fn capture_small_tail(tasks: &Tasks, task: &Task, log: File) {
        let (chunks, received) = mpsc::sync_channel(2);
        chunks
            .send(Ok((Instant::now(), b"tail\n".to_vec())))
            .unwrap();
        chunks
            .send(Ok((Instant::now(), b"later\n".to_vec())))
            .unwrap();
        drop(chunks);
        capture(tasks, task, received, log, None);
    }

    #[test]
    fn log_tail_capture_refunds_only_after_confirmed_rollback() {
        for rollback_fails in [false, true] {
            let (_runtime, tasks, _session) = fixture(Limits::OFF);
            let task = finished_pty_task(&tasks, 4, 2);
            let log = seed_capture_prefix(&tasks, &task);
            partial_log_write(&tasks, rollback_fails);
            capture_small_tail(&tasks, &task, log);
            let charge = if rollback_fails { 8 } else { 3 };
            {
                let out = lock(&task.out);
                assert_eq!((out.written, out.readable, out.lines), (3, 3, 1));
                assert_eq!(out.dropped, 11);
                assert_eq!(out.log_tail, charge - 3);
                assert!(out.log_error.is_some());
            }
            assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), charge);
            assert_eq!(
                std::fs::read(&task.log_path).unwrap(),
                if rollback_fails {
                    &b"ok\nta"[..]
                } else {
                    &b"ok\n"[..]
                },
            );
            let delayed = delay_log_delete(&tasks);
            tasks.evict_log(&task);
            assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), charge);
            delayed.store(false, Ordering::SeqCst);
            tasks.retry_pending_deletes();
            assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), 0);
            tasks.retry_pending_deletes();
            assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), 0);
            assert!(!task.log_path.exists());
            tasks.shutdown();
        }
    }

    #[test]
    fn log_tail_spool_keeps_failed_tail_out_of_later_transcripts() {
        for rollback_fails in [false, true] {
            let (_runtime, tasks, _session) = fixture(Limits::OFF);
            let task = finished_pty_task(&tasks, 4, 2);
            {
                let mut pty = lock(task.terminal.as_ref().unwrap());
                assert!(pty.spool.write(b"ok\n", true, &task, &tasks));
            }
            partial_log_write(&tasks, rollback_fails);
            {
                let mut pty = lock(task.terminal.as_ref().unwrap());
                assert!(!pty.spool.write(b"tail\n", false, &task, &tasks));
                pty.spool.append("later\n", &task, &tasks);
                assert_eq!(
                    (pty.spool.bytes, pty.spool.lines, pty.spool.pending),
                    (3, 1, 0)
                );
                assert!(pty.spool.stopped.is_some());
            }
            let charge = if rollback_fails { 8 } else { 3 };
            let memory = BytePool::new(1024);
            for _ in 0..2 {
                let transcript = task
                    .transcript(&tasks, &ReadState::default(), &memory)
                    .unwrap();
                assert_eq!((transcript.bytes, transcript.lines), (3, 1));
                assert!(transcript.pending.is_empty());
                assert!(transcript.stopped.is_some());
            }
            assert_eq!(lock(&task.out).transcript, charge);
            assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), charge);
            assert_eq!(
                std::fs::read(task.transcript_path()).unwrap(),
                if rollback_fails {
                    &b"ok\nta"[..]
                } else {
                    &b"ok\n"[..]
                },
            );
            let delayed = delay_log_delete(&tasks);
            tasks.evict_log(&task);
            assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), charge);
            delayed.store(false, Ordering::SeqCst);
            tasks.retry_pending_deletes();
            assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), 0);
            tasks.retry_pending_deletes();
            assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), 0);
            assert!(!task.transcript_path().exists());
            tasks.shutdown();
        }
    }

    #[test]
    fn log_tail_eviction_during_partial_write_reclaims_each_charge_once() {
        for transcript in [false, true] {
            for rollback_fails in [false, true] {
                let (_runtime, tasks, _session) = fixture(Limits::OFF);
                let task = finished_pty_task(&tasks, 4, 2);
                let log = if transcript {
                    let mut pty = lock(task.terminal.as_ref().unwrap());
                    assert!(pty.spool.write(b"ok\n", true, &task, &tasks));
                    None
                } else {
                    Some(seed_capture_prefix(&tasks, &task))
                };
                partial_log_write(&tasks, rollback_fails);
                let delayed = delay_log_delete(&tasks);
                let (writing, started) = mpsc::channel();
                let observed = Arc::clone(&task);
                *lock(&tasks.log_write_hook) = Some(Arc::new(move |file, bytes| {
                    file.write_all(&bytes[..2])?;
                    writing.send(()).unwrap();
                    let deadline = Instant::now() + Duration::from_secs(5);
                    while !observed.evicted() {
                        assert!(Instant::now() < deadline, "eviction did not start");
                        std::thread::yield_now();
                    }
                    Err(std::io::Error::other("injected partial write"))
                }));
                let evicting = Arc::clone(&tasks);
                let removed = Arc::clone(&task);
                let eviction = std::thread::spawn(move || {
                    started.recv_timeout(Duration::from_secs(5)).unwrap();
                    evicting.evict_log(&removed);
                });
                if transcript {
                    let mut pty = lock(task.terminal.as_ref().unwrap());
                    assert!(!pty.spool.write(b"tail\n", true, &task, &tasks));
                    assert_eq!(
                        (pty.spool.bytes, pty.spool.lines, pty.spool.pending),
                        (3, 1, 0)
                    );
                } else {
                    capture_small_tail(&tasks, &task, log.unwrap());
                    let out = lock(&task.out);
                    assert_eq!((out.written, out.readable, out.lines), (3, 3, 1));
                    assert_eq!(out.log_tail, 0, "eviction owns the new reservation");
                }
                eviction.join().unwrap();
                let charge = if rollback_fails { 8 } else { 3 };
                assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), charge);
                assert_eq!(
                    lock(&tasks.pending_deletes)
                        .iter()
                        .map(|entry| entry.bytes)
                        .sum::<u64>(),
                    charge
                );
                delayed.store(false, Ordering::SeqCst);
                tasks.retry_pending_deletes();
                assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), 0);
                tasks.retry_pending_deletes();
                assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), 0);
                tasks.shutdown();
            }
        }
    }

    #[test]
    fn terminal_snapshots_hold_leases_and_refuse_before_changing_transcript() {
        let (_runtime, tasks, _session) = fixture(Limits::OFF);
        let task = finished_pty_task(&tasks, 12, 3);
        let (screen_bytes, pending_bytes) = {
            let mut pty = lock(task.terminal.as_ref().unwrap());
            pty.live.process(b"first\r\nsecond", Instant::now(), None);
            (
                pty.live.screen_snapshot_bytes(),
                pty.live.pending_snapshot_bytes(),
            )
        };

        let memory = BytePool::new(screen_bytes);
        let occupied = memory.try_acquire(1).unwrap();
        let refused = task.screen(&memory).err().unwrap();
        assert!(refused.starts_with("Server active memory budget ("));
        assert!(refused.contains(&task.id));
        assert!(refused.contains(&crate::files::path::display_path(&task.log_path)));
        assert!(refused.contains("Do not retry start or resend input"));
        assert!(refused.contains("poll taskId without input"));
        assert_eq!(memory.used(), 1);
        drop(occupied);
        let screen = task.screen(&memory).unwrap();
        assert_eq!(screen.rows, ["first", "second"]);
        assert_eq!(memory.used(), screen_bytes);
        assert!(task.screen(&memory).is_err());
        drop(screen);
        assert_eq!(memory.used(), 0);

        let memory = BytePool::new(pending_bytes);
        let occupied = memory.try_acquire(1).unwrap();
        let state = ReadState::default();
        let before = {
            let pty = lock(task.terminal.as_ref().unwrap());
            (pty.spool.bytes, pty.spool.lines, pty.spool.pending)
        };
        let path = task.transcript_path();
        let file_before = std::fs::read(&path).unwrap();
        let quota_before = tasks.log_bytes.load(Ordering::SeqCst);
        assert!(
            task.transcript(&tasks, &state, &memory)
                .err()
                .unwrap()
                .contains("memory budget")
        );
        assert_eq!(memory.used(), 1);
        assert_eq!(state.shown, (0, 0));
        {
            let pty = lock(task.terminal.as_ref().unwrap());
            assert_eq!(
                (pty.spool.bytes, pty.spool.lines, pty.spool.pending),
                before
            );
        }
        assert_eq!(std::fs::read(&path).unwrap(), file_before);
        assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), quota_before);

        drop(occupied);
        let transcript = task.transcript(&tasks, &state, &memory).unwrap();
        assert_eq!(transcript.pending, "first\nsecond\n");
        assert_eq!(memory.used(), pending_bytes);
        assert!(task.transcript(&tasks, &state, &memory).is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), transcript.pending);
        drop(transcript);
        assert_eq!(memory.used(), 0);
        let next = task.transcript(&tasks, &state, &memory).unwrap();
        assert_eq!(next.pending, "first\nsecond\n");
        drop(next);
        assert_eq!(memory.used(), 0);
        tasks.shutdown();
    }

    #[test]
    fn pty_eviction_under_terminal_lock_does_not_deadlock_and_concurrent_spool_evict_tracks_delete()
    {
        let (_runtime, mut tasks, _session) = fixture(Limits::OFF);
        Arc::get_mut(&mut tasks).unwrap().set_log_limit(64);

        let task = finished_pty_task(&tasks, 80, 24);
        assert!(task.is_finished());
        // Seed initial transcript bytes while under the 64-byte limit.
        {
            let mut pty = lock(task.terminal.as_ref().unwrap());
            assert!(pty.spool.write(b"first-line\n", true, &task, &tasks));
        }
        assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), 11);

        // Hold `task.terminal` (exact state of `capture` with open output or `resize`) and write
        // 60 more bytes (11 + 60 > 64), forcing `reserve` -> `evict_log(&task)` on this thread!
        {
            let mut pty = lock(task.terminal.as_ref().unwrap());
            assert!(!pty.spool.write(&[b'z'; 60], true, &task, &tasks));
        }
        assert!(task.evicted());
        assert!(lock(&task.transcript_file).is_none());
        assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), 0);

        // Verify concurrent eviction during `Spool::write` closes `transcript_file` and records
        // `pending_deletes` when unlinking fails (R4-7).
        let task2 = finished_pty_task(&tasks, 80, 24);
        let fail_deletes = Arc::new(AtomicBool::new(true));
        let flag = Arc::clone(&fail_deletes);
        tasks.set_remove_hook(Some(Arc::new(move |path| {
            if flag.load(Ordering::SeqCst) {
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "locked",
                ))
            } else {
                std::fs::remove_file(path)
            }
        })));
        // Hold `task2.out` on a helper thread that flips `out.evicted = true` right after
        // `Spool::write` finishes writing bytes to disk and waits to call `shared.hold`!
        assert!(tasks.reserve(20));
        {
            let mut fg = lock(&task2.transcript_file);
            fg.as_mut().unwrap().write_all(&[b'a'; 20]).unwrap();
        }
        lock(&task2.out).evicted = true;
        assert!(!tasks.hold(&task2, 20));
        let closed = lock(&task2.transcript_file).take();
        drop(closed);
        tasks.reclaim_file(task2.transcript_path(), 20);
        assert_eq!(
            tasks.log_bytes.load(Ordering::SeqCst),
            20,
            "failed unlink must keep transcript bytes charged"
        );
        fail_deletes.store(false, Ordering::SeqCst);
        tasks.retry_pending_deletes();
        assert!(!task2.transcript_path().exists());
        assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), 0);
        tasks.set_remove_hook(None);
        tasks.shutdown();
    }

    #[test]
    fn eof_only_poll_after_initial_broken_pipe_stays_idempotent_and_huge_deadline_is_safe() {
        let (runtime, tasks, session) = fixture(Limits::OFF);
        let mut args = script_args(&tasks);
        args.kill_after_ms = Some(u64::MAX);
        let (task, _, _) = tasks.start_script(args, &session, "r001".into()).unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
                .await
                .unwrap()
                .unwrap();
        });
        assert_eq!(task.snapshot().exit_code, Some(0));

        // Deterministic race: initial input hits BrokenPipe while an empty-bytes eof: true
        // message is already queued behind it in the same writer channel.
        let workers = Workers::new(true).unwrap();
        let (pipe_reader, pipe_writer) = std::io::pipe().unwrap();
        drop(pipe_reader);
        let (input_tx, input_rx) = mpsc::channel();
        let payload = vec![b'x'; 4096];
        task.queued.store(payload.len(), Ordering::SeqCst);
        *lock(&task.input) = Some(input_tx.clone());
        input_tx
            .send(Input {
                bytes: payload,
                eof: false,
                initial: true,
                done: None,
                _input_owner: Default::default(),
            })
            .unwrap();
        let (eof_done_tx, eof_done_rx) = oneshot::channel();
        input_tx
            .send(Input {
                bytes: Vec::new(),
                eof: true,
                initial: false,
                done: Some(eof_done_tx),
                _input_owner: Default::default(),
            })
            .unwrap();
        drop(input_tx);
        let done_sig = WriterDone::new(false);
        workers
            .input
            .unwrap()
            .send(InputWork {
                task: Arc::clone(&task),
                input: Box::new(pipe_writer),
                received: input_rx,
                done_signal: Arc::clone(&done_sig),
            })
            .unwrap();
        let eof_res = runtime.block_on(eof_done_rx).unwrap();
        assert_eq!(
            eof_res,
            Ok(()),
            "idempotent eof close after BrokenPipe failed"
        );
        done_sig.wait_until(Instant::now() + Duration::from_secs(2));
        let snap = task.snapshot();
        assert!(
            snap.input_warning.as_deref().is_some_and(
                |w| w.starts_with("stdin closed after ") && w.ends_with(" of 4096 bytes")
            ),
            "{:?}",
            snap.input_warning
        );
        assert!(snap.input_error.is_none(), "{:?}", snap.input_error);

        tasks.shutdown();
    }

    #[test]
    fn self_evicting_chunk_records_dropped_bytes_last_output_and_reclaims_quota() {
        let (runtime, tasks, session) = fixture(Limits::OFF);
        let mut args = script_args(&tasks);
        args.code = "text('abc')".into();
        let (task, _, _) = tasks.start_script(args, &session, "ev01".into()).unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
                .await
                .unwrap()
                .unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        while task.is_running() || !task.output_closed() {
            assert!(Instant::now() < deadline, "script capture never closed");
            std::thread::sleep(Duration::from_millis(10));
        }
        let written_before = lock(&task.out).written;
        assert!(written_before >= 3);

        let before = tasks.log_limit - 97;
        tasks.log_bytes.store(before, Ordering::SeqCst);
        let log = File::create(tasks.dir.join("extra.log")).unwrap();
        let (chunks, received) = mpsc::sync_channel(1);
        let arrived = Instant::now() + Duration::from_secs(1);
        chunks.send(Ok((arrived, vec![b'x'; 99]))).unwrap();
        drop(chunks);
        capture(&tasks, &task, received, log, None);
        let out = lock(&task.out);
        assert!(out.evicted);
        assert_eq!((out.written, out.dropped), (written_before, 99));
        assert_eq!(out.last_output, Some(arrived));
        assert_eq!(
            tasks.log_bytes.load(Ordering::SeqCst),
            before - written_before
        );
        drop(out);
        tasks.shutdown();
    }
}
