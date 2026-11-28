use super::lifecycle::*;
use super::query::*;
use super::quota::lock;
use super::quota::*;
use super::session::*;
use super::state::*;
use super::store::*;
use super::workers::*;
use super::*;
use crate::env::LaunchContext;
use crate::id::{ALPHABET, is_valid_task_id, random_chars};
use crate::lease::BytePool;
use crate::media::MediaKind;
use crate::platform::members::Member;
use crate::platform::process;
use crate::term::terminal::Live;
use std::collections::HashSet;
use std::fs::File;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, watch};
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
#[allow(clippy::module_inception)]
mod tests {
    use super::*;
    use crate::render::types::AgentDelivery;
    use crate::service::schema::Request;
    use crate::service::{ExecContext, Service};
    use crate::term::output::{Cleaner, Matcher, Truncate, WindowSpec};

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

    fn start(tasks: &Arc<Tasks>, args: StartArgs) -> Result<Arc<Task>, TaskError> {
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
            parent_task_id: None,
            queued_guard: None,
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
        let before = tasks.quota.log_limit - 97;
        tasks.quota.log_bytes.store(before, Ordering::SeqCst);
        let log = File::create(tasks.dir.join("extra.log")).unwrap();
        let (chunks, received) = mpsc::sync_channel(1);
        chunks.send(Ok((Instant::now(), vec![b'x'; 99]))).unwrap();
        drop(chunks);
        capture(&tasks, &task, received, log, None);
        let out = lock(&task.out);
        assert!(out.evicted);
        assert_eq!((out.written, out.dropped), (3, 99));
        assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), before - 3);
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
        let ctx = ExecContext {
            session: &session,
            launch: None,
            cancel: tokio_util::sync::CancellationToken::new(),
            progress: None,
            call_id: None,
            parent_task_id: None,
            queued_guard: None,
            on_started: None,
            input_owner: Default::default(),
        };
        let list_res = runtime.block_on(crate::render::exec_agent(
            &service,
            request,
            ctx,
            AgentDelivery::default(),
        ));
        assert!(list_res.result.structured_content.is_none());
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
        assert!(matches!(&refused, TaskError::Overload(_)), "{refused}");
        assert!(refused.message().starts_with("TOO_MANY_TASKS"), "{refused}");
        assert!(refused.message().contains('2'), "{refused}");
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
        assert!(matches!(&refused, TaskError::Overload(_)), "{refused}");
        assert!(refused.message().starts_with("TOO_MANY_TASKS"), "{refused}");
        lock(&tasks.registry).pending = 0;
        // A stopping server admits nothing.
        lock(&tasks.registry).stopping = true;
        let Err(refused) = start(&tasks, start_args()) else {
            panic!("a start was admitted while the server was stopping");
        };
        assert!(matches!(&refused, TaskError::Io(_)), "{refused}");
        assert!(refused.message().contains("shutting down"), "{refused}");
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
        tasks
            .quota
            .log_bytes
            .store(tasks.quota.log_limit - 1, Ordering::SeqCst);
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
        let head_spec = WindowSpec {
            truncate: Truncate::Head,
            budget: 4096,
            raw: false,
            clean: true,
            keep_nul: false,
            include_markers: true,
            encoding: encoding_rs::UTF_8,
            source: "log",
        };
        let window = task
            .read_window_with_markers(&mut state, head_spec, None)
            .expect("an evicted log is not an I/O error");
        // Empty, with no invented line range: the snapshot reports the eviction.
        assert_eq!(window.text, "");
        assert_eq!(window.omitted, None);
        assert_eq!(state.cursor, written, "the cursor moved past the output");
        // A second read has nothing more to move past and stays empty.
        let again = task
            .read_window_with_markers(&mut state, head_spec, None)
            .unwrap();
        assert_eq!(again.text, "");
        assert_eq!(state.cursor, written);
        // A legacy encoding takes its own path through the same read.
        assert!(
            task.read_window_with_markers(
                &mut ReadState::default(),
                WindowSpec {
                    truncate: Truncate::Tail,
                    budget: 1024,
                    raw: false,
                    clean: true,
                    keep_nul: false,
                    include_markers: true,
                    encoding: encoding_rs::BIG5,
                    source: "log",
                },
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
        let ctx = || ExecContext {
            session: &session,
            launch: None,
            cancel: tokio_util::sync::CancellationToken::new(),
            progress: None,
            call_id: None,
            parent_task_id: None,
            queued_guard: None,
            on_started: None,
            input_owner: Default::default(),
        };
        let name = dir.file_name().unwrap().to_string_lossy().into_owned();
        // A pipe task and a PTY task see the same environment and the same default directory.
        for pty in [false, true] {
            let result = runtime
                .block_on(crate::render::exec_agent_json(
                    &service,
                    serde_json::json!({
                        "action": "start",
                        "command": command,
                        "pty": pty,
                        "waitMs": 30000,
                    }),
                    ctx(),
                    AgentDelivery::default(),
                ))
                .result;
            assert!(result.structured_content.is_none());
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
        assert!(matches!(&refused, TaskError::Io(_)), "{refused}");
        assert!(refused.message().contains("FASTEXEC_BASH"), "{refused}");
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
                .read_window_with_markers(
                    &mut state,
                    WindowSpec {
                        truncate: Truncate::Tail,
                        budget: 1024,
                        raw: false,
                        clean: true,
                        keep_nul: false,
                        include_markers: true,
                        encoding: encoding_rs::UTF_8,
                        source: "log",
                    },
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
            matches!(tasks.admit(true), Err(TaskError::Overload(message)) if message.contains("TOO_MANY_SCRIPTS"))
        );
        let (_, process) = tasks.admit(false).unwrap();
        assert!(
            matches!(tasks.admit(false), Err(TaskError::Overload(message)) if message.contains("TOO_MANY_TASKS"))
        );
        drop(script);
        let (_, next) = tasks.admit(true).unwrap();
        drop((next, process));
        let registry = lock(&tasks.registry);
        assert_eq!((registry.pending, registry.pending_scripts), (0, 0));
        drop(registry);
        tasks.shutdown();
    }

    fn script_args(tasks: &Arc<Tasks>) -> crate::codemode::ScriptSpawnArgs {
        crate::codemode::ScriptSpawnArgs {
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
        let ((task, _, _), _) =
            crate::codemode::spawn_script(&tasks, args, &session, "ownr".into()).unwrap();
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
        let ((task, _, _), _) =
            crate::codemode::spawn_script(&tasks, script_args(&tasks), &session, "ownr".into())
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
            crate::codemode::spawn_script(&tasks, script_args(&tasks), &session, "1234".into())
                .is_err()
        );
        assert_eq!(tasks.pending(), 0);
        assert_eq!(lock(&tasks.registry).pending_scripts, 0);
        assert!(tasks.all().is_empty());
        std::fs::create_dir(&tasks.dir).unwrap();
        let ((task, _, _), _) =
            crate::codemode::spawn_script(&tasks, script_args(&tasks), &session, "1234".into())
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
        let ((task, entry, _), _) =
            crate::codemode::spawn_script(&tasks, script_args(&tasks), &session, "meta".into())
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
                let owned =
                    crate::lease::Owned::new((value, claim), crate::lease::Owner::new(lease));
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
        let ((task, _, _), _) =
            crate::codemode::spawn_script(&tasks, script_args(&tasks), &session, "meta".into())
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
                let owned = crate::lease::Owned::new(snapshot, crate::lease::Owner::new(lease));
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
        let ((task, _, _), _) =
            crate::codemode::spawn_script(&tasks, script_args(&tasks), &session, "meta".into())
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
                let owned = crate::lease::Owned::new(snapshot, crate::lease::Owner::new(lease));
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
        let ((task, _, _), _) = crate::codemode::spawn_script(
            &tasks,
            crate::codemode::ScriptSpawnArgs {
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

    #[cfg(unix)]
    #[test]
    fn host_loss_publication_waits_for_list_classification_and_snapshot() {
        let (_runtime, tasks, session) = fixture(Limits::OFF);
        let (task, _) = make_synthetic_process_task(
            &tasks,
            &session,
            "host lost after root exit",
            None,
            Some(finished_final(false)),
            false,
        );
        let (newer, _) = make_synthetic_process_task(
            &tasks,
            &session,
            "newer finished task",
            None,
            Some(finished_final(false)),
            true,
        );
        let (child, mut drop_signal) = process::Child::failed_host_for_test();
        let (dropped, drop_seen) = mpsc::channel();
        let observer = std::thread::spawn(move || {
            let _ = dropped.send(drop_signal.read(&mut [0]));
        });
        let workers = Workers::new(false).unwrap();
        let (confirmed, completion) = mpsc::channel();
        let mut work = Some(EndWork {
            shared: Arc::clone(&tasks),
            task: Arc::clone(&task),
            child,
            script: tasks.dir.join("unconfirmed-script.sh"),
            exit_status: None,
            confirmed: Some(confirmed),
        });
        let listed = tasks
            .listed(
                &session,
                ListQuery {
                    all_scope: true,
                    active_filter: Some(false),
                    parent_task_id: None,
                    limit: 20,
                },
                &mut |_| {
                    if let Some(work) = work.take() {
                        // Both rows are classified; the newer row's snapshot precedes the lost host's.
                        workers.ender.send(work).unwrap();
                        assert_eq!(
                            drop_seen
                                .recv_timeout(Duration::from_secs(5))
                                .unwrap()
                                .unwrap(),
                            0,
                        );
                    }
                    Ok(())
                },
                &mut |tree| Ok(tree.members()),
            )
            .unwrap()
            .unwrap();
        drop(workers);
        completion.recv_timeout(Duration::from_secs(5)).unwrap();
        observer.join().unwrap();

        assert!(work.is_none());
        assert_eq!(listed.total_count, 2);
        assert_eq!(listed.items[0].task.id, newer.id);
        assert_eq!(listed.items[1].task.id, task.id);
        assert!(
            !listed.items[1].snapshot.kept,
            "host loss changed a row after it was classified as finished"
        );
        assert!(listed.items[1].snapshot.kill_error.is_none());

        let active = tasks
            .listed(
                &session,
                ListQuery {
                    all_scope: true,
                    active_filter: Some(true),
                    parent_task_id: None,
                    limit: 20,
                },
                &mut |_| Ok(()),
                &mut |tree| Ok(tree.members()),
            )
            .unwrap()
            .unwrap();
        assert_eq!(active.total_count, 1);
        assert_eq!(active.items[0].task.id, task.id);
        let snapshot = &active.items[0].snapshot;
        assert!(snapshot.kept && !snapshot.tree_ended && !snapshot.held_known);
        assert!(
            snapshot
                .kill_error
                .as_deref()
                .is_some_and(|error| error.contains("unconfirmed"))
        );
        tasks.shutdown();
    }

    #[test]
    fn host_loss_holds_the_task_before_or_after_the_root_result() {
        let (runtime, tasks, session) = fixture(Limits {
            max_running: 1,
            ..Limits::OFF
        });
        let ((task, _, _), _) =
            crate::codemode::spawn_script(&tasks, script_args(&tasks), &session, "1234".into())
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
            assert!(
                matches!(tasks.admit(false), Err(TaskError::Overload(message)) if message.contains("TOO_MANY_TASKS"))
            );
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
        let ((pending, _, _), _) =
            crate::codemode::spawn_script(&tasks, script_args(&tasks), &session, "1234".into())
                .unwrap();
        wait(&pending);
        pending.tree_done.send_replace(false);
        assert!(!pending.is_running() && !pending.keeps_processes());
        assert!(!pending.is_finished());

        let ((finished, _, _), _) =
            crate::codemode::spawn_script(&tasks, script_args(&tasks), &session, "5678".into())
                .unwrap();
        wait(&finished);
        assert!(
            tasks.find(&pending.id).is_some(),
            "pending trees do not retire"
        );
        assert!(!pending.evicted());

        let (_, admitted) = tasks.admit(false).unwrap();
        assert!(
            matches!(tasks.admit(false), Err(TaskError::Overload(message)) if message.contains("TOO_MANY_TASKS"))
        );
        drop(admitted);

        tasks.expire_finished(Instant::now());
        assert!(tasks.find(&finished.id).is_none());
        assert!(
            tasks.find(&pending.id).is_some(),
            "pending trees do not expire"
        );
        assert!(!tasks.reserve(tasks.quota.log_limit + 1));
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
        let ((task, _, _), _) =
            crate::codemode::spawn_script(&tasks, script_args(&tasks), &session, "1234".into())
                .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
                .await
                .unwrap()
                .unwrap();
        });
        let before = tasks.quota.log_bytes.load(Ordering::SeqCst);
        let first = tasks
            .store_attachment(&task, MediaKind::Image, "image/png", b"one")
            .unwrap();
        assert_eq!(first.index, 1);
        assert_eq!(std::fs::read(&first.path).unwrap(), b"one");
        let second = tasks
            .store_attachment(&task, MediaKind::Document, "application/pdf", b"two")
            .unwrap();
        assert_eq!(second.index, 2);
        assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), before + 6);
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
        assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 3);
        drop(pinned);
        assert!(!second.path.exists());
        assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 0);
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
        let ((task1, _, _), _) =
            crate::codemode::spawn_script(&tasks, script_args(&tasks), &session, "0001".into())
                .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task1.done().wait_for(|done| *done))
                .await
                .unwrap()
                .unwrap();
        });
        let baseline = tasks.quota.log_bytes.load(Ordering::SeqCst);
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
        assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 994);

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
        assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 994);

        drop(pin);
        assert!(att1.path.exists());
        assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 994);

        assert!(!tasks.reserve(40), "must not admit past 1024-byte limit");
        assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 994);

        fail_deletes.store(false, Ordering::SeqCst);
        assert!(tasks.reserve(40), "freed pending delete makes room");
        assert!(!att1.path.exists());
        assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 40);

        tasks.retry_pending_deletes();
        assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 40);
        tasks.quota.log_bytes.fetch_sub(40, Ordering::SeqCst);
        tasks.set_remove_hook(None);
        tasks.shutdown();
    }

    #[test]
    fn per_task_attachment_cap_stops_at_1024_before_creating_extra_files() {
        let (runtime, tasks, session) = fixture(Limits::OFF);
        let ((task, _, _), _) =
            crate::codemode::spawn_script(&tasks, script_args(&tasks), &session, "cap1".into())
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
        let script = Arc::new(super::script::ScriptControl::new(std::thread::current()));
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
            parent_task_id: None,
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
        *lock(&tasks.quota.log_write_hook) = Some(Arc::new(|file, bytes| {
            file.write_all(&bytes[..bytes.len().min(2)])?;
            Err(std::io::Error::other("injected partial write"))
        }));
        *lock(&tasks.quota.log_truncate_hook) = rollback_fails.then(|| {
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
            assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), charge);
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
            assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), charge);
            delayed.store(false, Ordering::SeqCst);
            tasks.retry_pending_deletes();
            assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 0);
            tasks.retry_pending_deletes();
            assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 0);
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
                assert!(pty.spool.write(b"ok\n", true, &task, &tasks.quota, &*tasks));
            }
            partial_log_write(&tasks, rollback_fails);
            {
                let mut pty = lock(task.terminal.as_ref().unwrap());
                assert!(
                    !pty.spool
                        .write(b"tail\n", false, &task, &tasks.quota, &*tasks)
                );
                pty.spool.append("later\n", &task, &tasks.quota, &*tasks);
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
            assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), charge);
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
            assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), charge);
            delayed.store(false, Ordering::SeqCst);
            tasks.retry_pending_deletes();
            assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 0);
            tasks.retry_pending_deletes();
            assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 0);
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
                    assert!(pty.spool.write(b"ok\n", true, &task, &tasks.quota, &*tasks));
                    None
                } else {
                    Some(seed_capture_prefix(&tasks, &task))
                };
                partial_log_write(&tasks, rollback_fails);
                let delayed = delay_log_delete(&tasks);
                let (writing, started) = mpsc::channel();
                let observed = Arc::clone(&task);
                *lock(&tasks.quota.log_write_hook) = Some(Arc::new(move |file, bytes| {
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
                    assert!(
                        !pty.spool
                            .write(b"tail\n", true, &task, &tasks.quota, &*tasks)
                    );
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
                assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), charge);
                assert_eq!(
                    lock(&tasks.quota.pending_deletes)
                        .iter()
                        .map(|entry| entry.bytes)
                        .sum::<u64>(),
                    charge
                );
                delayed.store(false, Ordering::SeqCst);
                tasks.retry_pending_deletes();
                assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 0);
                tasks.retry_pending_deletes();
                assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 0);
                tasks.shutdown();
            }
        }
    }

    #[test]
    fn input_capacity_is_overload_and_uncertain_delivery_is_io() {
        let (runtime, tasks, _session) = fixture(Limits::OFF);
        let task = finished_pty_task(&tasks, 12, 3);
        let (sender, receiver) = mpsc::channel();
        *lock(&task.input) = Some(sender);
        task.queued.store(INPUT_QUEUE_LIMIT, Ordering::SeqCst);
        let cancel = CancellationToken::new();
        let refused = runtime
            .block_on(task.send(Some(b"x"), false, &cancel, Default::default()))
            .unwrap_err();
        assert!(matches!(&refused, TaskError::Overload(_)), "{refused}");
        assert_eq!(task.queued.load(Ordering::SeqCst), INPUT_QUEUE_LIMIT);
        assert!(matches!(
            receiver.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        assert!(lock(&task.input_error).is_none());

        task.queued.store(0, Ordering::SeqCst);
        cancel.cancel();
        let uncertain = runtime
            .block_on(task.send(Some(b"x"), false, &cancel, Default::default()))
            .unwrap_err();
        assert!(matches!(&uncertain, TaskError::Io(_)), "{uncertain}");
        assert!(uncertain.message().contains("INPUT_DELIVERY_UNCONFIRMED"));
        assert_eq!(
            lock(&task.input_error).as_deref(),
            Some(uncertain.message())
        );
        let queued = receiver.try_recv().unwrap();
        assert_eq!(queued.bytes, b"x");
        task.queued.fetch_sub(queued.bytes.len(), Ordering::SeqCst);
        drop(queued);
        tasks.shutdown();
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
        let TaskError::Overload(refused) = task.screen(&memory).err().unwrap() else {
            panic!("screen snapshot capacity must be Overload");
        };
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
        let quota_before = tasks.quota.log_bytes.load(Ordering::SeqCst);
        assert!(
            matches!(task.transcript(&tasks, &state, &memory), Err(TaskError::Overload(message)) if message.contains("memory budget"))
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
        assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), quota_before);

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
            assert!(
                pty.spool
                    .write(b"first-line\n", true, &task, &tasks.quota, &*tasks)
            );
        }
        assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 11);

        // Hold `task.terminal` (exact state of `capture` with open output or `resize`) and write
        // 60 more bytes (11 + 60 > 64), forcing `reserve` -> `evict_log(&task)` on this thread!
        {
            let mut pty = lock(task.terminal.as_ref().unwrap());
            assert!(
                !pty.spool
                    .write(&[b'z'; 60], true, &task, &tasks.quota, &*tasks)
            );
        }
        assert!(task.evicted());
        assert!(lock(&task.transcript_file).is_none());
        assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 0);

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
            tasks.quota.log_bytes.load(Ordering::SeqCst),
            20,
            "failed unlink must keep transcript bytes charged"
        );
        fail_deletes.store(false, Ordering::SeqCst);
        tasks.retry_pending_deletes();
        assert!(!task2.transcript_path().exists());
        assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 0);
        tasks.set_remove_hook(None);
        tasks.shutdown();
    }

    #[test]
    fn transcript_rechecks_eviction_when_spool_write_races_deferred_delete() {
        // 1. Self-eviction inside `Spool::write -> reserve` while unlink is deferred.
        let (_runtime, mut tasks, _session) = fixture(Limits::OFF);
        Arc::get_mut(&mut tasks).unwrap().set_log_limit(64);
        let task = finished_pty_task(&tasks, 80, 24);
        {
            let mut pty = lock(task.terminal.as_ref().unwrap());
            assert!(
                pty.spool
                    .write(b"first-line\n", true, &task, &tasks.quota, &*tasks)
            );
            pty.live.process(
                b"pending-tail-that-crosses-64-byte-total-log-limit-here",
                Instant::now(),
                None,
            );
        }
        let delayed = delay_log_delete(&tasks);
        let memory = BytePool::new(16 << 20);
        let err = task
            .transcript(&tasks, &ReadState::default(), &memory)
            .err()
            .expect("eviction during spool.write must fail instead of dropping pending lines");
        assert!(
            matches!(err, TaskError::Io(ref msg) if msg == EVICTED_TRANSCRIPT),
            "{err:?}"
        );
        assert!(task.transcript_path().exists());
        delayed.store(false, Ordering::SeqCst);
        tasks.retry_pending_deletes();
        assert!(!task.transcript_path().exists());
        assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 0);
        tasks.set_remove_hook(None);
        tasks.shutdown();

        // 2. Concurrent `evict_log` mid-write before `shared.hold` while unlink is deferred.
        let (_runtime, tasks, _session) = fixture(Limits::OFF);
        let task = finished_pty_task(&tasks, 80, 24);
        {
            let mut pty = lock(task.terminal.as_ref().unwrap());
            assert!(
                pty.spool
                    .write(b"saved-line\n", true, &task, &tasks.quota, &*tasks)
            );
            pty.live.process(b"live-screen-tail", Instant::now(), None);
        }
        let delayed = delay_log_delete(&tasks);
        let evicting = Arc::clone(&tasks);
        let target = Arc::clone(&task);
        *lock(&tasks.quota.log_write_hook) = Some(Arc::new(move |file, bytes| {
            file.write_all(bytes)?;
            let prior = {
                let mut out = lock(&target.out);
                out.evicted = true;
                out.transcript
            };
            evicting.reclaim_file(target.transcript_path(), prior);
            Ok(())
        }));
        let err = task
            .transcript(&tasks, &ReadState::default(), &memory)
            .err()
            .expect("concurrent evict_log during spool.write must report EVICTED_TRANSCRIPT");
        assert!(
            matches!(err, TaskError::Io(ref msg) if msg == EVICTED_TRANSCRIPT),
            "{err:?}"
        );
        assert!(task.transcript_path().exists());
        *lock(&tasks.quota.log_write_hook) = None;
        delayed.store(false, Ordering::SeqCst);
        tasks.retry_pending_deletes();
        assert!(!task.transcript_path().exists());
        assert_eq!(tasks.quota.log_bytes.load(Ordering::SeqCst), 0);
        tasks.set_remove_hook(None);
        tasks.shutdown();
    }

    #[test]
    fn eof_only_poll_after_initial_broken_pipe_stays_idempotent_and_huge_deadline_is_safe() {
        let (runtime, tasks, session) = fixture(Limits::OFF);
        let mut args = script_args(&tasks);
        args.kill_after_ms = Some(u64::MAX);
        let ((task, _, _), _) =
            crate::codemode::spawn_script(&tasks, args, &session, "r001".into()).unwrap();
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
        let ((task, _, _), _) =
            crate::codemode::spawn_script(&tasks, args, &session, "ev01".into()).unwrap();
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

        let before = tasks.quota.log_limit - 97;
        tasks.quota.log_bytes.store(before, Ordering::SeqCst);
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
            tasks.quota.log_bytes.load(Ordering::SeqCst),
            before - written_before
        );
        drop(out);
        tasks.shutdown();
    }

    #[test]
    fn has_warnings_unlocked_matches_task_warnings_conditions() {
        let (_runtime, tasks, _session) = fixture(Limits::OFF);
        let task = finished_pty_task(&tasks, 80, 24);
        // `finished_pty_task` initializes `output_open = true` with `out.closed = false`.
        assert!(task.has_warnings_unlocked());
        lock(&task.end).as_mut().unwrap().output_open = false;
        assert!(!task.has_warnings_unlocked());

        // capture_error
        task.inject_io_errors(Some("read boom"), None);
        assert!(task.has_warnings_unlocked());
        task.inject_io_errors(None, None);
        assert!(!task.has_warnings_unlocked());

        // log_error
        task.inject_io_errors(None, Some("disk full"));
        assert!(task.has_warnings_unlocked());
        task.inject_io_errors(None, None);
        assert!(!task.has_warnings_unlocked());

        // dropped bytes
        lock(&task.out).dropped = 12;
        assert!(task.has_warnings_unlocked());
        lock(&task.out).dropped = 0;
        assert!(!task.has_warnings_unlocked());

        // evicted
        lock(&task.out).evicted = true;
        assert!(task.has_warnings_unlocked());
        lock(&task.out).evicted = false;
        assert!(!task.has_warnings_unlocked());

        // output_open requires !out.closed
        lock(&task.end).as_mut().unwrap().output_open = true;
        lock(&task.out).closed = true;
        assert!(!task.has_warnings_unlocked());
        lock(&task.out).closed = false;
        assert!(task.has_warnings_unlocked());
        lock(&task.end).as_mut().unwrap().output_open = false;
        assert!(!task.has_warnings_unlocked());

        // kill_error only warns while !tree_done
        *lock(&task.kill_error) = Some("kill failed".into());
        assert!(!task.has_warnings_unlocked());
        task.tree_done.send_replace(false);
        assert!(task.has_warnings_unlocked());
        task.tree_done.send_replace(true);
        *lock(&task.kill_error) = None;
        assert!(!task.has_warnings_unlocked());

        // input_warning and input_error
        *lock(&task.input_warning) = Some("canonical".into());
        assert!(task.has_warnings_unlocked());
        *lock(&task.input_warning) = None;
        *lock(&task.input_error) = Some("write failed".into());
        assert!(task.has_warnings_unlocked());
        *lock(&task.input_error) = None;
        assert!(!task.has_warnings_unlocked());

        tasks.shutdown();
    }

    #[test]
    fn background_skips_running_children_of_running_codemode_parent() {
        let (_runtime, tasks, session) = fixture(Limits::OFF);
        let make_running = |parent_task_id: Option<String>| -> Arc<Task> {
            let id = tasks.next_id();
            let log_path = tasks.dir.join(format!("{id}.log"));
            let _log = private_file(&log_path).unwrap();
            let script = Arc::new(super::script::ScriptControl::new(std::thread::current()));
            Arc::new(Task {
                id,
                command: "sleep 30".into(),
                pty: false,
                log_path,
                encoding: encoding_rs::UTF_8,
                keep: false,
                owner: "test-client".into(),
                session_id: Some(session.id.clone()),
                started_by: None,
                parent_task_id,
                started: Instant::now(),
                root: Root::Script(script),
                tree_done: watch::Sender::new(false),
                tree_ended_at: OnceLock::new(),
                kill_error: Mutex::new(None),
                kill_requested: AtomicBool::new(false),
                expired: AtomicBool::new(false),
                out: Mutex::default(),
                end: Mutex::new(None),
                done: watch::Sender::new(false),
                activity: watch::Sender::new(()),
                input: Mutex::new(None),
                writer_done: WriterDone::new(true),
                input_warning: Mutex::new(None),
                input_error: Mutex::new(None),
                queued: AtomicUsize::new(0),
                input_gate: tokio::sync::Mutex::new(()),
                terminal: None,
                transcript_file: Mutex::new(None),
                size: Mutex::new((0, 0)),
                pty_control: Mutex::new(None),
                size_error: Mutex::new(None),
            })
        };
        let parent = make_running(None);
        let child = make_running(Some(parent.id.clone()));
        {
            let mut registry = lock(&tasks.registry);
            let _ = session.entry(&parent);
            let _ = session.entry(&child);
            registry.tasks.push(Arc::clone(&parent));
            registry.tasks.push(Arc::clone(&child));
        }

        // Polling the running parent must not surface its own running child in background.
        let (notice, claim) = tasks
            .background(&session, &parent.id, 512, &mut |_| Ok(()), &mut |_| {
                Ok(Some(Vec::new()))
            })
            .unwrap();
        drop(claim);
        assert!(
            notice.is_none(),
            "running child leaked into footer: {:?}",
            notice.as_ref().map(|(line, _)| line)
        );

        // Once the parent is killed and finishes, the surviving running child surfaces.
        *lock(&parent.end) = Some(Final {
            exit_code: None,
            killed: true,
            expired: false,
            forced_end: false,
            elapsed: Duration::from_millis(50),
            output_open: false,
            leftovers: Vec::new(),
            kept: false,
        });
        parent.tree_done.send_replace(true);
        let (notice, _claim) = tasks
            .background(&session, &parent.id, 512, &mut |_| Ok(()), &mut |_| {
                Ok(Some(Vec::new()))
            })
            .unwrap();
        let (line, shown) = notice.expect("surviving child should appear after parent ends");
        assert!(line.contains(&child.id), "expected child id in {line}");
        assert_eq!(shown.len(), 1);
        assert_eq!(shown[0].0.id, child.id);

        tasks.shutdown();
    }

    fn make_synthetic_task(
        tasks: &Tasks,
        session: &Session,
        command: &str,
        parent_task_id: Option<String>,
        end: Option<Final>,
        tree_done: bool,
    ) -> (Arc<Task>, Arc<TaskEntry>) {
        let script = Arc::new(super::script::ScriptControl::new(std::thread::current()));
        make_synthetic_task_with_root(
            tasks,
            session,
            command,
            parent_task_id,
            end,
            tree_done,
            Root::Script(script),
        )
    }

    fn make_synthetic_process_task(
        tasks: &Tasks,
        session: &Session,
        command: &str,
        parent_task_id: Option<String>,
        end: Option<Final>,
        tree_done: bool,
    ) -> (Arc<Task>, Arc<TaskEntry>) {
        make_synthetic_task_with_root(
            tasks,
            session,
            command,
            parent_task_id,
            end,
            tree_done,
            Root::Process(process::Tree::synthetic_for_test()),
        )
    }

    fn make_synthetic_task_with_root(
        tasks: &Tasks,
        session: &Session,
        command: &str,
        parent_task_id: Option<String>,
        end: Option<Final>,
        tree_done: bool,
        root: Root,
    ) -> (Arc<Task>, Arc<TaskEntry>) {
        let id = tasks.next_id();
        let log_path = tasks.dir.join(format!("{id}.log"));
        let _log = private_file(&log_path).unwrap();
        let task = Arc::new(Task {
            id,
            command: command.into(),
            pty: false,
            log_path,
            encoding: encoding_rs::UTF_8,
            keep: end.as_ref().is_some_and(|final_state| final_state.kept),
            owner: session.label.clone(),
            session_id: Some(session.id.clone()),
            started_by: None,
            parent_task_id,
            started: Instant::now(),
            root,
            tree_done: watch::Sender::new(tree_done),
            tree_ended_at: OnceLock::new(),
            kill_error: Mutex::new(None),
            kill_requested: AtomicBool::new(false),
            expired: AtomicBool::new(false),
            out: Mutex::default(),
            end: Mutex::new(end),
            done: watch::Sender::new(true),
            activity: watch::Sender::new(()),
            input: Mutex::new(None),
            writer_done: WriterDone::new(true),
            input_warning: Mutex::new(None),
            input_error: Mutex::new(None),
            queued: AtomicUsize::new(0),
            input_gate: tokio::sync::Mutex::new(()),
            terminal: None,
            transcript_file: Mutex::new(None),
            size: Mutex::new((0, 0)),
            pty_control: Mutex::new(None),
            size_error: Mutex::new(None),
        });
        let entry = {
            let mut registry = lock(&tasks.registry);
            let entry = session.entry(&task);
            registry.tasks.push(Arc::clone(&task));
            entry
        };
        (task, entry)
    }

    fn finished_final(kept: bool) -> Final {
        Final {
            exit_code: Some(0),
            killed: false,
            expired: false,
            forced_end: false,
            elapsed: Duration::from_millis(10),
            output_open: false,
            leftovers: Vec::new(),
            kept,
        }
    }

    fn test_exec_context(session: &Arc<Session>) -> crate::service::ExecContext<'_> {
        crate::service::ExecContext {
            session,
            launch: None,
            cancel: CancellationToken::new(),
            progress: None,
            call_id: None,
            parent_task_id: None,
            queued_guard: None,
            on_started: None,
            input_owner: Default::default(),
        }
    }

    #[test]
    fn evicted_parent_promotes_child_and_preserves_parent_task_id_drill_down() {
        let (runtime, tasks, session) = fixture(Limits::OFF);
        let service = Arc::new(crate::service::Service::new(Arc::clone(&tasks), false));
        let (parent, _) = make_synthetic_task(
            &tasks,
            &session,
            "[codemode] parent()",
            None,
            Some(finished_final(false)),
            true,
        );
        let (child, _) = make_synthetic_task(
            &tasks,
            &session,
            "echo orphan-child",
            Some(parent.id.clone()),
            Some(finished_final(false)),
            true,
        );

        // Simulate retire_finished evicting the finished parent while keeping its child.
        lock(&tasks.registry)
            .tasks
            .retain(|task| task.id != parent.id);

        runtime.block_on(async {
            let default_list = crate::render::exec_agent_json(
                &service,
                serde_json::json!({ "action": "list" }),
                test_exec_context(&session),
                crate::render::types::AgentDelivery::default(),
            )
            .await
            .result;
            assert_ne!(default_list.is_error, Some(true), "{default_list:?}");
            assert!(default_list.structured_content.is_none());
            let text = default_list.content[0].as_text().unwrap().text.as_str();
            assert!(
                text.contains(&format!("via {}", parent.id)) && text.contains(&child.id),
                "expected promoted orphan with via <pid>: {text}"
            );
            assert_eq!(text.lines().count(), 1, "expected 1 listed task:\n{text}");

            let drill = crate::render::exec_agent_json(
                &service,
                serde_json::json!({ "action": "list", "parentTaskId": parent.id }),
                test_exec_context(&session),
                crate::render::types::AgentDelivery::default(),
            )
            .await
            .result;
            assert_ne!(drill.is_error, Some(true), "{drill:?}");
            assert!(drill.structured_content.is_none());
            let drill_text = drill.content[0].as_text().unwrap().text.as_str();
            assert!(
                drill_text.contains(&child.id) && drill_text.lines().count() == 1,
                "expected drilled child in text: {drill_text}"
            );

            let missing = crate::render::exec_agent_json(
                &service,
                serde_json::json!({ "action": "list", "parentTaskId": "zzzzzzzz-99" }),
                test_exec_context(&session),
                crate::render::types::AgentDelivery::default(),
            )
            .await
            .result;
            assert_eq!(missing.is_error, Some(true), "{missing:?}");
            assert!(missing.structured_content.is_none());
            let missing_text = missing.content[0].as_text().unwrap().text.as_str();
            assert!(
                missing_text.contains("Unknown parentTaskId"),
                "expected Unknown parentTaskId error: {missing_text}"
            );
        });

        tasks.shutdown();
    }

    #[test]
    fn list_bottom_up_trim_and_outbound_rejection_keep_mark_reported_consistent() {
        let (runtime, tasks, session) = fixture(Limits::OFF);
        let service = Arc::new(crate::service::Service::new(Arc::clone(&tasks), false));
        let (parent, parent_entry) = make_synthetic_task(
            &tasks,
            &session,
            "[codemode] parent()",
            None,
            Some(finished_final(false)),
            true,
        );
        let (_folded_plain, folded_plain_entry) = make_synthetic_task(
            &tasks,
            &session,
            "echo plain",
            Some(parent.id.clone()),
            Some(finished_final(false)),
            true,
        );
        let (_folded_kept, folded_kept_entry) = make_synthetic_task(
            &tasks,
            &session,
            "sleep 0.1 &",
            Some(parent.id.clone()),
            Some(finished_final(true)),
            true,
        );
        // Active kept children: started older first so active_bottom sits at the bottom.
        let (active_bottom, active_bottom_entry) = make_synthetic_task(
            &tasks,
            &session,
            "sleep 30 & # bottom-child-padding-0123456789abcdef0123456789abcdef0123456789abcdef",
            Some(parent.id.clone()),
            Some(finished_final(true)),
            false,
        );
        let (active_top, active_top_entry) = make_synthetic_task(
            &tasks,
            &session,
            "sleep 30 & # top-child-padding-0123456789abcdef0123456789",
            Some(parent.id.clone()),
            Some(finished_final(true)),
            false,
        );

        runtime.block_on(async {
            // Outbound frame rejection must leave parent, active children, and folded children unreported.
            let small_frame = Arc::new(session.launch.for_session().with_outbound_limit(512));
            let mut refused_ctx = test_exec_context(&session);
            refused_ctx.launch = Some(&small_frame);
            let refused = crate::render::exec_agent_json(
                &service,
                serde_json::json!({ "action": "list" }),
                refused_ctx,
                crate::render::types::AgentDelivery::default(),
            )
            .await;
            assert!(refused.overload, "{:?}", refused.result);
            assert!(refused.result.structured_content.is_none());
            assert!(
                refused.result.content[0]
                    .as_text()
                    .unwrap()
                    .text
                    .contains("frame limit")
            );
            assert!(!parent_entry.is_reported());
            assert!(!active_top_entry.is_reported());
            assert!(!active_bottom_entry.is_reported());
            assert!(!folded_plain_entry.is_reported());
            assert!(!folded_kept_entry.is_reported());
            assert!(!folded_kept_entry.tree_end_reported());
            drop(refused);

            // Budget sized so parent + active_top fit while active_bottom is trimmed from the bottom.
            let trimmed = crate::render::exec_agent_json(
                &service,
                serde_json::json!({ "action": "list", "maxBytes": 230 }),
                test_exec_context(&session),
                crate::render::types::AgentDelivery::default(),
            )
            .await
            .result;
            assert_ne!(trimmed.is_error, Some(true), "{trimmed:?}");
            assert!(trimmed.structured_content.is_none());
            let text = trimmed.content[0].as_text().unwrap().text.as_str();
            assert!(text.contains(&parent.id), "missing parent in:\n{text}");
            assert!(
                text.contains(&active_top.id),
                "missing surviving top active child in:\n{text}"
            );
            assert!(
                !text.contains(&active_bottom.id),
                "bottom active child should have been trimmed:\n{text}"
            );

            assert!(parent_entry.is_reported());
            assert!(active_top_entry.is_reported());
            assert!(
                !active_bottom_entry.is_reported(),
                "trimmed bottom active child must remain unreported"
            );
            assert!(folded_plain_entry.is_reported());
            assert!(!folded_plain_entry.tree_end_reported());
            assert!(folded_kept_entry.is_reported());
            assert!(folded_kept_entry.tree_end_reported());
        });

        tasks.shutdown();
    }

    #[test]
    fn cross_session_parent_task_id_respects_scope_without_creating_session_entries() {
        let (runtime, tasks, session_a) = fixture(Limits::OFF);
        let service = Arc::new(crate::service::Service::new(Arc::clone(&tasks), false));
        let session_b = Session::new(
            "session-b".into(),
            "session-b".into(),
            Arc::clone(&session_a.launch),
        );

        let (parent, _) = make_synthetic_task(
            &tasks,
            &session_a,
            "[codemode] parent()",
            None,
            Some(finished_final(false)),
            true,
        );
        let (child_a, child_a_entry) = make_synthetic_task(
            &tasks,
            &session_a,
            "echo from-a",
            Some(parent.id.clone()),
            Some(finished_final(false)),
            true,
        );
        let (child_b, child_b_entry) = make_synthetic_task(
            &tasks,
            &session_b,
            "echo from-b",
            Some(parent.id.clone()),
            Some(finished_final(false)),
            true,
        );
        assert!(session_b.existing(&parent).is_none());
        assert!(session_b.existing(&child_a).is_none());

        runtime.block_on(async {
            // Default scope with parentTaskId is "all": Session B sees Session A's child without
            // creating session entries in Session B for cross-session tasks.
            let all_view = crate::render::exec_agent_json(
                &service,
                serde_json::json!({ "action": "list", "parentTaskId": parent.id }),
                test_exec_context(&session_b),
                crate::render::types::AgentDelivery::default(),
            )
            .await
            .result;
            assert_ne!(all_view.is_error, Some(true), "{all_view:?}");
            assert!(all_view.structured_content.is_none());
            let all_text = all_view.content[0].as_text().unwrap().text.as_str();
            assert_eq!(all_text.lines().count(), 2, "{all_text}");
            assert!(all_text.contains(&child_a.id), "{all_text}");
            assert!(all_text.contains(&child_b.id), "{all_text}");
            assert!(session_b.existing(&parent).is_none());
            assert!(session_b.existing(&child_a).is_none());
            assert!(!child_a_entry.is_reported());
            assert!(child_b_entry.is_reported());

            // Explicit scope: "session" restricts to Session B's own children.
            let session_view = crate::render::exec_agent_json(
                &service,
                serde_json::json!({
                    "action": "list",
                    "parentTaskId": parent.id,
                    "scope": "session"
                }),
                test_exec_context(&session_b),
                crate::render::types::AgentDelivery::default(),
            )
            .await
            .result;
            assert_ne!(session_view.is_error, Some(true), "{session_view:?}");
            assert!(session_view.structured_content.is_none());
            let session_text = session_view.content[0].as_text().unwrap().text.as_str();
            assert_eq!(session_text.lines().count(), 1, "{session_text}");
            assert!(session_text.contains(&child_b.id), "{session_text}");
            assert!(!session_text.contains(&child_a.id), "{session_text}");
            assert!(session_b.existing(&parent).is_none());
            assert!(session_b.existing(&child_a).is_none());
        });

        tasks.shutdown();
    }

    #[test]
    fn list_single_block_narrow_budget_tiers_and_ack_conservation() {
        let (_runtime, tasks, session) = fixture(Limits::OFF);
        let service = Arc::new(crate::service::Service::new(Arc::clone(&tasks), false));
        let make_held_snapshot = |task: &Task, members: Option<Vec<Member>>| {
            let mut snap = task.notice_snapshot();
            crate::tasks::state::set_held(&mut snap, members);
            snap
        };

        // 1. 12 processes + maxBytes: 100 -> Tier 4 (`keeps <min-pid-name> +11` + ` … [cut]`),
        // records keeps_reported, and out-of-order members render identically.
        let members_ordered: Vec<Member> = (101..=112)
            .map(|pid| Member {
                pid,
                name: format!("worker-{pid}"),
            })
            .collect();
        let mut members_shuffled = members_ordered.clone();
        members_shuffled.reverse();
        let expected_pids: Vec<u32> = (101..=112).collect();

        let (task_a, entry_a) = make_synthetic_task(
            &tasks,
            &session,
            "sleep 300 & # padding-for-narrow-budget-test",
            None,
            Some(finished_final(true)),
            false,
        );
        let snap_a = make_held_snapshot(&task_a, Some(members_ordered.clone()));
        let text_a = crate::render::render_list_items_for_test(
            &service,
            vec![ListedTask {
                task: Arc::clone(&task_a),
                snapshot: snap_a,
                parent_task_id: None,
                children: None,
            }],
            100,
            test_exec_context(&session),
        );
        assert!(
            text_a.len() <= 100,
            "over budget ({}): {text_a}",
            text_a.len()
        );
        assert!(
            text_a.contains(" · keeps worker-101 +11"),
            "expected Tier 4 brief: {text_a}"
        );
        assert!(text_a.ends_with(" … [cut]"), "missing cut marker: {text_a}");
        assert!(entry_a.is_reported());
        assert!(entry_a.keeps_reported_eq(&expected_pids));

        // Out-of-order input yields the exact same text when normalized via set_held.
        let snap_shuffled = make_held_snapshot(&task_a, Some(members_shuffled));
        let text_shuffled = crate::render::render_list_items_for_test(
            &service,
            vec![ListedTask {
                task: Arc::clone(&task_a),
                snapshot: snap_shuffled,
                parent_task_id: None,
                children: None,
            }],
            100,
            test_exec_context(&session),
        );
        assert_eq!(text_a, text_shuffled);

        // Boundary A: candidate_full.len() <= budget after replacing tree_full with tree_brief
        // must still append ` … [cut]` and fit within budget.
        let (task_short_cmd, entry_short_cmd) = make_synthetic_task(
            &tasks,
            &session,
            "a",
            None,
            Some(finished_final(true)),
            false,
        );
        let snap_short = make_held_snapshot(&task_short_cmd, Some(members_ordered.clone()));
        let full_short = crate::render::render_list_items_for_test(
            &service,
            vec![ListedTask {
                task: Arc::clone(&task_short_cmd),
                snapshot: snap_short.clone(),
                parent_task_id: None,
                children: None,
            }],
            4096,
            test_exec_context(&session),
        );
        // Pick budget smaller than `head + tree_full + 10` (so Tiers 1-3 fail) yet large enough
        // that `candidate_full(tree_brief).len() + 10 <= budget`.
        let budget_tier4_short = 90;
        assert!(budget_tier4_short < full_short.len(), "{full_short}");
        let text_boundary_a = crate::render::render_list_items_for_test(
            &service,
            vec![ListedTask {
                task: Arc::clone(&task_short_cmd),
                snapshot: snap_short,
                parent_task_id: None,
                children: None,
            }],
            budget_tier4_short,
            test_exec_context(&session),
        );
        assert!(
            text_boundary_a.len() <= budget_tier4_short,
            "{text_boundary_a}"
        );
        assert!(
            text_boundary_a.contains(" · keeps worker-101 +11"),
            "{text_boundary_a}"
        );
        assert!(
            text_boundary_a.ends_with(" … [cut]"),
            "candidate_full <= budget must still carry cut marker: {text_boundary_a}"
        );
        assert!(entry_short_cmd.keeps_reported_eq(&expected_pids));

        // Boundary B: core.len() == budget - 10 exact boundary keeps all of core intact.
        let exact_core = format!("{} [exited 0] · keeps worker-101 +11", task_a.id);
        let exact_budget = exact_core.len() + " … [cut]".len();
        let text_boundary_b = crate::render::render_list_items_for_test(
            &service,
            vec![ListedTask {
                task: Arc::clone(&task_a),
                snapshot: make_held_snapshot(&task_a, Some(members_ordered)),
                parent_task_id: None,
                children: None,
            }],
            exact_budget,
            test_exec_context(&session),
        );
        assert_eq!(text_boundary_b, format!("{exact_core} … [cut]"));

        // 2. Long multibyte process name triggers Tier 5 (`keeps 12 processes` + ` … [cut]`)
        // without consuming keeps_reported.
        let mut mb_members: Vec<Member> = (201..=212)
            .map(|pid| Member {
                pid,
                name: format!("p{pid}"),
            })
            .collect();
        mb_members[0].name = "长进程名".repeat(8); // 96 UTF-8 bytes
        let mb_pids: Vec<u32> = (201..=212).collect();
        let (task_mb, entry_mb) = make_synthetic_task(
            &tasks,
            &session,
            "keeps sleep (999) # command text containing keeps must not false-ack",
            None,
            Some(finished_final(true)),
            false,
        );
        let text_mb = crate::render::render_list_items_for_test(
            &service,
            vec![ListedTask {
                task: Arc::clone(&task_mb),
                snapshot: make_held_snapshot(&task_mb, Some(mb_members)),
                parent_task_id: None,
                children: None,
            }],
            100,
            test_exec_context(&session),
        );
        assert!(text_mb.len() <= 100, "{text_mb}");
        assert!(
            text_mb.contains(" · keeps 12 processes"),
            "expected Tier 5 count marker: {text_mb}"
        );
        assert!(text_mb.ends_with(" … [cut]"), "{text_mb}");
        assert!(entry_mb.is_reported());
        assert!(
            !entry_mb.keeps_reported_eq(&mb_pids),
            "Tier 5 count-only cut must not consume keeps_reported"
        );

        // 3. !held_known and empty held set under narrow budget never produce `keeps 0 processes`,
        // and command text containing "keeps " never triggers false keeps_reported.
        let (task_unconf, entry_unconf) = make_synthetic_task(
            &tasks,
            &session,
            "echo keeps fake-process (1) and more padding to force narrow cut 0123456789abcdef",
            None,
            Some(finished_final(true)),
            false,
        );
        let text_unconf = crate::render::render_list_items_for_test(
            &service,
            vec![ListedTask {
                task: Arc::clone(&task_unconf),
                snapshot: make_held_snapshot(&task_unconf, None),
                parent_task_id: None,
                children: None,
            }],
            100,
            test_exec_context(&session),
        );
        assert!(text_unconf.len() <= 100, "{text_unconf}");
        assert!(!text_unconf.contains("keeps 0 processes"), "{text_unconf}");
        assert!(
            text_unconf.contains(" · keeps processes (unconfirmed)"),
            "{text_unconf}"
        );
        assert!(text_unconf.ends_with(" … [cut]"), "{text_unconf}");
        assert!(entry_unconf.is_reported());
        assert!(
            !entry_unconf.keeps_reported_eq(&[]),
            "!held_known must not record keeps_reported even when command contains 'keeps '"
        );

        let (task_empty, _entry_empty) = make_synthetic_task(
            &tasks,
            &session,
            "sleep 30 & # long command padding to force truncation in narrow budget 0123456789abcdef",
            None,
            Some(finished_final(true)),
            false,
        );
        let text_empty = crate::render::render_list_items_for_test(
            &service,
            vec![ListedTask {
                task: Arc::clone(&task_empty),
                snapshot: make_held_snapshot(&task_empty, Some(Vec::new())),
                parent_task_id: None,
                children: None,
            }],
            50,
            test_exec_context(&session),
        );
        assert!(text_empty.len() <= 50, "{text_empty}");
        assert!(!text_empty.contains("keeps 0 processes"), "{text_empty}");

        // 4. Codemode parent with long summary under narrow budget triggers Tier 2:
        // confirms parent root while leaving folded children unreported.
        let (cm_parent, cm_parent_entry) = make_synthetic_task(
            &tasks,
            &session,
            "[codemode] long_orchestrator_padding_0123456789abcdef",
            None,
            Some(finished_final(false)),
            true,
        );
        let (cm_child_plain, cm_child_plain_entry) = make_synthetic_task(
            &tasks,
            &session,
            "echo done",
            Some(cm_parent.id.clone()),
            Some(finished_final(false)),
            true,
        );
        let (cm_child_kept, cm_child_kept_entry) = make_synthetic_task(
            &tasks,
            &session,
            "sleep 0.1 &",
            Some(cm_parent.id.clone()),
            Some(finished_final(true)),
            true,
        );
        let parent_snap = cm_parent.notice_snapshot();
        let summary = ChildSummary {
            total: 4,
            done: 2,
            failed: 1,
            warned: 1,
            tree_ended: 1,
            active: 0,
            queued: 0,
            cancelled: 1,
            active_tasks: Vec::new(),
            folded_finished: vec![
                (Arc::clone(&cm_child_plain), false),
                (Arc::clone(&cm_child_kept), true),
            ],
        };
        // head + elapsed (~30 bytes) + 10 <= budget (48) < head + elapsed + summary (~95 bytes) + 10.
        let text_cm = crate::render::render_list_items_for_test(
            &service,
            vec![ListedTask {
                task: Arc::clone(&cm_parent),
                snapshot: parent_snap,
                parent_task_id: None,
                children: Some(summary),
            }],
            48,
            test_exec_context(&session),
        );
        let cm_first = text_cm.lines().next().unwrap_or("");
        assert!(cm_first.len() <= 48, "{text_cm}");
        assert!(cm_first.ends_with(" … [cut]"), "{text_cm}");
        assert!(cm_parent_entry.is_reported());
        assert!(
            !cm_child_plain_entry.is_reported(),
            "Tier 2 dropped summary so folded plain child must remain unreported"
        );
        assert!(
            !cm_child_kept_entry.is_reported() && !cm_child_kept_entry.tree_end_reported(),
            "Tier 2 dropped summary so folded kept child must remain unreported"
        );

        // 5. Control characters, newlines, and empty process names sanitize to single line + `pid <N>`.
        let (task_ctrl, _entry_ctrl) = make_synthetic_task(
            &tasks,
            &session,
            "echo ok\nline2\x07",
            None,
            Some(finished_final(true)),
            false,
        );
        let ctrl_members = vec![
            Member {
                pid: 301,
                name: "bad\n\t\x07name".into(),
            },
            Member {
                pid: 302,
                name: String::new(),
            },
            Member {
                pid: 303,
                name: " \r\n ".into(),
            },
        ];
        let text_ctrl = crate::render::render_list_items_for_test(
            &service,
            vec![ListedTask {
                task: Arc::clone(&task_ctrl),
                snapshot: make_held_snapshot(&task_ctrl, Some(ctrl_members)),
                parent_task_id: None,
                children: None,
            }],
            4096,
            test_exec_context(&session),
        );
        assert_eq!(
            text_ctrl.lines().count(),
            1,
            "must stay single line: {text_ctrl:?}"
        );
        assert!(
            !text_ctrl.chars().any(char::is_control),
            "control chars leaked: {text_ctrl:?}"
        );
        assert!(
            text_ctrl.contains("keeps bad name (301), pid 302, pid 303"),
            "{text_ctrl}"
        );

        tasks.shutdown();
    }

    #[test]
    fn list_folded_child_summary_shows_kept_ended_and_parent_drilldown() {
        let (runtime, tasks, session) = fixture(Limits::OFF);
        let service = Arc::new(crate::service::Service::new(Arc::clone(&tasks), false));
        let (parent, _) = make_synthetic_task(
            &tasks,
            &session,
            "[codemode] parent()",
            None,
            Some(finished_final(false)),
            true,
        );
        let (_plain, _) = make_synthetic_task(
            &tasks,
            &session,
            "echo plain",
            Some(parent.id.clone()),
            Some(finished_final(false)),
            true,
        );
        let (kept_ended, _) = make_synthetic_task(
            &tasks,
            &session,
            "sleep 0.1 &",
            Some(parent.id.clone()),
            Some(finished_final(true)),
            true,
        );

        runtime.block_on(async {
            let list = crate::render::exec_agent_json(
                &service,
                serde_json::json!({ "action": "list" }),
                test_exec_context(&session),
                crate::render::types::AgentDelivery::default(),
            )
            .await
            .result;
            let text = list.content[0].as_text().unwrap().text.as_str();
            assert!(
                text.contains("2 tasks (2 done, 1 kept ended)"),
                "expected folded kept ended summary: {text}"
            );

            let drill = crate::render::exec_agent_json(
                &service,
                serde_json::json!({ "action": "list", "parentTaskId": parent.id }),
                test_exec_context(&session),
                crate::render::types::AgentDelivery::default(),
            )
            .await
            .result;
            let drill_text = drill.content[0].as_text().unwrap().text.as_str();
            assert!(
                drill_text.contains(&format!(
                    "{} [exited 0] · kept processes ended",
                    kept_ended.id
                )),
                "expected kept processes ended in drilldown: {drill_text}"
            );
        });

        tasks.shutdown();
    }

    #[test]
    fn footer_races_overflow_counts_and_ordered_exited_events() {
        let (_runtime, tasks, session) = fixture(Limits::OFF);
        let bus = Arc::clone(tasks.event_bus());

        // 1. Batch root exit with kept processes counts overflow under `keeping processes`.
        let mut batch_tasks = Vec::new();
        for _ in 0..5 {
            batch_tasks.push(make_synthetic_task(
                &tasks,
                &session,
                "sleep 30 &",
                None,
                Some(finished_final(true)),
                false,
            ));
        }
        let (notice, claim) = tasks
            .background(&session, "", 512, &mut |_| Ok(()), &mut |_| {
                Ok(Some(vec![Member {
                    pid: 555,
                    name: "sleep".into(),
                }]))
            })
            .unwrap();
        drop(claim);
        let (line, _) = notice.expect("expected batch keeping overflow footer");
        assert!(
            line.contains("2 more; 5 keeping processes; use list."),
            "expected keeping processes overflow count: {line}"
        );
        assert!(
            !line.contains("finished"),
            "must not count kept roots as finished: {line}"
        );
        // Mark batch tasks finished so subsequent checks start clean.
        for (task, entry) in &batch_tasks {
            task.tree_done.send_replace(true);
            entry.mark_finished_reported(true);
        }

        // 2. snapshot_held_reserved + tree_done transition never reports `kept` after tree ends.
        let (race_snap_task, race_snap_entry) = make_synthetic_process_task(
            &tasks,
            &session,
            "sleep 0.1 &",
            None,
            Some(finished_final(true)),
            false,
        );
        let mut race_sampled = false;
        let raced = race_snap_task
            .snapshot_held_reserved(&mut |_| Ok(()), &mut |_| {
                race_sampled = true;
                race_snap_task.tree_done.send_replace(true);
                Ok(None)
            })
            .unwrap();
        assert!(race_sampled);
        assert!(!raced.kept && raced.tree_ended && raced.held_known && raced.held.is_empty());
        assert!(raced.kill_error.is_none());
        race_snap_entry.mark_reported(&raced);

        // 3. Prior Some([]) in keeps_reported must not suppress !held_known in Notice::Keeps,
        // and held_brief formats unconfirmed states with and without kill_error.
        let (unconf_task, unconf_entry) = make_synthetic_process_task(
            &tasks,
            &session,
            "sleep 30 &",
            None,
            Some(finished_final(true)),
            false,
        );
        let mut empty_known = unconf_task.notice_snapshot();
        crate::tasks::state::set_held(&mut empty_known, Some(Vec::new()));
        unconf_entry.mark_root_reported(&empty_known);
        unconf_entry.mark_keeps_reported(&empty_known);
        assert!(unconf_entry.keeps_reported_eq(&[]));
        let mut unconfirmed = unconf_task.notice_snapshot();
        crate::tasks::state::set_held(&mut unconfirmed, None);
        assert!(!unconfirmed.held_known);
        assert_eq!(held_brief(&unconfirmed), "processes (unconfirmed)");
        unconfirmed.kill_error = Some("EPERM".into());
        assert_eq!(
            held_brief(&unconfirmed),
            "processes (unconfirmed, kill failed)"
        );
        let (notice, claim) = tasks
            .background(&session, "", 512, &mut |_| Ok(()), &mut |_| Ok(None))
            .unwrap();
        drop(claim);
        let (line, _) = notice.expect("expected unconfirmed keeps footer");
        assert!(
            line.contains(&format!("{} keeps processes (unconfirmed)", unconf_task.id)),
            "{line}"
        );
        unconf_task.tree_done.send_replace(true);
        unconf_entry.mark_finished_reported(true);

        // 4. Notice::Root and Notice::Keeps tree_done transitions during sampling emit
        // `kept processes ended`, never `keeps`.
        let (ended_root, ended_root_entry) = make_synthetic_process_task(
            &tasks,
            &session,
            "sleep 0.1 &",
            None,
            Some(finished_final(true)),
            false,
        );
        let (notice, claim) = tasks
            .background(&session, "", 512, &mut |_| Ok(()), &mut |_| {
                ended_root.tree_done.send_replace(true);
                Ok(Some(Vec::new()))
            })
            .unwrap();
        drop(claim);
        let (line, shown) = notice.expect("expected ended root footer");
        assert!(!line.contains("keeps"), "{line}");
        assert!(
            line.contains(&format!(
                "{} exited 0 and its kept processes ended",
                ended_root.id
            )),
            "{line}"
        );
        ended_root_entry.mark_reported(&shown[0].1);

        let (ended_keeps, ended_keeps_entry) = make_synthetic_process_task(
            &tasks,
            &session,
            "sleep 0.1 &",
            None,
            Some(finished_final(true)),
            false,
        );
        let root_snap = ended_keeps.notice_snapshot();
        ended_keeps_entry.mark_root_reported(&root_snap);
        let (notice, claim) = tasks
            .background(&session, "", 512, &mut |_| Ok(()), &mut |_| {
                ended_keeps.tree_done.send_replace(true);
                Ok(Some(Vec::new()))
            })
            .unwrap();
        drop(claim);
        let (line, shown) = notice.expect("expected ended keeps footer");
        assert!(!line.contains("keeps"), "{line}");
        assert!(
            line.contains(&format!("{} kept processes ended", ended_keeps.id)),
            "{line}"
        );
        ended_keeps_entry.mark_reported(&shown[0].1);

        // 5. EventBus ordering: killed-running emits treeEnded: false; kept-then-ended emits
        // [treeEnded: false, treeEnded: true]; tree already ended at root publish emits single
        // merged treeEnded: true.
        let mut cursor = bus
            .poll(
                crate::events::PollRequest {
                    name: "task.exited".into(),
                    cursor: None,
                    arguments: None,
                    max_events: None,
                    max_age_ms: None,
                },
                None,
            )
            .unwrap()
            .value
            .cursor;
        let poll_all = |cursor: &mut String| {
            let page = bus
                .poll(
                    crate::events::PollRequest {
                        name: "task.exited".into(),
                        cursor: Some(cursor.clone()),
                        arguments: None,
                        max_events: None,
                        max_age_ms: None,
                    },
                    None,
                )
                .unwrap()
                .value;
            *cursor = page.cursor;
            page.events
                .into_iter()
                .map(|event| event.data)
                .collect::<Vec<_>>()
        };

        let (killed_running, _) = make_synthetic_task(
            &tasks,
            &session,
            "sleep 30",
            None,
            Some(Final {
                exit_code: None,
                killed: true,
                expired: false,
                forced_end: false,
                elapsed: Duration::from_millis(20),
                output_open: false,
                leftovers: Vec::new(),
                kept: false,
            }),
            true,
        );
        tasks.emit_task_exited(&killed_running);
        let events = poll_all(&mut cursor);
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(events[0]["taskId"], killed_running.id);
        assert_eq!(events[0]["treeEnded"], false);

        let (kept_then_ended, _) = make_synthetic_task(
            &tasks,
            &session,
            "sleep 30 &",
            None,
            Some(Final {
                exit_code: Some(0),
                killed: false,
                expired: false,
                forced_end: false,
                elapsed: Duration::from_millis(20),
                output_open: false,
                leftovers: vec![Member {
                    pid: 900,
                    name: "sleep".into(),
                }],
                kept: true,
            }),
            false,
        );
        tasks.emit_task_exited(&kept_then_ended);
        kept_then_ended.tree_done.send_replace(true);
        tasks.emit_task_exited(&kept_then_ended);
        let events = poll_all(&mut cursor);
        assert_eq!(events.len(), 2, "{events:?}");
        assert_eq!(events[0]["taskId"], kept_then_ended.id);
        assert_eq!(events[0]["treeEnded"], false);
        assert_eq!(events[0]["keptProcesses"], 1);
        assert_eq!(events[1]["taskId"], kept_then_ended.id);
        assert_eq!(events[1]["treeEnded"], true);
        assert_eq!(events[1]["keptProcesses"], 0);

        let (merged_ended, _) = make_synthetic_task(
            &tasks,
            &session,
            "sleep 0.01 &",
            None,
            Some(finished_final(true)),
            true,
        );
        tasks.emit_task_exited(&merged_ended);
        let events = poll_all(&mut cursor);
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(events[0]["taskId"], merged_ended.id);
        assert_eq!(events[0]["treeEnded"], true);
        assert_eq!(events[0]["keptProcesses"], 0);

        tasks.shutdown();
    }

    #[test]
    fn outside_claim_rolls_back_when_first_render_exhausts_byte_pool() {
        let (runtime, tasks, session_a) = fixture(Limits::OFF);
        let service = Arc::new(crate::service::Service::new(Arc::clone(&tasks), false));
        let (_outside_running, _) =
            make_synthetic_process_task(&tasks, &session_a, "sleep 30", None, None, false);

        let session_b = Session::new(
            "outside-budget".into(),
            "outside-budget".into(),
            Arc::clone(&session_a.launch),
        );
        let (own_task, own_entry) = make_synthetic_process_task(
            &tasks,
            &session_b,
            "printf ok",
            None,
            Some(finished_final(false)),
            true,
        );
        let reader = own_entry.start_reader("0001".into());
        let handle = reader.handle(&own_task.id);

        // Direct background reservation failure drops OutsideClaim without consuming it.
        let tiny_pool = BytePool::new(64);
        let mut tiny_lease = crate::lease::Lease::empty(&tiny_pool);
        let direct_err = tasks.background(
            &session_b,
            &own_task.id,
            512,
            &mut |bytes| {
                if tiny_lease.try_grow(tiny_lease.bytes().saturating_add(bytes)) {
                    Ok(())
                } else {
                    Err("budget full".into())
                }
            },
            &mut |_| Ok(Some(Vec::new())),
        );
        assert!(direct_err.is_err());
        drop(tiny_lease);
        assert_eq!(tiny_pool.used(), 0);
        assert!(session_b.claim_outside().is_claimed());

        runtime.block_on(async {
            // Exercise BytePool exhaustion across early workspace and final frame retention caps.
            for cap in [128usize, 1024, 2048, 3072] {
                let tight_pool = BytePool::new(cap);
                let tight_launch = Arc::new(
                    session_b
                        .launch
                        .for_session()
                        .with_memory(Arc::clone(&tight_pool)),
                );
                let mut ctx = test_exec_context(&session_b);
                ctx.launch = Some(&tight_launch);
                let refused = crate::render::exec_agent_json(
                    &service,
                        serde_json::json!({ "action": "poll", "taskId": handle, "waitMs": 0 }),
                        ctx,
                        crate::render::types::AgentDelivery::default(),
                    )
                    .await;
                assert!(
                    refused.overload,
                    "cap {cap} should fail with overload: {:?}",
                    refused.result
                );
                drop(refused);
                assert_eq!(tight_pool.used(), 0);
                assert!(
                    session_b.claim_outside().is_claimed(),
                    "OutsideClaim must stay unconsumed after BytePool failure at cap {cap}"
                );
            }

            let delivered = crate::render::exec_agent_json(
                    &service,
                    serde_json::json!({ "action": "poll", "taskId": handle, "waitMs": 0 }),
                    test_exec_context(&session_b),
                    crate::render::types::AgentDelivery::default(),
                )
                .await
                .result;
            assert_ne!(delivered.is_error, Some(true), "{delivered:?}");
            let first_text = delivered.content[0].as_text().unwrap().text.as_str();
            assert!(
                first_text.contains(
                    r#"(Background: 1 task running outside this session; use list with scope "all".)"#
                ),
                "first successful render must still deliver outside notice: {first_text}"
            );

            let second = crate::render::exec_agent_json(
                    &service,
                    serde_json::json!({ "action": "poll", "taskId": handle, "waitMs": 0 }),
                    test_exec_context(&session_b),
                    crate::render::types::AgentDelivery::default(),
                )
                .await
                .result;
            let second_text = second.content[0].as_text().unwrap().text.as_str();
            assert!(
                !second_text.contains("outside this session"),
                "{second_text}"
            );
        });

        tasks.shutdown();
    }

    #[test]
    fn concurrent_list_snapshots_keep_parent_child_folding_consistent() {
        let (_runtime, tasks, session) = fixture(Limits::OFF);
        let (parent, _) = make_synthetic_task(
            &tasks,
            &session,
            "[codemode] concurrent()",
            None,
            None,
            false,
        );
        let done_mutating = AtomicBool::new(false);

        let verify_default = |res: &ListedResult| {
            for item in &res.items {
                assert_ne!(
                    item.parent_task_id.as_deref(),
                    Some(parent.id.as_str()),
                    "child promoted as orphan while parent is present"
                );
            }
            let parent_item = res
                .items
                .iter()
                .find(|item| item.task.id == parent.id)
                .expect("parent must appear in unfiltered view");
            if let Some(children) = &parent_item.children {
                let finished = children.done + children.failed;
                assert_eq!(children.total, children.active + finished);
                assert_eq!(children.folded_finished.len(), finished);
                assert_eq!(
                    children.active_tasks.len(),
                    children.active.min(MAX_INLINE_ACTIVE)
                );
                assert!(children.warned <= finished);
                assert!(children.tree_ended <= finished);
                let finished_ids: HashSet<&str> = children
                    .folded_finished
                    .iter()
                    .map(|(t, _)| t.id.as_str())
                    .collect();
                for (child, snap) in &children.active_tasks {
                    assert!(
                        snap.state == "running" || snap.kept,
                        "inactive child in active_tasks: {}",
                        snap.state
                    );
                    assert!(!finished_ids.contains(child.id.as_str()));
                }
            }
        };

        std::thread::scope(|scope| {
            let writer_tasks = &tasks;
            let writer_session = &session;
            let parent_ref = &parent;
            let done_flag = &done_mutating;
            scope.spawn(move || {
                let mut spawned: Vec<Arc<Task>> = Vec::new();
                for idx in 0..32usize {
                    let (child, _) = make_synthetic_process_task(
                        writer_tasks,
                        writer_session,
                        &format!("step-{idx}"),
                        Some(parent_ref.id.clone()),
                        None,
                        false,
                    );
                    spawned.push(child);
                    if idx >= 2 {
                        let prev = &spawned[idx - 2];
                        let kept = idx % 3 == 0;
                        let exit_code = if idx % 5 == 0 { Some(1) } else { Some(0) };
                        {
                            let _reg = lock(&writer_tasks.registry);
                            *lock(&prev.end) = Some(Final {
                                exit_code,
                                killed: false,
                                expired: false,
                                forced_end: false,
                                elapsed: Duration::from_millis(5),
                                output_open: false,
                                leftovers: Vec::new(),
                                kept,
                            });
                            if !kept {
                                prev.tree_done.send_replace(true);
                            }
                        }
                    }
                    if idx >= 5 && (idx - 5) % 3 == 0 {
                        let kept_child = &spawned[idx - 5];
                        let _reg = lock(&writer_tasks.registry);
                        let _ = kept_child.tree_ended_at.set(Instant::now());
                        kept_child.tree_done.send_replace(true);
                    }
                    std::thread::yield_now();
                }
                {
                    let _reg = lock(&writer_tasks.registry);
                    *lock(&parent_ref.end) = Some(finished_final(false));
                    parent_ref.tree_done.send_replace(true);
                }
                done_flag.store(true, Ordering::Release);
            });

            for reader_idx in 0..3usize {
                let reader_tasks = &tasks;
                let reader_session = &session;
                let parent_id = parent.id.as_str();
                let done_flag = &done_mutating;
                scope.spawn(move || {
                    let mut checks = 0usize;
                    loop {
                        let finished = done_flag.load(Ordering::Acquire);
                        let all_scope = reader_idx % 2 == 0;
                        let listed = reader_tasks
                            .listed(
                                reader_session,
                                ListQuery {
                                    all_scope,
                                    active_filter: None,
                                    parent_task_id: None,
                                    limit: 50,
                                },
                                &mut |_| Ok(()),
                                &mut |_| Ok(Some(Vec::new())),
                            )
                            .unwrap()
                            .unwrap();
                        verify_default(&listed);

                        let drill = reader_tasks
                            .listed(
                                reader_session,
                                ListQuery {
                                    all_scope: true,
                                    active_filter: None,
                                    parent_task_id: Some(parent_id),
                                    limit: 50,
                                },
                                &mut |_| Ok(()),
                                &mut |_| Ok(Some(Vec::new())),
                            )
                            .unwrap()
                            .unwrap();
                        assert!(drill.total_count <= 32);
                        checks += 1;
                        if finished && checks >= 4 {
                            break;
                        }
                        std::thread::yield_now();
                    }
                });
            }
        });

        tasks.shutdown();
    }

    #[test]
    fn codemode_custom_cwd_anchors_relative_home_to_session_cwd() {
        let _serial = crate::tasks::ONE_TASKS
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let root = std::env::temp_dir().join(format!(
            "fastexec-rel-home-cm-{}-{}",
            std::process::id(),
            random_chars(10).unwrap()
        ));
        std::fs::create_dir_all(root.join("rel-home")).unwrap();
        std::fs::create_dir_all(root.join("nested-cwd/rel-home")).unwrap();

        let mut env: Vec<(std::ffi::OsString, std::ffi::OsString)> = std::env::vars_os()
            .filter(|(k, _)| {
                !k.eq_ignore_ascii_case("HOME")
                    && !k.eq_ignore_ascii_case("USERPROFILE")
                    && !k.eq_ignore_ascii_case("HOMEDRIVE")
                    && !k.eq_ignore_ascii_case("HOMEPATH")
            })
            .collect();
        env.push(("HOME".into(), "rel-home".into()));
        if cfg!(windows) {
            env.push(("USERPROFILE".into(), "rel-home".into()));
        }

        let tasks = runtime.block_on(async { Tasks::new(Limits::OFF).unwrap() });
        let service = Arc::new(crate::service::Service::new(Arc::clone(&tasks), false));
        let launch = Arc::new(LaunchContext::new(root.clone(), env));
        let session = Session::new("cm-rel-home".into(), "cm-rel-home".into(), launch);

        runtime.block_on(async {
            let req: crate::codemode::control::Request = serde_json::from_value(serde_json::json!({
                "code": "await tools.write({ path: '~/memo.txt', content: 'from-session-cwd\\n' }); await tools.edit({ path: '~/memo.txt', edits: [{ oldText: 'from-session-cwd', newText: 'anchored-ok' }] }); const r = await tools.read({ path: '~/memo.txt' }); return r.text;",
                "cwd": "nested-cwd",
                "waitMs": 10000,
                "killAfterMs": 10000
            }))
            .unwrap();
            let mut ctx = test_exec_context(&session);
            let delivery = crate::render::types::AgentDelivery::default();
            let outcome = crate::codemode::prepare_script(&service, req, &mut ctx, None, false).await;
            let out = crate::render::finish_exec_agent(&service, outcome, &ctx, &delivery)
                .await
                .result;
            assert_ne!(out.is_error, Some(true), "{out:?}");
            let text = out.content[0].as_text().unwrap().text.as_str();
            assert!(text.contains("anchored-ok"), "{text}");
        });

        assert_eq!(
            std::fs::read_to_string(root.join("rel-home/memo.txt")).unwrap(),
            "anchored-ok\n"
        );
        assert!(!root.join("nested-cwd/rel-home/memo.txt").exists());

        tasks.shutdown();
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn retire_finished_evicts_logs_outside_registry_lock() {
        let (runtime, tasks, session) = fixture(Limits {
            max_finished: 1,
            ..Limits::OFF
        });
        let ((task1, _, _), _) =
            crate::codemode::spawn_script(&tasks, script_args(&tasks), &session, "retire1".into())
                .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task1.done().wait_for(|done| *done))
                .await
                .unwrap()
                .unwrap();
        });
        assert!(task1.log_path.exists());

        let unlinked_unlocked = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&unlinked_unlocked);
        let weak_tasks = Arc::downgrade(&tasks);
        tasks.set_remove_hook(Some(Arc::new(move |path| {
            if let Some(shared) = weak_tasks.upgrade() {
                assert!(
                    shared.registry.try_lock().is_ok(),
                    "retire_finished must release registry lock before unlinking logs"
                );
                observed.store(true, Ordering::SeqCst);
            }
            std::fs::remove_file(path)
        })));

        std::thread::sleep(Duration::from_millis(5));
        let ((task2, _, _), _) =
            crate::codemode::spawn_script(&tasks, script_args(&tasks), &session, "retire2".into())
                .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task2.done().wait_for(|done| *done))
                .await
                .unwrap()
                .unwrap();
        });

        assert!(unlinked_unlocked.load(Ordering::SeqCst));
        assert!(!task1.log_path.exists());
        assert!(task2.log_path.exists());
        tasks.set_remove_hook(None);
        tasks.shutdown();
    }

    #[test]
    fn g1_reserve_script_slot_rolls_back_pending_counts_without_spawning_threads() {
        let (_runtime, tasks, session) = fixture(Limits {
            max_running: 2,
            max_scripts: 1,
            ..Limits::OFF
        });

        #[cfg(target_os = "linux")]
        let count_threads = || std::fs::read_dir("/proc/self/task").unwrap().count();
        #[cfg(target_os = "linux")]
        let baseline_threads = count_threads();

        let slot = tasks.reserve_script_slot().expect("first script slot");
        assert_eq!(tasks.pending(), 1);
        assert_eq!(lock(&tasks.registry).pending_scripts, 1);

        // Second script reservation fails with TOO_MANY_SCRIPTS before spawning any worker thread.
        let err = match tasks.reserve_script_slot() {
            Ok(_) => panic!("second script slot must be rejected"),
            Err(err) => err,
        };
        assert!(
            matches!(&err, TaskError::Overload(msg) if msg.contains("TOO_MANY_SCRIPTS")),
            "{err:?}"
        );
        let spawn_err = match crate::codemode::spawn_script(
            &tasks,
            script_args(&tasks),
            &session,
            "s002".into(),
        ) {
            Ok(_) => panic!("spawn_script must fail under held script slot"),
            Err(err) => err,
        };
        assert!(
            matches!(&spawn_err, TaskError::Overload(msg) if msg.contains("TOO_MANY_SCRIPTS")),
            "{spawn_err:?}"
        );
        assert_eq!(tasks.pending(), 1);
        assert_eq!(lock(&tasks.registry).pending_scripts, 1);
        #[cfg(target_os = "linux")]
        assert_eq!(count_threads(), baseline_threads);

        // Dropping the uncommitted reservation rolls back both pending counters to zero.
        drop(slot);
        assert_eq!(tasks.pending(), 0);
        assert_eq!(lock(&tasks.registry).pending_scripts, 0);

        // Also verify TOO_MANY_TASKS rollback symmetry.
        let (_rt2, tight_tasks, _sess2) = fixture(Limits {
            max_running: 1,
            max_scripts: 4,
            ..Limits::OFF
        });
        let slot_a = tight_tasks.reserve_script_slot().unwrap();
        let err_b = match tight_tasks.reserve_script_slot() {
            Ok(_) => panic!("second task slot must be rejected"),
            Err(err) => err,
        };
        assert!(
            matches!(&err_b, TaskError::Overload(msg) if msg.contains("TOO_MANY_TASKS")),
            "{err_b:?}"
        );
        assert_eq!(tight_tasks.pending(), 1);
        assert_eq!(lock(&tight_tasks.registry).pending_scripts, 1);
        drop(slot_a);
        assert_eq!(tight_tasks.pending(), 0);
        assert_eq!(lock(&tight_tasks.registry).pending_scripts, 0);

        tight_tasks.shutdown();
        tasks.shutdown();
    }

    #[test]
    fn g2_publish_script_closed_job_channel_closes_pipe_and_finishes_synchronously() {
        let (_runtime, tasks, session) = fixture(Limits::OFF);
        let pool = BytePool::new(128);
        let payload_owner = crate::lease::Owner::new(pool.try_acquire(128).unwrap());
        assert_eq!(pool.used(), 128);

        let reserved = tasks.reserve_script_slot().unwrap();
        let script = Arc::new(super::script::ScriptControl::new(std::thread::current()));
        let (job_tx, job_rx) = mpsc::sync_channel::<(ScriptIo, crate::lease::Owner)>(1);
        // Simulate worker failure before handoff by dropping the receiver end of the job channel.
        drop(job_rx);

        let (task, _entry, _reader) = tasks
            .publish_script(
                reserved,
                ScriptPublishArgs {
                    command: "[codemode] broken-worker".into(),
                    kill_after_ms: None,
                    call_id: Some(42),
                    script,
                    key: "g2rk".into(),
                },
                &session,
                job_tx,
                payload_owner,
            )
            .expect("publish_script recovers synchronously when job channel is closed");

        assert_eq!(pool.used(), 0, "dropped job payload must release its Owner");
        assert_eq!(tasks.pending(), 0);
        assert_eq!(lock(&tasks.registry).pending_scripts, 0);
        assert!(task.output_closed(), "PipeWriter must close immediately");
        assert!(task.is_finished(), "finish_script must run synchronously");
        assert!(*task.done().borrow());
        assert!(*task.tree_done().borrow());
        let snap = task.snapshot();
        assert_eq!(snap.state, "exited");
        assert_eq!(snap.exit_code, Some(1));
        assert!(!snap.output_open);

        let active = tasks
            .listed(
                &session,
                ListQuery {
                    all_scope: true,
                    active_filter: Some(true),
                    parent_task_id: None,
                    limit: 20,
                },
                &mut |_| Ok(()),
                &mut |_| Ok(Some(Vec::new())),
            )
            .unwrap()
            .unwrap();
        assert_eq!(active.total_count, 0, "task must not linger in active list");

        let finished = tasks
            .listed(
                &session,
                ListQuery {
                    all_scope: true,
                    active_filter: Some(false),
                    parent_task_id: None,
                    limit: 20,
                },
                &mut |_| Ok(()),
                &mut |_| Ok(Some(Vec::new())),
            )
            .unwrap()
            .unwrap();
        assert_eq!(finished.total_count, 1);
        assert_eq!(finished.items[0].task.id, task.id);

        tasks.shutdown();
    }

    #[test]
    fn pty_auto_answer_broken_pipe_leaves_input_error_none() {
        let (runtime, tasks, session) = fixture(Limits::OFF);
        let ((task, _, _), _) =
            crate::codemode::spawn_script(&tasks, script_args(&tasks), &session, "pty0".into())
                .unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), task.done().wait_for(|done| *done))
                .await
                .unwrap()
                .unwrap();
        });

        let workers = Workers::new(true).unwrap();
        let (pipe_reader, pipe_writer) = std::io::pipe().unwrap();
        drop(pipe_reader);
        let (input_tx, input_rx) = mpsc::channel();
        *lock(&task.input) = Some(input_tx.clone());
        answer_program(&task, &input_tx, b"\x1b[1;1R".to_vec());
        assert_eq!(task.queued.load(Ordering::SeqCst), 6);
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
        done_sig.wait_until(Instant::now() + Duration::from_secs(2));

        assert_eq!(task.queued.load(Ordering::SeqCst), 0);
        assert!(lock(&task.input).is_none());
        assert!(lock(&task.input_error).is_none());
        assert!(lock(&task.input_warning).is_none());
        let snap = task.snapshot();
        assert!(snap.input_error.is_none(), "{:?}", snap.input_error);
        assert!(snap.input_warning.is_none(), "{:?}", snap.input_warning);
        assert!(crate::service::core::task_io_ok(&snap));

        tasks.shutdown();
    }
}
