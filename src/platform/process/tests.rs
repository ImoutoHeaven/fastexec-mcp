//! Unit tests for process launch helpers, Unix task hosts, and Windows Job Objects.

mod member_tests {
    use super::super::types::{clean_member_name, normalize_members};
    use crate::platform::members::Member;

    #[test]
    fn member_normalization_sorts_deduplicates_and_cleans_names() {
        let mut list = vec![
            Member {
                pid: 9,
                name: "b".into(),
            },
            Member {
                pid: 3,
                name: "first".into(),
            },
            Member {
                pid: 3,
                name: "duplicate".into(),
            },
        ];
        normalize_members(&mut list);
        assert_eq!(list.len(), 2);
        assert_eq!((list[0].pid, list[0].name.as_str()), (3, "first"));
        assert_eq!((list[1].pid, list[1].name.as_str()), (9, "b"));

        assert_eq!(clean_member_name(""), None);
        assert_eq!(clean_member_name("\0\n\r\u{2028}\u{2029}"), None);
        assert_eq!(
            clean_member_name("  adb\nserver\u{2028}--daemon  "),
            Some("adb server --daemon".into())
        );
    }
}

mod env_tests {
    use super::super::probe::{parse_env_dump, write_env_dump};
    use crate::env::Unit;
    use std::ffi::OsString;

    #[test]
    fn login_environment_dump_preserves_native_units_and_rejects_incomplete_records() {
        #[cfg(unix)]
        let native = {
            use std::os::unix::ffi::OsStringExt;
            OsString::from_vec(vec![b'x', 0xff])
        };
        #[cfg(windows)]
        let native = {
            use std::os::windows::ffi::OsStringExt;
            OsString::from_wide(&[120, 0xd800])
        };
        let vars = vec![
            (native.clone(), OsString::new()),
            ("equals".into(), "one=two".into()),
            ("=C:".into(), native),
        ];
        let mut bytes = b"profile output\n".to_vec();
        write_env_dump(&mut bytes, vars.clone()).unwrap();
        assert_eq!(parse_env_dump(&bytes).unwrap(), vars);
        for cut in 1..=2 * size_of::<Unit>() {
            assert!(parse_env_dump(&bytes[..bytes.len() - cut]).is_err());
        }
        let mut empty = Vec::new();
        write_env_dump(&mut empty, []).unwrap();
        assert!(parse_env_dump(&empty).unwrap().is_empty());

        let mut short = [0; 3];
        assert!(write_env_dump(short.as_mut_slice(), []).is_err());
    }

    #[cfg(windows)]
    #[test]
    fn msys_paths_keep_non_unicode_native_units() {
        use super::super::types::bash_path;
        use std::os::windows::ffi::OsStringExt;
        use std::path::Path;
        let path = OsString::from_wide(&[67, 58, 92, 0xd800]);
        assert_eq!(
            bash_path(Path::new(&path)),
            OsString::from_wide(&[67, 58, 47, 0xd800])
        );
    }
}

#[cfg(unix)]
mod startup_tests {
    use super::super::types::{Launch, PTY_COLS, PTY_ROWS, bash_args};
    use super::super::unix::finish_host_startup;
    use crate::env::LaunchContext;
    use std::ffi::OsString;
    use std::io::{BufRead, BufReader, Read};
    use std::os::unix::process::CommandExt;
    use std::path::Path;
    use std::process::{Command, Stdio};

    #[test]
    fn script_arguments_preserve_unix_path_bytes() {
        use std::os::unix::ffi::OsStringExt;

        let script = std::path::PathBuf::from(OsString::from_vec(
            b"/tmp/native\\path-\xff/task.sh".to_vec(),
        ));
        let context = LaunchContext::new("/".into(), Vec::new());
        let launch = Launch {
            bash: Path::new("/bin/bash"),
            command: "",
            script: Some(&script),
            cwd: Path::new("/"),
            base_env: &context.env,
            login: false,
            pty: false,
            pipe_stdin: false,
            cols: PTY_COLS,
            rows: PTY_ROWS,
            exit_status: None,
        };
        assert_eq!(
            bash_args(&launch),
            vec![
                OsString::from("--noprofile"),
                OsString::from("--norc"),
                script.into_os_string()
            ]
        );
    }

    #[test]
    fn a_failed_startup_ends_commands_in_the_reserved_host_group() {
        let mut command = Command::new("/bin/sh");
        command
            .args(["-c", "sleep 10 & printf 'started\\n'; kill -KILL $$"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        // SAFETY: setsid is async-signal-safe and starts an isolated test session.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut host = command.spawn().unwrap();
        let mut output = BufReader::new(host.stdout.take().unwrap());
        let mut started = String::new();
        output.read_line(&mut started).unwrap();
        assert_eq!(started, "started\n");
        // A dead host closes its status writer without sending READY; its command still owns stdout.
        let (mut status, writer) = std::io::pipe().unwrap();
        drop(writer);
        let error = finish_host_startup(&mut host, &mut status).unwrap_err();
        assert!(error.to_string().contains("before it reported readiness"));
        let (tx, rx) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            let mut tail = Vec::new();
            let _ = tx.send(output.read_to_end(&mut tail));
        });
        let ended = rx.recv_timeout(std::time::Duration::from_secs(2));
        // A broken cleanup still leaves no fixture behind: the bounded sleep eventually exits.
        reader.join().unwrap();
        assert!(
            matches!(ended, Ok(Ok(0))),
            "the failed startup left its background command holding stdout: {ended:?}"
        );
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
mod host_end_tests {
    use super::super::types::{Child, Tree, TreeState, lock};
    use std::process::Command;

    #[test]
    fn reaped_hosts_confirm_only_clean_exits_and_never_resume_tree_queries() {
        for (command, code, complete) in [
            ("exit 0", 0, true),
            ("exit 23", 23, false),
            ("kill -TERM $$", 143, false),
        ] {
            let host = Command::new("/bin/sh")
                .args(["-c", command])
                .spawn()
                .unwrap();
            let (status, status_writer) = std::io::pipe().unwrap();
            drop(status_writer);
            let (_lifeline_reader, lifeline) = std::io::pipe().unwrap();
            let tree = Tree {
                host: host.id() as i32,
                _lifeline: lifeline,
                state: std::sync::Mutex::new(TreeState::Active),
            };
            let mut child = Child {
                host,
                status,
                master: None,
            };
            assert_eq!(child.reap(&tree), code);
            assert_eq!(child.wait_tree(&tree), complete);
            assert_eq!(
                *lock(&tree.state),
                if complete {
                    TreeState::Complete
                } else {
                    TreeState::Unconfirmed
                }
            );
            for _ in 0..2 {
                if complete {
                    assert!(tree.members().unwrap().is_empty());
                    tree.kill().unwrap();
                } else {
                    assert!(tree.members().is_none());
                    assert!(tree.kill().unwrap_err().to_string().contains("unconfirmed"));
                }
            }
        }
    }
}

#[cfg(unix)]
mod refusing_control_tests {
    use super::super::types::PtyControl;

    /// The native resize error reaches the caller as an error, not as a silent no-op. A plain
    /// file takes no terminal size: Linux answers ENOTTY and macOS ENODEV, as a terminal that is
    /// gone does.
    #[test]
    fn a_control_that_cannot_resize_reports_the_error() {
        let file = std::fs::File::open("/dev/null").expect("/dev/null");
        let control = PtyControl::new(&file).expect("a shared descriptor");
        let error = control
            .resize(100, 40)
            .expect_err("a resize on a plain file fails");
        assert!(
            matches!(
                error.raw_os_error(),
                Some(libc::ENOTTY) | Some(libc::ENODEV)
            ),
            "the ioctl answers ENOTTY or ENODEV: {error}"
        );
    }
}

#[cfg(windows)]
mod windows_tests {
    use super::super::windows::{
        JOB_LIST_HEADER_WORDS, JobProcess, collect_job_members, inspect_member, job_members,
        job_pids, job_process, kill_on_close_job, resume_threads,
    };
    use crate::platform::members::MemberBudget;
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
    use windows_sys::Win32::Foundation::{
        ERROR_INVALID_PARAMETER, ERROR_MORE_DATA, INVALID_HANDLE_VALUE,
    };
    use windows_sys::Win32::System::JobObjects::{
        CreateJobObjectW, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_BASIC_PROCESS_ID_LIST,
    };

    fn fill(buffer: &mut [usize], assigned: u32, listed: u32) {
        // SAFETY: job_pids supplies aligned, initialized storage for this header.
        let header = unsafe {
            &mut *buffer
                .as_mut_ptr()
                .cast::<JOBOBJECT_BASIC_PROCESS_ID_LIST>()
        };
        header.NumberOfAssignedProcesses = assigned;
        header.NumberOfProcessIdsInList = listed;
        for (index, pid) in buffer[JOB_LIST_HEADER_WORDS..]
            .iter_mut()
            .take(listed as usize)
            .enumerate()
        {
            *pid = index + 1;
        }
    }

    #[test]
    fn a_failed_job_query_is_unknown_and_an_empty_job_is_complete() {
        assert!(job_members(INVALID_HANDLE_VALUE, None).is_none());
        // SAFETY: null arguments create an unnamed job owned by this test.
        let raw = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        assert!(!raw.is_null());
        let job = unsafe { OwnedHandle::from_raw_handle(raw) };
        assert!(job_members(job.as_raw_handle(), None).unwrap().is_empty());
        assert!(job_pids(|_, _| Err(ERROR_INVALID_PARAMETER)).is_none());
        assert_eq!(job_pids(|_, _| Ok(())), Some(Vec::new()));
    }

    #[test]
    fn a_terminated_process_with_exit_code_259_is_not_a_live_member() {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::{CREATE_NO_WINDOW, CREATE_SUSPENDED};
        for code in [0, 3, 259] {
            let mut child = std::process::Command::new("cmd.exe")
                .args(["/d", "/c", &format!("exit {code}")])
                .creation_flags(CREATE_NO_WINDOW | CREATE_SUSPENDED)
                .spawn()
                .unwrap();
            let handle = child.as_raw_handle();
            let owned_job = kill_on_close_job(handle).unwrap_or_else(|error| {
                let _ = child.kill();
                panic!("assign child to job: {error}")
            });
            let job = owned_job.as_raw_handle();
            let root = Some((child.id(), handle));
            assert!(matches!(
                job_process(job, std::process::id(), &mut MemberBudget::unlimited()),
                JobProcess::Gone
            ));
            assert!(
                job_members(job, root)
                    .unwrap()
                    .iter()
                    .any(|member| member.pid == child.id())
            );
            resume_threads(child.id()).unwrap();
            assert_eq!(child.wait().unwrap().code(), Some(code));
            assert!(matches!(
                inspect_member(job, handle, true, &mut MemberBudget::unlimited()),
                JobProcess::Gone
            ));
            assert!(job_members(job, root).unwrap().is_empty());
            assert!(job_members(job, None).unwrap().is_empty());
            assert!(!crate::platform::runtime::is_alive(child.id()));
        }
    }

    #[test]
    fn filtering_terminated_job_ids_checks_for_new_processes() {
        let counts = |total, active| JOBOBJECT_BASIC_ACCOUNTING_INFORMATION {
            TotalProcesses: total,
            ActiveProcesses: active,
            ..Default::default()
        };
        for (after, empty) in [
            (Some(counts(3, 2)), false), // A new child appeared after the PID snapshot.
            (None, false),
            (Some(counts(2, 2)), true), // Exited PID still held, plus conhost.
            (Some(counts(3, 0)), true), // New child also exited before the check.
        ] {
            let mut samples = [Some(counts(2, 2)), after].into_iter();
            let members = collect_job_members(
                || samples.next().unwrap(),
                || Some(vec![21, 22]),
                |_, _| JobProcess::Gone,
                &mut MemberBudget::unlimited(),
            );
            assert_eq!(members.map(|members| members.len()), empty.then_some(0));
        }
    }

    #[test]
    fn an_unidentified_job_entry_needs_current_job_evidence() {
        let info = |active| {
            Some(JOBOBJECT_BASIC_ACCOUNTING_INFORMATION {
                TotalProcesses: 2,
                ActiveProcesses: active,
                ..Default::default()
            })
        };
        for (current, active, expected) in [
            (vec![20], 0, Some(0)),
            (vec![21], 1, None),
            (vec![], 1, Some(0)),
            (vec![20], 1, Some(1)),
        ] {
            let mut samples = [info(2), info(active)].into_iter();
            let mut lists = [vec![20], current].into_iter();
            let members = collect_job_members(
                || samples.next().unwrap(),
                || lists.next(),
                |_, _| JobProcess::Unknown,
                &mut MemberBudget::unlimited(),
            );
            assert_eq!(members.as_ref().map(Vec::len), expected);
            if expected == Some(1) {
                assert_eq!(members.unwrap()[0].name, "?");
            }
        }
    }

    #[test]
    fn member_budget_refusal_is_distinct_from_an_empty_job() {
        let mut deny = |_| false;
        let mut budget = MemberBudget::new(&mut deny);
        let result = collect_job_members(
            || {
                Some(JOBOBJECT_BASIC_ACCOUNTING_INFORMATION {
                    TotalProcesses: 1,
                    ActiveProcesses: 1,
                    ..Default::default()
                })
            },
            || Some(vec![21]),
            |_, budget| {
                if budget.temporary(6) {
                    JobProcess::Named("worker".into())
                } else {
                    JobProcess::Unknown
                }
            },
            &mut budget,
        );
        assert!(result.is_none());
        assert!(budget.refused());
    }

    #[test]
    fn job_queries_grow_for_more_data_and_successful_short_lists() {
        for first in [Ok(()), Err(ERROR_MORE_DATA)] {
            let mut calls = 0;
            let ids = job_pids(|buffer, bytes| {
                calls += 1;
                assert_eq!(bytes as usize, std::mem::size_of_val(buffer));
                let capacity = buffer.len() - JOB_LIST_HEADER_WORDS;
                if calls == 1 {
                    assert_eq!(capacity, 1024);
                    fill(buffer, 1025, 1024);
                    first
                } else {
                    assert!(capacity >= 1025);
                    fill(buffer, 1025, 1025);
                    Ok(())
                }
            })
            .unwrap();
            assert_eq!(calls, 2);
            assert_eq!(ids, (1..=1025).collect::<Vec<_>>());
        }
    }

    #[test]
    fn job_queries_never_accept_inconsistent_counts() {
        for (assigned, listed) in [(0, 1), (1025, 1025)] {
            assert!(
                job_pids(|buffer, _| {
                    fill(buffer, assigned, listed);
                    Ok(())
                })
                .is_none()
            );
        }
    }

    #[test]
    fn job_query_growth_is_bounded_and_byte_sizes_are_checked() {
        let mut calls = 0;
        assert!(
            job_pids(|buffer, _| {
                calls += 1;
                let capacity = (buffer.len() - JOB_LIST_HEADER_WORDS) as u32;
                fill(buffer, capacity + 1, capacity);
                Err(ERROR_MORE_DATA)
            })
            .is_none()
        );
        assert_eq!(calls, 8);

        let mut calls = 0;
        assert!(
            job_pids(|buffer, _| {
                calls += 1;
                fill(buffer, u32::MAX, 0);
                Err(ERROR_MORE_DATA)
            })
            .is_none()
        );
        assert_eq!(calls, 1);
    }
}

#[cfg(windows)]
mod pty_tests {
    use super::super::types::{Launch, PTY_COLS, PTY_ROWS};
    use super::super::windows::create_in_job;
    use crate::env::LaunchContext;
    use std::os::windows::io::{AsRawHandle, OwnedHandle};
    use windows_sys::Win32::System::Threading::{GetProcessId, ResumeThread, SuspendThread};

    fn pid_of(process: &OwnedHandle) -> u32 {
        // SAFETY: the handle is a live process handle owned by the caller.
        unsafe { GetProcessId(process.as_raw_handle()) }
    }

    /// The Job owns the root from before its first instruction: the root is a member while its
    /// only thread is still suspended, which a resumed or unsuspended creation cannot show.
    #[test]
    fn a_pty_root_is_in_its_job_before_it_runs() {
        let context = LaunchContext::current().expect("this process's context");
        let bash = crate::platform::bash::resolve(&context).expect("bash for this context");
        let cwd = std::env::current_dir().expect("a working directory");
        let launch = Launch {
            bash: &bash,
            command: "sleep 5",
            script: None,
            cwd: &cwd,
            base_env: &context.env,
            used_snapshot: false,
            login: false,
            pty: true,
            pipe_stdin: false,
            cols: PTY_COLS,
            rows: PTY_ROWS,
            exit_status: None,
        };
        let suspended = create_in_job(&launch).expect("a suspended PTY root");
        let pid = pid_of(&suspended.process);
        assert!(
            suspended
                .tree
                .members()
                .unwrap_or_default()
                .iter()
                .any(|member| member.pid == pid),
            "the root is in the job before it runs"
        );
        // SAFETY: the handle is the suspended root's own thread, and the calls balance.
        let previous = unsafe { SuspendThread(suspended.thread.as_raw_handle()) };
        assert_eq!(previous, 1, "the root is still suspended");
        // SAFETY: undoes the count above, leaving the root suspended as it was.
        unsafe { ResumeThread(suspended.thread.as_raw_handle()) };
        let _ = suspended.tree.kill();
    }
}
