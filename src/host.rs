//! The Unix task host: the root of each task's process tree, between the server and bash.
//!
//! The server starts the fastexec binary again with [`FLAG`] as the leader of a new session. The
//! host then:
//! - on Linux, becomes the child subreaper of its descendants, so processes that daemonize
//!   (`setsid`, a double fork) are adopted by it instead of init and stay in the tree;
//! - runs bash in its own session and process group, and stays its leader after bash exits, so
//!   a PTY task's terminal hangs up only when the whole tree is gone;
//! - writes bash's exit code to its status pipe, then reaps until no child is left, and exits;
//! - on macOS, which adopts no orphan: tracks the members of the tree by pid and start time from
//!   the moment bash starts, keeps running while it holds one, and answers the server's member and
//!   kill requests on its control socket, so no signal ever names a group by a number the server
//!   read elsewhere;
//! - ends the tree when its lifeline reaches EOF: the server dropped the tree or died, even by
//!   SIGKILL.

use std::ffi::OsString;
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::{FromRawFd, RawFd};

/// The first argument that makes the fastexec binary run as a task host.
pub const FLAG: &str = "--fastexec-task-host";

/// The host's first word on the status pipe: 0 when the host is ready to run bash, else a
/// Linux errno saying why it cannot own the task's tree. The root bash's exit code follows as a
/// second word.
pub const STARTUP_READY: u32 = 0;

extern "C" fn ignore(_: libc::c_int) {}

/// Runs the host with `args`: the status pipe's descriptor, the lifeline's descriptor, the
/// control socket's descriptor where the platform has one, then bash and its arguments.
pub fn main(args: &[OsString]) -> ! {
    let descriptor = |index: usize| -> Option<RawFd> { args.get(index)?.to_str()?.parse().ok() };
    #[cfg(target_os = "macos")]
    let (Some(status), Some(lifeline), Some(control), Some(bash)) =
        (descriptor(0), descriptor(1), descriptor(2), args.get(3))
    else {
        eprintln!(
            "fastexec: {FLAG} takes <status fd> <lifeline fd> <control fd> <bash> [arguments]"
        );
        std::process::exit(2);
    };
    #[cfg(not(target_os = "macos"))]
    let (Some(status), Some(lifeline), Some(bash)) = (descriptor(0), descriptor(1), args.get(2))
    else {
        eprintln!("fastexec: {FLAG} takes <status fd> <lifeline fd> <bash> [arguments]");
        std::process::exit(2);
    };
    // SAFETY: the server opened these descriptors for this process alone; bash must not
    // inherit them, or the pipes would outlive the host.
    let (mut status, lifeline) = unsafe {
        libc::fcntl(status, libc::F_SETFD, libc::FD_CLOEXEC);
        libc::fcntl(lifeline, libc::F_SETFD, libc::FD_CLOEXEC);
        (File::from_raw_fd(status), File::from_raw_fd(lifeline))
    };
    // The server waits for this word before it reports the task as started. A host that cannot
    // become the subreaper would silently leave daemonizing processes outside the tree, so it
    // fails the start instead of running bash.
    #[cfg(target_os = "linux")]
    // SAFETY: plain prctl; its failure is reported below.
    let startup = unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) };
    #[cfg(not(target_os = "linux"))]
    let startup = 0;
    if startup == -1 {
        let errno = std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or_default() as u32;
        let _ = status.write_all(&errno.to_le_bytes());
        std::process::exit(3);
    }
    if status.write_all(&STARTUP_READY.to_le_bytes()).is_err() {
        // The server is gone; nothing to run for.
        std::process::exit(3);
    }
    // The control socket is closed across exec again before bash starts: the server cleared that
    // flag only for the exec into this host.
    #[cfg(target_os = "macos")]
    let control = std::sync::Arc::new(std::sync::Mutex::new(crate::tree_macos::from_fd(control)));
    // The host shares bash's process group, the PTY's foreground group, so Ctrl-C, Ctrl-Z, the
    // hangup of a closing terminal, and a command's `kill 0` reach it too: it outlives every
    // signal but SIGKILL that would end or stop it. A handler, unlike SIG_IGN, resets to the
    // default action across exec, so bash gets the default dispositions, whatever the server
    // inherited. Synchronous faults keep theirs, and so do signals ignored by default; SIGPIPE
    // stays ignored, as Rust sets it, and std resets it for bash.
    const KEPT: [libc::c_int; 13] = [
        libc::SIGPIPE,
        libc::SIGKILL,
        libc::SIGSTOP,
        libc::SIGCHLD,
        libc::SIGCONT,
        libc::SIGWINCH,
        libc::SIGURG,
        libc::SIGSEGV,
        libc::SIGBUS,
        libc::SIGFPE,
        libc::SIGILL,
        libc::SIGTRAP,
        libc::SIGSYS,
    ];
    // SAFETY: each action is fully initialized and `ignore` is async-signal-safe; sigaction
    // rejects numbers that are not signals, and the mask is cleared before any thread starts.
    unsafe {
        for signal in (1..65).filter(|signal| !KEPT.contains(signal)) {
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = ignore as extern "C" fn(libc::c_int) as libc::sighandler_t;
            action.sa_flags = libc::SA_RESTART;
            libc::sigemptyset(&mut action.sa_mask);
            libc::sigaction(signal, &action, std::ptr::null_mut());
        }
        // A SIGCHLD the server ignores would make waitpid lose the children's statuses.
        libc::signal(libc::SIGCHLD, libc::SIG_DFL);
        // Blocked signals stay blocked across exec; bash starts with none.
        let mut none: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut none);
        libc::sigprocmask(libc::SIG_SETMASK, &none, std::ptr::null_mut());
    }
    // macOS passes a third descriptor, so its bash arguments start one place later.
    #[cfg(target_os = "macos")]
    let tail = &args[4..];
    #[cfg(not(target_os = "macos"))]
    let tail = &args[3..];
    let bash = match std::process::Command::new(bash).args(tail).spawn() {
        Ok(bash) => bash.id() as libc::pid_t,
        Err(error) => {
            eprintln!("fastexec: cannot start {}: {error}", bash.to_string_lossy());
            let _ = status.write_all(&127_i32.to_le_bytes());
            #[cfg(target_os = "macos")]
            if let Ok(mut control) = control.lock() {
                let _ = control.write_all(b"D");
            }
            std::process::exit(0);
        }
    };
    // The root's identity is taken now, while it is known to be this process: forgetting it at its
    // exit must not reach a successor that took the number.
    #[cfg(target_os = "macos")]
    let bash_start = crate::tree_macos::start_time(bash).unwrap_or(0);
    release_stdio();
    #[cfg(target_os = "macos")]
    let tracker = {
        let tracker = std::sync::Arc::new(std::sync::Mutex::new(crate::tree_macos::Tracker::new()));
        let served = std::sync::Arc::clone(&tracker);
        let serving = std::sync::Arc::clone(&control);
        std::thread::spawn(move || crate::tree_macos::serve(serving, served));
        // Members are observed while the tree runs, from the moment bash starts: a descendant that
        // detaches or is orphaned later is already known, so it stays in the tree. The scan is one
        // `proc_listallpids` and one `proc_pidinfo` per live process, every 100 ms.
        let ticking = std::sync::Arc::clone(&tracker);
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_millis(100));
                if let Ok(mut tracker) = ticking.lock() {
                    tracker.tick();
                }
            }
        });
        tracker
    };
    // The tracker is live from here: the lifeline handler ends a macOS tree by the same members
    // the server asks about, and the group last, all from inside the host that owns the tree.
    #[cfg(target_os = "macos")]
    {
        let tracked = std::sync::Arc::clone(&tracker);
        std::thread::spawn(move || watch_macos(lifeline, tracked));
    }
    #[cfg(not(target_os = "macos"))]
    std::thread::spawn(move || watch(lifeline));
    #[cfg(target_os = "macos")]
    wait_macos(bash, bash_start, &mut status, &tracker, &control);
    #[cfg(not(target_os = "macos"))]
    wait_linux(bash, &mut status);
}

/// Waits for the children on Linux: the host is the subreaper, so ECHILD means no descendant is
/// left and the tree has ended.
#[cfg(not(target_os = "macos"))]
fn wait_linux(bash: libc::pid_t, status: &mut File) -> ! {
    let mut reported = false;
    loop {
        let mut wait_status = 0;
        // SAFETY: plain syscall with a valid status pointer.
        let pid = unsafe { libc::waitpid(-1, &mut wait_status, 0) };
        if pid == -1 {
            if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            // ECHILD: no child is left, and on Linux no descendant either.
            std::process::exit(0);
        }
        if pid == bash && !reported {
            reported = true;
            let _ = status.write_all(&exit_code(wait_status).to_le_bytes());
        }
    }
}

/// Waits for the children on macOS, where an orphan is reparented to launchd: ECHILD means no
/// child is reapable, not that the tree ended. The host stays until the tracker holds no member,
/// so its status pipe stays open while it holds a tree — which is what a keepTree wait, a
/// deadline, and the server's own end-of-tree wait read.
#[cfg(target_os = "macos")]
fn wait_macos(
    bash: libc::pid_t,
    bash_start: u64,
    status: &mut File,
    tracker: &std::sync::Arc<std::sync::Mutex<crate::tree_macos::Tracker>>,
    control: &std::sync::Arc<std::sync::Mutex<std::os::unix::net::UnixStream>>,
) -> ! {
    let mut reported = false;
    let mut reaped_bash = false;
    loop {
        loop {
            let mut wait_status = 0;
            // SAFETY: plain syscall with a valid status pointer; WNOHANG returns at once.
            let pid = unsafe { libc::waitpid(-1, &mut wait_status, libc::WNOHANG) };
            if pid == 0 {
                break;
            }
            if pid == -1 {
                if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                // ECHILD: nothing is reapable right now. The scan below decides whether the tree
                // still holds a process, so this is not the end.
                break;
            }
            if pid == bash && !reported {
                reported = true;
                reaped_bash = true;
                // Membership is handed over before the root's code is published: the members are
                // re-read without the root, so a list taken right after it exits cannot show it.
                if let Ok(mut tracker) = tracker.lock() {
                    tracker.forget(pid, bash_start);
                }
                let _ = status.write_all(&exit_code(wait_status).to_le_bytes());
            }
        }
        if reaped_bash
            && tracker
                .lock()
                .map(|mut tracker| tracker.empty())
                .unwrap_or(false)
        {
            if let Ok(mut control) = control.lock() {
                let _ = control.write_all(b"D");
            }
            std::process::exit(0);
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// The bash convention: a signal end reads as 128 plus the signal.
fn exit_code(wait_status: libc::c_int) -> i32 {
    if libc::WIFSIGNALED(wait_status) {
        128 + libc::WTERMSIG(wait_status)
    } else {
        libc::WEXITSTATUS(wait_status)
    }
}

/// Points the host's standard streams at /dev/null, so the task's output reaches EOF once bash
/// and its descendants close it. A PTY stays the session's controlling terminal.
fn release_stdio() {
    let Ok(null) = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open("/dev/null")
    else {
        return;
    };
    for fd in 0..3 {
        // SAFETY: dup2 onto the standard descriptors, which the host no longer uses.
        unsafe { libc::dup2(std::os::fd::AsRawFd::as_raw_fd(&null), fd) };
    }
}

/// Ends the tree, the host included, once the lifeline reaches EOF.
#[cfg(not(target_os = "macos"))]
fn watch(mut lifeline: File) {
    read_lifeline(&mut lifeline);
    let _ = crate::process::kill_tree(std::process::id() as i32);
    std::process::exit(1);
}

/// Ends the tree on macOS once the lifeline reaches EOF: the tracker kills the members it holds,
/// then the host's own group, which the host names with its own number.
#[cfg(target_os = "macos")]
fn watch_macos(
    mut lifeline: File,
    tracker: std::sync::Arc<std::sync::Mutex<crate::tree_macos::Tracker>>,
) {
    read_lifeline(&mut lifeline);
    // The tree is held until it can be ended: a member this user may not signal keeps the host
    // alive with it, as Linux does, instead of being dropped.
    loop {
        let held = match tracker.lock() {
            Ok(mut tracker) => {
                if tracker.kill_members().is_ok() {
                    tracker.kill_group();
                    false
                } else {
                    true
                }
            }
            Err(_) => true,
        };
        if !held {
            std::process::exit(1);
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
}

/// Blocks until the server closes the lifeline, which it does when it drops the tree or dies.
fn read_lifeline(lifeline: &mut File) {
    let mut byte = [0; 1];
    loop {
        match lifeline.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => break,
        }
    }
}
