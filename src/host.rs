//! The Unix task host: the root of each task's process tree, between the server and bash.
//!
//! The server starts the fastexec binary again with [`FLAG`] as the leader of a new session. The
//! host then:
//! - on Linux, becomes the child subreaper of its descendants, so processes that daemonize
//!   (`setsid`, a double fork) are adopted by it instead of init and stay in the tree;
//! - runs bash in its own session and process group, and stays its leader after bash exits, so
//!   a PTY task's terminal hangs up only when the whole tree is gone;
//! - writes bash's exit code to its status pipe, then reaps until no child is left, and exits;
//! - ends the tree when its lifeline reaches EOF: the server dropped the tree or died, even by
//!   SIGKILL.

use std::ffi::OsString;
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::{FromRawFd, RawFd};

/// The first argument that makes the fastexec binary run as a task host.
pub const FLAG: &str = "--fastexec-task-host";

extern "C" fn ignore(_: libc::c_int) {}

/// Runs the host with `args`: the status pipe's descriptor, the lifeline's descriptor, then
/// bash and its arguments.
pub fn main(args: &[OsString]) -> ! {
    let descriptor = |index: usize| -> Option<RawFd> { args.get(index)?.to_str()?.parse().ok() };
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
    #[cfg(target_os = "linux")]
    // SAFETY: plain prctl; an error leaves daemonizing processes outside the tree.
    unsafe {
        libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0);
    };
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
    let bash = match std::process::Command::new(bash).args(&args[3..]).spawn() {
        Ok(bash) => bash.id() as libc::pid_t,
        Err(error) => {
            eprintln!("fastexec: cannot start {}: {error}", bash.to_string_lossy());
            let _ = status.write_all(&127_i32.to_le_bytes());
            std::process::exit(0);
        }
    };
    release_stdio();
    std::thread::spawn(move || watch(lifeline));
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
            let code = if libc::WIFSIGNALED(wait_status) {
                128 + libc::WTERMSIG(wait_status)
            } else {
                libc::WEXITSTATUS(wait_status)
            };
            let _ = status.write_all(&code.to_le_bytes());
        }
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
fn watch(mut lifeline: File) {
    let mut byte = [0; 1];
    loop {
        match lifeline.read(&mut byte) {
            Ok(0) => break,
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(_) => break,
        }
    }
    let _ = crate::process::kill_tree(std::process::id() as i32);
    std::process::exit(1);
}
