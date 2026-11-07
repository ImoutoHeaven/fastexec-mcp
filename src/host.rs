//! The Unix task host: the root of each task's process tree, between the server and bash.
//!
//! The server starts the fastexec binary again with [`FLAG`] as the leader of a new session. The
//! host then:
//! - on Linux and FreeBSD, becomes the child reaper of its descendants, so processes that daemonize
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

/// The host's first word on the status pipe: 0 after its bash spawn attempt, else a
/// Unix errno saying why it cannot own the task's tree. The root bash's exit code follows as a
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
    let startup = match crate::tree_linux::acquire() {
        Ok(()) => 0,
        Err(error) => {
            let errno = error.raw_os_error().unwrap_or(libc::EIO) as u32;
            let _ = status.write_all(&errno.to_le_bytes());
            std::process::exit(3);
        }
    };
    #[cfg(target_os = "freebsd")]
    // SAFETY: acquire reaper ownership before any descendants are created.
    let startup = unsafe {
        libc::procctl(
            libc::P_PID,
            0,
            libc::PROC_REAP_ACQUIRE,
            std::ptr::null_mut(),
        )
    };
    #[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
    let startup = 0;
    if startup == -1 {
        let errno = std::io::Error::last_os_error()
            .raw_os_error()
            .unwrap_or_default() as u32;
        let _ = status.write_all(&errno.to_le_bytes());
        std::process::exit(3);
    }
    // FreeBSD discards unread PTY bytes when the final slave closes or the session leader exits.
    // Keep one private slave until the whole tree has ended and the master has consumed its output.
    #[cfg(target_os = "freebsd")]
    let terminal = retain_terminal().unwrap_or_else(|error| fail_start(&mut status, error));
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
    // FreeBSD's <sys/signal.h> extends realtime signals through SIGRTMAX (126).
    // libc does not export that constant on FreeBSD; Linux's highest signal is 64.
    const LAST_SIGNAL: libc::c_int = if cfg!(target_os = "freebsd") { 126 } else { 64 };
    // SAFETY: each action is fully initialized and `ignore` is async-signal-safe; sigaction
    // rejects numbers that are not signals, and the mask is cleared before any thread starts.
    unsafe {
        for signal in (1..=LAST_SIGNAL).filter(|signal| !KEPT.contains(signal)) {
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
    // Lifeline cleanup waits for the one spawn attempt to finish. Otherwise a disconnected
    // server could trigger an empty-tree kill while the main thread is still creating bash.
    let spawning = std::sync::Arc::new(std::sync::Mutex::new(()));
    let spawn_guard = spawning.lock().unwrap_or_else(|error| error.into_inner());
    let after_spawn = std::sync::Arc::clone(&spawning);
    // Every helper thread must exist before bash runs. If a thread limit is exhausted, a
    // failed startup must not orphan a command that already detached from the host's group.
    #[cfg(target_os = "macos")]
    let tracker = {
        let tracker = std::sync::Arc::new(std::sync::Mutex::new(crate::tree_macos::Tracker::new()));
        let served = std::sync::Arc::clone(&tracker);
        let serving = std::sync::Arc::clone(&control);
        std::thread::Builder::new()
            .name("fastexec-control".into())
            .spawn(move || crate::tree_macos::serve(serving, served))
            .unwrap_or_else(|error| fail_start(&mut status, error));
        let ticking = std::sync::Arc::clone(&tracker);
        std::thread::Builder::new()
            .name("fastexec-track".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(std::time::Duration::from_millis(100));
                    if let Ok(mut tracker) = ticking.lock() {
                        tracker.tick();
                    }
                }
            })
            .unwrap_or_else(|error| fail_start(&mut status, error));
        let tracked = std::sync::Arc::clone(&tracker);
        std::thread::Builder::new()
            .name("fastexec-lifeline".into())
            .spawn(move || {
                let guard = after_spawn
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                drop(guard);
                watch_macos(lifeline, tracked);
            })
            .unwrap_or_else(|error| fail_start(&mut status, error));
        tracker
    };
    #[cfg(not(target_os = "macos"))]
    std::thread::Builder::new()
        .name("fastexec-lifeline".into())
        .spawn(move || {
            let guard = after_spawn
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            drop(guard);
            watch(lifeline);
        })
        .unwrap_or_else(|error| fail_start(&mut status, error));
    // macOS passes a third descriptor, so its bash arguments start one place later.
    #[cfg(target_os = "macos")]
    let tail = &args[4..];
    #[cfg(not(target_os = "macos"))]
    let tail = &args[3..];
    let bash = match std::process::Command::new(bash).args(tail).spawn() {
        Ok(bash) => bash.id() as libc::pid_t,
        Err(error) => {
            eprintln!("fastexec: cannot start {}: {error}", bash.to_string_lossy());
            let _ = status.write_all(&STARTUP_READY.to_le_bytes());
            let _ = status.write_all(&127_i32.to_le_bytes());
            #[cfg(target_os = "freebsd")]
            drain_terminal(terminal.as_ref());
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
    drop(spawn_guard);
    // A caller may kill immediately after readiness, so it must never observe a host that has
    // yet to create its command. A closed status pipe does not relinquish an existing tree:
    // the lifeline watcher owns cleanup if the server disappeared during startup.
    let _ = status.write_all(&STARTUP_READY.to_le_bytes());
    release_stdio();
    #[cfg(target_os = "macos")]
    wait_macos(bash, bash_start, &mut status, &tracker, &control);
    #[cfg(target_os = "freebsd")]
    wait_reaper(bash, &mut status, terminal);
    #[cfg(not(any(target_os = "macos", target_os = "freebsd")))]
    wait_reaper(bash, &mut status);
}

/// Startup failure is reported before the readiness word; no command has been launched.
fn fail_start(status: &mut File, error: std::io::Error) -> ! {
    let code = error.raw_os_error().unwrap_or(libc::EIO) as u32;
    let _ = status.write_all(&code.to_le_bytes());
    std::process::exit(3);
}

/// Waits for the children: Linux and FreeBSD retain every orphan under this reaper, so
/// ECHILD means no descendant is left and the tree has ended.
#[cfg(not(target_os = "macos"))]
fn wait_reaper(
    bash: libc::pid_t,
    status: &mut File,
    #[cfg(target_os = "freebsd")] terminal: Option<File>,
) -> ! {
    let mut reported = false;
    loop {
        let mut wait_status = 0;
        // SAFETY: plain syscall with a valid status pointer.
        let pid = unsafe { libc::waitpid(-1, &mut wait_status, 0) };
        if pid == -1 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ECHILD) {
                // No child or descendant is left under the Linux/FreeBSD reaper. Drain only
                // here: a kept background writer must never delay the root's exit status.
                #[cfg(target_os = "freebsd")]
                drain_terminal(terminal.as_ref());
                std::process::exit(0);
            }
            if error.kind() != std::io::ErrorKind::Interrupted {
                // An unexpected wait failure cannot prove completion or abandon owned children.
                std::thread::sleep(std::time::Duration::from_millis(250));
            }
            continue;
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

/// Keeps the PTY slave private to the host; pipe tasks retain no extra output descriptor.
#[cfg(target_os = "freebsd")]
fn retain_terminal() -> std::io::Result<Option<File>> {
    // SAFETY: stdin is installed by the parent before exec; isatty borrows that descriptor.
    if unsafe { libc::isatty(0) } == 0 {
        let error = std::io::Error::last_os_error();
        return if error.raw_os_error() == Some(libc::ENOTTY) {
            Ok(None)
        } else {
            Err(error)
        };
    }
    // SAFETY: duplicate the valid slave without exposing it to bash or any later exec.
    let fd = unsafe { libc::fcntl(0, libc::F_DUPFD_CLOEXEC, 3) };
    if fd == -1 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: fcntl returned a new descriptor owned by this File.
    Ok(Some(unsafe { File::from_raw_fd(fd) }))
}

/// FreeBSD's session teardown revokes its terminal and flushes unread output. With no child
/// writers left, tcdrain waits only for capture; closing the master releases a dead server's host.
#[cfg(target_os = "freebsd")]
fn drain_terminal(terminal: Option<&File>) {
    use std::os::fd::AsRawFd;
    let Some(terminal) = terminal else { return };
    // A command may leave another process group in the foreground. SIGTTOU must be blocked
    // for this thread's drain, even when that group is now gone; no command is spawned again.
    let mut blocked: libc::sigset_t = unsafe { std::mem::zeroed() };
    // SAFETY: the initialized signal set contains only SIGTTOU; only this thread's mask changes.
    unsafe {
        libc::sigemptyset(&mut blocked);
        libc::sigaddset(&mut blocked, libc::SIGTTOU);
    }
    // pthread_sigmask returns an error number directly. Do not drain as a background group
    // until the block is installed; a failure cannot justify discarding queued output.
    while unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &blocked, std::ptr::null_mut()) } != 0 {
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
    // Once no command can read input, late Ctrl-S/Ctrl-C must not stop or flush the final
    // output, and echoed input (including the full-queue bell) must not extend its drain.
    // Preserve output conversion; these flags affect only input interpretation and echo.
    let fd = terminal.as_raw_fd();
    let mut attributes: libc::termios = unsafe { std::mem::zeroed() };
    // SAFETY: the descriptor and termios record stay valid across each retry.
    if !terminal_operation(|| unsafe { libc::tcgetattr(fd, &mut attributes) }) {
        return;
    }
    attributes.c_iflag &= !(libc::IXON | libc::IXOFF | libc::IMAXBEL);
    attributes.c_lflag &= !(libc::ISIG | libc::IEXTEN | libc::ECHO | libc::ECHONL);
    // SAFETY: TCSANOW updates the initialized settings without flushing either queue.
    if !terminal_operation(|| unsafe { libc::tcsetattr(fd, libc::TCSANOW, &attributes) }) {
        return;
    }
    // A finished command can already have stopped output. Resume the final queue before
    // draining it; live kept trees never get here, so their input behavior is unchanged.
    for request in [libc::TIOCSTART, libc::TIOCDRAIN] {
        // SAFETY: these argument-free ioctls implement tcflow(TCOON) and tcdrain on
        // FreeBSD; the private slave stays open throughout both operations.
        if !terminal_operation(|| unsafe { libc::ioctl(fd, request, 0) }) {
            return;
        }
    }
}

/// Completes a final terminal operation, or stops once the terminal is no longer usable.
#[cfg(target_os = "freebsd")]
fn terminal_operation(mut operation: impl FnMut() -> libc::c_int) -> bool {
    loop {
        if operation() == 0 {
            return true;
        }
        let error = std::io::Error::last_os_error();
        if matches!(error.raw_os_error(), Some(libc::ENXIO) | Some(libc::ENOTTY)) {
            return false;
        }
        if error.kind() != std::io::ErrorKind::Interrupted {
            // A timeout or unexpected failure is not proof that queued output was consumed.
            std::thread::sleep(std::time::Duration::from_millis(250));
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
#[cfg(not(target_os = "macos"))]
fn watch(mut lifeline: File) {
    read_lifeline(&mut lifeline);
    // A privileged descendant may reject SIGKILL. Retain reaper ownership until it ends or
    // becomes signalable, including when the server has died and cannot retry the request.
    loop {
        if crate::process::kill_tree(std::process::id() as i32).is_ok() {
            // FreeBSD's main thread must finish the PTY drain before the session ends.
            #[cfg(target_os = "freebsd")]
            return;
            #[cfg(not(target_os = "freebsd"))]
            std::process::exit(1);
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
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
