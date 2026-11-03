//! Bash launch in pipe or PTY mode, and ownership of each task's whole process tree.
//!
//! The launch environment, long-command scripts, and the Windows kill-on-close Job
//! Object are derived from FastCtx `src/shell/process.rs` and `src/process_policy.rs`
//! (Apache-2.0, Copyright 2026 yc-duan), modified for fastexec.

use crate::session::LaunchContext;
use std::ffi::OsString;
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};

/// Commands longer than this run from a script file: the Git for Windows runtime cuts an
/// argument past 8,190 UTF-16 units, and a UTF-8 command has at least as many bytes as units.
pub const SCRIPT_THRESHOLD_BYTES: usize = 8_000;

pub struct Launch<'a> {
    pub bash: &'a Path,
    pub command: &'a str,
    /// Script file holding `command` when it exceeds [`SCRIPT_THRESHOLD_BYTES`].
    pub script: Option<&'a Path>,
    pub cwd: &'a Path,
    /// The session's environment: a task inherits it, never the server's own.
    pub env: &'a LaunchContext,
    pub login: bool,
    pub pty: bool,
    /// The terminal's size: columns, then screen rows.
    pub cols: u16,
    pub rows: u16,
    /// Where a Windows ConPTY root's script writes its exit status, so a root that never
    /// terminates still ends its task. `None` for pipe roots and other platforms.
    pub exit_status: Option<&'a Path>,
}

pub struct Spawned {
    pub child: Child,
    pub output: Box<dyn Read + Send>,
    pub input: Box<dyn Write + Send>,
    pub tree: Tree,
    /// A PTY task's size control, which resizes the terminal the program is on.
    pub pty: Option<PtyControl>,
}

/// A task's PTY size control: the terminal handle the program sees, shared with capture, the
/// ender, and resize, so resizing tells the program its terminal changed size. It stays open
/// while the task's tree runs and is dropped with the terminal, so a resize that arrives after
/// the tree ended finds it taken.
pub struct PtyControl {
    #[cfg(windows)]
    terminal: std::sync::Arc<crate::conpty_spawn::Terminal>,
    #[cfg(unix)]
    master: std::os::fd::OwnedFd,
}

impl PtyControl {
    #[cfg(windows)]
    pub(crate) fn new(terminal: std::sync::Arc<crate::conpty_spawn::Terminal>) -> PtyControl {
        PtyControl { terminal }
    }

    /// Shares the terminal master of `master` without taking its lifetime: the copy closes with
    /// the control, so the output still reaches EOF when the tree ends.
    #[cfg(unix)]
    pub(crate) fn new(master: &std::fs::File) -> std::io::Result<PtyControl> {
        use std::os::fd::AsFd;
        Ok(PtyControl {
            master: master.as_fd().try_clone_to_owned()?,
        })
    }

    /// The terminal is `cols` by `rows` from now on, and the program is told: Unix sends
    /// SIGWINCH to its process group, and ConPTY repaints the whole screen.
    pub fn resize(&self, cols: u16, rows: u16) -> std::io::Result<()> {
        #[cfg(windows)]
        {
            self.terminal.resize(cols, rows)
        }
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let size = libc::winsize {
                ws_row: rows,
                ws_col: cols,
                ws_xpixel: 0,
                ws_ypixel: 0,
            };
            // SAFETY: the descriptor is the task's own terminal master, and `size` is a winsize.
            let set = unsafe { libc::ioctl(self.master.as_raw_fd(), libc::TIOCSWINSZ, &size) };
            if set == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        }
    }
}

/// A process of a task's tree other than its root bash.
#[derive(Clone, Debug)]
pub struct Member {
    pub pid: u32,
    pub name: String,
}

/// The root of a task: bash itself on Windows, the task host (`src/host.rs`) on Unix.
#[cfg(windows)]
pub enum Child {
    Pipe(std::process::Child),
    Pty {
        process: std::os::windows::io::OwnedHandle,
        /// Held until the tree ends; dropping it closes the terminal so output reaches EOF. The
        /// task's size control holds another reference while the tree runs.
        #[allow(dead_code)] // held for the console's lifetime, never read here
        terminal: std::sync::Arc<crate::conpty_spawn::Terminal>,
        /// Where the script records its exit status, so a root that never terminates can still
        /// end its task.
        exit_status: Option<std::path::PathBuf>,
    },
}

#[cfg(windows)]
impl Child {
    /// Blocks until the root bash exits and returns its bash-convention exit code, and whether the
    /// end was forced because the script had finished while the root did not terminate.
    pub fn wait(&mut self, _tree: &Tree) -> (i32, bool) {
        let (code, forced) = match self {
            Child::Pipe(child) => (
                child.wait().map_or(1, |status| status.code().unwrap_or(1)),
                false,
            ),
            // The console's exit code carries the same signal convention the pipe's does.
            Child::Pty {
                process,
                exit_status,
                ..
            } => crate::conpty_spawn::wait(process, exit_status.as_deref()),
        };
        (msys_signal_exit(code), forced)
    }

    /// Blocks until no process of the tree runs.
    pub fn wait_tree(&mut self, tree: &Tree) {
        let mut pause = std::time::Duration::from_millis(10);
        while tree.members().is_none_or(|members| !members.is_empty()) {
            std::thread::sleep(pause);
            pause = (pause * 2).min(std::time::Duration::from_millis(500));
        }
    }
}

/// The root of a task: bash itself on Windows, the task host (`src/host.rs`) on Unix.
#[cfg(unix)]
pub struct Child {
    host: std::process::Child,
    /// The host writes the root bash's exit code here and holds it open until it exits.
    status: std::io::PipeReader,
    /// A PTY task's terminal, held until the tree ends.
    #[allow(dead_code)] // held for the terminal's lifetime, never read here
    master: Option<std::fs::File>,
}

#[cfg(unix)]
impl Child {
    /// Blocks until the root bash exits and returns its bash-convention exit code. A root that
    /// never terminates is not this platform's failure mode, so the end is never forced.
    pub fn wait(&mut self, tree: &Tree) -> (i32, bool) {
        let mut code = [0; 4];
        if self.status.read_exact(&mut code).is_ok() {
            return (i32::from_le_bytes(code), false);
        }
        // The host ended before bash did: the tree was killed, or a signal only SIGKILL is, such
        // as `kill -9 0`, ended the host. What still holds its session or group goes too;
        // processes that left both are out of reach.
        let _ = tree.kill();
        (self.reap(tree), false)
    }

    /// Blocks until no process of the tree runs: the host leaves once it has no child left.
    pub fn wait_tree(&mut self, tree: &Tree) {
        let _ = std::io::copy(&mut self.status, &mut std::io::sink());
        self.reap(tree);
    }

    /// Reaps the host once it has exited, which the end of its status pipe shows. The tree is
    /// marked ended first, under the lock `kill` holds, so no kill can reach a group ID that the
    /// reaped PID frees for reuse.
    fn reap(&mut self, tree: &Tree) -> i32 {
        *lock(&tree.ended) = true;
        self.host.wait().map_or(1, |status| {
            use std::os::unix::process::ExitStatusExt;
            status
                .signal()
                .map_or_else(|| status.code().unwrap_or(1), |signal| 128 + signal)
        })
    }
}

/// The msys runtime reports a process killed by signal N as exit status N << 8.
// ponytail: a native Windows program that exits with exactly 256..=16384 in steps of 256 is
// read as a signal exit too.
#[cfg(windows)]
pub(crate) fn msys_signal_exit(code: i32) -> i32 {
    if code & 0xff == 0 && (1..=64).contains(&(code >> 8)) {
        128 + (code >> 8)
    } else {
        code
    }
}

/// Creates a command that never allocates a console window on Windows.
pub fn quiet_command(program: impl AsRef<std::ffi::OsStr>) -> Command {
    #[allow(unused_mut)]
    let mut command = Command::new(program);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
    }
    command
}

pub fn spawn(launch: &Launch<'_>) -> std::io::Result<Spawned> {
    #[cfg(windows)]
    return if launch.pty {
        spawn_pty(launch)
    } else {
        spawn_pipe(launch)
    };
    #[cfg(unix)]
    return spawn_host(launch);
}

pub(crate) fn bash_args(launch: &Launch<'_>) -> Vec<OsString> {
    let mut args: Vec<OsString> = if launch.login {
        vec!["-l".into()]
    } else {
        vec!["--noprofile".into(), "--norc".into()]
    };
    match launch.script {
        // msys passes a native path through cleanly only with forward slashes.
        Some(script) => args.push(script.to_string_lossy().replace('\\', "/").into()),
        None => {
            args.push("-c".into());
            args.push(launch.command.into());
        }
    }
    args
}

pub(crate) fn environment(launch: &Launch<'_>) -> Vec<(&'static str, OsString)> {
    let mut env: Vec<(&'static str, OsString)> = vec![
        ("LANG", LOCALE.into()),
        ("LC_ALL", LOCALE.into()),
        ("PAGER", "cat".into()),
        ("GIT_PAGER", "cat".into()),
        ("PYTHONUNBUFFERED", "1".into()),
        ("PYTHONIOENCODING", "utf-8".into()),
    ];
    if launch.pty {
        env.push(("TERM", "xterm-256color".into()));
    } else {
        for (name, value) in [
            ("TERM", "dumb"),
            ("NO_COLOR", "1"),
            ("CLICOLOR", "0"),
            ("FORCE_COLOR", "0"),
            ("GIT_EDITOR", "true"),
            ("EDITOR", "true"),
            ("VISUAL", "true"),
            ("GIT_TERMINAL_PROMPT", "0"),
        ] {
            env.push((name, value.into()));
        }
    }
    #[cfg(windows)]
    windows_path_environment(launch, &mut env);
    if let Some(status) = launch.exit_status {
        // msys reads a native path only with forward slashes.
        env.push((
            "FASTEXEC_EXIT_STATUS",
            status.to_string_lossy().replace('\\', "/").into(),
        ));
    }
    env
}

/// A login shell composes PATH in /etc/profile; msys2 installs default to `minimal`, which drops
/// the user's tools, so `inherit` is set unless the user chose a value. A non-login shell skips
/// the profile and gets the Git toolset directories prepended instead.
#[cfg(windows)]
fn windows_path_environment(launch: &Launch<'_>, env: &mut Vec<(&'static str, OsString)>) {
    if launch.login {
        if launch.env.var("MSYS2_PATH_TYPE").is_none() {
            env.push(("MSYS2_PATH_TYPE", "inherit".into()));
        }
        return;
    }
    let Some(usr_bin) = launch.bash.parent() else {
        return;
    };
    let mut dirs = vec![usr_bin.to_path_buf()];
    if let Some(root) = usr_bin.parent().and_then(Path::parent) {
        for arch in ["mingw64", "mingw32", "clangarm64"] {
            let bin = root.join(arch).join("bin");
            if bin.is_dir() {
                dirs.push(bin);
            }
        }
    }
    dirs.extend(
        launch
            .env
            .var("PATH")
            .map(|path| std::env::split_paths(path).collect::<Vec<_>>())
            .unwrap_or_default(),
    );
    if let Ok(path) = std::env::join_paths(dirs) {
        env.push(("PATH", path));
    }
}

#[cfg(windows)]
fn spawn_pipe(launch: &Launch<'_>) -> std::io::Result<Spawned> {
    use std::os::windows::io::AsRawHandle;
    use std::os::windows::process::CommandExt;
    use windows_sys::Win32::System::Threading::{CREATE_NO_WINDOW, CREATE_SUSPENDED};
    let (reader, writer) = std::io::pipe()?;
    let mut command = quiet_command(launch.bash);
    command
        .args(bash_args(launch))
        .env_clear()
        .envs(launch.env.env.iter().map(|(name, value)| (name, value)))
        .envs(environment(launch))
        .current_dir(launch.cwd)
        .stdin(Stdio::piped())
        .stdout(writer.try_clone()?)
        .stderr(writer)
        .creation_flags(CREATE_NO_WINDOW | CREATE_SUSPENDED);
    let mut child = command.spawn()?;
    // The command still holds the pipe's write ends; dropping it lets the reader reach EOF.
    drop(command);
    let adopted = Tree::adopt(child.as_raw_handle()).and_then(|tree| {
        windows::resume_threads(child.id())?;
        Ok(tree)
    });
    let tree = match adopted {
        Ok(tree) => tree,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
    };
    let input = Box::new(child.stdin.take().expect("stdin is piped"));
    Ok(Spawned {
        child: Child::Pipe(child),
        output: Box::new(reader),
        input,
        tree,
        pty: None,
    })
}

/// PTY size; the terminal emulator behind `screen` uses the same.
pub const PTY_ROWS: u16 = 30;
pub const PTY_COLS: u16 = 120;
/// The size bounds a task's terminal may be resized to, columns first.
pub const PTY_COLS_MIN: u16 = 20;
pub const PTY_COLS_MAX: u16 = 500;
pub const PTY_ROWS_MIN: u16 = 5;
pub const PTY_ROWS_MAX: u16 = 200;

#[cfg(windows)]
fn spawn_pty(launch: &Launch<'_>) -> std::io::Result<Spawned> {
    crate::conpty_spawn::spawn_pty(launch)
}

/// Starts the task host (`src/host.rs`) as the leader of a new session, with bash's command
/// line after the descriptors of its status pipe and lifeline. A PTY task's terminal becomes the
/// host's controlling terminal, so bash ending leaves the terminal and its session up.
#[cfg(unix)]
fn spawn_host(launch: &Launch<'_>) -> std::io::Result<Spawned> {
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    let (mut status, status_writer) = std::io::pipe()?;
    let (lifeline_reader, lifeline) = std::io::pipe()?;
    // macOS: the host answers member and kill requests on a control socket, because the server
    // must not signal a group by a number the kernel may have reused. Other platforms answer from
    // /proc or their job, and pass no such descriptor.
    #[cfg(target_os = "macos")]
    let (control, control_host) = crate::tree_macos::socket_pair()?;
    #[cfg(target_os = "macos")]
    let passed = vec![
        status_writer.as_raw_fd(),
        lifeline_reader.as_raw_fd(),
        std::os::fd::AsRawFd::as_raw_fd(&control_host),
    ];
    #[cfg(not(target_os = "macos"))]
    let passed = vec![status_writer.as_raw_fd(), lifeline_reader.as_raw_fd()];
    // /proc/self/exe stays valid when the server's binary is replaced on disk.
    let host = if cfg!(target_os = "linux") {
        "/proc/self/exe".into()
    } else {
        std::env::current_exe()?
    };
    let mut command = Command::new(host);
    command
        .arg(crate::host::FLAG)
        .args(passed.iter().map(|fd| fd.to_string()))
        .arg(launch.bash)
        .args(bash_args(launch))
        // The host needs no environment of its own: it execs bash with this one.
        .env_clear()
        .envs(launch.env.env.iter().map(|(name, value)| (name, value)))
        .envs(environment(launch))
        .current_dir(launch.cwd);
    let (master, output): (_, Box<dyn Read + Send>) = if launch.pty {
        let (master, slave) = open_pty(launch.cols, launch.rows)?;
        command
            .stdin(slave.try_clone()?)
            .stdout(slave.try_clone()?)
            .stderr(slave);
        let output = Box::new(master.try_clone()?);
        (Some(master), output)
    } else {
        let (reader, writer) = std::io::pipe()?;
        command
            .stdin(Stdio::piped())
            .stdout(writer.try_clone()?)
            .stderr(writer);
        (None, Box::new(reader))
    };
    let pty = launch.pty;
    // SAFETY: setsid, ioctl, and fcntl are async-signal-safe.
    unsafe {
        command.pre_exec(move || {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            // Stdio is in place by now, so descriptor 0 is the terminal.
            if pty && libc::ioctl(0, libc::TIOCSCTTY as _, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            for &fd in &passed {
                if libc::fcntl(fd, libc::F_SETFD, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    let mut host = command.spawn()?;
    // The command and these ends hold copies the host has; dropping them lets the output and the
    // status pipe reach EOF when the tree ends.
    drop(command);
    drop((status_writer, lifeline_reader));
    #[cfg(target_os = "macos")]
    drop(control_host);
    // The host reports whether it runs bash before the task is reported as started; this word is
    // the first thing it writes to its status pipe, before it starts bash.
    let mut startup = [0_u8; 4];
    if let Err(error) = status.read_exact(&mut startup) {
        let _ = host.kill();
        let _ = host.wait();
        return Err(std::io::Error::other(format!(
            "The task host ended before it reported readiness: {error}"
        )));
    }
    let startup = u32::from_le_bytes(startup);
    if startup != crate::host::STARTUP_READY {
        let _ = host.kill();
        let _ = host.wait();
        let problem = std::io::Error::from_raw_os_error(startup as i32);
        return Err(std::io::Error::other(format!(
            "Cannot become the subreaper of the task tree: {problem}"
        )));
    }
    // Past the spawn, an error drops the lifeline, and the host ends the tree.
    let input: Box<dyn Write + Send> = match &master {
        Some(master) => Box::new(master.try_clone()?),
        None => Box::new(host.stdin.take().expect("stdin is piped")),
    };
    let pty = match &master {
        Some(master) => Some(PtyControl::new(master)?),
        None => None,
    };
    let tree = Tree {
        #[cfg(not(target_os = "macos"))]
        host: host.id() as i32,
        _lifeline: lifeline,
        ended: std::sync::Mutex::new(false),
        #[cfg(target_os = "macos")]
        control: crate::tree_macos::Client::new(control, host.id() as i32),
    };
    Ok(Spawned {
        child: Child {
            host,
            status,
            master,
        },
        output,
        input,
        tree,
        pty,
    })
}

/// A pseudo-terminal of `cols` by `rows`: (master, slave), neither inherited across exec.
#[cfg(unix)]
fn open_pty(cols: u16, rows: u16) -> std::io::Result<(std::fs::File, std::fs::File)> {
    use std::os::fd::FromRawFd;
    let (mut master, mut slave) = (0, 0);
    let mut size = libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: every pointer is valid for the call; null name and termios are allowed.
    let opened = unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &raw mut size,
        )
    };
    if opened == -1 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: openpty returned two fresh descriptors owned here.
    let pair = unsafe {
        (
            std::fs::File::from_raw_fd(master),
            std::fs::File::from_raw_fd(slave),
        )
    };
    for fd in [master, slave] {
        // SAFETY: plain fcntl on a descriptor owned by `pair`.
        if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(pair)
}

#[cfg(unix)]
fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(all(test, unix))]
mod refusing_control_tests {
    use super::*;

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

/// The process tree of one task.
pub struct Tree {
    #[cfg(windows)]
    job: std::os::windows::io::OwnedHandle,
    /// The task host: session leader, process group leader, and on Linux the subreaper that
    /// adopts every orphan of the tree. macOS never names it: the host answers instead.
    #[cfg(all(unix, not(target_os = "macos")))]
    host: i32,
    /// The write end of the host's lifeline. Closing it, when the tree is dropped or the server
    /// dies, makes the host end the tree, as closing a kill-on-close job does on Windows.
    #[cfg(unix)]
    _lifeline: std::io::PipeWriter,
    /// Set once the host is about to be reaped; `kill` and `members` then do nothing.
    #[cfg(unix)]
    ended: std::sync::Mutex<bool>,
    /// The host's control socket: macOS asks the host which processes the tree holds, so no
    /// membership list or signal target lives here.
    #[cfg(target_os = "macos")]
    control: crate::tree_macos::Client,
}

impl Tree {
    /// Terminates every process in the tree. Killing an empty tree succeeds.
    pub fn kill(&self) -> std::io::Result<()> {
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            // SAFETY: the job handle is owned by this tree and alive.
            let ended = unsafe {
                windows_sys::Win32::System::JobObjects::TerminateJobObject(
                    self.job.as_raw_handle(),
                    1,
                )
            };
            if ended == 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        }
        #[cfg(target_os = "macos")]
        {
            let ended = lock(&self.ended);
            if *ended {
                return Ok(());
            }
            self.control.kill()
        }
        #[cfg(all(unix, not(target_os = "macos")))]
        {
            let ended = lock(&self.ended);
            if *ended {
                return Ok(());
            }
            kill_tree(self.host)
        }
    }

    /// The processes of the tree other than its root: on Windows those in its job, apart from
    /// console hosts; on Linux the host's descendants and the members of its session and
    /// group. Other Unix systems list none.
    pub fn members(&self) -> Option<Vec<Member>> {
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            Some(windows::job_members(self.job.as_raw_handle()))
        }
        #[cfg(target_os = "macos")]
        {
            let ended = lock(&self.ended);
            if *ended {
                return Some(Vec::new());
            }
            // The host is still unreaped, so its number is reserved: a request that waits wakes
            // it, and one it never answers is `None`, which says nothing about the tree.
            self.control.members()
        }
        #[cfg(all(unix, not(target_os = "macos")))]
        {
            let ended = lock(&self.ended);
            if *ended {
                return Some(Vec::new());
            }
            Some(unix_members(self.host))
        }
    }

    #[cfg(windows)]
    pub(crate) fn adopt(process: std::os::windows::io::RawHandle) -> std::io::Result<Tree> {
        windows::kill_on_close_job(process).map(|job| Tree { job })
    }
}

/// Kills the tree whose host is `host`: its other members first, while the host lives to adopt
/// the orphans of each pass, then the host's group, the host included. A member the server may
/// not signal, such as a process sudo runs as root, fails the kill and leaves the host alive to
/// hold the tree.
#[cfg(all(unix, not(target_os = "macos")))]
pub fn kill_tree(host: i32) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        // A pass kills the members it sees; their children reparent to the host for the next.
        let mut denied = None;
        for _ in 0..8 {
            let members = unix_members(host);
            denied = None;
            if members.is_empty() {
                break;
            }
            for member in members {
                // SAFETY: plain syscall on a PID read from /proc this pass.
                if unsafe { libc::kill(member.pid as i32, libc::SIGKILL) } == -1 {
                    let error = std::io::Error::last_os_error();
                    if error.raw_os_error() != Some(libc::ESRCH) {
                        denied = Some(error);
                    }
                }
            }
        }
        if let Some(error) = denied {
            return Err(error);
        }
    }
    // SAFETY: plain syscall; the group ID stays reserved while the unreaped host holds it.
    if unsafe { libc::kill(-host, libc::SIGKILL) } == -1 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error);
        }
    }
    Ok(())
}

/// Live processes other than `host` that descend from it or share its session or group, from
/// /proc. Job control (`set -m`) moves processes into other groups of the session.
#[cfg(target_os = "linux")]
fn unix_members(host: i32) -> Vec<Member> {
    struct Entry {
        pid: i32,
        ppid: i32,
        group: i32,
        session: i32,
        zombie: bool,
        name: String,
    }
    let Ok(dir) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };
    let entries: Vec<Entry> = dir
        .flatten()
        .filter_map(|entry| {
            let pid = entry.file_name().to_str()?.parse::<i32>().ok()?;
            // comm holds whatever bytes a process named itself with.
            let stat = std::fs::read(format!("/proc/{pid}/stat")).ok()?;
            let stat = String::from_utf8_lossy(&stat);
            // pid (comm) state ppid pgrp session ...; comm may hold spaces and parentheses.
            let (head, rest) = stat.rsplit_once(')')?;
            let name = head.split_once('(')?.1.to_string();
            let fields: Vec<&str> = rest.split_whitespace().collect();
            let number = |index: usize| fields.get(index)?.parse::<i32>().ok();
            Some(Entry {
                pid,
                ppid: number(1)?,
                group: number(2)?,
                session: number(3)?,
                zombie: fields.first() == Some(&"Z"),
                name,
            })
        })
        .collect();
    let mut inside = std::collections::HashSet::from([host]);
    // Parents can come after their children in /proc order, so passes repeat until none adds.
    loop {
        let before = inside.len();
        for entry in &entries {
            if inside.contains(&entry.ppid) || entry.session == host || entry.group == host {
                inside.insert(entry.pid);
            }
        }
        if inside.len() == before {
            break;
        }
    }
    entries
        .into_iter()
        .filter(|entry| entry.pid != host && !entry.zombie && inside.contains(&entry.pid))
        .map(|entry| Member {
            pid: entry.pid as u32,
            name: entry.name,
        })
        .collect()
}

#[cfg(all(unix, not(target_os = "linux"), not(target_os = "macos")))]
fn unix_members(_host: i32) -> Vec<Member> {
    Vec::new()
}

/// Reports whether a process with this PID exists.
pub fn is_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // SAFETY: signal 0 only checks for existence and permission.
        let exists = unsafe { libc::kill(pid as i32, 0) } == 0;
        exists || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }
    #[cfg(windows)]
    {
        use windows_sys::Win32::Foundation::{CloseHandle, STILL_ACTIVE};
        use windows_sys::Win32::System::Threading::{
            GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
        };
        // SAFETY: the handle is checked and closed; the exit code buffer outlives the call.
        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            if handle.is_null() {
                // A process this user may not query still exists.
                return std::io::Error::last_os_error().raw_os_error()
                    == Some(windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED as i32);
            }
            let mut code = 0;
            let alive = GetExitCodeProcess(handle, &mut code) != 0 && code == STILL_ACTIVE as u32;
            CloseHandle(handle);
            alive
        }
    }
}

/// UTF-8 locale for commands: glibc 2.35+, musl, and msys provide `C.UTF-8`; macOS lacks it.
// ponytail: fixed locale; probe `locale -a` if an older distribution without C.UTF-8 matters.
pub const LOCALE: &str = if cfg!(target_os = "macos") {
    "en_US.UTF-8"
} else {
    "C.UTF-8"
};

#[cfg(windows)]
mod windows {
    use super::Member;
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_INVALID_PARAMETER, GetLastError, INVALID_HANDLE_VALUE, STILL_ACTIVE,
    };
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
    };
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectBasicProcessIdList,
        JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
    };
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, OpenThread, PROCESS_QUERY_LIMITED_INFORMATION,
        QueryFullProcessImageNameW, ResumeThread, THREAD_SUSPEND_RESUME,
    };

    /// Creates a job that terminates its processes when its last handle closes, and assigns
    /// `process` to it. Descendants join the job automatically.
    pub fn kill_on_close_job(process: RawHandle) -> std::io::Result<OwnedHandle> {
        // SAFETY: null arguments request an unnamed job with default security.
        let raw = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if raw.is_null() {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: CreateJobObjectW returned a fresh handle owned here.
        let job = unsafe { OwnedHandle::from_raw_handle(raw) };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: the buffer matches the information class and outlives the call.
        let configured = unsafe {
            SetInformationJobObject(
                job.as_raw_handle(),
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                std::mem::size_of_val(&limits) as u32,
            )
        };
        // SAFETY: both handles are alive for the call.
        if configured == 0 || unsafe { AssignProcessToJobObject(job.as_raw_handle(), process) } == 0
        {
            return Err(std::io::Error::last_os_error());
        }
        Ok(job)
    }

    /// The live processes of a job, without console hosts, which come and go with the
    /// console programs they serve. A process that has exited but not left the job's list yet
    /// is left out; one this user may not query counts, named `?`. Processes past the first
    /// 1024 are left out.
    pub fn job_members(job: RawHandle) -> Vec<Member> {
        // JOBOBJECT_BASIC_PROCESS_ID_LIST with room for 1024 IDs.
        #[repr(C)]
        struct List {
            assigned: u32,
            listed: u32,
            ids: [usize; 1024],
        }
        let mut list = List {
            assigned: 0,
            listed: 0,
            ids: [0; 1024],
        };
        // SAFETY: the buffer has the layout of the information class and outlives the call.
        // A full buffer fails with ERROR_MORE_DATA after filling it, so the result is not checked.
        unsafe {
            QueryInformationJobObject(
                job,
                JobObjectBasicProcessIdList,
                (&mut list as *mut List).cast(),
                std::mem::size_of::<List>() as u32,
                std::ptr::null_mut(),
            )
        };
        let listed = (list.listed as usize).min(list.ids.len());
        list.ids[..listed]
            .iter()
            .filter_map(|&pid| {
                let pid = pid as u32;
                let name = live_name(pid)?;
                (!name.eq_ignore_ascii_case("conhost.exe")).then_some(Member { pid, name })
            })
            .collect()
    }

    /// The file name of a process's image, `?` when it cannot be read, or `None` when the
    /// process has exited or is gone.
    fn live_name(pid: u32) -> Option<String> {
        // SAFETY: the handle is checked and closed; the buffers outlive the calls.
        unsafe {
            let process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            if process.is_null() {
                // An ID no process has any more is an invalid parameter.
                return (GetLastError() != ERROR_INVALID_PARAMETER).then(|| "?".into());
            }
            let mut code = 0;
            let exited = GetExitCodeProcess(process, &mut code) != 0 && code != STILL_ACTIVE as u32;
            // The longest path Windows supports.
            let mut buffer = vec![0_u16; 32_768];
            let mut size = buffer.len() as u32;
            let named = !exited
                && QueryFullProcessImageNameW(process, 0, buffer.as_mut_ptr(), &mut size) != 0;
            CloseHandle(process);
            if exited {
                return None;
            }
            if !named {
                return Some("?".into());
            }
            let path = String::from_utf16_lossy(&buffer[..size as usize]);
            Some(path.rsplit('\\').next().unwrap_or(&path).to_string())
        }
    }

    /// Resumes every thread of a process created with `CREATE_SUSPENDED`.
    pub fn resume_threads(pid: u32) -> std::io::Result<()> {
        // SAFETY: the snapshot handle is closed below on every path.
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if snapshot == INVALID_HANDLE_VALUE {
            return Err(std::io::Error::last_os_error());
        }
        let mut entry = THREADENTRY32 {
            dwSize: std::mem::size_of::<THREADENTRY32>() as u32,
            ..Default::default()
        };
        let mut result = Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "no suspended thread found",
        ));
        // SAFETY: entry has the documented size and lives across the enumeration.
        let mut more = unsafe { Thread32First(snapshot, &mut entry) } != 0;
        while more {
            if entry.th32OwnerProcessID == pid {
                // SAFETY: the thread id comes from the live snapshot; the handle is closed below.
                let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
                if thread.is_null() {
                    result = Err(std::io::Error::last_os_error());
                    break;
                }
                // SAFETY: the handle has THREAD_SUSPEND_RESUME access.
                let resumed = unsafe { ResumeThread(thread) };
                // SAFETY: this function owns the thread handle.
                unsafe { CloseHandle(thread) };
                if resumed == u32::MAX {
                    result = Err(std::io::Error::last_os_error());
                    break;
                }
                result = Ok(());
            }
            // SAFETY: as for Thread32First.
            more = unsafe { Thread32Next(snapshot, &mut entry) } != 0;
        }
        // SAFETY: this function owns the snapshot handle.
        unsafe { CloseHandle(snapshot) };
        result
    }
}
