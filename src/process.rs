//! Bash launch in pipe or PTY mode, and ownership of each task's whole process tree.
//!
//! The launch environment, long-command scripts, and the Windows kill-on-close Job
//! Object are derived from FastCtx `src/shell/process.rs` and `src/process_policy.rs`
//! (Apache-2.0, Copyright 2026 yc-duan), modified for fastexec.

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
    pub login: bool,
    pub pty: bool,
}

pub struct Spawned {
    pub child: Child,
    pub output: Box<dyn Read + Send>,
    pub input: Box<dyn Write + Send>,
    pub tree: Tree,
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
        child: Box<dyn portable_pty::Child + Send + Sync>,
        /// Held until the tree ends; dropping it closes the terminal so output reaches EOF.
        _master: Box<dyn portable_pty::MasterPty + Send>,
    },
}

#[cfg(windows)]
impl Child {
    /// Blocks until the root bash exits and returns its bash-convention exit code.
    pub fn wait(&mut self, _tree: &Tree) -> i32 {
        let code = match self {
            Child::Pipe(child) => child.wait().map_or(1, |status| status.code().unwrap_or(1)),
            Child::Pty { child, .. } => child.wait().map_or(1, |status| status.exit_code() as i32),
        };
        msys_signal_exit(code)
    }

    /// Blocks until no process of the tree runs.
    pub fn wait_tree(&mut self, tree: &Tree) {
        let mut pause = std::time::Duration::from_millis(10);
        while !tree.members().is_empty() {
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
    _master: Option<std::fs::File>,
}

#[cfg(unix)]
impl Child {
    /// Blocks until the root bash exits and returns its bash-convention exit code.
    pub fn wait(&mut self, tree: &Tree) -> i32 {
        let mut code = [0; 4];
        if self.status.read_exact(&mut code).is_ok() {
            return i32::from_le_bytes(code);
        }
        // The host ended before bash did: the tree was killed, or a signal only SIGKILL is, such
        // as `kill -9 0`, ended the host. What still holds its session or group goes too;
        // processes that left both are out of reach.
        let _ = tree.kill();
        self.reap(tree)
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
fn msys_signal_exit(code: i32) -> i32 {
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

fn bash_args(launch: &Launch<'_>) -> Vec<OsString> {
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

fn environment(launch: &Launch<'_>) -> Vec<(&'static str, OsString)> {
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
    env
}

/// A login shell composes PATH in /etc/profile; msys2 installs default to `minimal`, which drops
/// the user's tools, so `inherit` is set unless the user chose a value. A non-login shell skips
/// the profile and gets the Git toolset directories prepended instead.
#[cfg(windows)]
fn windows_path_environment(launch: &Launch<'_>, env: &mut Vec<(&'static str, OsString)>) {
    if launch.login {
        if std::env::var_os("MSYS2_PATH_TYPE").is_none() {
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
        std::env::var_os("PATH")
            .map(|p| std::env::split_paths(&p).collect::<Vec<_>>())
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
    })
}

/// PTY size; the terminal emulator behind `screen` uses the same.
pub const PTY_ROWS: u16 = 30;
pub const PTY_COLS: u16 = 120;

#[cfg(windows)]
fn spawn_pty(launch: &Launch<'_>) -> std::io::Result<Spawned> {
    use portable_pty::{CommandBuilder, PtySize, native_pty_system};
    fn other(error: impl std::fmt::Display) -> std::io::Error {
        std::io::Error::other(error.to_string())
    }
    // Before portable-pty first opens ConPTY, so it picks the bundled one.
    crate::conpty::load();
    let pair = native_pty_system()
        .openpty(PtySize {
            rows: PTY_ROWS,
            cols: PTY_COLS,
            pixel_width: 0,
            pixel_height: 0,
        })
        .map_err(other)?;
    let mut builder = CommandBuilder::new(launch.bash);
    // portable-pty seeds the environment from the registry, overriding the server's own values
    // (PATH entries added at runtime, host-configured env); start from the process environment
    // instead so PTY tasks see exactly what pipe tasks see.
    builder.env_clear();
    for (name, value) in std::env::vars_os() {
        builder.env(name, value);
    }
    builder.args(bash_args(launch));
    builder.cwd(launch.cwd);
    for (name, value) in environment(launch) {
        builder.env(name, value);
    }
    let mut child = pair.slave.spawn_command(builder).map_err(other)?;
    drop(pair.slave);
    // ponytail: the child runs before it joins the job, so a process it starts in that window
    // escapes; spawning suspended needs a portable-pty patch.
    let tree = match child.as_raw_handle().map(Tree::adopt) {
        Some(Ok(tree)) => tree,
        Some(Err(error)) => {
            let _ = child.kill();
            return Err(error);
        }
        None => {
            let _ = child.kill();
            return Err(std::io::Error::other("the PTY child has no process handle"));
        }
    };
    let output = pair.master.try_clone_reader().map_err(other)?;
    let input = pair.master.take_writer().map_err(other)?;
    Ok(Spawned {
        child: Child::Pty {
            child,
            _master: pair.master,
        },
        output,
        input,
        tree,
    })
}

/// Starts the task host (`src/host.rs`) as the leader of a new session, with bash's command
/// line after the descriptors of its status pipe and lifeline. A PTY task's terminal becomes the
/// host's controlling terminal, so bash ending leaves the terminal and its session up.
#[cfg(unix)]
fn spawn_host(launch: &Launch<'_>) -> std::io::Result<Spawned> {
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    let (status, status_writer) = std::io::pipe()?;
    let (lifeline_reader, lifeline) = std::io::pipe()?;
    let passed = [status_writer.as_raw_fd(), lifeline_reader.as_raw_fd()];
    // /proc/self/exe stays valid when the server's binary is replaced on disk.
    let host = if cfg!(target_os = "linux") {
        "/proc/self/exe".into()
    } else {
        std::env::current_exe()?
    };
    let mut command = Command::new(host);
    command
        .arg(crate::host::FLAG)
        .args(passed.map(|fd| fd.to_string()))
        .arg(launch.bash)
        .args(bash_args(launch))
        .envs(environment(launch))
        .current_dir(launch.cwd);
    let (master, output): (_, Box<dyn Read + Send>) = if launch.pty {
        let (master, slave) = open_pty()?;
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
            for fd in passed {
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
    // Past the spawn, an error drops the lifeline, and the host ends the tree.
    let input: Box<dyn Write + Send> = match &master {
        Some(master) => Box::new(master.try_clone()?),
        None => Box::new(host.stdin.take().expect("stdin is piped")),
    };
    let tree = Tree {
        host: host.id() as i32,
        _lifeline: lifeline,
        ended: std::sync::Mutex::new(false),
    };
    Ok(Spawned {
        child: Child {
            host,
            status,
            _master: master,
        },
        output,
        input,
        tree,
    })
}

/// A 120x30 pseudo-terminal: (master, slave), neither inherited across exec.
#[cfg(unix)]
fn open_pty() -> std::io::Result<(std::fs::File, std::fs::File)> {
    use std::os::fd::FromRawFd;
    let (mut master, mut slave) = (0, 0);
    let mut size = libc::winsize {
        ws_row: PTY_ROWS,
        ws_col: PTY_COLS,
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

/// The process tree of one task.
pub struct Tree {
    #[cfg(windows)]
    job: std::os::windows::io::OwnedHandle,
    /// The task host: session leader, process group leader, and on Linux the subreaper that
    /// adopts every orphan of the tree.
    #[cfg(unix)]
    host: i32,
    /// The write end of the host's lifeline. Closing it, when the tree is dropped or the server
    /// dies, makes the host end the tree, as closing a kill-on-close job does on Windows.
    #[cfg(unix)]
    _lifeline: std::io::PipeWriter,
    /// Set once the host is about to be reaped; `kill` and `members` then do nothing.
    #[cfg(unix)]
    ended: std::sync::Mutex<bool>,
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
        #[cfg(unix)]
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
    pub fn members(&self) -> Vec<Member> {
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            windows::job_members(self.job.as_raw_handle())
        }
        #[cfg(unix)]
        {
            let ended = lock(&self.ended);
            if *ended {
                return Vec::new();
            }
            unix_members(self.host)
        }
    }

    #[cfg(windows)]
    fn adopt(process: std::os::windows::io::RawHandle) -> std::io::Result<Tree> {
        windows::kill_on_close_job(process).map(|job| Tree { job })
    }
}

/// Kills the tree whose host is `host`: its other members first, while the host lives to adopt
/// the orphans of each pass, then the host's group, the host included. A member the server may
/// not signal, such as a process sudo runs as root, fails the kill and leaves the host alive to
/// hold the tree.
#[cfg(unix)]
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

#[cfg(all(unix, not(target_os = "linux")))]
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
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
    };
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectBasicProcessIdList,
        JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
    };
    use windows_sys::Win32::System::Threading::{
        OpenProcess, OpenThread, PROCESS_QUERY_LIMITED_INFORMATION, QueryFullProcessImageNameW,
        ResumeThread, THREAD_SUSPEND_RESUME,
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
    /// console programs they serve. A process this user may not query is named `?`. Processes
    /// past the first 1024 are left out.
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
                let name = image_name(pid).unwrap_or_else(|| "?".into());
                (!name.eq_ignore_ascii_case("conhost.exe")).then_some(Member { pid, name })
            })
            .collect()
    }

    /// The file name of a live process's image.
    fn image_name(pid: u32) -> Option<String> {
        // SAFETY: the handle is checked and closed; the buffer outlives the call.
        unsafe {
            let process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            if process.is_null() {
                return None;
            }
            // The longest path Windows supports.
            let mut buffer = vec![0_u16; 32_768];
            let mut size = buffer.len() as u32;
            let named = QueryFullProcessImageNameW(process, 0, buffer.as_mut_ptr(), &mut size);
            CloseHandle(process);
            if named == 0 {
                return None;
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
