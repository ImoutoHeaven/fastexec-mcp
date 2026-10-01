//! Bash launch in pipe or PTY mode, and ownership of each task's whole process tree.
//!
//! The launch environment, long-command scripts, and the Windows kill-on-close Job
//! Object are derived from FastCtx `src/shell/process.rs` and `src/process_policy.rs`
//! (Apache-2.0, Copyright 2026 yc-duan), modified for fastexec.

use portable_pty::{CommandBuilder, MasterPty, PtySize, native_pty_system};
use std::ffi::OsString;
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};

/// Commands longer than this run from a script file: Windows caps a command line at 32,767
/// UTF-16 units and argument quoting can double the text.
pub const SCRIPT_THRESHOLD_BYTES: usize = 12_000;

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

pub enum Child {
    Pipe(std::process::Child),
    Pty {
        child: Box<dyn portable_pty::Child + Send + Sync>,
        /// Held until the child exits; dropping it closes the terminal so output reaches EOF.
        _master: Box<dyn MasterPty + Send>,
    },
}

impl Child {
    /// Blocks until the root process exits and returns its bash-convention exit code.
    pub fn wait(&mut self) -> i32 {
        let code = match self {
            Child::Pipe(child) => child.wait().map_or(1, |status| {
                #[cfg(unix)]
                {
                    use std::os::unix::process::ExitStatusExt;
                    if let Some(signal) = status.signal() {
                        return 128 + signal;
                    }
                }
                status.code().unwrap_or(1)
            }),
            Child::Pty { child, .. } => child.wait().map_or(1, |status| match status.signal() {
                Some(name) => 128 + signal_number(name),
                None => status.exit_code() as i32,
            }),
        };
        msys_signal_exit(code)
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

#[cfg(unix)]
fn msys_signal_exit(code: i32) -> i32 {
    code
}

#[cfg(unix)]
fn signal_number(name: &str) -> i32 {
    // portable-pty reports signals by their strsignal() description.
    (1..65)
        .find(|&signal| {
            // SAFETY: strsignal returns a pointer to a static or thread-local string.
            let text = unsafe { libc::strsignal(signal) };
            !text.is_null() && unsafe { std::ffi::CStr::from_ptr(text) }.to_string_lossy() == name
        })
        .unwrap_or(0)
}

#[cfg(windows)]
fn signal_number(_name: &str) -> i32 {
    0
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
    if launch.pty {
        spawn_pty(launch)
    } else {
        spawn_pipe(launch)
    }
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

fn spawn_pipe(launch: &Launch<'_>) -> std::io::Result<Spawned> {
    let (reader, writer) = std::io::pipe()?;
    let mut command = quiet_command(launch.bash);
    command
        .args(bash_args(launch))
        .envs(environment(launch))
        .current_dir(launch.cwd)
        .stdin(Stdio::piped())
        .stdout(writer.try_clone()?)
        .stderr(writer);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        use windows_sys::Win32::System::Threading::{CREATE_NO_WINDOW, CREATE_SUSPENDED};
        command.creation_flags(CREATE_NO_WINDOW | CREATE_SUSPENDED);
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // SAFETY: setsid is async-signal-safe; the child leads a new session and process group.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
    let mut child = command.spawn()?;
    // The command still holds the pipe's write ends; dropping it lets the reader reach EOF.
    drop(command);
    #[cfg(windows)]
    let tree = {
        use std::os::windows::io::AsRawHandle;
        let adopted = Tree::adopt(child.as_raw_handle()).and_then(|tree| {
            windows::resume_threads(child.id())?;
            Ok(tree)
        });
        match adopted {
            Ok(tree) => tree,
            Err(error) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error);
            }
        }
    };
    #[cfg(unix)]
    let tree = Tree {
        pgid: child.id() as i32,
    };
    let input = Box::new(child.stdin.take().expect("stdin is piped"));
    Ok(Spawned {
        child: Child::Pipe(child),
        output: Box::new(reader),
        input,
        tree,
    })
}

fn spawn_pty(launch: &Launch<'_>) -> std::io::Result<Spawned> {
    fn other(error: impl std::fmt::Display) -> std::io::Error {
        std::io::Error::other(error.to_string())
    }
    let pair = native_pty_system()
        .openpty(PtySize {
            rows: 30,
            cols: 120,
            pixel_width: 0,
            pixel_height: 0,
        })
        .map_err(other)?;
    let mut builder = CommandBuilder::new(launch.bash);
    // On Windows, portable-pty seeds the environment from the registry, overriding the server's
    // own values (PATH entries added at runtime, host-configured env); start from the process
    // environment instead so PTY tasks see exactly what pipe tasks see.
    builder.env_clear();
    for (name, value) in std::env::vars_os() {
        builder.env(name, value);
    }
    builder.args(bash_args(launch));
    builder.cwd(launch.cwd);
    for (name, value) in environment(launch) {
        builder.env(name, value);
    }
    #[cfg_attr(unix, allow(unused_mut))]
    let mut child = pair.slave.spawn_command(builder).map_err(other)?;
    drop(pair.slave);
    #[cfg(windows)]
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
    #[cfg(unix)]
    // portable-pty calls setsid before exec, so the child PID is the process group ID.
    let tree = Tree {
        pgid: child.process_id().unwrap_or(0) as i32,
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

/// The process tree of one task.
pub struct Tree {
    #[cfg(windows)]
    job: std::os::windows::io::OwnedHandle,
    #[cfg(unix)]
    pgid: i32,
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
        }
        #[cfg(unix)]
        {
            if self.pgid <= 0 {
                return Err(std::io::Error::other("the task has no process group"));
            }
            // SAFETY: plain syscall; a group ID cannot be reused while the group has members.
            if unsafe { libc::kill(-self.pgid, libc::SIGKILL) } == -1 {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(libc::ESRCH) {
                    return Err(error);
                }
            }
            // Job control (`set -m`) moves descendants into other groups of the same session; the
            // root's PID is also its session ID. Processes that start a new session leave the tree.
            #[cfg(target_os = "linux")]
            {
                let mut failure = None;
                for _ in 0..3 {
                    let members = session_members(self.pgid);
                    if members.is_empty() {
                        break;
                    }
                    for pid in members {
                        // SAFETY: plain syscall on a PID read from /proc this pass.
                        if unsafe { libc::kill(pid, libc::SIGKILL) } == -1 {
                            let error = std::io::Error::last_os_error();
                            if error.raw_os_error() != Some(libc::ESRCH) {
                                failure.get_or_insert(error);
                            }
                        }
                    }
                }
                if let Some(error) = failure {
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    #[cfg(windows)]
    fn adopt(process: std::os::windows::io::RawHandle) -> std::io::Result<Tree> {
        windows::kill_on_close_job(process).map(|job| Tree { job })
    }
}

/// PIDs whose session ID is `sid`, from /proc/<pid>/stat.
#[cfg(target_os = "linux")]
fn session_members(sid: i32) -> Vec<i32> {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| entry.file_name().to_str()?.parse::<i32>().ok())
        .filter(|&pid| {
            let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
                return false;
            };
            // Fields after the parenthesized command: state ppid pgrp session ...
            let fields = stat.rsplit_once(')').map_or("", |(_, rest)| rest);
            fields
                .split_whitespace()
                .nth(3)
                .and_then(|field| field.parse::<i32>().ok())
                == Some(sid)
        })
        .collect()
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
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
    };
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
        SetInformationJobObject,
    };
    use windows_sys::Win32::System::Threading::{OpenThread, ResumeThread, THREAD_SUSPEND_RESUME};

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
