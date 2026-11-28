//! Cross-platform process launch types, PTY size control, and shared helpers.

#[cfg(windows)]
use crate::env::var_in;
use crate::platform::members::Member;
use std::ffi::OsString;
use std::io::{Read, Write};
use std::path::Path;

/// Hidden CLI flag used by the per-session login environment probe.
pub const ENV_FLAG: &str = "--fastexec-dump-env";

/// The first argument that makes the fastexec binary run as a task host.
#[cfg(unix)]
pub const HOST_FLAG: &str = "--fastexec-task-host";

/// The host's first word on the status pipe: 0 after its bash spawn attempt, else a
/// Unix errno saying why it cannot own the task's tree. The root bash's exit code follows as a
/// second word.
#[cfg(unix)]
pub const HOST_STARTUP_READY: u32 = 0;

/// Commands longer than this run from a script file: the Git for Windows runtime cuts an
/// argument past 8,190 UTF-16 units, and a UTF-8 command has at least as many bytes as units.
pub const SCRIPT_THRESHOLD_BYTES: usize = 8_000;

/// PTY size; the terminal emulator behind `screen` uses the same.
pub const PTY_ROWS: u16 = 30;
pub const PTY_COLS: u16 = 120;
/// The size bounds a task's terminal may be resized to, columns first.
pub const PTY_COLS_MIN: u16 = 20;
pub const PTY_COLS_MAX: u16 = 500;
pub const PTY_ROWS_MIN: u16 = 5;
pub const PTY_ROWS_MAX: u16 = 200;

/// UTF-8 locale for commands: glibc 2.35+, musl, and msys provide `C.UTF-8`; macOS lacks it.
// ponytail: fixed locale; probe `locale -a` if an older distribution without C.UTF-8 matters.
const LOCALE: &str = if cfg!(target_os = "macos") {
    "en_US.UTF-8"
} else {
    "C.UTF-8"
};

pub struct Launch<'a> {
    pub bash: &'a Path,
    pub command: &'a str,
    /// Script file holding `command` when it exceeds [`SCRIPT_THRESHOLD_BYTES`].
    pub script: Option<&'a Path>,
    pub cwd: &'a Path,
    /// Base environment pairs for this task (snapshot or raw session environment).
    pub base_env: &'a [(OsString, OsString)],
    /// True when `base_env` already comes from the session's login-shell snapshot.
    #[cfg(windows)]
    pub used_snapshot: bool,
    pub login: bool,
    pub pty: bool,
    /// Whether a pipe task gets a writable stdin pipe rather than `/dev/null` (`NUL`).
    pub pipe_stdin: bool,
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
    pub input: Option<Box<dyn Write + Send>>,
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
    terminal: std::sync::Arc<crate::platform::conpty_native::Terminal>,
    #[cfg(unix)]
    master: std::os::fd::OwnedFd,
}

impl PtyControl {
    #[cfg(windows)]
    pub(crate) fn new(
        terminal: std::sync::Arc<crate::platform::conpty_native::Terminal>,
    ) -> PtyControl {
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

    /// Whether the current line discipline is canonical; ConPTY does not expose Unix termios.
    pub fn canonical_input(&self) -> std::io::Result<Option<bool>> {
        #[cfg(windows)]
        {
            Ok(None)
        }
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let mut attributes: libc::termios = unsafe { std::mem::zeroed() };
            // SAFETY: the owned master is live and attributes is writable for tcgetattr.
            if unsafe { libc::tcgetattr(self.master.as_raw_fd(), &mut attributes) } == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(Some(attributes.c_lflag & libc::ICANON != 0))
        }
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

/// The root of a task: bash itself on Windows, the task host (`src/platform/host.rs`) on Unix.
#[cfg(windows)]
pub enum Child {
    Pipe(std::process::Child),
    Pty {
        process: std::os::windows::io::OwnedHandle,
        /// Held until the tree ends; dropping it closes the terminal so output reaches EOF. The
        /// task's size control holds another reference while the tree runs.
        #[expect(
            dead_code,
            reason = "held for the console's lifetime until the tree ends"
        )]
        terminal: std::sync::Arc<crate::platform::conpty_native::Terminal>,
        /// Where the script records its exit status, so a root that never terminates can still
        /// end its task.
        exit_status: Option<std::path::PathBuf>,
    },
}

/// The root of a task: bash itself on Windows, the task host (`src/platform/host.rs`) on Unix.
#[cfg(unix)]
pub struct Child {
    pub(super) host: std::process::Child,
    /// The host writes the root bash's exit code here and holds it open until it exits.
    pub(super) status: std::io::PipeReader,
    /// A PTY task's terminal, held until the tree ends.
    #[expect(
        dead_code,
        reason = "held for the terminal's lifetime until the tree ends"
    )]
    pub(super) master: Option<std::fs::File>,
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum TreeState {
    Active,
    Complete,
    /// The host was reaped without confirming completion; its PID may already be reused.
    Unconfirmed,
}

/// The process tree of one task.
pub struct Tree {
    #[cfg(windows)]
    pub(super) job: std::os::windows::io::OwnedHandle,
    /// The original root identity stays queryable by every observer until the tree ends.
    #[cfg(windows)]
    pub(super) root: std::sync::Mutex<Option<std::os::windows::io::OwnedHandle>>,
    /// The task host: session leader, process group leader, and Linux/FreeBSD reaper that
    /// adopts every orphan of the tree. macOS never names it: the host answers instead.
    #[cfg(all(unix, not(target_os = "macos")))]
    pub(super) host: i32,
    /// The write end of the host's lifeline. Closing it, when the tree is dropped or the server
    /// dies, makes the host end the tree, as closing a kill-on-close job does on Windows.
    #[cfg(unix)]
    pub(super) _lifeline: std::io::PipeWriter,
    /// Whether the host still reserves its PID and, after reaping, confirmed completion.
    #[cfg(unix)]
    pub(super) state: std::sync::Mutex<TreeState>,
    /// The host's control socket: macOS asks the host which processes the tree holds, so no
    /// membership list or signal target lives here.
    #[cfg(target_os = "macos")]
    pub(super) control: crate::platform::tree_macos::Client,
}

pub(super) struct ProbeReader(std::io::PipeReader);

impl Read for ProbeReader {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        #[cfg(windows)]
        {
            use std::os::windows::io::AsRawHandle;
            use windows_sys::Win32::Foundation::ERROR_BROKEN_PIPE;
            use windows_sys::Win32::System::Pipes::PeekNamedPipe;
            let mut available = 0;
            // SAFETY: this owns a readable anonymous pipe; optional buffers are null.
            let peeked = unsafe {
                PeekNamedPipe(
                    self.0.as_raw_handle(),
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null_mut(),
                    &mut available,
                    std::ptr::null_mut(),
                )
            };
            if peeked == 0 {
                let error = std::io::Error::last_os_error();
                return if error.raw_os_error() == Some(ERROR_BROKEN_PIPE as i32) {
                    Ok(0)
                } else {
                    Err(error)
                };
            }
            if available == 0 {
                return Err(std::io::ErrorKind::WouldBlock.into());
            }
            let take = buffer.len().min(available as usize);
            self.0.read(&mut buffer[..take])
        }
        #[cfg(unix)]
        self.0.read(buffer)
    }
}

pub(super) fn pipe_output(
    reader: std::io::PipeReader,
    probe: bool,
) -> std::io::Result<Box<dyn Read + Send>> {
    if !probe {
        return Ok(Box::new(reader));
    }
    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        // SAFETY: this owns the descriptor, and both calls only update its file status flags.
        let flags = unsafe { libc::fcntl(reader.as_raw_fd(), libc::F_GETFL) };
        if flags == -1
            || unsafe { libc::fcntl(reader.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) }
                == -1
        {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(Box::new(ProbeReader(reader)))
}

pub(crate) fn normalize_members(list: &mut Vec<Member>) {
    list.sort_by_key(|m| m.pid);
    list.dedup_by_key(|m| m.pid);
}

pub(crate) fn clean_member_name(name: &str) -> Option<String> {
    let cleaned: String = name
        .chars()
        .map(|c| {
            if c.is_control() || matches!(c, '\u{2028}' | '\u{2029}') {
                ' '
            } else {
                c
            }
        })
        .collect();
    let mut parts = cleaned.split_whitespace();
    let first = parts.next()?;
    let mut out = String::from(first);
    for part in parts {
        out.push(' ');
        out.push_str(part);
    }
    Some(out)
}

/// A path to pass to bash, preserving its native units and using MSYS separators on Windows.
pub(super) fn bash_path(path: &Path) -> OsString {
    #[cfg(unix)]
    {
        path.as_os_str().to_owned()
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::{OsStrExt, OsStringExt};
        let mut units: Vec<_> = path.as_os_str().encode_wide().collect();
        for unit in &mut units {
            if *unit == u16::from(b'\\') {
                *unit = u16::from(b'/');
            }
        }
        OsString::from_wide(&units)
    }
}

pub(super) fn bash_args(launch: &Launch<'_>) -> Vec<OsString> {
    let mut args: Vec<OsString> = if launch.login {
        vec!["-l".into()]
    } else {
        vec!["--noprofile".into(), "--norc".into()]
    };
    match launch.script {
        Some(script) => args.push(bash_path(script)),
        None => {
            args.push("-c".into());
            args.push(launch.command.into());
        }
    }
    args
}

pub(super) fn environment(launch: &Launch<'_>) -> Vec<(&'static str, OsString)> {
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
        env.push(("FASTEXEC_EXIT_STATUS", bash_path(status)));
    }
    env
}

/// A login shell composes PATH in /etc/profile; msys2 installs default to `minimal`, which drops
/// the user's tools, so `inherit` is set unless the user chose a value. When a non-login shell has
/// no login snapshot, the Git toolset directories are prepended instead.
#[cfg(windows)]
fn windows_path_environment(launch: &Launch<'_>, env: &mut Vec<(&'static str, OsString)>) {
    if launch.login {
        if var_in(launch.base_env, "MSYS2_PATH_TYPE").is_none() {
            env.push(("MSYS2_PATH_TYPE", "inherit".into()));
        }
        return;
    }
    if launch.used_snapshot {
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
        var_in(launch.base_env, "PATH")
            .map(|path| std::env::split_paths(path).collect::<Vec<_>>())
            .unwrap_or_default(),
    );
    if let Ok(path) = std::env::join_paths(dirs) {
        env.push(("PATH", path));
    }
}

pub(super) fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
