//! Bash launch in pipe or PTY mode, and ownership of each task's whole process tree.
//!
//! The launch environment, long-command scripts, and the Windows kill-on-close Job
//! Object are derived from FastCtx `src/shell/process.rs` and `src/process_policy.rs`
//! (Apache-2.0, Copyright 2026 yc-duan), modified for fastexec.

#[cfg(windows)]
use crate::env::var_in;
use crate::env::{LaunchContext, ProbeWarning, Unit};
#[cfg(windows)]
use crate::platform::conpty_native::{Attributes, ConPty, Terminal};
use crate::platform::members::{Member, MemberBudget};
#[cfg(unix)]
use crate::platform::unix_pty::open as open_pty;
#[cfg(windows)]
use crate::platform::windows_command;
use std::ffi::OsString;
use std::io::{Read, Write};
#[cfg(windows)]
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::Path;
use std::process::{Command, Stdio};

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

const ENV_MARKER: &[u8] = b"\0__FASTEXEC_ENV_START__\0";
const LOGIN_ENV_LIMIT: u64 = 1 << 20;

/// Commands longer than this run from a script file: the Git for Windows runtime cuts an
/// argument past 8,190 UTF-16 units, and a UTF-8 command has at least as many bytes as units.
pub const SCRIPT_THRESHOLD_BYTES: usize = 8_000;

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

#[cfg(test)]
mod member_tests {
    use super::{Member, clean_member_name, normalize_members};

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

/// The root of a task: bash itself on Windows, the task host (src/platform/host.rs) on Unix.
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
            } => crate::platform::conpty_native::wait(process, exit_status.as_deref()),
        };
        (msys_signal_exit(code), forced)
    }

    /// A pipe probe needs a nonblocking root check while it drains a bounded banner.
    fn try_wait(&mut self, _tree: &Tree) -> std::io::Result<Option<i32>> {
        match self {
            Child::Pipe(child) => child
                .try_wait()
                .map(|status| status.map(|status| msys_signal_exit(status.code().unwrap_or(1)))),
            Child::Pty { .. } => unreachable!("version probes use pipes"),
        }
    }

    /// Blocks until no process of the tree runs.
    pub fn wait_tree(&mut self, tree: &Tree) -> bool {
        let mut pause = std::time::Duration::from_millis(10);
        while tree.members().is_none_or(|members| !members.is_empty()) {
            std::thread::sleep(pause);
            pause = (pause * 2).min(std::time::Duration::from_millis(500));
        }
        // No more member queries need the original root identity once the tree ends.
        *lock(&tree.root) = None;
        true
    }
}

/// The root of a task: bash itself on Windows, the task host (`src/platform/host.rs`) on Unix.
#[cfg(unix)]
pub struct Child {
    host: std::process::Child,
    /// The host writes the root bash's exit code here and holds it open until it exits.
    status: std::io::PipeReader,
    /// A PTY task's terminal, held until the tree ends.
    #[expect(
        dead_code,
        reason = "held for the terminal's lifetime until the tree ends"
    )]
    master: Option<std::fs::File>,
}

#[cfg(unix)]
impl Child {
    #[cfg(test)]
    pub(crate) fn failed_host_for_test() -> (Self, std::io::PipeReader) {
        let host = Command::new("/bin/sh")
            .args(["-c", "exit 1"])
            .spawn()
            .unwrap();
        let (status, status_writer) = std::io::pipe().unwrap();
        drop(status_writer);
        let (dropped, signal) = std::io::pipe().unwrap();
        let signal: std::os::fd::OwnedFd = signal.into();
        (
            Self {
                host,
                status,
                // EOF tells the test when finish_tree has dropped the child.
                master: Some(signal.into()),
            },
            dropped,
        )
    }

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

    /// The host writes one atomic four-byte exit record. A readable or closed status pipe
    /// can therefore be consumed without waiting for a running candidate.
    fn try_wait(&mut self, tree: &Tree) -> std::io::Result<Option<i32>> {
        use std::os::fd::AsRawFd;
        let mut status = libc::pollfd {
            fd: self.status.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one initialized pollfd; zero timeout never waits.
        match unsafe { libc::poll(&mut status, 1, 0) } {
            -1 => {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::Interrupted {
                    Ok(None)
                } else {
                    Err(error)
                }
            }
            0 => Ok(None),
            _ => Ok(Some(self.wait(tree).0)),
        }
    }

    /// Blocks until no process of the tree runs: the host leaves once it has no child left.
    pub fn wait_tree(&mut self, tree: &Tree) -> bool {
        let _ = std::io::copy(&mut self.status, &mut std::io::sink());
        self.reap(tree);
        *lock(&tree.state) == TreeState::Complete
    }

    /// Reaps the host under the lock held by tree queries. Only a clean reaper exit, or the
    /// macOS host's explicit confirmation, proves completion; either outcome releases the PID.
    fn reap(&mut self, tree: &Tree) -> i32 {
        let mut state = lock(&tree.state);
        #[cfg(target_os = "macos")]
        let confirmed = tree.control.finish();
        let status = self.host.wait();
        #[cfg(not(target_os = "macos"))]
        let confirmed = status.as_ref().is_ok_and(|status| status.success());
        *state = if confirmed {
            TreeState::Complete
        } else {
            TreeState::Unconfirmed
        };
        status.map_or(1, |status| {
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
#[cfg(windows)]
pub fn quiet_command(program: impl AsRef<std::ffi::OsStr>) -> Command {
    use std::os::windows::process::CommandExt;

    let mut command = Command::new(program);
    command.creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW);
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

/// The marker is byte-oriented so profile output can precede it. The entries use native
/// units (bytes on Unix, little-endian UTF-16 on Windows), followed by an empty end record.
fn write_env_dump(
    mut out: impl Write,
    vars: impl IntoIterator<Item = (OsString, OsString)>,
) -> std::io::Result<()> {
    out.write_all(ENV_MARKER)?;
    for (mut name, value) in vars {
        name.push("=");
        name.push(value);
        name.push("\0");
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            out.write_all(name.as_bytes())?;
        }
        #[cfg(windows)]
        {
            use std::os::windows::ffi::OsStrExt;
            let bytes: Vec<_> = name.encode_wide().flat_map(u16::to_le_bytes).collect();
            out.write_all(&bytes)?;
        }
    }
    out.write_all(&[0; size_of::<Unit>()])?;
    out.flush()
}

pub fn dump_env() -> ! {
    let result = write_env_dump(std::io::stdout().lock(), std::env::vars_os());
    std::process::exit(i32::from(result.is_err()));
}

/// A path to pass to bash, preserving its native units and using MSYS separators on Windows.
fn bash_path(path: &Path) -> OsString {
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

fn fastexec_image_path() -> Result<OsString, String> {
    #[cfg(test)]
    if let Ok(exe) = std::env::current_exe()
        && let Some(bin) = exe
            .parent()
            .and_then(Path::parent)
            .map(|dir| {
                dir.join(if cfg!(windows) {
                    "fastexec.exe"
                } else {
                    "fastexec"
                })
            })
            .filter(|bin| bin.is_file())
    {
        return Ok(bash_path(&bin));
    }
    #[cfg(target_os = "linux")]
    {
        Ok(format!("/proc/{}/exe", std::process::id()).into())
    }
    #[cfg(not(target_os = "linux"))]
    {
        std::env::current_exe()
            .map(|exe| bash_path(&exe))
            .map_err(|error| format!("cannot locate fastexec image: {error}"))
    }
}

fn parse_env_dump(bytes: &[u8]) -> Result<Vec<(OsString, OsString)>, String> {
    let Some(pos) = bytes
        .windows(ENV_MARKER.len())
        .position(|window| window == ENV_MARKER)
    else {
        return Err("login shell exited without reporting its environment".to_owned());
    };
    let payload = &bytes[pos + ENV_MARKER.len()..];
    let invalid = || "login shell reported an incomplete or malformed environment".to_owned();
    #[cfg(unix)]
    let from_units = |units: &[u8]| {
        use std::os::unix::ffi::OsStrExt;
        std::ffi::OsStr::from_bytes(units).to_owned()
    };
    #[cfg(windows)]
    let (units, from_units) = {
        use std::os::windows::ffi::OsStringExt;
        let (pairs, remainder) = payload.as_chunks::<2>();
        if !remainder.is_empty() {
            return Err(invalid());
        }
        let units: Vec<_> = pairs.iter().copied().map(u16::from_le_bytes).collect();
        (units, OsString::from_wide)
    };
    #[cfg(windows)]
    let payload = units.as_slice();
    let mut records = payload.strip_suffix(&[0]).ok_or_else(invalid)?;
    let mut vars = Vec::new();
    while !records.is_empty() {
        let end = records
            .iter()
            .position(|&unit| unit == 0)
            .ok_or_else(invalid)?;
        let entry = &records[..end];
        // A Windows per-drive entry starts with '=', for example `=C:=C:\work`.
        let equals = entry
            .get(1..)
            .and_then(|rest| {
                rest.iter()
                    .position(|&unit| u32::from(unit) == u32::from(b'='))
            })
            .ok_or_else(invalid)?
            + 1;
        vars.push((
            from_units(&entry[..equals]),
            from_units(&entry[equals + 1..]),
        ));
        records = &records[end + 1..];
    }
    Ok(vars)
}

#[cfg(test)]
mod env_tests {
    use super::*;

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
        use std::os::windows::ffi::OsStringExt;
        let path = OsString::from_wide(&[67, 58, 92, 0xd800]);
        assert_eq!(
            bash_path(Path::new(&path)),
            OsString::from_wide(&[67, 58, 47, 0xd800])
        );
    }
}

/// Runs a bounded, nonblocking probe under the same host/Job ownership as a task.
fn run_probe(
    label: &str,
    launch: &Launch<'_>,
    args: Vec<OsString>,
    raw_probe_env: bool,
    output_limit: u64,
    limit_label: &str,
    timeout: std::time::Duration,
) -> Result<Vec<u8>, String> {
    let (cleanup, pending) = std::sync::mpsc::channel::<(Child, Tree)>();
    let (settled, settlement) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("fastexec-probe-reap".into())
        .spawn(move || {
            if let Ok((mut child, tree)) = pending.recv() {
                let ended = child.wait_tree(&tree);
                let _ = settled.send(ended);
            }
        })
        .map_err(|error| format!("cannot prepare the {label} reaper: {error}"))?;
    #[cfg(unix)]
    let spawned = spawn_host_args(launch, args, true, raw_probe_env);
    #[cfg(windows)]
    let spawned = spawn_pipe_args(launch, args, true, raw_probe_env);
    let Spawned {
        mut child,
        mut output,
        input,
        tree,
        ..
    } = spawned.map_err(|error| error.to_string())?;
    drop(input);
    let result = (|| {
        let deadline = std::time::Instant::now() + timeout;
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 8192];
        let mut status = None;
        let mut output_ended = false;
        loop {
            if std::time::Instant::now() >= deadline {
                return Err(format!(
                    "{label} did not finish within {} s",
                    timeout.as_secs()
                ));
            }
            if status.is_none() {
                status = child.try_wait(&tree).map_err(|error| error.to_string())?;
                if status.is_some() {
                    tree.kill()
                        .map_err(|error| format!("cannot end the {label} tree: {error}"))?;
                }
            }
            while !output_ended {
                match output.read(&mut buffer) {
                    Ok(0) => output_ended = true,
                    Ok(read) => {
                        bytes.extend_from_slice(&buffer[..read]);
                        if bytes.len() as u64 > output_limit {
                            return Err(format!("{label} printed more than {limit_label}"));
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(format!("cannot read {label} output: {error}")),
                }
            }
            if let Some(code) = status
                && output_ended
            {
                return if code == 0 {
                    Ok(bytes)
                } else {
                    Err(format!("{label} failed (exit code {code})"))
                };
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    })();
    let killed = tree.kill();
    cleanup
        .send((child, tree))
        .unwrap_or_else(|_| unreachable!("prepared probe reaper stopped"));
    if let Err(error) = killed {
        return Err(format!(
            "cannot terminate the {label} process tree: {error}"
        ));
    }
    if settlement.recv_timeout(std::time::Duration::from_secs(1)) != Ok(true) {
        return Err(format!(
            "the {label} process tree did not confirm cleanup within 1 s"
        ));
    }
    result
}

/// The banner probe uses the same host/Job ownership as a task. Its output is nonblocking,
/// so a candidate or descendant holding stdout cannot strand a reader thread after timeout.
pub(crate) fn bash_version(
    path: &Path,
    context: &LaunchContext,
    banner_limit: u64,
    timeout: std::time::Duration,
) -> Result<Vec<u8>, String> {
    let launch = Launch {
        bash: path,
        command: "",
        script: None,
        // Discovery must still work when the session's old working directory is gone.
        cwd: path
            .parent()
            .ok_or("the bash path has no parent directory")?,
        base_env: &context.env,
        #[cfg(windows)]
        used_snapshot: false,
        login: false,
        pty: false,
        pipe_stdin: false,
        cols: PTY_COLS,
        rows: PTY_ROWS,
        exit_status: None,
    };
    run_probe(
        "--version",
        &launch,
        vec!["--version".into()],
        false,
        banner_limit,
        "4 KiB",
        timeout,
    )
}

/// Returns `(base_env, used_snapshot, one_time_warning)` for a task launch.
pub fn task_env<'a>(
    launch: &'a LaunchContext,
    bash: &Path,
    login: bool,
) -> (&'a [(OsString, OsString)], bool, Option<ProbeWarning>) {
    launch.cached_task_env(login, |ctx| probe_login_env(bash, ctx))
}

/// Probes `bash -l` once per session to capture the login environment snapshot.
fn probe_login_env(
    bash: &Path,
    context: &LaunchContext,
) -> Result<Vec<(OsString, OsString)>, String> {
    let image = fastexec_image_path()?;
    let launch = Launch {
        bash,
        command: "",
        script: None,
        cwd: bash
            .parent()
            .ok_or("the bash path has no parent directory")?,
        base_env: &context.env,
        #[cfg(windows)]
        used_snapshot: false,
        login: true,
        pty: false,
        pipe_stdin: false,
        cols: PTY_COLS,
        rows: PTY_ROWS,
        exit_status: None,
    };
    let args = vec![
        "-l".into(),
        "-c".into(),
        format!("exec \"$0\" {ENV_FLAG}").into(),
        image,
    ];
    let bytes = run_probe(
        "login shell",
        &launch,
        args,
        true,
        LOGIN_ENV_LIMIT,
        "1 MiB",
        std::time::Duration::from_secs(5),
    )?;
    parse_env_dump(&bytes)
}

struct ProbeReader(std::io::PipeReader);

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

fn pipe_output(reader: std::io::PipeReader, probe: bool) -> std::io::Result<Box<dyn Read + Send>> {
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

pub(crate) fn bash_args(launch: &Launch<'_>) -> Vec<OsString> {
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

#[cfg(windows)]
fn probe_overrides(launch: &Launch<'_>) -> Vec<(&'static str, OsString)> {
    let mut env = Vec::new();
    if var_in(launch.base_env, "MSYS2_PATH_TYPE").is_none() {
        env.push(("MSYS2_PATH_TYPE", "inherit".into()));
    }
    env
}

#[cfg(windows)]
fn spawn_pipe(launch: &Launch<'_>) -> std::io::Result<Spawned> {
    spawn_pipe_args(launch, bash_args(launch), false, false)
}

#[cfg(windows)]
fn spawn_pipe_args(
    launch: &Launch<'_>,
    args: Vec<OsString>,
    probe: bool,
    raw_probe_env: bool,
) -> std::io::Result<Spawned> {
    use std::os::windows::io::AsRawHandle;
    use std::os::windows::process::CommandExt;
    use windows_sys::Win32::System::Threading::{CREATE_NO_WINDOW, CREATE_SUSPENDED};
    let (reader, writer) = std::io::pipe()?;
    let output = pipe_output(reader, probe)?;
    let overrides = if raw_probe_env {
        probe_overrides(launch)
    } else {
        environment(launch)
    };
    let mut command = quiet_command(launch.bash);
    command
        .args(args)
        .env_clear()
        .envs(launch.base_env.iter().map(|(name, value)| (name, value)))
        .envs(overrides)
        .current_dir(launch.cwd)
        .stdin(if launch.pipe_stdin {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(writer.try_clone()?)
        .stderr(writer)
        .creation_flags(CREATE_NO_WINDOW | CREATE_SUSPENDED);
    if probe {
        command.stderr(Stdio::null());
    }
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
    let input = launch.pipe_stdin.then(|| -> Box<dyn Write + Send> {
        Box::new(child.stdin.take().expect("stdin is piped"))
    });
    Ok(Spawned {
        child: Child::Pipe(child),
        output,
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

/// A root bash created suspended inside its console and already in the task's Job.
#[cfg(windows)]
struct Suspended {
    process: OwnedHandle,
    thread: OwnedHandle,
    tree: Tree,
    terminal: Terminal,
    output: Box<dyn Read + Send>,
    input: Box<dyn Write + Send>,
}

#[cfg(windows)]
fn spawn_pty(launch: &Launch<'_>) -> std::io::Result<Spawned> {
    let suspended = create_in_job(launch)?;
    if let Err(error) = resume(&suspended.thread) {
        let _ = suspended.tree.kill();
        end(&suspended.process);
        return Err(error);
    }
    let terminal = std::sync::Arc::new(suspended.terminal);
    Ok(Spawned {
        child: Child::Pty {
            process: suspended.process,
            terminal: std::sync::Arc::clone(&terminal),
            exit_status: launch.exit_status.map(std::path::Path::to_path_buf),
        },
        output: suspended.output,
        input: Some(suspended.input),
        tree: suspended.tree,
        pty: Some(PtyControl::new(terminal)),
    })
}

/// Creates bash suspended inside its console and puts it in the task's Job. The root runs no
/// instruction until [`resume`] is called on it.
#[cfg(windows)]
fn create_in_job(launch: &Launch<'_>) -> std::io::Result<Suspended> {
    use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows_sys::Win32::System::Threading::{
        CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, CreateProcessW, EXTENDED_STARTUPINFO_PRESENT,
        PROCESS_INFORMATION, STARTF_USESTDHANDLES, STARTUPINFOEXW,
    };
    let api = ConPty::load()?;
    let (input_reader, input_writer) = std::io::pipe()?;
    let (output_reader, output_writer) = std::io::pipe()?;
    let terminal = api.create(
        input_reader.as_raw_handle(),
        output_writer.as_raw_handle(),
        launch.cols,
        launch.rows,
    )?;
    // The console keeps its own copies of the two ends it was handed.
    drop((input_reader, output_writer));
    let app = windows_command::nul_terminated(launch.bash.as_os_str())?;
    let mut command =
        windows_command::command_line_of(launch.bash.as_os_str(), &bash_args(launch))?;
    let block = environment_block(launch)?;
    let cwd = windows_command::nul_terminated(launch.cwd.as_os_str())?;
    let mut attributes = Attributes::new(1)?;
    attributes.set_console(&terminal)?;
    let mut startup = STARTUPINFOEXW::default();
    startup.StartupInfo.cb = std::mem::size_of::<STARTUPINFOEXW>() as u32;
    // The console is the child's stdio. Handles inherited in their place would leak through as
    // if they were, which portable-pty guarded against the same way.
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    startup.StartupInfo.hStdInput = INVALID_HANDLE_VALUE;
    startup.StartupInfo.hStdOutput = INVALID_HANDLE_VALUE;
    startup.StartupInfo.hStdError = INVALID_HANDLE_VALUE;
    startup.lpAttributeList = attributes.as_mut();
    let mut info = PROCESS_INFORMATION::default();
    // SAFETY: every pointer refers to storage that outlives the call: the NUL-terminated app and
    // cwd, the mutable command line, the environment block, and the attribute list, which keeps
    // the console live as well.
    let created = unsafe {
        CreateProcessW(
            app.as_ptr(),
            command.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            0,
            EXTENDED_STARTUPINFO_PRESENT | CREATE_UNICODE_ENVIRONMENT | CREATE_SUSPENDED,
            block.as_ptr().cast(),
            cwd.as_ptr(),
            &startup.StartupInfo,
            &mut info,
        )
    };
    if created == 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: a successful CreateProcessW returns two handles, owned here from now on.
    let (process, thread) = unsafe {
        (
            OwnedHandle::from_raw_handle(info.hProcess),
            OwnedHandle::from_raw_handle(info.hThread),
        )
    };
    let tree = match Tree::adopt(process.as_raw_handle()) {
        Ok(tree) => tree,
        Err(error) => {
            end(&process);
            return Err(error);
        }
    };
    Ok(Suspended {
        process,
        thread,
        tree,
        terminal,
        output: Box::new(output_reader),
        input: Box::new(input_writer),
    })
}

/// Ends a root that never became a task and waits for it, so a failed start leaves no process
/// terminating behind it after its handles are released.
#[cfg(windows)]
fn end(process: &OwnedHandle) {
    use windows_sys::Win32::System::Threading::{INFINITE, TerminateProcess, WaitForSingleObject};
    // SAFETY: the handle is a live process of this call.
    unsafe {
        TerminateProcess(process.as_raw_handle(), 1);
        WaitForSingleObject(process.as_raw_handle(), INFINITE);
    }
}

#[cfg(windows)]
fn resume(thread: &OwnedHandle) -> std::io::Result<()> {
    use windows_sys::Win32::System::Threading::ResumeThread;
    // SAFETY: the thread handle is the suspended root's own and alive.
    if unsafe { ResumeThread(thread.as_raw_handle()) } == u32::MAX {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// The session's environment, then this launch's values over it, as `CreateProcessW` wants them:
/// names sorted in ordinal ignore-case order, each `name=value` NUL-terminated, and the block
/// NUL-terminated.
#[cfg(windows)]
fn environment_block(launch: &Launch<'_>) -> std::io::Result<Vec<u16>> {
    let overrides = environment(launch);
    let pairs = launch
        .base_env
        .iter()
        .map(|(name, value)| (name.as_os_str(), value.as_os_str()))
        .chain(
            overrides
                .iter()
                .map(|(name, value)| (std::ffi::OsStr::new(*name), value.as_os_str())),
        );
    windows_command::block(pairs)
}

/// Starts the task host (`src/platform/host.rs`) as the leader of a new session, with bash's command
/// line after the descriptors of its status pipe and lifeline. A PTY task's terminal becomes the
/// host's controlling terminal, so bash ending leaves the terminal and its session up.
#[cfg(unix)]
fn spawn_host(launch: &Launch<'_>) -> std::io::Result<Spawned> {
    spawn_host_args(launch, bash_args(launch), false, false)
}

#[cfg(unix)]
fn spawn_host_args(
    launch: &Launch<'_>,
    args: Vec<OsString>,
    probe: bool,
    raw_probe_env: bool,
) -> std::io::Result<Spawned> {
    use std::os::fd::AsRawFd;
    use std::os::unix::process::CommandExt;
    let (mut status, status_writer) = std::io::pipe()?;
    let (lifeline_reader, lifeline) = std::io::pipe()?;
    // macOS: the host answers member and kill requests on a control socket, because the server
    // must not signal a group by a number the kernel may have reused. Other platforms answer from
    // /proc or their job, and pass no such descriptor.
    #[cfg(target_os = "macos")]
    let (control, control_host) = crate::platform::tree_macos::socket_pair()?;
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
    let overrides = if raw_probe_env {
        Vec::new()
    } else {
        environment(launch)
    };
    let mut command = Command::new(host);
    command
        .arg(HOST_FLAG)
        .args(passed.iter().map(|fd| fd.to_string()))
        .arg(launch.bash)
        .args(args)
        // The host needs no environment of its own: it execs bash with this one.
        .env_clear()
        .envs(launch.base_env.iter().map(|(name, value)| (name, value)))
        .envs(overrides)
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
            .stdin(if launch.pipe_stdin {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(writer.try_clone()?)
            .stderr(writer);
        (None, pipe_output(reader, probe)?)
    };
    if probe {
        command.stderr(Stdio::null());
    }
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
    // Prepare every fallible PTY copy before bash can run or its host needs a waiter.
    let input = master
        .as_ref()
        .map(|master| master.try_clone())
        .transpose()?;
    let pty = master.as_ref().map(PtyControl::new).transpose()?;
    let mut host = command.spawn()?;
    // The command and these ends hold copies the host has; dropping them lets the output and the
    // status pipe reach EOF when the tree ends.
    drop(command);
    drop((status_writer, lifeline_reader));
    #[cfg(target_os = "macos")]
    drop(control_host);
    finish_host_startup(&mut host, &mut status)?;
    let input: Option<Box<dyn Write + Send>> = match input {
        Some(master) => Some(Box::new(master)),
        None if launch.pipe_stdin => Some(Box::new(host.stdin.take().expect("stdin is piped"))),
        None => None,
    };
    let tree = Tree {
        #[cfg(not(target_os = "macos"))]
        host: host.id() as i32,
        _lifeline: lifeline,
        state: std::sync::Mutex::new(TreeState::Active),
        #[cfg(target_os = "macos")]
        control: crate::platform::tree_macos::Client::new(control, host.id() as i32),
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

/// Readiness is the first status word after the bash spawn attempt finishes.
#[cfg(unix)]
fn finish_host_startup(
    host: &mut std::process::Child,
    status: &mut std::io::PipeReader,
) -> std::io::Result<()> {
    let mut startup = [0_u8; 4];
    let error = match status.read_exact(&mut startup) {
        Err(error) => std::io::Error::other(format!(
            "The task host ended before it reported readiness: {error}"
        )),
        Ok(()) => {
            let startup = u32::from_le_bytes(startup);
            if startup == HOST_STARTUP_READY {
                return Ok(());
            }
            let problem = std::io::Error::from_raw_os_error(startup as i32);
            std::io::Error::other(format!("Cannot initialize the task host: {problem}"))
        }
    };
    // Bash may have started and killed the host before READY. Its session still belongs to
    // this unreaped child, so end the reserved group before releasing the host's PID.
    // SAFETY: pre_exec established this child's session, and no wait has released its number.
    unsafe { libc::kill(-(host.id() as libc::pid_t), libc::SIGKILL) };
    let _ = host.kill();
    let _ = host.wait();
    Err(error)
}

#[cfg(all(test, unix))]
mod startup_tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::os::unix::process::CommandExt;

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

#[cfg(all(test, unix, not(target_os = "macos")))]
mod host_end_tests {
    use super::*;

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

#[cfg(unix)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TreeState {
    Active,
    Complete,
    /// The host was reaped without confirming completion; its PID may already be reused.
    Unconfirmed,
}

/// The process tree of one task.
pub struct Tree {
    #[cfg(windows)]
    job: std::os::windows::io::OwnedHandle,
    /// The original root identity stays queryable by every observer until the tree ends.
    #[cfg(windows)]
    root: std::sync::Mutex<Option<std::os::windows::io::OwnedHandle>>,
    /// The task host: session leader, process group leader, and Linux/FreeBSD reaper that
    /// adopts every orphan of the tree. macOS never names it: the host answers instead.
    #[cfg(all(unix, not(target_os = "macos")))]
    host: i32,
    /// The write end of the host's lifeline. Closing it, when the tree is dropped or the server
    /// dies, makes the host end the tree, as closing a kill-on-close job does on Windows.
    #[cfg(unix)]
    _lifeline: std::io::PipeWriter,
    /// Whether the host still reserves its PID and, after reaping, confirmed completion.
    #[cfg(unix)]
    state: std::sync::Mutex<TreeState>,
    /// The host's control socket: macOS asks the host which processes the tree holds, so no
    /// membership list or signal target lives here.
    #[cfg(target_os = "macos")]
    control: crate::platform::tree_macos::Client,
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
            let state = lock(&self.state);
            match *state {
                TreeState::Complete => return Ok(()),
                TreeState::Unconfirmed => {
                    return Err(std::io::Error::other(
                        "task host disconnected; tree cleanup is unconfirmed",
                    ));
                }
                TreeState::Active => {}
            }
            #[cfg(target_os = "macos")]
            {
                self.control.kill()
            }
            #[cfg(not(target_os = "macos"))]
            {
                kill_tree(self.host)
            }
        }
    }

    /// The processes of the tree other than its root: on Windows those in its job, apart from
    /// console hosts; on Linux the host's descendants and the members of its session and
    /// group; on FreeBSD the kernel reaper's descendants, including nested reapers.
    pub fn members(&self) -> Option<Vec<Member>> {
        self.members_with_budget(&mut MemberBudget::unlimited())
    }

    pub(crate) fn members_with_budget(&self, budget: &mut MemberBudget<'_>) -> Option<Vec<Member>> {
        let mut result = {
            #[cfg(windows)]
            {
                use std::os::windows::io::AsRawHandle;
                use windows_sys::Win32::System::Threading::GetProcessId;
                let held = lock(&self.root);
                let Some(process) = held.as_ref() else {
                    return Some(Vec::new());
                };
                let handle = process.as_raw_handle();
                // SAFETY: the held handle keeps this root's PID reserved during the query.
                let pid = unsafe { GetProcessId(handle) };
                windows::job_members_with_budget(
                    self.job.as_raw_handle(),
                    Some((pid, handle)),
                    budget,
                )
            }
            #[cfg(unix)]
            {
                let state = lock(&self.state);
                match *state {
                    TreeState::Complete => return Some(Vec::new()),
                    TreeState::Unconfirmed => return None,
                    TreeState::Active => {}
                }
                #[cfg(target_os = "macos")]
                {
                    self.control.members_with_budget(budget)
                }
                #[cfg(target_os = "freebsd")]
                {
                    crate::platform::tree_freebsd::members(self.host, budget).ok()
                }
                #[cfg(target_os = "linux")]
                {
                    crate::platform::tree_linux::members(self.host, budget).ok()
                }
                #[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
                {
                    let _ = budget;
                    Some(unix_members(self.host))
                }
            }
        };
        if let Some(list) = result.as_mut() {
            normalize_members(list);
        }
        result
    }

    #[cfg(windows)]
    pub(crate) fn adopt(process: std::os::windows::io::RawHandle) -> std::io::Result<Tree> {
        use std::os::windows::io::BorrowedHandle;
        // SAFETY: the caller owns the process handle for the duration of adoption.
        let root = unsafe { BorrowedHandle::borrow_raw(process) }.try_clone_to_owned()?;
        Ok(Tree {
            job: windows::kill_on_close_job(process)?,
            root: std::sync::Mutex::new(Some(root)),
        })
    }

    /// A dummy process tree for unit tests that inject their own `members` callback.
    #[cfg(test)]
    pub(crate) fn synthetic_for_test() -> Tree {
        #[cfg(windows)]
        {
            use std::os::windows::io::FromRawHandle;
            use windows_sys::Win32::System::JobObjects::CreateJobObjectW;
            // SAFETY: null arguments request an unnamed empty job with default security.
            let raw = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
            assert!(!raw.is_null(), "CreateJobObjectW failed");
            // SAFETY: CreateJobObjectW returned a fresh handle owned here.
            let job = unsafe { std::os::windows::io::OwnedHandle::from_raw_handle(raw) };
            Tree {
                job,
                root: std::sync::Mutex::new(None),
            }
        }
        #[cfg(unix)]
        {
            let (_, lifeline) = std::io::pipe().expect("synthetic test pipe");
            Tree {
                #[cfg(not(target_os = "macos"))]
                host: 0,
                _lifeline: lifeline,
                state: std::sync::Mutex::new(TreeState::Complete),
                #[cfg(target_os = "macos")]
                control: {
                    let (stream, _) =
                        std::os::unix::net::UnixStream::pair().expect("synthetic test socket");
                    crate::platform::tree_macos::Client::new(stream, 0)
                },
            }
        }
    }
}

/// Kills the tree whose host is `host` while it lives to adopt the orphans of each pass. An empty
/// FreeBSD host finishes its PTY drain before exiting; a dead host's reserved group is still
/// cleaned up. A member the server may not signal fails the kill and leaves the host alive.
#[cfg(all(unix, not(target_os = "macos")))]
pub fn kill_tree(host: i32) -> std::io::Result<()> {
    #[cfg(target_os = "freebsd")]
    if matches!(
        crate::platform::tree_freebsd::kill(host)?,
        crate::platform::tree_freebsd::KillResult::EmptyHost
    ) {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    crate::platform::tree_linux::kill(host)?;
    // SAFETY: plain syscall; the group ID stays reserved while the unreaped host holds it.
    if unsafe { libc::kill(-host, libc::SIGKILL) } == -1 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error);
        }
    }
    Ok(())
}

#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))
))]
fn unix_members(_host: i32) -> Vec<Member> {
    Vec::new()
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
    use crate::platform::members::{Member, MemberBudget};
    use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_INVALID_PARAMETER, ERROR_MORE_DATA, GetLastError, INVALID_HANDLE_VALUE,
        STILL_ACTIVE, WAIT_OBJECT_0, WAIT_TIMEOUT,
    };
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
    };
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, IsProcessInJob,
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
        JOBOBJECT_BASIC_PROCESS_ID_LIST, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JobObjectBasicAccountingInformation, JobObjectBasicProcessIdList,
        JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
    };
    use windows_sys::Win32::System::Threading::{
        GetExitCodeProcess, OpenProcess, OpenThread, PROCESS_QUERY_LIMITED_INFORMATION,
        PROCESS_SYNCHRONIZE, QueryFullProcessImageNameW, ResumeThread, THREAD_SUSPEND_RESUME,
        WaitForSingleObject,
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

    /// The live processes of a job, without console hosts or exited processes. A failed or
    /// incomplete query stays unknown; a process this user may not query counts, named ?.
    #[cfg(test)]
    pub fn job_members(job: RawHandle, root: Option<(u32, RawHandle)>) -> Option<Vec<Member>> {
        job_members_with_budget(job, root, &mut MemberBudget::unlimited())
    }

    pub fn job_members_with_budget(
        job: RawHandle,
        root: Option<(u32, RawHandle)>,
        budget: &mut MemberBudget<'_>,
    ) -> Option<Vec<Member>> {
        let accounting = || {
            let mut info = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
            // SAFETY: info has the layout and size requested for this job query.
            let ok = unsafe {
                QueryInformationJobObject(
                    job,
                    JobObjectBasicAccountingInformation,
                    (&mut info as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                    std::mem::size_of_val(&info) as u32,
                    std::ptr::null_mut(),
                )
            };
            (ok != 0).then_some(info)
        };
        let ids = || {
            job_pids(|buffer, bytes| {
                // SAFETY: job_pids supplies a usize-aligned buffer with room for the header and IDs.
                let queried = unsafe {
                    QueryInformationJobObject(
                        job,
                        JobObjectBasicProcessIdList,
                        buffer.as_mut_ptr().cast(),
                        bytes,
                        std::ptr::null_mut(),
                    )
                };
                if queried == 0 {
                    Err(unsafe { GetLastError() })
                } else {
                    Ok(())
                }
            })
        };
        collect_job_members(
            accounting,
            ids,
            |pid, budget| {
                if let Some((root_pid, handle)) = root
                    && root_pid == pid
                {
                    inspect_member(job, handle, true, budget)
                } else {
                    job_process(job, pid, budget)
                }
            },
            budget,
        )
    }

    fn collect_job_members(
        mut accounting: impl FnMut() -> Option<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>,
        mut ids: impl FnMut() -> Option<Vec<u32>>,
        mut inspect: impl FnMut(u32, &mut MemberBudget<'_>) -> JobProcess,
        budget: &mut MemberBudget<'_>,
    ) -> Option<Vec<Member>> {
        let before = accounting()?;
        if before.ActiveProcesses == 0 {
            return Some(Vec::new());
        }
        let mut live = Vec::new();
        let mut unknown = Vec::new();
        let initial = ids()?;
        let mut uncovered = false;
        for &pid in &initial {
            let inspected = inspect(pid, budget);
            if budget.refused() {
                return None;
            }
            match inspected {
                JobProcess::Named(name) => {
                    budget.retain(name.capacity());
                    live.push(Member { pid, name });
                }
                JobProcess::Unknown => unknown.push(pid),
                JobProcess::Gone => {}
            }
        }
        if !unknown.is_empty() {
            let mut current: std::collections::HashSet<u32> = ids()?.into_iter().collect();
            for pid in unknown.into_iter().filter(|pid| current.contains(pid)) {
                if !budget.temporary(1) {
                    return None;
                }
                live.push(Member {
                    pid,
                    name: "?".into(),
                });
                budget.retain(1);
            }
            for pid in initial {
                current.remove(&pid);
            }
            uncovered = !current.is_empty();
        }
        // A process may exit while we read its image, or a parent may fork after the PID list.
        // Zero active members is authoritative even when an earlier PID could not be inspected.
        let after = accounting()?;
        if after.ActiveProcesses == 0 {
            return Some(Vec::new());
        }
        if live.is_empty() && (uncovered || after.TotalProcesses != before.TotalProcesses) {
            return None;
        }
        Some(live)
    }

    const JOB_LIST_HEADER_WORDS: usize =
        std::mem::offset_of!(JOBOBJECT_BASIC_PROCESS_ID_LIST, ProcessIdList)
            / std::mem::size_of::<usize>();

    /// A growing job may need another snapshot; bounded retries keep that uncertainty explicit.
    fn job_pids(mut query: impl FnMut(&mut [usize], u32) -> Result<(), u32>) -> Option<Vec<u32>> {
        let mut capacity = 1024_usize;
        for _ in 0..8 {
            let words = JOB_LIST_HEADER_WORDS.checked_add(capacity)?;
            let bytes = u32::try_from(words.checked_mul(std::mem::size_of::<usize>())?).ok()?;
            let mut buffer = Vec::new();
            buffer.try_reserve_exact(words).ok()?;
            buffer.resize(words, 0_usize);
            let complete = match query(&mut buffer, bytes) {
                Ok(()) => true,
                Err(ERROR_MORE_DATA) => false,
                Err(_) => return None,
            };
            // SAFETY: usize storage has the header's alignment and at least one PID slot.
            let list = unsafe { &*buffer.as_ptr().cast::<JOBOBJECT_BASIC_PROCESS_ID_LIST>() };
            let (assigned, listed) = (
                list.NumberOfAssignedProcesses as usize,
                list.NumberOfProcessIdsInList as usize,
            );
            if listed > capacity || listed > assigned {
                return None;
            }
            if complete && listed == assigned {
                return buffer[JOB_LIST_HEADER_WORDS..JOB_LIST_HEADER_WORDS + listed]
                    .iter()
                    .map(|&pid| u32::try_from(pid).ok())
                    .collect();
            }
            capacity = capacity.checked_mul(2)?.max(assigned);
        }
        None
    }

    enum JobProcess {
        Gone,
        Named(String),
        Unknown,
    }

    fn job_process(job: RawHandle, pid: u32, budget: &mut MemberBudget<'_>) -> JobProcess {
        // SAFETY: every successful OpenProcess handle is owned and closed in this function.
        unsafe {
            let mut raw = OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                0,
                pid,
            );
            let can_wait = !raw.is_null();
            if !can_wait && GetLastError() != ERROR_INVALID_PARAMETER {
                // Query rights alone may still expose the exit code and job membership.
                raw = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            }
            if raw.is_null() {
                if GetLastError() == ERROR_INVALID_PARAMETER {
                    return JobProcess::Gone;
                }
                // A process may allow waiting without granting image or exit-code queries.
                let raw = OpenProcess(PROCESS_SYNCHRONIZE, 0, pid);
                if raw.is_null() {
                    return if GetLastError() == ERROR_INVALID_PARAMETER {
                        JobProcess::Gone
                    } else {
                        JobProcess::Unknown
                    };
                }
                let process = OwnedHandle::from_raw_handle(raw);
                return if WaitForSingleObject(process.as_raw_handle(), 0) == WAIT_OBJECT_0 {
                    JobProcess::Gone
                } else {
                    JobProcess::Unknown
                };
            }
            let process = OwnedHandle::from_raw_handle(raw);
            inspect_member(job, process.as_raw_handle(), can_wait, budget)
        }
    }

    fn inspect_member(
        job: RawHandle,
        process: RawHandle,
        can_wait: bool,
        budget: &mut MemberBudget<'_>,
    ) -> JobProcess {
        // SAFETY: both handles remain open and each buffer lives through its call.
        unsafe {
            if can_wait && WaitForSingleObject(process, 0) == WAIT_OBJECT_0 {
                return JobProcess::Gone;
            }
            let mut in_job = 0;
            let membership_known = IsProcessInJob(process, job, &mut in_job) != 0;
            if membership_known && in_job == 0 {
                return JobProcess::Gone;
            }
            if !budget.temporary(32_768 * 2) {
                return JobProcess::Unknown;
            }
            let mut buffer = vec![0_u16; 32_768];
            let mut size = buffer.len() as u32;
            let named = membership_known
                && QueryFullProcessImageNameW(process, 0, buffer.as_mut_ptr(), &mut size) != 0;
            // Inspect termination after the image lookup. A normal exit code can be available
            // during teardown; 259 is also a valid final code, so only a wait resolves it.
            let mut code = 0;
            if GetExitCodeProcess(process, &mut code) != 0 && code != STILL_ACTIVE as u32 {
                return JobProcess::Gone;
            }
            let waited = can_wait.then(|| WaitForSingleObject(process, 0));
            if waited == Some(WAIT_OBJECT_0) {
                return JobProcess::Gone;
            }
            if named {
                if !budget.temporary(buffer.capacity() * 2 + size as usize * 9) {
                    return JobProcess::Unknown;
                }
                let path = String::from_utf16_lossy(&buffer[..size as usize]);
                let name = path.rsplit('\\').next().unwrap_or(&path);
                if name.eq_ignore_ascii_case("conhost.exe") {
                    return JobProcess::Gone;
                }
                if waited == Some(WAIT_TIMEOUT) {
                    return JobProcess::Named(name.to_owned());
                }
            }
            JobProcess::Unknown
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

    #[cfg(test)]
    mod tests {
        use super::*;

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
}

#[cfg(all(test, windows))]
mod pty_tests {
    use super::*;
    use crate::env::LaunchContext;
    use windows_sys::Win32::System::Threading::{ResumeThread, SuspendThread};

    fn pid_of(process: &OwnedHandle) -> u32 {
        // SAFETY: the handle is a live process handle owned by the caller.
        unsafe { windows_sys::Win32::System::Threading::GetProcessId(process.as_raw_handle()) }
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
