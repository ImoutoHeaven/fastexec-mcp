//! Unix task-host spawning and process-tree lifecycle.

use super::types::{
    Child, HOST_FLAG, HOST_STARTUP_READY, Launch, PtyControl, Spawned, Tree, TreeState, bash_args,
    environment, lock, normalize_members, pipe_output,
};
use crate::platform::members::{Member, MemberBudget};
use crate::platform::unix_pty::open as open_pty;
use std::ffi::OsString;
use std::io::{Read, Write};
use std::process::{Command, Stdio};

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
    pub(super) fn try_wait(&mut self, tree: &Tree) -> std::io::Result<Option<i32>> {
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
    pub(super) fn reap(&mut self, tree: &Tree) -> i32 {
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

impl Tree {
    /// Terminates every process in the tree. Killing an empty tree succeeds.
    pub fn kill(&self) -> std::io::Result<()> {
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

    /// The processes of the tree other than its root: on Linux the host's descendants and the
    /// members of its session and group; on FreeBSD the kernel reaper's descendants, including
    /// nested reapers; on macOS those reported by the task host.
    pub fn members(&self) -> Option<Vec<Member>> {
        self.members_with_budget(&mut MemberBudget::unlimited())
    }

    pub(crate) fn members_with_budget(&self, budget: &mut MemberBudget<'_>) -> Option<Vec<Member>> {
        let mut result = {
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
        };
        if let Some(list) = result.as_mut() {
            normalize_members(list);
        }
        result
    }

    /// A dummy process tree for unit tests that inject their own `members` callback.
    #[cfg(test)]
    pub(crate) fn synthetic_for_test() -> Tree {
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

/// Starts the task host (`src/platform/host.rs`) as the leader of a new session, with bash's
/// command line after the descriptors of its status pipe and lifeline. A PTY task's terminal
/// becomes the host's controlling terminal, so bash ending leaves the terminal and its session up.
pub fn spawn(launch: &Launch<'_>) -> std::io::Result<Spawned> {
    spawn_host_args(launch, bash_args(launch), false, false)
}

pub(super) fn spawn_host_args(
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
pub(super) fn finish_host_startup(
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

/// Kills the tree whose host is `host` while it lives to adopt the orphans of each pass. An empty
/// FreeBSD host finishes its PTY drain before exiting; a dead host's reserved group is still
/// cleaned up. A member the server may not signal fails the kill and leaves the host alive.
#[cfg(not(target_os = "macos"))]
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

#[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "freebsd")))]
fn unix_members(_host: i32) -> Vec<Member> {
    Vec::new()
}
