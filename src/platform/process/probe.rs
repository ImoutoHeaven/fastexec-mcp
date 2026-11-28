//! Bounded bash version and login-environment probes.

use super::types::{Child, ENV_FLAG, Launch, PTY_COLS, PTY_ROWS, Spawned, Tree};
#[cfg(unix)]
use super::unix::spawn_host_args;
#[cfg(windows)]
use super::windows::spawn_pipe_args;
use crate::env::{LaunchContext, ProbeWarning, Unit};
use std::ffi::OsString;
use std::io::{Read, Write};
use std::path::Path;

const ENV_MARKER: &[u8] = b"\0__FASTEXEC_ENV_START__\0";
const LOGIN_ENV_LIMIT: u64 = 1 << 20;

/// The marker is byte-oriented so profile output can precede it. The entries use native
/// units (bytes on Unix, little-endian UTF-16 on Windows), followed by an empty end record.
pub(super) fn write_env_dump(
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
        return Ok(super::types::bash_path(&bin));
    }
    #[cfg(target_os = "linux")]
    {
        Ok(format!("/proc/{}/exe", std::process::id()).into())
    }
    #[cfg(not(target_os = "linux"))]
    {
        std::env::current_exe()
            .map(|exe| super::types::bash_path(&exe))
            .map_err(|error| format!("cannot locate fastexec image: {error}"))
    }
}

pub(super) fn parse_env_dump(bytes: &[u8]) -> Result<Vec<(OsString, OsString)>, String> {
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
