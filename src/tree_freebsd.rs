//! FreeBSD's kernel reaper owns detached descendants and selects signal targets itself.

use crate::process::Member;
use std::io;

// The public, padded ABI in <sys/procctl.h>; libc exposes the commands but not these records.
#[repr(C)]
#[derive(Default)]
struct ReaperStatus {
    flags: u32,
    children: u32,
    descendants: u32,
    reaper: libc::pid_t,
    pid: libc::pid_t,
    padding: [u32; 15],
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct ReaperPid {
    pid: libc::pid_t,
    subtree: libc::pid_t,
    flags: u32,
    padding: [u32; 15],
}

#[repr(C)]
struct ReaperPids {
    count: u32,
    padding: [u32; 15],
    pids: *mut ReaperPid,
}

#[repr(C)]
struct ReaperKill {
    signal: libc::c_int,
    flags: u32,
    subtree: libc::pid_t,
    killed: u32,
    failed_pid: libc::pid_t,
    padding: [u32; 15],
}

const OWNED: u32 = 1;
const VALID: u32 = 1;
const REAPER: u32 = 4;

fn control<T>(pid: libc::pid_t, command: libc::c_int, data: &mut T) -> io::Result<()> {
    // SAFETY: every caller supplies the record specified by this procctl command.
    if unsafe {
        libc::procctl(
            libc::P_PID,
            pid as libc::id_t,
            command,
            std::ptr::from_mut(data).cast(),
        )
    } == -1
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn status(host: libc::pid_t) -> io::Result<ReaperStatus> {
    let mut status = ReaperStatus::default();
    control(host, libc::PROC_REAP_STATUS, &mut status)?;
    // A dead host must never redirect an operation to its ancestor's reaper.
    if status.flags & OWNED == 0 || status.reaper != host {
        return Err(io::Error::other("the task host no longer owns its reaper"));
    }
    Ok(status)
}

pub enum KillResult {
    /// No descendant remains; let the live host drain its PTY and exit by itself.
    EmptyHost,
    /// The host is gone; the caller still reserves its PID for same-group cleanup.
    HostGone,
}

/// Kernel-selected descendants include nested reapers; signal delivery does not prove completion.
pub fn kill(host: libc::pid_t) -> io::Result<KillResult> {
    for _ in 0..40 {
        match status(host) {
            Ok(status) if status.descendants == 0 => return Ok(KillResult::EmptyHost),
            Ok(_) => {}
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => {
                return Ok(KillResult::HostGone);
            }
            Err(error) => return Err(error),
        }
        let mut request = ReaperKill {
            signal: libc::SIGKILL,
            flags: 0,
            subtree: 0,
            killed: 0,
            failed_pid: -1,
            padding: [0; 15],
        };
        match control(host, libc::PROC_REAP_KILL, &mut request) {
            // Some kernels also use ESRCH when signal permission hides every target. Only
            // the next STATUS query, never this return code, proves the reaper is empty.
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => {}
            result => result?,
        }
        if request.failed_pid != -1 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!(
                    "cannot signal descendant {} of task host {host}",
                    request.failed_pid
                ),
            ));
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    Err(io::Error::new(
        io::ErrorKind::WouldBlock,
        "descendants remain after SIGKILL; the task host stays alive",
    ))
}

fn descendants(host: libc::pid_t) -> io::Result<Vec<ReaperPid>> {
    let mut count = status(host)?.descendants.saturating_add(16);
    // A full array can hide a concurrent fork: grow and retry instead of reporting an incomplete tree.
    for _ in 0..8 {
        let mut pids = vec![ReaperPid::default(); count as usize];
        let mut request = ReaperPids {
            count,
            padding: [0; 15],
            pids: pids.as_mut_ptr(),
        };
        control(host, libc::PROC_REAP_GETPIDS, &mut request)?;
        if let Some(end) = pids.iter().position(|pid| pid.flags & VALID == 0) {
            pids.truncate(end);
            return Ok(pids);
        }
        count = count
            .checked_mul(2)
            .ok_or_else(|| io::Error::other("process tree is too large"))?;
    }
    Err(io::Error::other(
        "process tree changed during every membership query",
    ))
}

fn info(pid: libc::pid_t) -> io::Result<Option<libc::kinfo_proc>> {
    let mut mib = [libc::CTL_KERN, libc::KERN_PROC, libc::KERN_PROC_PID, pid];
    // SAFETY: kinfo_proc is a C record filled by sysctl, and the length names its full allocation.
    let mut value: libc::kinfo_proc = unsafe { std::mem::zeroed() };
    let mut length = std::mem::size_of_val(&value);
    let read = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            mib.len() as u32,
            std::ptr::from_mut(&mut value).cast(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    };
    if read == -1 {
        return Err(io::Error::last_os_error());
    }
    if length == 0 {
        return Ok(None);
    }
    if length != std::mem::size_of_val(&value) || value.ki_pid != pid {
        return Err(io::Error::other("unexpected kern.proc.pid record"));
    }
    Ok(Some(value))
}

pub fn members(host: libc::pid_t) -> io::Result<Vec<Member>> {
    let mut pending = vec![host];
    let mut seen = std::collections::HashSet::new();
    let mut members = Vec::new();
    while let Some(reaper) = pending.pop() {
        for pid in descendants(reaper)? {
            if !seen.insert(pid.pid) {
                continue;
            }
            let name = match info(pid.pid) {
                Ok(Some(info)) => {
                    if info.ki_stat == libc::SZOMB {
                        continue;
                    }
                    let bytes: Vec<u8> = info
                        .ki_comm
                        .iter()
                        .take_while(|byte| **byte != 0)
                        .map(|byte| *byte as u8)
                        .collect();
                    String::from_utf8_lossy(&bytes).into_owned()
                }
                Ok(None) => continue,
                Err(error) if error.raw_os_error() == Some(libc::ESRCH) => continue,
                // A process hidden by credential policy is still owned and must stay visible as held.
                Err(_) => "unknown".into(),
            };
            members.push(Member {
                pid: pid.pid as u32,
                name,
            });
            if pid.flags & REAPER != 0 {
                pending.push(pid.pid);
            }
        }
    }
    members.sort_by_key(|member| member.pid);
    Ok(members)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn procctl_records_preserve_the_public_padded_abi() {
        assert_eq!(std::mem::size_of::<ReaperStatus>(), 80);
        assert_eq!(std::mem::size_of::<ReaperPid>(), 72);
        assert_eq!(
            std::mem::size_of::<ReaperPids>(),
            64 + std::mem::size_of::<*mut ReaperPid>()
        );
        assert_eq!(std::mem::size_of::<ReaperKill>(), 80);
    }
}
