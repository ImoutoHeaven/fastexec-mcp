//! FreeBSD's kernel reaper owns detached descendants and selects signal targets itself.

use crate::process::{Member, MemberBudget};
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum HostExit {
    /// Not a child of this process or still running outside `exit1()`.
    Running,
    /// Exited normally with code 0 after `waitpid(-1)` reached `ECHILD`.
    Clean,
    /// Terminated by signal or non-zero exit status.
    Abnormal,
}

enum HostReaper {
    Active(ReaperStatus),
    CleanExit,
    Gone(io::Error),
}

/// Peeks at an unreaped direct child `host` with `WNOWAIT` so `Child::reap` still reaps it.
/// When `PROC_REAP_STATUS` observed `OWNED == 0`, `host` is inside `exit1()` clearing its reaper
/// microseconds before `PRS_ZOMBIE` becomes visible to `waitid`, so `wait_for_zombie` polls briefly.
fn host_exit(host: libc::pid_t, wait_for_zombie: bool) -> HostExit {
    let attempts = if wait_for_zombie { 50 } else { 1 };
    for attempt in 0..attempts {
        // SAFETY: zeroed siginfo_t is valid for waitid; WNOWAIT leaves the zombie unreaped.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let rc = unsafe {
            libc::waitid(
                libc::P_PID,
                host as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOWAIT | libc::WNOHANG,
            )
        };
        if rc == 0 {
            if info.si_pid == host {
                return if info.si_code == libc::CLD_EXITED && info.si_status == 0 {
                    HostExit::Clean
                } else {
                    HostExit::Abnormal
                };
            }
            if attempt + 1 < attempts {
                std::thread::sleep(std::time::Duration::from_millis(1));
                continue;
            }
            return HostExit::Running;
        }
        let error = io::Error::last_os_error();
        if error.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return HostExit::Running;
    }
    HostExit::Running
}

fn host_status(host: libc::pid_t) -> io::Result<HostReaper> {
    let mut status = ReaperStatus::default();
    match control(host, libc::PROC_REAP_STATUS, &mut status) {
        Ok(()) if status.flags & OWNED != 0 && status.reaper == host => {
            Ok(HostReaper::Active(status))
        }
        // A dead or exiting host must never redirect an operation to its ancestor's reaper.
        Ok(()) => match host_exit(host, true) {
            HostExit::Clean => Ok(HostReaper::CleanExit),
            HostExit::Abnormal => Ok(HostReaper::Gone(io::Error::other(
                "the task host exited abnormally",
            ))),
            HostExit::Running => Err(io::Error::other("the task host no longer owns its reaper")),
        },
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => match host_exit(host, false) {
            HostExit::Clean => Ok(HostReaper::CleanExit),
            HostExit::Abnormal | HostExit::Running => Ok(HostReaper::Gone(error)),
        },
        Err(error) => Err(error),
    }
}

fn nested_status(reaper: libc::pid_t) -> io::Result<Option<ReaperStatus>> {
    let mut status = ReaperStatus::default();
    match control(reaper, libc::PROC_REAP_STATUS, &mut status) {
        Ok(()) if status.flags & OWNED != 0 && status.reaper == reaper => Ok(Some(status)),
        Ok(()) => Ok(None),
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(None),
        Err(error) => Err(error),
    }
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
        match host_status(host)? {
            HostReaper::Active(status) if status.descendants == 0 => {
                return Ok(KillResult::EmptyHost);
            }
            HostReaper::Active(_) => {}
            HostReaper::CleanExit => return Ok(KillResult::EmptyHost),
            HostReaper::Gone(_) => return Ok(KillResult::HostGone),
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
            // Some kernels also use ESRCH when signal permission hides every target, or when the
            // host exited after status was read. Only the next host_status query proves why.
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

/// Returns `Ok(None)` when `reaper` exited or released its reaper before/during `GETPIDS`, so
/// `GETPIDS` results that may have redirected to an ancestor reaper are discarded and retried.
fn descendants(
    reaper: libc::pid_t,
    initial_descendants: u32,
) -> io::Result<Option<Vec<ReaperPid>>> {
    let mut count = initial_descendants.saturating_add(16);
    // A full array can hide a concurrent fork: grow and retry instead of reporting an incomplete tree.
    for _ in 0..8 {
        let mut pids = vec![ReaperPid::default(); count as usize];
        let mut request = ReaperPids {
            count,
            padding: [0; 15],
            pids: pids.as_mut_ptr(),
        };
        match control(reaper, libc::PROC_REAP_GETPIDS, &mut request) {
            Ok(()) => {}
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => return Ok(None),
            Err(error) => return Err(error),
        }
        // Re-verify reaper ownership after GETPIDS: if reaper entered exit1() mid-call, GETPIDS
        // could have read its ancestor reaper instead of its own table.
        if nested_status(reaper)?.is_none() {
            return Ok(None);
        }
        if let Some(end) = pids.iter().position(|pid| pid.flags & VALID == 0) {
            pids.truncate(end);
            return Ok(Some(pids));
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

pub fn members(host: libc::pid_t, budget: &mut MemberBudget<'_>) -> io::Result<Vec<Member>> {
    'scan: for _ in 0..8 {
        budget.reset();
        let root = match host_status(host)? {
            HostReaper::Active(status) => status,
            HostReaper::CleanExit => return Ok(Vec::new()),
            HostReaper::Gone(error) => return Err(error),
        };
        let mut pending = vec![(host, root.descendants)];
        let mut seen = std::collections::HashSet::new();
        let mut members = Vec::new();
        while let Some((reaper, initial_descendants)) = pending.pop() {
            let Some(pids) = descendants(reaper, initial_descendants)? else {
                // Either host exited mid-query (re-checked via host_status on the next pass) or a
                // nested reaper exited and reparented its children to host.
                continue 'scan;
            };
            for pid in pids {
                if !seen.insert(pid.pid) {
                    continue;
                }
                let name = match info(pid.pid) {
                    Ok(Some(info)) => {
                        if info.ki_stat == libc::SZOMB {
                            if pid.flags & REAPER != 0 {
                                continue 'scan;
                            }
                            continue;
                        }
                        if !budget.temporary(info.ki_comm.len().saturating_mul(11)) {
                            return Err(io::Error::other(
                                "member names exceed the active memory budget",
                            ));
                        }
                        let bytes: Vec<u8> = info
                            .ki_comm
                            .iter()
                            .take_while(|byte| **byte != 0)
                            .map(|byte| *byte as u8)
                            .collect();
                        String::from_utf8_lossy(&bytes).into_owned()
                    }
                    Ok(None) => {
                        if pid.flags & REAPER != 0 {
                            continue 'scan;
                        }
                        continue;
                    }
                    Err(error) if error.raw_os_error() == Some(libc::ESRCH) => {
                        if pid.flags & REAPER != 0 {
                            continue 'scan;
                        }
                        continue;
                    }
                    // A process hidden by credential policy is still owned and must stay visible as held.
                    Err(_) => {
                        if !budget.temporary(7) {
                            return Err(io::Error::other(
                                "member names exceed the active memory budget",
                            ));
                        }
                        "unknown".into()
                    }
                };
                budget.retain(name.capacity());
                members.push(Member {
                    pid: pid.pid as u32,
                    name,
                });
                if pid.flags & REAPER != 0 {
                    // Its children may now belong to host, outside the earlier GETPIDS snapshot.
                    let Some(sub) = nested_status(pid.pid)? else {
                        continue 'scan;
                    };
                    pending.push((pid.pid, sub.descendants));
                }
            }
        }
        members.sort_by_key(|member| member.pid);
        return Ok(members);
    }
    Err(io::Error::other(
        "process tree changed during every membership query",
    ))
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

    #[test]
    fn unreaped_clean_exit_reports_empty_tree_while_signal_exit_stays_unconfirmed() {
        use std::io::Read;
        use std::process::{Command, Stdio};

        let mut clean = Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut eof = Vec::new();
        clean.stdout.take().unwrap().read_to_end(&mut eof).unwrap();
        let clean_pid = clean.id() as libc::pid_t;
        assert!(matches!(host_exit(clean_pid, true), HostExit::Clean));
        assert!(
            members(clean_pid, &mut MemberBudget::unlimited())
                .unwrap()
                .is_empty()
        );
        assert!(matches!(kill(clean_pid).unwrap(), KillResult::EmptyHost));
        assert!(clean.wait().unwrap().success());

        let mut killed = Command::new("/bin/sh")
            .args(["-c", "kill -KILL $$"])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        killed.stdout.take().unwrap().read_to_end(&mut eof).unwrap();
        let killed_pid = killed.id() as libc::pid_t;
        assert!(matches!(host_exit(killed_pid, true), HostExit::Abnormal));
        assert!(members(killed_pid, &mut MemberBudget::unlimited()).is_err());
        assert!(matches!(kill(killed_pid).unwrap(), KillResult::HostGone));
        assert!(!killed.wait().unwrap().success());
    }
}
