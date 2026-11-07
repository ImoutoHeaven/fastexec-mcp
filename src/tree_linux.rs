//! Linux subreaper ownership; pidfds keep a scanned PID from becoming an unrelated kill target.

use crate::process::Member;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

struct Pinned(OwnedFd);

impl Pinned {
    fn open(pid: i32) -> io::Result<Self> {
        // SAFETY: pidfd_open takes a PID and zero flags and returns a new owned descriptor.
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(unsafe { OwnedFd::from_raw_fd(fd as i32) }))
    }

    fn signal(&self, signal: i32) -> io::Result<()> {
        // SAFETY: the descriptor pins one process; a null siginfo asks for ordinary signal semantics.
        let result = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.0.as_raw_fd(),
                signal,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        };
        if result < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn exited(&self) -> io::Result<bool> {
        let mut fd = libc::pollfd {
            fd: self.0.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: the initialized array has one valid descriptor, and the poll never waits.
        let result = unsafe { libc::poll(&mut fd, 1, 0) };
        if result < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(result > 0)
    }
}

/// Verify both pidfd syscalls before a task can create children that require safe cleanup.
pub fn acquire() -> io::Result<()> {
    Pinned::open(std::process::id() as i32)?.signal(0)?;
    // SAFETY: this process has not spawned children yet, and the flag is inherited by none of them.
    if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[derive(Debug)]
struct Entry {
    pid: i32,
    parent: i32,
    group: i32,
    session: i32,
    start: u64,
    zombie: bool,
    name: String,
}

fn parse(pid: i32, bytes: &[u8]) -> io::Result<Entry> {
    let stat = String::from_utf8_lossy(bytes);
    let (head, rest) = stat
        .rsplit_once(')')
        .ok_or_else(|| io::Error::other("invalid process stat"))?;
    let name = head
        .split_once('(')
        .ok_or_else(|| io::Error::other("invalid process name"))?
        .1
        .to_string();
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let number = |index: usize| -> io::Result<i32> {
        fields
            .get(index)
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| io::Error::other("invalid process ancestry"))
    };
    let start = fields
        .get(19)
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| io::Error::other("invalid process start time"))?;
    Ok(Entry {
        pid,
        parent: number(1)?,
        group: number(2)?,
        session: number(3)?,
        start,
        zombie: fields.first() == Some(&"Z"),
        name,
    })
}

fn entry(pid: i32) -> io::Result<Option<Entry>> {
    match std::fs::read(format!("/proc/{pid}/stat")) {
        Ok(bytes) => parse(pid, &bytes).map(Some),
        Err(error) if matches!(error.raw_os_error(), Some(libc::ENOENT | libc::ESRCH)) => Ok(None),
        Err(error) => Err(error),
    }
}

fn snapshot(host: i32) -> io::Result<Vec<Entry>> {
    let mut entries = Vec::new();
    for item in std::fs::read_dir("/proc")? {
        let item = item?;
        let Some(pid) = item.file_name().to_str().and_then(|name| name.parse().ok()) else {
            continue;
        };
        if let Some(entry) = entry(pid)? {
            entries.push(entry);
        }
    }
    if !entries.iter().any(|entry| entry.pid == host) {
        return Err(io::Error::other("the task host is absent from /proc"));
    }
    let inside = owned(host, entries.iter());
    Ok(entries
        .into_iter()
        .filter(|entry| entry.pid != host && !entry.zombie && inside.contains(&entry.pid))
        .collect())
}

fn owned<'a>(
    host: i32,
    entries: impl Iterator<Item = &'a Entry> + Clone,
) -> std::collections::HashSet<i32> {
    let mut inside = std::collections::HashSet::from([host]);
    loop {
        let before = inside.len();
        for entry in entries.clone() {
            if inside.contains(&entry.parent) || entry.session == host || entry.group == host {
                inside.insert(entry.pid);
            }
        }
        if inside.len() == before {
            break;
        }
    }
    inside
}

pub fn members(host: i32) -> io::Result<Vec<Member>> {
    Ok(snapshot(host)?
        .into_iter()
        .map(|entry| Member {
            pid: entry.pid as u32,
            name: entry.name,
        })
        .collect())
}

pub fn kill(host: i32) -> io::Result<()> {
    let reaper = Pinned::open(host)?;
    for _ in 0..40 {
        let entries = snapshot(host)?;
        // An empty scan can overlap an orphan's adoption. The host itself exits only after
        // waitpid confirms ECHILD; wait for its pidfd rather than killing a still-owning reaper.
        if entries.is_empty() && reaper.exited()? {
            return Ok(());
        }
        // The first scan only selects candidates. Pin every candidate before reading any
        // authoritative ancestry record, then rebuild membership. Comparing start ticks alone
        // would admit an unrelated successor if a numeric PID was reused within the same tick.
        let mut pinned = Vec::with_capacity(entries.len());
        for old in entries {
            match Pinned::open(old.pid) {
                Ok(handle) => pinned.push((old, handle)),
                Err(error) if error.raw_os_error() == Some(libc::ESRCH) => {}
                Err(error) => return Err(error),
            }
        }
        let mut current = Vec::with_capacity(pinned.len());
        for (old, handle) in pinned {
            if let Some(entry) = entry(old.pid)?
                && entry.start == old.start
                && !entry.zombie
                && !handle.exited()?
            {
                current.push((entry, handle));
            }
        }
        let inside = owned(host, current.iter().map(|(entry, _)| entry));
        let mut denied = None;
        for (entry, handle) in current {
            if inside.contains(&entry.pid)
                && let Err(error) = handle.signal(libc::SIGKILL)
                && error.raw_os_error() != Some(libc::ESRCH)
            {
                denied = Some(error);
            }
        }
        if let Some(error) = denied {
            return Err(error);
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    Err(io::Error::new(
        io::ErrorKind::WouldBlock,
        "descendants remain after SIGKILL; the task host stays alive",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stat_names_may_contain_parentheses_and_non_utf8_bytes() {
        let mut stat = b"27 (a ) \xff name) S 4 5 6 ".to_vec();
        stat.extend_from_slice(format!("{}123 0", "0 ".repeat(15)).as_bytes());
        let entry = parse(27, &stat).unwrap();
        assert_eq!(
            (entry.parent, entry.group, entry.session, entry.start),
            (4, 5, 6, 123)
        );
        assert_eq!(entry.name, "a ) \u{fffd} name");
        assert!(parse(27, b"27 (broken) S").is_err());
    }

    #[test]
    fn a_pidfd_for_a_reaped_process_cannot_signal_a_later_process() {
        let mut first = std::process::Command::new("/bin/sh")
            .args(["-c", "exit 0"])
            .spawn()
            .unwrap();
        let pinned = Pinned::open(first.id() as i32).unwrap();
        first.wait().unwrap();
        let mut second = std::process::Command::new("/bin/sh")
            .args(["-c", "exec sleep 30"])
            .spawn()
            .unwrap();
        assert!(pinned.exited().unwrap());
        assert_eq!(
            pinned.signal(libc::SIGKILL).unwrap_err().raw_os_error(),
            Some(libc::ESRCH)
        );
        assert!(second.try_wait().unwrap().is_none());
        second.kill().unwrap();
        second.wait().unwrap();
    }
}
