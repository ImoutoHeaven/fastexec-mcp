//! macOS tree ownership: the host tracks the tree, the server asks about it.
//!
//! On macOS nothing adopts an orphan: a descendant whose parent exits is reparented to launchd,
//! so `waitpid` reaching ECHILD says nothing about the tree, and a process group ID read by the
//! server can outlive the process it named. The task host owns membership instead, tracking the
//! processes it started by identity — pid with its start time — and answering two requests over
//! the control socket it was started with: list the members, and kill the tree. The server keeps
//! no membership set and never signals a macOS group by number.
//!
//! The control socket is its own channel, so the status pipe keeps carrying only the startup word
//! and the root's exit code, and its EOF keeps meaning what it did.
//!
//! Wire format, both directions little-endian: a request is one byte, `M` or `K`. A members reply
//! is `n` followed by a count and, per member, its pid, its name, and its name; a kill reply is
//! `O`, or `E` with the errno of a member the host may not signal. The host writes `O` before the
//! group signal ends it, so a socket that closes without a reply is a kill that succeeded.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::os::fd::{FromRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::process::Member;

/// `SZOMBIE` from `<sys/proc.h>`: the state of a process that has ended and not been reaped.
const ZOMBIE: u32 = 5;

/// One cadence for every scan the host makes: the tracking thread, a member request, and a kill
/// all read the same scan when it is this recent, so the tree is never re-read once per caller.
const CADENCE: Duration = Duration::from_millis(100);

/// A socket pair: the first end belongs to the server, the second to the host. `UnixStream` sets
/// close-on-exec on both ends, so neither bash nor another task's host inherits a channel it could
/// answer on; the server clears that on the host end alone for the exec into the host, which sets
/// it again before bash starts.
pub fn socket_pair() -> std::io::Result<(UnixStream, UnixStream)> {
    UnixStream::pair()
}

/// Takes the descriptor of the host's control socket, which the server passed so bash does not
/// inherit it, and closes it across exec again: the server cleared that flag for the exec into
/// this process only.
pub fn from_fd(fd: RawFd) -> UnixStream {
    // SAFETY: the caller passes a descriptor this process owns.
    let stream = unsafe { UnixStream::from_raw_fd(fd) };
    // SAFETY: plain fcntl; the host keeps this end for its own requests, bash must not get it.
    unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) };
    stream
}

/// What one live process shows: who its parent is, which group and session it is in, when it
/// started, its state, and what it is called.
struct Entry {
    ppid: i32,
    group: i32,
    session: i32,
    start: u64,
    status: u32,
    name: String,
}

/// What one scan established: every process it could describe, the ones it could not, the ones it
/// found as zombies, and whether it described the whole process table at all.
struct Scan {
    live: HashMap<i32, Entry>,
    unknown: HashSet<i32>,
    zombies: HashSet<i32>,
    complete: bool,
}

/// The members of one task's tree, as the host sees them. Identity is the pid with its start
/// time, so a reused pid is never the member that had it.
pub struct Tracker {
    members: HashMap<(i32, u64), u64>,
    /// The host's own process group and session, which every member starts in.
    group: i32,
    session: i32,
    /// When the last scan ran, so every caller shares one cadence instead of rescanning.
    last_scan: Option<Instant>,
    /// Whether the last scan described every process; an incomplete one proves nothing.
    complete: bool,
}

impl Tracker {
    /// A tracker for this process's tree: the host leads the task's session and group.
    pub fn new() -> Tracker {
        // SAFETY: plain syscalls with no pointer arguments to get wrong.
        let (group, session) = unsafe { (libc::getpgrp(), libc::getsid(0)) };
        Tracker {
            members: HashMap::new(),
            group,
            session,
            last_scan: None,
            complete: false,
        }
    }

    /// Whether the tree holds a process. A scan that could not describe every process proves
    /// nothing, so it reads as a tree that still holds one.
    pub fn empty(&mut self) -> bool {
        self.refresh(false);
        self.complete && self.members.is_empty()
    }

    /// The tracking cadence: one scan, at most one per `CADENCE`, driven by the host's own thread.
    pub fn tick(&mut self) {
        self.refresh(false);
    }

    /// The live members: everything the scan still finds by identity, whether or not it stayed in
    /// the session, group, or parent chain it was admitted by.
    pub fn list(&mut self) -> Vec<Member> {
        self.refresh(false);
        let mut members: Vec<(i32, String)> = Vec::new();
        let mut disproved: Vec<(i32, u64)> = Vec::new();
        for &(pid, start) in self.members.keys() {
            match membership(&describe(pid), start) {
                // The number now belongs to another process, or the process ended: it is not a
                // member, and it is never reported as a held one.
                Membership::Drop => disproved.push((pid, start)),
                Membership::Keep(name) => members.push((pid, name)),
                // Unreadable: it may still run, so it stays a member with no name to show.
                Membership::KeepUnnamed => members.push((pid, String::new())),
            }
        }
        for key in disproved {
            self.members.remove(&key);
        }
        members.sort();
        members
            .into_iter()
            .map(|(pid, name)| Member {
                pid: pid as u32,
                name,
            })
            .collect()
    }

    /// Kills every member by identity, in passes, and reports the first member the host may not
    /// signal — sudo or root — which leaves the host holding the tree, as on Linux. A member whose
    /// identity could not be confirmed is not treated as killed.
    pub fn kill_members(&mut self) -> std::io::Result<()> {
        let mut denied = None;
        for pass in 0..8 {
            self.refresh(true);
            let keys: Vec<(i32, u64)> = self.members.keys().copied().collect();
            if keys.is_empty() {
                // Nothing to kill is only a finished kill when the scan that found it empty read
                // the whole process table.
                return self.confirmed_empty();
            }
            denied = None;
            for (pid, start) in keys {
                match describe(pid) {
                    // The identity is checked again right before the signal: a member whose pid was
                    // reused after the scan is dropped, not signalled as its successor.
                    Lookup::Entry(entry) if entry.start != start => {
                        self.members.remove(&(pid, start));
                    }
                    // The kernel says it ended: nothing to kill and nothing to hold.
                    Lookup::Gone => {
                        self.members.remove(&(pid, start));
                    }
                    // A zombie runs nothing: the next scan drops it.
                    Lookup::Entry(entry) if entry.status == ZOMBIE => {}
                    Lookup::Entry(_) => denied = denied.or(kill_member(pid)),
                    // The process could not be described: it may still run, so it is neither
                    // killed nor reported gone.
                    Lookup::Unknown => {}
                }
            }
            if pass < 7 {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        if let Some(error) = denied {
            return Err(error);
        }
        self.refresh(true);
        if self.members.is_empty() {
            return self.confirmed_empty();
        }
        Err(std::io::Error::other(
            "a member of the tree could not be identified",
        ))
    }

    /// Signals the host's own group, the last step of a kill. The host names the group with its
    /// own number, so it never reaches another task's group, and it ends here, which is what makes
    /// a kill's success observable as the closed control socket.
    pub fn kill_group(&self) {
        // SAFETY: plain syscall; the group ID is this process's own and stays reserved while the
        // unreaped host holds it.
        unsafe { libc::kill(-self.group, libc::SIGKILL) };
    }

    /// Drops a process that ended from the members and hands over fresh membership, so a list
    /// taken right after the root exits cannot show it as a leftover.
    pub fn forget(&mut self, pid: i32, start: u64) {
        self.members.remove(&(pid, start));
        self.refresh(true);
    }

    /// An empty set is a finished kill only when the scan that emptied it read the whole process
    /// table.
    fn confirmed_empty(&self) -> std::io::Result<()> {
        if self.complete {
            Ok(())
        } else {
            Err(std::io::Error::other(
                "the process table could not be read to confirm the kill",
            ))
        }
    }

    /// Scans when the last one is older than the cadence, or always when a kill needs the truth.
    fn refresh(&mut self, force: bool) {
        let stale = self.last_scan.is_none_or(|last| last.elapsed() >= CADENCE);
        if force || stale {
            self.scan();
        }
    }

    /// One bounded scan: the process table read once, then settled by [`Tracker::apply`].
    fn scan(&mut self) {
        self.last_scan = Some(Instant::now());
        match describe_all() {
            Some(scan) => self.apply(&scan),
            None => self.complete = false,
        }
    }

    /// Settles membership from one scan: passes so a child listed before its parent is still
    /// admitted, and no member dropped on the strength of a scan that failed.
    fn apply(&mut self, scan: &Scan) {
        self.complete = scan.complete;
        // A member stays a member while the process it named exists: a descendant that leaves the
        // session, group, and parent chain after it was seen keeps its place in the tree. A scan
        // prunes only what it disproves — the same pid with a different start time, or one that is
        // now a zombie — and keeps what it could not describe, since that process may still run.
        // A partial scan cannot prove absence; a complete one can.
        self.members
            .retain(|&(pid, start), _| match scan.live.get(&pid) {
                Some(entry) => entry.start == start,
                None if scan.zombies.contains(&pid) => false,
                None if scan.unknown.contains(&pid) => true,
                None => !scan.complete,
            });
        self.admit(scan);
    }

    /// Admits the processes the scan shows as members, in passes so a child listed before its
    /// parent still settles. Identity is checked first, so a reused pid that took an old parent's
    /// number cannot adopt that parent's children.
    fn admit(&mut self, scan: &Scan) {
        loop {
            let before = self.members.len();
            for (&pid, entry) in &scan.live {
                if pid == self.group || self.members.contains_key(&(pid, entry.start)) {
                    continue;
                }
                // The parent must be a member with the identity this scan describes: a pid that
                // was reused by an unrelated process cannot adopt that process's children, even
                // when the scan is partial and nothing was pruned.
                // A child is admitted through a parent edge only when the edge holds up: the
                // parent is a member, the child is younger than it, and the parent's identity is
                // still that one when it is read again now. A scan reads processes one by one, so
                // either half of the snapshot can be stale, and both orders are covered.
                let known_parent = scan.live.get(&entry.ppid).is_some_and(|parent| {
                    let tracked = self.members.contains_key(&(entry.ppid, parent.start));
                    let fresh = match describe(entry.ppid) {
                        Lookup::Entry(parent) => Some(parent.start),
                        Lookup::Gone | Lookup::Unknown => None,
                    };
                    parent_edge(entry, parent, tracked, fresh)
                });
                if known_parent || entry.group == self.group || entry.session == self.session {
                    self.members.insert((pid, entry.start), entry.start);
                }
            }
            if self.members.len() == before {
                return;
            }
        }
    }
}

/// Serves control requests until the server closes the socket, which it does when the tree ends
/// or the server dies. The host ends the tree on its lifeline meanwhile, as on Linux.
pub fn serve(control: UnixStream, tracker: Arc<Mutex<Tracker>>) {
    let mut control = control;
    loop {
        let mut op = [0_u8; 1];
        if control.read_exact(&mut op).is_err() {
            return;
        }
        let reply = match op[0] {
            b'M' => {
                let members = lock(&tracker).list();
                let mut reply = Vec::with_capacity(5 + members.len() * 14);
                reply.push(b'n');
                reply.extend((members.len() as u32).to_le_bytes());
                for member in &members {
                    reply.extend((member.pid as i32).to_le_bytes());
                    let bytes = member.name.as_bytes();
                    let length = bytes.len().min(u16::MAX as usize);
                    reply.extend((length as u16).to_le_bytes());
                    reply.extend(&bytes[..length]);
                }
                reply
            }
            b'K' => {
                // The guard is released before the reply: the group signal below ends this process,
                // and a lock still held would be a deadlock, not a kill.
                let denied = lock(&tracker).kill_members();
                match denied {
                    Ok(()) => {
                        // The reply leaves before the group signal ends the host, so the server
                        // learns the kill succeeded rather than a socket closing under it.
                        if control.write_all(b"O").is_err() {
                            return;
                        }
                        let _ = control.flush();
                        lock(&tracker).kill_group();
                        std::process::exit(1);
                    }
                    Err(error) => {
                        // An error without an errno still reports a failure: EIO, never zero,
                        // which the client would read as success.
                        let mut reply = vec![b'E'];
                        let errno = error.raw_os_error().unwrap_or(libc::EIO);
                        reply.extend(errno.to_le_bytes());
                        reply
                    }
                }
            }
            _ => return,
        };
        if control.write_all(&reply).is_err() {
            return;
        }
    }
}

/// Whether a child may be admitted through a parent edge: the parent is a member, the child is
/// younger than it, and the parent's identity read again now is the one the tracker holds.
fn parent_edge(child: &Entry, parent: &Entry, tracked: bool, fresh: Option<u64>) -> bool {
    tracked && child.start > parent.start && fresh == Some(parent.start)
}

/// Signals one identified member; a signal that failed for a reason other than the process being
/// gone is the kill's error.
fn kill_member(pid: i32) -> Option<std::io::Error> {
    // SAFETY: plain syscall on a pid the caller just identified.
    if unsafe { libc::kill(pid, libc::SIGKILL) } == -1 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Some(error);
        }
    }
    None
}

/// The server's end of the control socket, and the number of the host that serves it.
///
/// A request is read in slices with a short timeout, and each slice that waits re-sends `SIGCONT`
/// to the host: a host stopped with `SIGSTOP` — as `kill -STOP 0` in a task's own group does —
/// cannot answer, and a signal the server sends is not one the host has to run to receive.
///
/// What a failure means is kept apart: a closed peer is the host ending, which is an answer, while
/// a host that never answers inside one deadline, or one that answers with something other than the
/// protocol, leaves the socket broken for good — no later request reads on it, so a late reply byte
/// can never be mistaken for the start of the next one.
pub struct Client {
    control: Mutex<Control>,
    host: i32,
}

struct Control {
    stream: UnixStream,
    state: State,
}

/// Whether the host is still answering on this socket.
enum State {
    Open,
    /// The host ended: the peer closed the socket, or a write found it gone.
    Closed,
    /// The host did not answer in time, or answered with something the protocol does not allow.
    Broken,
}

/// How long a request waits for its reply, and how long one slice of that wait is.
const READ_SLICE: Duration = Duration::from_millis(100);
const MEMBERS_BUDGET: Duration = Duration::from_secs(2);
const KILL_BUDGET: Duration = Duration::from_secs(5);

/// Whether an error means the slice's wait ended rather than the socket failing.
fn waited(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}

/// Whether an error means the peer ended the connection.
fn closed(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
    )
}

impl Client {
    pub fn new(control: UnixStream, host: i32) -> Client {
        let _ = control.set_read_timeout(Some(READ_SLICE));
        Client {
            control: Mutex::new(Control {
                stream: control,
                state: State::Open,
            }),
            host,
        }
    }

    /// Writes one request byte, recording whether the host ended or the socket failed.
    fn send(&self, control: &mut Control, byte: u8) -> bool {
        match control.stream.write_all(&[byte]) {
            Ok(()) => true,
            Err(error) => {
                control.state = if closed(&error) {
                    State::Closed
                } else {
                    State::Broken
                };
                false
            }
        }
    }

    /// Reads exactly `buf`, waking a stopped host on each slice that waits, until `deadline`. Bytes
    /// already read are kept in `filled`, so a slice that times out mid-message resumes where it
    /// stopped rather than losing its place, and the deadline is checked before every read: a reply
    /// that keeps arriving slowly cannot extend the wait.
    fn read_exact(&self, control: &mut Control, buf: &mut [u8], deadline: Instant) -> bool {
        let mut filled = 0;
        while filled < buf.len() {
            if Instant::now() >= deadline {
                control.state = State::Broken;
                return false;
            }
            match control.stream.read(&mut buf[filled..]) {
                Ok(0) => {
                    control.state = State::Closed;
                    return false;
                }
                Ok(read) => filled += read,
                Err(error) if waited(&error) => {
                    // A stopped host runs nothing, but a signal is delivered to it all the same.
                    // SAFETY: plain syscall on the reserved, unreaped host this client serves.
                    unsafe { libc::kill(self.host, libc::SIGCONT) };
                }
                Err(error) => {
                    control.state = if closed(&error) {
                        State::Closed
                    } else {
                        State::Broken
                    };
                    return false;
                }
            }
        }
        true
    }

    /// What the host said, if it said anything: `Some(list)` for an answer or for a host that has
    /// ended, `None` when it never answered or answered wrongly — which says nothing about the
    /// tree, so it is never read as an empty one.
    pub fn members(&self) -> Option<Vec<Member>> {
        let mut control = lock(&self.control);
        match control.state {
            State::Closed => return Some(Vec::new()),
            State::Broken => return None,
            State::Open => {}
        }
        if !self.send(&mut control, b'M') {
            return ended_or_silent(&control);
        }
        let deadline = Instant::now() + MEMBERS_BUDGET;
        let mut head = [0_u8; 5];
        if !self.read_exact(&mut control, &mut head, deadline) {
            return ended_or_silent(&control);
        }
        if head[0] != b'n' {
            control.state = State::Broken;
            return None;
        }
        let count = u32::from_le_bytes([head[1], head[2], head[3], head[4]]);
        let mut members = Vec::new();
        for _ in 0..count {
            let mut fixed = [0_u8; 6];
            if !self.read_exact(&mut control, &mut fixed, deadline) {
                return ended_or_silent(&control);
            }
            let pid = i32::from_le_bytes([fixed[0], fixed[1], fixed[2], fixed[3]]);
            let length = u16::from_le_bytes([fixed[4], fixed[5]]) as usize;
            let mut name = vec![0_u8; length];
            if !self.read_exact(&mut control, &mut name, deadline) {
                return ended_or_silent(&control);
            }
            members.push(Member {
                pid: pid as u32,
                name: String::from_utf8_lossy(&name).into_owned(),
            });
        }
        Some(members)
    }

    /// Kills the tree, or reports the errno of a member the host may not signal. A host that ends
    /// instead of answering has ended the tree, which is what a kill asks for.
    pub fn kill(&self) -> std::io::Result<()> {
        let mut control = lock(&self.control);
        match control.state {
            State::Closed => return Ok(()),
            State::Broken => return Err(std::io::Error::from(std::io::ErrorKind::TimedOut)),
            State::Open => {}
        }
        if !self.send(&mut control, b'K') {
            return killed_or_silent(&control);
        }
        let deadline = Instant::now() + KILL_BUDGET;
        let mut op = [0_u8; 1];
        if !self.read_exact(&mut control, &mut op, deadline) {
            return killed_or_silent(&control);
        }
        match op[0] {
            b'O' => Ok(()),
            b'E' => {
                let mut errno = [0_u8; 4];
                if !self.read_exact(&mut control, &mut errno, deadline) {
                    // Without the number there is no refusal to report, and the four bytes are
                    // still unread: nothing more is read on this socket.
                    return match control.state {
                        State::Closed => {
                            control.state = State::Closed;
                            Err(std::io::Error::from_raw_os_error(libc::EIO))
                        }
                        _ => {
                            control.state = State::Broken;
                            Err(std::io::Error::from(std::io::ErrorKind::TimedOut))
                        }
                    };
                }
                Err(std::io::Error::from_raw_os_error(i32::from_le_bytes(errno)))
            }
            _ => {
                control.state = State::Broken;
                Err(std::io::Error::from(std::io::ErrorKind::InvalidData))
            }
        }
    }
}

/// The list a request that did not finish describes: an ended host holds nothing, while one that
/// stayed silent describes nothing at all.
fn ended_or_silent(control: &Control) -> Option<Vec<Member>> {
    match control.state {
        State::Closed => Some(Vec::new()),
        State::Open | State::Broken => None,
    }
}

/// What a kill that did not get its answer means: an ended host took the tree with it, while a
/// silent one confirmed nothing.
fn killed_or_silent(control: &Control) -> std::io::Result<()> {
    match control.state {
        State::Closed => Ok(()),
        State::Open | State::Broken => Err(std::io::Error::from(std::io::ErrorKind::TimedOut)),
    }
}

/// The start time of a live pid, which names the process beside its number.
pub fn start_time(pid: i32) -> Option<u64> {
    match describe(pid) {
        Lookup::Entry(entry) => Some(entry.start),
        Lookup::Gone | Lookup::Unknown => None,
    }
}

/// What one pid shows now. `Gone` is a fact the kernel confirmed — the process ended — and it is
/// never the same as `Unknown`, which only says this user could not read it: a member the kernel
/// says ended is dropped, while one that could not be read may still run and is kept. An `Entry`
/// carries the state it showed, zombie included: its start time still names the process.
enum Lookup {
    Entry(Entry),
    Gone,
    Unknown,
}

/// What one process shows, from libproc when it answers and from the kernel's own table when it
/// does not: libproc refuses a process of another user (`proc_pidinfo` answers EPERM for a root
/// child), while `sysctl`'s `KERN_PROC_PID` row describes every uid and answers with an empty row
/// for a pid that ended. The session comes from `getsid` either way.
fn describe(pid: i32) -> Lookup {
    let entry = match libproc_entry(pid) {
        Some(entry) => entry,
        None => match kernel_entry(pid) {
            Row::Entry(entry) => entry,
            Row::Gone => return Lookup::Gone,
            Row::Unknown => return Lookup::Unknown,
        },
    };
    // The session is read after the identity metadata, never before it: a pid reused in between
    // would otherwise attach the old session to the new process. The next scan's identity check
    // catches a session that changed under it.
    let entry = Entry {
        // SAFETY: plain syscall; a process that ended reports no session.
        session: unsafe { libc::getsid(pid) },
        ..entry
    };
    Lookup::Entry(entry)
}

/// The offsets of the fields read from a `KERN_PROC_PID` row, taken from Darwin's own headers on
/// the CI runner (`offsetof`/`sizeof` over `<sys/sysctl.h>`, probe `wip/block-2-probe-sudo` at
/// `42a2436`: `sizeof(struct kinfo_proc)=648`, `p_starttime=0`, `p_stat=36`, `e_ppid=560`,
/// `e_pgid=564`), and cross-checked against the numbers `ps` reports for a same-uid process and a
/// root one. A row of another size is no answer at all.
const KINFO_ROW: usize = 648;
const OFF_START_SEC: usize = 0;
const OFF_START_USEC: usize = 8;
const OFF_STAT: usize = 36;
const OFF_PPID: usize = 560;
const OFF_PGID: usize = 564;

/// What a member's lookup means: kept with the name the lookup gave, kept without a name because
/// this user could not read it, or dropped because the kernel disproved it.
#[derive(PartialEq, Eq, Debug)]
enum Membership {
    Keep(String),
    KeepUnnamed,
    Drop,
}

fn membership(lookup: &Lookup, start: u64) -> Membership {
    match lookup {
        // A reused number, a process that ended, or one that runs nothing: none is a member.
        Lookup::Entry(entry) if entry.start != start || entry.status == ZOMBIE => Membership::Drop,
        Lookup::Entry(entry) => Membership::Keep(entry.name.clone()),
        Lookup::Gone => Membership::Drop,
        Lookup::Unknown => Membership::KeepUnnamed,
    }
}

/// What the kernel's own table says about one pid.
enum Row {
    Entry(Entry),
    /// The call answered, and the row is empty: no process has this number.
    Gone,
    /// The call failed, or answered with a row of a size whose offsets would be wrong.
    Unknown,
}

/// One process from the kernel's table, which answers for every uid. A row of an unexpected size is
/// no answer at all: the offsets would be wrong, and a wrong identity is worse than none.
fn kernel_entry(pid: i32) -> Row {
    let mut buffer = [0_u8; KINFO_ROW];
    let mut size = buffer.len();
    let mut mib = [
        libc::CTL_KERN,
        libc::KERN_PROC,
        libc::KERN_PROC_PID,
        pid as libc::c_int,
    ];
    // SAFETY: the mib is four integers and the buffer is as long as the size passed.
    let result = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            mib.len() as libc::c_uint,
            buffer.as_mut_ptr() as *mut libc::c_void,
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    if result != 0 {
        return Row::Unknown;
    }
    if size == 0 {
        // The call answered and filled nothing: no process has this number.
        return Row::Gone;
    }
    if size != KINFO_ROW {
        return Row::Unknown;
    }
    let word = |at: usize| u64::from_le_bytes(buffer[at..at + 8].try_into().unwrap_or_default());
    let number = |at: usize| i32::from_le_bytes(buffer[at..at + 4].try_into().unwrap_or_default());
    let start = word(OFF_START_SEC) * 1_000_000
        + u32::from_le_bytes(
            buffer[OFF_START_USEC..OFF_START_USEC + 4]
                .try_into()
                .unwrap_or_default(),
        ) as u64;
    Row::Entry(Entry {
        ppid: number(OFF_PPID),
        group: number(OFF_PGID),
        // Filled in by `describe` after this metadata, so the order of the two reads is fixed.
        session: 0,
        start,
        status: buffer[OFF_STAT] as u32,
        name: String::new(),
    })
}

/// One process from libproc: `proc_pidinfo` with `PROC_PIDTBSDINFO` for the parent, group, start
/// time, state, and name.
fn libproc_entry(pid: i32) -> Option<Entry> {
    let mut info: libc::proc_bsdinfo = unsafe { std::mem::zeroed() };
    let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
    // SAFETY: the buffer is exactly the size given, and libproc writes at most that many bytes.
    let read = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            &mut info as *mut libc::proc_bsdinfo as *mut libc::c_void,
            size,
        )
    };
    if read < size {
        return None;
    }
    let name: Vec<u8> = info
        .pbi_name
        .iter()
        .map(|&byte| byte as u8)
        .take_while(|&byte| byte != 0)
        .collect();
    Some(Entry {
        ppid: info.pbi_ppid as i32,
        group: info.pbi_pgid as i32,
        session: 0,
        start: info.pbi_start_tvsec * 1_000_000 + info.pbi_start_tvusec,
        status: info.pbi_status,
        name: String::from_utf8_lossy(&name).into_owned(),
    })
}

/// Every process the scan could describe, and the ones it could not; none at all when the process
/// table itself could not be read. A table that fills the buffer is grown and re-read, and a scan
/// that never fits is reported as incomplete rather than as a short list.
fn describe_all() -> Option<Scan> {
    let mut size = 4096_usize;
    let (pids, complete) = loop {
        let mut pids = vec![0 as libc::pid_t; size];
        // SAFETY: the buffer is as long as the size passed, and libproc writes whole pids into it.
        let count = unsafe {
            libc::proc_listallpids(
                pids.as_mut_ptr() as *mut libc::c_void,
                (pids.len() * std::mem::size_of::<libc::pid_t>()) as libc::c_int,
            )
        };
        if count <= 0 {
            return None;
        }
        let count = count as usize;
        if count < size {
            pids.truncate(count);
            break (pids, true);
        }
        if size >= 65536 {
            // The table is larger than any buffer tried: a real answer, but not a whole one.
            break (pids, false);
        }
        size *= 2;
    };
    let mut scan = Scan {
        live: HashMap::new(),
        unknown: HashSet::new(),
        zombies: HashSet::new(),
        complete,
    };
    for pid in pids {
        if pid <= 0 {
            continue;
        }
        match describe(pid) {
            Lookup::Entry(entry) if entry.status == ZOMBIE => {
                // A zombie holds a slot in its group but runs nothing: it is not a member, and it
                // is not a live parent for anything either.
                scan.zombies.insert(pid);
            }
            Lookup::Entry(entry) => {
                scan.live.insert(pid, entry);
            }
            // A process that ended between the listing and the query is gone, which the kernel
            // proved: it is neither a member nor an unknown one.
            Lookup::Gone => {}
            // One this user may not describe may still run, so it is not reported as gone.
            Lookup::Unknown => {
                scan.unknown.insert(pid);
            }
        }
    }
    Some(scan)
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(ppid: i32, group: i32, session: i32, start: u64) -> Entry {
        Entry {
            ppid,
            group,
            session,
            start,
            status: 1,
            name: "p".to_string(),
        }
    }

    fn scan(live: Vec<(i32, Entry)>, complete: bool) -> Scan {
        Scan {
            live: live.into_iter().collect(),
            unknown: HashSet::new(),
            zombies: HashSet::new(),
            complete,
        }
    }

    fn tracker() -> Tracker {
        Tracker {
            members: HashMap::new(),
            group: 100,
            session: 100,
            last_scan: None,
            complete: true,
        }
    }

    /// A member keeps the identity it was admitted with: the same pid with a new start time is a
    /// different process and leaves the tree, and it cannot adopt the child of the number it took.
    #[test]
    fn a_reused_pid_is_not_a_member_and_adopts_nothing() {
        let mut tracker = tracker();
        tracker.members.insert((500, 7), 7);
        tracker.apply(&scan(
            vec![
                (500, entry(1, 200, 200, 9)),
                (501, entry(500, 200, 200, 11)),
            ],
            true,
        ));
        assert!(tracker.members.is_empty(), "{:?}", tracker.members.keys());
    }

    /// A process the scan could not describe may still run, so it keeps its membership, in a whole
    /// scan and in a partial one alike.
    #[test]
    fn an_unreadable_process_keeps_its_membership() {
        let mut whole = tracker();
        whole.members.insert((600, 3), 3);
        let mut described = scan(vec![], true);
        described.unknown.insert(600);
        whole.apply(&described);
        assert!(whole.members.contains_key(&(600, 3)));

        let mut partial = tracker();
        partial.members.insert((600, 3), 3);
        partial.apply(&scan(vec![], false));
        assert!(partial.members.contains_key(&(600, 3)));
    }

    /// Absence proves an end only when the scan read the whole table; a zombie or a different
    /// birth time disproves the identity in either kind of scan.
    #[test]
    fn absence_zombies_and_birth_times_are_judged_by_what_the_scan_proves() {
        let mut complete = tracker();
        complete.members.insert((610, 4), 4);
        complete.apply(&scan(vec![], true));
        assert!(!complete.members.contains_key(&(610, 4)), "absent is gone");

        let mut partial = tracker();
        partial.members.insert((620, 5), 5);
        partial.apply(&scan(vec![], false));
        assert!(
            partial.members.contains_key(&(620, 5)),
            "absent is unproven"
        );

        let mut dead = tracker();
        dead.members.insert((630, 6), 6);
        // A scan describes a zombie as ended, not as a live process: it is absent from `live`.
        let mut with_zombie = scan(vec![], false);
        with_zombie.zombies.insert(630);
        dead.apply(&with_zombie);
        assert!(!dead.members.contains_key(&(630, 6)), "a zombie ended");

        let mut replaced = tracker();
        replaced.members.insert((640, 8), 8);
        replaced.apply(&scan(vec![(640, entry(1, 200, 200, 12))], false));
        assert!(
            !replaced.members.contains_key(&(640, 8)),
            "a different birth time is a different process"
        );
    }

    /// Once observed, a member stays a member after it leaves the session, the group and its
    /// parent chain; it is dropped when its number passes to another process.
    #[test]
    fn an_observed_detached_member_stays_in_the_tree() {
        let mut tracker = tracker();
        tracker.apply(&scan(vec![(700, entry(1, 100, 100, 5))], true));
        assert!(tracker.members.contains_key(&(700, 5)));
        tracker.apply(&scan(vec![(700, entry(1, 900, 900, 5))], true));
        assert!(tracker.members.contains_key(&(700, 5)));
        tracker.apply(&scan(vec![(700, entry(1, 900, 900, 8))], true));
        assert!(!tracker.members.contains_key(&(700, 5)));
    }

    /// A peer that ended is an answer: the tree holds nothing and the kill is done. A peer that
    /// never answers is not: nothing about the tree is confirmed, so the list says nothing and the
    /// kill fails rather than reporting a tree that ended.
    #[test]
    fn a_closed_peer_answers_and_a_silent_one_does_not() {
        let (server, peer) = UnixStream::pair().expect("a socket pair");
        let client = Client::new(server, 0);
        drop(peer);
        assert_eq!(
            client.members().map(|members| members.len()),
            Some(0),
            "a host that ended holds nothing"
        );
        assert!(client.kill().is_ok(), "a host that ended took the tree");

        let (server, peer) = UnixStream::pair().expect("a socket pair");
        let client = Client::new(server, 0);
        // The peer stays open and never answers: the request waits out its deadline.
        let begun = Instant::now();
        assert!(
            client.members().is_none(),
            "a silent host describes nothing"
        );
        assert!(begun.elapsed() >= MEMBERS_BUDGET);
        assert_eq!(
            client.kill().map_err(|error| error.kind()),
            Err(std::io::ErrorKind::TimedOut),
            "a silent host confirms nothing"
        );
        // The socket is broken for good: no later request reads a late reply byte as a header.
        assert!(client.members().is_none(), "the socket stays broken");
        drop(peer);
    }

    /// The kernel telling us a process ended is a fact, and it is not the same as failing to read
    /// one: a member the kernel disproves is dropped from the list and from a kill, while one this
    /// user could not read stays, unnamed.
    #[test]
    fn a_pid_the_kernel_says_ended_is_gone_not_unknown() {
        let mut zombie = entry(1, 300, 100, 7);
        zombie.status = ZOMBIE;
        let decisions = [
            (
                Lookup::Entry(entry(1, 300, 100, 7)),
                Membership::Keep("p".to_string()),
            ),
            (Lookup::Entry(entry(1, 300, 100, 8)), Membership::Drop),
            (Lookup::Entry(zombie), Membership::Drop),
            (Lookup::Gone, Membership::Drop),
            (Lookup::Unknown, Membership::KeepUnnamed),
        ];
        for (lookup, expected) in decisions {
            assert_eq!(membership(&lookup, 7), expected, "a member decision");
        }
        // A real process that ended and was reaped: the kernel answers with an empty row, which is
        // `Gone`, and the tracker drops the member instead of holding it.
        let mut child = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg("exit 0")
            .spawn()
            .expect("a child to end");
        let pid = child.id() as i32;
        child.wait().expect("the child ended");
        assert!(
            matches!(describe(pid), Lookup::Gone),
            "the kernel says the number has no process"
        );
        let mut listing = tracker();
        listing.members.insert((pid, 1), 1);
        assert!(listing.list().is_empty(), "a gone pid is no member");
        assert!(!listing.members.contains_key(&(pid, 1)));
        // The kill drops it too, and finishes rather than reporting an unidentifiable member.
        let mut killing = tracker();
        killing.members.insert((pid, 1), 1);
        assert!(
            killing.kill_members().is_ok(),
            "a gone member is not a kill failure"
        );
    }

    /// A parent edge is admitted only while the parent's identity holds when it is read again: a
    /// scan can read the owned parent first, then see that number recycled, then read a foreign
    /// child that names it.
    #[test]
    fn a_parent_edge_needs_the_parent_identity_read_again() {
        let parent = entry(1, 300, 100, 10);
        let child = entry(900, 500, 500, 30);
        assert!(
            parent_edge(&child, &parent, true, Some(10)),
            "the same parent"
        );
        assert!(
            !parent_edge(&child, &parent, true, Some(20)),
            "a recycled parent number is no edge"
        );
        assert!(
            !parent_edge(&child, &parent, true, None),
            "an unread parent"
        );
        assert!(
            !parent_edge(&child, &parent, false, Some(10)),
            "an untracked parent"
        );
        assert!(
            !parent_edge(&entry(1, 500, 500, 5), &parent, true, Some(10)),
            "a child older than its parent"
        );
    }

    /// A child that predates the parent identity it names is foreign, even when the number is a
    /// member's: a scan reads processes one by one, so the child can be older than a recycled
    /// parent.
    #[test]
    fn a_child_older_than_its_parent_identity_is_not_admitted() {
        let mut tracker = tracker();
        tracker.members.insert((900, 20), 20);
        tracker.apply(&scan(
            vec![
                (900, entry(1, 300, 100, 20)),
                (901, entry(900, 500, 500, 10)),
            ],
            true,
        ));
        assert!(
            !tracker.members.contains_key(&(901, 10)),
            "a child born before its parent cannot be that parent's"
        );
    }

    /// Membership comes from the host's session and group: a process in another session and group,
    /// whose parent is not a member, is not admitted.
    #[test]
    fn admission_follows_the_host_session_and_group() {
        let mut tracker = tracker();
        // The parent is this test's own process: a parent edge is admitted only after the parent is
        // read again, so the pid must be one the kernel can describe with a start time the test
        // knows.
        let host = std::process::id() as i32;
        let born = match describe(host) {
            Lookup::Entry(entry) => entry.start,
            Lookup::Gone | Lookup::Unknown => panic!("the test process is described"),
        };
        tracker.apply(&scan(vec![(host, entry(1, 300, 100, born))], true));
        assert!(
            tracker.members.contains_key(&(host, born)),
            "the host's session admits it"
        );
        tracker.apply(&scan(vec![(810, entry(1, 300, 400, 2))], true));
        assert!(
            !tracker.members.contains_key(&(810, 2)),
            "another session and group do not"
        );
        // A child of a member is admitted even when it shares neither, because its parent is in
        // this scan with the identity the tracker holds.
        let child = host + 200_000;
        tracker.apply(&scan(
            vec![
                (host, entry(1, 300, 100, born)),
                (child, entry(host, 500, 500, born + 1)),
            ],
            true,
        ));
        assert!(
            tracker.members.contains_key(&(child, born + 1)),
            "a member's child"
        );
    }
}
