//! Task registry: spawn, log capture, unseen-output cursors, input queues, and shutdown.

use crate::output::{
    Cleaner, Matcher, Truncate, Window, WindowBuilder, WindowSpec, incomplete_legacy_suffix,
    incomplete_utf8_suffix,
};
use crate::process::{self, Launch, Member, Tree};
use crate::terminal::Live;
use encoding_rs::Encoding;
use std::collections::HashSet;
use std::fs::File;
use std::hash::{BuildHasher, RandomState};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, mpsc};
use std::time::{Duration, Instant};
use tokio::sync::watch;

pub const MAX_RUNNING: usize = 16;
const MAX_FINISHED: usize = 64;
const TASK_LOG_LIMIT: u64 = 64 << 20;
const TOTAL_LOG_LIMIT: u64 = 1 << 30;
const INPUT_QUEUE_LIMIT: usize = 1 << 20;
/// Output that pauses this long has finished a frame: the chunks of one redraw arrive within
/// a millisecond or two of each other, frames 8 ms or more apart.
const FRAME_PAUSE: Duration = Duration::from_millis(4);
/// A pause this long showed its frame long enough to read, even in the middle of a redraw.
const LONG_PAUSE: Duration = Duration::from_millis(100);
/// How long output may stay open after the root exits and its tree is killed.
const DRAIN_CAP: Duration = Duration::from_secs(2);
/// Kept processes may hold the output open for good, so with them the drain also ends once the
/// output pauses this long.
const KEPT_PAUSE: Duration = Duration::from_millis(100);

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub struct Tasks {
    dir: PathBuf,
    registry: Mutex<Registry>,
    log_bytes: AtomicU64,
    bash: OnceLock<Result<PathBuf, String>>,
}

#[derive(Default)]
struct Registry {
    /// Every ID this server has issued, so a retired task's ID never names a later task.
    issued: HashSet<String>,
    tasks: Vec<Arc<Task>>,
}

/// Five random characters from Crockford's base32 alphabet in lowercase, at least one of them
/// a letter, unique within this server. Random IDs make an ID from another fastexec server,
/// such as a nested one, unlikely to name a task here.
fn new_id(issued: &mut HashSet<String>) -> String {
    const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";
    loop {
        let bits = RandomState::new().hash_one(issued.len());
        let id: String = (0..5)
            .map(|i| ALPHABET[(bits >> (5 * i)) as usize & 31] as char)
            .collect();
        if id.bytes().any(|b| b.is_ascii_alphabetic()) && issued.insert(id.clone()) {
            return id;
        }
    }
}

pub struct Task {
    pub id: String,
    pub command: String,
    pub pty: bool,
    pub log_path: PathBuf,
    pub encoding: &'static Encoding,
    /// `keepTree`: processes the root leaves running stay until kill or session end.
    pub keep: bool,
    started: Instant,
    tree: Tree,
    /// Set once no process of the tree runs.
    tree_done: watch::Sender<bool>,
    /// Why the last kill of the processes held past the root's exit failed.
    kill_error: Mutex<Option<String>>,
    kill_requested: AtomicBool,
    /// Set when `killAfterMs` elapsed and ended the task.
    expired: AtomicBool,
    /// Set once a result has shown this task's final state.
    reported: AtomicBool,
    out: Mutex<Output>,
    view: Mutex<View>,
    end: Mutex<Option<Final>>,
    done: watch::Sender<bool>,
    /// Signals each captured output chunk and each synchronized update its timeout ends.
    activity: watch::Sender<()>,
    input: Mutex<Option<mpsc::Sender<Input>>>,
    queued: AtomicUsize,
    /// PTY tasks: a terminal emulator fed with every output byte, and the transcript it
    /// records. Its replies to terminal queries, such as the cursor-position report and device
    /// attributes ConPTY waits for at startup, go to the program as input.
    terminal: Option<Mutex<Pty>>,
}

struct Pty {
    live: Live,
    spool: Spool,
}

/// A PTY task's transcript file: the final lines, those the terminal wrote out, then the
/// pending lines as the last `transcript` read them.
struct Spool {
    /// `None` for a task started with an encoding, or once the log was evicted.
    file: Option<File>,
    /// Bytes and lines of the final lines.
    bytes: u64,
    lines: u64,
    /// Bytes of the pending lines after them.
    pending: u64,
    /// Bytes and lines of the final lines a `transcript` result has shown.
    shown: (u64, u64),
    /// Why the file takes no more lines.
    stopped: Option<String>,
}

/// What a `transcript` call reads: the file's final lines from the first one no result has
/// shown, then the pending lines.
pub struct Transcript {
    pub path: PathBuf,
    /// Where this read starts: its first line, one-based, and that line's byte in the file.
    pub first_line: u64,
    pub start: u64,
    /// Final lines, and their bytes.
    pub lines: u64,
    pub bytes: u64,
    /// The lines the terminal has not written out yet, each ending in a newline.
    pub pending: String,
    /// Pending lines past the per-task limit are left out.
    pub cut: bool,
    /// Why the file takes no more lines.
    pub stopped: Option<String>,
}

/// The rendered terminal of a PTY task.
pub struct Screen {
    /// Visible rows without trailing spaces; blank rows at the bottom are dropped.
    pub rows: Vec<String>,
    /// Zero-based row and column.
    pub cursor: (u16, u16),
    /// End of the readable log at the moment the screen was taken.
    pub log_end: u64,
}

#[derive(Default)]
struct Output {
    written: u64,
    /// End of the readable log: excludes an unfinished UTF-8 sequence while output continues.
    readable: u64,
    lines: u64,
    dropped: u64,
    evicted: bool,
    /// The first log write failure; capture stops storing output after it.
    log_error: Option<String>,
    /// Output reached EOF: every captured byte is final.
    closed: bool,
    /// When the last output byte arrived, stored or not, or the end of a synchronized update by
    /// timeout or end of output showed the frame it held.
    last_output: Option<Instant>,
    /// Bytes the transcript file holds, counted under the total log limit.
    transcript: u64,
}

#[derive(Default)]
struct View {
    cursor: u64,
    lines_before: u64,
    cleaner: Cleaner,
}

#[derive(Clone)]
struct Final {
    exit_code: i32,
    killed: bool,
    expired: bool,
    elapsed: Duration,
    output_open: bool,
    /// Processes the root left running when it exited.
    leftovers: Vec<Member>,
    /// The task holds its tree past the root's exit: it keeps the leftovers, or killing them
    /// failed.
    kept: bool,
}

enum Input {
    Data(Vec<u8>),
    Eof,
}

/// Task state for rendering.
pub struct Snapshot {
    pub state: &'static str,
    pub exit_code: Option<i32>,
    pub elapsed: Duration,
    pub lines: u64,
    pub dropped: u64,
    pub evicted: bool,
    pub output_open: bool,
    pub expired: bool,
    pub log_error: Option<String>,
    /// Processes the root left running when it exited.
    pub leftovers: Vec<Member>,
    /// The task holds its tree past the root's exit, and some of it still runs.
    pub kept: bool,
    /// Why the last kill of the processes it holds failed.
    pub kill_error: Option<String>,
}

pub struct StartArgs {
    pub command: String,
    pub cwd: PathBuf,
    pub pty: bool,
    pub login: bool,
    pub keep: bool,
    pub encoding: &'static Encoding,
}

impl Tasks {
    pub fn new() -> std::io::Result<Arc<Self>> {
        let temp = std::env::temp_dir();
        remove_stale_dirs(&temp);
        let dir = temp.join(format!("{}{}", dir_prefix(), std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        #[cfg_attr(not(unix), allow(unused_mut))]
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
        builder.create(&dir)?;
        Ok(Arc::new(Self {
            dir,
            registry: Mutex::default(),
            log_bytes: AtomicU64::new(0),
            bash: OnceLock::new(),
        }))
    }

    pub fn find(&self, id: &str) -> Option<Arc<Task>> {
        lock(&self.registry)
            .tasks
            .iter()
            .find(|task| task.id == id)
            .cloned()
    }

    /// Every task, newest first.
    pub fn all(&self) -> Vec<Arc<Task>> {
        lock(&self.registry).tasks.iter().rev().cloned().collect()
    }

    pub fn start(self: &Arc<Self>, args: StartArgs) -> Result<Arc<Task>, String> {
        let bash = self.bash.get_or_init(crate::bash::find).clone()?;
        let mut registry = lock(&self.registry);
        let running = registry
            .tasks
            .iter()
            .filter(|task| task.is_running() || task.keeps_processes())
            .count();
        if running >= MAX_RUNNING {
            return Err(format!(
                "TOO_MANY_TASKS: {MAX_RUNNING} tasks are running or keep processes. Wait for one to finish or kill one, then retry."
            ));
        }
        let id = new_id(&mut registry.issued);
        let log_path = self.dir.join(format!("{id}.log"));
        let log = private_file(&log_path)
            .map_err(|error| format!("Cannot create the task log: {error}."))?;
        let script_path = self.dir.join(format!("{id}.sh"));
        let script = if args.command.len() > process::SCRIPT_THRESHOLD_BYTES {
            std::fs::write(&script_path, &args.command)
                .map_err(|error| format!("Cannot write the command script: {error}."))?;
            Some(script_path.as_path())
        } else {
            None
        };
        let launch = Launch {
            bash: &bash,
            command: &args.command,
            script,
            cwd: &args.cwd,
            login: args.login,
            pty: args.pty,
        };
        let spawned = match process::spawn(&launch) {
            Ok(spawned) => spawned,
            Err(error) => {
                let _ = std::fs::remove_file(&log_path);
                let _ = std::fs::remove_file(&script_path);
                return Err(format!("Cannot start the command: {error}."));
            }
        };
        let (input_tx, input_rx) = mpsc::channel();
        let terminal = args.pty.then(|| {
            let mut spool = Spool {
                file: None,
                bytes: 0,
                lines: 0,
                pending: 0,
                shown: (0, 0),
                stopped: None,
            };
            if args.encoding == encoding_rs::UTF_8 {
                match private_file(&self.dir.join(format!("{id}.transcript.txt"))) {
                    Ok(file) => spool.file = Some(file),
                    Err(error) => spool.stopped = Some(format!("it cannot be created: {error}")),
                }
            }
            Mutex::new(Pty {
                live: Live::new(),
                spool,
            })
        });
        let task = Arc::new(Task {
            id,
            command: args.command,
            pty: args.pty,
            log_path,
            encoding: args.encoding,
            keep: args.keep,
            started: Instant::now(),
            tree: spawned.tree,
            tree_done: watch::Sender::new(false),
            kill_error: Mutex::new(None),
            kill_requested: AtomicBool::new(false),
            expired: AtomicBool::new(false),
            reported: AtomicBool::new(false),
            out: Mutex::default(),
            view: Mutex::default(),
            end: Mutex::new(None),
            done: watch::Sender::new(false),
            activity: watch::Sender::new(()),
            input: Mutex::new(Some(input_tx.clone())),
            queued: AtomicUsize::new(0),
            terminal,
        });
        registry.tasks.push(Arc::clone(&task));
        drop(registry);

        let (eof_tx, eof_rx) = mpsc::channel::<()>();
        let (shared, reader_task, output) = (Arc::clone(self), Arc::clone(&task), spawned.output);
        let answer = args.pty.then(|| input_tx.clone());
        std::thread::spawn(move || {
            capture(&shared, &reader_task, output, log, answer);
            let _ = eof_tx.send(());
        });
        let (input_task, mut input) = (Arc::clone(&task), Some(spawned.input));
        std::thread::spawn(move || {
            for message in input_rx {
                match message {
                    Input::Data(bytes) => {
                        let delivered = input.as_mut().is_some_and(|input| {
                            input.write_all(&bytes).and_then(|()| input.flush()).is_ok()
                        });
                        input_task.queued.fetch_sub(bytes.len(), Ordering::SeqCst);
                        if !delivered && input.take().is_some() {
                            // The program closed stdin; later input must be refused, not queued.
                            *lock(&input_task.input) = None;
                        }
                    }
                    Input::Eof => drop(input.take()),
                }
            }
        });
        let (waiter_shared, waiter_task, mut child) =
            (Arc::clone(self), Arc::clone(&task), spawned.child);
        std::thread::spawn(move || {
            let exit_code = child.wait(&waiter_task.tree);
            // Read at the root's exit, so a kill or deadline during the drain below leaves a
            // natural exit labeled as such.
            let killed = waiter_task.kill_requested.load(Ordering::SeqCst);
            let expired = waiter_task.expired.load(Ordering::SeqCst);
            // A task is its whole tree: whatever the root leaves behind ends with it, unless
            // the task keeps it.
            let mut leftovers = if killed {
                Vec::new()
            } else {
                waiter_task.tree.members()
            };
            let mut kept = waiter_task.keep && !leftovers.is_empty();
            let kill_error = if kept {
                None
            } else {
                waiter_task.tree.kill().err().map(|error| error.to_string())
            };
            // A tree that could not be killed stays held, so kill and shutdown retry, and the
            // leftovers are what survived, also after a kill of the root.
            if kill_error.is_some() {
                kept = true;
                leftovers = waiter_task.tree.members();
                *lock(&waiter_task.kill_error) = kill_error;
            }
            // The end of the tree closes a PTY, so its output reaches EOF; ConPTY's close can
            // block, so it runs apart.
            let ender = Arc::clone(&waiter_task);
            std::thread::spawn(move || {
                child.wait_tree(&ender.tree);
                drop(child);
                ender.tree_done.send_replace(true);
            });
            let output_open = drain(&waiter_task, &eof_rx, kept);
            *lock(&waiter_task.input) = None;
            *lock(&waiter_task.end) = Some(Final {
                exit_code,
                killed,
                expired,
                elapsed: waiter_task.started.elapsed(),
                output_open,
                leftovers,
                kept,
            });
            let _ = std::fs::remove_file(&script_path);
            waiter_task.done.send_replace(true);
            waiter_shared.retire_finished(&mut lock(&waiter_shared.registry));
        });
        Ok(task)
    }

    /// Keeps the [`MAX_FINISHED`] most recently finished tasks and deletes the logs of older
    /// ones. Runs right after each completion, so the task that just finished always stays.
    fn retire_finished(&self, registry: &mut Registry) {
        // A task that keeps processes stays, so kill can still reach them.
        let mut finished: Vec<(Instant, String)> = registry
            .tasks
            .iter()
            .filter(|task| !task.keeps_processes())
            .filter_map(|task| task.ended_at().map(|at| (at, task.id.clone())))
            .collect();
        if finished.len() <= MAX_FINISHED {
            return;
        }
        finished.sort();
        let victims: Vec<String> = finished[..finished.len() - MAX_FINISHED]
            .iter()
            .map(|(_, id)| id.clone())
            .collect();
        registry.tasks.retain(|task| {
            if !victims.contains(&task.id) {
                return true;
            }
            self.evict_log(task);
            false
        });
    }

    fn evict_log(&self, task: &Task) {
        let mut out = lock(&task.out);
        if !out.evicted {
            out.evicted = true;
            self.log_bytes
                .fetch_sub(out.written + out.transcript, Ordering::SeqCst);
            let _ = std::fs::remove_file(&task.log_path);
            let _ = std::fs::remove_file(task.transcript_path());
        }
    }

    /// Reserves log space under the total limit, evicting the oldest finished logs if needed.
    fn reserve(&self, bytes: u64) -> bool {
        if self.log_bytes.fetch_add(bytes, Ordering::SeqCst) + bytes <= TOTAL_LOG_LIMIT {
            return true;
        }
        let candidates: Vec<Arc<Task>> = lock(&self.registry)
            .tasks
            .iter()
            .filter(|task| !task.is_running())
            .cloned()
            .collect();
        for task in candidates {
            if self.log_bytes.load(Ordering::SeqCst) <= TOTAL_LOG_LIMIT {
                break;
            }
            self.evict_log(&task);
        }
        if self.log_bytes.load(Ordering::SeqCst) <= TOTAL_LOG_LIMIT {
            return true;
        }
        self.log_bytes.fetch_sub(bytes, Ordering::SeqCst);
        false
    }

    /// Counts `bytes` a task's transcript file took, reserved before; an evicted task's files
    /// are gone, and the bytes go back.
    fn hold(&self, task: &Task, bytes: u64) {
        let mut out = lock(&task.out);
        if out.evicted {
            self.log_bytes.fetch_sub(bytes, Ordering::SeqCst);
        } else {
            out.transcript += bytes;
        }
    }

    /// Gives back `bytes` a task's transcript file no longer holds.
    fn release(&self, task: &Task, bytes: u64) {
        let mut out = lock(&task.out);
        if !out.evicted {
            out.transcript -= bytes;
            self.log_bytes.fetch_sub(bytes, Ordering::SeqCst);
        }
    }

    /// Kills every tree, kept processes included, and removes the log directory.
    pub fn shutdown(&self) {
        for task in lock(&self.registry).tasks.iter() {
            if task.is_running() {
                task.kill_requested.store(true, Ordering::SeqCst);
            }
            let _ = task.tree.kill();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }

    /// One line naming the other tasks that need attention, at most `cap` bytes: finished tasks
    /// whose final state no result has shown yet (failures first), then running tasks. Returns
    /// the line and the finished tasks it names, which the caller marks reported once the line
    /// is delivered.
    pub fn background(&self, exclude: &str, cap: usize) -> Option<(String, Vec<Arc<Task>>)> {
        // Rank 0: failed or expired; 1: other finished; 2: running. Newest first within a rank.
        let mut entries: Vec<(u8, Arc<Task>, Snapshot)> = self
            .all()
            .into_iter()
            .filter(|task| task.id != exclude)
            .filter_map(|task| {
                let snapshot = task.snapshot();
                let rank = match snapshot.state {
                    "running" => 2,
                    _ if task.reported.load(Ordering::SeqCst) => return None,
                    "exited" if snapshot.exit_code != Some(0) => 0,
                    _ if snapshot.expired => 0,
                    _ => 1,
                };
                Some((rank, task, snapshot))
            })
            .collect();
        if entries.is_empty() {
            return None;
        }
        entries.sort_by_key(|(rank, ..)| *rank);
        let count = |rank: u8| entries.iter().filter(|entry| entry.0 == rank).count();
        let (failed, running) = (count(0), count(2));
        let finished = entries.len() - running;
        let mut counts = Vec::new();
        if running > 0 {
            counts.push(format!("{running} running"));
        }
        if finished > 0 {
            counts.push(format!("{finished} finished"));
        }
        if failed > 0 {
            counts.push(format!("{failed} failed"));
        }
        let counts = counts.join(", ");
        let finished_tasks = |shown: usize| {
            entries[..shown]
                .iter()
                .filter(|entry| entry.0 < 2)
                .map(|entry| Arc::clone(&entry.1))
                .collect()
        };
        for shown in (1..=entries.len().min(3)).rev() {
            let named: Vec<String> = entries[..shown]
                .iter()
                .map(
                    |(_, task, snapshot)| match (snapshot.state, snapshot.exit_code) {
                        ("running", _) => {
                            format!("{} running {}", task.id, short(snapshot.elapsed))
                        }
                        ("exited", Some(code)) => format!("{} exited {code}", task.id),
                        _ if snapshot.expired => format!("{} killed by killAfterMs", task.id),
                        _ => format!("{} killed", task.id),
                    },
                )
                .collect();
            let rest = entries.len() - shown;
            let line = if rest == 0 {
                format!("(Background: {}.)", named.join(", "))
            } else {
                format!(
                    "(Background: {}; {rest} more; {counts}; use list.)",
                    named.join(", ")
                )
            };
            if line.len() <= cap {
                return Some((line, finished_tasks(shown)));
            }
        }
        let line = format!("(Background: {counts}; use list.)");
        (line.len() <= cap).then(|| (line, Vec::new()))
    }
}

impl Spool {
    /// Appends final lines, each ending in a newline, in place of the pending lines. Past the
    /// per-task limit, the file keeps the lines that fit and takes no more.
    fn append(&mut self, text: &str, task: &Task, shared: &Tasks) {
        if text.is_empty() || self.stopped.is_some() {
            return;
        }
        let mut text = text.as_bytes();
        let room = (TASK_LOG_LIMIT - self.bytes) as usize;
        if text.len() > room {
            let fit = text[..room].iter().rposition(|&byte| byte == b'\n');
            text = &text[..fit.map_or(0, |end| end + 1)];
            self.stopped = Some(format!("it reached its {} MiB limit", TASK_LOG_LIMIT >> 20));
        }
        self.write(text, true, task, shared);
    }

    /// Writes `text` after the final lines in place of the pending lines, as final lines or as
    /// the new pending lines; returns whether the file holds `text`.
    fn write(&mut self, text: &[u8], as_final: bool, task: &Task, shared: &Tasks) -> bool {
        let Some(file) = &mut self.file else {
            return false;
        };
        if lock(&task.out).evicted {
            self.file = None;
            return false;
        }
        if self.pending > 0 {
            if let Err(error) = file.set_len(self.bytes) {
                self.stopped = Some(format!("writing it failed: {error}"));
                return false;
            }
            shared.release(task, self.pending);
            self.pending = 0;
        }
        let size = text.len() as u64;
        if text.is_empty() {
            return true;
        }
        if !shared.reserve(size) {
            self.stopped = Some("the 1 GiB total log limit is reached".into());
            return false;
        }
        match file
            .seek(SeekFrom::Start(self.bytes))
            .and_then(|_| file.write_all(text))
        {
            Ok(()) => {
                shared.hold(task, size);
                if as_final {
                    self.bytes += size;
                    self.lines += text.iter().filter(|&&byte| byte == b'\n').count() as u64;
                } else {
                    self.pending = size;
                }
                true
            }
            Err(error) => {
                shared.log_bytes.fetch_sub(size, Ordering::SeqCst);
                self.stopped = Some(format!("writing it failed: {error}"));
                false
            }
        }
    }
}

/// Waits for the output to reach EOF, at most [`DRAIN_CAP`], and returns whether it stayed
/// open. With kept processes, which may hold it open for good, the wait also ends once the
/// output pauses for [`KEPT_PAUSE`].
fn drain(task: &Task, eof: &mpsc::Receiver<()>, kept: bool) -> bool {
    let deadline = Instant::now() + DRAIN_CAP;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return true;
        }
        let step = if kept { left.min(KEPT_PAUSE) } else { left };
        match eof.recv_timeout(step) {
            Ok(()) => return false,
            Err(mpsc::RecvTimeoutError::Disconnected) => return true,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        if kept
            && task
                .last_output()
                .0
                .is_none_or(|last| last.elapsed() >= KEPT_PAUSE)
        {
            return true;
        }
    }
}

/// Elapsed time in its largest two units: `42s`, `4m3s`, `2h5m`.
fn short(elapsed: Duration) -> String {
    let seconds = elapsed.as_secs();
    match seconds {
        0..60 => format!("{seconds}s"),
        60..3600 => format!("{}m{}s", seconds / 60, seconds % 60),
        _ => format!("{}h{}m", seconds / 3600, seconds % 3600 / 60),
    }
}

/// Reads `output` until EOF and passes it on in chunks, each with the time it was read.
fn read_chunks(mut output: Box<dyn Read + Send>, chunks: mpsc::SyncSender<(Instant, Vec<u8>)>) {
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        match output.read(&mut buffer) {
            Ok(0) => break,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => break, // a closed PTY reports EIO on Unix
            Ok(read) => {
                if chunks
                    .send((Instant::now(), buffer[..read].to_vec()))
                    .is_err()
                {
                    break;
                }
            }
        }
    }
}

/// Sends the terminal emulator's replies to the program. Replies share the input limit; past
/// it they are dropped, never queued unbounded.
fn answer_program(task: &Task, answer: &mpsc::Sender<Input>, reply: Vec<u8>) {
    if !reply.is_empty()
        && task.admit(reply.len())
        && let Err(error) = answer.send(Input::Data(reply))
        && let Input::Data(reply) = error.0
    {
        task.queued.fetch_sub(reply.len(), Ordering::SeqCst);
    }
}

/// Drains one task's output into its log until EOF, never blocking the child on log limits.
///
/// For a PTY, the output also feeds the task's terminal emulator, and `answer` carries its
/// replies to terminal queries.
fn capture(
    shared: &Tasks,
    task: &Task,
    output: Box<dyn Read + Send>,
    log: File,
    answer: Option<mpsc::Sender<Input>>,
) {
    // Output is read on its own thread, so a synchronized update ends at its deadline while
    // the program writes nothing.
    let (chunks, received) = mpsc::sync_channel(1);
    std::thread::spawn(move || read_chunks(output, chunks));
    let mut last = Vec::with_capacity(8);
    let mut log = Some(log);
    loop {
        let deadline = task
            .terminal
            .as_ref()
            .and_then(|terminal| lock(terminal).live.sync_deadline());
        let received = match deadline {
            None => received.recv().ok(),
            Some(deadline) => {
                match received.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                    Ok(chunk) => Some(chunk),
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        if let (Some(terminal), Some(answer)) = (&task.terminal, &answer) {
                            let mut pty = lock(terminal);
                            answer_program(task, answer, pty.live.end_sync());
                            let written = pty.live.take_written();
                            pty.spool.append(&written, task, shared);
                            // The held frame shows now, so quiet counts from now; the time is
                            // set under the terminal lock that `last_output` also takes.
                            lock(&task.out).last_output = Some(Instant::now());
                        }
                        task.activity.send_replace(());
                        continue;
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => None,
                }
            }
        };
        let Some((arrived, chunk)) = received else {
            break;
        };
        let chunk = chunk.as_slice();
        // The emulator stays locked until the chunk is in the log, so a screen and the log
        // offset taken under that lock show the same output.
        let mut terminal = task.terminal.as_ref().map(lock);
        if let (Some(pty), Some(answer)) = (terminal.as_mut(), &answer) {
            // Output that paused showed a finished frame before this chunk.
            let gap = lock(&task.out).last_output.map_or(Duration::ZERO, |last| {
                arrived.saturating_duration_since(last)
            });
            let pause = (gap >= FRAME_PAUSE).then_some(gap >= LONG_PAUSE);
            answer_program(task, answer, pty.live.process(chunk, arrived, pause));
            let written = pty.live.take_written();
            pty.spool.append(&written, task, shared);
        }
        // Storing stops at the first byte that does not fit, so the log stays a prefix of the
        // output: a later chunk that would fit is not stored after the gap.
        let room = {
            let out = lock(&task.out);
            if out.evicted || out.dropped > 0 {
                0
            } else {
                TASK_LOG_LIMIT - out.written
            }
        };
        let keep = &chunk[..chunk.len().min(room as usize)];
        let size = keep.len() as u64;
        let mut stored = false;
        let mut write_error = None;
        if size > 0
            && let Some(file) = log.as_mut()
            && shared.reserve(size)
        {
            match file.write_all(keep) {
                Ok(()) => stored = true,
                Err(error) => {
                    // A partial write leaves bytes past `written`; storing nothing more keeps
                    // every readable offset valid.
                    shared.log_bytes.fetch_sub(size, Ordering::SeqCst);
                    log = None;
                    write_error = Some(error.to_string());
                }
            }
        }
        let mut out = lock(&task.out);
        if write_error.is_some() {
            out.log_error = write_error;
        }
        let stored = if stored { keep } else { &[] };
        if !stored.is_empty() {
            last.extend_from_slice(stored);
            last.drain(..last.len().saturating_sub(3));
            out.written += size;
            out.lines += stored.iter().filter(|&&byte| byte == b'\n').count() as u64;
            out.readable = out.written - incomplete_utf8_suffix(&last) as u64;
        }
        out.dropped += (chunk.len() - stored.len()) as u64;
        out.last_output = Some(arrived);
        drop(out);
        drop(terminal);
        task.activity.send_replace(());
    }
    // No later output can end a synchronized update; the screen shows what it held, and that
    // frame restarts the quiet time as a timeout's does. Every pending transcript line is
    // final now.
    let mut terminal = task.terminal.as_ref().map(lock);
    let held = terminal
        .as_mut()
        .is_some_and(|pty| pty.live.sync_deadline().is_some());
    if let Some(pty) = terminal.as_mut() {
        pty.live.end_sync();
        pty.live.finish();
        let written = pty.live.take_written();
        pty.spool.append(&written, task, shared);
    }
    let mut out = lock(&task.out);
    out.readable = out.written;
    out.closed = true;
    if held {
        out.last_output = Some(Instant::now());
    }
    drop(out);
    drop(terminal);
    task.activity.send_replace(());
}

impl Task {
    pub fn is_running(&self) -> bool {
        lock(&self.end).is_none()
    }

    fn ended_at(&self) -> Option<Instant> {
        lock(&self.end)
            .as_ref()
            .map(|end| self.started + end.elapsed)
    }

    /// The root has exited, and processes it left run on, under `keepTree` or because killing
    /// them failed.
    fn keeps_processes(&self) -> bool {
        lock(&self.end).as_ref().is_some_and(|end| end.kept) && !*self.tree_done.borrow()
    }

    pub fn done(&self) -> watch::Receiver<bool> {
        self.done.subscribe()
    }

    /// Becomes true once no process of the task's tree runs.
    pub fn tree_done(&self) -> watch::Receiver<bool> {
        self.tree_done.subscribe()
    }

    pub fn activity(&self) -> watch::Receiver<()> {
        self.activity.subscribe()
    }

    /// When output last arrived or showed, and whether a PTY task's terminal holds output in a
    /// synchronized update that has not ended yet, taken together.
    pub fn last_output(&self) -> (Option<Instant>, bool) {
        let terminal = self.terminal.as_ref().map(lock);
        let out = lock(&self.out);
        let sync_pending = terminal.is_some_and(|pty| pty.live.sync_deadline().is_some());
        (out.last_output, sync_pending)
    }

    /// Where unseen output starts: the poll cursor and the cleaning state there.
    pub fn unseen(&self) -> (u64, Cleaner) {
        let view = lock(&self.view);
        (view.cursor, view.cleaner.clone())
    }

    /// Feeds `matcher` the stored output from `from` on, advancing `from` past what it read,
    /// and ends it once output has ended; returns the lowest needle the matcher has found.
    pub fn scan(&self, matcher: &mut Matcher, from: &mut u64) -> std::io::Result<Option<usize>> {
        let (written, evicted, closed) = {
            let out = lock(&self.out);
            (out.written, out.evicted, out.closed)
        };
        if evicted {
            return Ok(None);
        }
        if written > *from {
            let mut file = File::open(&self.log_path)?;
            file.seek(SeekFrom::Start(*from))?;
            let mut buffer = vec![0_u8; 64 * 1024];
            while *from < written {
                let want = (written - *from).min(buffer.len() as u64) as usize;
                let read = file.read(&mut buffer[..want])?;
                if read == 0 {
                    break;
                }
                *from += read as u64;
                if matcher.push(&buffer[..read]) == Some(0) {
                    return Ok(Some(0));
                }
            }
        }
        Ok(if closed && *from >= written {
            matcher.finish()
        } else {
            matcher.best()
        })
    }

    pub fn snapshot(&self) -> Snapshot {
        let kept = self.keeps_processes();
        let end = lock(&self.end).clone();
        let out = lock(&self.out);
        Snapshot {
            state: match &end {
                None => "running",
                Some(end) if end.killed => "killed",
                Some(_) => "exited",
            },
            exit_code: end.as_ref().map(|end| end.exit_code),
            elapsed: end
                .as_ref()
                .map_or_else(|| self.started.elapsed(), |end| end.elapsed),
            lines: out.lines,
            dropped: out.dropped,
            evicted: out.evicted,
            output_open: end.as_ref().is_some_and(|end| end.output_open),
            expired: end.as_ref().is_some_and(|end| end.expired),
            log_error: out.log_error.clone(),
            kill_error: lock(&self.kill_error).clone(),
            leftovers: end.map(|end| end.leftovers).unwrap_or_default(),
            kept,
        }
    }

    /// Records that a delivered result showed this task's final state. Callers pass only
    /// tasks whose shown snapshot was final, so a task that ends after its snapshot stays due.
    pub fn mark_reported(&self) {
        self.reported.store(true, Ordering::SeqCst);
    }

    /// Input still queued for the writer: bytes the program's stdin has not accepted yet.
    pub fn input_pending(&self) -> bool {
        self.queued.load(Ordering::SeqCst) > 0
    }

    /// Reserves room in the input queue for `size` bytes.
    fn admit(&self, size: usize) -> bool {
        if self.queued.fetch_add(size, Ordering::SeqCst) + size > INPUT_QUEUE_LIMIT {
            self.queued.fetch_sub(size, Ordering::SeqCst);
            return false;
        }
        true
    }

    /// The keyboard modes of a PTY task's program.
    pub fn modes(&self) -> Result<crate::keys::Modes, String> {
        let terminal = self
            .terminal
            .as_ref()
            .ok_or("keys applies to PTY tasks only.")?;
        lock(terminal).live.modes()
    }

    /// The rendered terminal of a PTY task.
    pub fn screen(&self) -> Result<Screen, String> {
        let terminal = lock(
            self.terminal
                .as_ref()
                .ok_or("screen applies to PTY tasks only.")?,
        );
        let (rows, cursor) = terminal.live.screen()?;
        let log_end = lock(&self.out).readable.min(terminal.live.shown());
        Ok(Screen {
            rows,
            cursor,
            log_end,
        })
    }

    /// Where `transcript` writes its text, beside the log.
    pub fn transcript_path(&self) -> PathBuf {
        self.log_path
            .with_file_name(format!("{}.transcript.txt", self.id))
    }

    /// Writes the pending lines into the transcript file after its final lines, and returns
    /// what this read covers: the final lines no earlier read has shown, then the pending
    /// lines. The next read starts after these final lines.
    pub fn transcript(&self, shared: &Tasks) -> Result<Transcript, String> {
        let terminal = self
            .terminal
            .as_ref()
            .ok_or("transcript applies to PTY tasks only.")?;
        let mut pty = lock(terminal);
        if lock(&self.out).evicted {
            return Err("The files of this task were evicted under the 1 GiB total limit; its transcript is gone.".into());
        }
        let Pty { live, spool } = &mut *pty;
        if spool.file.is_none() {
            let reason = spool.stopped.as_deref().unwrap_or("it is gone");
            return Err(format!("The transcript file is missing: {reason}."));
        }
        // The result shows the pending lines only as the file holds them, those that fit the
        // per-task limit.
        let mut pending = String::new();
        let mut cut = false;
        if spool.stopped.is_none() {
            pending = live.pending();
            let room = (TASK_LOG_LIMIT - spool.bytes) as usize;
            if pending.len() > room {
                let fit = pending.as_bytes()[..room]
                    .iter()
                    .rposition(|&byte| byte == b'\n');
                pending.truncate(fit.map_or(0, |end| end + 1));
                cut = true;
            }
            if !spool.write(pending.as_bytes(), false, self, shared) {
                pending.clear();
            }
        }
        let stopped = if live.stopped() {
            Some("the terminal emulator stopped after an internal failure".to_string())
        } else {
            spool.stopped.clone()
        };
        let (start, shown) = std::mem::replace(&mut spool.shown, (spool.bytes, spool.lines));
        Ok(Transcript {
            path: self.transcript_path(),
            first_line: shown + 1,
            start,
            lines: spool.lines,
            bytes: spool.bytes,
            pending,
            cut,
            stopped,
        })
    }

    /// Queues `input`, then closes stdin when `eof` is set. Validation happens before any write.
    pub fn send(&self, input: Option<&[u8]>, eof: bool) -> Result<(), String> {
        let data = input.filter(|bytes| !bytes.is_empty());
        if eof && self.pty {
            return Err("EOF_UNSUPPORTED_IN_PTY: a PTY has no stdin half-close. Press Ctrl-D with keys: [\"C-d\"], or kill the task.".into());
        }
        let mut sender = lock(&self.input);
        let Some(tx) = sender.as_ref() else {
            return match data {
                None => Ok(()), // nothing to deliver; closing a closed stdin is a no-op
                Some(_) if self.is_running() => Err(
                    "stdin is closed (eof was sent or the program closed it); the input was not delivered.".into(),
                ),
                Some(_) => Err("The task has exited; the input was not delivered.".into()),
            };
        };
        if let Some(text) = data {
            if !self.admit(text.len()) {
                return Err("INPUT_BACKPRESSURE: 1 MiB of earlier input is still waiting for the program to read it. Poll for output, then retry.".into());
            }
            if tx.send(Input::Data(text.to_vec())).is_err() {
                self.queued.fetch_sub(text.len(), Ordering::SeqCst);
                *sender = None;
                return Err("stdin is closed; the input was not delivered.".into());
            }
        }
        if eof {
            let _ = tx.send(Input::Eof);
            *sender = None;
        }
        Ok(())
    }

    /// Terminates the task's tree. A running task is marked killed, and the waiter thread records
    /// the exit; a finished task keeps its result while its kept processes end.
    pub fn kill(&self) -> std::io::Result<()> {
        if self.is_running() {
            self.kill_requested.store(true, Ordering::SeqCst);
        } else if !self.keeps_processes() {
            return Ok(());
        }
        // Recorded for a running task too: its root may have exited already, with the waiter
        // past its own kill.
        let killed = self.tree.kill();
        *lock(&self.kill_error) = killed.as_ref().err().map(|error| error.to_string());
        killed
    }

    /// Ends the task's tree because its `killAfterMs` lifetime elapsed.
    pub fn expire(&self) {
        if self.is_running() {
            self.expired.store(true, Ordering::SeqCst);
        }
        let _ = self.kill();
    }

    /// Returns the output after the cursor, up to `until` when given, as a window, and
    /// advances the cursor past it.
    pub fn read_window(
        &self,
        truncate: Truncate,
        budget: usize,
        raw: bool,
        encoding: &'static Encoding,
        until: Option<u64>,
    ) -> std::io::Result<Window> {
        let mut view = lock(&self.view);
        let (readable, written, evicted, closed) = {
            let out = lock(&self.out);
            (out.readable, out.written, out.evicted, out.closed)
        };
        let start = view.cursor;
        // `readable` holds back an unfinished UTF-8 sequence; other encodings hold back their
        // own unfinished character here, until the rest arrives or output ends.
        let end = if encoding == encoding_rs::UTF_8 || closed || evicted {
            readable
        } else {
            written - self.legacy_holdback(start, written, encoding)?
        };
        let end = until.map_or(end, |until| end.min(until));
        let first_line = view.lines_before + 1;
        let spec = WindowSpec {
            truncate,
            budget,
            raw,
            encoding,
            source: "log",
        };
        let mut builder = WindowBuilder::new(spec, &mut view.cleaner, first_line);
        if !evicted && end > start {
            let mut file = File::open(&self.log_path)?;
            file.seek(SeekFrom::Start(start))?;
            let mut remaining = end - start;
            let mut buffer = vec![0_u8; 64 * 1024];
            while remaining > 0 {
                let want = remaining.min(buffer.len() as u64) as usize;
                let read = file.read(&mut buffer[..want])?;
                if read == 0 {
                    break;
                }
                builder.push(&buffer[..read]);
                remaining -= read as u64;
            }
        }
        let window = builder.finish();
        view.cursor = end.max(start);
        view.lines_before += window.newlines;
        Ok(window)
    }

    /// Length of an unfinished `encoding` character at the end of the log range `start..end`.
    /// Decoding starts after the last byte below 0x30, which no ASCII-compatible encoding uses
    /// inside a multibyte character, or at `start`, where the previous window ended on a
    /// boundary.
    fn legacy_holdback(
        &self,
        start: u64,
        end: u64,
        encoding: &'static Encoding,
    ) -> std::io::Result<u64> {
        let mut file = File::open(&self.log_path)?;
        let mut buffer = [0_u8; 4096];
        let mut from = end;
        while from > start {
            let chunk_start = start.max(from.saturating_sub(buffer.len() as u64));
            let chunk = &mut buffer[..(from - chunk_start) as usize];
            file.seek(SeekFrom::Start(chunk_start))?;
            file.read_exact(chunk)?;
            if let Some(boundary) = chunk.iter().rposition(|&byte| byte < 0x30) {
                from = chunk_start + boundary as u64 + 1;
                break;
            }
            from = chunk_start;
        }
        let mut tail = vec![0_u8; (end - from) as usize];
        file.seek(SeekFrom::Start(from))?;
        file.read_exact(&mut tail)?;
        Ok(incomplete_legacy_suffix(&tail, encoding) as u64)
    }
}

fn private_file(path: &Path) -> std::io::Result<File> {
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options.open(path)
}

/// Log directories are `<prefix><pid>`. On Unix the prefix carries the user ID, so servers of
/// different users sharing /tmp neither collide nor examine each other's directories.
fn dir_prefix() -> String {
    #[cfg(unix)]
    // SAFETY: getuid has no preconditions and cannot fail.
    return format!("fastexec-{}-", unsafe { libc::getuid() });
    #[cfg(windows)]
    return "fastexec-".to_string();
}

/// Removes this user's log directories of fastexec servers that are no longer running.
fn remove_stale_dirs(temp: &Path) {
    let Ok(entries) = std::fs::read_dir(temp) else {
        return;
    };
    let prefix = dir_prefix();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let pid = name
            .to_str()
            .and_then(|name| name.strip_prefix(prefix.as_str()))
            .and_then(|pid| pid.parse().ok());
        if pid.is_some_and(|pid| pid != std::process::id() && !process::is_alive(pid)) {
            let _ = std::fs::remove_dir_all(entry.path());
        }
    }
}
