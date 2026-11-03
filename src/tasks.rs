//! Task registry: spawn, log capture, unseen-output cursors, input queues, and shutdown.

use crate::output::{
    Matcher, Truncate, Window, WindowBuilder, WindowSpec, incomplete_legacy_suffix,
    incomplete_utf8_suffix,
};
use crate::process::{self, Launch, Member, Tree};
use crate::session::{LaunchContext, ReadState, Session};
use crate::terminal::Live;
use encoding_rs::Encoding;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, mpsc};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::watch;

/// What a server admits and keeps: how many tasks may run at once, how many finished tasks stay
/// in the registry, and how long a finished one is kept.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_running: usize,
    pub max_finished: usize,
    pub finished_ttl: Option<Duration>,
}

impl Limits {
    /// A stdio server's limits: a session ends with the client, so finished tasks are kept and
    /// never expire.
    pub const OFF: Limits = Limits {
        max_running: 16,
        max_finished: 64,
        finished_ttl: None,
    };

    /// A daemon's limits: it serves more than one client and lives longer, so it admits more
    /// tasks and lets finished ones go after a day.
    #[allow(dead_code)] // the daemon entry point picks it in block 9
    pub const ON: Limits = Limits {
        max_running: 64,
        max_finished: 256,
        finished_ttl: Some(Duration::from_secs(24 * 60 * 60)),
    };
}

const TASK_LOG_LIMIT: u64 = 64 << 20;
const TOTAL_LOG_LIMIT: u64 = 1 << 30;
const INPUT_QUEUE_LIMIT: usize = 1 << 20;
/// Output that pauses this long has finished a frame: the chunks of one redraw arrive within
/// a millisecond or two of each other, frames 8 ms or more apart.
const FRAME_PAUSE: Duration = Duration::from_millis(4);
/// A pause this long showed its frame long enough to read, even in the middle of a redraw.
const LONG_PAUSE: Duration = Duration::from_millis(100);
/// How long output may stay open after the root exits and its tree is killed.
pub const DRAIN_CAP: Duration = Duration::from_secs(2);
/// Kept processes may hold the output open for good, so with them the drain also ends once the
/// output pauses this long.
const KEPT_PAUSE: Duration = Duration::from_millis(100);

/// Runs before a Windows ConPTY root's command, on its first line, so the script's exit status is
/// on disk as the script ends. The root can deadlock inside its own exit; this file is what lets
/// its task end anyway. A user's own EXIT cleanup is never overwritten, and a command that
/// replaces the trap or execs keeps the plain wait.
const EXIT_TRAP: &str = r#"[ -n "${FASTEXEC_EXIT_STATUS-}" ] && [ "${BASH_SUBSHELL-0}" -eq 0 ] && builtin export -n FASTEXEC_EXIT_STATUS && [ -z "$(builtin trap -p EXIT)" ] && builtin trap 'builtin printf "%s\n" "$?" 2>/dev/null > "$FASTEXEC_EXIT_STATUS" || :' EXIT || :; "#;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub struct Tasks {
    dir: PathBuf,
    /// Arms `killAfterMs`: the deadline belongs to the Core, not to the starting request.
    handle: Handle,
    /// This server's ID epoch: 128 bits from the OS random source as 26 Crockford base32
    /// characters. Every task ID starts with it, so an ID from another server is told apart.
    epoch: String,
    /// The counter value of the next ID; IDs never repeat within an epoch.
    next: AtomicU64,
    registry: Mutex<Registry>,
    limits: Limits,
    log_bytes: AtomicU64,
    /// [`TOTAL_LOG_LIMIT`]; debug builds take a smaller one from
    /// `FASTEXEC_TEST_TOTAL_LOG_LIMIT`, so tests reach it without writing 1 GiB.
    log_limit: u64,
}

#[derive(Default)]
struct Registry {
    tasks: Vec<Arc<Task>>,
    /// Starts admitted but not yet inserted; they count against [`Limits::max_running`].
    pending: usize,
    /// Set by [`Tasks::shutdown`]: no start is admitted from then on.
    stopping: bool,
}

/// A start in flight: it holds a place against [`Limits::max_running`] until it is inserted
/// or fails.
struct Pending<'a> {
    registry: &'a Mutex<Registry>,
    counted: bool,
}

impl Pending<'_> {
    /// Gives the place back inside the caller's critical section: its own drop must never
    /// re-lock a registry mutex the caller still holds.
    fn release(&mut self, registry: &mut Registry) {
        if self.counted {
            registry.pending -= 1;
            self.counted = false;
        }
    }
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if self.counted {
            lock(self.registry).pending -= 1;
        }
    }
}

/// Crockford's base32 alphabet in lowercase.
pub(crate) const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// `value` in Crockford's base32, most significant digit first, no padding beyond `digits`.
fn base32(mut value: u128, digits: usize) -> String {
    let digits = digits.max(1);
    let mut out = Vec::with_capacity(digits);
    loop {
        out.push(ALPHABET[(value & 31) as usize] as char);
        value >>= 5;
        if value == 0 && out.len() >= digits {
            break;
        }
    }
    out.iter().rev().collect()
}

/// An epoch and a counter value as a task ID.
fn task_id(epoch: &str, number: u64) -> String {
    format!("{epoch}-{}", base32(number as u128, 1))
}

/// `digits` characters (at most 26) from 128 random bits, for the epoch and session names.
pub fn random_chars(digits: usize) -> std::io::Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| {
        std::io::Error::other(format!(
            "Cannot read the OS random source for the task ID epoch: {error}."
        ))
    })?;
    let mut out = base32(u128::from_be_bytes(bytes), 26);
    out.truncate(digits);
    Ok(out)
}

/// A fresh 128-bit epoch from the OS random source: 26 characters, the top two bits zero.
fn new_epoch() -> std::io::Result<String> {
    random_chars(26)
}

/// The error a transcript read returns for a task whose files the total log limit evicted.
pub const EVICTED_TRANSCRIPT: &str =
    "The files of this task were evicted under the 1 GiB total limit; its transcript is gone.";

pub struct Task {
    pub id: String,
    pub command: String,
    pub pty: bool,
    pub log_path: PathBuf,
    pub encoding: &'static Encoding,
    /// `keepTree`: processes the root leaves running stay until kill or session end.
    pub keep: bool,
    /// The session that started the task: its client name and short session ID.
    pub owner: String,
    /// The session ID and daemon call that started the task, when a daemon call started it.
    pub started_by: Option<(String, u64)>,
    started: Instant,
    tree: Tree,
    /// Set once no process of the tree runs.
    tree_done: watch::Sender<bool>,
    /// When the tree ended, set before `tree_done`.
    tree_ended_at: OnceLock<Instant>,
    /// Why the last kill of the processes held past the root's exit failed.
    kill_error: Mutex<Option<String>>,
    kill_requested: AtomicBool,
    /// Set when `killAfterMs` elapsed and ended the task.
    expired: AtomicBool,
    out: Mutex<Output>,
    end: Mutex<Option<Final>>,
    done: watch::Sender<bool>,
    /// Signals each captured output chunk and each synchronized update its timeout ends.
    activity: watch::Sender<()>,
    input: Mutex<Option<mpsc::Sender<Input>>>,
    queued: AtomicUsize,
    /// One poll's `input`, `keys`, and `eof` go out as one sequence: this gate holds the next
    /// sequence out, so a delayed key press cannot land in the middle of another write.
    pub input_gate: tokio::sync::Mutex<()>,
    /// PTY tasks: a terminal emulator fed with every output byte, and the transcript it
    /// records. Its replies to terminal queries, such as the cursor-position report and device
    /// attributes ConPTY waits for at startup, go to the program as input.
    terminal: Option<Mutex<Pty>>,
    /// The terminal's size, columns first, as the program's terminal has it.
    size: Mutex<(u16, u16)>,
    /// A PTY task's size control, taken when the tree ends so a resize that arrives then fails
    /// before the terminal closes. A resize holds it while it runs, so the ender's close waits
    /// for it and the console's last reference stays on the ender.
    pty_control: Mutex<Option<process::PtyControl>>,
    /// Why the terminal's state is no longer trustworthy: a native resize failed, so the
    /// emulator and the program's terminal disagree. Screen and keys report it; raw output
    /// capture continues.
    size_error: Mutex<Option<String>>,
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
    /// The terminal's size, columns first.
    pub cols: u16,
    pub rows_count: u16,
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

#[derive(Clone)]
struct Final {
    exit_code: i32,
    killed: bool,
    expired: bool,
    /// The script had finished but the root did not terminate, so the root was ended for it.
    forced_end: bool,
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
#[derive(Clone)]
pub struct Snapshot {
    pub state: &'static str,
    pub exit_code: Option<i32>,
    /// The script had finished but the root did not terminate, so the root was ended for it.
    pub forced_end: bool,
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
    /// The processes the task held past the root's exit have ended.
    pub tree_ended: bool,
    /// The processes the task holds, when [`Task::snapshot_held`] took the snapshot.
    pub held: Vec<Member>,
    /// Why the last kill of the processes it holds failed.
    pub kill_error: Option<String>,
}

pub struct StartArgs {
    pub command: String,
    pub cwd: PathBuf,
    pub pty: bool,
    /// The terminal's size: columns, then screen rows. A pipe task ignores them.
    pub cols: u16,
    pub rows: u16,
    pub login: bool,
    pub keep: bool,
    pub encoding: &'static Encoding,
    /// `killAfterMs`: end the task's tree this many ms after launch; 0 or None means no limit.
    pub kill_after_ms: Option<u64>,
    /// The session that starts the task, shown as its owner.
    pub owner: String,
    /// The session ID and daemon call that started the task, shown by `list`; `None` for a
    /// stdio call, which has neither.
    pub started_by: Option<(String, u64)>,
    /// What the task inherits: its environment, and the bash found in it.
    pub launch: Arc<LaunchContext>,
}

/// `Tasks::new` uses one directory per process, so only one test builds one at a time.
#[cfg(test)]
pub(crate) static ONE_TASKS: Mutex<()> = Mutex::new(());

impl Tasks {
    pub fn new(limits: Limits) -> std::io::Result<Arc<Self>> {
        let temp = std::env::temp_dir();
        remove_stale_dirs(&temp);
        let dir = temp.join(format!("{}{}", dir_prefix(), std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        Self::new_in(limits, dir)
    }

    /// A registry whose task logs live in `dir`, which is created if it is missing.
    pub fn new_in(limits: Limits, dir: PathBuf) -> std::io::Result<Arc<Self>> {
        let epoch = new_epoch()?;
        let handle = Handle::try_current().map_err(|error| {
            std::io::Error::other(format!(
                "Tasks needs a tokio runtime for the killAfterMs timer: {error}."
            ))
        })?;
        #[cfg_attr(not(unix), allow(unused_mut))]
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        #[cfg(unix)]
        std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
        builder.create(&dir)?;
        Ok(Arc::new(Self {
            dir,
            handle,
            epoch,
            next: AtomicU64::new(1),
            registry: Mutex::default(),
            limits,
            log_bytes: AtomicU64::new(0),
            log_limit: std::env::var("FASTEXEC_TEST_TOTAL_LOG_LIMIT")
                .ok()
                .and_then(|limit| limit.parse().ok())
                .filter(|limit| cfg!(debug_assertions) && (1..=TOTAL_LOG_LIMIT).contains(limit))
                .unwrap_or(TOTAL_LOG_LIMIT),
        }))
    }

    pub fn find(&self, id: &str) -> Option<Arc<Task>> {
        lock(&self.registry)
            .tasks
            .iter()
            .find(|task| task.id == id)
            .cloned()
    }

    /// This server's ID epoch, the prefix every task ID of this server carries.
    pub fn epoch(&self) -> &str {
        &self.epoch
    }

    /// The next task ID: this epoch and the next counter value.
    fn next_id(&self) -> String {
        let number = self.next.fetch_add(1, Ordering::Relaxed);
        task_id(&self.epoch, number)
    }

    /// Every task, newest first.
    pub fn all(&self) -> Vec<Arc<Task>> {
        lock(&self.registry).tasks.iter().rev().cloned().collect()
    }

    /// Every task with the state a client sees. Both are read under the registry guard, so a
    /// list either runs before a completion's end is visible or after its retirement: nothing
    /// can show an end that the cap has not accounted for.
    pub fn listed(&self) -> Vec<(Arc<Task>, Snapshot)> {
        // The registry guard covers the state a list reads together with membership in it. The
        // processes a task holds are asked for after the guard is dropped, so a host that cannot
        // answer at once never blocks an admission, a list, or a retirement.
        let mut entries: Vec<(Arc<Task>, Snapshot)> = {
            let registry = lock(&self.registry);
            registry
                .tasks
                .iter()
                .rev()
                .map(|task| (Arc::clone(task), task.snapshot()))
                .collect()
        };
        for (task, snapshot) in &mut entries {
            if snapshot.kept {
                snapshot.held = task.tree.members().unwrap_or_default();
            }
        }
        entries
    }

    /// Takes a place against [`Limits::max_running`] and the next task ID. The guard gives the
    /// place back on drop, so every failed start releases it.
    fn admit(&self) -> Result<(String, Pending<'_>), String> {
        let mut registry = lock(&self.registry);
        if registry.stopping {
            return Err("The server is shutting down; the task was not started.".into());
        }
        let running = registry
            .tasks
            .iter()
            .filter(|task| task.is_running() || task.keeps_processes())
            .count();
        let limit = self.limits.max_running;
        if running + registry.pending >= limit {
            return Err(format!(
                "TOO_MANY_TASKS: {limit} tasks are running or keep processes. Wait for one to finish or kill one, then retry."
            ));
        }
        registry.pending += 1;
        Ok((
            self.next_id(),
            Pending {
                registry: &self.registry,
                counted: true,
            },
        ))
    }

    /// Ends the task when `killAfterMs` elapses. The deadline belongs to the task, not to the
    /// request that started it: it survives that request's cancellation and every later wait,
    /// and bounds kept processes.
    fn arm_deadline(&self, task: &Arc<Task>, kill_after_ms: Option<u64>) {
        let Some(limit) = kill_after_ms.filter(|&limit| limit > 0) else {
            return;
        };
        let (expiring, mut done) = (Arc::clone(task), task.tree_done());
        self.handle.spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(limit)) => expiring.expire(),
                _ = done.wait_for(|done| *done) => {}
            }
        });
    }

    pub fn start(self: &Arc<Self>, args: StartArgs) -> Result<Arc<Task>, String> {
        let kill_after_ms = args.kill_after_ms;
        // Admission holds a place against the running limit, so the bash lookup, the file
        // creation, and the spawn below all run without the registry lock.
        let (id, mut pending) = self.admit()?;
        let bash = args.launch.bash()?.to_path_buf();
        let log_path = self.dir.join(format!("{id}.log"));
        let log = private_file(&log_path)
            .map_err(|error| format!("Cannot create the task log: {error}."))?;
        let script_path = self.dir.join(format!("{id}.sh"));
        // A Windows ConPTY root can deadlock inside its own exit and never terminate. Its script
        // records its status as it ends, so the task can end anyway; a command that replaces the
        // trap or execs keeps the plain wait.
        let exit_status = (args.pty && cfg!(windows)).then(|| self.dir.join(format!("{id}.exit")));
        let command = match exit_status {
            Some(_) => format!("{EXIT_TRAP}{}", args.command),
            None => args.command.clone(),
        };
        let script = if command.len() > process::SCRIPT_THRESHOLD_BYTES {
            std::fs::write(&script_path, &command)
                .map_err(|error| format!("Cannot write the command script: {error}."))?;
            Some(script_path.as_path())
        } else {
            None
        };
        let launch = Launch {
            bash: &bash,
            command: &command,
            script,
            cwd: &args.cwd,
            env: &args.launch,
            login: args.login,
            pty: args.pty,
            cols: args.cols,
            rows: args.rows,
            exit_status: exit_status.as_deref(),
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
                stopped: None,
            };
            if args.encoding == encoding_rs::UTF_8 {
                match private_file(&self.dir.join(format!("{id}.transcript.txt"))) {
                    Ok(file) => spool.file = Some(file),
                    Err(error) => spool.stopped = Some(format!("it cannot be created: {error}")),
                }
            }
            Mutex::new(Pty {
                live: Live::at(args.cols, args.rows),
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
            owner: args.owner,
            started_by: args.started_by,
            started: Instant::now(),
            tree: spawned.tree,
            tree_done: watch::Sender::new(false),
            tree_ended_at: OnceLock::new(),
            kill_error: Mutex::new(None),
            kill_requested: AtomicBool::new(false),
            expired: AtomicBool::new(false),
            out: Mutex::default(),
            end: Mutex::new(None),
            done: watch::Sender::new(false),
            activity: watch::Sender::new(()),
            input: Mutex::new(Some(input_tx.clone())),
            queued: AtomicUsize::new(0),
            input_gate: tokio::sync::Mutex::new(()),
            terminal,
            size: Mutex::new((args.cols, args.rows)),
            pty_control: Mutex::new(spawned.pty),
            size_error: Mutex::new(None),
        });
        let mut registry = lock(&self.registry);
        registry.tasks.push(Arc::clone(&task));
        pending.release(&mut registry);
        let stopping = registry.stopping;
        drop(registry);
        if stopping {
            // The server began stopping while this tree was created: it ends now, and it joins
            // the registry anyway so the capture, input, and waiter threads clean up as usual.
            task.kill_requested.store(true, Ordering::SeqCst);
            let _ = task.tree.kill();
        }
        self.arm_deadline(&task, kill_after_ms);

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
            let (exit_code, forced_end) = child.wait(&waiter_task.tree);
            // Read at the root's exit, so a kill or deadline during the drain below leaves a
            // natural exit labeled as such.
            let killed = waiter_task.kill_requested.load(Ordering::SeqCst);
            let expired = waiter_task.expired.load(Ordering::SeqCst);
            // A task is its whole tree: whatever the root leaves behind ends with it, unless
            // the task keeps it.
            // A tree whose members could not be listed is not an empty tree: it stays held.
            let listed = if killed {
                Some(Vec::new())
            } else {
                waiter_task.tree.members()
            };
            let mut leftovers = listed.clone().unwrap_or_default();
            let mut kept = waiter_task.keep && !leftovers.is_empty();
            let kill_error = if kept {
                None
            } else {
                waiter_task.tree.kill().err().map(|error| error.to_string())
            };
            // A tree that could not be killed stays held, so kill and shutdown retry, and the
            // leftovers are what survived, also after a kill of the root.
            if kill_error.is_some() || listed.is_none() {
                kept = true;
                leftovers = waiter_task.tree.members().unwrap_or(leftovers);
                *lock(&waiter_task.kill_error) = kill_error;
            }
            // The end of the tree closes a PTY, so its output reaches EOF; ConPTY's close can
            // block, so it runs apart. The script stays until then: kept processes may run `$0`.
            let ender = Arc::clone(&waiter_task);
            let ender_shared = Arc::clone(&waiter_shared);
            std::thread::spawn(move || {
                let tree_ended = child.wait_tree(&ender.tree);
                if !tree_ended {
                    *lock(&ender.kill_error) =
                        Some("task host disconnected; tree cleanup is unconfirmed".into());
                    ender.activity.send_replace(());
                }
                if tree_ended {
                    // Unknown children may still use stdin, the terminal, or their script.
                    *lock(&ender.input) = None;
                    *lock(&ender.pty_control) = None;
                    let _ = std::fs::remove_file(&script_path);
                    if let Some(status) = &exit_status {
                        let _ = std::fs::remove_file(status);
                    }
                }
                drop(child);
                // A task held at its root's exit is finished only here, so the cap is applied
                // again under the guard that publishes the tree's end: retirement counts a task
                // once its tree ended, and this is when that becomes true.
                let mut registry = lock(&ender_shared.registry);
                if tree_ended {
                    let _ = ender.tree_ended_at.set(Instant::now());
                    ender.tree_done.send_replace(true);
                }
                ender_shared.retire_finished(&mut registry);
                drop(registry);
            });
            let output_open = drain(&waiter_task, &eof_rx, kept);
            // The registry guard spans the end and the retirement that follows it: a client that
            // reads the end, through a result or a list, then sees a registry within the cap.
            // Readers take the registry first, so they cannot look between the two steps.
            let mut registry = lock(&waiter_shared.registry);
            *lock(&waiter_task.end) = Some(Final {
                exit_code,
                killed,
                expired,
                forced_end,
                elapsed: waiter_task.started.elapsed(),
                output_open,
                leftovers,
                kept,
            });
            waiter_shared.retire_finished(&mut registry);
            drop(registry);
            waiter_task.done.send_replace(true);
        });
        Ok(task)
    }

    /// Keeps the [`Limits::max_finished`] most recently finished tasks and deletes the logs of
    /// older ones. Runs at each completion under the registry guard that shows its end, so the
    /// task that just finished always stays and no client sees the cap exceeded.
    fn retire_finished(&self, registry: &mut Registry) {
        // A task that keeps processes stays, so kill can still reach them.
        let mut finished: Vec<(Instant, String)> = registry
            .tasks
            .iter()
            .filter(|task| !task.keeps_processes())
            .filter_map(|task| task.ended_at().map(|at| (at, task.id.clone())))
            .collect();
        let keep = self.limits.max_finished;
        if finished.len() <= keep {
            return;
        }
        finished.sort();
        let victims: Vec<String> = finished[..finished.len() - keep]
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

    /// Removes every finished task that ended at least `finished_ttl` before `now`, deleting its
    /// log as `retire_finished` does. A task that keeps processes is not finished, so it stays.
    pub fn expire_finished(&self, now: Instant) {
        let Some(ttl) = self.limits.finished_ttl else {
            return;
        };
        // The victims leave the registry under the lock; their logs are deleted after it, so no
        // file work happens under the registry.
        let expired: Vec<Arc<Task>> = {
            let mut registry = lock(&self.registry);
            let mut expired = Vec::new();
            registry.tasks.retain(|task| {
                let gone = !task.keeps_processes()
                    && task
                        .ended_at()
                        .is_some_and(|ended| now.saturating_duration_since(ended) >= ttl);
                if gone {
                    expired.push(Arc::clone(task));
                }
                !gone
            });
            expired
        };
        for task in &expired {
            self.evict_log(task);
        }
    }

    /// Whether the registry holds nothing at all: no task and no start in flight.
    pub fn idle(&self) -> bool {
        let registry = lock(&self.registry);
        registry.tasks.is_empty() && registry.pending == 0
    }

    /// How many starts are admitted but not yet inserted.
    pub fn pending(&self) -> usize {
        lock(&self.registry).pending
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
    /// A task that keeps processes is not finished: they may still write to its log.
    fn reserve(&self, bytes: u64) -> bool {
        if self.log_bytes.fetch_add(bytes, Ordering::SeqCst) + bytes <= self.log_limit {
            return true;
        }
        let mut candidates: Vec<(Instant, Arc<Task>)> = lock(&self.registry)
            .tasks
            .iter()
            .filter(|task| !task.is_running() && !task.keeps_processes())
            .filter_map(|task| task.ended_at().map(|at| (at, Arc::clone(task))))
            .collect();
        // The log that ended earliest goes first, as retire_finished removes it: registry order
        // is start order, and a task that started first can end last.
        candidates
            .sort_by(|(a, first), (b, second)| a.cmp(b).then_with(|| first.id.cmp(&second.id)));
        for (_, task) in candidates {
            if self.log_bytes.load(Ordering::SeqCst) <= self.log_limit {
                break;
            }
            self.evict_log(&task);
        }
        if self.log_bytes.load(Ordering::SeqCst) <= self.log_limit {
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
        let mut registry = lock(&self.registry);
        // Admission closes first: no start is admitted from here on, and one already admitted
        // still inserts its task and then ends its tree.
        registry.stopping = true;
        for task in registry.tasks.iter() {
            if task.is_running() {
                task.kill_requested.store(true, Ordering::SeqCst);
            }
            let _ = task.tree.kill();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }

    /// One line naming the other tasks that need attention, at most `cap` bytes: finished tasks
    /// whose final state no result has shown yet (failures first), tasks whose held processes
    /// ended unseen, then running tasks and tasks whose held processes changed since a result
    /// showed them or failed to be killed. Returns the line and the
    /// tasks it names with the snapshots it showed, which the caller marks reported once the
    /// line is delivered.
    /// Drops this session's views of tasks that have retired. Registry then session locks, the
    /// order every other reader takes them in.
    pub fn prune_views(&self, session: &Session) {
        let registry = lock(&self.registry);
        session.touched(&registry.tasks);
    }

    pub fn background(
        &self,
        session: &Session,
        exclude: &str,
        cap: usize,
    ) -> Option<(String, Vec<Shown>)> {
        // Registry and session locks are taken together, in that order, so a task that starts
        // meanwhile cannot be pruned as retired; both are released before any snapshot.
        let touched = {
            let registry = lock(&self.registry);
            session.touched(&registry.tasks)
        };
        // Rank 0: failed or expired; 1: other finished, or held processes ended; 2: running or
        // holding processes. Newest first within a rank.
        #[derive(PartialEq)]
        enum Notice {
            Root,
            TreeEnded,
            Keeps,
            Running,
        }
        let mut entries: Vec<(u8, Notice, Arc<Task>, Snapshot)> = touched
            .into_iter()
            .filter(|(task, _)| task.id != exclude)
            .filter_map(|(task, view)| {
                let mut snapshot = task.snapshot();
                let (rank, notice) = match snapshot.state {
                    "running" => (2, Notice::Running),
                    _ if !view.is_reported() => {
                        let failed = snapshot.state == "exited" && snapshot.exit_code != Some(0)
                            || snapshot.expired;
                        (if failed { 0 } else { 1 }, Notice::Root)
                    }
                    _ if snapshot.tree_ended && !view.tree_end_reported() => (1, Notice::TreeEnded),
                    _ if snapshot.kept => {
                        snapshot.held = task.tree.members().unwrap_or_default();
                        if snapshot.kill_error.is_none()
                            && view.keeps_reported().as_ref() == Some(&pids(&snapshot.held))
                        {
                            return None;
                        }
                        (2, Notice::Keeps)
                    }
                    _ => return None,
                };
                Some((rank, notice, task, snapshot))
            })
            .collect();
        if entries.is_empty() {
            return None;
        }
        entries.sort_by_key(|(rank, ..)| *rank);
        let count = |rank: u8| entries.iter().filter(|entry| entry.0 == rank).count();
        let failed = count(0);
        let running = entries
            .iter()
            .filter(|entry| entry.1 == Notice::Running)
            .count();
        let keeping = count(2) - running;
        let finished = entries.len() - count(2);
        let mut counts = Vec::new();
        for (number, label) in [
            (running, "running"),
            (keeping, "keeping processes"),
            (finished, "finished"),
            (failed, "failed"),
        ] {
            if number > 0 {
                counts.push(format!("{number} {label}"));
            }
        }
        let counts = counts.join(", ");
        let shown_tasks = |shown: usize| {
            entries[..shown]
                .iter()
                .map(|(_, _, task, snapshot)| (Arc::clone(task), snapshot.clone()))
                .collect()
        };
        for shown in (1..=entries.len().min(3)).rev() {
            let named: Vec<String> = entries[..shown]
                .iter()
                .map(|(_, notice, task, snapshot)| {
                    let id = &task.id;
                    match (notice, snapshot.exit_code) {
                        (Notice::Running, _) => {
                            format!("{id} running {}", short(snapshot.elapsed))
                        }
                        (Notice::Keeps, _) => format!("{id} keeps {}", brief(&snapshot.held)),
                        (Notice::TreeEnded, _) => format!("{id} kept processes ended"),
                        (_, code) => {
                            let root = match code {
                                Some(code) if snapshot.state == "exited" => {
                                    format!("exited {code}")
                                }
                                _ if snapshot.expired => "killed by killAfterMs".into(),
                                _ => "killed".into(),
                            };
                            // The notice marks the end of the held processes shown too.
                            let tree = if snapshot.tree_ended {
                                " and its kept processes ended"
                            } else {
                                ""
                            };
                            format!("{id} {root}{tree}")
                        }
                    }
                })
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
                return Some((line, shown_tasks(shown)));
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

/// A task and the snapshot a result showed of it.
pub type Shown = (Arc<Task>, Snapshot);

/// The sorted PIDs of `held`.
pub(crate) fn pids(held: &[Member]) -> Vec<u32> {
    let mut pids: Vec<u32> = held.iter().map(|member| member.pid).collect();
    pids.sort_unstable();
    pids
}

/// The first held process and how many more: `adb.exe +2`.
fn brief(held: &[Member]) -> String {
    match held {
        [] => "processes".into(),
        [one] => one.name.clone(),
        [first, rest @ ..] => format!("{} +{}", first.name, rest.len()),
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
        // An eviction since the room check, its own reservation's included, already gave back
        // what the log held; the chunk's reservation goes back too.
        if stored && out.evicted {
            shared.log_bytes.fetch_sub(size, Ordering::SeqCst);
            stored = false;
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

    /// When the task finished: its root, or the processes it held past the root's exit if they
    /// ended later.
    fn ended_at(&self) -> Option<Instant> {
        let root = lock(&self.end)
            .as_ref()
            .map(|end| self.started + end.elapsed)?;
        Some(
            self.tree_ended_at
                .get()
                .map_or(root, |&tree| tree.max(root)),
        )
    }

    /// The root has exited, and processes it left run on, under `keepTree` or because killing
    /// them failed.
    pub fn keeps_processes(&self) -> bool {
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
            if let Err(error) = self.scan_log(matcher, from, written) {
                // Eviction can delete the log between the flags above and this read.
                if !lock(&self.out).evicted {
                    return Err(error);
                }
                return Ok(None);
            }
            if matcher.best() == Some(0) {
                return Ok(Some(0));
            }
        }
        Ok(if closed && *from >= written {
            matcher.finish()
        } else {
            matcher.best()
        })
    }

    /// Feeds the matcher the stored output from `from` to `written`, advancing `from`; an error
    /// leaves `from` where the last complete read put it.
    fn scan_log(&self, matcher: &mut Matcher, from: &mut u64, written: u64) -> std::io::Result<()> {
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
                break;
            }
        }
        Ok(())
    }

    /// How the root ended, once it has: its exit code, and whether the task held processes past
    /// the exit.
    pub fn root_end(&self) -> Option<(i32, bool)> {
        lock(&self.end)
            .as_ref()
            .map(|end| (end.exit_code, end.kept))
    }

    /// Output has reached EOF.
    pub fn output_closed(&self) -> bool {
        lock(&self.out).closed
    }

    /// Whether the total log limit took this task's files.
    pub fn evicted(&self) -> bool {
        lock(&self.out).evicted
    }

    /// A snapshot that also lists the processes the task holds.
    pub fn snapshot_held(&self) -> Snapshot {
        let mut snapshot = self.snapshot();
        if snapshot.kept {
            snapshot.held = self.tree.members().unwrap_or_default();
        }
        snapshot
    }

    pub fn snapshot(&self) -> Snapshot {
        let end = lock(&self.end).clone();
        let tree_done = *self.tree_done.borrow();
        let held_past_exit = end.as_ref().is_some_and(|end| end.kept);
        let out = lock(&self.out);
        Snapshot {
            state: match &end {
                None => "running",
                Some(end) if end.killed => "killed",
                Some(_) => "exited",
            },
            exit_code: end.as_ref().map(|end| end.exit_code),
            forced_end: end.as_ref().is_some_and(|end| end.forced_end),
            elapsed: end
                .as_ref()
                .map_or_else(|| self.started.elapsed(), |end| end.elapsed),
            lines: out.lines,
            dropped: out.dropped,
            evicted: out.evicted,
            output_open: end.as_ref().is_some_and(|end| end.output_open) && !out.closed,
            expired: end.as_ref().is_some_and(|end| end.expired),
            log_error: out.log_error.clone(),
            kill_error: lock(&self.kill_error).clone(),
            leftovers: end.map(|end| end.leftovers).unwrap_or_default(),
            kept: held_past_exit && !tree_done,
            tree_ended: held_past_exit && tree_done,
            held: Vec::new(),
        }
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
        self.check_size()?;
        let terminal = self
            .terminal
            .as_ref()
            .ok_or("keys applies to PTY tasks only.")?;
        lock(terminal).live.modes()
    }

    /// A terminal whose size the program never took has no screen or keyboard state to read.
    fn check_size(&self) -> Result<(), String> {
        match &*lock(&self.size_error) {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    /// The terminal's size, columns first.
    pub fn size(&self) -> (u16, u16) {
        *lock(&self.size)
    }

    /// Sends the terminal's replies to the program, as capture sends the replies of the output
    /// it reads. The input slot says when that is possible: it is cleared at EOF, at a write
    /// failure, and when the tree ends, and it is the only sender the writer thread's receiver
    /// outlives.
    pub fn answer(&self, replies: Vec<u8>) {
        let sender = lock(&self.input).clone();
        if let Some(sender) = sender {
            answer_program(self, &sender, replies);
        }
    }

    /// The terminal is `cols` by `rows` from now on. The emulator settles the transcript at the
    /// old size and reflows at the new one, the output's own replies go to the program, and then
    /// the program is told through the terminal's own handle, outside the lock capture needs. On
    /// failure the terminal's state is reported unusable while capture continues.
    pub fn resize(&self, shared: &Tasks, cols: u16, rows: u16) -> Result<(), String> {
        self.check_size()?;
        let terminal = self.terminal.as_ref().ok_or(
            "A pipe task has no terminal size; its output is a byte stream with no screen.",
        )?;
        // The control slot stays held from here to the end of the native resize, so the ender's
        // close waits for it and the console's last reference never moves into this thread. A
        // control the ender already took means the terminal is closed: nothing changes.
        let control = lock(&self.pty_control);
        let Some(control) = control.as_ref() else {
            return Err(
                "The task's tree has ended and its terminal is closed, so the size did not change."
                    .into(),
            );
        };
        let (old_cols, old_rows) = self.size();
        if (cols, rows) == (old_cols, old_rows) {
            // The size a task already has changes nothing: no SIGWINCH, no repaint.
            return Ok(());
        }
        let replies = {
            let mut pty = lock(terminal);
            // A synchronized update the resize ends shows its frame now, so quiet counts from
            // now, exactly as capture's own end of one does. An ordinary resize shows nothing.
            let ended_frame = pty.live.sync_deadline().is_some();
            if pty.live.stopped() {
                return Err(
                    "The terminal emulator stopped after an internal failure, so the size did not change; raw output capture continues."
                        .into(),
                );
            }
            let replies = pty.live.resize(cols, rows);
            let written = pty.live.take_written();
            pty.spool.append(&written, self, shared);
            if ended_frame {
                lock(&self.out).last_output = Some(Instant::now());
            }
            // The resize runs the emulator, which stops on an internal failure rather than
            // unwinding: a stopped one never took the new size.
            if pty.live.stopped() {
                *lock(&self.size_error) = Some(
                    "The terminal emulator stopped after an internal failure during a resize, so its size is unknown. Raw output capture continues; start a new task for a terminal of a known size."
                        .into(),
                );
                self.activity.send_replace(());
                return Err(
                    "The terminal emulator stopped after an internal failure, so the size did not change; raw output capture continues."
                        .into(),
                );
            }
            replies
        };
        // The replies answer the output applied at the old size: they go to the program whatever
        // happens to the native call below.
        self.answer(replies);
        // The screen changed under every reader: a wait in another call, on screenAppears or
        // screenGone, wakes now rather than at its deadline. A failure wakes it for the state it
        // has to report.
        self.activity.send_replace(());
        if let Err(error) = control.resize(cols, rows) {
            // The program's terminal kept the size it had, so the task still reports that one.
            // The program's terminal kept the size it had, and a reflow cannot be reversed, so
            // the emulator's size and the program's disagree from here on. Screen and keys say
            // so; raw output capture continues, as it does after an internal emulator failure.
            *lock(&self.size_error) = Some(format!(
                "The terminal's size is unknown: a resize to {cols}x{rows} failed ({error}), so the screen may not match the program. Raw output capture continues; start a new task for a terminal of a known size."
            ));
            // A waiter may have taken the wake above before the failure landed; this one wakes
            // the state it has to report.
            self.activity.send_replace(());
            return Err(format!(
                "Cannot resize the terminal to {cols}x{rows}: {error}. The task keeps running and its output is captured; start a new task for a terminal of a known size."
            ));
        }
        *lock(&self.size) = (cols, rows);
        Ok(())
    }

    /// The rendered terminal of a PTY task.
    pub fn screen(&self) -> Result<Screen, String> {
        self.check_size()?;
        let terminal = lock(
            self.terminal
                .as_ref()
                .ok_or("screen applies to PTY tasks only.")?,
        );
        let (rows, cursor) = terminal.live.screen()?;
        let log_end = lock(&self.out).readable.min(terminal.live.shown());
        // The screen and its dimensions are one snapshot of the emulator's grid: another call's
        // resize may already have moved it past what the program's terminal has taken.
        let (cols, rows_count) = terminal.live.size();
        Ok(Screen {
            cols,
            rows_count,
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
    /// what this read covers: the final lines no earlier read of this session has shown, then
    /// the pending lines. The next read starts after these final lines.
    pub fn transcript(&self, shared: &Tasks, state: &mut ReadState) -> Result<Transcript, String> {
        let terminal = self
            .terminal
            .as_ref()
            .ok_or("transcript applies to PTY tasks only.")?;
        let mut pty = lock(terminal);
        if lock(&self.out).evicted {
            return Err(EVICTED_TRANSCRIPT.into());
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
        let (start, shown) = std::mem::replace(&mut state.shown, (spool.bytes, spool.lines));
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

    /// Returns the output after the session's cursor, up to `until` when given, as a window,
    /// and advances that cursor past it.
    pub fn read_window(
        &self,
        state: &mut ReadState,
        truncate: Truncate,
        budget: usize,
        raw: bool,
        encoding: &'static Encoding,
        until: Option<u64>,
    ) -> std::io::Result<Window> {
        if lock(&self.out).evicted {
            return Ok(self.read_evicted(state, truncate, budget, raw, encoding, until));
        }
        match self.read_stored(state, truncate, budget, raw, encoding, until) {
            Ok(window) => Ok(window),
            Err(_) if lock(&self.out).evicted => {
                // Eviction deleted the log between the flags above and the read. The window is
                // empty either way, so the half-read state is simply not committed.
                Ok(self.read_evicted(state, truncate, budget, raw, encoding, until))
            }
            Err(error) => Err(error),
        }
    }

    /// The window of an evicted task: an empty one. The cursor moves past the bytes that are
    /// gone, so the next read starts where this one would have ended; the eviction itself is
    /// reported by the snapshot, not by inventing an omitted line range.
    fn read_evicted(
        &self,
        state: &mut ReadState,
        truncate: Truncate,
        budget: usize,
        raw: bool,
        encoding: &'static Encoding,
        until: Option<u64>,
    ) -> Window {
        let end = lock(&self.out).readable;
        let start = state.cursor;
        let end = until.map_or(end, |until| end.min(until));
        let spec = WindowSpec {
            truncate,
            budget,
            raw,
            encoding,
            source: "log",
        };
        let window = WindowBuilder::new(spec, &mut state.cleaner, state.lines_before + 1).finish();
        state.cursor = end.max(start);
        window
    }

    fn read_stored(
        &self,
        state: &mut ReadState,
        truncate: Truncate,
        budget: usize,
        raw: bool,
        encoding: &'static Encoding,
        until: Option<u64>,
    ) -> std::io::Result<Window> {
        let (readable, written, closed) = {
            let out = lock(&self.out);
            (out.readable, out.written, out.closed)
        };
        let start = state.cursor;
        // `readable` holds back an unfinished UTF-8 sequence; other encodings hold back their
        // own unfinished character here, until the rest arrives or output ends.
        let end = if encoding == encoding_rs::UTF_8 || closed {
            readable
        } else {
            written - self.legacy_holdback(start, written, encoding)?
        };
        let end = until.map_or(end, |until| end.min(until));
        let first_line = state.lines_before + 1;
        let spec = WindowSpec {
            truncate,
            budget,
            raw,
            encoding,
            source: "log",
        };
        // A read that fails part way leaves the cleaning state as it was: the fallback starts
        // from the same bytes, so nothing is half-consumed.
        let mut cleaner = state.cleaner.clone();
        let mut builder = WindowBuilder::new(spec, &mut cleaner, first_line);
        if end > start {
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
        state.cursor = end.max(start);
        state.lines_before += window.newlines;
        state.cleaner = cleaner;
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

// Pure ID logic, so it runs on every platform; the tests below launch commands.
#[cfg(test)]
mod ids {
    use super::*;

    #[test]
    fn epoch_is_26_crockford_characters_and_differs_between_servers() {
        let epoch = new_epoch().unwrap();
        assert_eq!(epoch.len(), 26, "{epoch}");
        assert!(epoch.bytes().all(|b| ALPHABET.contains(&b)), "{epoch}");
        assert_ne!(epoch, new_epoch().unwrap());
    }

    #[test]
    fn base32_is_minimal_and_most_significant_first() {
        assert_eq!(base32(1, 1), "1");
        assert_eq!(base32(31, 1), "z");
        assert_eq!(base32(32, 1), "10");
        assert_eq!(base32(0, 1), "0");
        // A zero source still fills the whole epoch width.
        assert_eq!(base32(0, 26), "0".repeat(26));
    }

    #[test]
    fn task_ids_carry_the_epoch_and_never_repeat() {
        let epoch = new_epoch().unwrap();
        let first = task_id(&epoch, 1);
        let second = task_id(&epoch, 2);
        assert_eq!(first, format!("{epoch}-1"));
        assert_eq!(second, format!("{epoch}-2"));
        assert_ne!(first, second);
        assert_ne!(task_id(&epoch, 1), task_id(&new_epoch().unwrap(), 1));
    }
}

// Unix tasks run under the fastexec binary as their host, which a test binary is not.
#[cfg(all(test, windows))]
mod tests {
    use super::*;
    use crate::output::Cleaner;
    use crate::service::{Call, OutputMode, Render, Request, Service};

    /// A `Tasks` for a test: it needs a runtime for its `killAfterMs` timer, which these tests
    /// do not arm.
    fn tasks() -> (tokio::runtime::Runtime, Arc<Tasks>) {
        tasks_with(Limits::OFF)
    }

    fn tasks_with(limits: Limits) -> (tokio::runtime::Runtime, Arc<Tasks>) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let tasks = {
            let _entered = runtime.enter();
            Tasks::new(limits).unwrap()
        };
        (runtime, tasks)
    }

    /// This process's environment, as a session's launch context.
    fn launch() -> Arc<LaunchContext> {
        Arc::new(LaunchContext::new(
            std::env::temp_dir(),
            std::env::vars_os().collect(),
        ))
    }

    fn session(label: &str) -> Arc<Session> {
        Session::new(label.into(), label.into(), launch())
    }

    fn start_args() -> StartArgs {
        StartArgs {
            command: "printf abc".into(),
            cwd: std::env::temp_dir(),
            pty: false,
            cols: process::PTY_COLS,
            rows: process::PTY_ROWS,
            login: false,
            keep: false,
            encoding: encoding_rs::UTF_8,
            kill_after_ms: None,
            owner: "test-client".into(),
            started_by: None,
            launch: launch(),
        }
    }

    #[test]
    fn a_chunk_whose_reservation_evicts_its_own_log_gives_it_back() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (_runtime, tasks) = tasks();
        let task = tasks.start(start_args()).unwrap();
        // Its own capture is done too, so nothing else touches the accounting below.
        let deadline = Instant::now() + Duration::from_secs(30);
        while task.is_running() || !task.output_closed() {
            assert!(Instant::now() < deadline, "the task never finished");
            std::thread::sleep(Duration::from_millis(10));
        }
        // Reserving the 99-byte chunk crosses the limit and evicts the finished task's 3 bytes.
        let before = tasks.log_limit - 97;
        tasks.log_bytes.store(before, Ordering::SeqCst);
        let log = File::create(tasks.dir.join("extra.log")).unwrap();
        capture(
            &tasks,
            &task,
            Box::new(std::io::Cursor::new([b'x'; 99])),
            log,
            None,
        );
        let out = lock(&task.out);
        assert!(out.evicted);
        assert_eq!((out.written, out.dropped), (3, 99));
        assert_eq!(tasks.log_bytes.load(Ordering::SeqCst), before - 3);
        drop(out);
        tasks.shutdown();
    }

    #[test]
    fn a_list_call_drops_the_views_of_tasks_that_have_retired() {
        let _one_at_a_time = lock(&ONE_TASKS);
        // One finished task is kept, so the earlier ones retire as the loop goes on.
        let (runtime, tasks) = tasks_with(Limits {
            max_running: 16,
            max_finished: 1,
            finished_ttl: None,
        });
        let session = session("prune");
        let mut started = Vec::new();
        for _ in 0..3 {
            let task = tasks.start(start_args()).unwrap();
            // What a start does with the task it created, and what a retired one leaves behind.
            session.view(&task);
            finish(&task);
            started.push(task);
        }
        let retired = &started[0];
        assert!(
            tasks.find(&retired.id).is_none(),
            "the registry kept the retired task"
        );
        assert!(
            session.existing(retired).is_some(),
            "the view was gone before the call"
        );
        // A list call is all it takes; it never reaches the background footer.
        let service = Service::new(Arc::clone(&tasks), false);
        let request: Request =
            serde_json::from_value(serde_json::json!({"action": "list"})).unwrap();
        let call = Call {
            session: &session,
            render: Render {
                output_mode: OutputMode::Text,
                structured_content: true,
            },
            cancel: tokio_util::sync::CancellationToken::new(),
            progress: None,
            call_id: None,
        };
        runtime.block_on(service.call(request, call));
        assert!(
            session.existing(retired).is_none(),
            "a retired task's view survived a list call"
        );
        tasks.shutdown();
    }

    #[test]
    fn expire_finished_drops_a_task_past_its_ttl_and_keeps_a_running_one() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (_runtime, tasks) = tasks_with(Limits::ON);
        let finished = tasks.start(start_args()).unwrap();
        finish(&finished);
        let running = tasks
            .start(StartArgs {
                command: "sleep 30".into(),
                ..start_args()
            })
            .unwrap();
        let ttl = Limits::ON.finished_ttl.unwrap();
        // Nothing is old enough yet, so nothing goes.
        tasks.expire_finished(Instant::now());
        assert!(tasks.find(&finished.id).is_some(), "a fresh finish expired");
        // A day later the finished task goes, its log with it, and the running one stays.
        tasks.expire_finished(Instant::now() + ttl);
        assert!(
            tasks.find(&finished.id).is_none(),
            "the expired task stays in the registry"
        );
        assert!(
            !tasks.dir.join(format!("{}.log", finished.id)).exists(),
            "the expired task's log stays"
        );
        assert!(
            tasks.find(&running.id).is_some(),
            "a running task expired with the finished ones"
        );
        tasks.shutdown();
    }

    #[test]
    fn idle_and_pending_report_an_empty_registry() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (_runtime, tasks) = tasks_with(Limits::OFF);
        assert!(tasks.idle(), "a fresh registry is not idle");
        assert_eq!(tasks.pending(), 0);
        // A start in flight is not idle, even before its task is inserted.
        lock(&tasks.registry).pending = 1;
        assert_eq!(tasks.pending(), 1);
        assert!(!tasks.idle(), "a start in flight counts as idle");
        lock(&tasks.registry).pending = 0;
        tasks.shutdown();
    }

    #[test]
    fn the_running_limit_is_the_configured_one_and_the_refusal_names_it() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (_runtime, tasks) = tasks_with(Limits {
            max_running: 2,
            ..Limits::OFF
        });
        let long = || StartArgs {
            command: "sleep 30".into(),
            ..start_args()
        };
        let first = tasks.start(long()).unwrap();
        let second = tasks.start(long()).unwrap();
        let Err(refused) = tasks.start(start_args()) else {
            panic!("a third task was admitted under a limit of 2");
        };
        assert!(refused.starts_with("TOO_MANY_TASKS"), "{refused}");
        assert!(refused.contains('2'), "{refused}");
        // The limit is the configured one, not a constant.
        assert_eq!(
            tasks.all().iter().filter(|task| task.is_running()).count(),
            2
        );
        drop((first, second));
        tasks.shutdown();
    }

    #[test]
    fn admission_counts_pending_starts_and_closes_while_stopping() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (_runtime, tasks) = tasks();
        // A start in flight counts against the limit as if its task were already running.
        lock(&tasks.registry).pending = Limits::OFF.max_running;
        let Err(refused) = tasks.start(start_args()) else {
            panic!(
                "a start was admitted with {} starts in flight",
                Limits::OFF.max_running
            );
        };
        assert!(refused.starts_with("TOO_MANY_TASKS"), "{refused}");
        lock(&tasks.registry).pending = 0;
        // A stopping server admits nothing.
        lock(&tasks.registry).stopping = true;
        let Err(refused) = tasks.start(start_args()) else {
            panic!("a start was admitted while the server was stopping");
        };
        assert!(refused.contains("shutting down"), "{refused}");
        lock(&tasks.registry).stopping = false;
        // A start that fails after admission gives its place back, so the limit is not eaten.
        let failed = StartArgs {
            cwd: std::env::temp_dir().join("fastexec-no-such-directory"),
            ..start_args()
        };
        assert!(
            tasks.start(failed).is_err(),
            "a start with a missing cwd ran"
        );
        assert_eq!(lock(&tasks.registry).pending, 0);
        tasks.shutdown();
    }

    /// The text of a result, whatever its content blocks.
    fn result_text(result: &rmcp::model::CallToolResult) -> String {
        result
            .content
            .iter()
            .filter_map(|block| block.as_text().map(|text| text.text.clone()))
            .collect()
    }

    /// Waits for `task` to finish: its root ended, its output closed, and its tree done, since
    /// `ended_at` takes the later of the root's end and the tree's.
    fn finish(task: &Arc<Task>) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while task.is_running() || !task.output_closed() || !*task.tree_done().borrow() {
            assert!(Instant::now() < deadline, "the task never finished");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn eviction_takes_the_log_that_ended_earliest_not_the_one_that_started_first() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (_runtime, tasks) = tasks();
        // The first task cannot finish until it gets a line: no timing guess.
        let slow = tasks
            .start(StartArgs {
                command: "read -r line; printf '%s' \"$line\"".into(),
                ..start_args()
            })
            .unwrap();
        let quick = tasks.start(start_args()).unwrap();
        finish(&quick);
        slow.send(
            Some(
                b"done
",
            ),
            false,
        )
        .unwrap();
        finish(&slow);
        // Registry order is start order here, and the end times are the other way round.
        let (slow_end, quick_end) = (slow.ended_at().unwrap(), quick.ended_at().unwrap());
        assert!(
            quick_end < slow_end,
            "the fixture did not end in the wrong order"
        );
        // One byte over the limit: exactly one eviction is needed, and it must be the earliest end.
        tasks.log_bytes.store(tasks.log_limit - 1, Ordering::SeqCst);
        assert!(tasks.reserve(2), "the reservation failed");
        assert!(
            lock(&quick.out).evicted,
            "the log that ended earliest was not evicted first"
        );
        assert!(
            !lock(&slow.out).evicted,
            "the log that ended last was evicted first"
        );
        assert!(!tasks.dir.join(format!("{}.log", quick.id)).exists());
        assert!(tasks.dir.join(format!("{}.log", slow.id)).exists());
        tasks.shutdown();
    }

    #[test]
    fn a_read_of_an_evicted_log_is_an_outcome_not_an_error() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (_runtime, tasks) = tasks();
        let task = tasks.start(start_args()).unwrap();
        finish(&task);
        let written = lock(&task.out).written;
        assert!(written > 0, "the task wrote nothing");
        tasks.evict_log(&task);
        let mut state = ReadState::default();
        // Every read path that opens the deleted file reports the eviction instead of an error.
        let window = task
            .read_window(
                &mut state,
                Truncate::None,
                4096,
                false,
                encoding_rs::UTF_8,
                None,
            )
            .expect("an evicted log is not an I/O error");
        // Empty, with no invented line range: the snapshot reports the eviction.
        assert_eq!(window.fit(usize::MAX).0, "");
        assert_eq!(window.omitted, None);
        assert_eq!(state.cursor, written, "the cursor moved past the output");
        // A second read has nothing more to move past and stays empty.
        let again = task
            .read_window(
                &mut state,
                Truncate::None,
                4096,
                false,
                encoding_rs::UTF_8,
                None,
            )
            .unwrap();
        assert_eq!(again.fit(usize::MAX).0, "");
        assert_eq!(state.cursor, written);
        // A legacy encoding takes its own path through the same read.
        assert!(
            task.read_window(
                &mut ReadState::default(),
                Truncate::Tail,
                1024,
                false,
                encoding_rs::BIG5,
                None,
            )
            .is_ok()
        );
        // So does the return_when scan.
        let mut matcher =
            Matcher::new(&["abc".to_string()], encoding_rs::UTF_8, Cleaner::default());
        let mut from = 0;
        assert_eq!(task.scan(&mut matcher, &mut from).unwrap(), None);
        // A log that is gone while the task is not evicted stays an error.
        let live = tasks.start(start_args()).unwrap();
        finish(&live);
        std::fs::remove_file(tasks.dir.join(format!("{}.log", live.id))).unwrap();
        let mut matcher =
            Matcher::new(&["abc".to_string()], encoding_rs::UTF_8, Cleaner::default());
        let mut from = 0;
        assert!(
            live.scan(&mut matcher, &mut from).is_err(),
            "a missing log of a task that was not evicted is a real error"
        );
        tasks.shutdown();
    }

    #[test]
    fn a_session_launch_context_supplies_the_environment_and_default_cwd() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (runtime, tasks) = tasks();
        let dir = std::env::temp_dir().join(format!("fastexec-ctx-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        // A variable of this process that the context leaves out: a task must not inherit it.
        let leaked = std::env::vars_os()
            .find(|(name, _)| name.to_string_lossy().starts_with("CARGO_"))
            .map(|(name, _)| name.to_string_lossy().into_owned());
        let mut env: Vec<(std::ffi::OsString, std::ffi::OsString)> = std::env::vars_os()
            .filter(|(name, _)| Some(name.to_string_lossy()) != leaked.as_deref().map(Into::into))
            .collect();
        env.push(("FASTEXEC_CTX_PROBE".into(), "xyz".into()));
        let launch = Arc::new(LaunchContext::new(dir.clone(), env));
        let probe = leaked
            .as_ref()
            .map(|name| format!("${{{name}-NONE}}"))
            .unwrap_or_default();
        let command =
            format!("printf '%s|%s|%s' \"$FASTEXEC_CTX_PROBE\" \"$(pwd -P)\" \"{probe}\"");
        let service = crate::service::Service::new(Arc::clone(&tasks), false);
        let session = Session::new("ctx-1".into(), "ctx-1".into(), Arc::clone(&launch));
        let call = || crate::service::Call {
            session: &session,
            render: crate::service::Render {
                output_mode: crate::service::OutputMode::Text,
                structured_content: true,
            },
            cancel: tokio_util::sync::CancellationToken::new(),
            progress: None,
            call_id: None,
        };
        let name = dir.file_name().unwrap().to_string_lossy().into_owned();
        // A pipe task and a PTY task see the same environment and the same default directory.
        for pty in [false, true] {
            let result = runtime.block_on(service.call_json(
                serde_json::json!({
                    "action": "start",
                    "command": command,
                    "pty": pty,
                    "waitMs": 30000,
                }),
                call(),
            ));
            let text = result_text(&result);
            assert!(
                text.contains("xyz|"),
                "the context's environment reached the pty={pty} task: {text}"
            );
            assert!(
                text.contains(&name),
                "the default cwd came from the context in the pty={pty} task: {text}"
            );
            if leaked.is_some() {
                assert!(
                    text.contains("|NONE"),
                    "the variable the context left out is unset in the pty={pty} task: {text}"
                );
            }
        }
        tasks.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A native resize that fails leaves the terminal's state unusable while raw output capture
    /// continues, and the size the program's terminal kept is the one the task reports. A console
    /// A finished PTY task is released: no sender, thread, or registry entry holds it once it
    /// ended, was retired, and every owner dropped. A stored answer sender kept the writer
    /// thread's receiver connected, and the writer thread held the task: the weak reference
    /// never expired.
    #[test]
    fn a_finished_pty_task_is_released() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (runtime, tasks) = tasks_with(Limits {
            max_finished: 0,
            ..Limits::OFF
        });
        let mut args = start_args();
        args.command = "read -r line".into();
        args.pty = true;
        let task = tasks.start(args).unwrap();
        let weak = Arc::downgrade(&task);
        task.kill().unwrap();
        runtime.block_on(async {
            let mut tree_done = task.tree_done();
            let _ = tree_done.wait_for(|done| *done).await;
        });
        // The ender retires the task just after it publishes the end: the registry lets go.
        let id = task.id.clone();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while tasks.listed().iter().any(|(task, _)| task.id == id) {
            assert!(
                std::time::Instant::now() < deadline,
                "the finished task stays in the registry"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        drop(task);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while weak.upgrade().is_some() {
            assert!(
                std::time::Instant::now() < deadline,
                "the finished task is still held"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        tasks.shutdown();
    }

    /// The permanent state a failed native resize leaves: the size is unknown from then on.
    fn size_unknown(error: &str) -> bool {
        error.contains("size is unknown")
    }

    /// A resize racing a kill: the two run concurrently, every resize either applies or is
    /// refused with the reason the terminal gives, and a resize after the end always refuses.
    #[test]
    fn a_resize_racing_a_kill_applies_or_refuses_and_never_breaks_the_task() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (runtime, tasks) = tasks();
        let mut args = start_args();
        args.command = "read -r line".into();
        args.pty = true;
        let task = tasks.start(args).unwrap();
        let racer = Arc::clone(&task);
        let shared = Arc::clone(&tasks);
        let ended = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let first = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stop = Arc::clone(&ended);
        let ran = Arc::clone(&first);
        let resizes = std::thread::spawn(move || {
            let (mut applied, mut refused) = (0, 0);
            let mut step = 0u16;
            while !stop.load(Ordering::SeqCst) {
                step = step.wrapping_add(1);
                match racer.resize(&shared, 100 + step % 100, 30) {
                    Ok(()) => applied += 1,
                    Err(error) => {
                        assert!(
                            error.contains("tree has ended")
                                || error.contains("terminal is closed")
                                || error.contains("Cannot resize the terminal")
                                || size_unknown(&error),
                            "a resize before the end applies, fails natively, or reports the state \
                             that failure left: {error}"
                        );
                        refused += 1;
                    }
                }
                ran.store(true, Ordering::SeqCst);
            }
            // The ender clears the control before it publishes the tree's end, so the first
            // resize after that end refuses at once. A native failure earlier in the race left a
            // permanent state error, which also refuses: a refusal either way, never a success.
            let error = racer
                .resize(&shared, 90, 20)
                .expect_err("a resize after the end refuses at once");
            assert!(
                error.contains("tree has ended") || size_unknown(&error),
                "{error}"
            );
            (applied, refused)
        });
        // The kill goes out once the worker has made a resize, so its first outcome is real.
        let ack = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !first.load(Ordering::SeqCst) {
            assert!(
                std::time::Instant::now() < ack,
                "the resize worker never ran"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        task.kill().unwrap();
        runtime.block_on(async {
            let mut tree_done = task.tree_done();
            let _ = tree_done.wait_for(|done| *done).await;
        });
        ended.store(true, Ordering::SeqCst);
        let (applied, refused) = resizes.join().unwrap();
        assert!(applied + refused > 0, "the race has outcomes");
        // The size it already has is refused too: the closed terminal is checked before the no-op.
        let (cols, rows) = task.size();
        let error = task
            .resize(&tasks, cols, rows)
            .expect_err("a closed terminal refuses its own size");
        assert!(
            error.contains("tree has ended") || size_unknown(&error),
            "{error}"
        );
        let error = task
            .resize(&tasks, 90, 20)
            .expect_err("a closed terminal refuses a resize");
        assert!(
            error.contains("tree has ended") || size_unknown(&error),
            "{error}"
        );
        tasks.shutdown();
    }

    /// A resize is a change a pending wait can see. The task prints rows and then is silent, so
    /// only the resize can wake it: the wake carries the reflowed screen, where a shrunk terminal
    /// no longer shows the rows that scrolled out. Dropping the resize's own wake leaves the wait
    /// asleep and this fails on the deadline.
    #[test]
    fn a_resize_wakes_a_wait_with_the_reflowed_screen() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (runtime, tasks) = tasks();
        let mut args = start_args();
        args.pty = true;
        args.cols = 60;
        args.rows = 30;
        args.command = "for i in $(seq 1 20); do printf 'row %02d\\n' $i; done; \
                        printf 'tail-marker\\n'; read -r line"
            .into();
        let task = tasks.start(args).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !task
            .screen()
            .map(|screen| screen.rows.iter().any(|row| row.contains("tail-marker")))
            .unwrap_or(false)
        {
            assert!(
                std::time::Instant::now() < deadline,
                "the program never printed its rows"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        // Nothing else changes from here: the program is waiting for input.
        std::thread::sleep(std::time::Duration::from_millis(200));
        let before = task.screen().unwrap();
        let mut activity = task.activity();
        while activity.has_changed().unwrap_or(false) {
            activity.borrow_and_update();
        }
        let racer = Arc::clone(&task);
        let shared = Arc::clone(&tasks);
        let resizer = std::thread::spawn(move || racer.resize(&shared, 60, 10));
        let woke = runtime.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(10), activity.changed()).await
        });
        assert!(woke.is_ok(), "the resize woke the pending wait");
        resizer.join().unwrap().expect("the resize applies");
        let screen = task.screen().unwrap();
        assert_eq!(
            (screen.cols, screen.rows_count),
            (60, 10),
            "the screen is the new size"
        );
        // Reflow keeps the side the cursor is on, so which rows survive is the emulator's call;
        // that the screen the wait woke with is no longer the old one is the contract here.
        assert!(
            screen.rows != before.rows && screen.rows.len() <= 10,
            "the woken screen is the reflowed one: {:?} then {:?}",
            before.rows,
            screen.rows
        );
        assert_eq!(task.size(), (60, 10), "the task reports the new size");
        task.kill().unwrap();
        tasks.shutdown();
    }

    #[test]
    fn two_contexts_do_not_share_a_bash_result() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (_runtime, tasks) = tasks();
        let bad = Arc::new(LaunchContext::new(
            std::env::temp_dir(),
            vec![("FASTEXEC_BASH".into(), "/nonexistent/bash".into())],
        ));
        let Err(refused) = tasks.start(StartArgs {
            launch: Arc::clone(&bad),
            ..start_args()
        }) else {
            panic!("a task started with an invalid bash override");
        };
        assert!(refused.contains("FASTEXEC_BASH"), "{refused}");
        // A context that finds bash still starts a task, and its start got its place back.
        let task = tasks.start(start_args()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while task.is_running() || !task.output_closed() {
            assert!(Instant::now() < deadline, "the task never finished");
            std::thread::sleep(Duration::from_millis(10));
        }
        tasks.shutdown();
    }

    #[test]
    fn two_sessions_read_one_task_independently() {
        let _one_at_a_time = lock(&ONE_TASKS);
        let (_runtime, tasks) = tasks();
        let task = tasks
            .start(StartArgs {
                command: "printf 'a\\nb\\n'".into(),
                ..start_args()
            })
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while task.is_running() || !task.output_closed() {
            assert!(Instant::now() < deadline, "the task never finished");
            std::thread::sleep(Duration::from_millis(10));
        }
        let (first, second) = (session("a-1"), session("b-2"));
        let read = |session: &Arc<Session>| -> String {
            let view = session.view(&task);
            let mut state = view.read.try_lock().expect("the read gate is free");
            let window = task
                .read_window(
                    &mut state,
                    Truncate::Tail,
                    1024,
                    false,
                    encoding_rs::UTF_8,
                    None,
                )
                .unwrap();
            window.fit(1024).0
        };
        let (from_first, from_second) = (read(&first), read(&second));
        assert!(
            from_first.contains('a') && from_first.contains('b'),
            "{from_first:?}"
        );
        assert_eq!(
            from_second, from_first,
            "each session reads the whole output"
        );
        assert_eq!(
            read(&first),
            "",
            "a second read of one session sees nothing new"
        );
        tasks.shutdown();
    }
}
