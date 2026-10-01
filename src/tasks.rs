//! Task registry: spawn, log capture, unseen-output cursors, input queues, and shutdown.

use crate::output::{Cleaner, Truncate, Window, WindowBuilder, WindowSpec, incomplete_utf8_suffix};
use crate::process::{self, Launch, Tree};
use encoding_rs::Encoding;
use std::fs::File;
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
/// How long output may stay open after the root exits and its tree is killed.
const DRAIN_CAP: Duration = Duration::from_secs(2);

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
    next_id: u64,
    tasks: Vec<Arc<Task>>,
}

pub struct Task {
    pub id: String,
    pub command: String,
    pub pty: bool,
    pub log_path: PathBuf,
    pub encoding: &'static Encoding,
    started: Instant,
    tree: Tree,
    kill_requested: AtomicBool,
    out: Mutex<Output>,
    view: Mutex<View>,
    end: Mutex<Option<Final>>,
    done: watch::Sender<bool>,
    input: Mutex<Option<mpsc::Sender<Input>>>,
    queued: AtomicUsize,
}

#[derive(Default)]
struct Output {
    written: u64,
    /// End of the readable log: excludes an unfinished UTF-8 sequence while output continues.
    readable: u64,
    lines: u64,
    dropped: u64,
    evicted: bool,
}

#[derive(Default)]
struct View {
    cursor: u64,
    lines_before: u64,
    cleaner: Cleaner,
}

#[derive(Clone, Copy)]
struct Final {
    exit_code: i32,
    killed: bool,
    elapsed: Duration,
    output_open: bool,
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
}

pub struct StartArgs {
    pub command: String,
    pub cwd: PathBuf,
    pub pty: bool,
    pub login: bool,
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
            .filter(|task| task.is_running())
            .count();
        if running >= MAX_RUNNING {
            return Err(format!(
                "TOO_MANY_TASKS: {MAX_RUNNING} tasks are running. Wait for one to finish or kill one, then retry."
            ));
        }
        registry.next_id += 1;
        let id = format!("t{}", registry.next_id);
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
        let task = Arc::new(Task {
            id,
            command: args.command,
            pty: args.pty,
            log_path,
            encoding: args.encoding,
            started: Instant::now(),
            tree: spawned.tree,
            kill_requested: AtomicBool::new(false),
            out: Mutex::default(),
            view: Mutex::default(),
            end: Mutex::new(None),
            done: watch::Sender::new(false),
            input: Mutex::new(Some(input_tx.clone())),
            queued: AtomicUsize::new(0),
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
            let exit_code = child.wait();
            // A task is its whole tree: whatever the root leaves behind ends with it.
            waiter_task.tree.kill();
            // Closing a PTY lets its output reach EOF; ConPTY's close can block, so it runs apart.
            std::thread::spawn(move || drop(child));
            let output_open = eof_rx.recv_timeout(DRAIN_CAP).is_err();
            *lock(&waiter_task.input) = None;
            *lock(&waiter_task.end) = Some(Final {
                exit_code,
                killed: waiter_task.kill_requested.load(Ordering::SeqCst),
                elapsed: waiter_task.started.elapsed(),
                output_open,
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
        let mut finished: Vec<(Instant, String)> = registry
            .tasks
            .iter()
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
            self.log_bytes.fetch_sub(out.written, Ordering::SeqCst);
            let _ = std::fs::remove_file(&task.log_path);
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

    /// Kills every running tree and removes the log directory.
    pub fn shutdown(&self) {
        for task in lock(&self.registry).tasks.iter() {
            if task.is_running() {
                task.kill_requested.store(true, Ordering::SeqCst);
                task.tree.kill();
            }
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// Drains one task's output into its log until EOF, never blocking the child on log limits.
///
/// For a PTY, `answer` replies to cursor-position queries (`ESC[6n`): ConPTY sends one at
/// startup and holds all output until a terminal answers.
fn capture(
    shared: &Tasks,
    task: &Task,
    mut output: Box<dyn Read + Send>,
    mut log: File,
    answer: Option<mpsc::Sender<Input>>,
) {
    const QUERY: &[u8] = b"[6n";
    const REPLY: &[u8] = b"[1;1R";
    let mut buffer = vec![0_u8; 64 * 1024];
    let mut last = Vec::with_capacity(8);
    let mut seam = Vec::new();
    loop {
        let read = match output.read(&mut buffer) {
            Ok(0) | Err(_) => break, // a closed PTY reports EIO on Unix
            Ok(read) => read,
        };
        let chunk = &buffer[..read];
        if let Some(answer) = &answer {
            // `seam` keeps the previous chunk's last bytes so a query split across reads is found.
            seam.extend_from_slice(chunk);
            for _ in seam.windows(QUERY.len()).filter(|window| *window == QUERY) {
                // Replies share the input limit; past it they are dropped, never queued unbounded.
                if task.admit(REPLY.len()) && answer.send(Input::Data(REPLY.to_vec())).is_err() {
                    task.queued.fetch_sub(REPLY.len(), Ordering::SeqCst);
                }
            }
            seam.drain(..seam.len().saturating_sub(QUERY.len() - 1));
        }
        let size = read as u64;
        let fits = {
            let out = lock(&task.out);
            !out.evicted && out.written + size <= TASK_LOG_LIMIT
        };
        let stored = fits && shared.reserve(size) && {
            let written = log.write_all(chunk).is_ok();
            if !written {
                shared.log_bytes.fetch_sub(size, Ordering::SeqCst);
            }
            written
        };
        let mut out = lock(&task.out);
        if stored {
            last.extend_from_slice(chunk);
            last.drain(..last.len().saturating_sub(3));
            out.written += size;
            out.lines += chunk.iter().filter(|&&byte| byte == b'\n').count() as u64;
            out.readable = out.written - incomplete_utf8_suffix(&last) as u64;
        } else {
            out.dropped += size;
        }
    }
    let mut out = lock(&task.out);
    out.readable = out.written;
}

impl Task {
    pub fn is_running(&self) -> bool {
        lock(&self.end).is_none()
    }

    fn ended_at(&self) -> Option<Instant> {
        lock(&self.end).map(|end| self.started + end.elapsed)
    }

    pub fn done(&self) -> watch::Receiver<bool> {
        self.done.subscribe()
    }

    pub fn snapshot(&self) -> Snapshot {
        let end = *lock(&self.end);
        let out = lock(&self.out);
        Snapshot {
            state: match end {
                None => "running",
                Some(end) if end.killed => "killed",
                Some(_) => "exited",
            },
            exit_code: end.map(|end| end.exit_code),
            elapsed: end.map_or_else(|| self.started.elapsed(), |end| end.elapsed),
            lines: out.lines,
            dropped: out.dropped,
            evicted: out.evicted,
            output_open: end.is_some_and(|end| end.output_open),
        }
    }

    /// Reserves room in the input queue for `size` bytes.
    fn admit(&self, size: usize) -> bool {
        if self.queued.fetch_add(size, Ordering::SeqCst) + size > INPUT_QUEUE_LIMIT {
            self.queued.fetch_sub(size, Ordering::SeqCst);
            return false;
        }
        true
    }

    /// Queues `input`, then closes stdin when `eof` is set. Validation happens before any write.
    pub fn send(&self, input: Option<&str>, eof: bool) -> Result<(), String> {
        let data = input.filter(|text| !text.is_empty());
        if eof && self.pty {
            return Err("EOF_UNSUPPORTED_IN_PTY: a PTY has no stdin half-close. Send \\u0004 (Ctrl-D) as input, or kill the task.".into());
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
            if tx.send(Input::Data(text.as_bytes().to_vec())).is_err() {
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

    /// Marks the task killed and terminates its tree; the waiter thread records the exit.
    pub fn kill(&self) {
        if self.is_running() {
            self.kill_requested.store(true, Ordering::SeqCst);
            self.tree.kill();
        }
    }

    /// Returns the output after the cursor, as a window, and advances the cursor past it.
    pub fn read_window(
        &self,
        truncate: Truncate,
        budget: usize,
        raw: bool,
        encoding: &'static Encoding,
    ) -> std::io::Result<Window> {
        let mut view = lock(&self.view);
        let (end, evicted) = {
            let out = lock(&self.out);
            (out.readable, out.evicted)
        };
        let first_line = view.lines_before + 1;
        let start = view.cursor;
        let spec = WindowSpec {
            truncate,
            budget,
            raw,
            encoding,
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
