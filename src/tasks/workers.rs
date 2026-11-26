//! Background output, input, wait, and process-tree cleanup workers.

use super::Tasks;
use super::state::{
    DRAIN_CAP, Final, Input, TASK_LOG_LIMIT, Task, WriterDone, WriterDoneGuard, answer_program,
    lock,
};
use crate::platform::members::Member;
use crate::platform::process;
use crate::term::output::incomplete_utf8_suffix;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

/// Output that pauses this long has finished a frame: the chunks of one redraw arrive within
/// a millisecond or two of each other, frames 8 ms or more apart.
const FRAME_PAUSE: Duration = Duration::from_millis(4);
/// A pause this long showed its frame long enough to read, even in the middle of a redraw.
const LONG_PAUSE: Duration = Duration::from_millis(100);
/// Kept processes may hold the output open for good, so with them the drain also ends once the
/// output pauses this long.
const KEPT_PAUSE: Duration = Duration::from_millis(100);

/// Workers are reserved before a process is created. A failed thread allocation drops the
/// remaining job senders, so the workers already created exit without owning a task.
pub(super) struct OutputWorkers {
    pub(super) reader: mpsc::Sender<ReadWork>,
    pub(super) capture: mpsc::Sender<CaptureWork>,
}

pub(super) struct Workers {
    pub(super) output: OutputWorkers,
    pub(super) input: Option<mpsc::Sender<InputWork>>,
    pub(super) waiter: mpsc::Sender<WaitWork>,
    pub(super) ender: mpsc::Sender<EndWork>,
}

pub(super) type OutputChunk = std::io::Result<(Instant, Vec<u8>)>;

pub(super) struct ReadWork {
    pub(super) output: Box<dyn Read + Send>,
    pub(super) chunks: mpsc::SyncSender<OutputChunk>,
    pub(super) pty: bool,
}

pub(super) struct CaptureWork {
    pub(super) shared: Arc<Tasks>,
    pub(super) task: Arc<Task>,
    pub(super) received: mpsc::Receiver<OutputChunk>,
    pub(super) log: File,
    pub(super) answer: Option<mpsc::Sender<Input>>,
    pub(super) eof: mpsc::Sender<()>,
}

pub(super) struct InputWork {
    pub(super) task: Arc<Task>,
    pub(super) input: Box<dyn Write + Send>,
    pub(super) received: mpsc::Receiver<Input>,
    pub(super) done_signal: Arc<WriterDone>,
}

fn is_broken_pipe(error: &std::io::Error) -> bool {
    #[cfg(windows)]
    if matches!(error.raw_os_error(), Some(109 | 232 | 233)) {
        return true;
    }
    error.kind() == std::io::ErrorKind::BrokenPipe
}

pub(super) struct WaitWork {
    pub(super) shared: Arc<Tasks>,
    pub(super) task: Arc<Task>,
    pub(super) child: process::Child,
    pub(super) script: PathBuf,
    pub(super) exit_status: Option<PathBuf>,
    pub(super) eof: mpsc::Receiver<()>,
    pub(super) ender: mpsc::Sender<EndWork>,
}

pub(super) struct EndWork {
    pub(super) shared: Arc<Tasks>,
    pub(super) task: Arc<Task>,
    pub(super) child: process::Child,
    pub(super) script: PathBuf,
    pub(super) exit_status: Option<PathBuf>,
    pub(super) confirmed: Option<mpsc::Sender<()>>,
}

fn worker<T: Send + 'static>(
    name: &str,
    work: impl FnOnce(T) + Send + 'static,
) -> std::io::Result<mpsc::Sender<T>> {
    let (send, received) = mpsc::channel();
    std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || {
            if let Ok(job) = received.recv() {
                work(job);
            }
        })?;
    Ok(send)
}

impl OutputWorkers {
    pub(super) fn new() -> std::io::Result<Self> {
        Ok(Self {
            reader: worker("fastexec-output", |job: ReadWork| {
                read_chunks(job.output, job.chunks, job.pty);
            })?,
            capture: worker("fastexec-capture", |job: CaptureWork| {
                capture(&job.shared, &job.task, job.received, job.log, job.answer);
                let _ = job.eof.send(());
            })?,
        })
    }
}

impl Workers {
    pub(super) fn new(with_input: bool) -> std::io::Result<Self> {
        let output = OutputWorkers::new()?;
        let input = if with_input {
            Some(worker("fastexec-input", |job: InputWork| {
                let _guard = WriterDoneGuard(job.done_signal);
                let mut input = Some(job.input);
                for message in job.received {
                    let total = message.bytes.len();
                    let mut written = 0usize;
                    let write_res: std::io::Result<()> = if total == 0 {
                        Ok(())
                    } else {
                        match input.as_mut() {
                            None => Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
                            Some(writer) => (|| {
                                while written < total {
                                    match writer.write(&message.bytes[written..]) {
                                        Ok(0) => {
                                            return Err(std::io::Error::new(
                                                std::io::ErrorKind::WriteZero,
                                                "write returned 0 bytes",
                                            ));
                                        }
                                        Ok(count) => written += count,
                                        Err(err)
                                            if err.kind() == std::io::ErrorKind::Interrupted =>
                                        {
                                            continue;
                                        }
                                        Err(err) => return Err(err),
                                    }
                                }
                                loop {
                                    match writer.flush() {
                                        Ok(()) => return Ok(()),
                                        Err(err)
                                            if err.kind() == std::io::ErrorKind::Interrupted =>
                                        {
                                            continue;
                                        }
                                        Err(err) => return Err(err),
                                    }
                                }
                            })(),
                        }
                    };
                    job.task.queued.fetch_sub(total, Ordering::SeqCst);
                    let delivered = match write_res {
                        Ok(()) => Ok(()),
                        Err(error) if message.initial && is_broken_pipe(&error) => {
                            *lock(&job.task.input_warning) =
                                Some(format!("stdin closed after {written} of {total} bytes"));
                            *lock(&job.task.input) = None;
                            drop(input.take());
                            Ok(())
                        }
                        Err(_) if !message.initial && message.done.is_none() => {
                            *lock(&job.task.input) = None;
                            drop(input.take());
                            Ok(())
                        }
                        Err(error) => {
                            let msg = format!(
                                "INPUT_WRITE_FAILED: stdin is closed or unwritable: {error}; the input may have been partially delivered."
                            );
                            *lock(&job.task.input_error) = Some(msg.clone());
                            *lock(&job.task.input) = None;
                            drop(input.take());
                            Err(msg)
                        }
                    };
                    if message.eof {
                        drop(input.take());
                    }
                    if let Some(done) = message.done {
                        let _ = done.send(delivered);
                    }
                }
            })?)
        } else {
            None
        };
        Ok(Self {
            output,
            input,
            waiter: worker("fastexec-wait", wait_task)?,
            ender: worker("fastexec-end", |job: EndWork| {
                finish_tree(
                    &job.shared,
                    &job.task,
                    job.child,
                    &job.script,
                    job.exit_status.as_deref(),
                );
                if let Some(confirmed) = job.confirmed {
                    let _ = confirmed.send(());
                }
            })?,
        })
    }
}

/// Decide how to handle the root's last observed members. A successful requested cleanup
/// still takes the normal output-drain path when the initial observation was unavailable.
pub(super) fn root_leftovers(
    preserve: bool,
    listed: Option<Vec<Member>>,
    kill: impl FnOnce() -> std::io::Result<()>,
) -> (Vec<Member>, bool, Option<std::io::Error>) {
    let unknown = listed.is_none();
    let leftovers = listed.unwrap_or_default();
    if preserve {
        let kept = unknown || !leftovers.is_empty();
        (leftovers, kept, None)
    } else {
        let error = kill().err();
        (leftovers, error.is_some(), error)
    }
}

/// Waits for the root and publishes its result. The separately prepared ender owns the rest
/// of the tree and closes the PTY in parallel: a slow OS wait or ConPTY close must not prevent
/// the root result from being published after the bounded output drain.
fn wait_task(job: WaitWork) {
    let WaitWork {
        shared,
        task,
        mut child,
        script,
        exit_status,
        eof,
        ender,
    } = job;
    let (exit_code, forced_end) = child.wait(task.root.process());
    let killed = task.kill_requested.load(Ordering::SeqCst);
    let expired = task.expired.load(Ordering::SeqCst);
    let listed = if killed {
        Some(Vec::new())
    } else {
        task.root.members()
    };
    let preserve = task.keep && !killed && !expired;
    let (mut leftovers, mut kept, kill_error) =
        root_leftovers(preserve, listed, || task.root.kill());
    if let Some(error) = kill_error {
        leftovers = task.root.members().unwrap_or(leftovers);
        *lock(&task.kill_error) = Some(error.to_string());
    }
    if !kept && !preserve {
        *lock(&task.input) = None;
    }
    // A process listing can miss a fork while the parent disappears. Keep the input and
    // lifetime alive while the ender independently waits for the operating system's proof.
    let (confirmed, completion) = (preserve && leftovers.is_empty())
        .then(mpsc::channel)
        .unzip();
    ender
        .send(EndWork {
            confirmed,
            shared: Arc::clone(&shared),
            task: Arc::clone(&task),
            child,
            script,
            exit_status,
        })
        .unwrap_or_else(|_| unreachable!("prepared ender stopped"));
    let drain_deadline = Instant::now() + DRAIN_CAP;
    let output_open = drain_until(&task, &eof, kept, drain_deadline);
    if let Some(completion) = completion {
        let _ = completion.recv_timeout(drain_deadline.saturating_duration_since(Instant::now()));
        kept = !*task.tree_done.borrow();
    }
    if !kept {
        task.writer_done.wait_until(drain_deadline);
    }
    let mut registry = lock(&shared.registry);
    task.publish_end(Final {
        exit_code: Some(exit_code),
        killed,
        expired,
        forced_end,
        elapsed: task.started.elapsed(),
        output_open,
        leftovers,
        kept,
    });
    let retired = shared.retire_finished(&mut registry);
    shared.emit_task_exited(&task);
    drop(registry);
    for evicted in &retired {
        shared.evict_log(evicted);
    }
    task.done.send_replace(true);
}

fn finish_tree(
    shared: &Tasks,
    task: &Task,
    mut child: process::Child,
    script: &Path,
    exit_status: Option<&Path>,
) {
    let tree_ended = child.wait_tree(task.root.process());
    if !tree_ended {
        drop(child);
    } else {
        *lock(&task.input) = None;
        *lock(&task.pty_control) = None;
        // Drop child before waiting on writer_done: on Windows ConPTY, Child::Pty holds an
        // Arc<Terminal>, and ClosePseudoConsole only closes the output pipe (releasing
        // capture's answer sender) once every Arc<Terminal> is dropped.
        drop(child);
        task.writer_done.wait_until(Instant::now() + DRAIN_CAP);
        let _ = std::fs::remove_file(script);
        if let Some(status) = exit_status {
            let _ = std::fs::remove_file(status);
        }
    }
    let mut registry = lock(&shared.registry);
    if tree_ended {
        let _ = task.tree_ended_at.set(Instant::now());
        task.tree_done.send_replace(true);
    } else {
        task.hold_unconfirmed_tree();
    }
    let retired = shared.retire_finished(&mut registry);
    if tree_ended && lock(&task.end).as_ref().is_some_and(|end| end.kept) {
        shared.emit_task_exited(task);
    }
    drop(registry);
    for evicted in &retired {
        shared.evict_log(evicted);
    }
}

/// Waits for the output to reach EOF, at most [`DRAIN_CAP`], and returns whether it stayed
/// open. With kept processes, which may hold it open for good, the wait also ends once the
/// output pauses for [`KEPT_PAUSE`].
pub(super) fn drain(task: &Task, eof: &mpsc::Receiver<()>, kept: bool) -> bool {
    drain_until(task, eof, kept, Instant::now() + DRAIN_CAP)
}

fn drain_until(task: &Task, eof: &mpsc::Receiver<()>, kept: bool, deadline: Instant) -> bool {
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

/// Reads `output` until EOF and passes it on in chunks, each with the time it was read.
pub(super) fn read_chunks(
    mut output: Box<dyn Read + Send>,
    chunks: mpsc::SyncSender<OutputChunk>,
    pty: bool,
) {
    #[cfg(not(unix))]
    let _ = pty;
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        match output.read(&mut buffer) {
            Ok(0) => break,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            #[cfg(unix)]
            Err(error) if pty && error.raw_os_error() == Some(libc::EIO) => break,
            Err(error) => {
                let _ = chunks.send(Err(error));
                break;
            }
            Ok(read) => {
                if chunks
                    .send(Ok((Instant::now(), buffer[..read].to_vec())))
                    .is_err()
                {
                    break;
                }
            }
        }
    }
}

/// Drains one task's output into its log until EOF, never blocking the child on log limits.
///
/// For a PTY, the output also feeds the task's terminal emulator, and `answer` carries its
/// replies to terminal queries.
pub(super) fn capture(
    shared: &Tasks,
    task: &Task,
    received: mpsc::Receiver<OutputChunk>,
    log: File,
    answer: Option<mpsc::Sender<Input>>,
) {
    // The prepared output worker reads independently, so synchronized updates still end
    // at their deadline when the program writes nothing.
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
        let Some(received) = received else {
            break;
        };
        let (arrived, chunk) = match received {
            Ok(chunk) => chunk,
            Err(error) => {
                lock(&task.out).capture_error = Some(error.to_string());
                break;
            }
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
        let (room, prefix) = {
            let out = lock(&task.out);
            let room = if out.evicted || out.dropped > 0 {
                0
            } else {
                TASK_LOG_LIMIT - out.written
            };
            (room, out.written)
        };
        let keep = &chunk[..chunk.len().min(room as usize)];
        let size = keep.len() as u64;
        let mut stored = false;
        let mut retained_tail = false;
        let mut write_error = None;
        if size > 0
            && let Some(file) = log.as_mut()
            && shared.reserve(size)
        {
            match shared.write_log(file, keep) {
                Ok(()) => stored = true,
                Err(error) => {
                    if shared.truncate_log(file, prefix).is_ok() {
                        shared.log_bytes.fetch_sub(size, Ordering::SeqCst);
                    } else {
                        retained_tail = true;
                    }
                    log = None;
                    write_error = Some(error.to_string());
                }
            }
        }
        let mut out = lock(&task.out);
        if write_error.is_some() {
            out.log_error = write_error;
        }
        // An eviction since the room check may not have known about these bytes yet; drop the
        // handle and reclaim the file after recording dropped bytes and output activity.
        let reclaim_evicted = (stored || retained_tail) && out.evicted;
        if reclaim_evicted {
            stored = false;
            log = None;
        } else if retained_tail {
            out.log_tail += size;
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
        if reclaim_evicted {
            shared.reclaim_file(task.log_path.clone(), size);
        }
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
