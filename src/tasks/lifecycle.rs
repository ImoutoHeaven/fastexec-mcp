//! Task admission, process/script spawning, deadline arming, and server shutdown.

use super::quota::{lock, private_file};
use super::script::{QueuedStartGuard, ScriptControl};
use super::session::{Reader, Session, TaskEntry};
use super::state::{Final, Input, Pty, Root, Spool, Task, TaskError, WriterDone};
use super::store::{Registry, Tasks};
use super::workers::{CaptureWork, InputWork, OutputWorkers, ReadWork, WaitWork, Workers, drain};
use crate::env::{LaunchContext, ProbeWarning};
use crate::platform::process::{self, Launch};
use crate::term::terminal::Live;
use encoding_rs::Encoding;
use std::io::{PipeWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::time::{Duration, Instant};
use tokio::sync::watch;

/// Runs before a Windows ConPTY root's command, on its first line, so the script's exit status is
/// on disk as the script ends. The root can deadlock inside its own exit; this file is what lets
/// its task end anyway. A user's own EXIT cleanup is never overwritten, and a command that
/// replaces the trap or execs keeps the plain wait.
const EXIT_TRAP: &str = r#"[ -n "${FASTEXEC_EXIT_STATUS-}" ] && [ "${BASH_SUBSHELL-0}" -eq 0 ] && builtin export -n FASTEXEC_EXIT_STATUS && [ -z "$(builtin trap -p EXIT)" ] && builtin trap 'builtin printf "%s\n" "$?" 2>/dev/null > "$FASTEXEC_EXIT_STATUS" || :' EXIT || :; "#;

/// A start in flight: it holds a place against [`super::quota::Limits::max_running`] until it is
/// inserted or fails.
pub(super) struct Pending<'a> {
    registry: &'a Mutex<Registry>,
    counted: bool,
    script: bool,
}

impl Pending<'_> {
    /// Gives the place back inside the caller's critical section: its own drop must never
    /// re-lock a registry mutex the caller still holds.
    fn release(&mut self, registry: &mut Registry) {
        if self.counted {
            registry.pending -= 1;
            if self.script {
                registry.pending_scripts -= 1;
            }
            self.counted = false;
        }
    }
}

impl Drop for Pending<'_> {
    fn drop(&mut self) {
        if self.counted {
            let mut registry = lock(self.registry);
            self.release(&mut registry);
        }
    }
}

pub type Started = (Arc<Task>, Arc<TaskEntry>, Arc<Reader>);

pub struct StartArgs {
    pub command: String,
    pub cwd: PathBuf,
    pub pty: bool,
    /// The terminal's size: columns, then screen rows. A pipe task ignores them.
    pub cols: u16,
    pub rows: u16,
    pub login: bool,
    pub keep: bool,
    pub input: Option<String>,
    pub eof: Option<bool>,
    pub encoding: &'static Encoding,
    /// `killAfterMs`: end the task's tree this many ms after launch; 0 or None means no limit.
    pub kill_after_ms: Option<u64>,
    /// The session that starts the task, shown as its owner.
    pub owner: String,
    /// The session ID and daemon call that started the task, shown by `list`; `None` for a
    /// stdio call, which has neither.
    pub started_by: Option<(String, u64)>,
    /// Parent codemode task ID when started via nested `exec start`.
    pub parent_task_id: Option<String>,
    /// Releases the parent script's `queued_starts` counter once the task enters `registry.tasks`.
    pub queued_guard: Option<QueuedStartGuard>,
    /// What the task inherits: its environment, and the bash found in it.
    pub launch: Arc<LaunchContext>,
    pub input_owner: crate::lease::Owner,
}

/// Holds an admitted task and script slot until [`Tasks::publish_script`] inserts the task.
pub(crate) struct ReservedScript {
    tasks: Arc<Tasks>,
    id: String,
    committed: bool,
}

impl ReservedScript {
    fn commit(&mut self, registry: &mut Registry) {
        if !self.committed {
            registry.pending -= 1;
            registry.pending_scripts -= 1;
            self.committed = true;
        }
    }
}

impl Drop for ReservedScript {
    fn drop(&mut self) {
        if !self.committed {
            self.committed = true;
            let mut registry = lock(&self.tasks.registry);
            registry.pending -= 1;
            registry.pending_scripts -= 1;
        }
    }
}

pub(crate) struct ScriptPublishArgs {
    pub command: String,
    pub kill_after_ms: Option<u64>,
    pub call_id: Option<u64>,
    pub script: Arc<ScriptControl>,
    pub key: String,
}

pub(crate) struct ScriptIo {
    pub task: Arc<Task>,
    pub tasks: Arc<Tasks>,
    pub writer: PipeWriter,
    pub captured: mpsc::Receiver<()>,
}

impl Tasks {
    /// Takes a place against [`super::quota::Limits::max_running`] and the next task ID. The guard
    /// gives the place back on drop, so every failed start releases it.
    pub(super) fn admit(&self, script: bool) -> Result<(String, Pending<'_>), TaskError> {
        let mut registry = lock(&self.registry);
        if registry.stopping {
            return Err("The server is shutting down; the task was not started.".into());
        }
        let running = registry
            .tasks
            .iter()
            .filter(|task| !task.is_finished())
            .count();
        let limit = self.limits.max_running;
        if running + registry.pending >= limit {
            return Err(TaskError::Overload(format!(
                "TOO_MANY_TASKS: {limit} tasks are running or keep processes. Wait for one to finish or kill one, then retry."
            )));
        }
        if script {
            let running = registry
                .tasks
                .iter()
                .filter(|task| task.is_script() && task.is_running())
                .count();
            let limit = self.limits.max_scripts;
            if running + registry.pending_scripts >= limit {
                return Err(TaskError::Overload(format!(
                    "TOO_MANY_SCRIPTS: {limit} codemode tasks are running. Wait for one to finish or kill one, then retry."
                )));
            }
            registry.pending_scripts += 1;
        }
        registry.pending += 1;
        Ok((
            self.next_id(),
            Pending {
                registry: &self.registry,
                counted: true,
                script,
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
        let Some(deadline) = tokio::time::Instant::now().checked_add(Duration::from_millis(limit))
        else {
            return;
        };
        let (expiring, mut done) = (Arc::clone(task), task.tree_done());
        self.handle.spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => expiring.expire(),
                _ = done.wait_for(|done| *done) => {}
            }
        });
    }

    pub fn start(
        self: &Arc<Self>,
        args: StartArgs,
        session: &Session,
        key: String,
    ) -> Result<(Started, Option<ProbeWarning>), TaskError> {
        let mut queued_guard = args.queued_guard;
        let kill_after_ms = args.kill_after_ms;
        // Admission holds a place against the running limit, so the bash lookup, the file
        // creation, and the spawn below all run without the registry lock.
        let (pipe_stdin, initial_input, keep_stdin_open) = if args.pty {
            (true, None, true)
        } else {
            let close_after = args.eof != Some(false);
            match args.input {
                None if close_after => (false, None, false),
                None => (true, None, true),
                Some(text) if text.is_empty() && close_after => (true, None, false),
                Some(text) if text.is_empty() => (true, None, true),
                Some(text) => (true, Some((text.into_bytes(), close_after)), !close_after),
            }
        };
        let need_input_worker = args.pty || keep_stdin_open || initial_input.is_some();
        let (id, mut pending) = self.admit(false)?;
        let bash = crate::platform::bash::resolve(&args.launch)?;
        let (base_env, _used_snapshot, probe_warning) =
            process::task_env(&args.launch, &bash, args.login);
        let workers = Workers::new(need_input_worker)
            .map_err(|error| format!("Cannot prepare the task workers: {error}."))?;
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
            if let Err(error) =
                private_file(&script_path).and_then(|mut file| file.write_all(command.as_bytes()))
            {
                let _ = std::fs::remove_file(&log_path);
                let _ = std::fs::remove_file(&script_path);
                return Err(TaskError::Io(format!(
                    "Cannot write the command script: {error}."
                )));
            }
            Some(script_path.as_path())
        } else {
            None
        };
        let launch = Launch {
            bash: &bash,
            command: &command,
            script,
            cwd: &args.cwd,
            base_env,
            #[cfg(windows)]
            used_snapshot: _used_snapshot,
            login: args.login,
            pty: args.pty,
            pipe_stdin,
            cols: args.cols,
            rows: args.rows,
            exit_status: exit_status.as_deref(),
        };
        let mut spawned = match process::spawn(&launch) {
            Ok(spawned) => spawned,
            Err(error) => {
                let _ = std::fs::remove_file(&log_path);
                let _ = std::fs::remove_file(&script_path);
                return Err(TaskError::Io(format!("Cannot start the command: {error}.")));
            }
        };
        if !need_input_worker {
            // Explicit empty pipe (`input: ""`) closes its write end immediately here.
            drop(spawned.input.take());
        }
        let (input_tx, input_rx) = mpsc::channel();
        let mut initial_queued = 0usize;
        if let Some((bytes, eof)) = initial_input {
            initial_queued = bytes.len();
            input_tx
                .send(Input {
                    bytes,
                    eof,
                    initial: true,
                    done: None,
                    _input_owner: args.input_owner.clone(),
                })
                .expect("input_rx is held in this frame");
        }
        let writer_done = WriterDone::new(!need_input_worker);
        let mut initial_transcript_file = None;
        let terminal = args.pty.then(|| {
            let mut spool = Spool {
                bytes: 0,
                lines: 0,
                pending: 0,
                stopped: None,
            };
            if args.encoding == encoding_rs::UTF_8 {
                match private_file(&self.dir.join(format!("{id}.transcript.txt"))) {
                    Ok(file) => initial_transcript_file = Some(file),
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
            session_id: Some(session.id.clone()),
            started_by: args.started_by,
            parent_task_id: args.parent_task_id,
            started: Instant::now(),
            root: Root::Process(spawned.tree),
            tree_done: watch::Sender::new(false),
            tree_ended_at: OnceLock::new(),
            kill_error: Mutex::new(None),
            kill_requested: AtomicBool::new(false),
            expired: AtomicBool::new(false),
            out: Mutex::default(),
            end: Mutex::new(None),
            done: watch::Sender::new(false),
            activity: watch::Sender::new(()),
            input: Mutex::new(keep_stdin_open.then(|| input_tx.clone())),
            writer_done: Arc::clone(&writer_done),
            input_warning: Mutex::new(None),
            input_error: Mutex::new(None),
            queued: AtomicUsize::new(initial_queued),
            input_gate: tokio::sync::Mutex::new(()),
            terminal,
            transcript_file: Mutex::new(initial_transcript_file),
            size: Mutex::new((args.cols, args.rows)),
            pty_control: Mutex::new(spawned.pty),
            size_error: Mutex::new(None),
        });
        let mut registry = lock(&self.registry);
        let entry = session.entry(&task);
        let reader = entry.start_reader(key);
        registry.tasks.push(Arc::clone(&task));
        if let Some(guard) = queued_guard.as_mut() {
            guard.disarm();
        }
        pending.release(&mut registry);
        let stopping = registry.stopping;
        drop(registry);
        if stopping {
            // The server began stopping while this tree was created: it ends now, and it joins
            // the registry anyway so the capture, input, and waiter threads clean up as usual.
            task.kill_requested.store(true, Ordering::SeqCst);
            let _ = task.root.kill();
        }
        self.arm_deadline(&task, kill_after_ms);

        let (eof_tx, eof_rx) = mpsc::channel::<()>();
        let (chunks, received) = mpsc::sync_channel(1);
        // A prepared worker waits for exactly one job and cannot fail before receiving it.
        workers
            .output
            .reader
            .send(ReadWork {
                output: spawned.output,
                chunks,
                pty: args.pty,
            })
            .unwrap_or_else(|_| unreachable!("prepared output worker stopped"));
        workers
            .output
            .capture
            .send(CaptureWork {
                shared: Arc::clone(self),
                task: Arc::clone(&task),
                received,
                log,
                answer: args.pty.then(|| input_tx.clone()),
                eof: eof_tx,
            })
            .unwrap_or_else(|_| unreachable!("prepared capture worker stopped"));
        drop(input_tx);
        if let Some(input_worker) = workers.input {
            input_worker
                .send(InputWork {
                    task: Arc::clone(&task),
                    input: spawned.input.expect("input worker requires piped stdin"),
                    received: input_rx,
                    done_signal: writer_done,
                })
                .unwrap_or_else(|_| unreachable!("prepared input worker stopped"));
        }
        workers
            .waiter
            .send(WaitWork {
                shared: Arc::clone(self),
                task: Arc::clone(&task),
                child: spawned.child,
                script: script_path,
                exit_status,
                eof: eof_rx,
                ender: workers.ender,
            })
            .unwrap_or_else(|_| unreachable!("prepared waiter worker stopped"));
        Ok(((task, entry, reader), probe_warning))
    }

    pub(crate) fn reserve_script_slot(self: &Arc<Self>) -> Result<ReservedScript, TaskError> {
        let (id, mut pending) = self.admit(true)?;
        pending.counted = false;
        Ok(ReservedScript {
            tasks: Arc::clone(self),
            id,
            committed: false,
        })
    }

    pub(crate) fn publish_script<P>(
        self: &Arc<Self>,
        mut reserved: ReservedScript,
        args: ScriptPublishArgs,
        session: &Session,
        job_tx: mpsc::SyncSender<(ScriptIo, P)>,
        payload: P,
    ) -> Result<Started, TaskError> {
        debug_assert!(Arc::ptr_eq(self, &reserved.tasks));
        let output = OutputWorkers::new()
            .map_err(|error| format!("Cannot prepare the script workers: {error}."))?;
        let (pipe, writer) =
            std::io::pipe().map_err(|error| format!("Cannot create the script pipe: {error}."))?;
        let id = reserved.id.clone();
        let log_path = self.dir.join(format!("{id}.log"));
        let log = private_file(&log_path)
            .map_err(|error| format!("Cannot create the script log: {error}."))?;
        let task = Arc::new(Task {
            id,
            command: args.command,
            pty: false,
            log_path,
            encoding: encoding_rs::UTF_8,
            keep: false,
            owner: session.label.clone(),
            session_id: Some(session.id.clone()),
            started_by: args.call_id.map(|id| (session.id.clone(), id)),
            parent_task_id: None,
            started: Instant::now(),
            root: Root::Script(args.script),
            tree_done: watch::Sender::new(false),
            tree_ended_at: OnceLock::new(),
            kill_error: Mutex::new(None),
            kill_requested: AtomicBool::new(false),
            expired: AtomicBool::new(false),
            out: Mutex::default(),
            end: Mutex::new(None),
            done: watch::Sender::new(false),
            activity: watch::Sender::new(()),
            input: Mutex::new(None),
            writer_done: WriterDone::new(true),
            input_warning: Mutex::new(None),
            input_error: Mutex::new(None),
            queued: AtomicUsize::new(0),
            input_gate: tokio::sync::Mutex::new(()),
            terminal: None,
            transcript_file: Mutex::new(None),
            size: Mutex::new((0, 0)),
            pty_control: Mutex::new(None),
            size_error: Mutex::new(None),
        });
        let mut registry = lock(&self.registry);
        let entry = session.entry(&task);
        let reader = entry.start_reader(args.key);
        registry.tasks.push(Arc::clone(&task));
        reserved.commit(&mut registry);
        let stopping = registry.stopping;
        drop(registry);
        if stopping {
            let _ = task.kill();
        }
        self.arm_deadline(&task, args.kill_after_ms);
        let (eof, captured) = mpsc::channel();
        let (chunks, received) = mpsc::sync_channel(1);
        output
            .reader
            .send(ReadWork {
                output: Box::new(pipe),
                chunks,
                pty: false,
            })
            .unwrap_or_else(|_| unreachable!("prepared output worker stopped"));
        output
            .capture
            .send(CaptureWork {
                shared: Arc::clone(self),
                task: Arc::clone(&task),
                received,
                log,
                answer: None,
                eof,
            })
            .unwrap_or_else(|_| unreachable!("prepared capture worker stopped"));
        let io = ScriptIo {
            task: Arc::clone(&task),
            tasks: Arc::clone(self),
            writer,
            captured,
        };
        if let Err(mpsc::SendError((io, payload))) = job_tx.send((io, payload)) {
            let ScriptIo {
                writer, captured, ..
            } = io;
            drop(payload);
            drop(writer);
            self.finish_script(&task, Some(1), captured);
        }
        Ok((task, entry, reader))
    }

    pub(crate) fn finish_script(
        &self,
        task: &Task,
        exit_code: Option<i32>,
        captured: mpsc::Receiver<()>,
    ) {
        let output_open = drain(task, &captured, false);
        let mut registry = lock(&self.registry);
        let killed = task.kill_requested.load(Ordering::SeqCst);
        let _ = task.tree_ended_at.set(Instant::now());
        task.tree_done.send_replace(true);
        *lock(&task.end) = Some(Final {
            exit_code: if killed { None } else { exit_code },
            killed,
            expired: task.expired.load(Ordering::SeqCst),
            forced_end: false,
            elapsed: task.started.elapsed(),
            output_open,
            leftovers: Vec::new(),
            kept: false,
        });
        let retired = self.retire_finished(&mut registry);
        self.emit_task_exited(task);
        drop(registry);
        for evicted in &retired {
            self.evict_log(evicted);
        }
        task.done.send_replace(true);
    }

    /// Kills every tree, kept processes included, and removes the log directory.
    pub fn shutdown(&self) {
        let tasks = {
            let mut registry = lock(&self.registry);
            // An already admitted start still publishes, then ends its tree.
            registry.stopping = true;
            for task in &registry.tasks {
                if task.is_running() {
                    task.kill_requested.store(true, Ordering::SeqCst);
                }
            }
            registry.tasks.clone()
        };
        for task in tasks {
            let _ = task.root.kill();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}
