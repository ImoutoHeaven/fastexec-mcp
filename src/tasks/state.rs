//! Task runtime state, terminal snapshots, transcript spool, and attachment leases.

use super::workers::{WriterDone, answer_program};
use super::{
    EVICTED_TRANSCRIPT, INPUT_QUEUE_LIMIT, INPUT_WRITE_LIMIT, TASK_LOG_LIMIT, Tasks, lock,
};
use crate::codemode;
use crate::lease::{BytePool, Lease};
use crate::media::MediaKind;
use crate::platform::process::{self, Member, Tree};
use crate::session::ReadState;
use crate::term::output::{
    MATCH_SCAN_BYTES, Matcher, Truncate, Window, WindowBuilder, WindowSpec,
    incomplete_legacy_suffix_reader,
};
use crate::term::terminal::Live;
use encoding_rs::Encoding;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak, mpsc};
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, watch};
use tokio_util::sync::CancellationToken;

pub(super) enum Root {
    Process(Tree),
    Script(codemode::control::ScriptHandle),
}

impl Root {
    pub(super) fn kill(&self) -> std::io::Result<()> {
        match self {
            Self::Process(tree) => tree.kill(),
            Self::Script(script) => {
                script.kill();
                Ok(())
            }
        }
    }

    pub(super) fn members(&self) -> Option<Vec<Member>> {
        match self {
            Self::Process(tree) => tree.members(),
            Self::Script(_) => Some(Vec::new()),
        }
    }

    pub(super) fn process(&self) -> &Tree {
        match self {
            Self::Process(tree) => tree,
            Self::Script(_) => unreachable!("a script has no process tree"),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AttachmentMeta {
    pub index: usize,
    pub kind: MediaKind,
    pub mime: String,
    pub path: PathBuf,
    pub size: u64,
    pub base64_len: usize,
}

pub(super) struct AttachmentSlotState {
    pub(super) in_task: bool,
    pub(super) leases: usize,
    pub(super) reclaimed: bool,
}

pub(crate) struct AttachmentSlot {
    pub(super) tasks: Weak<Tasks>,
    pub meta: AttachmentMeta,
    pub(super) state: Mutex<AttachmentSlotState>,
}

impl AttachmentSlot {
    pub(super) fn try_lease(self: &Arc<Self>) -> Option<AttachmentLease> {
        let mut state = lock(&self.state);
        if state.reclaimed {
            return None;
        }
        state.leases += 1;
        drop(state);
        Some(AttachmentLease(Arc::clone(self)))
    }

    pub(super) fn detach_from_task(&self) {
        let reclaim = {
            let mut state = lock(&self.state);
            state.in_task = false;
            if state.leases == 0 && !state.reclaimed {
                state.reclaimed = true;
                true
            } else {
                false
            }
        };
        if reclaim && let Some(tasks) = self.tasks.upgrade() {
            tasks.reclaim_file(self.meta.path.clone(), self.meta.size);
        }
    }
}

/// Short-term read lease pinning an attachment file on disk across task eviction until dropped.
pub(crate) struct AttachmentLease(Arc<AttachmentSlot>);

impl AttachmentLease {
    pub fn meta(&self) -> &AttachmentMeta {
        &self.0.meta
    }
}

impl Clone for AttachmentLease {
    fn clone(&self) -> Self {
        let mut state = lock(&self.0.state);
        state.leases += 1;
        drop(state);
        Self(Arc::clone(&self.0))
    }
}

impl Drop for AttachmentLease {
    fn drop(&mut self) {
        let reclaim = {
            let mut state = lock(&self.0.state);
            state.leases = state.leases.saturating_sub(1);
            if !state.in_task && state.leases == 0 && !state.reclaimed {
                state.reclaimed = true;
                true
            } else {
                false
            }
        };
        if reclaim && let Some(tasks) = self.0.tasks.upgrade() {
            tasks.reclaim_file(self.0.meta.path.clone(), self.0.meta.size);
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
    /// The session that started the task: its client name and short session ID.
    pub owner: String,
    /// The session ID that started the task, used for `task.exited` scope filtering.
    pub session_id: Option<String>,
    /// The session ID and daemon call that started the task, when a daemon call started it.
    pub started_by: Option<(String, u64)>,
    /// Parent codemode task ID for nested `exec start` children; `None` for top-level tasks.
    pub parent_task_id: Option<String>,
    pub(super) started: Instant,
    pub(super) root: Root,
    /// Set once no process of the tree runs.
    pub(super) tree_done: watch::Sender<bool>,
    /// When the tree ended, set before `tree_done`.
    pub(super) tree_ended_at: OnceLock<Instant>,
    /// Why the last kill of the processes held past the root's exit failed.
    pub(super) kill_error: Mutex<Option<String>>,
    pub(super) kill_requested: AtomicBool,
    /// Set when `killAfterMs` elapsed and ended the task.
    pub(super) expired: AtomicBool,
    pub(super) out: Mutex<Output>,
    pub(super) end: Mutex<Option<Final>>,
    pub(super) done: watch::Sender<bool>,
    /// Signals each captured output chunk and each synchronized update its timeout ends.
    pub(super) activity: watch::Sender<()>,
    pub(super) input: Mutex<Option<mpsc::Sender<Input>>>,
    pub(super) writer_done: Arc<WriterDone>,
    pub(super) input_warning: Mutex<Option<String>>,
    pub(super) input_error: Mutex<Option<String>>,
    pub(super) queued: AtomicUsize,
    /// One poll's `input`, `keys`, and `eof` go out as one sequence: this gate holds the next
    /// sequence out, so a delayed key press cannot land in the middle of another write.
    pub input_gate: tokio::sync::Mutex<()>,
    /// PTY tasks: a terminal emulator fed with every output byte, and the transcript it
    /// records. Its replies to terminal queries, such as the cursor-position report and device
    /// attributes ConPTY waits for at startup, go to the program as input.
    pub(super) terminal: Option<Mutex<Pty>>,
    /// Open handle for `<id>.transcript.txt`, isolated from `terminal` so `evict_log` never locks `terminal`.
    pub(super) transcript_file: Mutex<Option<File>>,
    /// The terminal's size, columns first, as the program's terminal has it.
    pub(super) size: Mutex<(u16, u16)>,
    /// A PTY task's size control, taken when the tree ends so a resize that arrives then fails
    /// before the terminal closes. A resize holds it while it runs, so the ender's close waits
    /// for it and the console's last reference stays on the ender.
    pub(super) pty_control: Mutex<Option<process::PtyControl>>,
    /// Why the terminal's state is no longer trustworthy: a native resize failed, so the
    /// emulator and the program's terminal disagree. Screen and keys report it; raw output
    /// capture continues.
    pub(super) size_error: Mutex<Option<String>>,
}

pub(super) struct Pty {
    pub(super) live: Live,
    pub(super) spool: Spool,
}

fn terminal_memory_error(memory: &BytePool, task: &Task) -> String {
    format!(
        "Server active memory budget ({} bytes) cannot fit the terminal snapshot; the operation may have run (taskId: {}, logPath: {}). Do not retry start or resend input; poll taskId without input once in-flight transfers complete.",
        memory.limit(),
        task.id,
        crate::files::path::display_path(&task.log_path)
    )
}

/// A PTY task's transcript file: the final lines, those the terminal wrote out, then the
/// pending lines as the last `transcript` read them.
pub(super) struct Spool {
    /// Bytes and lines of the final lines.
    pub(super) bytes: u64,
    pub(super) lines: u64,
    /// Bytes of the pending lines after them.
    pub(super) pending: u64,
    /// Why the file takes no more lines.
    pub(super) stopped: Option<String>,
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
    /// Keeps snapshot buffers charged until the caller finishes consuming them.
    pub(crate) _lease: Lease,
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
    /// Keeps snapshot buffers charged until the caller finishes consuming them.
    pub(crate) _lease: Lease,
}

#[derive(Default)]
pub(super) struct Output {
    pub(super) written: u64,
    /// Reserved bytes past the confirmed prefix when a failed write could not be truncated.
    pub(super) log_tail: u64,
    /// End of the readable log: excludes an unfinished UTF-8 sequence while output continues.
    pub(super) readable: u64,
    pub(super) lines: u64,
    pub(super) dropped: u64,
    pub(super) evicted: bool,
    /// The first log write failure; capture stops storing output after it.
    pub(super) log_error: Option<String>,
    /// An unexpected output read failure: the captured prefix is incomplete.
    pub(super) capture_error: Option<String>,
    /// Capture ended, at EOF or with a recorded read error: captured bytes are final.
    pub(super) closed: bool,
    /// When the last output byte arrived, stored or not, or the end of a synchronized update by
    /// timeout or end of output showed the frame it held.
    pub(super) last_output: Option<Instant>,
    /// Transcript charge, including any failed write whose rollback was not confirmed.
    pub(super) transcript: u64,
    pub(super) next_attachment_index: usize,
    pub(super) pending_attachments: usize,
    pub(super) attachments: Vec<Arc<AttachmentSlot>>,
}

#[derive(Clone)]
pub(super) struct Final {
    pub(super) exit_code: Option<i32>,
    pub(super) killed: bool,
    pub(super) expired: bool,
    /// The script had finished but the root did not terminate, so the root was ended for it.
    pub(super) forced_end: bool,
    pub(super) elapsed: Duration,
    pub(super) output_open: bool,
    /// Processes the root left running when it exited.
    pub(super) leftovers: Vec<Member>,
    /// The task holds its tree past the root's exit: it keeps the leftovers, or killing them
    /// failed.
    pub(super) kept: bool,
}

impl Final {
    pub(super) fn clone_reserved(
        &self,
        reserve: &mut impl FnMut(usize) -> Result<(), String>,
    ) -> Result<Self, String> {
        for member in &self.leftovers {
            reserve(member.name.capacity().saturating_add(32))?;
        }
        Ok(self.clone())
    }
}

pub(super) struct Input {
    pub(super) bytes: Vec<u8>,
    pub(super) eof: bool,
    pub(super) initial: bool,
    pub(super) done: Option<oneshot::Sender<Result<(), String>>>,
    pub(super) _input_owner: crate::lease::Owner,
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
    pub capture_error: Option<String>,
    pub input_warning: Option<String>,
    pub input_error: Option<String>,
    /// Processes the root left running when it exited.
    pub leftovers: Vec<Member>,
    /// The task holds its tree past the root's exit, and some of it still runs.
    pub kept: bool,
    /// The processes the task held past the root's exit have ended.
    pub tree_ended: bool,
    /// The processes the task holds, when [`Task::snapshot_held_reserved`] took the snapshot.
    pub held: Vec<Member>,
    /// Why the last kill of the processes it holds failed.
    pub kill_error: Option<String>,
}

fn copy_snapshot_text(
    value: &Option<String>,
    reserve: &mut impl FnMut(usize) -> Result<(), String>,
) -> Result<Option<String>, String> {
    if let Some(text) = value {
        reserve(text.capacity().saturating_add(128))?;
    }
    Ok(value.clone())
}

impl Spool {
    /// Appends final lines, each ending in a newline, in place of the pending lines. Past the
    /// per-task limit, the file keeps the lines that fit and takes no more.
    pub(super) fn append(&mut self, text: &str, task: &Task, shared: &Tasks) {
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
    pub(super) fn write(
        &mut self,
        text: &[u8],
        as_final: bool,
        task: &Task,
        shared: &Tasks,
    ) -> bool {
        if lock(&task.out).evicted {
            *lock(&task.transcript_file) = None;
            return false;
        }
        if lock(&task.transcript_file).is_none() {
            return false;
        }
        if self.pending > 0 {
            let truncated = {
                let mut file_guard = lock(&task.transcript_file);
                let Some(file) = file_guard.as_mut() else {
                    self.pending = 0;
                    return false;
                };
                shared.truncate_log(file, self.bytes)
            };
            if let Err(error) = truncated {
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
            self.stopped = Some("the 4 GiB total log limit is reached".into());
            return false;
        }
        let write_result = {
            let mut file_guard = lock(&task.transcript_file);
            match file_guard.as_mut() {
                Some(file) => {
                    let result = file
                        .seek(SeekFrom::Start(self.bytes))
                        .and_then(|_| shared.write_log(file, text));
                    let rolled_back =
                        result.is_err() && shared.truncate_log(file, self.bytes).is_ok();
                    Some((result, rolled_back))
                }
                None => None,
            }
        };
        let Some((write_result, rolled_back)) = write_result else {
            shared.log_bytes.fetch_sub(size, Ordering::SeqCst);
            return false;
        };
        match write_result {
            Ok(()) => {
                if !shared.hold(task, size) {
                    let closed = lock(&task.transcript_file).take();
                    drop(closed);
                    shared.reclaim_file(task.transcript_path(), size);
                    return false;
                }
                if as_final {
                    self.bytes += size;
                    self.lines += text.iter().filter(|&&byte| byte == b'\n').count() as u64;
                } else {
                    self.pending = size;
                }
                true
            }
            Err(error) => {
                if rolled_back {
                    shared.log_bytes.fetch_sub(size, Ordering::SeqCst);
                } else if !shared.hold(task, size) {
                    let closed = lock(&task.transcript_file).take();
                    drop(closed);
                    shared.reclaim_file(task.transcript_path(), size);
                }
                // Unconfirmed bytes stay charged, but never become final or pending lines.
                // Stopping also prevents later transcript refreshes from overwriting this tail.
                self.stopped = Some(format!("writing it failed: {error}"));
                false
            }
        }
    }
}

impl Task {
    /// The ender may discover host loss before or after the root's result is published.
    pub(super) fn publish_end(&self, mut final_state: Final) {
        let mut end = lock(&self.end);
        final_state.kept |= lock(&self.kill_error).is_some();
        *end = Some(final_state);
    }

    pub(super) fn hold_unconfirmed_tree(&self) {
        let mut end = lock(&self.end);
        *lock(&self.kill_error) =
            Some("task host disconnected; tree cleanup is unconfirmed".into());
        if let Some(end) = end.as_mut() {
            end.kept = true;
        }
        drop(end);
        self.activity.send_replace(());
    }

    #[cfg(test)]
    pub(crate) fn inject_io_errors(&self, capture_error: Option<&str>, log_error: Option<&str>) {
        let mut out = lock(&self.out);
        out.capture_error = capture_error.map(str::to_owned);
        out.log_error = log_error.map(str::to_owned);
    }

    pub fn is_script(&self) -> bool {
        matches!(self.root, Root::Script(_))
    }

    pub(crate) fn script(&self) -> Option<&codemode::control::ScriptHandle> {
        match &self.root {
            Root::Script(script) => Some(script),
            Root::Process(_) => None,
        }
    }

    pub(crate) fn attachment_path(&self, index: usize, mime: &str) -> PathBuf {
        let extension = crate::media::extension_for_mime(mime);
        self.log_path
            .with_file_name(format!("{}-att-{index}.{extension}", self.id))
    }

    pub(crate) fn unseen_attachments(&self, seen: usize) -> Vec<AttachmentLease> {
        let out = lock(&self.out);
        if out.evicted {
            return Vec::new();
        }
        out.attachments
            .iter()
            .filter(|slot| slot.meta.index > seen)
            .filter_map(AttachmentSlot::try_lease)
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn unseen_attachment_count(&self, seen: usize) -> usize {
        let out = lock(&self.out);
        if out.evicted {
            0
        } else {
            out.attachments
                .iter()
                .filter(|slot| slot.meta.index > seen)
                .count()
        }
    }

    pub fn is_running(&self) -> bool {
        lock(&self.end).is_none()
    }

    pub(super) fn is_finished(&self) -> bool {
        lock(&self.end).is_some() && *self.tree_done.borrow()
    }

    /// When the task finished: its root, or the processes it held past the root's exit if they
    /// ended later.
    pub(super) fn ended_at(&self) -> Option<Instant> {
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
    pub fn scan(
        &self,
        matcher: &mut Matcher,
        from: &mut u64,
        cancel: &CancellationToken,
    ) -> std::io::Result<Option<usize>> {
        let (written, evicted, closed) = {
            let out = lock(&self.out);
            (out.written, out.evicted, out.closed)
        };
        if evicted {
            return Ok(None);
        }
        if written > *from {
            if let Err(error) = self.scan_log(matcher, from, written, cancel) {
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
    fn scan_log(
        &self,
        matcher: &mut Matcher,
        from: &mut u64,
        written: u64,
        cancel: &CancellationToken,
    ) -> std::io::Result<()> {
        let mut file = File::open(&self.log_path)?;
        file.seek(SeekFrom::Start(*from))?;
        let mut buffer = vec![0_u8; MATCH_SCAN_BYTES];
        while *from < written && !cancel.is_cancelled() {
            let want = (written - *from).min(buffer.len() as u64) as usize;
            file.read_exact(&mut buffer[..want])?;
            *from += want as u64;
            if matcher.push(&buffer[..want]) == Some(0) {
                break;
            }
        }
        Ok(())
    }

    /// How the root ended, once it has: its exit code, and whether the task held processes past
    /// the exit.
    pub fn root_end(&self) -> Option<(Option<i32>, bool)> {
        lock(&self.end)
            .as_ref()
            .map(|end| (end.exit_code, end.kept))
    }

    /// Output capture has ended, normally or with a recorded error.
    pub fn output_closed(&self) -> bool {
        lock(&self.out).closed
    }

    /// Whether the total log limit took this task's files.
    pub fn evicted(&self) -> bool {
        lock(&self.out).evicted
    }

    /// Reserves diagnostic copies under their locks and admits held-process names before reading.
    pub fn snapshot_held_reserved(
        &self,
        reserve: &mut impl FnMut(usize) -> Result<(), String>,
        members: &mut impl FnMut(&Tree) -> Result<Option<Vec<Member>>, String>,
    ) -> Result<Snapshot, String> {
        let end = lock(&self.end)
            .as_ref()
            .map(|end| end.clone_reserved(reserve))
            .transpose()?;
        let tree_done = *self.tree_done.borrow();
        let mut snapshot = self.snapshot_at_reserved(end, tree_done, reserve)?;
        if snapshot.kept {
            snapshot.held = match &self.root {
                Root::Process(tree) => members(tree)?.unwrap_or_default(),
                Root::Script(_) => Vec::new(),
            };
        }
        Ok(snapshot)
    }

    /// Refreshes only the output fields that can change while a window is read.
    pub fn refresh_snapshot_output(
        &self,
        snapshot: &mut Snapshot,
        reserve: &mut impl FnMut(usize) -> Result<(), String>,
    ) -> Result<(), String> {
        let out = lock(&self.out);
        let log_error = copy_snapshot_text(&out.log_error, reserve)?;
        let capture_error = copy_snapshot_text(&out.capture_error, reserve)?;
        let input_warning = copy_snapshot_text(&lock(&self.input_warning), reserve)?;
        let input_error = copy_snapshot_text(&lock(&self.input_error), reserve)?;
        snapshot.lines = out.lines;
        snapshot.dropped = out.dropped;
        snapshot.evicted = out.evicted;
        snapshot.log_error = log_error;
        snapshot.capture_error = capture_error;
        snapshot.input_warning = input_warning;
        snapshot.input_error = input_error;
        Ok(())
    }

    /// Whether the task has any condition that `task_warnings` reports, without cloning
    /// diagnostics. Uses the same non-nested lock order as `snapshot_at_reserved`.
    pub fn has_warnings_unlocked(&self) -> bool {
        let end_output_open = lock(&self.end).as_ref().is_some_and(|end| end.output_open);
        let tree_done = *self.tree_done.borrow();
        let out_warn = {
            let out = lock(&self.out);
            out.capture_error.is_some()
                || out.log_error.is_some()
                || out.dropped > 0
                || out.evicted
                || (end_output_open && !out.closed)
        };
        out_warn
            || (!tree_done && lock(&self.kill_error).is_some())
            || lock(&self.input_warning).is_some()
            || lock(&self.input_error).is_some()
    }

    /// State used by script reports, without copying logs or process diagnostics.
    pub fn report_state(&self) -> (&'static str, bool) {
        let (state, held_past_exit) = {
            let end = lock(&self.end);
            let state = match end.as_ref() {
                None => "running",
                Some(end) if end.killed => "killed",
                Some(_) => "exited",
            };
            (state, end.as_ref().is_some_and(|end| end.kept))
        };
        (state, held_past_exit && !*self.tree_done.borrow())
    }

    /// Footer classification needs scalar state and a kill-error flag, not diagnostic text.
    pub(super) fn notice_snapshot(&self) -> Snapshot {
        let (state, exit_code, expired, elapsed, held_past_exit) = {
            let end = lock(&self.end);
            (
                match end.as_ref() {
                    None => "running",
                    Some(end) if end.killed => "killed",
                    Some(_) => "exited",
                },
                end.as_ref().and_then(|end| end.exit_code),
                end.as_ref().is_some_and(|end| end.expired),
                end.as_ref()
                    .map_or_else(|| self.started.elapsed(), |end| end.elapsed),
                end.as_ref().is_some_and(|end| end.kept),
            )
        };
        let tree_done = *self.tree_done.borrow();
        Snapshot {
            state,
            exit_code,
            elapsed,
            expired,
            kept: held_past_exit && !tree_done,
            tree_ended: held_past_exit && tree_done,
            // Shown uses the ordinary commit interface, which only checks presence.
            kill_error: (!tree_done && lock(&self.kill_error).is_some()).then(String::new),
            held: Vec::new(),
            forced_end: false,
            lines: 0,
            dropped: 0,
            evicted: false,
            output_open: false,
            log_error: None,
            capture_error: None,
            input_warning: None,
            input_error: None,
            leftovers: Vec::new(),
        }
    }

    #[cfg(test)]
    pub fn snapshot(&self) -> Snapshot {
        let end = lock(&self.end).clone();
        let tree_done = *self.tree_done.borrow();
        self.snapshot_at_reserved(end, tree_done, &mut |_| Ok(()))
            .expect("unrestricted test snapshot")
    }

    pub(super) fn snapshot_at_reserved(
        &self,
        end: Option<Final>,
        tree_done: bool,
        reserve: &mut impl FnMut(usize) -> Result<(), String>,
    ) -> Result<Snapshot, String> {
        let held_past_exit = end.as_ref().is_some_and(|end| end.kept);
        let out = lock(&self.out);
        Ok(Snapshot {
            state: match &end {
                None => "running",
                Some(end) if end.killed => "killed",
                Some(_) => "exited",
            },
            exit_code: end.as_ref().and_then(|end| end.exit_code),
            forced_end: end.as_ref().is_some_and(|end| end.forced_end),
            elapsed: end
                .as_ref()
                .map_or_else(|| self.started.elapsed(), |end| end.elapsed),
            lines: out.lines,
            dropped: out.dropped,
            evicted: out.evicted,
            output_open: end.as_ref().is_some_and(|end| end.output_open) && !out.closed,
            expired: end.as_ref().is_some_and(|end| end.expired),
            log_error: copy_snapshot_text(&out.log_error, reserve)?,
            capture_error: copy_snapshot_text(&out.capture_error, reserve)?,
            input_warning: copy_snapshot_text(&lock(&self.input_warning), reserve)?,
            input_error: copy_snapshot_text(&lock(&self.input_error), reserve)?,
            // A kill can record an error after the ender confirms completion. Only an
            // unconfirmed tree can still need a retry.
            kill_error: if tree_done {
                None
            } else {
                copy_snapshot_text(&lock(&self.kill_error), reserve)?
            },
            leftovers: end.map(|end| end.leftovers).unwrap_or_default(),
            kept: held_past_exit && !tree_done,
            tree_ended: held_past_exit && tree_done,
            held: Vec::new(),
        })
    }

    /// Reserves room in the input queue for `size` bytes.
    pub(super) fn admit(&self, size: usize) -> bool {
        if self.queued.fetch_add(size, Ordering::SeqCst) + size > INPUT_QUEUE_LIMIT {
            self.queued.fetch_sub(size, Ordering::SeqCst);
            return false;
        }
        true
    }

    /// The keyboard modes of a PTY task's program.
    pub fn modes(&self) -> Result<crate::term::keys::Modes, String> {
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
    pub fn screen(&self, memory: &Arc<BytePool>) -> Result<Screen, String> {
        self.check_size()?;
        let terminal = lock(
            self.terminal
                .as_ref()
                .ok_or("screen applies to PTY tasks only.")?,
        );
        let lease = memory
            .try_acquire(terminal.live.screen_snapshot_bytes())
            .ok_or_else(|| terminal_memory_error(memory, self))?;
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
            _lease: lease,
        })
    }

    /// Where `transcript` writes its text, beside the log.
    pub fn transcript_path(&self) -> PathBuf {
        self.log_path
            .with_file_name(format!("{}.transcript.txt", self.id))
    }

    /// Writes the pending lines into the transcript file after its final lines, and returns
    /// what this read covers: the final lines no earlier read of this session has shown, then
    /// the pending lines. The caller commits the next position only after reading them.
    pub fn transcript(
        &self,
        shared: &Tasks,
        state: &ReadState,
        memory: &Arc<BytePool>,
    ) -> Result<Transcript, String> {
        let terminal = self
            .terminal
            .as_ref()
            .ok_or("transcript applies to PTY tasks only.")?;
        let mut pty = lock(terminal);
        if lock(&self.out).evicted {
            return Err(EVICTED_TRANSCRIPT.into());
        }
        let Pty { live, spool } = &mut *pty;
        if lock(&self.transcript_file).is_none() {
            let reason = spool.stopped.as_deref().unwrap_or("it is gone");
            return Err(format!("The transcript file is missing: {reason}."));
        }
        // The result shows the pending lines only as the file holds them, those that fit the
        // per-task limit.
        let lease = if spool.stopped.is_none() {
            memory
                .try_acquire(live.pending_snapshot_bytes())
                .ok_or_else(|| terminal_memory_error(memory, self))?
        } else {
            Lease::empty(memory)
        };
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
        let (start, shown) = state.shown;
        Ok(Transcript {
            path: self.transcript_path(),
            first_line: shown + 1,
            start,
            lines: spool.lines,
            bytes: spool.bytes,
            pending,
            cut,
            stopped,
            _lease: lease,
        })
    }

    /// Queues one write and confirms its result; a timeout never claims delivery or safe retry.
    pub async fn send(
        &self,
        input: Option<&[u8]>,
        eof: bool,
        cancel: &CancellationToken,
        input_owner: crate::lease::Owner,
    ) -> Result<(), String> {
        let Some(done) = self.queue_input(input, eof, input_owner)? else {
            return Ok(());
        };
        let uncertain = tokio::select! {
            biased;
            result = done => {
                let result = result.unwrap_or_else(|_| Err(
                    "INPUT_DELIVERY_UNCONFIRMED: the input writer stopped before confirming delivery; input may have been partially delivered. Do not blindly retry.".into()
                ));
                if let Err(error) = &result {
                    *lock(&self.input_error) = Some(error.clone());
                }
                return result;
            },
            _ = cancel.cancelled() => "the call was cancelled",
            _ = tokio::time::sleep(INPUT_WRITE_LIMIT) => "the input writer did not finish within 5 s",
        };
        let error = format!(
            "INPUT_DELIVERY_UNCONFIRMED: {uncertain}; input may be partially delivered or still queued, and may take effect later. Do not blindly retry."
        );
        *lock(&self.input_error) = Some(error.clone());
        Err(error)
    }

    fn queue_input(
        &self,
        input: Option<&[u8]>,
        eof: bool,
        input_owner: crate::lease::Owner,
    ) -> Result<Option<oneshot::Receiver<Result<(), String>>>, String> {
        // A pipe write, including an explicit empty string, requires writable stdin.
        let data = input.filter(|bytes| !self.pty || !bytes.is_empty());
        if eof && self.pty {
            return Err("EOF_UNSUPPORTED_IN_PTY: a PTY has no stdin half-close. Press Ctrl-D with keys: [\"C-d\"], or kill the task.".into());
        }
        if data.is_none() && !eof {
            return Ok(None);
        }
        let bytes = data.unwrap_or_default();
        if self.pty && !bytes.is_empty() {
            let mode = lock(&self.pty_control)
                .as_ref()
                .map(|control| control.canonical_input());
            let warning = match mode {
                Some(Ok(Some(false))) => None,
                Some(Ok(Some(true))) => Some(
                    "Canonical PTY input may silently discard long lines (Linux: over 4095 bytes), even across multiple polls. A terminal write does not confirm delivery to the program. Use pipe stdin or a file for long text.".to_owned()
                ),
                #[cfg(windows)]
                _ => Some(
                    "The Windows PTY input mode could not be verified. MSYS console readers may discard a leading input byte after a resize, even for short keys; terminal input can also transform or discard long lines across polls. A terminal write does not confirm delivery to the program. Use pipe stdin or a file when exact text delivery is required.".to_owned()
                ),
                #[cfg(not(windows))]
                _ => Some(
                    "The PTY input mode could not be verified; terminal input may be transformed or discarded, including long lines split across polls. A terminal write does not confirm delivery to the program. Use pipe stdin or a file for long text.".to_owned()
                ),
            };
            if let Some(warning) = warning {
                *lock(&self.input_warning) = Some(warning);
            }
        }
        let mut sender = lock(&self.input);
        let Some(tx) = sender.as_ref() else {
            return match data {
                None => Ok(None), // closing a closed stdin is a no-op
                Some(_) if self.is_running() => Err(
                    "stdin is closed (pipe tasks default to /dev/null and close stdin after start input; start with eof: false to keep stdin open for poll input); the input was not delivered.".into(),
                ),
                Some(_) => Err(
                    "The task has exited and stdin is closed (start pipe tasks with eof: false to keep stdin open while running); the input was not delivered.".into(),
                ),
            };
        };
        if bytes.is_empty() && !eof {
            return Ok(None);
        }
        if !self.admit(bytes.len()) {
            return Err("INPUT_BACKPRESSURE: 1 MiB of earlier input is still waiting for the program to read it. Poll for output, then retry.".into());
        }
        let (done, result) = oneshot::channel();
        if tx
            .send(Input {
                bytes: bytes.to_vec(),
                eof,
                initial: false,
                done: Some(done),
                _input_owner: input_owner,
            })
            .is_err()
        {
            self.queued.fetch_sub(bytes.len(), Ordering::SeqCst);
            *sender = None;
            return Err("stdin is closed (start pipe tasks with eof: false to keep stdin open for poll input); the input was not delivered.".into());
        }
        if eof {
            *sender = None;
        }
        Ok(Some(result))
    }

    /// Terminates the task's tree. A running task is marked killed, and the waiter thread records
    /// the exit; a finished task keeps its result while its kept processes end.
    pub fn kill(&self) -> std::io::Result<()> {
        if self.is_running() {
            self.kill_requested.store(true, Ordering::SeqCst);
        } else if self.is_finished() {
            return Ok(());
        }
        // Recorded for a running task too: its root may have exited already, with the waiter
        // past its own kill.
        let killed = self.root.kill();
        if let Err(error) = &killed {
            *lock(&self.kill_error) = Some(error.to_string());
        }
        // Only confirmed tree completion clears an earlier cleanup failure: a successful
        // kill can return before the ender records host loss, then resume after that record.
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
    #[allow(clippy::too_many_arguments)]
    pub fn read_window(
        &self,
        state: &mut ReadState,
        truncate: Truncate,
        budget: usize,
        raw: bool,
        clean: bool,
        keep_nul: bool,
        encoding: &'static Encoding,
        until: Option<u64>,
    ) -> std::io::Result<Window> {
        if lock(&self.out).evicted {
            return Ok(self.read_evicted(
                state, truncate, budget, raw, clean, keep_nul, encoding, until,
            ));
        }
        match self.read_stored(
            state, truncate, budget, raw, clean, keep_nul, encoding, until,
        ) {
            Ok(window) => Ok(window),
            Err(_) if lock(&self.out).evicted => {
                // Eviction deleted the log between the flags above and the read. The window is
                // empty either way, so the half-read state is simply not committed.
                Ok(self.read_evicted(
                    state, truncate, budget, raw, clean, keep_nul, encoding, until,
                ))
            }
            Err(error) => Err(error),
        }
    }

    /// The window of an evicted task: an empty one. The cursor moves past the bytes that are
    /// gone, so the next read starts where this one would have ended; the eviction itself is
    /// reported by the snapshot, not by inventing an omitted line range.
    #[allow(clippy::too_many_arguments)]
    fn read_evicted(
        &self,
        state: &mut ReadState,
        truncate: Truncate,
        budget: usize,
        raw: bool,
        clean: bool,
        keep_nul: bool,
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
            clean,
            keep_nul,
            include_markers: true,
            encoding,
            source: "log",
        };
        let window = WindowBuilder::new(spec, &mut state.cleaner, state.lines_before + 1).finish();
        state.cursor = end.max(start);
        window
    }

    #[allow(clippy::too_many_arguments)]
    fn read_stored(
        &self,
        state: &mut ReadState,
        truncate: Truncate,
        budget: usize,
        raw: bool,
        clean: bool,
        keep_nul: bool,
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
            clean,
            keep_nul,
            include_markers: true,
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
                file.read_exact(&mut buffer[..want])?;
                builder.push(&buffer[..want]);
                remaining -= want as u64;
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
        file.seek(SeekFrom::Start(from))?;
        incomplete_legacy_suffix_reader(file.take(end - from), encoding).map(|held| held as u64)
    }
}
