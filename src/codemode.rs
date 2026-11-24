//! JavaScript tasks and their asynchronous bridge to the four host tools.

pub(crate) mod bridge;
pub(crate) mod control;
pub(crate) mod encode;
#[cfg(test)]
mod tests;
mod vm;

use crate::service::Service;
use crate::session::{LaunchContext, Session};
use crate::tasks::Tasks;
use crate::tasks::state::Task;
use control::{Control, Report, ScriptHandle};
use std::io::PipeWriter;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use tokio::runtime::Handle;
use tokio_util::sync::CancellationToken;

pub struct Work {
    pub service: Arc<Service>,
    pub session: Arc<Session>,
    pub launch: Arc<LaunchContext>,
    pub transfer: bool,
    pub call_id: Option<u64>,
    pub code: String,
    pub writer: PipeWriter,
    pub task: Arc<Task>,
    pub tasks: Arc<Tasks>,
    pub captured: mpsc::Receiver<()>,
    pub input_owner: crate::lease::Owner,
}

pub struct ScriptWorker(mpsc::SyncSender<Work>);

impl ScriptWorker {
    pub fn start(self, work: Work) {
        self.0
            .send(work)
            .expect("the prepared script worker stopped");
    }
}

/// Creates a waiting VM thread. Dropping the worker abandons it.
pub fn prepare(runtime: Handle) -> Result<(ScriptHandle, ScriptWorker), String> {
    let (wake, receiver) = mpsc::channel();
    let script = ScriptHandle(Arc::new(Control {
        stopped: AtomicBool::new(false),
        exited: AtomicBool::new(false),
        cancel: CancellationToken::new(),
        wake,
        report: Mutex::new(Report::default()),
        in_flight: (Mutex::new(0), Condvar::new()),
        held_attachments: Mutex::default(),
        queued_starts: AtomicUsize::new(0),
        cancelled_starts: AtomicUsize::new(0),
    }));
    let (sender, job) = mpsc::sync_channel(1);
    let control = script.clone();
    std::thread::Builder::new()
        .name("fastexec-script".into())
        .stack_size(2 * 1024 * 1024)
        .spawn(move || {
            if let Ok(work) = job.recv() {
                vm::run(work, control, receiver, runtime);
            }
        })
        .map_err(|error| format!("Cannot create the script worker: {error}"))?;
    Ok((script, ScriptWorker(sender)))
}
