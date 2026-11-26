//! Session-scoped task notices and independent, bounded reader cursors.

use super::state::{ReadState, Snapshot, Task, TaskError, lock, pids};
use crate::env::LaunchContext;
use crate::id::random_chars;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

/// Serializes a session's final synchronous render and commit of its outside-task count.
/// Hold only after the last await; dropping an uncommitted claim leaves the count due.
pub struct OutsideClaim<'a> {
    state: Option<MutexGuard<'a, bool>>,
}

impl<'a> OutsideClaim<'a> {
    pub fn none() -> Self {
        Self { state: None }
    }

    pub fn is_claimed(&self) -> bool {
        self.state.is_some()
    }

    /// A budget-clipped count stays available to another result.
    pub fn release(&mut self) {
        self.state = None;
    }

    /// The result carrying or checking the count has committed.
    pub fn consume(mut self) {
        if let Some(state) = self.state.as_mut() {
            **state = true;
        }
    }
}

/// A session's task entries and background-notice scope.
pub struct Session {
    pub id: String,
    pub label: String,
    pub launch: Arc<LaunchContext>,
    tasks: Mutex<HashMap<String, Arc<TaskEntry>>>,
    outside_consumed: Mutex<bool>,
}

impl Session {
    pub fn new(id: String, label: String, launch: Arc<LaunchContext>) -> Arc<Self> {
        Arc::new(Self {
            id,
            label,
            launch,
            tasks: Mutex::default(),
            outside_consumed: Mutex::new(false),
        })
    }

    /// Final renderers wait for an earlier commit instead of overtaking an in-flight claim.
    pub fn claim_outside(&self) -> OutsideClaim<'_> {
        let state = lock(&self.outside_consumed);
        if *state {
            OutsideClaim::none()
        } else {
            OutsideClaim { state: Some(state) }
        }
    }

    pub fn entry(&self, task: &Task) -> Arc<TaskEntry> {
        Arc::clone(
            lock(&self.tasks)
                .entry(task.id.clone())
                .or_insert_with(|| Arc::new(TaskEntry::default())),
        )
    }

    /// Never inserts: a list or footer cannot attach the caller to another task.
    pub fn existing(&self, task: &Task) -> Option<Arc<TaskEntry>> {
        lock(&self.tasks).get(&task.id).cloned()
    }

    /// The caller holds the registry lock; entries retire with their tasks.
    pub fn touched(&self, tasks: &[Arc<Task>]) -> Vec<(Arc<Task>, Arc<TaskEntry>)> {
        let mut entries = lock(&self.tasks);
        let present: HashSet<&str> = tasks.iter().map(|task| task.id.as_str()).collect();
        entries.retain(|id, _| present.contains(id.as_str()));
        tasks
            .iter()
            .rev()
            .filter_map(|task| {
                entries
                    .get(&task.id)
                    .map(|entry| (Arc::clone(task), Arc::clone(entry)))
            })
            .collect()
    }
}

const MAX_READERS: usize = 16;

#[derive(Default)]
struct Readers {
    by_key: HashMap<String, Arc<Reader>>,
    lru: VecDeque<String>,
}

/// This session's state for one task: its readers and what its results told it.
#[derive(Default)]
pub struct TaskEntry {
    readers: Mutex<Readers>,
    reported: AtomicBool,
    tree_end_reported: AtomicBool,
    keeps_reported: Mutex<Option<Vec<u32>>>,
}

impl TaskEntry {
    /// Called while the registry is locked, before this new task becomes visible.
    pub fn start_reader(&self, key: String) -> Arc<Reader> {
        let mut readers = lock(&self.readers);
        assert!(readers.by_key.is_empty(), "a new task already has readers");
        let reader = Arc::new(Reader::new(key.clone(), false));
        readers.lru.push_back(key.clone());
        readers.by_key.insert(key, Arc::clone(&reader));
        reader
    }

    /// Resolve and touch atomically, so one live handle always uses one read gate.
    pub fn reader(&self, key: Option<&str>) -> Result<Arc<Reader>, TaskError> {
        let mut readers = lock(&self.readers);
        if let Some(key) = key
            && let Some(reader) = readers.by_key.get(key).cloned()
        {
            readers.lru.retain(|old| old != key);
            readers.lru.push_back(key.into());
            return Ok(reader);
        }
        let key = match key {
            Some(key) => key.into(),
            None => loop {
                let key =
                    random_chars(4).map_err(|error| format!("Cannot name the reader: {error}"))?;
                if !readers.by_key.contains_key(&key) {
                    break key;
                }
            },
        };
        if readers.by_key.len() == MAX_READERS {
            let idle = readers.lru.iter().position(|key| {
                let reader = &readers.by_key[key];
                Arc::strong_count(reader) == 1 && Arc::strong_count(&reader.read) == 1
            });
            let Some(idle) = idle else {
                return Err(TaskError::Overload(
                    "All 16 readers for this task are busy; retry when a read finishes.".into(),
                ));
            };
            let old = readers
                .lru
                .remove(idle)
                .expect("the idle reader is present");
            readers.by_key.remove(&old);
        }
        let reader = Arc::new(Reader::new(key.clone(), true));
        readers.lru.push_back(key.clone());
        readers.by_key.insert(key, Arc::clone(&reader));
        Ok(reader)
    }

    pub fn is_reported(&self) -> bool {
        self.reported.load(Ordering::SeqCst)
    }

    pub fn tree_end_reported(&self) -> bool {
        self.tree_end_reported.load(Ordering::SeqCst)
    }

    /// Compare borrowed prior PIDs without copying a possibly longer previous list.
    pub fn keeps_reported_eq(&self, current: &[u32]) -> bool {
        lock(&self.keeps_reported).as_deref() == Some(current)
    }

    pub fn mark_root_reported(&self, shown: &Snapshot) {
        if shown.state != "running" {
            self.reported.store(true, Ordering::SeqCst);
        }
    }

    pub fn mark_tree_end_reported(&self) {
        self.tree_end_reported.store(true, Ordering::SeqCst);
    }

    pub fn mark_reported(&self, shown: &Snapshot) {
        self.mark_root_reported(shown);
        if shown.tree_ended {
            self.mark_tree_end_reported();
        }
    }

    /// Marks a folded finished child reported without building a full `Snapshot`.
    pub fn mark_finished_reported(&self, tree_ended: bool) {
        self.reported.store(true, Ordering::SeqCst);
        if tree_ended {
            self.mark_tree_end_reported();
        }
    }

    /// A task's own status line may be cut before its held processes.
    pub fn mark_keeps_reported(&self, shown: &Snapshot) {
        if shown.kept && shown.held_known && shown.kill_error.is_none() {
            *lock(&self.keeps_reported) = Some(pids(&shown.held));
        }
    }
}

/// One cursor over one task, including the root exit this reader has seen.
pub struct Reader {
    pub key: String,
    pub read: Arc<tokio::sync::Mutex<ReadState>>,
    reported: AtomicBool,
    fresh: AtomicBool,
}

impl Reader {
    fn new(key: String, fresh: bool) -> Self {
        Self {
            key,
            read: Arc::default(),
            reported: AtomicBool::new(false),
            fresh: AtomicBool::new(fresh),
        }
    }

    pub fn handle(&self, task_id: &str) -> String {
        format!("{task_id}.{}", self.key)
    }

    pub fn read_gate(&self) -> Arc<tokio::sync::Mutex<ReadState>> {
        Arc::clone(&self.read)
    }

    pub fn is_reported(&self) -> bool {
        self.reported.load(Ordering::SeqCst)
    }

    pub fn mark_reported(&self, shown: &Snapshot) {
        if shown.state != "running" {
            self.reported.store(true, Ordering::SeqCst);
        }
    }

    pub fn is_fresh(&self) -> bool {
        self.fresh.load(Ordering::SeqCst)
    }

    pub fn mark_delivered(&self) {
        self.fresh.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn background_metadata_pid_comparison_borrows_prior_list_and_preserves_none() {
        let entry = TaskEntry::default();
        assert!(!entry.keeps_reported_eq(&[]));
        *lock(&entry.keeps_reported) = Some(vec![3, 7, 9]);
        let previous = lock(&entry.keeps_reported).as_ref().unwrap().as_ptr();
        assert!(entry.keeps_reported_eq(&[3, 7, 9]));
        assert!(!entry.keeps_reported_eq(&[3]));
        assert!(!entry.keeps_reported_eq(&[]));
        assert!(!entry.keeps_reported_eq(&[9, 7, 3]));
        assert_eq!(
            lock(&entry.keeps_reported).as_ref().unwrap().as_ptr(),
            previous
        );
        *lock(&entry.keeps_reported) = Some(Vec::new());
        assert!(entry.keeps_reported_eq(&[]));
    }

    #[test]
    fn readers_attach_recreate_and_touch_the_idle_lru() {
        let entry = TaskEntry::default();
        let initial = entry.start_reader("0000".into());
        assert!(!initial.is_fresh());
        initial.read.try_lock().unwrap().cursor = 12;
        let same = entry.reader(Some("0000")).unwrap();
        assert!(Arc::ptr_eq(&initial, &same));
        drop((initial, same));
        for n in 1..16 {
            let reader = entry.reader(Some(&format!("{n:04}"))).unwrap();
            reader.read.try_lock().unwrap().cursor = 42;
            reader.mark_delivered();
        }
        assert_eq!(
            entry
                .reader(Some("0000"))
                .unwrap()
                .read
                .try_lock()
                .unwrap()
                .cursor,
            12
        );
        entry.reader(Some("zzzz")).unwrap();
        assert!(lock(&entry.readers).by_key.contains_key("0000"));
        assert!(!lock(&entry.readers).by_key.contains_key("0001"));
        let recreated = entry.reader(Some("0001")).unwrap();
        assert!(recreated.is_fresh());
        assert_eq!(recreated.read.try_lock().unwrap().cursor, 0);
        let attached = entry.reader(None).unwrap();
        assert!(attached.is_fresh());
        assert_eq!(attached.key.len(), 4);
        assert!(
            attached
                .key
                .bytes()
                .all(|byte| crate::id::ALPHABET.contains(&byte))
        );
        assert_ne!(attached.key, recreated.key);
        assert_eq!(lock(&entry.readers).by_key.len(), MAX_READERS);
    }

    #[test]
    fn sixteen_busy_readers_refuse_an_attach_without_losing_a_gate() {
        let entry = TaskEntry::default();
        let busy: Vec<_> = (0..16)
            .map(|n| entry.reader(Some(&format!("{n:04}"))).unwrap())
            .collect();
        assert!(
            matches!(entry.reader(None), Err(TaskError::Overload(message)) if message.contains("16 readers"))
        );
        let same = entry.reader(Some(&busy[0].key)).unwrap();
        assert!(Arc::ptr_eq(&same, &busy[0]));
        drop((same, busy));
        assert!(entry.reader(None).is_ok());
    }

    #[tokio::test]
    async fn a_waiting_gate_is_not_evicted_by_sixteen_more_attaches() {
        let entry = TaskEntry::default();
        let reader = entry.reader(Some("gate")).unwrap();
        let gate = reader.read_gate();
        let held = reader.read_gate().lock_owned().await;
        drop(reader);
        let queued = Arc::clone(&gate).lock_owned();
        tokio::pin!(queued);
        assert!(
            std::future::poll_fn(|cx| {
                std::task::Poll::Ready(queued.as_mut().poll(cx).is_pending())
            })
            .await
        );
        for n in 0..16 {
            entry.reader(Some(&format!("{n:04}"))).unwrap();
        }
        let current = entry.reader(Some("gate")).unwrap();
        assert!(Arc::ptr_eq(&current.read, &gate));
        drop(held);
        let waiting = queued.await;
        drop(waiting);
    }

    #[tokio::test]
    async fn a_cancelled_blocking_read_keeps_its_gate_until_it_finishes() {
        let entry = TaskEntry::default();
        let reader = entry.reader(Some("gate")).unwrap();
        let held = reader.read_gate().lock_owned().await;
        drop(reader);
        let (started, ready) = std::sync::mpsc::channel();
        let (finish, released) = std::sync::mpsc::channel();
        let reading = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            released.recv().unwrap();
            drop(held);
        });
        ready
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        reading.abort();
        for n in 0..16 {
            entry.reader(Some(&format!("{n:04}"))).unwrap();
        }
        let gate_refs = lock(&entry.readers)
            .by_key
            .get("gate")
            .map(|reader| Arc::strong_count(&reader.read));
        finish.send(()).unwrap();
        reading.await.unwrap();
        assert_eq!(gate_refs, Some(2));
    }

    #[test]
    fn outside_count_commits_in_claim_order_and_release_leaves_it_due() {
        let session = Session::new(
            "outside".into(),
            "outside".into(),
            Arc::new(LaunchContext::current().unwrap()),
        );
        let mut released = session.claim_outside();
        assert!(released.is_claimed());
        released.release();
        let first = session.claim_outside();
        assert!(first.is_claimed());
        let other = Arc::clone(&session);
        let (entered, started) = std::sync::mpsc::channel();
        let (commit, committed) = std::sync::mpsc::channel();
        let second = std::thread::spawn(move || {
            entered.send(()).unwrap();
            let claim = other.claim_outside();
            commit.send(claim.is_claimed()).unwrap();
            claim.consume();
        });
        started
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        let overtook = committed.recv_timeout(std::time::Duration::from_millis(25));
        first.consume();
        assert!(
            matches!(overtook, Err(std::sync::mpsc::RecvTimeoutError::Timeout)),
            "a second renderer must not commit before the first claim"
        );
        assert!(
            !committed
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap()
        );
        second.join().unwrap();
        assert!(!session.claim_outside().is_claimed());
    }
}
