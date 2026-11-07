//! Session-scoped task notices and independent, bounded reader cursors.

use crate::output::Cleaner;
use crate::tasks::{ALPHABET, Snapshot, Task, pids, random_chars};
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// What a session's tasks inherit: the directory they start in and their whole environment.
/// A task's environment comes from here, never from the server's own globals, so one server can
/// serve sessions with different environments.
pub struct LaunchContext {
    /// The directory a task starts in when its request names none.
    pub cwd: PathBuf,
    pub env: Vec<(OsString, OsString)>,
    /// GNU bash for this context, found on first use and reused by every task of the session. A
    /// lookup that fails is retried by the next task.
    bash: OnceLock<PathBuf>,
}

impl LaunchContext {
    pub fn new(cwd: PathBuf, env: Vec<(OsString, OsString)>) -> Self {
        Self {
            cwd,
            env,
            bash: OnceLock::new(),
        }
    }

    /// This process's own directory and environment, for a server serving one session.
    pub fn current() -> std::io::Result<Self> {
        Ok(Self::new(
            std::env::current_dir()?,
            std::env::vars_os().collect(),
        ))
    }

    /// This context with `name` removed from its environment, so the tasks it starts cannot carry
    /// a secret the caller used to reach the server.
    pub fn without(mut self, name: &str) -> Self {
        self.env.retain(|(key, _)| !name_matches(key, name));
        self
    }

    /// The value of `name`. Windows compares environment names case-insensitively; Unix does
    /// not, so a context can hold `Path` and `PATH` as distinct names there.
    pub fn var(&self, name: &str) -> Option<&OsStr> {
        self.env
            .iter()
            .find(|(key, _)| name_matches(key, name))
            .map(|(_, value)| value.as_os_str())
    }

    /// The home directory this context's environment names: `USERPROFILE`, else `HOMEDRIVE` with
    /// `HOMEPATH`, else `HOME` on Windows, and `HOME` on Unix. An empty value is a missing one.
    pub fn home(&self) -> Option<PathBuf> {
        let value = |name: &str| self.var(name).filter(|value| !value.is_empty());
        if cfg!(windows) {
            if let Some(profile) = value("USERPROFILE") {
                return Some(PathBuf::from(profile.to_os_string()));
            }
            if let (Some(drive), Some(path)) = (value("HOMEDRIVE"), value("HOMEPATH")) {
                // `HOMEPATH` starts at the root of the drive, so the two join as text: joining
                // them as paths would drop the drive.
                let mut home = drive.to_os_string();
                home.push(path);
                return Some(PathBuf::from(home));
            }
        }
        value("HOME").map(|home| PathBuf::from(home.to_os_string()))
    }

    /// The bash these tasks run, found in this context's environment.
    pub fn bash(&self) -> Result<&Path, String> {
        if let Some(found) = self.bash.get() {
            return Ok(found.as_path());
        }
        let found = crate::bash::find(self)?;
        Ok(self.bash.get_or_init(|| found).as_path())
    }
}

/// Environment names compare case-insensitively on Windows and exactly on Unix, where a name
/// that is not valid UTF-8 is a distinct name of its own.
fn name_matches(key: &OsStr, name: &str) -> bool {
    if cfg!(windows) {
        key.to_str()
            .is_some_and(|key| key.eq_ignore_ascii_case(name))
    } else {
        key == OsStr::new(name)
    }
}

/// A session's task entries and background-notice scope.
pub struct Session {
    pub id: String,
    pub label: String,
    pub launch: Arc<LaunchContext>,
    tasks: Mutex<HashMap<String, Arc<TaskEntry>>>,
}

impl Session {
    pub fn new(id: String, label: String, launch: Arc<LaunchContext>) -> Arc<Self> {
        Arc::new(Self {
            id,
            label,
            launch,
            tasks: Mutex::default(),
        })
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

/// Splits an optional reader suffix before lookup or any task side effect.
pub fn parse_handle(handle: &str) -> Result<(&str, Option<&str>), String> {
    let Some((id, key)) = handle.split_once('.') else {
        return Ok((handle, None));
    };
    if id.is_empty() || key.len() != 4 || !key.bytes().all(|byte| ALPHABET.contains(&byte)) {
        return Err(
            "taskId must be a bare task ID or <taskId>.<reader>, with 4 Crockford base32 reader characters."
                .into(),
        );
    }
    Ok((id, Some(key)))
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
    pub fn reader(&self, key: Option<&str>) -> Result<Arc<Reader>, String> {
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
                return Err(
                    "All 16 readers for this task are busy; retry when a read finishes.".into(),
                );
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

    pub fn keeps_reported(&self) -> Option<Vec<u32>> {
        lock(&self.keeps_reported).clone()
    }

    pub fn mark_reported(&self, shown: &Snapshot) {
        if shown.state != "running" {
            self.reported.store(true, Ordering::SeqCst);
        }
        if shown.tree_ended {
            self.tree_end_reported.store(true, Ordering::SeqCst);
        }
    }

    /// A task's own status line may be cut before its held processes.
    pub fn mark_keeps_reported(&self, shown: &Snapshot) {
        if shown.kept && shown.kill_error.is_none() {
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

/// Where this reader's next read of a task starts, and how much of a PTY task's transcript it
/// has shown.
#[derive(Default)]
pub struct ReadState {
    pub cursor: u64,
    pub lines_before: u64,
    pub cleaner: Cleaner,
    /// Bytes and lines of the final transcript lines a result has shown.
    pub shown: (u64, u64),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handles_are_bare_ids_or_exact_reader_suffixes() {
        assert_eq!(parse_handle("task-1").unwrap(), ("task-1", None));
        assert_eq!(
            parse_handle("task-1.abcd").unwrap(),
            ("task-1", Some("abcd"))
        );
        for invalid in [
            "a.b.abcd", ".abcd", "a.abc", "a.abcde", "a.ABCD", "a.ilou", "a.abç",
        ] {
            assert!(
                parse_handle(invalid)
                    .unwrap_err()
                    .contains("<taskId>.<reader>")
            );
        }
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
        assert!(attached.key.bytes().all(|byte| ALPHABET.contains(&byte)));
        assert_ne!(attached.key, recreated.key);
        assert_eq!(lock(&entry.readers).by_key.len(), MAX_READERS);
    }

    #[test]
    fn sixteen_busy_readers_refuse_an_attach_without_losing_a_gate() {
        let entry = TaskEntry::default();
        let busy: Vec<_> = (0..16)
            .map(|n| entry.reader(Some(&format!("{n:04}"))).unwrap())
            .collect();
        assert!(entry.reader(None).err().unwrap().contains("16 readers"));
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

    fn context(env: &[(&str, &str)]) -> LaunchContext {
        LaunchContext::new(
            std::env::temp_dir(),
            env.iter()
                .map(|(name, value)| ((*name).into(), (*value).into()))
                .collect(),
        )
    }

    // Unix discovery runs under the real task-host binary, so the corresponding test lives
    // at the MCP boundary in tests/mcp.rs rather than in this test-harness executable.
    #[cfg(windows)]
    #[test]
    fn bash_is_found_while_the_starting_directory_is_gone() {
        let gone = std::env::temp_dir().join(format!("fastexec-gone-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&gone);
        let ctx = LaunchContext::new(gone, std::env::vars_os().collect());
        let found = ctx
            .bash()
            .expect("bash is found without the session's starting directory");
        assert!(found.is_file(), "{}", found.display());
        // A second call answers from the same lookup.
        assert_eq!(ctx.bash().unwrap(), found);
    }

    #[test]
    fn a_context_finds_a_variable_by_the_case_its_platform_compares() {
        let ctx = context(&[("Path", "/first"), ("PATH", "/second")]);
        if cfg!(windows) {
            // The first name matches, and Windows compares environment names case-insensitively.
            assert_eq!(ctx.var("PATH").unwrap(), OsStr::new("/first"));
            assert_eq!(ctx.var("path").unwrap(), OsStr::new("/first"));
        } else {
            assert_eq!(ctx.var("PATH").unwrap(), OsStr::new("/second"));
            assert_eq!(ctx.var("path"), None);
        }
        assert_eq!(ctx.var("MISSING"), None);
    }

    #[test]
    fn home_follows_the_platforms_variable_order() {
        if cfg!(windows) {
            let all = context(&[
                ("USERPROFILE", r"C:\Users\profile"),
                ("HOMEDRIVE", "D:"),
                ("HOMEPATH", r"\Users\pair"),
                ("HOME", "/home/third"),
            ]);
            assert_eq!(all.home().unwrap(), PathBuf::from(r"C:\Users\profile"));
            // An empty value is a missing one, so the next name in the order answers.
            let pair = context(&[
                ("USERPROFILE", ""),
                ("HOMEDRIVE", "D:"),
                ("HOMEPATH", r"\Users\pair"),
                ("HOME", "/home/third"),
            ]);
            assert_eq!(pair.home().unwrap(), PathBuf::from(r"D:\Users\pair"));
            let half = context(&[("HOMEDRIVE", "D:"), ("HOME", "/home/third")]);
            assert_eq!(half.home().unwrap(), PathBuf::from("/home/third"));
        } else {
            assert_eq!(
                context(&[("HOME", "/home/me"), ("USERPROFILE", "/ignored")]).home(),
                Some(PathBuf::from("/home/me"))
            );
            assert_eq!(
                context(&[("HOME", ""), ("USERPROFILE", "/ignored")]).home(),
                None
            );
        }
        assert_eq!(context(&[]).home(), None);
    }

    #[cfg(unix)]
    #[test]
    fn a_name_that_is_not_utf8_is_not_a_utf8_name() {
        use std::os::unix::ffi::OsStrExt;
        let key = OsStr::from_bytes(b"FASTEXEC_\xff");
        let ctx = LaunchContext::new(std::env::temp_dir(), vec![(key.to_owned(), "1".into())]);
        // The replacement character stands in for the byte, and must not alias the name.
        assert_eq!(ctx.var("FASTEXEC_\u{fffd}"), None);
        assert_eq!(ctx.var("FASTEXEC_"), None);
    }
}
