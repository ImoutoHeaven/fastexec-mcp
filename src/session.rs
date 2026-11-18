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

enum LoginEnv {
    Snapshot(Vec<(OsString, OsString)>),
    Failed(Arc<Mutex<Option<String>>>),
}

/// The start that ran the probe reserves its failure message until it can deliver it.
/// If that start fails or is cancelled, another fallback start can report it.
pub struct ProbeWarning {
    reserved: Option<String>,
    unreported: Arc<Mutex<Option<String>>>,
}

impl ProbeWarning {
    pub fn deliver(mut self) -> Option<String> {
        self.reserved
            .take()
            .or_else(|| lock(&self.unreported).take())
    }
}

impl Drop for ProbeWarning {
    fn drop(&mut self) {
        if let Some(message) = self.reserved.take() {
            *lock(&self.unreported) = Some(message);
        }
    }
}

type BashLookup = OnceLock<Result<PathBuf, String>>;

#[derive(Default)]
struct LaunchCache {
    /// GNU bash for this context, found on first use and reused by every task of the session.
    /// Concurrent callers share an attempt, including its failure. A later caller may retry.
    bash: Mutex<Arc<BashLookup>>,
    /// Login-shell environment snapshot captured once on the first non-login start.
    login_env: OnceLock<LoginEnv>,
}

impl LaunchCache {
    fn bash_lookup(&self) -> Arc<BashLookup> {
        let mut attempt = lock(&self.bash);
        if attempt.get().is_some_and(Result::is_err) {
            *attempt = Arc::default();
        }
        Arc::clone(&attempt)
    }
}

const SHELL_MANAGED_VARS: [&str; 6] = ["PWD", "OLDPWD", "SHLVL", "_", "SHELLOPTS", "BASHOPTS"];

/// What a session's tasks inherit: the directory they start in and their whole environment.
/// A task's environment comes from here, never from the server's own globals, so one server can
/// serve sessions with different environments.
pub struct LaunchContext {
    /// The directory a task starts in when its request names none.
    pub cwd: PathBuf,
    pub env: Vec<(OsString, OsString)>,
    /// The directory that anchors a relative home even after a codemode call changes its cwd.
    session_cwd: PathBuf,
    /// Variable names stripped from this session (e.g. `FASTEXEC_HTTP_TOKEN`), removed from
    /// login-profile snapshots as well.
    removed: Vec<String>,
    cache: Arc<LaunchCache>,
}

impl LaunchContext {
    pub fn new(cwd: PathBuf, env: Vec<(OsString, OsString)>) -> Self {
        Self {
            session_cwd: cwd.clone(),
            cwd,
            env,
            removed: Vec::new(),
            cache: Arc::new(LaunchCache::default()),
        }
    }

    /// This process's own directory and environment, for a server serving one session.
    pub fn current() -> std::io::Result<Self> {
        Ok(Self::new(
            std::env::current_dir()?,
            std::env::vars_os().collect(),
        ))
    }

    /// The same launch environment for a new session, with independent discovery and snapshot caches.
    pub fn for_session(&self) -> Self {
        Self {
            cwd: self.cwd.clone(),
            env: self.env.clone(),
            session_cwd: self.session_cwd.clone(),
            removed: self.removed.clone(),
            cache: Arc::new(LaunchCache::default()),
        }
    }

    /// This context with `cwd` replaced, sharing its cached bash path and login snapshot.
    pub fn with_cwd(&self, cwd: PathBuf) -> Self {
        Self {
            cwd,
            env: self.env.clone(),
            session_cwd: self.session_cwd.clone(),
            removed: self.removed.clone(),
            cache: Arc::clone(&self.cache),
        }
    }

    /// This context with `name` removed from its environment and future login snapshots, so the
    /// tasks it starts cannot carry a secret the caller used to reach the server.
    pub fn without(mut self, name: &str) -> Self {
        self.env.retain(|(key, _)| !name_matches(key, name));
        self.removed.push(name.to_owned());
        self.cache = Arc::new(LaunchCache::default());
        self
    }

    /// The value of `name`. Windows compares environment names case-insensitively; Unix does
    /// not, so a context can hold `Path` and `PATH` as distinct names there.
    pub fn var(&self, name: &str) -> Option<&OsStr> {
        var_in(&self.env, name)
    }

    /// The absolute home directory for file tools and the Host description.
    pub fn home(&self) -> Option<PathBuf> {
        self.env_home()
            .and_then(|path| crate::files::resolve_from(&path, &self.session_cwd).ok())
    }

    /// The home directory this context's environment names: `USERPROFILE`, else `HOMEDRIVE` with
    /// `HOMEPATH`, else `HOME` on Windows, and `HOME` on Unix. An empty value is a missing one.
    fn env_home(&self) -> Option<PathBuf> {
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

    /// The bash these tasks run, found once per session cache.
    pub fn bash(&self) -> Result<PathBuf, String> {
        self.cache
            .bash_lookup()
            .get_or_init(|| crate::bash::find(self))
            .clone()
    }

    /// Returns `(base_env, used_snapshot, one_time_warning)` for a task launch.
    pub fn task_env(
        &self,
        bash: &Path,
        login: bool,
    ) -> (&[(OsString, OsString)], bool, Option<ProbeWarning>) {
        if login {
            return (&self.env, false, None);
        }
        let mut reserved = None;
        let entry = self.cache.login_env.get_or_init(|| {
            match crate::process::probe_login_env(bash, self) {
                Ok(mut vars) => {
                    vars.retain(|(key, _)| {
                        !SHELL_MANAGED_VARS
                            .iter()
                            .any(|managed| name_matches(key, managed))
                            && !self
                                .removed
                                .iter()
                                .any(|removed| name_matches(key, removed))
                    });
                    LoginEnv::Snapshot(vars)
                }
                Err(reason) => {
                    reserved = Some(format!(
                        "Login environment probe failed ({reason}); tasks use the server's environment. Pass loginShell: true to run the profile."
                    ));
                    LoginEnv::Failed(Arc::default())
                }
            }
        });
        match entry {
            LoginEnv::Snapshot(vars) => (vars.as_slice(), true, None),
            LoginEnv::Failed(unreported) => (
                self.env.as_slice(),
                false,
                Some(ProbeWarning {
                    reserved,
                    unreported: Arc::clone(unreported),
                }),
            ),
        }
    }
}

pub(crate) fn var_in<'a>(env: &'a [(OsString, OsString)], name: &str) -> Option<&'a OsStr> {
    env.iter()
        .find(|(key, _)| name_matches(key, name))
        .map(|(_, value)| value.as_os_str())
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

/// Splits an optional reader suffix before lookup or any task side effect.
pub fn parse_handle(handle: &str) -> Result<(&str, Option<&str>), String> {
    let Some((id, key)) = handle.split_once('.') else {
        return Ok((handle, None));
    };
    if id.is_empty() || key.len() != 4 || !key.bytes().all(|byte| ALPHABET.contains(&byte)) {
        return Err(format!(
            "Unknown taskId {handle:?}. Use a bare task ID or <taskId>.<reader>, with 4 Crockford base32 reader characters."
        ));
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
#[derive(Clone, Default)]
pub struct ReadState {
    pub cursor: u64,
    /// Images returned or warned about, including markers lost with dropped log bytes.
    pub images_seen: usize,
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
            let message = parse_handle(invalid).unwrap_err();
            assert!(message.contains("Unknown taskId"), "{message}");
            assert!(message.contains("<taskId>.<reader>"), "{message}");
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

    fn context(env: &[(&str, &str)]) -> LaunchContext {
        LaunchContext::new(
            std::env::temp_dir(),
            env.iter()
                .map(|(name, value)| ((*name).into(), (*value).into()))
                .collect(),
        )
    }

    #[test]
    fn concurrent_bash_lookups_share_a_failure_and_a_later_call_retries() {
        let cache = LaunchCache::default();
        let ready = std::sync::Barrier::new(4);
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    let lookup = cache.bash_lookup();
                    ready.wait(); // Every caller has joined this attempt before it can fail.
                    assert_eq!(
                        lookup.get_or_init(|| {
                            attempts.fetch_add(1, Ordering::Relaxed);
                            Err("unavailable".into())
                        }),
                        &Err("unavailable".into())
                    );
                });
            }
        });
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
        let path = PathBuf::from("available");
        let retry = cache.bash_lookup();
        assert_eq!(retry.get_or_init(|| Ok(path.clone())), &Ok(path));
        assert!(Arc::ptr_eq(&retry, &cache.bash_lookup()));
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
            let half = context(&[("HOMEDRIVE", "D:"), ("HOME", "/d/home/third")]);
            assert_eq!(half.home().unwrap(), PathBuf::from(r"D:\home\third"));
            assert_eq!(context(&[("HOME", "/home/third")]).home(), None);
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
