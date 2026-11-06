//! A reader's own state: the cursor and reported flags a result consumes, kept per task so two
//! sessions reading one task each see all of it.

use crate::output::Cleaner;
use crate::tasks::{Snapshot, Task, pids};
use std::collections::{HashMap, HashSet};
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

/// A reader: its own cursors, cleaning state, and reported flags for every task it addresses.
pub struct Session {
    /// The session ID its own client chose: the full one for a daemon session, and the short one
    /// the stdio adapter makes for itself.
    pub id: String,
    /// The client name and a short ID, shown as a task's owner.
    pub label: String,
    /// What this session's tasks inherit.
    pub launch: Arc<LaunchContext>,
    views: Mutex<HashMap<String, Arc<TaskView>>>,
}

impl Session {
    pub fn new(id: String, label: String, launch: Arc<LaunchContext>) -> Arc<Self> {
        Arc::new(Self {
            id,
            label,
            launch,
            views: Mutex::default(),
        })
    }

    /// This session's state for `task`, added the first time the session addresses it.
    pub fn view(&self, task: &Task) -> Arc<TaskView> {
        Arc::clone(
            lock(&self.views)
                .entry(task.id.clone())
                .or_insert_with(|| Arc::new(TaskView::default())),
        )
    }

    /// This session's state for `task`, if it has addressed it. Never inserts.
    pub fn existing(&self, task: &Task) -> Option<Arc<TaskView>> {
        lock(&self.views).get(&task.id).cloned()
    }

    /// The views of the tasks in `tasks` this session has addressed, newest first, dropping
    /// entries whose task has retired. `tasks` is the registry's order, oldest first; the
    /// caller holds the registry lock, so a task cannot be added or retired meanwhile.
    pub fn touched(&self, tasks: &[Arc<Task>]) -> Vec<(Arc<Task>, Arc<TaskView>)> {
        let mut views = lock(&self.views);
        let present: HashSet<&str> = tasks.iter().map(|task| task.id.as_str()).collect();
        views.retain(|id, _| present.contains(id.as_str()));
        tasks
            .iter()
            .rev()
            .filter_map(|task| {
                views
                    .get(&task.id)
                    .map(|view| (Arc::clone(task), Arc::clone(view)))
            })
            .collect()
    }
}

/// One session's state for one task: what its results have read and what they have shown.
#[derive(Default)]
pub struct TaskView {
    /// The read state. Held for a whole read, so one session's reads of a task run one at a
    /// time; another session's reads of it do not wait.
    pub read: Arc<tokio::sync::Mutex<ReadState>>,
    /// Set once a result of this session has shown the task's final state.
    reported: AtomicBool,
    /// Set once a result of this session has shown that the processes held past the root's exit
    /// ended.
    tree_end_reported: AtomicBool,
    /// The sorted PIDs of the held processes a result of this session last showed.
    keeps_reported: Mutex<Option<Vec<u32>>>,
}

impl TaskView {
    /// The gate a read holds for its whole duration.
    pub fn read_gate(&self) -> Arc<tokio::sync::Mutex<ReadState>> {
        Arc::clone(&self.read)
    }
    /// Whether a result of this session has shown the task's final state.
    pub fn is_reported(&self) -> bool {
        self.reported.load(Ordering::SeqCst)
    }

    /// Whether a result of this session has shown the end of the processes held past the exit.
    pub fn tree_end_reported(&self) -> bool {
        self.tree_end_reported.load(Ordering::SeqCst)
    }

    /// The held processes this session's results last showed by name.
    pub fn keeps_reported(&self) -> Option<Vec<u32>> {
        lock(&self.keeps_reported).clone()
    }

    /// Records what a delivered result of this session showed: the task's final state, and the
    /// end of the processes it held. A task that ends after its snapshot stays due.
    pub fn mark_reported(&self, shown: &Snapshot) {
        if shown.state != "running" {
            self.reported.store(true, Ordering::SeqCst);
        }
        if shown.tree_ended {
            self.tree_end_reported.store(true, Ordering::SeqCst);
        }
    }

    /// Records the held processes a background footer or the list showed by name; a task's own
    /// status line may be cut before them.
    pub fn mark_keeps_reported(&self, shown: &Snapshot) {
        if shown.kept && shown.kill_error.is_none() {
            *lock(&self.keeps_reported) = Some(pids(&shown.held));
        }
    }
}

/// Where this session's next read of a task starts, and how much of a PTY task's transcript it
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
