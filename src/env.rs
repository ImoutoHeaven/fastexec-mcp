//! Launch environment, per-session probe cache, and media environment parsing.

use crate::lease::{self, BytePool};
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

pub const IMAGE_COMPRESS: &str = "FASTEXEC_IMAGE_COMPRESS";
pub const MAX_MEDIA_BYTES: &str = "FASTEXEC_MAX_MEDIA_BYTES";
pub const DEFAULT_MAX_MEDIA_BYTES: u64 = 100 << 20;
pub const MIN_MAX_MEDIA_BYTES: u64 = 1 << 20;
pub const MAX_MAX_MEDIA_BYTES: u64 = 360 << 20;

pub fn parse_image_compress(value: Option<&OsStr>) -> Result<bool, String> {
    let Some(value) = value else {
        return Ok(false);
    };
    match value.to_str() {
        Some("false") => Ok(false),
        Some("true") => Ok(true),
        _ => Err(format!(
            "invalid {IMAGE_COMPRESS} {value:?}; expected true or false"
        )),
    }
}

pub fn parse_max_media_bytes(value: Option<&OsStr>) -> Result<u64, String> {
    let Some(value) = value else {
        return Ok(DEFAULT_MAX_MEDIA_BYTES);
    };
    let parsed = value
        .to_str()
        .filter(|raw| !raw.is_empty() && raw.bytes().all(|byte| byte.is_ascii_digit()))
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|bytes| (MIN_MAX_MEDIA_BYTES..=MAX_MAX_MEDIA_BYTES).contains(bytes));
    parsed.ok_or_else(|| {
        format!(
            "invalid {MAX_MEDIA_BYTES} {value:?}; expected an integer in {MIN_MAX_MEDIA_BYTES}..={MAX_MAX_MEDIA_BYTES}"
        )
    })
}

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
    pub fn message(&mut self) -> Option<&str> {
        if self.reserved.is_none() {
            self.reserved = lock(&self.unreported).take();
        }
        self.reserved.as_deref()
    }

    pub fn commit(mut self) {
        self.reserved.take();
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

/// The native unit of an OS string: UTF-16 on Windows, a byte on Unix.
#[cfg(windows)]
pub(crate) type Unit = u16;
/// The native unit of an OS string: UTF-16 on Windows, a byte on Unix.
#[cfg(unix)]
pub(crate) type Unit = u8;

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
    memory: Arc<BytePool>,
    control_memory: Arc<BytePool>,
    outbound_limit: usize,
}

fn default_outbound_limit() -> usize {
    std::env::var("FASTEXEC_TEST_OUTBOUND_LIMIT")
        .ok()
        .and_then(|raw| raw.parse::<usize>().ok())
        .filter(|limit| {
            cfg!(debug_assertions) && (1024..=crate::lease::FRAME_LIMIT).contains(limit)
        })
        .unwrap_or(crate::lease::FRAME_LIMIT)
}

impl LaunchContext {
    pub fn new(cwd: PathBuf, env: Vec<(OsString, OsString)>) -> Self {
        Self {
            session_cwd: cwd.clone(),
            cwd,
            env,
            removed: Vec::new(),
            cache: Arc::new(LaunchCache::default()),
            memory: Arc::clone(lease::process_pool()),
            control_memory: Arc::clone(lease::control_pool()),
            outbound_limit: default_outbound_limit(),
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
            memory: Arc::clone(&self.memory),
            control_memory: Arc::clone(&self.control_memory),
            outbound_limit: self.outbound_limit,
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
            memory: Arc::clone(&self.memory),
            control_memory: Arc::clone(&self.control_memory),
            outbound_limit: self.outbound_limit,
        }
    }

    /// Attaches a shared active-byte pool (used by daemon sessions and unit tests).
    pub fn with_memory(mut self, memory: Arc<BytePool>) -> Self {
        self.memory = memory;
        self
    }

    pub fn with_control_memory(mut self, control_memory: Arc<BytePool>) -> Self {
        self.control_memory = control_memory;
        self
    }

    #[cfg(test)]
    pub fn with_outbound_limit(mut self, outbound_limit: usize) -> Self {
        self.outbound_limit = outbound_limit;
        self
    }

    pub fn memory(&self) -> &Arc<BytePool> {
        &self.memory
    }

    pub fn control_memory(&self) -> &Arc<BytePool> {
        &self.control_memory
    }

    pub fn outbound_limit(&self) -> usize {
        self.outbound_limit
    }

    pub fn image_compress(&self) -> Result<bool, String> {
        parse_image_compress(self.var(IMAGE_COMPRESS))
    }

    pub fn max_media_bytes(&self) -> Result<u64, String> {
        parse_max_media_bytes(self.var(MAX_MEDIA_BYTES))
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

    /// Borrowed source bytes that resolving this request can copy or expand.
    pub(crate) fn path_source_bytes(&self, path: &str) -> usize {
        let mut bytes = path
            .len()
            .saturating_add(self.cwd.as_os_str().as_encoded_bytes().len());
        let home =
            path == "~" || path.starts_with("~/") || cfg!(windows) && path.starts_with("~\\");
        if home {
            bytes = bytes.saturating_add(self.session_cwd.as_os_str().as_encoded_bytes().len());
            let value = |name| self.var(name).filter(|value| !value.is_empty());
            let home_bytes = if cfg!(windows) && value("USERPROFILE").is_some() {
                value("USERPROFILE").unwrap().as_encoded_bytes().len()
            } else if cfg!(windows) && value("HOMEDRIVE").is_some() && value("HOMEPATH").is_some() {
                value("HOMEDRIVE")
                    .unwrap()
                    .as_encoded_bytes()
                    .len()
                    .saturating_add(value("HOMEPATH").unwrap().as_encoded_bytes().len())
            } else {
                value("HOME").map_or(0, |value| value.as_encoded_bytes().len())
            };
            bytes = bytes.saturating_add(home_bytes);
        }
        bytes
    }

    /// The initial directory of the session, unaffected by `with_cwd`.
    pub(crate) fn session_cwd(&self) -> &Path {
        &self.session_cwd
    }

    /// The home directory this context's environment names: `USERPROFILE`, else `HOMEDRIVE` with
    /// `HOMEPATH`, else `HOME` on Windows, and `HOME` on Unix. An empty value is a missing one.
    pub(crate) fn raw_home(&self) -> Option<PathBuf> {
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

    /// The bash these tasks run, found once per session cache and retried after failure.
    pub(crate) fn cached_bash(
        &self,
        probe: impl FnOnce(&Self) -> Result<PathBuf, String>,
    ) -> Result<PathBuf, String> {
        self.cache.bash_lookup().get_or_init(|| probe(self)).clone()
    }

    /// Returns `(base_env, used_snapshot, one_time_warning)` for a task launch.
    pub(crate) fn cached_task_env(
        &self,
        login: bool,
        probe: impl FnOnce(&Self) -> Result<Vec<(OsString, OsString)>, String>,
    ) -> (&[(OsString, OsString)], bool, Option<ProbeWarning>) {
        if login {
            return (&self.env, false, None);
        }
        let mut reserved = None;
        let entry = self.cache.login_env.get_or_init(|| match probe(self) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        let ctx = context(&[]);
        let ready = std::sync::Barrier::new(4);
        let attempts = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    let lookup = ctx.cache.bash_lookup();
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

        let expected = PathBuf::from("/recovered/bash");
        let found = ctx
            .cached_bash(|_| {
                attempts.fetch_add(1, Ordering::Relaxed);
                Ok(expected.clone())
            })
            .unwrap();
        assert_eq!(found, expected);
        assert_eq!(attempts.load(Ordering::Relaxed), 2);

        // Subsequent calls and `with_cwd` derivations reuse the cached bash without re-probing.
        let nested = ctx.with_cwd(std::env::temp_dir().join("sub"));
        assert_eq!(
            nested
                .cached_bash(|_| panic!("cached bash must not re-probe"))
                .unwrap(),
            expected
        );
        assert_eq!(
            ctx.for_session()
                .cached_bash(|_| Ok(PathBuf::from("/fresh/session/bash")))
                .unwrap(),
            PathBuf::from("/fresh/session/bash")
        );
    }

    #[test]
    fn probe_warning_rolls_back_on_cancel_and_snapshot_filters_managed_and_removed_vars() {
        let ctx = context(&[("BASE", "1"), ("FASTEXEC_HTTP_TOKEN", "secret")])
            .without("FASTEXEC_HTTP_TOKEN");
        let probes = AtomicUsize::new(0);

        // `login: true` bypasses the login snapshot cache completely.
        let (login_env, used, warn) =
            ctx.cached_task_env(true, |_| panic!("login shell must not probe snapshot"));
        assert_eq!(login_env.len(), 1);
        assert!(!used);
        assert!(warn.is_none());

        // First non-login call probes and fails; dropping `ProbeWarning` before `message()` rolls back.
        let (fallback, used, first_warn) = ctx.cached_task_env(false, |_| {
            probes.fetch_add(1, Ordering::Relaxed);
            Err("profile hung".into())
        });
        assert_eq!(probes.load(Ordering::Relaxed), 1);
        assert_eq!(fallback.len(), 1);
        assert!(!used);
        drop(first_warn.expect("failed probe returns warning handle"));

        // Second call sees the rolled-back warning without re-running the probe.
        let (_, _, second_warn) = ctx.cached_task_env(false, |_| {
            panic!("failed login probe is cached per session")
        });
        let mut second_warn = second_warn.unwrap();
        assert!(
            second_warn
                .message()
                .is_some_and(|msg| msg.contains("profile hung"))
        );

        // While `second_warn` holds the reservation, another concurrent start sees `None`.
        let (_, _, concurrent_warn) =
            ctx.cached_task_env(false, |_| unreachable!("already initialized"));
        let mut concurrent_warn = concurrent_warn.unwrap();
        assert!(concurrent_warn.message().is_none());
        drop(concurrent_warn);

        // Dropping `second_warn` without `commit()` (cancelled/refused start) restores the warning.
        drop(second_warn);
        let (_, _, third_warn) =
            ctx.cached_task_env(false, |_| unreachable!("already initialized"));
        let mut third_warn = third_warn.unwrap();
        assert!(
            third_warn
                .message()
                .is_some_and(|msg| msg.contains("profile hung"))
        );
        third_warn.commit();

        // Once committed, no later call repeats the warning.
        let (_, _, after_commit) =
            ctx.cached_task_env(false, |_| unreachable!("already initialized"));
        assert!(after_commit.unwrap().message().is_none());

        // A fresh session snapshot strips shell-managed variables and `without()` secrets.
        let snapshot_ctx =
            context(&[("FASTEXEC_HTTP_TOKEN", "secret")]).without("FASTEXEC_HTTP_TOKEN");
        let (snap, used_snap, snap_warn) = snapshot_ctx.cached_task_env(false, |_| {
            Ok(vec![
                ("PATH".into(), "/custom/bin".into()),
                ("PWD".into(), "/tmp/probe".into()),
                ("OLDPWD".into(), "/tmp/old".into()),
                ("SHLVL".into(), "2".into()),
                ("_".into(), "/bin/bash".into()),
                ("SHELLOPTS".into(), "braceexpand".into()),
                ("BASHOPTS".into(), "checkwinsize".into()),
                ("FASTEXEC_HTTP_TOKEN".into(), "leaked-from-profile".into()),
            ])
        });
        assert!(used_snap);
        assert!(snap_warn.is_none());
        assert_eq!(snap, &[("PATH".into(), "/custom/bin".into())]);
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
    fn raw_home_follows_the_platforms_variable_order() {
        if cfg!(windows) {
            let all = context(&[
                ("USERPROFILE", r"C:\Users\profile"),
                ("HOMEDRIVE", "D:"),
                ("HOMEPATH", r"\Users\pair"),
                ("HOME", "/home/third"),
            ]);
            assert_eq!(all.raw_home().unwrap(), PathBuf::from(r"C:\Users\profile"));
            let pair = context(&[
                ("USERPROFILE", ""),
                ("HOMEDRIVE", "D:"),
                ("HOMEPATH", r"\Users\pair"),
                ("HOME", "/home/third"),
            ]);
            assert_eq!(pair.raw_home().unwrap(), PathBuf::from(r"D:\Users\pair"));
            let half = context(&[("HOMEDRIVE", "D:"), ("HOME", "/d/home/third")]);
            assert_eq!(half.raw_home().unwrap(), PathBuf::from("/d/home/third"));
        } else {
            assert_eq!(
                context(&[("HOME", "/home/me"), ("USERPROFILE", "/ignored")]).raw_home(),
                Some(PathBuf::from("/home/me"))
            );
            assert_eq!(
                context(&[("HOME", ""), ("USERPROFILE", "/ignored")]).raw_home(),
                None
            );
        }
        assert_eq!(context(&[]).raw_home(), None);
    }

    #[test]
    fn media_env_settings_validate_bounds_and_per_session_context() {
        assert_eq!(parse_image_compress(None), Ok(false));
        assert_eq!(parse_image_compress(Some(OsStr::new("false"))), Ok(false));
        assert_eq!(parse_image_compress(Some(OsStr::new("true"))), Ok(true));
        for bad in ["", "1", "0", "on", "off", "TRUE", " true "] {
            let err = parse_image_compress(Some(OsStr::new(bad))).unwrap_err();
            assert!(err.contains(IMAGE_COMPRESS), "{err}");
        }

        assert_eq!(parse_max_media_bytes(None), Ok(DEFAULT_MAX_MEDIA_BYTES));
        assert_eq!(
            parse_max_media_bytes(Some(OsStr::new("1048576"))),
            Ok(MIN_MAX_MEDIA_BYTES)
        );
        assert_eq!(
            parse_max_media_bytes(Some(OsStr::new("377487360"))),
            Ok(MAX_MAX_MEDIA_BYTES)
        );
        for bad in [
            "",
            "0",
            "1048575",
            "377487361",
            "+1048576",
            "-1048576",
            " 1048576",
            "100MiB",
            "18446744073709551616",
        ] {
            let err = parse_max_media_bytes(Some(OsStr::new(bad))).unwrap_err();
            assert!(err.contains(MAX_MEDIA_BYTES), "{err}");
        }

        let default_ctx = context(&[]);
        assert_eq!(default_ctx.image_compress(), Ok(false));
        assert_eq!(default_ctx.max_media_bytes(), Ok(DEFAULT_MAX_MEDIA_BYTES));

        let custom_ctx = context(&[(IMAGE_COMPRESS, "true"), (MAX_MEDIA_BYTES, "2097152")]);
        assert_eq!(custom_ctx.image_compress(), Ok(true));
        assert_eq!(custom_ctx.max_media_bytes(), Ok(2_097_152));
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
