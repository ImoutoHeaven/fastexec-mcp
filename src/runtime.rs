//! Who this server is and where its daemon keeps its state: the build ID, the namespace, the
//! runtime directory and its endpoint, the locks that serialize starters, and the cached image a
//! daemon is started from.
#![cfg_attr(not(test), allow(dead_code))]

use crate::session::LaunchContext;
use crate::transport;
use sha2::{Digest, Sha256};
use std::ffi::OsString;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

/// How long a start waits for a daemon to answer, from the first connect to the last.
pub const START_WAIT: Duration = Duration::from_secs(10);
/// How often a waiting start looks again.
const POLL: Duration = Duration::from_millis(50);
/// A cached image older than this, and not the one in use, is swept.
#[cfg(any(windows, target_os = "linux"))]
const CACHE_AGE: Duration = Duration::from_secs(60);
/// How many bytes of the image are hashed at a time.
const HASH_CHUNK: usize = 1 << 20;
/// The lock file that serializes starters over the image cache, which the cache never deletes.
pub const CACHE_LOCK: &str = "cache.lock";
/// The lock files a namespace directory keeps, which a sweep never deletes.
pub const STARTUP_LOCK: &str = "startup.lock";
pub const DAEMON_LOCK: &str = "daemon.lock";

/// What a daemon is known by: the image it runs and the namespace it serves.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub build: String,
    pub namespace: String,
}

/// Where a namespace keeps its state.
#[derive(Clone, Debug)]
pub struct Paths {
    pub root: PathBuf,
    pub ns: PathBuf,
    pub endpoint: OsString,
}

/// The running image's path: the kernel's own view of it on Linux, so a replaced file on disk
/// does not change what is hashed.
pub fn image_path() -> io::Result<PathBuf> {
    #[cfg(target_os = "linux")]
    return Ok(PathBuf::from("/proc/self/exe"));
    #[cfg(not(target_os = "linux"))]
    std::env::current_exe()
}

/// The lowercase hex SHA-256 of this running image.
pub fn build_id() -> io::Result<String> {
    build_id_of(&image_path()?)
}

/// The namespace one build and scope share: the first 16 hex characters of their SHA-256.
fn namespace_of(build: &str, scope: &[u8]) -> String {
    let full = digest(&[build.as_bytes(), b"\0", scope]);
    full[..16].to_owned()
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn digest(parts: &[&[u8]]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    hex(hasher.finalize().as_slice())
}

/// This image's build, and the namespace its daemon serves: `FASTEXEC_NAMESPACE` when the launch
/// context has one, else the canonical path of the image, so two copies of one image share a
/// daemon only when asked to.
pub fn identity(launch: &LaunchContext) -> io::Result<Identity> {
    let build = build_id()?;
    // The scope is hashed as the OS bytes it is, so two names that only differ outside UTF-8
    // still get a namespace each.
    let scope: Vec<u8> = match launch.var("FASTEXEC_NAMESPACE") {
        Some(value) if !value.is_empty() => value.as_encoded_bytes().to_vec(),
        _ => std::fs::canonicalize(image_path()?)?
            .as_os_str()
            .as_encoded_bytes()
            .to_vec(),
    };
    Ok(Identity {
        build: build.clone(),
        namespace: namespace_of(&build, &scope),
    })
}

/// The runtime directory of this user, created private.
fn root() -> io::Result<PathBuf> {
    let root = match test_root() {
        Some(dir) => dir,
        None => platform_root()?,
    };
    create_private(&root)?;
    Ok(root)
}

/// Tests point the runtime at a directory of their own; a release build never does.
fn test_root() -> Option<PathBuf> {
    if !cfg!(debug_assertions) {
        return None;
    }
    std::env::var_os("FASTEXEC_TEST_RUNTIME_DIR")
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
}

#[cfg(windows)]
fn platform_root() -> io::Result<PathBuf> {
    let local = std::env::var_os("LOCALAPPDATA").ok_or_else(|| {
        io::Error::other("LOCALAPPDATA is not set, so there is no runtime directory to use")
    })?;
    Ok(PathBuf::from(local).join("fastexec"))
}

#[cfg(unix)]
fn platform_root() -> io::Result<PathBuf> {
    match std::env::var_os("XDG_RUNTIME_DIR").filter(|dir| !dir.is_empty()) {
        Some(dir) => Ok(PathBuf::from(dir).join("fastexec")),
        None => Ok(std::env::temp_dir().join(format!("fastexec-{}", user_id()))),
    }
}

#[cfg(unix)]
fn user_id() -> u32 {
    // SAFETY: geteuid has no preconditions and cannot fail.
    unsafe { libc::geteuid() }
}

/// Creates `dir`, private to this user, and checks that what is there is a directory this user
/// owns: mode 0700 and no symlink on Unix, an owner-only DACL on Windows when it is created.
fn create_private(dir: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        // A directory that is already there is used as it is: the profile directory this sits in
        // is already private to the user.
        if dir.exists() {
            return Ok(());
        }
        // The parents are made first; only the last component gets the owner-only DACL.
        if let Some(parent) = dir.parent() {
            std::fs::create_dir_all(parent)?;
        }
        create_private_directory(dir)
    }
    #[cfg(unix)]
    {
        #[cfg_attr(not(unix), allow(unused_mut))]
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
        builder.create(dir)?;
        create_private_unix_check(dir)
    }
}

/// Creates `dir` with an owner-only DACL whose ACE its children inherit.
#[cfg(windows)]
fn create_private_directory(dir: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::CreateDirectoryW;

    let wide: Vec<u16> = dir
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let attributes = crate::transport::SecurityAttributes::owner_only(true)?;
    let created = attributes.with_raw(|raw| {
        // SAFETY: the path is NUL-terminated and `raw` points at valid attributes for the call.
        unsafe {
            CreateDirectoryW(
                wide.as_ptr(),
                raw.cast::<windows_sys::Win32::Security::SECURITY_ATTRIBUTES>()
                    .cast_const(),
            )
        }
    });
    if created != 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    // Another starter may have created it between the check and the call.
    if dir.exists() {
        return Ok(());
    }
    Err(error)
}

/// Checks that what is at `dir` is a real directory this user owns with mode 0700.
#[cfg(unix)]
fn create_private_unix_check(dir: &Path) -> io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    let meta = std::fs::symlink_metadata(dir)?;
    if !meta.is_dir() {
        return Err(io::Error::other(format!(
            "{} is not a directory this user owns with mode 0700",
            dir.display()
        )));
    }
    if meta.uid() != user_id() {
        return Err(io::Error::other(format!(
            "{} is not owned by this user",
            dir.display()
        )));
    }
    if meta.permissions().mode() & 0o777 != 0o700 {
        return Err(io::Error::other(format!(
            "{} is not mode 0700",
            dir.display()
        )));
    }
    Ok(())
}

/// A namespace is exactly 16 lowercase hex characters, which is what an identity produces and
/// what a sweep looks for.
pub fn valid_namespace(namespace: &str) -> bool {
    namespace.len() == 16
        && namespace
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// The runtime paths of `namespace`, with the namespace directory created.
pub fn paths(namespace: &str) -> io::Result<Paths> {
    if !valid_namespace(namespace) {
        return Err(io::Error::other(format!(
            "{namespace:?} is not a namespace: 16 lowercase hex characters"
        )));
    }
    let root = root()?;
    let ns = root.join("ns").join(namespace);
    create_private(&ns)?;
    let endpoint = endpoint(&ns, namespace)?;
    Ok(Paths { root, ns, endpoint })
}

#[cfg(windows)]
fn endpoint(_ns: &Path, namespace: &str) -> io::Result<OsString> {
    // The name carries this user's SID, so another user's daemon cannot be reached by guessing a
    // namespace: the pipe's DACL is the access rule, and the name keeps them apart.
    let sid = transport::user_sid()?;
    let user = digest(&[sid.as_bytes()]);
    Ok(OsString::from(format!(
        r"\\.\pipe\fastexec-{}-{namespace}",
        &user[..16]
    )))
}

#[cfg(unix)]
fn endpoint(ns: &Path, namespace: &str) -> io::Result<OsString> {
    let path = ns.join("sock");
    // A Unix socket path is limited to about 108 bytes, so a deep runtime directory falls back to
    // a short one that is private in the same way.
    if path.as_os_str().len() <= 100 {
        return Ok(path.into_os_string());
    }
    // The literal /tmp, not the temporary directory of the environment: this path exists to be
    // short, and a long TMPDIR would defeat that.
    let dir = std::path::Path::new("/tmp").join(format!("fastexec-{}", user_id()));
    create_private(&dir)?;
    Ok(dir.join(format!("{namespace}.sock")).into_os_string())
}

/// A held file lock: dropping it releases the lock, and the file it holds is what keeps it.
pub struct Lock(#[expect(dead_code, reason = "the file is the lock")] std::fs::File);

impl Lock {
    /// Takes the lock at `path`, or returns `None` when another process holds it. The file is
    /// created if it is missing and never deleted, so the lock always has one inode.
    pub fn try_lock(path: &Path) -> io::Result<Option<Lock>> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)?;
        match file.try_lock() {
            Ok(()) => Ok(Some(Lock(file))),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(error)) => Err(error),
        }
    }
}

/// The cached copy of the image a daemon is started from, named by its build. It is written once,
/// to a temporary name and then renamed, so a half-written copy is never started.
pub fn cached_exe(root: &Path, build: &str, deadline: Instant) -> io::Result<PathBuf> {
    let dir = root.join("bin");
    std::fs::create_dir_all(&dir)?;
    // One starter at a time touches the cache: publishing an image, marking it used, and sweeping
    // older ones must not interleave. The lock file stays, so every starter locks one inode.
    let _cache_lock = loop {
        match Lock::try_lock(&dir.join(CACHE_LOCK))? {
            Some(lock) => break lock,
            None if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(50));
            }
            None => {
                return Err(io::Error::other(
                    "the image cache is held by another starter",
                ));
            }
        }
    };
    let target = dir.join(format!("{build}{}", std::env::consts::EXE_SUFFIX));
    if !target.exists() {
        let image = image_path()?;
        let temp = dir.join(format!("{}.tmp", std::process::id()));
        std::fs::copy(&image, &temp)?;
        if build_id_of(&temp)? != build {
            let _ = std::fs::remove_file(&temp);
            return Err(io::Error::other(format!(
                "{} is not build {build}",
                image.display()
            )));
        }
        if std::fs::rename(&temp, &target).is_err() {
            let _ = std::fs::remove_file(&temp);
            if !target.exists() {
                return Err(io::Error::other(format!(
                    "cannot place the cached image at {}",
                    target.display()
                )));
            }
        }
    }
    // The target is now the newest image in the cache, so another starter's cleanup leaves it.
    touch(&target)?;
    #[cfg(any(windows, target_os = "linux"))]
    sweep_cache(&dir, &target);
    Ok(target)
}

/// Marks `path` as just used. A daemon may already have the image mapped, and a mapped image
/// rejects an open that asks to write its data, so on Windows only its attributes are opened.
fn touch(path: &Path) -> io::Result<()> {
    let now = std::time::SystemTime::now();
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_WRITE_ATTRIBUTES;

        let file = std::fs::OpenOptions::new()
            .access_mode(FILE_WRITE_ATTRIBUTES)
            .open(path)?;
        file.set_modified(now)
    }
    #[cfg(not(windows))]
    {
        // A running image cannot be opened for writing, but its owner may still set its times.
        let file = std::fs::File::open(path)?;
        file.set_modified(now)
    }
}

fn build_id_of(path: &Path) -> io::Result<String> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; HASH_CHUNK];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex(hasher.finalize().as_slice()))
}

/// Deletes images in `dir` that are not `keep` and are old enough that no starter can still be
/// reading them. macOS keeps every image: there a daemon execs its task host by pathname
/// (`current_exe`), so unlinking a running old image breaks its later task starts.
#[cfg(any(windows, target_os = "linux"))]
fn sweep_cache(dir: &Path, keep: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path == keep || path.file_name().is_some_and(|name| name == CACHE_LOCK) {
            continue;
        }
        let old = entry
            .metadata()
            .and_then(|meta| meta.modified())
            .is_ok_and(|modified| modified.elapsed().is_ok_and(|age| age > CACHE_AGE));
        if old {
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// Deletes the state of every namespace that no daemon and no starter holds. A namespace whose
/// locks are held is skipped: its daemon is running or starting.
pub fn sweep(root: &Path, own_namespace: &str) {
    let Ok(entries) = std::fs::read_dir(root.join("ns")) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        // Only what an identity can produce is looked at, and only if it is a real directory.
        if name == own_namespace || !valid_namespace(name) {
            continue;
        }
        let Ok(meta) = std::fs::symlink_metadata(entry.path()) else {
            continue;
        };
        if !meta.is_dir() {
            continue;
        }
        let dir = entry.path();
        let Ok(Some(_startup)) = Lock::try_lock(&dir.join(STARTUP_LOCK)) else {
            continue;
        };
        let Ok(Some(_daemon)) = Lock::try_lock(&dir.join(DAEMON_LOCK)) else {
            continue;
        };
        // A namespace whose endpoint sits in the short fallback directory keeps it outside `ns`,
        // so deleting the namespace's own entries would leave it behind.
        #[cfg(unix)]
        {
            let fallback = std::path::Path::new("/tmp")
                .join(format!("fastexec-{}", user_id()))
                .join(format!("{name}.sock"));
            let _ = std::fs::remove_file(fallback);
        }
        let Ok(files) = std::fs::read_dir(&dir) else {
            continue;
        };
        for file in files.flatten() {
            let name = file.file_name();
            if name == std::ffi::OsStr::new(STARTUP_LOCK)
                || name == std::ffi::OsStr::new(DAEMON_LOCK)
            {
                continue;
            }
            let path = file.path();
            let _ = if file.file_type().is_ok_and(|kind| kind.is_dir()) {
                std::fs::remove_dir_all(&path)
            } else {
                std::fs::remove_file(&path)
            };
        }
    }
}

/// Connects to the namespace's daemon, starting one if none answers. The returned lock, when
/// there is one, keeps the start exclusive until the caller has a `Welcome`.
pub async fn connect_or_start(
    identity: &Identity,
    paths: &Paths,
    deadline: Instant,
    extra_args: &[OsString],
) -> Result<(transport::Client, Option<Lock>), String> {
    match connect_until(&paths.endpoint, deadline).await {
        Ok(client) => return Ok((client, None)),
        Err(error) if absent(&error) => {}
        Err(error) => {
            return Err(format!(
                "cannot connect to {}: {error}",
                endpoint_display(paths)
            ));
        }
    }
    // One starter at a time per namespace: the lock is taken here and kept until the caller has
    // its Welcome.
    let startup = take_startup(paths, deadline).await?;
    match connect_until(&paths.endpoint, deadline).await {
        Ok(client) => return Ok((client, Some(startup))),
        Err(error) if absent(&error) => {}
        Err(error) => {
            return Err(format!(
                "cannot connect to {}: {error}",
                endpoint_display(paths)
            ));
        }
    }
    // Everything that touches the cache and starts a process runs on a blocking worker, so the
    // async side holds no state while it does. A worker that outlives the deadline is abandoned:
    // the loop below still ends at the deadline.
    let root = paths.root.clone();
    let namespace = identity.namespace.clone();
    let build = identity.build.clone();
    let extra = extra_args.to_vec();
    let start = tokio::task::spawn_blocking(move || -> Result<(), String> {
        let exe = cached_exe(&root, &build, deadline)
            .map_err(|error| format!("cannot use the cached image: {error}"))?;
        // A daemon started after the deadline would be one nobody waits for.
        if Instant::now() >= deadline {
            return Err(format!(
                "the daemon for namespace {namespace} was not started in time"
            ));
        }
        spawn_daemon(&exe, &namespace, &build, &extra)
    });
    match tokio::time::timeout_at(deadline.into(), start).await {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(message))) => return Err(message),
        Ok(Err(error)) => return Err(format!("the daemon start did not run: {error}")),
        // Abandoned: the connect loop below ends at the deadline.
        Err(_) => {}
    }
    loop {
        match connect_until(&paths.endpoint, deadline).await {
            Ok(client) => return Ok((client, Some(startup))),
            Err(error) if absent(&error) => {}
            Err(error) => {
                return Err(format!(
                    "cannot connect to {}: {error}",
                    endpoint_display(paths)
                ));
            }
        }
        if Instant::now() >= deadline {
            return Err(format!(
                "the daemon for namespace {} did not answer within {} s; see {}",
                identity.namespace,
                START_WAIT.as_secs(),
                paths.ns.join("daemon.log").display()
            ));
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Whether a connect error says that nothing is listening, rather than that the connection was
/// refused for another reason.
pub fn absent(error: &io::Error) -> bool {
    #[cfg(windows)]
    {
        // A pipe name no instance is listening on is `ERROR_FILE_NOT_FOUND`, and no other error
        // means that: a path that is missing for another reason is a failure.
        error.raw_os_error() == Some(2)
    }
    #[cfg(unix)]
    {
        matches!(
            error.kind(),
            io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
        )
    }
}

/// Connects, giving up at the shared deadline: a start that has run out of time does not begin
/// another connect that cannot finish.
async fn connect_until(
    endpoint: &std::ffi::OsStr,
    deadline: Instant,
) -> io::Result<transport::Client> {
    match tokio::time::timeout_at(deadline.into(), transport::connect(endpoint)).await {
        Ok(result) => result,
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "the start ran out of time",
        )),
    }
}

fn endpoint_display(paths: &Paths) -> String {
    std::path::Path::new(&paths.endpoint).display().to_string()
}

/// Waits for the namespace's startup lock, so two endpoints do not start two daemons. The
/// waiting is file work, so it runs on a blocking worker and the caller waits under the deadline.
async fn take_startup(paths: &Paths, deadline: Instant) -> Result<Lock, String> {
    let path = paths.ns.join(STARTUP_LOCK);
    let waiting = tokio::task::spawn_blocking(move || -> Result<Lock, String> {
        loop {
            match Lock::try_lock(&path) {
                Ok(Some(lock)) => return Ok(lock),
                Ok(None) if Instant::now() < deadline => std::thread::sleep(POLL),
                Ok(None) => {
                    return Err(format!(
                        "another fastexec is already starting a daemon for namespace {}",
                        path.display()
                    ));
                }
                Err(error) => {
                    return Err(format!("cannot take {}: {error}", path.display()));
                }
            }
        }
    });
    match tokio::time::timeout_at(deadline.into(), waiting).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(format!("the start lock did not run: {error}")),
        // The worker is abandoned; the caller's own deadline ends the start.
        Err(_) => Err("another fastexec is already starting a daemon".to_owned()),
    }
}

/// Whether this process is in a Job object, or the error of not being able to ask. A daemon
/// started from one would die with it, so a caller that cannot ask treats it as a yes.
#[cfg(windows)]
pub fn in_job() -> io::Result<bool> {
    use windows_sys::Win32::System::JobObjects::IsProcessInJob;
    use windows_sys::Win32::System::Threading::GetCurrentProcess;

    let mut inside = 0;
    // SAFETY: the current-process pseudo handle is valid, and a null job asks about any job.
    let asked = unsafe { IsProcessInJob(GetCurrentProcess(), std::ptr::null_mut(), &mut inside) };
    if asked == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(inside != 0)
}

#[cfg(unix)]
pub fn in_job() -> io::Result<bool> {
    Ok(false)
}

/// The error a start reports when the daemon cannot leave the Job it was started in.
#[cfg(windows)]
pub const JOB_REFUSAL: &str = "The daemon could not leave this host's Job; run `fastexec daemon start` from a terminal outside the host.";

#[cfg(windows)]
fn spawn_daemon(
    exe: &Path,
    namespace: &str,
    build: &str,
    extra: &[OsString],
) -> Result<(), String> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_ACCESS_DENIED};
    use windows_sys::Win32::System::JobObjects::IsProcessInJob;
    use windows_sys::Win32::System::Threading::{
        CREATE_BREAKAWAY_FROM_JOB, CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW, CreateProcessW,
        DETACHED_PROCESS, PROCESS_INFORMATION, STARTUPINFOW, TerminateProcess, WaitForSingleObject,
    };

    let mut args: Vec<OsString> = vec![
        "daemon".into(),
        "serve".into(),
        namespace.into(),
        build.into(),
    ];
    args.extend(extra.iter().cloned());
    let mut command_line = crate::conpty_spawn::command_line_of(exe.as_os_str(), &args)
        .map_err(|error| format!("cannot build the daemon command line: {error}"))?;
    let application: Vec<u16> = exe
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    let startup = STARTUPINFOW {
        cb: std::mem::size_of::<STARTUPINFOW>() as u32,
        ..STARTUPINFOW::default()
    };
    let mut process: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };
    // A daemon must not die with its starter, so it leaves the Job when there is one; the flags
    // also give it no console and its own process group.
    let mut flags = DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW;
    // A question that cannot be answered refuses here, before a child exists: guessing would
    // start a daemon that may die with its starter.
    match in_job() {
        Ok(true) => flags |= CREATE_BREAKAWAY_FROM_JOB,
        Ok(false) => {}
        Err(_) => return Err(JOB_REFUSAL.to_owned()),
    }
    // SAFETY: the command line and application name are NUL-terminated buffers that outlive the
    // call, and `startup` and `process` are sized as the call expects.
    let created = unsafe {
        CreateProcessW(
            application.as_ptr(),
            command_line.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            0,
            flags,
            std::ptr::null(),
            std::ptr::null(),
            &startup,
            &mut process,
        )
    };
    if created == 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(ERROR_ACCESS_DENIED as i32) {
            return Err(JOB_REFUSAL.to_owned());
        }
        return Err(format!("cannot start the daemon: {error}"));
    }
    // The child must be outside any Job, or it would die with this one.
    let mut child_inside = 0;
    // SAFETY: the child handle is alive for the call, and a null job asks about any job.
    let asked =
        unsafe { IsProcessInJob(process.hProcess, std::ptr::null_mut(), &mut child_inside) };
    // A child that is in a Job, or a question that could not be answered, is ended here rather
    // than left to die with its starter later.
    if asked == 0 || child_inside != 0 {
        // SAFETY: both handles came from CreateProcessW; the process is ended, waited for, and
        // both handles are closed once.
        unsafe {
            TerminateProcess(process.hProcess, 1);
            WaitForSingleObject(process.hProcess, 5000);
            CloseHandle(process.hProcess);
            CloseHandle(process.hThread);
        }
        return Err(JOB_REFUSAL.to_owned());
    }
    // SAFETY: both handles came from CreateProcessW and are closed once.
    unsafe {
        CloseHandle(process.hProcess);
        CloseHandle(process.hThread);
    }
    Ok(())
}

#[cfg(unix)]
fn spawn_daemon(
    exe: &Path,
    namespace: &str,
    build: &str,
    extra: &[OsString],
) -> Result<(), String> {
    use std::os::unix::process::CommandExt;

    let mut command = std::process::Command::new(exe);
    command
        .arg("daemon")
        .arg("serve")
        .arg(namespace)
        .arg(build)
        .args(extra)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    // SAFETY: the closure runs between fork and exec, where only async-signal-safe calls are
    // allowed; setsid and the error read are both safe there.
    unsafe {
        command.pre_exec(|| {
            // A daemon that cannot leave the starter's session would take its signals, so it does
            // not start at all.
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = command
        .spawn()
        .map_err(|error| format!("cannot start the daemon: {error}"))?;
    // Nothing waits for a daemon, and an unwaited child would linger as a zombie.
    std::thread::spawn(move || {
        let _ = child.wait();
    });
    Ok(())
}

/// Points this process's standard error at `path`, truncated. The daemon's own errors, and those
/// of the tasks it runs, end up there.
pub fn redirect_stderr(path: &Path) -> io::Result<()> {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(path)?;
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::System::Console::{STD_ERROR_HANDLE, SetStdHandle};

        let handle = file.as_raw_handle() as windows_sys::Win32::Foundation::HANDLE;
        // SAFETY: the handle stays valid for the process's life, because the file is kept.
        if unsafe { SetStdHandle(STD_ERROR_HANDLE, handle) } == 0 {
            return Err(io::Error::last_os_error());
        }
        std::mem::forget(file);
    }
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;

        // SAFETY: both descriptors are open, and dup2 closes the previous standard error.
        if unsafe { libc::dup2(file.as_raw_fd(), 2) } < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn launch_with(namespace: Option<&str>) -> LaunchContext {
        let mut env: Vec<(OsString, OsString)> = Vec::new();
        if let Some(namespace) = namespace {
            env.push(("FASTEXEC_NAMESPACE".into(), namespace.into()));
        }
        LaunchContext::new(std::env::temp_dir(), env)
    }

    #[test]
    fn a_namespace_follows_the_scope_and_the_build() {
        let first = identity(&launch_with(Some("alpha"))).unwrap();
        let same = identity(&launch_with(Some("alpha"))).unwrap();
        let other = identity(&launch_with(Some("beta"))).unwrap();
        assert_eq!(first, same, "the same scope gave a different namespace");
        assert_ne!(
            first.namespace, other.namespace,
            "two scopes share one namespace"
        );
        assert_eq!(first.namespace.len(), 16, "{}", first.namespace);
        assert!(
            first.namespace.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "{}",
            first.namespace
        );
        // The build is part of the name, so an image that changed gets its own daemon.
        let build = first.build.clone();
        assert_eq!(build.len(), 64, "{build}");
        assert_eq!(namespace_of(&build, b"alpha"), first.namespace);
        assert_ne!(
            namespace_of(&build, b"alpha"),
            namespace_of("another-build", b"alpha"),
            "the build is not part of the namespace"
        );
        // Scope bytes are hashed as they are, even when they are not valid UTF-8.
        assert_ne!(
            namespace_of(&build, &[0xff, 0xfe]),
            namespace_of(&build, &[0xff, 0xfd])
        );
        // An empty variable is not a scope, so the image path decides.
        let empty = identity(&launch_with(Some(""))).unwrap();
        let none = identity(&launch_with(None)).unwrap();
        assert_eq!(empty, none, "an empty namespace was taken as a scope");
    }

    #[cfg(unix)]
    #[test]
    fn a_runtime_directory_that_others_can_reach_is_refused() {
        use std::os::unix::fs::PermissionsExt;

        let dir = std::env::temp_dir().join(format!("fastexec-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let refused = create_private(&dir);
        assert!(refused.is_err(), "a 0755 directory was accepted");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(create_private(&dir).is_ok(), "a 0700 directory was refused");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_namespace_is_sixteen_lowercase_hex_characters() {
        assert!(valid_namespace("0123456789abcdef"));
        assert!(!valid_namespace("0123456789abcde"), "too short");
        assert!(!valid_namespace("0123456789abcdef0"), "too long");
        assert!(!valid_namespace("0123456789ABCDEF"), "uppercase");
        assert!(!valid_namespace("0123456789abcdeg"), "not hex");
        assert!(paths("not-a-namespace").is_err(), "paths accepted a name");
    }

    #[test]
    fn a_lock_is_exclusive_until_it_is_dropped() {
        let dir = std::env::temp_dir().join(format!("fastexec-lock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(DAEMON_LOCK);
        let held = Lock::try_lock(&path).unwrap();
        assert!(held.is_some(), "the first lock was refused");
        assert!(
            Lock::try_lock(&path).unwrap().is_none(),
            "a second lock was granted"
        );
        drop(held);
        assert!(
            Lock::try_lock(&path).unwrap().is_some(),
            "the lock was not released"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
