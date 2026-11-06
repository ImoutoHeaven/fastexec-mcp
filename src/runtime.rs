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

/// Creates a private directory, validating an existing leaf instead of trusting its parent.
pub(crate) fn create_private(dir: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        if let Some(parent) = dir.parent() {
            std::fs::create_dir_all(parent)?;
        }
        match create_private_new(dir) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                create_private_windows_check(dir)
            }
            Err(error) => Err(error),
        }
    }
    #[cfg(target_os = "macos")]
    {
        private_macos::create_all(dir)
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let mut builder = std::fs::DirBuilder::new();
        builder.recursive(true);
        std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
        builder.create(dir)?;
        create_private_unix_check(dir)
    }
}

/// Atomically reserves a new private directory. Existing names, including links, are errors;
/// callers choosing random names can retry without ever adopting someone else's directory.
pub(crate) fn create_private_new(dir: &Path) -> io::Result<()> {
    #[cfg(windows)]
    {
        create_private_directory(dir)?;
        create_private_windows_check(dir)
    }
    #[cfg(target_os = "macos")]
    {
        private_macos::create_leaf(dir)?;
        create_private_unix_check(dir)
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let mut builder = std::fs::DirBuilder::new();
        std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
        builder.create(dir)?;
        create_private_unix_check(dir)
    }
}

/// Darwin ACL grants are evaluated independently of mode bits. Creating mode 0700 and then
/// removing an inherited ACL is too late: another user could already have opened the directory.
#[cfg(target_os = "macos")]
mod private_macos {
    use super::*;
    use std::ffi::{CString, c_void};
    use std::fs::File;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStrExt;

    // Public Darwin <sys/acl.h>/<sys/fcntl.h> APIs not currently exposed by Rust libc.
    const FILESEC_MODE: libc::c_int = 4;
    const FILESEC_ACL: libc::c_int = 5;
    const ACL_TYPE_EXTENDED: libc::c_int = 0x100;
    const ACL_FIRST_ENTRY: libc::c_int = 0;
    const ACL_FLAG_NO_INHERIT: libc::c_int = 1 << 17;

    unsafe extern "C" {
        fn acl_init(count: libc::c_int) -> *mut c_void;
        fn acl_free(object: *mut c_void) -> libc::c_int;
        fn acl_get_flagset_np(object: *mut c_void, flags: *mut *mut c_void) -> libc::c_int;
        fn acl_add_flag_np(flags: *mut c_void, flag: libc::c_int) -> libc::c_int;
        fn acl_get_fd_np(fd: libc::c_int, kind: libc::c_int) -> *mut c_void;
        fn acl_get_entry(
            acl: *mut c_void,
            entry_id: libc::c_int,
            entry: *mut *mut c_void,
        ) -> libc::c_int;
        fn filesec_init() -> *mut c_void;
        fn filesec_free(security: *mut c_void);
        fn filesec_set_property(
            security: *mut c_void,
            property: libc::c_int,
            value: *const c_void,
        ) -> libc::c_int;
        fn mkdirx_np(path: *const libc::c_char, security: *mut c_void) -> libc::c_int;
    }

    struct Acl(*mut c_void);
    impl Drop for Acl {
        fn drop(&mut self) {
            // SAFETY: this object was allocated by acl_init or acl_get_fd_np and is owned once.
            unsafe { acl_free(self.0) };
        }
    }

    struct Security(*mut c_void);
    impl Drop for Security {
        fn drop(&mut self) {
            // SAFETY: this filesec_init allocation and its copied ACL are owned once.
            unsafe { filesec_free(self.0) };
        }
    }

    pub(super) fn create_leaf(dir: &Path) -> io::Result<()> {
        let path = CString::new(dir.as_os_str().as_bytes())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        // SAFETY: zero requests an empty ACL; allocation failure is reported as null.
        let acl = unsafe { acl_init(0) };
        if acl.is_null() {
            return Err(io::Error::last_os_error());
        }
        let acl = Acl(acl);
        let mut flags = std::ptr::null_mut();
        // SAFETY: the live ACL owns the returned flagset; setting NO_INHERIT suppresses the
        // parent ACE merge inside the kernel's kauth_acl_inherit during mkdir itself.
        if unsafe { acl_get_flagset_np(acl.0, &mut flags) } != 0
            || unsafe { acl_add_flag_np(flags, ACL_FLAG_NO_INHERIT) } != 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: this API has no arguments and returns a newly allocated descriptor.
        let security = unsafe { filesec_init() };
        if security.is_null() {
            return Err(io::Error::last_os_error());
        }
        let security = Security(security);
        let mode: libc::mode_t = 0o700;
        // SAFETY: MODE takes a mode_t pointer and ACL takes an acl_t pointer. filesec copies the
        // ACL, so both RAII allocations remain independently owned throughout the mkdir call.
        if unsafe {
            filesec_set_property(security.0, FILESEC_MODE, std::ptr::addr_of!(mode).cast())
        } != 0
            || unsafe {
                filesec_set_property(security.0, FILESEC_ACL, std::ptr::addr_of!(acl.0).cast())
            } != 0
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the terminated path and complete mode/ACL descriptor live through the call.
        if unsafe { mkdirx_np(path.as_ptr(), security.0) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    // Existing ancestors keep normal path semantics, including a caller-selected parent
    // symlink. Every missing ancestor is created privately, just as the Unix recursive builder.
    fn parents(dir: &Path) -> io::Result<()> {
        match std::fs::metadata(dir) {
            Ok(metadata) if metadata.is_dir() => return Ok(()),
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::NotADirectory,
                    format!("{} is not a directory", dir.display()),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        if let Some(parent) = dir.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            parents(parent)?;
        }
        match super::create_private_new(dir) {
            Ok(()) => Ok(()),
            Err(error)
                if error.kind() == io::ErrorKind::AlreadyExists
                    && std::fs::metadata(dir).is_ok_and(|metadata| metadata.is_dir()) =>
            {
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn create_all(dir: &Path) -> io::Result<()> {
        if let Some(parent) = dir.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            parents(parent)?;
        }
        match super::create_private_new(dir) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                super::create_private_unix_check(dir)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn check_acl(file: &File, dir: &Path) -> io::Result<()> {
        let fd = file.as_raw_fd();
        let refuse = |reason: &str| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("{} is not a private directory: {reason}", dir.display()),
            )
        };
        // SAFETY: statfs is a plain C output structure and fd belongs to a live directory.
        let mut filesystem: libc::statfs = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstatfs(fd, &mut filesystem) } != 0 {
            return Err(io::Error::last_os_error());
        }
        if filesystem.f_flags & libc::MNT_IGNORE_OWNERSHIP as u32 != 0 {
            return Err(refuse("its volume ignores file ownership and permissions"));
        }
        // SAFETY: the descriptor is live. Reset errno to distinguish an indeterminate result
        // from a real error; neither is proof that ACL enforcement is absent.
        let extended = unsafe {
            *libc::__error() = 0;
            libc::fpathconf(fd, libc::_PC_EXTENDED_SECURITY_NP)
        };
        if extended == 0 {
            return Ok(());
        }
        if extended < 0 {
            return Err(refuse("extended ACL support could not be determined"));
        }
        // SAFETY: fd is live and ACL_TYPE_EXTENDED is the supported Darwin ACL flavor.
        let acl = unsafe { acl_get_fd_np(fd, ACL_TYPE_EXTENDED) };
        if acl.is_null() {
            let error = io::Error::last_os_error();
            // Darwin's fstatx/filesec reports ENOENT for an absent ACL on a live descriptor.
            return if error.raw_os_error() == Some(libc::ENOENT) {
                Ok(())
            } else {
                Err(error)
            };
        }
        let acl = Acl(acl);
        let mut entry = std::ptr::null_mut();
        // SAFETY: acl is a live returned allocation and entry is a writable output pointer.
        let found = unsafe { acl_get_entry(acl.0, ACL_FIRST_ENTRY, &mut entry) };
        // Darwin returns 0 for an entry and -1/EINVAL for the end, unlike Linux's 1/0 convention.
        if found == 0 {
            return Err(refuse(
                "its extended ACL contains access entries despite mode 0700",
            ));
        }
        let error = io::Error::last_os_error();
        if found == -1 && error.raw_os_error() == Some(libc::EINVAL) {
            Ok(())
        } else {
            Err(error)
        }
    }
}

/// Creates the leaf with a protected DACL whose user-only ACE its children inherit.
#[cfg(windows)]
fn create_private_directory(dir: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::CreateDirectoryW;

    let wide: Vec<u16> = dir.as_os_str().encode_wide().chain([0]).collect();
    let attributes = crate::transport::SecurityAttributes::owner_only(true)?;
    let created = attributes.with_raw(|raw| {
        // SAFETY: the path is terminated and the attributes live throughout CreateDirectoryW.
        unsafe {
            CreateDirectoryW(
                wide.as_ptr(),
                raw.cast::<windows_sys::Win32::Security::SECURITY_ATTRIBUTES>()
                    .cast_const(),
            )
        }
    });
    if created == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// LocalAlloc-backed security data returned by the Windows security APIs.
#[cfg(windows)]
struct SecurityData(windows_sys::Win32::Foundation::HLOCAL);

#[cfg(windows)]
impl Drop for SecurityData {
    fn drop(&mut self) {
        // SAFETY: each non-null allocation returned by a security API is owned and freed once.
        unsafe { windows_sys::Win32::Foundation::LocalFree(self.0) };
    }
}

#[cfg(windows)]
fn sid_text(sid: windows_sys::Win32::Security::PSID) -> io::Result<String> {
    use windows_sys::Win32::Security::Authorization::ConvertSidToStringSidW;
    let mut text = std::ptr::null_mut();
    // SAFETY: sid is part of a live OS-returned security descriptor; the API allocates text.
    if unsafe { ConvertSidToStringSidW(sid, &mut text) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let _allocation = SecurityData(text.cast());
    let mut length = 0;
    // SAFETY: a successful ConvertSidToStringSidW returns a NUL-terminated UTF-16 allocation.
    unsafe {
        while *text.add(length) != 0 {
            length += 1;
        }
        Ok(String::from_utf16_lossy(std::slice::from_raw_parts(
            text, length,
        )))
    }
}

/// Inspect the opened directory itself, without following a final reparse point. Administrators
/// and SYSTEM are trusted OS principals (they can already take ownership); no ordinary other
/// user may own or access the directory. Creator/owner ACEs are safe to inherit to its children.
#[cfg(windows)]
fn create_private_windows_check(dir: &Path) -> io::Result<()> {
    use std::os::windows::fs::{MetadataExt, OpenOptionsExt};
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT};
    use windows_sys::Win32::Security::{
        ACCESS_ALLOWED_ACE, ACE_HEADER, DACL_SECURITY_INFORMATION, GetAce,
        OWNER_SECURITY_INFORMATION,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_READ_ATTRIBUTES, READ_CONTROL,
    };

    use windows_sys::Win32::System::SystemServices::{
        ACCESS_ALLOWED_ACE_TYPE, ACCESS_DENIED_ACE_TYPE,
    };

    let file = std::fs::OpenOptions::new()
        .access_mode(READ_CONTROL | FILE_READ_ATTRIBUTES)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
        .open(dir)?;
    let metadata = file.metadata()?;
    let refuse = |reason: &str| {
        io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("{} is not a private directory: {reason}", dir.display()),
        )
    };
    if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(refuse("the leaf is not a real directory"));
    }
    let mut owner = std::ptr::null_mut();
    let mut dacl = std::ptr::null_mut();
    let mut descriptor = std::ptr::null_mut();
    // SAFETY: file owns the handle; output pointers are initialized by GetSecurityInfo, whose
    // returned allocation keeps both the owner SID and every ACE alive until it is freed.
    let status = unsafe {
        GetSecurityInfo(
            file.as_raw_handle(),
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &mut owner,
            std::ptr::null_mut(),
            &mut dacl,
            std::ptr::null_mut(),
            &mut descriptor,
        )
    };
    if status != 0 {
        return Err(io::Error::from_raw_os_error(status as i32));
    }
    let _allocation = SecurityData(descriptor);
    let user = transport::user_sid()?;
    let privileged = |sid: &str| sid == user || matches!(sid, "S-1-5-18" | "S-1-5-32-544");
    if owner.is_null() || !privileged(&sid_text(owner)?) {
        return Err(refuse("it belongs to another user"));
    }
    if dacl.is_null() {
        return Err(refuse("its DACL grants unrestricted access"));
    }
    // SAFETY: GetSecurityInfo returned an initialized ACL in the still-live descriptor.
    let count = unsafe { (*dacl).AceCount };
    for index in 0..count {
        let mut ace = std::ptr::null_mut();
        // SAFETY: index is within AceCount and ace is a writable pointer.
        if unsafe { GetAce(dacl, u32::from(index), &mut ace) } == 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: every ACE starts with an ACE_HEADER; the API returned it from a valid ACL.
        let kind = unsafe { (*ace.cast::<ACE_HEADER>()).AceType };
        if u32::from(kind) == ACCESS_DENIED_ACE_TYPE {
            continue;
        }
        if u32::from(kind) != ACCESS_ALLOWED_ACE_TYPE {
            return Err(refuse("its DACL contains an unsupported access rule"));
        }
        let allowed = ace.cast::<ACCESS_ALLOWED_ACE>();
        // SAFETY: this is an ACCESS_ALLOWED_ACE; its trailing SidStart begins its variable SID.
        let sid = unsafe { std::ptr::addr_of_mut!((*allowed).SidStart).cast() };
        let sid = sid_text(sid)?;
        if !privileged(&sid) && !matches!(sid.as_str(), "S-1-3-0" | "S-1-3-4") {
            return Err(refuse("its DACL grants access to another user"));
        }
    }
    Ok(())
}

/// Checks that what is at `dir` is a real directory this user owns with mode 0700.
#[cfg(unix)]
pub(crate) fn create_private_unix_check(dir: &Path) -> io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};

    #[cfg(target_os = "macos")]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(dir)?
    };
    #[cfg(target_os = "macos")]
    let meta = file.metadata()?;
    #[cfg(not(target_os = "macos"))]
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
    #[cfg(target_os = "macos")]
    private_macos::check_acl(&file, dir)?;
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
    // Reserve the reaper first: a thread allocation failure after spawn must not turn a
    // successfully detached daemon into a failed, untracked startup.
    let (reap, pending) = std::sync::mpsc::channel::<std::process::Child>();
    std::thread::Builder::new()
        .name("fastexec-daemon-reap".into())
        .spawn(move || {
            if let Ok(mut child) = pending.recv() {
                let _ = child.wait();
            }
        })
        .map_err(|error| format!("cannot prepare the daemon reaper: {error}"))?;
    let child = command
        .spawn()
        .map_err(|error| format!("cannot start the daemon: {error}"))?;
    reap.send(child)
        .unwrap_or_else(|_| unreachable!("prepared daemon reaper stopped"));
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

    #[cfg(target_os = "macos")]
    fn inheritable_directory_acl(dir: &Path) {
        let status = std::process::Command::new("/bin/chmod")
            .args(["+a", "group:everyone allow list,search,readattr,readextattr,readsecurity,file_inherit,directory_inherit"])
            .arg(dir).status().unwrap();
        assert!(status.success(), "could not install the test directory ACL");
    }

    #[cfg(target_os = "macos")]
    fn directory_acl_listing(dir: &Path) -> Vec<u8> {
        let output = std::process::Command::new("/bin/ls")
            .arg("-lde")
            .arg(dir)
            .output()
            .unwrap();
        assert!(output.status.success());
        output.stdout
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_private_directory_refuses_existing_acl_without_changing_it() {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!(
            "fastexec-private-existing-acl-{}-{}",
            std::process::id(),
            crate::tasks::random_chars(12).unwrap()
        ));
        create_private_new(&root).unwrap();
        inheritable_directory_acl(&root);
        assert_eq!(
            std::fs::metadata(&root).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let before = directory_acl_listing(&root);
        let error = create_private(&root).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("extended ACL"), "{error}");
        assert_eq!(
            directory_acl_listing(&root),
            before,
            "validation rewrote an existing ACL"
        );
        assert_eq!(
            create_private_new(&root).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_private_directories_do_not_inherit_parent_access_entries() {
        let root = std::env::temp_dir().join(format!(
            "fastexec-private-inherited-acl-{}-{}",
            std::process::id(),
            crate::tasks::random_chars(12).unwrap()
        ));
        create_private_new(&root).unwrap();
        inheritable_directory_acl(&root);
        let direct = root.join("direct");
        create_private_new(&direct).unwrap();
        create_private(&direct).unwrap();
        let recursive = root.join("missing/intermediate/leaf");
        create_private(&recursive).unwrap();
        for relative in [
            "missing",
            "missing/intermediate",
            "missing/intermediate/leaf",
        ] {
            create_private(&root.join(relative)).unwrap();
        }
        assert!(
            create_private(&root).is_err(),
            "the parent's ACL must remain unchanged"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn private_directory_creation_never_adopts_an_existing_leaf() {
        let root = std::env::temp_dir().join(format!(
            "fastexec-private-create-{}-{}",
            std::process::id(),
            crate::tasks::random_chars(12).unwrap()
        ));
        create_private_new(&root).unwrap();
        let directory = root.join("directory");
        create_private_new(&directory).unwrap();
        std::fs::write(directory.join("sentinel"), b"preserve").unwrap();
        assert_eq!(
            create_private_new(&directory).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists,
        );
        assert_eq!(
            std::fs::read(directory.join("sentinel")).unwrap(),
            b"preserve"
        );
        let file = root.join("file");
        std::fs::write(&file, b"preserve").unwrap();
        assert!(create_private_new(&file).is_err());
        assert!(create_private(&file).is_err());
        assert_eq!(std::fs::read(&file).unwrap(), b"preserve");
        #[cfg(unix)]
        {
            use std::os::unix::fs::{PermissionsExt, symlink};
            assert_eq!(
                std::fs::metadata(&directory).unwrap().permissions().mode() & 0o777,
                0o700
            );
            let link = root.join("link");
            symlink(&directory, &link).unwrap();
            assert!(create_private_new(&link).is_err());
            assert!(create_private(&link).is_err());
            assert_eq!(
                std::fs::read(directory.join("sentinel")).unwrap(),
                b"preserve"
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn an_existing_windows_runtime_directory_with_a_public_dacl_is_refused() {
        use std::os::windows::fs::OpenOptionsExt;
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Security::Authorization::{SE_FILE_OBJECT, SetSecurityInfo};
        use windows_sys::Win32::Security::{
            DACL_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION,
        };
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_FLAG_BACKUP_SEMANTICS, READ_CONTROL, WRITE_DAC,
        };

        let root = std::env::temp_dir().join(format!(
            "fastexec-private-dacl-{}-{}",
            std::process::id(),
            crate::tasks::random_chars(12).unwrap()
        ));
        create_private_new(&root).unwrap();
        assert!(
            create_private(&root).is_ok(),
            "the private DACL is accepted"
        );
        let file = std::fs::OpenOptions::new()
            .access_mode(READ_CONTROL | WRITE_DAC)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(&root)
            .unwrap();
        // This deliberately grants everyone access on an empty fixture only. The validator
        // must reject the insecure existing leaf instead of trusting its parent directory.
        // SAFETY: the live handle owns this test's empty directory; a null DACL is deliberate.
        let status = unsafe {
            SetSecurityInfo(
                file.as_raw_handle(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        assert_eq!(status, 0);
        drop(file);
        assert_eq!(
            create_private(&root).unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        std::fs::remove_dir(root).unwrap();
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
