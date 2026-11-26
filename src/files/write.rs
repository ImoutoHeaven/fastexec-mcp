use super::edit::EditSource;
use super::path::{
    append_path, display_path, ends_in_directory, native_path_bytes, read_target_link,
    reserve_target_workspace, resolve, target_memory_error, with_path_workspace,
};
use super::{FileError, WriteRequest, lock};
use crate::env::LaunchContext;
use crate::lease::{BytePool, Lease};
#[cfg(all(unix, not(any(target_os = "macos", target_os = "freebsd"))))]
use std::fs::Metadata;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileWriteData {
    pub path: String,
    pub bytes: u64,
}

/// Writes one file, creating its parent directories and keeping an existing file's permissions.
pub async fn write_core(
    request: WriteRequest,
    launch: Arc<LaunchContext>,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    input_owner: crate::lease::Owner,
) -> crate::lease::Owned<Result<FileWriteData, FileError>> {
    let input = crate::lease::Owned::new((request, permit), input_owner);
    match tokio::task::spawn_blocking(move || {
        input.flat_map(|(request, _permit)| write_blocking(&request, &launch))
    })
    .await
    {
        Ok(outcome) => outcome,
        Err(error) => crate::lease::Owned::new(
            Err(FileError::Io(format!(
                "the file operation did not finish: {error}"
            ))),
            Default::default(),
        ),
    }
}

pub(crate) fn write_blocking(
    request: &WriteRequest,
    launch: &LaunchContext,
) -> crate::lease::Owned<Result<FileWriteData, FileError>> {
    with_path_workspace(&request.path, launch, 0, |lease| {
        write_text_reserved(request, launch, lease)
    })
    .unwrap_or_else(|message| {
        crate::lease::Owned::new(Err(FileError::Overload(message)), Default::default())
    })
}

/// Writes one file, creating its parent directories and keeping an existing file's permissions.
#[cfg(test)]
pub(super) fn write_text(
    request: &WriteRequest,
    launch: &LaunchContext,
) -> Result<(String, String), String> {
    write_blocking(request, launch)
        .run(|result| result)
        .map(|data| (format!("Successfully wrote to {}", data.path), data.path))
        .map_err(FileError::into_message)
}

pub(super) fn write_text_reserved(
    request: &WriteRequest,
    launch: &LaunchContext,
    lease: &mut Lease,
) -> Result<FileWriteData, FileError> {
    let resolved = resolve(&request.path, launch).map_err(FileError::Validation)?;
    let shown_path = display_path(&resolved);
    let _guard = lock();
    let map_io = |error: io::Error| {
        let message = format!("Cannot write {shown_path}: {error}.");
        match error.kind() {
            io::ErrorKind::OutOfMemory => FileError::Overload(message),
            io::ErrorKind::NotFound => FileError::NotFound(message),
            _ => FileError::Io(message),
        }
    };
    let target = write_target(&resolved, launch.memory()).map_err(map_io)?;
    reserve_target_workspace(&target.value, lease, launch.memory()).map_err(map_io)?;
    create_target_directories(&target.value, launch.memory()).map_err(map_io)?;
    save_target(
        &target.value,
        request.content.as_bytes(),
        None,
        launch.memory(),
    )
    .map_err(map_io)?;
    Ok(FileWriteData {
        path: shown_path,
        bytes: request.content.len() as u64,
    })
}

/// Creates the final target's parent directories for an initial upload. Its real
/// blocking worker owns the resolved target even when the HTTP waiter is dropped.
pub(crate) fn create_target_parents(path: &Path) -> io::Result<crate::lease::Owned<PathBuf>> {
    create_target_parents_in(path, crate::lease::process_pool())
}

pub(super) fn create_target_parents_in(
    path: &Path,
    pool: &Arc<BytePool>,
) -> io::Result<crate::lease::Owned<PathBuf>> {
    let target = write_target(path, pool)?;
    create_target_directories(&target.value, pool)?;
    Ok(target)
}

fn create_target_directories(path: &Path, pool: &Arc<BytePool>) -> io::Result<()> {
    let native_bytes = native_path_bytes(path).ok_or_else(|| target_memory_error(pool))?;
    let _native_lease = pool
        .try_acquire(native_bytes)
        .ok_or_else(|| target_memory_error(pool))?;
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

/// Follows final-component links, including dangling targets, retaining the original
/// forty-step limit and native directory requirements.
pub(crate) fn write_target(
    path: &Path,
    pool: &Arc<BytePool>,
) -> io::Result<crate::lease::Owned<PathBuf>> {
    let initial = native_path_bytes(path).ok_or_else(|| target_memory_error(pool))?;
    let mut lease = pool
        .try_acquire(initial)
        .ok_or_else(|| target_memory_error(pool))?;
    let mut target = path.to_path_buf();
    for depth in 0..=40 {
        let working = native_path_bytes(&target)
            .and_then(|n| n.checked_add(target.capacity()))
            .ok_or_else(|| target_memory_error(pool))?;
        if !lease.try_resize(working) {
            return Err(target_memory_error(pool));
        }
        if ends_in_directory(&target) {
            return Err(io::Error::other(
                "the path requires a directory, not a regular file",
            ));
        }
        match std::fs::symlink_metadata(&target) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                if depth == 0
                    && let Err(error) = std::fs::metadata(path)
                    && error.kind() != io::ErrorKind::NotFound
                {
                    return Err(error);
                }
                let link = read_target_link(&target, pool)?;
                let joined = target
                    .as_os_str()
                    .as_encoded_bytes()
                    .len()
                    .checked_add(link.value.as_os_str().as_encoded_bytes().len())
                    .and_then(|n| n.checked_add(8))
                    .and_then(|n| n.checked_mul(16))
                    .and_then(|n| n.checked_add(target.capacity()))
                    .and_then(|n| n.checked_add(link.value.capacity()))
                    .ok_or_else(|| target_memory_error(pool))?;
                if !lease.try_resize(joined) {
                    return Err(target_memory_error(pool));
                }
                target = if link.value.is_absolute() {
                    // The new lease already covers the moved path before its old owner ends.
                    link.run(|path| path)
                } else {
                    append_path(
                        target.parent().unwrap_or_else(|| Path::new(".")),
                        &link.value,
                    )
                };
            }
            Ok(_) => {
                lease.shrink_to(target.capacity());
                return Ok(crate::lease::Owned::new(
                    target,
                    crate::lease::Owner::new(lease),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                lease.shrink_to(target.capacity());
                return Ok(crate::lease::Owned::new(
                    target,
                    crate::lease::Owner::new(lease),
                ));
            }
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "too many symbolic links",
    ))
}

/// A fully staged replacement, removed on every failure. Closing before removal also makes
/// cleanup work on Windows.
pub(super) struct Staged {
    path: PathBuf,
    pub(super) file: Option<File>,
    remove_on_drop: bool,
    #[cfg(target_os = "freebsd")]
    pub(super) directory: Option<PathBuf>,
}

impl Drop for Staged {
    fn drop(&mut self) {
        drop(self.file.take());
        if self.remove_on_drop {
            let _ = std::fs::remove_file(&self.path);
        }
        #[cfg(target_os = "freebsd")]
        if let Some(directory) = &self.directory {
            let _ = std::fs::remove_dir(directory);
        }
    }
}

impl Staged {
    pub(super) fn new(parent: &Path, private: bool) -> io::Result<Self> {
        for _ in 0..32 {
            let mut random = [0u8; 16];
            getrandom::fill(&mut random).map_err(|error| io::Error::other(error.to_string()))?;
            let name = format!(".fastexec-save-{:032x}", u128::from_le_bytes(random));
            let path = append_path(parent, Path::new(&name));
            #[cfg(target_os = "freebsd")]
            let directory = if private {
                match crate::platform::runtime::create_private_new(&path) {
                    Ok(()) => Some(path.clone()),
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                    Err(error) => return Err(error),
                }
            } else {
                None
            };
            #[cfg(target_os = "freebsd")]
            let path = directory.as_ref().map_or(path, |dir| dir.join("content"));
            match create_staging_file(&path, private) {
                Ok(file) => {
                    return Ok(Self {
                        path,
                        file: Some(file),
                        remove_on_drop: true,
                        #[cfg(target_os = "freebsd")]
                        directory,
                    });
                }
                Err(error) => {
                    #[cfg(target_os = "freebsd")]
                    if let Some(directory) = directory {
                        let _ = std::fs::remove_dir(directory);
                    }
                    if error.kind() != io::ErrorKind::AlreadyExists {
                        return Err(error);
                    }
                }
            }
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not create a unique staging file",
        ))
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
pub(super) fn create_staging_file(path: &Path, private: bool) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(if private { 0o600 } else { 0o666 })
        .open(path)
}

/// Darwin ACL grants are independent of the mode bits. Suppress inheritance in the creation
/// syscall itself, so another user cannot open an inherited read handle to staged content.
#[cfg(target_os = "macos")]
pub(super) fn create_staging_file(path: &Path, private: bool) -> io::Result<File> {
    if private {
        macos_access::create_private(path)
    } else {
        OpenOptions::new().write(true).create_new(true).open(path)
    }
}

/// Extended ACLs are not among Darwin's public xattrs. Apple Libc uses fstatx/fchmodx for the
/// ACL APIs, and XNU honors ACL_FLAG_NO_INHERIT on an initial openx_np security descriptor.
#[cfg(target_os = "macos")]
pub(super) mod macos_access {
    use super::*;
    use std::ffi::{CString, c_char, c_int, c_void};
    use std::os::fd::{AsRawFd, FromRawFd};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::MetadataExt;

    const ACL_TYPE_EXTENDED: c_int = 0x100;
    const ACL_FLAG_NO_INHERIT: c_int = 1 << 17;
    const FILESEC_OWNER: c_int = 1;
    const FILESEC_GROUP: c_int = 2;
    const FILESEC_MODE: c_int = 4;
    const FILESEC_ACL: c_int = 5;

    // These public Darwin interfaces are in libSystem but are not declared by the libc crate.
    unsafe extern "C" {
        fn acl_init(count: c_int) -> *mut c_void;
        fn acl_free(acl: *mut c_void) -> c_int;
        fn acl_get_fd_np(fd: c_int, kind: c_int) -> *mut c_void;
        fn acl_get_entry(acl: *mut c_void, id: c_int, entry: *mut *mut c_void) -> c_int;
        fn acl_get_flagset_np(acl: *mut c_void, flags: *mut *mut c_void) -> c_int;
        fn acl_add_flag_np(flags: *mut c_void, flag: c_int) -> c_int;
        fn filesec_init() -> *mut c_void;
        fn filesec_free(security: *mut c_void);
        fn filesec_set_property(
            security: *mut c_void,
            property: c_int,
            value: *const c_void,
        ) -> c_int;
        fn openx_np(path: *const c_char, flags: c_int, security: *mut c_void) -> c_int;
        fn fchmodx_np(fd: c_int, security: *mut c_void) -> c_int;
        #[cfg(test)]
        fn acl_to_text(acl: *mut c_void, length: *mut libc::ssize_t) -> *mut c_char;
    }

    struct Acl(*mut c_void);

    impl Drop for Acl {
        fn drop(&mut self) {
            // SAFETY: Acl exclusively owns an allocation returned by a Darwin ACL function.
            unsafe { acl_free(self.0) };
        }
    }

    impl Acl {
        fn empty(no_inherit: bool) -> io::Result<Self> {
            // SAFETY: zero entries creates an independently allocated, empty ACL.
            let raw = unsafe { acl_init(0) };
            if raw.is_null() {
                return Err(io::Error::last_os_error());
            }
            let acl = Self(raw);
            if no_inherit {
                let mut flags = std::ptr::null_mut();
                // SAFETY: acl owns a live ACL and flags points into it until it is freed.
                if unsafe { acl_get_flagset_np(acl.0, &mut flags) } != 0
                    || unsafe { acl_add_flag_np(flags, ACL_FLAG_NO_INHERIT) } != 0
                {
                    return Err(io::Error::last_os_error());
                }
            }
            Ok(acl)
        }

        fn read(file: &File) -> io::Result<Option<Self>> {
            // SAFETY: the descriptor remains open for this synchronous metadata query.
            let raw = unsafe { acl_get_fd_np(file.as_raw_fd(), ACL_TYPE_EXTENDED) };
            if !raw.is_null() {
                return Ok(Some(Self(raw)));
            }
            let error = io::Error::last_os_error();
            // With a live descriptor, Darwin's FILESEC_ACL property reports ENOENT when no
            // ACL exists. Other errors cannot establish absence and must leave the target alone.
            if error.raw_os_error() == Some(libc::ENOENT) {
                Ok(None)
            } else {
                Err(error)
            }
        }

        fn has_entries(&self) -> io::Result<bool> {
            let mut entry = std::ptr::null_mut();
            // SAFETY: ACL_FIRST_ENTRY is 0; Darwin returns 0 for an entry, -1/EINVAL at the end.
            if unsafe { acl_get_entry(self.0, 0, &mut entry) } == 0 {
                return Ok(true);
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINVAL) {
                Ok(false)
            } else {
                Err(error)
            }
        }
    }

    struct Security(*mut c_void);

    impl Drop for Security {
        fn drop(&mut self) {
            // SAFETY: Security owns a successful filesec_init allocation.
            unsafe { filesec_free(self.0) };
        }
    }

    impl Security {
        fn new() -> io::Result<Self> {
            // SAFETY: filesec_init returns a new descriptor, or null on failure.
            let raw = unsafe { filesec_init() };
            if raw.is_null() {
                Err(io::Error::last_os_error())
            } else {
                Ok(Self(raw))
            }
        }

        fn set<T>(&self, property: c_int, value: &T) -> io::Result<()> {
            // SAFETY: callers supply the documented type; filesec copies each value/ACL.
            if unsafe { filesec_set_property(self.0, property, (value as *const T).cast()) } != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        }
    }

    pub(super) fn create_private(path: &Path) -> io::Result<File> {
        let path_c = CString::new(path.as_os_str().as_bytes()).map_err(io::Error::other)?;
        let acl = Acl::empty(true)?;
        let security = Security::new()?;
        let mode: libc::mode_t = 0o600;
        security.set(FILESEC_MODE, &mode)?;
        security.set(FILESEC_ACL, &acl.0)?;
        // SAFETY: path and security live through the call. Exclusive creation refuses every
        // existing name; the restrictive mode and non-inheriting ACL take effect atomically.
        let fd = unsafe {
            openx_np(
                path_c.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                security.0,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful exclusive openx_np returned a new owned descriptor.
        let file = unsafe { File::from_raw_fd(fd) };
        let verify = (|| {
            let metadata = file.metadata()?;
            // SAFETY: geteuid has no arguments and no side effects.
            if !metadata.is_file()
                || metadata.uid() != unsafe { libc::geteuid() }
                || metadata.mode() & 0o077 != 0
            {
                return Err(io::Error::other("the staging file is not private"));
            }
            let mut filesystem = std::mem::MaybeUninit::<libc::statfs>::uninit();
            // SAFETY: the descriptor is live and fstatfs initializes the output on success.
            if unsafe { libc::fstatfs(file.as_raw_fd(), filesystem.as_mut_ptr()) } != 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: successful fstatfs initialized every field.
            if unsafe { filesystem.assume_init() }.f_flags & libc::MNT_IGNORE_OWNERSHIP as u32 != 0
            {
                return Err(io::Error::other(
                    "the volume ignores ownership and cannot keep staging private",
                ));
            }
            if let Some(acl) = Acl::read(&file)?
                && acl.has_entries()?
            {
                return Err(io::Error::other(
                    "the staging file inherited an access-control entry",
                ));
            }
            Ok(())
        })();
        if let Err(error) = verify {
            // No content has been written, including on a filesystem that ignored the ACL.
            drop(file);
            let _ = std::fs::remove_file(path);
            return Err(error);
        }
        Ok(file)
    }

    pub(super) fn preserve(source: &File, staged: &File) -> io::Result<()> {
        let acl = match Acl::read(source)? {
            Some(acl) => acl,
            // An empty ACL replaces every staging entry, including any inherited grant.
            None => Acl::empty(false)?,
        };
        let metadata = source.metadata()?;
        let mode = (metadata.mode() & 0o7777) as libc::mode_t;
        let security = Security::new()?;
        security.set(FILESEC_OWNER, &metadata.uid())?;
        security.set(FILESEC_GROUP, &metadata.gid())?;
        security.set(FILESEC_MODE, &mode)?;
        security.set(FILESEC_ACL, &acl.0)?;
        // SAFETY: the live file and security descriptor cover this synchronous operation.
        // Set mode, ownership and ACL together: chmod(0644) followed by copying a deny ACL
        // would briefly expose completed content, as would chown before restoring a mode of 0.
        if unsafe { fchmodx_np(staged.as_raw_fd(), security.0) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(in crate::files) fn acl_text(file: &File) -> io::Result<String> {
        let Some(acl) = Acl::read(file)? else {
            return Ok(String::new());
        };
        // SAFETY: the ACL is live; a null length pointer is allowed by acl_to_text.
        let text = unsafe { acl_to_text(acl.0, std::ptr::null_mut()) };
        if text.is_null() {
            return Err(io::Error::last_os_error());
        }
        let allocation = Acl(text.cast());
        // SAFETY: acl_to_text returns a NUL-terminated string owned by allocation.
        let result = unsafe { std::ffi::CStr::from_ptr(text) }
            .to_string_lossy()
            .into_owned();
        drop(allocation);
        Ok(result)
    }

    #[cfg(test)]
    pub(in crate::files) fn has_entries(file: &File) -> io::Result<bool> {
        Acl::read(file)?.map_or(Ok(false), |acl| acl.has_entries())
    }
}

/// Install the restrictive DACL in CreateFile itself. Tightening an inherited DACL after opening
/// an empty file would still let another user open a handle before it is tightened.
#[cfg(windows)]
pub(super) fn create_staging_file(path: &Path, private: bool) -> io::Result<File> {
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::io::FromRawHandle;
    use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
    use windows_sys::Win32::Storage::FileSystem::{
        CREATE_NEW, CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_GENERIC_WRITE, FILE_READ_ATTRIBUTES,
        FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, WRITE_DAC, WRITE_OWNER,
    };
    if !private {
        return OpenOptions::new().write(true).create_new(true).open(path);
    }
    let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let attributes = crate::transport::SecurityAttributes::owner_only(false)?;
    let handle = attributes.with_raw(|raw| {
        // SAFETY: the path and owner-only descriptor live through the synchronous call.
        unsafe {
            CreateFileW(
                wide.as_ptr(),
                FILE_GENERIC_WRITE | FILE_READ_ATTRIBUTES | WRITE_DAC | WRITE_OWNER,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                raw.cast::<windows_sys::Win32::Security::SECURITY_ATTRIBUTES>()
                    .cast_const(),
                CREATE_NEW,
                FILE_ATTRIBUTE_NORMAL,
                std::ptr::null_mut(),
            )
        }
    });
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a successful CREATE_NEW returned this new, uniquely owned handle.
    Ok(unsafe { File::from_raw_handle(handle) })
}

/// Replacing one hard-linked name would silently detach it from its peers. There is no atomic
/// way to rewrite the shared inode, so require an explicit in-place update through exec instead.
fn single_link(file: &File) -> io::Result<()> {
    #[cfg(unix)]
    let links = {
        use std::os::unix::fs::MetadataExt;
        file.metadata()?.nlink()
    };
    #[cfg(windows)]
    let links = {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, GetFileInformationByHandle,
        };
        // SAFETY: the live file owns the handle and the structure is a writable output buffer.
        let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
        if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 {
            return Err(io::Error::last_os_error());
        }
        u64::from(info.nNumberOfLinks)
    };
    if links > 1 {
        return Err(io::Error::other(format!(
            "the file has {links} hard links; an atomic replacement would split them. Use exec for an explicit in-place update"
        )));
    }
    Ok(())
}

/// Preserves an existing Unix file's owner before restoring its mode. A new inode owned by the
/// server would otherwise take a user's file away from them when a privileged server edits it.
#[cfg(all(unix, not(any(target_os = "macos", target_os = "freebsd"))))]
fn preserve_owner(file: &File, original: &Metadata) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::MetadataExt;
    let current = file.metadata()?;
    if current.uid() != original.uid() || current.gid() != original.gid() {
        // SAFETY: fchown acts on the live staging file, not a path another process can replace.
        if unsafe { libc::fchown(file.as_raw_fd(), original.uid(), original.gid()) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(any(unix, windows, test))]
fn save_memory_error(pool: &BytePool) -> io::Error {
    io::Error::new(
        io::ErrorKind::OutOfMemory,
        format!(
            "Server active memory budget ({} bytes) is full while preserving file metadata; the original file was not replaced.",
            pool.limit()
        ),
    )
}

#[cfg(any(unix, windows, test))]
pub(super) struct SaveBuffer<T> {
    pub(super) data: Vec<T>,
    pub(super) _lease: Lease,
}

#[cfg(any(unix, windows, test))]
impl<T: Default + Clone> SaveBuffer<T> {
    pub(super) fn new(count: usize, pool: &Arc<BytePool>) -> io::Result<Self> {
        let memory = || save_memory_error(pool);
        let bytes = count
            .checked_mul(std::mem::size_of::<T>())
            .ok_or_else(memory)?;
        let mut lease = pool.try_acquire(bytes).ok_or_else(memory)?;
        let mut data = Vec::new();
        data.try_reserve_exact(count).map_err(|_| memory())?;
        let actual = data
            .capacity()
            .checked_mul(std::mem::size_of::<T>())
            .ok_or_else(memory)?;
        if !lease.try_resize(actual) {
            return Err(memory());
        }
        data.resize(count, T::default());
        Ok(Self {
            data,
            _lease: lease,
        })
    }
}

/// Keeps ACLs and extended attributes on the Unix platforms supported by the server. Linux
/// file capabilities are deliberately not restored: a content write normally clears them.
#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(super) fn preserve_xattrs(
    original: &File,
    staged: &File,
    pool: &Arc<BytePool>,
) -> io::Result<()> {
    use std::ffi::CStr;
    use std::os::fd::AsRawFd;
    let (from, to) = (original.as_raw_fd(), staged.as_raw_fd());
    let list = |fd, buffer: *mut libc::c_char, size: usize| {
        #[cfg(target_os = "linux")]
        // SAFETY: the caller supplies a writable buffer of size bytes, or null for a size query.
        unsafe {
            libc::flistxattr(fd, buffer, size)
        }
        #[cfg(target_os = "macos")]
        // SAFETY: same contract, with no platform-specific flags.
        unsafe {
            libc::flistxattr(fd, buffer, size, 0)
        }
    };
    let count = list(from, std::ptr::null_mut(), 0);
    if count < 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ENOTSUP) {
            return Ok(());
        }
        return Err(error);
    }
    let mut names = SaveBuffer::<u8>::new(count as usize, pool)?;
    if !names.data.is_empty() {
        let count = list(from, names.data.as_mut_ptr().cast(), names.data.len());
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        names.data.truncate(count as usize);
    }
    // The sibling may inherit a default ACL or other directory attributes the original does
    // not have. Remove those first; chmod alone could otherwise activate extra named ACL users.
    let count = list(to, std::ptr::null_mut(), 0);
    if count < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut inherited = SaveBuffer::<u8>::new(count as usize, pool)?;
    if !inherited.data.is_empty() {
        let count = list(to, inherited.data.as_mut_ptr().cast(), inherited.data.len());
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        inherited.data.truncate(count as usize);
    }
    for name in inherited.data.split_inclusive(|&byte| byte == 0) {
        if names
            .data
            .split_inclusive(|&byte| byte == 0)
            .any(|original| original == name)
        {
            continue;
        }
        let name = CStr::from_bytes_with_nul(name).map_err(io::Error::other)?;
        #[cfg(target_os = "linux")]
        // SAFETY: the descriptor is live and the attribute name is terminated.
        let removed = unsafe { libc::fremovexattr(to, name.as_ptr()) };
        #[cfg(target_os = "macos")]
        // SAFETY: same contract, with no platform-specific flags.
        let removed = unsafe { libc::fremovexattr(to, name.as_ptr(), 0) };
        if removed != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    for name in names.data.split_inclusive(|&byte| byte == 0) {
        let name = CStr::from_bytes_with_nul(name).map_err(io::Error::other)?;
        #[cfg(target_os = "linux")]
        if name.to_bytes() == b"security.capability" {
            continue;
        }
        let read = |buffer: *mut libc::c_void, size: usize| {
            #[cfg(target_os = "linux")]
            // SAFETY: name is terminated and buffer follows the same query/read contract above.
            unsafe {
                libc::fgetxattr(from, name.as_ptr(), buffer, size)
            }
            #[cfg(target_os = "macos")]
            // SAFETY: same contract, with position and flags both zero.
            unsafe {
                libc::fgetxattr(from, name.as_ptr(), buffer, size, 0, 0)
            }
        };
        let size = read(std::ptr::null_mut(), 0);
        if size < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut value = SaveBuffer::<u8>::new(size as usize, pool)?;
        let size = read(value.data.as_mut_ptr().cast(), value.data.len());
        if size < 0 {
            return Err(io::Error::last_os_error());
        }
        if size as usize > value.data.len() {
            return Err(io::Error::other(
                "an extended attribute changed while it was being copied",
            ));
        }
        #[cfg(target_os = "linux")]
        // SAFETY: the names and buffers remain valid for the synchronous call.
        let saved = unsafe {
            libc::fsetxattr(
                to,
                name.as_ptr(),
                value.data.as_ptr().cast(),
                size as usize,
                0,
            )
        };
        #[cfg(target_os = "macos")]
        // SAFETY: same contract, with position and flags both zero.
        let saved = unsafe {
            libc::fsetxattr(
                to,
                name.as_ptr(),
                value.data.as_ptr().cast(),
                size as usize,
                0,
                0,
            )
        };
        if saved != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

/// Restore access only after the staging content is complete. ReplaceFile preserves DACLs but
/// does not promise to preserve the owner or group, so copy those explicitly before publishing.
/// A protected staging DACL keeps directory inheritance from widening access to an existing file.
#[cfg(windows)]
pub(super) fn preserve_windows_access(
    original: &File,
    staged: &File,
    protect: bool,
    pool: &Arc<BytePool>,
) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::{ERROR_INSUFFICIENT_BUFFER, ERROR_SUCCESS};
    use windows_sys::Win32::Security::Authorization::{SE_FILE_OBJECT, SetSecurityInfo};
    use windows_sys::Win32::Security::{
        ACL, DACL_SECURITY_INFORMATION, EqualSid, GROUP_SECURITY_INFORMATION,
        GetKernelObjectSecurity, GetSecurityDescriptorControl, GetSecurityDescriptorDacl,
        GetSecurityDescriptorGroup, GetSecurityDescriptorOwner, OWNER_SECURITY_INFORMATION,
        PROTECTED_DACL_SECURITY_INFORMATION, PSECURITY_DESCRIPTOR, PSID, SE_DACL_PROTECTED,
        UNPROTECTED_DACL_SECURITY_INFORMATION,
    };
    use windows_sys::Win32::Storage::FileSystem::GetVolumeInformationByHandleW;
    let mut filesystem_flags = 0u32;
    // A volume query is only an optimization for filesystems without ACLs. SMB does not
    // support this API, so an unavailable query must still try the file-security APIs below.
    // SAFETY: only filesystem flags are requested from the volume holding the live handle.
    let volume_known = unsafe {
        GetVolumeInformationByHandleW(
            original.as_raw_handle(),
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut filesystem_flags,
            std::ptr::null_mut(),
            0,
        )
    } != 0;
    // FILE_PERSISTENT_ACLS: FAT/exFAT have no security descriptor to preserve.
    if volume_known && filesystem_flags & 0x00000008 == 0 {
        return Ok(());
    }
    struct Security {
        buffer: SaveBuffer<usize>,
        owner: PSID,
        group: PSID,
        dacl: *mut ACL,
    }
    impl Security {
        fn descriptor(&self) -> PSECURITY_DESCRIPTOR {
            self.buffer.data.as_ptr().cast_mut().cast()
        }
    }
    let read = |file: &File, information| -> io::Result<Security> {
        let mut required = 0u32;
        // SAFETY: this is a size-only query on the same live handle used below.
        let queried = unsafe {
            GetKernelObjectSecurity(
                file.as_raw_handle(),
                information,
                std::ptr::null_mut(),
                0,
                &mut required,
            )
        };
        if queried == 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_INSUFFICIENT_BUFFER as i32) {
                return Err(error);
            }
        }
        if required == 0 {
            return Err(io::Error::other("the file's security descriptor is empty"));
        }
        // Self-relative descriptors are DWORD-aligned. A usize buffer supplies at
        // least that alignment on supported Windows targets; charge its rounded size.
        let unit = std::mem::size_of::<usize>();
        let count = (required as usize)
            .checked_add(unit - 1)
            .ok_or_else(|| save_memory_error(pool))?
            / unit;
        let mut buffer = SaveBuffer::<usize>::new(count, pool)?;
        let mut written = 0u32;
        // SAFETY: the aligned buffer has at least required bytes. If the descriptor
        // grew since the query, Windows returns an error before we use its contents.
        if unsafe {
            GetKernelObjectSecurity(
                file.as_raw_handle(),
                information,
                buffer.data.as_mut_ptr().cast(),
                required,
                &mut written,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let mut security = Security {
            buffer,
            owner: std::ptr::null_mut(),
            group: std::ptr::null_mut(),
            dacl: std::ptr::null_mut(),
        };
        let descriptor = security.descriptor();
        let mut defaulted = 0;
        // SAFETY: both calls borrow SIDs from the still-owned descriptor buffer.
        if unsafe { GetSecurityDescriptorOwner(descriptor, &mut security.owner, &mut defaulted) }
            == 0
            || unsafe {
                GetSecurityDescriptorGroup(descriptor, &mut security.group, &mut defaulted)
            } == 0
        {
            return Err(io::Error::last_os_error());
        }
        if information & DACL_SECURITY_INFORMATION != 0 {
            let mut present = 0;
            // SAFETY: the returned ACL is borrowed from the same owned buffer.
            if unsafe {
                GetSecurityDescriptorDacl(
                    descriptor,
                    &mut present,
                    &mut security.dacl,
                    &mut defaulted,
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        if security.owner.is_null() || security.group.is_null() {
            return Err(io::Error::other("the file's owner or group is missing"));
        }
        Ok(security)
    };
    let ownership = OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION;
    let source = read(original, ownership | DACL_SECURITY_INFORMATION)?;
    let current = read(staged, ownership)?;
    let (mut control, mut revision) = (0u16, 0u32);
    // SAFETY: the source owns the live security descriptor buffer.
    if unsafe { GetSecurityDescriptorControl(source.descriptor(), &mut control, &mut revision) }
        == 0
    {
        return Err(io::Error::last_os_error());
    }
    let inheritance = if protect || control & SE_DACL_PROTECTED != 0 {
        PROTECTED_DACL_SECURITY_INFORMATION
    } else {
        UNPROTECTED_DACL_SECURITY_INFORMATION
    };
    let mut information = DACL_SECURITY_INFORMATION | inheritance;
    // Avoid an unnecessary ownership change when normal creation already chose the same SID.
    // SAFETY: each non-null SID came from its still-live security descriptor.
    if unsafe { EqualSid(source.owner, current.owner) } == 0 {
        information |= OWNER_SECURITY_INFORMATION;
    }
    // SAFETY: same live-descriptor contract for the primary group SIDs.
    if unsafe { EqualSid(source.group, current.group) } == 0 {
        information |= GROUP_SECURITY_INFORMATION;
    }
    // SAFETY: these SIDs and ACL remain alive during this synchronous file-security call. A
    // caller unable to preserve a different owner/group fails here, before touching the target.
    let result = unsafe {
        SetSecurityInfo(
            staged.as_raw_handle(),
            SE_FILE_OBJECT,
            information,
            source.owner,
            source.group,
            source.dacl,
            std::ptr::null(),
        )
    };
    if result != ERROR_SUCCESS {
        return Err(io::Error::from_raw_os_error(result as i32));
    }
    Ok(())
}

/// The documented ReplaceFile failure state in which the old file has already moved to the
/// backup. Keep this recovery logic independent of Win32 so its destructive failure paths can
/// also be exercised on the other supported platforms.
#[cfg(any(windows, test))]
pub(super) fn replace_with_backup(
    target: &Path,
    replace: impl FnOnce(&Path) -> io::Result<()>,
    restore: impl FnOnce(&Path) -> io::Result<()>,
) -> io::Result<()> {
    let parent = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    // Reserve a unique, owner-only name. ReplaceFile replaces this empty placeholder with the
    // original file; no previously existing user's backup is overwritten.
    let mut backup = Staged::new(parent, true)?;
    drop(backup.file.take());
    // A panic or an unexpected unwind must never remove a backup after the native call moved
    // the original into it. Re-enable cleanup only in states that leave a complete target.
    backup.remove_on_drop = false;
    match replace(&backup.path) {
        Ok(()) => {
            backup.remove_on_drop = true;
            Ok(())
        }
        // ERROR_UNABLE_TO_MOVE_REPLACEMENT_2: the original is now at the supplied backup path.
        Err(error) if error.raw_os_error() == Some(1177) => match restore(&backup.path) {
            Ok(()) => {
                backup.remove_on_drop = true;
                Err(error)
            }
            Err(restore_error) => Err(io::Error::new(
                error.kind(),
                format!(
                    "could not replace {} ({error}) or restore it ({restore_error}); the original file is preserved at {}",
                    display_path(target),
                    display_path(&backup.path)
                ),
            )),
        },
        // With a non-null backup path, 1176 and all other documented failures keep the original
        // at its original name. Only an unused backup placeholder needs cleanup in these states.
        Err(error) => {
            backup.remove_on_drop = true;
            Err(error)
        }
    }
}

/// A no-replace move works on filesystems without hard links and cannot overwrite a file that
/// another writer created during a failed replacement's rollback.
#[cfg(windows)]
fn move_new_windows(source: &Path, target: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::MoveFileExW;
    let source: Vec<u16> = source.as_os_str().encode_wide().chain(Some(0)).collect();
    let target: Vec<u16> = target.as_os_str().encode_wide().chain(Some(0)).collect();
    // SAFETY: both paths are terminated; no REPLACE_EXISTING or COPY_ALLOWED flags are set.
    if unsafe { MoveFileExW(source.as_ptr(), target.as_ptr(), 0) } == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[cfg(windows)]
pub(super) fn windows_file_identity(file: &File) -> io::Result<(u32, u32, u32)> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT,
        GetFileInformationByHandle,
    };
    // SAFETY: the live file owns this handle and info is a writable output buffer.
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 {
        return Err(io::Error::last_os_error());
    }
    if info.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT) != 0 {
        return Err(io::Error::other("the target is not a regular file"));
    }
    Ok((
        info.dwVolumeSerialNumber,
        info.nFileIndexHigh,
        info.nFileIndexLow,
    ))
}

#[cfg(windows)]
pub(super) fn unchanged_windows_target(original: &File, target: &Path) -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES,
    };
    let now = OpenOptions::new()
        .read(true)
        .access_mode(FILE_READ_ATTRIBUTES)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
        .open(target)?;
    if windows_file_identity(original)? != windows_file_identity(&now)? {
        return Err(io::Error::other(
            "the target changed while the replacement was being saved",
        ));
    }
    Ok(())
}

/// Publishes a complete replacement. Windows ReplaceFile preserves the existing DACL and
/// alternate streams; an ordinary rename would discard them. Ownership was copied beforehand.
pub(super) fn publish(staged: &Path, target: &Path, exists: bool) -> io::Result<()> {
    #[cfg(unix)]
    {
        if exists {
            return std::fs::rename(staged, target);
        }
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            use std::ffi::CString;
            use std::os::unix::ffi::OsStrExt;
            let source = CString::new(staged.as_os_str().as_bytes()).map_err(io::Error::other)?;
            let destination =
                CString::new(target.as_os_str().as_bytes()).map_err(io::Error::other)?;
            #[cfg(target_os = "linux")]
            // Use the kernel entry point: the static musl shipped by supported Rust toolchains
            // need not export the newer renameat2 C wrapper.
            // SAFETY: both paths are terminated; the arguments match Linux renameat2's ABI.
            let moved = unsafe {
                libc::syscall(
                    libc::SYS_renameat2,
                    libc::AT_FDCWD,
                    source.as_ptr(),
                    libc::AT_FDCWD,
                    destination.as_ptr(),
                    libc::RENAME_NOREPLACE,
                )
            };
            #[cfg(target_os = "macos")]
            // SAFETY: both paths are terminated and RENAME_EXCL refuses an existing name.
            let moved = unsafe {
                libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL)
            };
            if moved == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if !matches!(
                error.raw_os_error(),
                Some(libc::ENOSYS | libc::EINVAL | libc::ENOTSUP)
            ) {
                return Err(error);
            }
        }
        // Older kernels/filesystems can still publish the complete inode with a link. Neither
        // route overwrites a file or a symlink created meanwhile.
        std::fs::hard_link(staged, target)
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::ReplaceFileW;
        if !exists {
            return move_new_windows(staged, target);
        }
        let wide_target: Vec<u16> = target.as_os_str().encode_wide().chain(Some(0)).collect();
        let wide_staged: Vec<u16> = staged.as_os_str().encode_wide().chain(Some(0)).collect();
        replace_with_backup(
            target,
            |backup| {
                let backup: Vec<u16> = backup.as_os_str().encode_wide().chain(Some(0)).collect();
                // SAFETY: all three terminated paths remain live for the call. Supplying a backup
                // prevents ERROR_UNABLE_TO_MOVE_REPLACEMENT (1176) from deleting the original.
                if unsafe {
                    ReplaceFileW(
                        wide_target.as_ptr(),
                        wide_staged.as_ptr(),
                        backup.as_ptr(),
                        0,
                        std::ptr::null(),
                        std::ptr::null(),
                    )
                } == 0
                {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            },
            |backup| move_new_windows(backup, target),
        )
    }
}

/// Stage the complete replacement before publishing, and reject an edit of a changed file.
pub(super) fn save_target(
    target: &Path,
    bytes: &[u8],
    source: Option<&EditSource>,
    pool: &Arc<BytePool>,
) -> io::Result<()> {
    let mut original = match std::fs::symlink_metadata(target) {
        Ok(metadata) => {
            if !metadata.is_file() {
                return Err(io::Error::other("the target is not a regular file"));
            }
            let mut options = OpenOptions::new();
            options.write(true).read(source.is_some());
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                // A replaced final symlink is not followed, and a raced FIFO cannot block.
                options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
            }
            #[cfg(windows)]
            {
                use std::os::windows::fs::OpenOptionsExt;
                use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
                options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
            }
            let file = options.open(target)?;
            #[cfg(windows)]
            windows_file_identity(&file)?;
            if !file.metadata()?.is_file() {
                return Err(io::Error::other("the target is not a regular file"));
            }
            single_link(&file)?;
            Some(file)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    if let Some(source) = source {
        source.verify_target(original.as_ref(), pool)?;
    }
    let metadata = original.as_ref().map(File::metadata).transpose()?;
    let parent = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut staged = Staged::new(parent, true)?;
    // An empty probe obtains the ordinary create mode/DACL under this directory without ever
    // exposing staged content, and without changing the process-global Unix umask.
    let defaults = if metadata.is_none() {
        Some(Staged::new(parent, false)?)
    } else {
        None
    };
    let file = staged.file.as_mut().expect("a new staging file is open");
    #[cfg(not(any(target_os = "macos", target_os = "freebsd")))]
    let permissions = match &metadata {
        Some(metadata) => metadata.permissions(),
        None => defaults
            .as_ref()
            .unwrap()
            .file
            .as_ref()
            .unwrap()
            .metadata()?
            .permissions(),
    };
    file.write_all(bytes)?;
    #[cfg(windows)]
    {
        let source = original
            .as_ref()
            .or_else(|| defaults.as_ref().and_then(|probe| probe.file.as_ref()))
            .unwrap();
        preserve_windows_access(source, file, original.is_some(), pool)?;
    }
    #[cfg(all(unix, not(any(target_os = "macos", target_os = "freebsd"))))]
    if let Some(metadata) = &metadata {
        preserve_owner(file, metadata)?;
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    if let Some(original) = &original {
        preserve_xattrs(original, file, pool)?;
    }
    #[cfg(not(any(target_os = "macos", target_os = "freebsd")))]
    file.set_permissions(permissions)?;
    #[cfg(target_os = "freebsd")]
    {
        let source = original
            .as_ref()
            .or_else(|| defaults.as_ref().and_then(|probe| probe.file.as_ref()))
            .unwrap();
        crate::platform::freebsd_fs::preserve(source, file, original.is_some(), pool)?;
    }
    #[cfg(target_os = "macos")]
    {
        let source = original
            .as_ref()
            .or_else(|| defaults.as_ref().and_then(|probe| probe.file.as_ref()))
            .unwrap();
        macos_access::preserve(source, file)?;
    }
    file.sync_all()?;
    if let Some(source) = source {
        source.verify_content(original.as_mut().ok_or_else(EditSource::changed)?)?;
        source.verify_target(original.as_ref(), pool)?;
    }
    if let Some(original) = &original {
        single_link(original)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let before = original.metadata()?;
            let now = std::fs::symlink_metadata(target)?;
            if !now.is_file() || before.dev() != now.dev() || before.ino() != now.ino() {
                return Err(io::Error::other(
                    "the target changed while the replacement was being saved",
                ));
            }
        }
        #[cfg(windows)]
        unchanged_windows_target(original, target)?;
    }
    drop(staged.file.take());
    publish(&staged.path, target, original.is_some())
}
