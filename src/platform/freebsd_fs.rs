//! FreeBSD native ACLs and extended attributes, including private staging on ACL-inheriting volumes.

use crate::lease::{BytePool, Owned, Owner};
use std::ffi::{CString, c_char, c_int, c_uint, c_void};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::Path;
use std::sync::Arc;

const ACL_TYPE_ACCESS: c_int = 2;
const ACL_TYPE_DEFAULT: c_int = 3;
const ACL_TYPE_NFS4: c_int = 4;
const ACL_FIRST_ENTRY: c_int = 0;

// FreeBSD's public <sys/acl.h> and renameat2 APIs; libc does not expose these functions.
unsafe extern "C" {
    fn renameat2(
        from_fd: c_int,
        from: *const c_char,
        to_fd: c_int,
        to: *const c_char,
        flags: c_uint,
    ) -> c_int;
    fn acl_get_fd_np(fd: c_int, kind: c_int) -> *mut c_void;
    fn acl_set_fd_np(fd: c_int, acl: *mut c_void, kind: c_int) -> c_int;
    fn acl_delete_fd_np(fd: c_int, kind: c_int) -> c_int;
    fn acl_free(acl: *mut c_void) -> c_int;
    fn acl_strip_np(acl: *mut c_void, recalculate_mask: c_int) -> *mut c_void;
    fn acl_is_trivial_np(acl: *mut c_void, trivial: *mut c_int) -> c_int;
    fn acl_cmp_np(first: *mut c_void, second: *mut c_void) -> c_int;
    fn acl_get_entry(acl: *mut c_void, entry_id: c_int, entry: *mut *mut c_void) -> c_int;
}

struct Acl {
    raw: *mut c_void,
    kind: c_int,
}

impl Drop for Acl {
    fn drop(&mut self) {
        // SAFETY: every Acl owns exactly one allocation from the native ACL API.
        unsafe { acl_free(self.raw) };
    }
}

impl Acl {
    fn read(file: &File, kind: c_int) -> io::Result<Self> {
        // SAFETY: the descriptor is live; the ACL allocation is checked and owned below.
        let raw = unsafe { acl_get_fd_np(file.as_raw_fd(), kind) };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { raw, kind })
    }

    fn set(&self, file: &File) -> io::Result<()> {
        // SAFETY: FreeBSD takes (fd, acl, type), and both objects remain live for the call.
        if unsafe { acl_set_fd_np(file.as_raw_fd(), self.raw, self.kind) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn trivial(&self) -> io::Result<bool> {
        let mut trivial = 0;
        // SAFETY: the native ACL is valid and the output points to writable storage.
        if unsafe { acl_is_trivial_np(self.raw, &mut trivial) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(trivial != 0)
    }

    fn empty(&self) -> io::Result<bool> {
        let mut entry = std::ptr::null_mut();
        // SAFETY: the allocation owns any entry the API returns; it is not freed separately.
        match unsafe { acl_get_entry(self.raw, ACL_FIRST_ENTRY, &mut entry) } {
            0 => Ok(true),
            1 => Ok(false),
            _ => Err(io::Error::last_os_error()),
        }
    }

    fn stripped(&self) -> io::Result<Self> {
        // SAFETY: the returned ACL is a separately owned copy with extended entries removed.
        // Recalculating the POSIX mask would add a fourth entry and keep the ACL non-trivial.
        let raw = unsafe { acl_strip_np(self.raw, 0) };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            raw,
            kind: self.kind,
        })
    }
}

fn unsupported(error: &io::Error) -> bool {
    matches!(
        error.raw_os_error(),
        Some(libc::EINVAL) | Some(libc::EOPNOTSUPP)
    )
}

fn access_acl(file: &File) -> io::Result<Option<Acl>> {
    for kind in [ACL_TYPE_NFS4, ACL_TYPE_ACCESS] {
        match Acl::read(file, kind) {
            Ok(acl) => return Ok(Some(acl)),
            Err(error) if unsupported(&error) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

/// Existing runtime directories are checked without changing their owner's policy.
pub fn check_private_acl(file: &File) -> io::Result<()> {
    if let Some(acl) = access_acl(file)? {
        if !acl.trivial()? {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "a private directory has a non-trivial ACL",
            ));
        }
        if acl.kind == ACL_TYPE_ACCESS && !Acl::read(file, ACL_TYPE_DEFAULT)?.empty()? {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "a private directory has an inheritable default ACL",
            ));
        }
    }
    Ok(())
}

fn open_directory(dir: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(dir)
}

/// Checks the opened leaf's owner, mode, and ACL.
pub(super) fn check_private_directory(dir: &Path) -> io::Result<()> {
    let file = open_directory(dir)?;
    crate::platform::private_unix::check_metadata(dir, &file.metadata()?)?;
    check_private_acl(&file)
}

/// The directory stays empty until its ACL is private; an old directory handle cannot read
/// the subsequently created 0600 file. Never sanitize an existing directory or remove its contents.
pub fn create_private_directory(dir: &Path) -> io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;

    fs::DirBuilder::new().mode(0o700).create(dir)?;
    let result = (|| {
        let file = open_directory(dir).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("cannot open the new private directory: {error}"),
            )
        })?;
        // SAFETY: geteuid has no preconditions.
        if file.metadata()?.uid() != unsafe { libc::geteuid() } {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "the new directory changed owner",
            ));
        }
        if let Some(acl) = access_acl(&file)? {
            if acl.kind == ACL_TYPE_ACCESS {
                // SAFETY: the live descriptor names this new directory; only its default ACL is removed.
                if unsafe { acl_delete_fd_np(file.as_raw_fd(), ACL_TYPE_DEFAULT) } != 0 {
                    return Err(io::Error::last_os_error());
                }
            }
            acl.stripped()?.set(&file).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("cannot remove inherited directory ACL grants: {error}"),
                )
            })?;
        }
        // ZFS aclmode=restricted accepts chmod only after the ACL has become trivial.
        file.set_permissions(fs::Permissions::from_mode(0o700))
            .map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("cannot set private directory mode: {error}"),
                )
            })?;
        check_private_acl(&file)?;
        let held = file.metadata()?;
        let named = fs::symlink_metadata(dir)?;
        if held.dev() != named.dev() || held.ino() != named.ino() || !named.is_dir() {
            return Err(io::Error::other("the new private directory was replaced"));
        }
        check_private_directory(dir)?;
        if fs::read_dir(dir)?.next().transpose()?.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "the new private directory is not empty",
            ));
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_dir(dir);
    }
    result
}

/// Missing ancestors are restricted before they can hold any state; existing ancestors are
/// ordinary path components, while the requested leaf must always pass the private-directory check.
pub fn create_private_all(dir: &Path) -> io::Result<()> {
    match fs::symlink_metadata(dir) {
        Ok(_) => return check_private_directory(dir),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    if let Some(parent) = dir.parent().filter(|parent| !parent.as_os_str().is_empty())
        && !parent.try_exists()?
    {
        create_private_all(parent)?;
    }
    publish_private_directory(dir)
}

/// Other starters must not see the directory until its ACL and empty-state checks finish.
/// NOREPLACE is atomic on FreeBSD 15.1 UFS and ZFS; an unsupported filesystem fails closed.
fn publish_private_directory(dir: &Path) -> io::Result<()> {
    let destination = CString::new(dir.as_os_str().as_bytes())?;
    let parent = dir
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    for _ in 0..16 {
        let temporary = parent.join(format!(
            ".fastexec-private-{}",
            crate::id::random_chars(16)?
        ));
        match create_private_directory(&temporary) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
        // Both paths have the same parent, already checked for interior NULs above.
        let source = CString::new(temporary.as_os_str().as_bytes()).unwrap();
        const RENAME_NOREPLACE: c_uint = 0x0001;
        // SAFETY: the live C strings name the fully prepared sibling and final directory.
        let renamed = unsafe {
            renameat2(
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                destination.as_ptr(),
                RENAME_NOREPLACE,
            )
        };
        if renamed == 0 {
            // The temporary name no longer exists. Never clean up the published directory.
            return check_private_directory(dir);
        }
        let error = io::Error::last_os_error();
        // Only our random, empty sibling is removed, never the winner or its contents.
        fs::remove_dir(&temporary)?;
        return if error.kind() == io::ErrorKind::AlreadyExists {
            check_private_directory(dir)
        } else {
            Err(error)
        };
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "cannot choose a new private directory name",
    ))
}

fn attribute_memory_error(pool: &BytePool) -> io::Error {
    io::Error::new(
        io::ErrorKind::OutOfMemory,
        format!(
            "Server active memory budget ({} bytes) is full while preserving file metadata; the original file was not replaced.",
            pool.limit()
        ),
    )
}

/// FreeBSD extattr reads may return a truncated prefix instead of ERANGE.
fn read_sized(
    mut read: impl FnMut(*mut c_void, usize) -> isize,
    pool: &Arc<BytePool>,
) -> io::Result<Owned<Vec<u8>>> {
    let size = read(std::ptr::null_mut(), 0);
    if size < 0 {
        return Err(io::Error::last_os_error());
    }
    let memory = || attribute_memory_error(pool);
    let mut lease = pool.try_acquire(size as usize).ok_or_else(memory)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size as usize)
        .map_err(|_| memory())?;
    if !lease.try_resize(bytes.capacity()) {
        return Err(memory());
    }
    bytes.resize(size as usize, 0);
    let copied = read(bytes.as_mut_ptr().cast(), bytes.len());
    if copied < 0 {
        return Err(io::Error::last_os_error());
    }
    let after = read(std::ptr::null_mut(), 0);
    if after < 0 {
        return Err(io::Error::last_os_error());
    }
    if copied != size || after != size {
        return Err(io::Error::other(
            "an extended attribute changed while it was being copied",
        ));
    }
    Ok(Owned::new(bytes, Owner::new(lease)))
}

fn attribute_names(bytes: &[u8], pool: &Arc<BytePool>) -> io::Result<Owned<Vec<CString>>> {
    let mut count = 0usize;
    let mut longest = 0usize;
    let mut remaining = bytes;
    while let Some((&length, tail)) = remaining.split_first() {
        let length = usize::from(length);
        if length == 0 || length > tail.len() {
            return Err(io::Error::other("a truncated extended attribute name list"));
        }
        if tail[..length].contains(&0) {
            return Err(io::Error::other(
                "an extended attribute name contains a NUL byte",
            ));
        }
        count += 1;
        longest = longest.max(length + 1);
        remaining = &tail[length..];
    }
    let memory = || attribute_memory_error(pool);
    // Each length prefix becomes a NUL. CString conversion may briefly own
    // both its Vec and boxed slice, alongside all completed names and the raw list.
    let peak = count
        .checked_mul(std::mem::size_of::<CString>())
        .and_then(|backing| backing.checked_add(bytes.len()))
        .and_then(|retained| retained.checked_add(longest))
        .ok_or_else(memory)?;
    let mut lease = pool.try_acquire(peak).ok_or_else(memory)?;
    let mut names = Vec::new();
    names.try_reserve_exact(count).map_err(|_| memory())?;
    let retained = names
        .capacity()
        .checked_mul(std::mem::size_of::<CString>())
        .and_then(|backing| backing.checked_add(bytes.len()))
        .ok_or_else(memory)?;
    let peak = retained.checked_add(longest).ok_or_else(memory)?;
    if !lease.try_resize(peak) {
        return Err(memory());
    }
    let mut remaining = bytes;
    while let Some((&length, tail)) = remaining.split_first() {
        let length = usize::from(length);
        let mut name = Vec::new();
        name.try_reserve_exact(length + 1).map_err(|_| memory())?;
        if !lease.try_grow(name.capacity().saturating_sub(length + 1)) {
            return Err(memory());
        }
        name.extend_from_slice(&tail[..length]);
        name.push(0);
        names.push(CString::from_vec_with_nul(name).expect("the name has one trailing NUL"));
        lease.shrink_to(peak);
        remaining = &tail[length..];
    }
    lease.shrink_to(retained);
    Ok(Owned::new(names, Owner::new(lease)))
}

fn list_attributes(
    file: &File,
    namespace: c_int,
    pool: &Arc<BytePool>,
) -> io::Result<Owned<Vec<CString>>> {
    let bytes = read_sized(
        |buffer, size| {
            // SAFETY: file is live and read_sized supplies valid query/read buffers.
            unsafe { libc::extattr_list_fd(file.as_raw_fd(), namespace, buffer, size) }
        },
        pool,
    )?;
    attribute_names(&bytes.value, pool)
}

fn native_acl_attribute(name: &CString, namespace: c_int) -> bool {
    namespace == libc::EXTATTR_NAMESPACE_SYSTEM
        && matches!(
            name.to_bytes(),
            b"posix1e.acl_access" | b"posix1e.acl_default" | b"nfs4.acl"
        )
}

/// The kernel reports which objects loaded MAC policies label. A missing MAC framework has
/// no labels; an unavailable query must never be treated as proof that labels are absent.
fn mac_labels_present() -> io::Result<bool> {
    let mut labeled: u64 = 0;
    let mut size = std::mem::size_of_val(&labeled);
    // SAFETY: the name is terminated and the output has the size supplied to sysctl.
    if unsafe {
        libc::sysctlbyname(
            c"security.mac.labeled".as_ptr(),
            (&mut labeled as *mut u64).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    } != 0
    {
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ENOENT) {
            return Ok(false);
        }
        return Err(error);
    }
    if size != std::mem::size_of_val(&labeled) {
        return Err(io::Error::other("unexpected MAC label status size"));
    }
    // MPC_OBJECT_VNODE from FreeBSD sys/security/mac/mac_internal.h.
    Ok(labeled & 0x4 != 0)
}

fn preserve_attributes(
    source: &File,
    target: &File,
    preserve_system: bool,
    pool: &Arc<BytePool>,
) -> io::Result<()> {
    for namespace in [libc::EXTATTR_NAMESPACE_USER, libc::EXTATTR_NAMESPACE_SYSTEM] {
        if namespace == libc::EXTATTR_NAMESPACE_SYSTEM && !preserve_system {
            // A new vnode already has the kernel's creation label. Copying a probe's persistent
            // MAC attributes would change storage without updating that in-memory label.
            continue;
        }
        let names = match list_attributes(source, namespace, pool) {
            Ok(names) => names,
            Err(error) if error.raw_os_error() == Some(libc::EOPNOTSUPP) => continue,
            Err(error)
                if namespace == libc::EXTATTR_NAMESPACE_SYSTEM
                    && matches!(error.raw_os_error(), Some(libc::EPERM) | Some(libc::EACCES)) =>
            {
                // Only privileged callers may inspect SYSTEM, even when it is empty.
                // Native ACLs are preserved separately through the public ACL API.
                continue;
            }
            Err(error) => return Err(error),
        };
        let inherited = list_attributes(target, namespace, pool)?;
        for name in &inherited.value {
            if native_acl_attribute(name, namespace) || names.value.contains(name) {
                continue;
            }
            // SAFETY: the descriptor and terminated name remain valid for the call.
            if unsafe { libc::extattr_delete_fd(target.as_raw_fd(), namespace, name.as_ptr()) } != 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        drop(inherited);
        for name in &names.value {
            if native_acl_attribute(name, namespace) {
                continue;
            }
            let bytes = read_sized(
                |buffer, size| {
                    // SAFETY: the descriptor/name are live and the supplied buffer is writable.
                    unsafe {
                        libc::extattr_get_fd(
                            source.as_raw_fd(),
                            namespace,
                            name.as_ptr(),
                            buffer,
                            size,
                        )
                    }
                },
                pool,
            )?;
            // SAFETY: the descriptor, terminated name and complete value stay live.
            let copied = unsafe {
                libc::extattr_set_fd(
                    target.as_raw_fd(),
                    namespace,
                    name.as_ptr(),
                    bytes.value.as_ptr().cast(),
                    bytes.value.len(),
                )
            };
            if copied < 0 {
                return Err(io::Error::last_os_error());
            }
            if copied as usize != bytes.value.len() {
                return Err(io::Error::other(
                    "an extended attribute was not saved completely",
                ));
            }
        }
        let after = list_attributes(source, namespace, pool)?;
        if after.value != names.value {
            return Err(io::Error::other(
                "extended attribute names changed while being copied",
            ));
        }
    }
    Ok(())
}

/// Restore access only after all content is complete. chmod precedes the ACL because ZFS
/// aclmode=discard would otherwise remove the original named grants.
pub fn preserve(
    source: &File,
    target: &File,
    existing: bool,
    pool: &Arc<BytePool>,
) -> io::Result<()> {
    // Copying persistent SYSTEM attributes does not relabel the target's in-memory vnode.
    // Even a privileged caller cannot preserve active MAC policy through this atomic replacement.
    let mac_labels = mac_labels_present()?;
    if existing && mac_labels {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "cannot atomically replace an existing file while vnode MAC labels are active; use exec for an explicit in-place update",
        ));
    }
    let acl = access_acl(source)?;
    preserve_attributes(source, target, !mac_labels, pool)?;
    let original = source.metadata()?;
    let current = target.metadata()?;
    if original.uid() != current.uid() || original.gid() != current.gid() {
        // SAFETY: the descriptor names the completed staging file.
        if unsafe { libc::fchown(target.as_raw_fd(), original.uid(), original.gid()) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    target.set_permissions(original.permissions())?;
    if let Some(acl) = acl {
        acl.set(target)?;
        let copied = Acl::read(target, acl.kind)?;
        // SAFETY: both ACL allocations are live and have the same brand.
        match unsafe { acl_cmp_np(acl.raw, copied.raw) } {
            0 => {}
            -1 => return Err(io::Error::last_os_error()),
            _ => return Err(io::Error::other("the native ACL was not preserved")),
        }
    }
    let saved = target.metadata()?;
    if saved.uid() != original.uid()
        || saved.gid() != original.gid()
        || saved.mode() & 0o7777 != original.mode() & 0o7777
    {
        return Err(io::Error::other(
            "the original owner, group or mode was not preserved",
        ));
    }
    Ok(())
}

#[cfg(test)]
pub fn test_acl_is_nfs4(file: &File) -> io::Result<Option<bool>> {
    Ok(access_acl(file)?.map(|acl| acl.kind == ACL_TYPE_NFS4))
}

#[cfg(test)]
pub fn test_equal_acl(first: &File, second: &File) -> io::Result<bool> {
    match (access_acl(first)?, access_acl(second)?) {
        (Some(first), Some(second)) if first.kind == second.kind => {
            // SAFETY: both allocations have the same ACL brand and remain live.
            match unsafe { acl_cmp_np(first.raw, second.raw) } {
                0 => Ok(true),
                -1 => Err(io::Error::last_os_error()),
                _ => Ok(false),
            }
        }
        (None, None) => Ok(true),
        _ => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn concurrent_private_directory_creators_can_immediately_create_contents() {
        let root = std::env::temp_dir().join(format!(
            "fastexec-private-concurrent-{}-{}",
            std::process::id(),
            crate::id::random_chars(16).unwrap()
        ));
        create_private_directory(&root).unwrap();
        let mut failures = Vec::new();
        for iteration in 0..64 {
            let directory = root.join(iteration.to_string());
            let barrier = std::sync::Barrier::new(8);
            let results = std::thread::scope(|scope| {
                let threads: Vec<_> = (0..8)
                    .map(|slot| {
                        let directory = &directory;
                        let barrier = &barrier;
                        scope.spawn(move || {
                            barrier.wait();
                            create_private_all(directory)?;
                            fs::write(directory.join(slot.to_string()), b"ready")
                        })
                    })
                    .collect();
                threads
                    .into_iter()
                    .map(|thread| thread.join().unwrap())
                    .collect::<Vec<_>>()
            });
            failures.extend(results.into_iter().filter_map(Result::err));
            if !failures.is_empty() {
                break;
            }
            assert_eq!(fs::read_dir(&directory).unwrap().count(), 8);
            check_private_directory(&directory).unwrap();
        }
        fs::remove_dir_all(root).unwrap();
        assert!(
            failures.is_empty(),
            "concurrent initialization failed: {failures:?}"
        );
    }

    #[test]
    fn private_directory_publication_preserves_existing_entries() {
        let root = std::env::temp_dir().join(format!(
            "fastexec-private-publish-{}-{}",
            std::process::id(),
            crate::id::random_chars(16).unwrap()
        ));
        create_private_directory(&root).unwrap();
        let directory = root.join("winner");
        create_private_directory(&directory).unwrap();
        fs::write(directory.join("payload"), b"winner data").unwrap();
        let inode = fs::metadata(&directory).unwrap().ino();
        publish_private_directory(&directory).unwrap();
        assert_eq!(fs::metadata(&directory).unwrap().ino(), inode);
        assert_eq!(fs::read(directory.join("payload")).unwrap(), b"winner data");

        let file = root.join("file");
        fs::write(&file, b"original data").unwrap();
        let inode = fs::metadata(&file).unwrap().ino();
        assert!(publish_private_directory(&file).is_err());
        assert_eq!(fs::metadata(&file).unwrap().ino(), inode);
        assert_eq!(fs::read(&file).unwrap(), b"original data");

        let link = root.join("link");
        std::os::unix::fs::symlink("winner", &link).unwrap();
        let inode = fs::symlink_metadata(&link).unwrap().ino();
        assert!(publish_private_directory(&link).is_err());
        assert_eq!(fs::symlink_metadata(&link).unwrap().ino(), inode);
        assert_eq!(fs::read_link(&link).unwrap(), Path::new("winner"));
        assert_eq!(fs::read_dir(&root).unwrap().count(), 3);
        fs::remove_dir_all(root).unwrap();
    }

    fn read_attribute_fixture(bytes: &[u8], pool: &Arc<BytePool>) -> io::Result<Owned<Vec<u8>>> {
        read_sized(
            |buffer, length| {
                if !buffer.is_null() {
                    assert_eq!(length, bytes.len());
                    // SAFETY: the queried length is the complete fixture length.
                    unsafe {
                        std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.cast(), bytes.len())
                    };
                }
                bytes.len() as isize
            },
            pool,
        )
    }

    fn names_fixture(bytes: &[u8], pool: &Arc<BytePool>) -> io::Result<Owned<Vec<CString>>> {
        let raw = read_attribute_fixture(bytes, pool)?;
        attribute_names(&raw.value, pool)
    }

    #[test]
    fn freebsd_attribute_buffers_admit_before_read_and_release_after_failure() {
        for limit in [3, 4, 5] {
            let pool = BytePool::new(limit);
            let mut calls = 0;
            let result = read_sized(
                |buffer, length| {
                    calls += 1;
                    if !buffer.is_null() {
                        assert_eq!(length, 4);
                        assert_eq!(pool.used(), 4);
                        // SAFETY: read_sized supplied the queried four-byte buffer.
                        unsafe {
                            std::ptr::copy_nonoverlapping(b"data".as_ptr(), buffer.cast(), 4)
                        };
                    }
                    4
                },
                &pool,
            );
            if limit < 4 {
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::OutOfMemory);
                assert_eq!(calls, 1, "refusal precedes the native copy");
            } else {
                let result = result.unwrap();
                assert_eq!(result.value, b"data");
                assert_eq!(pool.used(), result.value.capacity());
                assert_eq!(calls, 3);
                drop(result);
            }
            assert_eq!(pool.used(), 0);
        }
        for fail_at in [2, 3] {
            let pool = BytePool::new(4);
            let mut calls = 0;
            let result = read_sized(
                |_, _| {
                    calls += 1;
                    if calls == fail_at { -1 } else { 4 }
                },
                &pool,
            );
            assert!(result.is_err());
            assert_eq!(calls, fail_at);
            assert_eq!(pool.used(), 0);
        }
        let pool = BytePool::new(0);
        assert!(read_attribute_fixture(b"", &pool).unwrap().value.is_empty());
        assert!(attribute_names(b"", &pool).unwrap().value.is_empty());
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn freebsd_attribute_names_admit_conversion_peak_and_keep_actual_capacity() {
        let bytes = b"\x03one\x03two";
        let retained = 2 * std::mem::size_of::<CString>() + bytes.len();
        let peak = bytes.len() + retained + 4;
        for limit in [peak - 1, peak, peak + 1] {
            let pool = BytePool::new(limit);
            let raw = read_attribute_fixture(bytes, &pool).unwrap();
            let names = attribute_names(&raw.value, &pool);
            if limit < peak {
                assert_eq!(names.unwrap_err().kind(), io::ErrorKind::OutOfMemory);
                assert_eq!(pool.used(), raw.value.capacity());
                drop(raw);
            } else {
                let names = names.unwrap();
                assert_eq!(
                    names.value,
                    [CString::new("one").unwrap(), CString::new("two").unwrap()]
                );
                let actual = names.value.capacity() * std::mem::size_of::<CString>()
                    + names
                        .value
                        .iter()
                        .map(|name| name.to_bytes_with_nul().len())
                        .sum::<usize>();
                assert_eq!(actual, retained);
                assert_eq!(pool.used(), raw.value.capacity() + actual);
                drop(raw);
                assert_eq!(pool.used(), actual);
                drop(names);
            }
            assert_eq!(pool.used(), 0);
        }
    }

    #[test]
    fn freebsd_attribute_groups_and_values_hold_independent_leases() {
        let bytes = b"\x03one";
        let retained = std::mem::size_of::<CString>() + bytes.len();
        let peak = 3 * retained + 2 + bytes.len() + 4;
        for limit in [peak - 1, peak, peak + 1] {
            let pool = BytePool::new(limit);
            let source = names_fixture(bytes, &pool).unwrap();
            let target = names_fixture(bytes, &pool).unwrap();
            let value = read_attribute_fixture(b"ok", &pool).unwrap();
            assert_eq!(pool.used(), 2 * retained + 2);
            let after = names_fixture(bytes, &pool);
            if limit < peak {
                assert_eq!(after.unwrap_err().kind(), io::ErrorKind::OutOfMemory);
            } else {
                let after = after.unwrap();
                assert_eq!(after.value, source.value);
                assert_eq!(pool.used(), 3 * retained + 2);
                drop(after);
            }
            assert_eq!(pool.used(), 2 * retained + 2);
            drop(value);
            assert_eq!(pool.used(), 2 * retained);
            drop(target);
            assert_eq!(pool.used(), retained);
            drop(source);
            assert_eq!(pool.used(), 0);
        }
    }

    #[test]
    fn freebsd_attribute_names_reject_partial_or_invalid_entries() {
        let pool = BytePool::new(128);
        let names = attribute_names(b"\x03one\x03two", &pool).unwrap();
        assert_eq!(
            names.value,
            [CString::new("one").unwrap(), CString::new("two").unwrap()]
        );
        drop(names);
        for malformed in [b"\x03on".as_slice(), b"\x00", b"\x03o\0e"] {
            assert!(attribute_names(malformed, &pool).is_err(), "{malformed:?}");
            assert_eq!(pool.used(), 0);
        }
    }

    #[test]
    fn freebsd_attribute_growth_is_not_saved_as_a_truncated_value() {
        let pool = BytePool::new(2);
        let mut calls = 0;
        let result = read_sized(
            |buffer, length| {
                calls += 1;
                if calls == 1 {
                    return 2;
                }
                if length != 0 {
                    // SAFETY: read_sized allocated this two-byte buffer.
                    unsafe { std::ptr::copy_nonoverlapping(b"ab".as_ptr(), buffer.cast(), 2) };
                    2
                } else {
                    3
                }
            },
            &pool,
        );
        assert!(result.is_err());
        assert_eq!(calls, 3);
        assert_eq!(pool.used(), 0);
    }
}
