//! Two-phase anchored directory handle for symlink-free, TOCTOU-resistant traversal and reads.

use std::ffi::OsString;
use std::fs::File;
use std::io;
use std::path::Path;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EntryKind {
    File,
    Directory,
    Link,
    Other,
}

#[derive(Debug)]
pub(crate) struct DirEntry {
    pub name: OsString,
    pub kind: EntryKind,
}

/// Strips redundant trailing path separators while keeping filesystem roots (`/`, `C:\`,
/// `\\?\C:\`, UNC roots) intact so final-component symlinks cannot bypass `O_NOFOLLOW` or
/// `symlink_metadata`.
fn strip_trailing_separators(root: &Path) -> std::path::PathBuf {
    if root.as_os_str().is_empty() {
        return root.to_path_buf();
    }
    root.components().collect()
}

#[cfg(unix)]
#[derive(Debug)]
pub(crate) struct DirFd {
    fd: std::os::fd::OwnedFd,
}

#[cfg(unix)]
impl DirFd {
    pub(crate) fn heap_bytes(&self) -> usize {
        0
    }

    pub(crate) fn open_root(root: &Path) -> io::Result<Self> {
        use std::ffi::CString;
        use std::os::fd::FromRawFd;
        use std::os::unix::ffi::OsStrExt;

        let root = strip_trailing_separators(root);
        let meta = std::fs::symlink_metadata(&root)?;
        if meta.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "skill root must not be a symlink",
            ));
        }
        if !meta.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                "skill root must be a directory",
            ));
        }
        let c_path = CString::new(root.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains a NUL byte"))?;
        // SAFETY: c_path is NUL-terminated; O_NOFOLLOW rejects a final-component symlink
        // while still permitting ancestor symlinks such as /var -> /private/var.
        let raw = unsafe {
            libc::open(
                c_path.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: raw is a newly opened directory file descriptor.
        let fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(raw) };
        Ok(Self { fd })
    }

    pub(crate) fn open_subdir(&self, segment: &str) -> io::Result<Self> {
        use std::os::fd::{AsRawFd, FromRawFd};

        let c_seg = segment_cstring(segment)?;
        // SAFETY: self.fd is live and c_seg is a NUL-terminated single path segment.
        let raw = unsafe {
            libc::openat(
                self.fd.as_raw_fd(),
                c_seg.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: raw is a newly opened directory file descriptor.
        let fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(raw) };
        Ok(Self { fd })
    }

    pub(crate) fn open_file_segment(&self, segment: &str) -> io::Result<(File, u64)> {
        use std::os::fd::{AsRawFd, FromRawFd};

        let c_seg = segment_cstring(segment)?;
        // SAFETY: O_NONBLOCK prevents blocking if a FIFO was swapped into the path.
        let raw = unsafe {
            libc::openat(
                self.fd.as_raw_fd(),
                c_seg.as_ptr(),
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
            )
        };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: raw is a newly opened file descriptor owned by this File.
        let file = unsafe { File::from_raw_fd(raw) };
        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: file.as_raw_fd() is live and stat is valid writable memory.
        if unsafe { libc::fstat(file.as_raw_fd(), &mut stat) } != 0 {
            return Err(io::Error::last_os_error());
        }
        if (stat.st_mode & libc::S_IFMT) != libc::S_IFREG {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "skill resource is not a regular file",
            ));
        }
        let size = u64::try_from(stat.st_size)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative file size"))?;
        Ok((file, size))
    }

    pub(crate) fn open_file(&self, segments: &[&str]) -> io::Result<(File, u64)> {
        let (last, parents) = segments.split_last().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "file path must not be empty")
        })?;
        if parents.is_empty() {
            return self.open_file_segment(last);
        }
        let mut current = self.open_subdir(parents[0])?;
        for segment in &parents[1..] {
            current = current.open_subdir(segment)?;
        }
        current.open_file_segment(last)
    }

    pub(crate) fn read_entries(&self, limit: usize) -> io::Result<Vec<DirEntry>> {
        use std::ffi::CStr;
        use std::os::fd::AsRawFd;
        use std::os::unix::ffi::OsStringExt;

        // Reopen "." relative to self.fd so each enumeration gets an independent cursor.
        // SAFETY: self.fd is live and c"." is a valid NUL-terminated path.
        let raw = unsafe {
            libc::openat(
                self.fd.as_raw_fd(),
                c".".as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fdopendir takes ownership of raw on success; close raw on failure.
        let dir = unsafe { libc::fdopendir(raw) };
        if dir.is_null() {
            let err = io::Error::last_os_error();
            // SAFETY: raw was not adopted by fdopendir.
            unsafe { libc::close(raw) };
            return Err(err);
        }
        struct DirGuard(*mut libc::DIR);
        impl Drop for DirGuard {
            fn drop(&mut self) {
                // SAFETY: self.0 was returned by fdopendir and is closed once here.
                unsafe { libc::closedir(self.0) };
            }
        }
        let _guard = DirGuard(dir);

        let mut entries = Vec::new();
        loop {
            clear_errno();
            // SAFETY: dir is live and owned exclusively by this call.
            let ent = unsafe { libc::readdir(dir) };
            if ent.is_null() {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(0) {
                    break;
                }
                return Err(err);
            }
            // SAFETY: ent is non-null and d_name is NUL-terminated.
            let c_name = unsafe { CStr::from_ptr((*ent).d_name.as_ptr()) };
            let bytes = c_name.to_bytes();
            if bytes == b"." || bytes == b".." {
                continue;
            }
            if entries.len() >= limit {
                return Err(io::Error::other("directory exceeds entry limit"));
            }
            // SAFETY: ent is non-null.
            let d_type = unsafe { (*ent).d_type };
            let kind = match d_type {
                libc::DT_REG => EntryKind::File,
                libc::DT_DIR => EntryKind::Directory,
                libc::DT_LNK => EntryKind::Link,
                libc::DT_UNKNOWN => self.stat_entry_kind(c_name)?,
                _ => EntryKind::Other,
            };
            entries.push(DirEntry {
                name: OsString::from_vec(bytes.to_vec()),
                kind,
            });
        }
        Ok(entries)
    }

    fn stat_entry_kind(&self, c_name: &std::ffi::CStr) -> io::Result<EntryKind> {
        use std::os::fd::AsRawFd;

        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        // SAFETY: self.fd is live and c_name is NUL-terminated.
        if unsafe {
            libc::fstatat(
                self.fd.as_raw_fd(),
                c_name.as_ptr(),
                &mut stat,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(match stat.st_mode & libc::S_IFMT {
            libc::S_IFREG => EntryKind::File,
            libc::S_IFDIR => EntryKind::Directory,
            libc::S_IFLNK => EntryKind::Link,
            _ => EntryKind::Other,
        })
    }
}

#[cfg(unix)]
fn segment_cstring(segment: &str) -> io::Result<std::ffi::CString> {
    use std::ffi::{CString, OsStr};
    use std::os::unix::ffi::OsStrExt;

    if segment.is_empty()
        || segment == "."
        || segment == ".."
        || segment.contains('/')
        || segment.contains('\0')
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid relative path segment",
        ));
    }
    CString::new(OsStr::new(segment).as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "segment contains a NUL byte"))
}

#[cfg(unix)]
fn clear_errno() {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    unsafe {
        *libc::__errno_location() = 0;
    }
    #[cfg(any(target_os = "macos", target_os = "freebsd", target_os = "ios"))]
    unsafe {
        *libc::__error() = 0;
    }
}

#[cfg(windows)]
#[derive(Debug)]
pub(crate) struct DirFd {
    handle: File,
    canonical_root: String,
}

#[cfg(windows)]
impl DirFd {
    pub(crate) fn heap_bytes(&self) -> usize {
        self.canonical_root.capacity()
    }

    pub(crate) fn open_root(root: &Path) -> io::Result<Self> {
        use std::os::windows::fs::MetadataExt;
        use std::os::windows::io::FromRawHandle;
        use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
        use windows_sys::Win32::Storage::FileSystem::{
            CreateFileW, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS,
            FILE_FLAG_OPEN_REPARSE_POINT, FILE_LIST_DIRECTORY, FILE_READ_ATTRIBUTES,
            FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_TRAVERSE, OPEN_EXISTING,
            SYNCHRONIZE,
        };

        let root = strip_trailing_separators(root);
        let meta = std::fs::symlink_metadata(&root)?;
        if meta.file_type().is_symlink()
            || (meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT) != 0
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "skill root must not be a symlink or reparse point",
            ));
        }
        if !meta.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                "skill root must be a directory",
            ));
        }
        let wide = wide_nul(root.as_os_str())?;
        // SAFETY: wide is NUL-terminated; FILE_FLAG_OPEN_REPARSE_POINT avoids following a
        // final-component reparse point while allowing ancestor symlinks.
        let raw = unsafe {
            CreateFileW(
                wide.as_ptr(),
                FILE_LIST_DIRECTORY | FILE_TRAVERSE | FILE_READ_ATTRIBUTES | SYNCHRONIZE,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                std::ptr::null(),
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
                std::ptr::null_mut(),
            )
        };
        if raw == INVALID_HANDLE_VALUE || raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: raw is a valid newly opened directory handle.
        let handle = unsafe { File::from_raw_handle(raw) };
        let opened_meta = handle.metadata()?;
        if (opened_meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT) != 0
            || !opened_meta.is_dir()
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "skill root is a reparse point or not a directory",
            ));
        }
        let canonical_root = final_path_by_handle(&handle)?;
        Ok(Self {
            handle,
            canonical_root,
        })
    }

    pub(crate) fn open_subdir(&self, segment: &str) -> io::Result<Self> {
        let handle = open_relative_nt(&self.handle, segment, true)?;
        verify_beneath(&self.canonical_root, &handle)?;
        Ok(Self {
            handle,
            canonical_root: self.canonical_root.clone(),
        })
    }

    pub(crate) fn open_file_segment(&self, segment: &str) -> io::Result<(File, u64)> {
        let file = open_relative_nt(&self.handle, segment, false)?;
        verify_beneath(&self.canonical_root, &file)?;
        let meta = file.metadata()?;
        Ok((file, meta.len()))
    }

    pub(crate) fn open_file(&self, segments: &[&str]) -> io::Result<(File, u64)> {
        let (last, parents) = segments.split_last().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "file path must not be empty")
        })?;
        if parents.is_empty() {
            return self.open_file_segment(last);
        }
        let mut current = self.open_subdir(parents[0])?;
        for segment in &parents[1..] {
            current = current.open_subdir(segment)?;
        }
        current.open_file_segment(last)
    }

    pub(crate) fn read_entries(&self, limit: usize) -> io::Result<Vec<DirEntry>> {
        use std::os::windows::ffi::OsStringExt;
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Foundation::ERROR_NO_MORE_FILES;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_ATTRIBUTE_DEVICE, FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_REPARSE_POINT,
            FILE_ID_BOTH_DIR_INFO, FileIdBothDirectoryInfo, FileIdBothDirectoryRestartInfo,
            GetFileInformationByHandleEx,
        };

        let mut entries = Vec::new();
        let mut buffer = vec![0u64; 8192];
        let length = (buffer.len() * std::mem::size_of::<u64>()) as u32;
        let mut first = true;
        loop {
            let class = if first {
                first = false;
                FileIdBothDirectoryRestartInfo
            } else {
                FileIdBothDirectoryInfo
            };
            // SAFETY: self.handle is a live directory handle and buffer is writable.
            let ok = unsafe {
                GetFileInformationByHandleEx(
                    self.handle.as_raw_handle(),
                    class,
                    buffer.as_mut_ptr().cast(),
                    length,
                )
            };
            if ok == 0 {
                let err = io::Error::last_os_error();
                if err.raw_os_error() == Some(ERROR_NO_MORE_FILES as i32) {
                    break;
                }
                return Err(err);
            }
            let bytes = buffer.as_ptr().cast::<u8>();
            let mut offset = 0usize;
            loop {
                const NAME_OFFSET: usize = std::mem::offset_of!(FILE_ID_BOTH_DIR_INFO, FileName);
                if !offset.is_multiple_of(std::mem::align_of::<FILE_ID_BOTH_DIR_INFO>())
                    || offset + NAME_OFFSET > length as usize
                {
                    return Err(io::Error::other("invalid directory record"));
                }
                // SAFETY: offset is within bounds and aligned to FILE_ID_BOTH_DIR_INFO.
                let entry = unsafe { &*bytes.add(offset).cast::<FILE_ID_BOTH_DIR_INFO>() };
                let name_len = entry.FileNameLength as usize;
                if !name_len.is_multiple_of(2) || offset + NAME_OFFSET + name_len > length as usize
                {
                    return Err(io::Error::other("invalid directory filename"));
                }
                // SAFETY: slice lies inside buffer and has even byte length.
                let units = unsafe {
                    std::slice::from_raw_parts(
                        bytes.add(offset + NAME_OFFSET).cast::<u16>(),
                        name_len / 2,
                    )
                };
                let name = OsString::from_wide(units);
                if name != "." && name != ".." {
                    if entries.len() >= limit {
                        return Err(io::Error::other("directory exceeds entry limit"));
                    }
                    let kind = if (entry.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0 {
                        EntryKind::Link
                    } else if (entry.FileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 {
                        EntryKind::Directory
                    } else if (entry.FileAttributes & FILE_ATTRIBUTE_DEVICE) != 0 {
                        EntryKind::Other
                    } else {
                        EntryKind::File
                    };
                    entries.push(DirEntry { name, kind });
                }
                if entry.NextEntryOffset == 0 {
                    break;
                }
                if entry.NextEntryOffset < NAME_OFFSET as u32 {
                    return Err(io::Error::other("invalid directory offset"));
                }
                offset += entry.NextEntryOffset as usize;
            }
        }
        Ok(entries)
    }
}

#[cfg(windows)]
fn open_relative_nt(parent: &File, segment: &str, directory: bool) -> io::Result<File> {
    use std::ffi::OsStr;
    use std::os::windows::ffi::OsStrExt;
    use std::os::windows::fs::MetadataExt;
    use std::os::windows::io::{AsRawHandle, FromRawHandle};
    use windows_sys::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows_sys::Wdk::Storage::FileSystem::{
        FILE_DIRECTORY_FILE, FILE_NON_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_REPARSE_POINT,
        FILE_SYNCHRONOUS_IO_NONALERT, NtCreateFile,
    };
    use windows_sys::Win32::Foundation::{
        CloseHandle, INVALID_HANDLE_VALUE, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE,
        RtlNtStatusToDosError, UNICODE_STRING,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_REPARSE_POINT, FILE_LIST_DIRECTORY,
        FILE_READ_ATTRIBUTES, FILE_READ_DATA, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
        FILE_TRAVERSE, FILE_TYPE_DISK, GetFileType, SYNCHRONIZE,
    };
    use windows_sys::Win32::System::IO::IO_STATUS_BLOCK;

    if segment.is_empty()
        || segment == "."
        || segment == ".."
        || segment.contains(['/', '\\', ':', '\0'])
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid relative path segment",
        ));
    }
    let mut units: Vec<u16> = OsStr::new(segment).encode_wide().collect();
    let byte_len = u16::try_from(units.len().saturating_mul(2))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "segment name is too long"))?;
    let mut name = UNICODE_STRING {
        Length: byte_len,
        MaximumLength: byte_len,
        Buffer: units.as_mut_ptr(),
    };
    let access = FILE_READ_ATTRIBUTES
        | SYNCHRONIZE
        | if directory {
            FILE_LIST_DIRECTORY | FILE_TRAVERSE
        } else {
            FILE_READ_DATA
        };
    let create_options = FILE_OPEN_REPARSE_POINT
        | FILE_SYNCHRONOUS_IO_NONALERT
        | if directory {
            FILE_DIRECTORY_FILE
        } else {
            FILE_NON_DIRECTORY_FILE
        };
    let attributes = OBJECT_ATTRIBUTES {
        Length: std::mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
        RootDirectory: parent.as_raw_handle(),
        ObjectName: &mut name,
        Attributes: OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
        SecurityDescriptor: std::ptr::null_mut(),
        SecurityQualityOfService: std::ptr::null_mut(),
    };
    let mut handle = std::ptr::null_mut();
    let mut status_block = IO_STATUS_BLOCK::default();
    // SAFETY: parent handle, UNICODE_STRING buffer, OBJECT_ATTRIBUTES, and IO_STATUS_BLOCK
    // remain live for the duration of NtCreateFile.
    let status = unsafe {
        NtCreateFile(
            &mut handle,
            access,
            &attributes,
            &mut status_block,
            std::ptr::null(),
            FILE_ATTRIBUTE_NORMAL,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            FILE_OPEN,
            create_options,
            std::ptr::null(),
            0,
        )
    };
    if status != 0 {
        if !handle.is_null() && handle != INVALID_HANDLE_VALUE {
            // SAFETY: close partially returned handle on error.
            unsafe { CloseHandle(handle) };
        }
        // SAFETY: RtlNtStatusToDosError maps any NTSTATUS to a Win32 error code.
        return Err(io::Error::from_raw_os_error(
            unsafe { RtlNtStatusToDosError(status) } as i32,
        ));
    }
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::other("NtCreateFile returned no handle"));
    }
    // SAFETY: handle is a valid newly opened kernel handle.
    let file = unsafe { File::from_raw_handle(handle) };
    // SAFETY: file owns this live handle.
    if !directory && unsafe { GetFileType(file.as_raw_handle()) } != FILE_TYPE_DISK {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "skill resource is not a regular file",
        ));
    }
    let meta = file.metadata()?;
    if (meta.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT) != 0
        || if directory {
            !meta.is_dir()
        } else {
            !meta.is_file()
        }
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "skill entry is a reparse point or unexpected file type",
        ));
    }
    Ok(file)
}

#[cfg(windows)]
fn final_path_by_handle(file: &File) -> io::Result<String> {
    use std::os::windows::ffi::OsStringExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{GetFinalPathNameByHandleW, VOLUME_NAME_DOS};

    // SAFETY: querying required buffer length with null pointer.
    let mut required = unsafe {
        GetFinalPathNameByHandleW(
            file.as_raw_handle(),
            std::ptr::null_mut(),
            0,
            VOLUME_NAME_DOS,
        )
    };
    if required == 0 {
        return Err(io::Error::last_os_error());
    }
    loop {
        let mut buf = vec![0u16; required as usize];
        // SAFETY: buf has `required` writable u16 slots.
        let count = unsafe {
            GetFinalPathNameByHandleW(
                file.as_raw_handle(),
                buf.as_mut_ptr(),
                required,
                VOLUME_NAME_DOS,
            )
        };
        if count == 0 {
            return Err(io::Error::last_os_error());
        }
        if count >= required {
            required = count;
            continue;
        }
        let os = OsString::from_wide(&buf[..count as usize]);
        let s = os.into_string().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "final path is not valid UTF-8")
        })?;
        return Ok(s.trim_end_matches('\\').to_owned());
    }
}

#[cfg(windows)]
fn verify_beneath(canonical_root: &str, handle: &File) -> io::Result<()> {
    let final_path = final_path_by_handle(handle)?;
    let root_lower = canonical_root.to_ascii_lowercase();
    let path_lower = final_path.to_ascii_lowercase();
    if let Some(rest) = path_lower.strip_prefix(&root_lower)
        && rest.starts_with('\\')
        && rest.len() > 1
    {
        return Ok(());
    }
    Err(io::Error::new(
        io::ErrorKind::PermissionDenied,
        "resolved handle escapes skill root",
    ))
}

#[cfg(windows)]
fn wide_nul(value: &std::ffi::OsStr) -> io::Result<Vec<u16>> {
    use std::os::windows::ffi::OsStrExt;

    let mut units: Vec<u16> = value.encode_wide().collect();
    if units.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path contains a NUL character",
        ));
    }
    units.push(0);
    Ok(units)
}
