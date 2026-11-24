use super::write::SaveBuffer;
use crate::lease::{BytePool, Lease};
use crate::session::LaunchContext;
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(super) fn path_workspace_bytes(path: &str, launch: &LaunchContext, extra: usize) -> usize {
    // Resolution, home expansion, native/display conversions, response copies and
    // growth can coexist. Borrow all source lengths before copying a path.
    launch
        .path_source_bytes(path)
        .saturating_mul(32)
        .saturating_add(extra)
        .saturating_add(4096)
}

pub(crate) fn with_path_workspace<T>(
    path: &str,
    launch: &LaunchContext,
    extra: usize,
    operation: impl FnOnce(&mut Lease) -> T,
) -> Result<crate::ipc::Owned<T>, String> {
    let mut lease = launch.memory().try_acquire(path_workspace_bytes(path, launch, extra))
        .ok_or_else(|| format!(
            "Server active memory budget ({} bytes) is full while preparing the file path; the file operation was not dispatched.",
            launch.memory().limit()
        ))?;
    let value = operation(&mut lease);
    Ok(crate::ipc::Owned::new(
        value,
        crate::lease::Owner::new(lease),
    ))
}

/// Formats a path for Agent text and Program data: forward slashes on Windows, stripping
/// verbatim `\\?\` prefixes and turning `\\server\share` into `//server/share`.
pub fn display_path(path: &Path) -> String {
    #[cfg(windows)]
    {
        format_windows_display(&path.to_string_lossy())
    }
    #[cfg(not(windows))]
    {
        path.display().to_string()
    }
}

#[cfg(any(windows, test))]
pub(super) fn format_windows_display(raw: &str) -> String {
    if raw.len() >= 8 && raw.as_bytes()[..8].eq_ignore_ascii_case(b"\\\\?\\UNC\\") {
        return format!("//{}", raw[8..].replace('\\', "/"));
    }
    let drive_path = raw.strip_prefix("\\\\?\\").filter(|rest| {
        rest.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
            && rest.as_bytes().get(1..3) == Some(b":\\")
    });
    // Volume GUID paths have no drive-letter equivalent; //?/Volume{...}/... round-trips.
    let mut shown = drive_path.unwrap_or(raw).replace('\\', "/");
    if shown.as_bytes().get(1) == Some(&b':') && shown.as_bytes()[0].is_ascii_lowercase() {
        let upper = (shown.as_bytes()[0] as char).to_ascii_uppercase();
        shown.replace_range(..1, &upper.to_string());
    }
    shown
}

/// Rewrites Git Bash `/<letter>` and `/<letter>/...` drive paths to `<LETTER>:/...`, and
/// forward-slash UNC `//server/share...` paths to `\\server\share...` so `std::path::Path`
/// parses `Component::Prefix(Prefix::UNC)` on Windows.
#[cfg(any(windows, test))]
pub(super) fn git_bash_drive_path(path: &str) -> Option<String> {
    if path.starts_with("//") && !path.starts_with("///") {
        let rest = &path[2..];
        if let Some((server, after)) = rest.split_once('/')
            && !server.is_empty()
        {
            let share = after.split('/').next().unwrap_or_default();
            if !share.is_empty() {
                return Some(format!("\\\\{}", rest.replace('/', "\\")));
            }
        }
    }
    let bytes = path.as_bytes();
    if bytes.first() == Some(&b'/') && bytes.len() >= 2 && bytes[1].is_ascii_alphabetic() {
        let drive = (bytes[1] as char).to_ascii_uppercase();
        if bytes.len() == 2 {
            return Some(format!("{drive}:/"));
        }
        if bytes[2] == b'/' {
            return Some(format!("{drive}:/{}", &path[3..]));
        }
    }
    None
}

/// A final separator or dot requires a directory even when the preceding component is a file.
pub(super) fn ends_in_directory(path: &Path) -> bool {
    let bytes = path.as_os_str().as_encoded_bytes();
    matches!(
        bytes
            .rsplit(|&b| std::path::is_separator(char::from(b)))
            .next(),
        Some(b"" | b"." | b"..")
    )
}

/// Rebuilds paths without `PathBuf::push`, which folds `..` on Windows verbatim paths.
fn clean_components(path: &Path) -> PathBuf {
    use std::path::Component;
    let mut out = std::ffi::OsString::new();
    let mut separator = false;
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Prefix(_) | Component::RootDir => {
                out.push(component.as_os_str());
                separator = false;
            }
            other => {
                if separator {
                    out.push(std::path::MAIN_SEPARATOR_STR);
                }
                out.push(other.as_os_str());
                separator = true;
            }
        }
    }
    if out.is_empty() {
        PathBuf::from(".")
    } else {
        if separator
            && ends_in_directory(path)
            && !matches!(path.components().next_back(), Some(Component::ParentDir))
        {
            out.push(std::path::MAIN_SEPARATOR_STR);
        }
        PathBuf::from(out)
    }
}

/// Joins without folding parent components, including when the base is a verbatim Windows path.
pub(super) fn append_path(base: &Path, path: &Path) -> PathBuf {
    use std::path::Component;

    if path.is_absolute() || matches!(path.components().next(), Some(Component::Prefix(_))) {
        return clean_components(path);
    }
    // A Windows root-relative target replaces the directory but keeps the drive or UNC prefix.
    let mut joined = if path.has_root() {
        match base.components().next() {
            Some(Component::Prefix(prefix)) => prefix.as_os_str().to_owned(),
            _ => std::ffi::OsString::new(),
        }
    } else {
        base.as_os_str().to_owned()
    };
    if !joined.is_empty()
        && !joined
            .as_encoded_bytes()
            .last()
            .is_some_and(|byte| std::path::is_separator(char::from(*byte)))
    {
        joined.push(std::path::MAIN_SEPARATOR_STR);
    }
    // Parse the appended part first so Windows also accepts its forward slashes.
    joined.push(clean_components(path));
    clean_components(Path::new(&joined))
}

/// Resolves a path parameter against `launch`: expands `~`, maps Git Bash `/c/...` drive roots on
/// Windows, refuses driveless rooted and drive-relative Windows paths, joins relative paths onto
/// `launch.cwd`, and removes `.` and repeated separators while leaving `..` to the filesystem.
pub fn resolve(path: &str, launch: &LaunchContext) -> Result<PathBuf, String> {
    if path.is_empty() {
        return Err("Path must not be empty.".into());
    }
    let home = || {
        launch
            .home()
            .ok_or_else(|| format!("Cannot expand ~ in {path}: the home directory is unknown."))
    };
    if path == "~" {
        return Ok(clean_components(&home()?));
    }
    let home_tail = path.strip_prefix("~/");
    #[cfg(windows)]
    let home_tail = home_tail.or_else(|| path.strip_prefix("~\\"));
    if let Some(rest) = home_tail {
        let relative = Path::new(rest.trim_start_matches(std::path::is_separator));
        if matches!(
            relative.components().next(),
            Some(std::path::Component::Prefix(_))
        ) {
            return Err(format!(
                "Cannot resolve {path}: the path after ~ must be relative to the home directory, without a Windows path prefix."
            ));
        }
        return resolve_from(relative, &home()?);
    }
    resolve_from(Path::new(path), &launch.cwd)
}

#[cfg(any(windows, test))]
pub(super) fn is_windows_reserved_device_name(component: &str) -> bool {
    let stem = component
        .split(['.', ':'])
        .next()
        .unwrap_or(component)
        .trim_end_matches(' ');
    if stem.eq_ignore_ascii_case("CON")
        || stem.eq_ignore_ascii_case("PRN")
        || stem.eq_ignore_ascii_case("AUX")
        || stem.eq_ignore_ascii_case("NUL")
    {
        return true;
    }
    let Some(prefix) = stem.get(..3) else {
        return false;
    };
    (prefix.eq_ignore_ascii_case("COM") || prefix.eq_ignore_ascii_case("LPT"))
        && matches!(
            &stem[3..],
            "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9" | "¹" | "²" | "³"
        )
}

/// Resolves a native or Git Bash path against a fixed base, without tilde expansion.
pub(crate) fn resolve_from(path: &Path, base: &Path) -> Result<PathBuf, String> {
    #[cfg(windows)]
    {
        use std::path::{Component, Prefix};

        let rewritten = path.to_str().and_then(git_bash_drive_path);
        let candidate = rewritten.as_deref().map_or(path, Path::new);
        let shown = path.to_string_lossy();
        let mut comps = candidate.components();
        let first = comps.next();
        let second = comps.next();
        let resolved = match (first, second) {
            (Some(Component::Prefix(prefix)), second) => match prefix.kind() {
                Prefix::DeviceNS(_) => {
                    return Err(format!(
                        "Cannot resolve {shown}: Windows device namespace paths are not supported."
                    ));
                }
                Prefix::UNC(_, share) | Prefix::VerbatimUNC(_, share)
                    if share.eq_ignore_ascii_case("pipe") =>
                {
                    return Err(format!(
                        "Cannot resolve {shown}: Windows named pipe paths are not supported."
                    ));
                }
                Prefix::Verbatim(name) if name.eq_ignore_ascii_case("pipe") => {
                    return Err(format!(
                        "Cannot resolve {shown}: Windows named pipe paths are not supported."
                    ));
                }
                Prefix::Disk(_) if second != Some(Component::RootDir) => {
                    return Err(format!(
                        "Cannot resolve {shown}: drive-relative paths such as C:file depend on a per-drive current directory; pass C:/... or a relative path."
                    ));
                }
                _ => clean_components(candidate),
            },
            (Some(Component::RootDir), _) => {
                return Err(format!(
                    "Cannot resolve {shown}: driveless paths such as /tmp name Git Bash locations on Windows; pass C:/... or /c/..., or convert it with cygpath -m."
                ));
            }
            _ => append_path(base, candidate),
        };
        for component in candidate.components() {
            if let Component::Normal(name) = component
                && is_windows_reserved_device_name(&name.to_string_lossy())
            {
                return Err(format!(
                    "Cannot resolve {shown}: Windows reserved device names are not supported."
                ));
            }
        }
        Ok(resolved)
    }
    #[cfg(not(windows))]
    {
        Ok(append_path(base, path))
    }
}

/// Both text readers reject special files before reading any data. The initial check avoids
/// opening devices at all; the handle check covers replacement between metadata and open. On
/// Unix, a FIFO swapped into that gap must not block open while edit holds the mutation lock.
pub(crate) fn open_regular(path: &Path) -> io::Result<(File, std::fs::Metadata)> {
    open_regular_for(path, false, false)
}

pub(crate) fn open_regular_for_upload(
    path: &Path,
    create: bool,
) -> io::Result<(File, std::fs::Metadata)> {
    open_regular_for(path, true, create)
}

fn open_regular_for(
    path: &Path,
    write: bool,
    create: bool,
) -> io::Result<(File, std::fs::Metadata)> {
    match std::fs::metadata(path) {
        Ok(initial) => check_regular(&initial)?,
        Err(error) if create && error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut options = OpenOptions::new();
    options.read(!write).write(write).create(create);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK | libc::O_NOCTTY);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::{SECURITY_ANONYMOUS, SECURITY_SQOS_PRESENT};
        options.security_qos_flags(SECURITY_SQOS_PRESENT | SECURITY_ANONYMOUS);
    }
    let file = options.open(path)?;
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{FILE_TYPE_DISK, GetFileType};
        // SAFETY: file owns this live handle. Reject named pipes/consoles before any read.
        if unsafe { GetFileType(file.as_raw_handle()) } != FILE_TYPE_DISK {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "it is not a regular file",
            ));
        }
    }
    let metadata = file.metadata()?;
    check_regular(&metadata)?;
    Ok((file, metadata))
}

fn check_regular(metadata: &std::fs::Metadata) -> io::Result<()> {
    if metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::IsADirectory,
            "it is a directory",
        ));
    }
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "it is not a regular file",
        ));
    }
    Ok(())
}

/// Space for native path arguments and conversions, in addition to the owned path itself.
/// UTF-16 input, normalized input and their growth can coexist on Windows. Unix's
/// terminated input copy needs less. The fixed part covers the short native-path buffer.
pub(crate) fn native_path_bytes(path: &Path) -> Option<usize> {
    path.as_os_str()
        .as_encoded_bytes()
        .len()
        .checked_add(8)?
        .checked_mul(16)?
        .checked_add(1024)
}

pub(super) fn target_memory_error(pool: &BytePool) -> io::Error {
    io::Error::new(
        io::ErrorKind::OutOfMemory,
        format!(
            "Server active memory budget ({} bytes) is full while preparing the file target; the original file was not replaced.",
            pool.limit()
        ),
    )
}

/// Saving may hold staging/default/backup paths, native argument buffers and formatted
/// target errors together. Keep that allowance in the outer result owner, including on Err.
pub(crate) fn reserve_target_workspace(
    path: &Path,
    lease: &mut Lease,
    pool: &BytePool,
) -> io::Result<()> {
    let bytes = path
        .as_os_str()
        .as_encoded_bytes()
        .len()
        .checked_add(64)
        .and_then(|n| n.checked_mul(64))
        .and_then(|n| n.checked_add(4096));
    if bytes.is_some_and(|bytes| lease.try_grow(bytes)) {
        Ok(())
    } else {
        Err(target_memory_error(pool))
    }
}

#[cfg(unix)]
pub(super) fn native_path_input(
    path: &Path,
    pool: &Arc<BytePool>,
) -> io::Result<crate::ipc::Owned<std::ffi::CString>> {
    use std::os::unix::ffi::OsStrExt;
    let bytes = native_path_bytes(path).ok_or_else(|| target_memory_error(pool))?;
    let lease = pool
        .try_acquire(bytes)
        .ok_or_else(|| target_memory_error(pool))?;
    let value = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains a NUL byte"))?;
    Ok(crate::ipc::Owned::new(
        value,
        crate::lease::Owner::new(lease),
    ))
}

/// Canonicalize into application-owned storage. Native-library internal scratch is
/// opaque; this lease covers our caller buffer and the returned PathBuf.
#[cfg(unix)]
pub(super) fn canonical_target(
    path: &Path,
    pool: &Arc<BytePool>,
) -> io::Result<crate::ipc::Owned<PathBuf>> {
    use std::os::unix::ffi::OsStringExt;
    let input = native_path_input(path, pool)?;
    // POSIX realpath's caller buffer includes its terminating NUL.
    let mut buffer = SaveBuffer::<u8>::new(libc::PATH_MAX as usize, pool)?;
    // SAFETY: input is terminated and the writable caller buffer has PATH_MAX bytes.
    let result = unsafe { libc::realpath(input.value.as_ptr(), buffer.data.as_mut_ptr().cast()) };
    if result.is_null() {
        // Preserve errno before dropping any buffer or running another native call.
        let error = io::Error::last_os_error();
        return Err(error);
    }
    let end = buffer
        .data
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| io::Error::other("the canonical path is not terminated"))?;
    let SaveBuffer {
        mut data,
        _lease: lease,
    } = buffer;
    data.truncate(end);
    let value = PathBuf::from(std::ffi::OsString::from_vec(data));
    Ok(crate::ipc::Owned::new(
        value,
        crate::lease::Owner::new(lease),
    ))
}

#[cfg(windows)]
pub(super) fn canonical_target(
    path: &Path,
    pool: &Arc<BytePool>,
) -> io::Result<crate::ipc::Owned<PathBuf>> {
    use std::os::windows::ffi::OsStringExt;
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, GetFinalPathNameByHandleW, VOLUME_NAME_DOS,
    };
    let input_bytes = native_path_bytes(path).ok_or_else(|| target_memory_error(pool))?;
    let _input_lease = pool
        .try_acquire(input_bytes)
        .ok_or_else(|| target_memory_error(pool))?;
    // Match std::fs::canonicalize: reopen the current name, with normalized DOS output.
    let file = OpenOptions::new()
        .access_mode(0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    // SAFETY: a zero-length call queries the required UTF-16 units, including NUL.
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
        let mut buffer = SaveBuffer::<u16>::new(required as usize, pool)?;
        // SAFETY: this buffer owns the advertised UTF-16 units through the call.
        let count = unsafe {
            GetFinalPathNameByHandleW(
                file.as_raw_handle(),
                buffer.data.as_mut_ptr(),
                required,
                VOLUME_NAME_DOS,
            )
        };
        if count == 0 {
            return Err(io::Error::last_os_error());
        }
        if count >= required {
            // The path changed after the query. Drop the old allocation before reserving
            // its replacement; a successful count excludes the final NUL.
            drop(buffer);
            required = count;
            continue;
        }
        // An unpaired surrogate needs three WTF-8 bytes; a pair needs four for two units.
        // Include String growth while the complete UTF-16 buffer remains owned.
        let bytes = (count as usize)
            .checked_mul(3)
            .and_then(|n| super::edit::edit_growth(n, 1))
            .ok_or_else(|| target_memory_error(pool))?;
        let mut lease = pool
            .try_acquire(bytes)
            .ok_or_else(|| target_memory_error(pool))?;
        let value = PathBuf::from(std::ffi::OsString::from_wide(
            &buffer.data[..count as usize],
        ));
        drop(buffer);
        lease.shrink_to(value.capacity());
        return Ok(crate::ipc::Owned::new(
            value,
            crate::lease::Owner::new(lease),
        ));
    }
}

#[cfg(unix)]
pub(super) fn read_target_link(
    path: &Path,
    pool: &Arc<BytePool>,
) -> io::Result<crate::ipc::Owned<PathBuf>> {
    use std::os::unix::ffi::OsStringExt;
    let input = native_path_input(path, pool)?;
    let mut capacity = 256usize;
    loop {
        let mut buffer = SaveBuffer::<u8>::new(capacity, pool)?;
        // SAFETY: input is terminated and buffer owns capacity writable bytes.
        let count = unsafe {
            libc::readlink(
                input.value.as_ptr(),
                buffer.data.as_mut_ptr().cast(),
                capacity,
            )
        };
        if count < 0 {
            let error = io::Error::last_os_error();
            return Err(error);
        }
        if count as usize == capacity {
            drop(buffer);
            capacity = capacity
                .checked_mul(2)
                .ok_or_else(|| target_memory_error(pool))?;
            continue;
        }
        let SaveBuffer {
            mut data,
            _lease: lease,
        } = buffer;
        data.truncate(count as usize);
        let value = PathBuf::from(std::ffi::OsString::from_vec(data));
        return Ok(crate::ipc::Owned::new(
            value,
            crate::lease::Owner::new(lease),
        ));
    }
}

#[cfg(windows)]
pub(super) fn read_target_link(
    path: &Path,
    pool: &Arc<BytePool>,
) -> io::Result<crate::ipc::Owned<PathBuf>> {
    use windows_sys::Win32::Storage::FileSystem::MAXIMUM_REPARSE_DATA_BUFFER_SIZE;
    // Rust 1.92 uses one fixed reparse buffer, a UTF-16 substitute-name Vec, and a
    // WTF-8 String. Eight reparse buffers cover those allocations and growth plus
    // the <=260-unit prefix check. Retain std's relative/junction conversion rules.
    let bytes = native_path_bytes(path)
        .and_then(|n| n.checked_add(8 * MAXIMUM_REPARSE_DATA_BUFFER_SIZE as usize))
        .ok_or_else(|| target_memory_error(pool))?;
    let mut lease = pool
        .try_acquire(bytes)
        .ok_or_else(|| target_memory_error(pool))?;
    let value = std::fs::read_link(path)?;
    lease.shrink_to(value.capacity());
    Ok(crate::ipc::Owned::new(
        value,
        crate::lease::Owner::new(lease),
    ))
}
