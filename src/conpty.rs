//! The ConPTY bundled for Windows x64 and arm64 (`vendor/conpty`).
//!
//! The system ConPTY of older Windows releases re-renders a program's output: lines pushed off
//! the screen through scroll regions, history cleared by full redraws, and alternate-screen
//! switches do not reach the task log. The bundled ConPTY passes the program's output through.
//!
//! The launcher uses the exact module loaded from the verified extracted image. A failed
//! extraction falls back to the system API without searching the working directory or PATH.

#[cfg(all(windows, target_arch = "x86_64"))]
macro_rules! arch {
    () => {
        "x64"
    };
}

#[cfg(all(windows, target_arch = "aarch64"))]
macro_rules! arch {
    () => {
        "arm64"
    };
}

#[cfg(all(windows, any(target_arch = "x86_64", target_arch = "aarch64")))]
mod bundled {
    use std::fs::File;
    use std::io::{Read, Write};
    use std::os::windows::fs::OpenOptionsExt;
    use std::path::{Path, PathBuf};

    const VERSION: &str = "1.24.260710001";
    const FILES: [(&str, &[u8]); 2] = [
        (
            "conpty.dll",
            include_bytes!(concat!("../vendor/conpty/", arch!(), "/conpty.dll")),
        ),
        (
            "OpenConsole.exe",
            include_bytes!(concat!("../vendor/conpty/", arch!(), "/OpenConsole.exe")),
        ),
    ];

    pub fn load() -> Result<usize, String> {
        let dll = install()?;
        let wide: Vec<u16> = std::os::windows::ffi::OsStrExt::encode_wide(dll.as_os_str())
            .chain([0])
            .collect();
        // SAFETY: `wide` is a NUL-terminated path. The module stays loaded for the process
        // lifetime, as the launcher keeps resolving the console API through it.
        let module = unsafe {
            windows_sys::Win32::System::LibraryLoader::LoadLibraryExW(
                wide.as_ptr(),
                std::ptr::null_mut(),
                windows_sys::Win32::System::LibraryLoader::LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR
                    | windows_sys::Win32::System::LibraryLoader::LOAD_LIBRARY_SEARCH_SYSTEM32,
            )
        };
        if module.is_null() {
            return Err(format!(
                "cannot load {}: {}",
                dll.display(),
                std::io::Error::last_os_error()
            ));
        }
        Ok(module as usize)
    }

    /// Extracts into an exclusively created private directory. The shared temp directory
    /// may be writable by other users; neither a predictable cache name nor a preexisting
    /// temporary file may choose which path this process writes or loads.
    fn install() -> Result<PathBuf, String> {
        let dir = install_directory()?;
        let installed = (|| {
            let mut pins = Vec::new();
            for (name, bytes) in FILES {
                let path = dir.join(name);
                let nonce = crate::tasks::random_chars(12).map_err(|error| error.to_string())?;
                let temp = dir.join(format!("{name}.{nonce}.tmp"));
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&temp)
                    .map_err(|error| format!("cannot create {}: {error}", temp.display()))?;
                file.write_all(bytes)
                    .map_err(|error| format!("cannot write {}: {error}", temp.display()))?;
                drop(file);
                std::fs::rename(&temp, &path)
                    .map_err(|error| format!("cannot install {}: {error}", path.display()))?;
                let mut file = pin(&path)
                    .map_err(|error| format!("cannot open {}: {error}", path.display()))?;
                if !holds(&mut file, bytes) {
                    return Err(format!(
                        "cannot install {}: content differs",
                        path.display()
                    ));
                }
                pins.push(file);
            }
            // Pin only a complete installation, so a failed extraction can delete its files.
            for file in pins {
                std::mem::forget(file);
            }
            Ok(dir.join("conpty.dll"))
        })();
        if installed.is_err() {
            let _ = std::fs::remove_dir_all(&dir);
        }
        installed
    }

    /// A new owner-only directory for this process. Completed processes leave their DLLs on
    /// disk because Windows cannot remove a loaded image; the next start retires those copies.
    fn install_directory() -> Result<PathBuf, String> {
        let temp = std::env::temp_dir();
        let prefix = format!("fastexec-conpty-{VERSION}-{}-", arch!());
        if let Ok(entries) = std::fs::read_dir(&temp) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let old_pid = name
                    .to_str()
                    .and_then(|name| name.strip_prefix(&prefix))
                    .and_then(|tail| tail.split_once('-'))
                    .filter(|(_, nonce)| {
                        nonce.len() == 12
                            && nonce
                                .bytes()
                                .all(|byte| crate::tasks::ALPHABET.contains(&byte))
                    })
                    .and_then(|(pid, _)| pid.parse::<u32>().ok());
                if old_pid
                    .is_some_and(|pid| pid != std::process::id() && !crate::process::is_alive(pid))
                    && crate::runtime::create_private(&entry.path()).is_ok()
                {
                    let _ = std::fs::remove_dir_all(entry.path());
                }
            }
        }
        for _ in 0..8 {
            let nonce = crate::tasks::random_chars(12).map_err(|error| error.to_string())?;
            let dir = temp.join(format!("{prefix}{}-{nonce}", std::process::id()));
            match crate::runtime::create_private_new(&dir) {
                Ok(()) => return Ok(dir),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(format!("cannot create {}: {error}", dir.display())),
            }
        }
        Err("cannot reserve a fresh private ConPTY directory".into())
    }

    /// Opens `path` for reading so that no process can write, replace, or delete it while the
    /// handle is open; loading and running it remain possible.
    fn pin(path: &Path) -> std::io::Result<File> {
        const FILE_SHARE_READ: u32 = 0x1;
        use std::os::windows::fs::MetadataExt;
        use windows_sys::Win32::Storage::FileSystem::{
            FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_OPEN_REPARSE_POINT,
        };
        let file = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
            return Err(std::io::Error::other(
                "a ConPTY image must be a regular file, not a reparse point",
            ));
        }
        Ok(file)
    }

    fn holds(file: &mut File, bytes: &[u8]) -> bool {
        if !file
            .metadata()
            .is_ok_and(|metadata| metadata.len() == bytes.len() as u64)
        {
            return false;
        }
        let mut found = Vec::with_capacity(bytes.len());
        file.take(bytes.len() as u64 + 1)
            .read_to_end(&mut found)
            .is_ok()
            && found == bytes
    }
}

#[cfg(all(windows, any(target_arch = "x86_64", target_arch = "aarch64")))]
fn loaded() -> &'static Result<usize, String> {
    static LOADED: std::sync::OnceLock<Result<usize, String>> = std::sync::OnceLock::new();
    LOADED.get_or_init(|| {
        bundled::load().map_err(|error| {
            let message = format!("bundled ConPTY unavailable ({error}); using the system ConPTY");
            eprintln!("fastexec: {message}");
            message
        })
    })
}

/// The verified module's actual handle. Looking it up again by its basename could pick an
/// unrelated DLL already loaded into the process, so the handle returned by LoadLibraryExW
/// is kept for the process lifetime.
#[cfg(windows)]
pub(crate) fn module() -> Option<windows_sys::Win32::Foundation::HMODULE> {
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    return loaded().as_ref().ok().map(|&handle| handle as _);
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    return None;
}

/// Loads the bundled ConPTY once, before the first PTY starts. Returns why PTY tasks use the
/// system ConPTY instead, on Windows; `None` elsewhere.
pub fn load() -> Option<&'static str> {
    #[cfg(all(windows, any(target_arch = "x86_64", target_arch = "aarch64")))]
    return loaded().as_ref().err().map(String::as_str);
    #[cfg(all(windows, not(any(target_arch = "x86_64", target_arch = "aarch64"))))]
    return Some("this build bundles no ConPTY for its architecture; using the system ConPTY");
    #[cfg(unix)]
    return None;
}
