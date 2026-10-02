//! The ConPTY bundled for Windows x64 (`vendor/conpty`).
//!
//! The system ConPTY of older Windows releases re-renders a program's output: lines pushed off
//! the screen through scroll regions, history cleared by full redraws, and alternate-screen
//! switches do not reach the task log. The bundled ConPTY passes the program's output through.
//!
//! portable-pty loads `conpty.dll` by name and prefers it to the system ConPTY. Loading the
//! extracted copy by its full path first makes that name resolve to it.

#[cfg(all(windows, target_arch = "x86_64"))]
mod bundled {
    use std::fs::File;
    use std::io::Read;
    use std::os::windows::fs::OpenOptionsExt;
    use std::path::{Path, PathBuf};

    const VERSION: &str = "1.24.260710001";
    const FILES: [(&str, &[u8]); 2] = [
        (
            "conpty.dll",
            include_bytes!("../vendor/conpty/x64/conpty.dll"),
        ),
        (
            "OpenConsole.exe",
            include_bytes!("../vendor/conpty/x64/OpenConsole.exe"),
        ),
    ];

    pub fn load() -> Result<(), String> {
        let dll = install()?;
        let wide: Vec<u16> = std::os::windows::ffi::OsStrExt::encode_wide(dll.as_os_str())
            .chain([0])
            .collect();
        // SAFETY: `wide` is a NUL-terminated path. The module stays loaded for the process
        // lifetime, as portable-pty keeps using it.
        let module =
            unsafe { windows_sys::Win32::System::LibraryLoader::LoadLibraryW(wide.as_ptr()) };
        if module.is_null() {
            return Err(format!(
                "cannot load {}: {}",
                dll.display(),
                std::io::Error::last_os_error()
            ));
        }
        Ok(())
    }

    /// Writes the files to a versioned directory in the user's temp directory, shared by every
    /// server of this version, and pins them; returns the path of `conpty.dll`, whose host
    /// `OpenConsole.exe` sits beside it and starts with every PTY.
    fn install() -> Result<PathBuf, String> {
        let dir = std::env::temp_dir().join(format!("fastexec-conpty-{VERSION}"));
        std::fs::create_dir_all(&dir)
            .map_err(|error| format!("cannot create {}: {error}", dir.display()))?;
        for (name, bytes) in FILES {
            let path = dir.join(name);
            let mut installed = Ok(());
            if !pin(&path).is_ok_and(|mut file| holds(&mut file, bytes)) {
                // Written under a temporary name, then renamed, so a server starting at the
                // same time never loads a partial file.
                let temp = dir.join(format!("{name}.{}.tmp", std::process::id()));
                installed =
                    std::fs::write(&temp, bytes).and_then(|()| std::fs::rename(&temp, &path));
                let _ = std::fs::remove_file(&temp);
            }
            // A concurrent server may hold the same file pinned, which a rename cannot replace;
            // the content check below accepts it.
            let mut file =
                pin(&path).map_err(|error| format!("cannot open {}: {error}", path.display()))?;
            if !holds(&mut file, bytes) {
                let error = installed
                    .err()
                    .map_or("content differs".into(), |e| e.to_string());
                return Err(format!("cannot install {}: {error}", path.display()));
            }
            // The verified file stays pinned for the server's lifetime.
            std::mem::forget(file);
        }
        Ok(dir.join("conpty.dll"))
    }

    /// Opens `path` for reading so that no process can write, replace, or delete it while the
    /// handle is open; loading and running it remain possible.
    fn pin(path: &Path) -> std::io::Result<File> {
        const FILE_SHARE_READ: u32 = 0x1;
        std::fs::OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ)
            .open(path)
    }

    fn holds(file: &mut File, bytes: &[u8]) -> bool {
        let mut found = Vec::with_capacity(bytes.len());
        file.read_to_end(&mut found).is_ok() && found == bytes
    }
}

/// Loads the bundled ConPTY once, before the first PTY starts. Returns why PTY tasks use the
/// system ConPTY instead, on Windows; `None` elsewhere.
pub fn load() -> Option<&'static str> {
    #[cfg(all(windows, target_arch = "x86_64"))]
    {
        static LOADED: std::sync::OnceLock<Result<(), String>> = std::sync::OnceLock::new();
        let result = LOADED.get_or_init(|| {
            bundled::load().map_err(|error| {
                let message =
                    format!("bundled ConPTY unavailable ({error}); using the system ConPTY");
                eprintln!("fastexec: {message}");
                message
            })
        });
        result.as_ref().err().map(String::as_str)
    }
    #[cfg(all(windows, not(target_arch = "x86_64")))]
    return Some("this build bundles no ConPTY for its architecture; using the system ConPTY");
    #[cfg(unix)]
    return None;
}
