//! GNU bash discovery with an explicit override.
//!
//! Derived from FastCtx `src/shell/bash.rs` (Apache-2.0, Copyright 2026 yc-duan), modified for fastexec.

use std::collections::HashSet;
#[cfg(windows)]
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

const OVERRIDE: &str = "FASTEXEC_BASH";

pub fn find() -> Result<PathBuf, String> {
    if let Some(value) = std::env::var_os(OVERRIDE) {
        let path = PathBuf::from(&value);
        let problem = if !path.is_absolute() {
            Some("the path is not absolute".to_string())
        } else if excluded(&path) {
            Some("the path is the WSL launcher or a WindowsApps shim".to_string())
        } else {
            validate(&path).err()
        };
        return match problem {
            None => Ok(path),
            Some(reason) => Err(format!(
                "Invalid {OVERRIDE} value {value:?}: not a working GNU bash ({reason}). Fix or unset it."
            )),
        };
    }
    let mut seen = HashSet::new();
    for candidate in candidates() {
        if candidate.is_absolute()
            && !excluded(&candidate)
            && seen.insert(candidate.to_string_lossy().to_lowercase())
            && validate(&candidate).is_ok()
        {
            return Ok(candidate);
        }
    }
    Err(if cfg!(windows) {
        format!(
            "Cannot find GNU bash. Install Git for Windows (https://git-scm.com/downloads) or set {OVERRIDE} to the absolute path of bash.exe. C:/Windows/System32/bash.exe is the WSL launcher and does not qualify."
        )
    } else {
        format!("Cannot find GNU bash. Install bash or set {OVERRIDE} to its absolute path.")
    })
}

/// Longest `--version` output a candidate may print.
const BANNER_LIMIT: u64 = 4096;

fn validate(path: &Path) -> Result<(), String> {
    if !path.is_file() {
        return Err("the file does not exist".to_string());
    }
    let mut child = crate::process::quiet_command(path)
        .arg("--version")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .map_err(|error| error.to_string())?;
    // `--version` reads no startup files, but a broken candidate must not stall discovery: one
    // deadline covers both its exit and the end of its stdout.
    // ponytail: on timeout only the candidate is killed; a descendant it started is left alone.
    let timeout = std::time::Duration::from_secs(5);
    let deadline = std::time::Instant::now() + timeout;
    let mut stdout = child.stdout.take().expect("piped");
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        // A version banner is short; the cap bounds memory if a stray descendant keeps writing.
        // Reading one byte past it tells a full cap apart from a real end of output.
        let mut bytes = Vec::new();
        let _ = std::io::Read::read_to_end(
            &mut std::io::Read::take(&mut stdout, BANNER_LIMIT + 1),
            &mut bytes,
        );
        let _ = tx.send(bytes);
    });
    let text = rx.recv_timeout(timeout);
    while child
        .try_wait()
        .map_err(|error| error.to_string())?
        .is_none()
    {
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err("--version did not finish within 5 s".to_string());
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let bytes = text.map_err(|_| "--version output stayed open past 5 s".to_string())?;
    if bytes.len() as u64 > BANNER_LIMIT {
        return Err("--version printed more than 4 KiB".to_string());
    }
    if String::from_utf8_lossy(&bytes).contains("GNU bash") {
        Ok(())
    } else {
        Err("--version did not report GNU bash".to_string())
    }
}

fn path_candidates(name: &str) -> Vec<PathBuf> {
    std::env::var_os("PATH")
        .map(|path| {
            std::env::split_paths(&path)
                .map(|dir| dir.join(name))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(windows)]
fn candidates() -> Vec<PathBuf> {
    let mut out = Vec::new();
    // <git root>/usr/bin/bash.exe is the real bash; <git root>/bin/bash.exe is a launcher shim.
    for git in path_candidates("git.exe")
        .into_iter()
        .filter(|git| git.is_file())
    {
        let mut ancestor = git.parent();
        for _ in 0..4 {
            let Some(dir) = ancestor else { break };
            out.push(dir.join("usr").join("bin").join("bash.exe"));
            ancestor = dir.parent();
        }
    }
    let env = |name: &str| std::env::var_os(name).map(PathBuf::from);
    for root in ["ProgramFiles", "ProgramFiles(x86)"]
        .into_iter()
        .filter_map(env)
    {
        out.push(root.join("Git/usr/bin/bash.exe"));
    }
    if let Some(root) = env("LocalAppData") {
        out.push(root.join("Programs/Git/usr/bin/bash.exe"));
    }
    if let Some(root) = env("SCOOP") {
        out.push(root.join("apps/git/current/usr/bin/bash.exe"));
    }
    if let Some(profile) = env("USERPROFILE") {
        out.push(profile.join("scoop/apps/git/current/usr/bin/bash.exe"));
    }
    out.extend(path_candidates("bash.exe"));
    out
}

#[cfg(not(windows))]
fn candidates() -> Vec<PathBuf> {
    let mut out = vec![PathBuf::from("/bin/bash"), PathBuf::from("/usr/bin/bash")];
    out.extend(path_candidates("bash"));
    out
}

#[cfg(windows)]
fn excluded(path: &Path) -> bool {
    fn normalized(path: &OsStr) -> String {
        let path = Path::new(path);
        let canonical = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
        let text = canonical
            .to_string_lossy()
            .replace('/', "\\")
            .to_lowercase();
        text.strip_prefix(r"\\?\")
            .map(str::to_string)
            .unwrap_or(text)
    }
    let candidate = normalized(path.as_os_str());
    if candidate.contains(r"\windowsapps\") {
        return true;
    }
    std::env::var_os("SystemRoot").is_some_and(|root| {
        let root = normalized(&root);
        candidate.starts_with(&format!("{}\\", root.trim_end_matches('\\')))
    })
}

#[cfg(not(windows))]
fn excluded(_path: &Path) -> bool {
    false
}
