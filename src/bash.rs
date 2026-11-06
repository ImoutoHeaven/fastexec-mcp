//! GNU bash discovery with an explicit override.
//!
//! Derived from FastCtx `src/shell/bash.rs` (Apache-2.0, Copyright 2026 yc-duan), modified for fastexec.

use crate::session::LaunchContext;
use std::collections::HashSet;
#[cfg(windows)]
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

const OVERRIDE: &str = "FASTEXEC_BASH";

/// GNU bash for `ctx`: its override, then its PATH and the usual install roots. Everything comes
/// from the context, so two sessions can find different bashes.
pub fn find(ctx: &LaunchContext) -> Result<PathBuf, String> {
    if let Some(value) = ctx.var(OVERRIDE) {
        let path = PathBuf::from(&value);
        let problem = if !path.is_absolute() {
            Some("the path is not absolute".to_string())
        } else if excluded(&path, ctx) {
            Some("the path is the WSL launcher or a WindowsApps shim".to_string())
        } else {
            validate(&path, ctx).err()
        };
        return match problem {
            None => Ok(path),
            Some(reason) => Err(format!(
                "Invalid {OVERRIDE} value {value:?}: not a working GNU bash ({reason}). Fix or unset it."
            )),
        };
    }
    let mut seen = HashSet::new();
    for candidate in candidates(ctx) {
        // Windows paths compare case-insensitively; Unix paths that differ in case are distinct.
        let key = candidate.to_string_lossy();
        let key = if cfg!(windows) {
            key.to_lowercase()
        } else {
            key.into_owned()
        };
        if candidate.is_absolute()
            && !excluded(&candidate, ctx)
            && seen.insert(key)
            && validate(&candidate, ctx).is_ok()
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

fn validate(path: &Path, ctx: &LaunchContext) -> Result<(), String> {
    if !path.is_file() {
        return Err("the file does not exist".to_string());
    }
    let bytes =
        crate::process::bash_version(path, ctx, BANNER_LIMIT, std::time::Duration::from_secs(5))?;
    if String::from_utf8_lossy(&bytes).contains("GNU bash") {
        Ok(())
    } else {
        Err("--version did not report GNU bash".to_string())
    }
}

fn path_candidates(name: &str, ctx: &LaunchContext) -> Vec<PathBuf> {
    ctx.var("PATH")
        .map(|path| {
            std::env::split_paths(path)
                .map(|dir| dir.join(name))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(windows)]
fn candidates(ctx: &LaunchContext) -> Vec<PathBuf> {
    let mut out = Vec::new();
    // <git root>/usr/bin/bash.exe is the real bash; <git root>/bin/bash.exe is a launcher shim.
    for git in path_candidates("git.exe", ctx)
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
    let env = |name: &str| ctx.var(name).map(PathBuf::from);
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
    out.extend(path_candidates("bash.exe", ctx));
    out
}

#[cfg(not(windows))]
fn candidates(ctx: &LaunchContext) -> Vec<PathBuf> {
    let mut out = vec![PathBuf::from("/bin/bash"), PathBuf::from("/usr/bin/bash")];
    out.extend(path_candidates("bash", ctx));
    out
}

#[cfg(windows)]
fn excluded(path: &Path, ctx: &LaunchContext) -> bool {
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
    ctx.var("SystemRoot").is_some_and(|root| {
        let root = normalized(root);
        candidate.starts_with(&format!("{}\\", root.trim_end_matches('\\')))
    })
}

#[cfg(not(windows))]
fn excluded(_path: &Path, _ctx: &LaunchContext) -> bool {
    false
}
