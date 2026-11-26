//! GNU bash discovery with an explicit override.
//!
//! Derived from FastCtx `src/shell/bash.rs` (Apache-2.0, Copyright 2026 yc-duan), modified for fastexec.

use crate::env::LaunchContext;
use std::collections::HashSet;
#[cfg(windows)]
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

const OVERRIDE: &str = "FASTEXEC_BASH";

/// The GNU bash for `launch`, found once per session cache and retried after failure.
pub fn resolve(launch: &LaunchContext) -> Result<PathBuf, String> {
    launch.cached_bash(find)
}

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
    if let Some(candidate) =
        find_candidate(ctx, candidates(ctx), |path| validate(path, ctx).is_ok())
    {
        return Ok(candidate);
    }
    Err(if cfg!(windows) {
        format!(
            "Cannot find GNU bash. Install Git for Windows (https://git-scm.com/downloads) or set {OVERRIDE} to the absolute path of bash.exe. C:/Windows/System32/bash.exe is the WSL launcher and does not qualify."
        )
    } else {
        format!("Cannot find GNU bash. Install bash or set {OVERRIDE} to its absolute path.")
    })
}

fn find_candidate(
    ctx: &LaunchContext,
    candidates: Vec<PathBuf>,
    mut is_bash: impl FnMut(&Path) -> bool,
) -> Option<PathBuf> {
    let mut seen = HashSet::new();
    for candidate in candidates {
        // Windows paths compare case-insensitively; Unix paths retain their native bytes.
        #[cfg(windows)]
        let key = candidate.to_string_lossy().to_lowercase();
        #[cfg(not(windows))]
        let key = candidate.as_os_str().to_owned();
        if candidate.is_absolute()
            && !excluded(&candidate, ctx)
            && seen.insert(key)
            && is_bash(&candidate)
        {
            return Some(candidate);
        }
    }
    None
}

/// Longest `--version` output a candidate may print.
const BANNER_LIMIT: u64 = 4096;

fn validate(path: &Path, ctx: &LaunchContext) -> Result<(), String> {
    if !path.is_file() {
        return Err("the file does not exist".to_string());
    }
    let bytes = crate::platform::process::bash_version(
        path,
        ctx,
        BANNER_LIMIT,
        std::time::Duration::from_secs(5),
    )?;
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
    #[cfg(target_os = "freebsd")]
    out.push(PathBuf::from("/usr/local/bin/bash"));
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

#[cfg(all(test, windows))]
mod windows_tests {
    use super::*;

    #[test]
    fn bash_is_found_while_the_starting_directory_is_gone() {
        let gone = std::env::temp_dir().join(format!("fastexec-gone-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&gone);
        let ctx = LaunchContext::new(gone, std::env::vars_os().collect());
        let found = resolve(&ctx).expect("bash is found without the session's starting directory");
        assert!(found.is_file(), "{}", found.display());
        assert_eq!(resolve(&ctx).unwrap(), found);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    #[test]
    fn path_discovery_tries_distinct_native_names_with_the_same_lossy_text() {
        let real_bash = ["/bin/bash", "/usr/bin/bash", "/usr/local/bin/bash"]
            .into_iter()
            .map(PathBuf::from)
            .find(|path| path.is_file())
            .expect("system GNU Bash");
        let root = std::env::temp_dir().join(format!(
            "fastexec-bash-native-{}-{}",
            std::process::id(),
            crate::id::random_chars(10).unwrap(),
        ));
        let missing = root.join(OsString::from_vec(b"bin-\xff".to_vec()));
        let working = root.join("bin-\u{fffd}");
        assert_ne!(missing, working);
        assert_eq!(missing.to_string_lossy(), working.to_string_lossy());
        std::fs::create_dir_all(&working).unwrap();
        let expected = working.join("bash");
        std::os::unix::fs::symlink(&real_bash, &expected).unwrap();
        let ctx = LaunchContext::new(
            root.clone(),
            vec![(
                "PATH".into(),
                std::env::join_paths([&missing, &working]).unwrap(),
            )],
        );
        // The unit-test executable cannot act as the production Unix task host.
        // The same candidate loop uses an ordinary GNU Bash banner check here.
        let selected = find_candidate(&ctx, path_candidates("bash", &ctx), |candidate| {
            candidate.is_file()
                && std::process::Command::new(candidate)
                    .arg("--version")
                    .env_clear()
                    .output()
                    .is_ok_and(|output| {
                        output.status.success()
                            && String::from_utf8_lossy(&output.stdout).contains("GNU bash")
                    })
        });
        std::fs::remove_dir_all(&root).unwrap();
        assert_eq!(selected, Some(expected));
    }
}
