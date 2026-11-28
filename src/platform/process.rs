//! Bash launch in pipe or PTY mode, and ownership of each task's whole process tree.
//!
//! The launch environment, long-command scripts, and the Windows kill-on-close Job
//! Object are derived from FastCtx `src/shell/process.rs` and `src/process_policy.rs`
//! (Apache-2.0, Copyright 2026 yc-duan), modified for fastexec.

mod probe;
#[cfg(test)]
mod tests;
mod types;
#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

pub(crate) use probe::bash_version;
pub use probe::{dump_env, task_env};
pub use types::{
    Child, ENV_FLAG, Launch, PTY_COLS, PTY_COLS_MAX, PTY_COLS_MIN, PTY_ROWS, PTY_ROWS_MAX,
    PTY_ROWS_MIN, PtyControl, SCRIPT_THRESHOLD_BYTES, Tree,
};
#[cfg(unix)]
pub use types::{HOST_FLAG, HOST_STARTUP_READY};
pub(crate) use types::{clean_member_name, normalize_members};
#[cfg(all(unix, not(target_os = "macos")))]
pub use unix::kill_tree;
#[cfg(unix)]
pub use unix::spawn;
#[cfg(windows)]
pub use windows::spawn;
