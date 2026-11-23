//! OS-gated platform modules for process execution, PTYs, shell launch, and runtime paths.

pub(crate) mod bash;
pub(crate) mod conpty;
#[cfg(windows)]
pub(crate) mod conpty_spawn;
#[cfg(target_os = "freebsd")]
pub(crate) mod freebsd_fs;
#[cfg(unix)]
pub(crate) mod host;
pub(crate) mod process;
pub(crate) mod runtime;
#[cfg(target_os = "freebsd")]
pub(crate) mod tree_freebsd;
#[cfg(target_os = "linux")]
pub(crate) mod tree_linux;
#[cfg(target_os = "macos")]
pub(crate) mod tree_macos;
#[cfg(unix)]
pub(crate) mod unix_pty;
