//! The owner and mode checks shared by Unix private-directory implementations.

use std::fs::Metadata;
use std::io;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::Path;

/// Requires a directory owned by this user with mode 0700.
pub(super) fn check_metadata(dir: &Path, meta: &Metadata) -> io::Result<()> {
    if !meta.is_dir() {
        return Err(io::Error::other(format!(
            "{} is not a directory this user owns with mode 0700",
            dir.display()
        )));
    }
    // SAFETY: geteuid has no preconditions and cannot fail.
    if meta.uid() != unsafe { libc::geteuid() } {
        return Err(io::Error::other(format!(
            "{} is not owned by this user",
            dir.display()
        )));
    }
    if meta.permissions().mode() & 0o777 != 0o700 {
        return Err(io::Error::other(format!(
            "{} is not mode 0700",
            dir.display()
        )));
    }
    Ok(())
}
