//! Allocate matched PTYs with close-on-exec set at creation.

use std::fs::File;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, RawFd};

/// A matched PTY pair at the requested size, safe from a concurrent fork and exec.
pub(crate) fn open(cols: u16, rows: u16) -> io::Result<(File, File)> {
    open_observed(cols, rows, |_| {})
}

/// Tests can pause immediately after either allocation, while its descriptor is owned here.
pub(crate) fn open_observed(
    cols: u16,
    rows: u16,
    mut allocated: impl FnMut(RawFd),
) -> io::Result<(File, File)> {
    let allocate = || {
        // SAFETY: posix_openpt has no pointer arguments and returns a new descriptor.
        let master = unsafe { libc::posix_openpt(libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC) };
        if master == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(master)
        }
    };
    #[cfg(target_os = "macos")]
    let master = retry_master_open(allocate)?;
    #[cfg(not(target_os = "macos"))]
    let master = allocate()?;
    // SAFETY: the successful call returned this descriptor, which is owned only here.
    let master = unsafe { File::from_raw_fd(master) };
    allocated(master.as_raw_fd());
    // SAFETY: master is a live PTY master owned by this function.
    if unsafe { libc::grantpt(master.as_raw_fd()) } == -1
        || unsafe { libc::unlockpt(master.as_raw_fd()) } == -1
    {
        return Err(io::Error::last_os_error());
    }
    #[cfg(target_os = "linux")]
    let slave = {
        // Opening the peer through its master also works when devpts paths name another instance.
        // SAFETY: master is a live PTY; the ioctl returns a new descriptor with these open flags.
        let slave = unsafe {
            libc::ioctl(
                master.as_raw_fd(),
                libc::TIOCGPTPEER as _,
                libc::O_RDWR | libc::O_NOCTTY | libc::O_CLOEXEC,
            )
        };
        if slave == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the successful ioctl returned this descriptor, owned only here.
        unsafe { File::from_raw_fd(slave) }
    };
    #[cfg(not(target_os = "linux"))]
    let slave = {
        use std::ffi::{CStr, OsStr};
        use std::fs::OpenOptions;
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::OpenOptionsExt;

        #[cfg(target_os = "macos")]
        let mut name = [0u8; 128]; // TIOCPTYGNAME encodes a 128-byte output buffer.
        #[cfg(not(target_os = "macos"))]
        let mut name = [0u8; libc::PATH_MAX as usize];
        // SAFETY: name is writable for the native API's output, and master remains open.
        #[cfg(target_os = "macos")]
        let named = unsafe {
            libc::ioctl(
                master.as_raw_fd(),
                libc::TIOCPTYGNAME as _,
                name.as_mut_ptr(),
            )
        };
        #[cfg(not(target_os = "macos"))]
        let named =
            unsafe { libc::ptsname_r(master.as_raw_fd(), name.as_mut_ptr().cast(), name.len()) };
        if named != 0 {
            // The native APIs use -1 with errno; also accept a POSIX error number.
            return Err(if named == -1 {
                io::Error::last_os_error()
            } else {
                io::Error::from_raw_os_error(named)
            });
        }
        let name = CStr::from_bytes_until_nul(&name)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOCTTY | libc::O_CLOEXEC)
            .open(OsStr::from_bytes(name.to_bytes()))?
    };
    allocated(slave.as_raw_fd());
    let size = libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    // SAFETY: slave is a live terminal descriptor and size is an initialized winsize.
    if unsafe { libc::ioctl(slave.as_raw_fd(), libc::TIOCSWINSZ as _, &size) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok((master, slave))
}

/// Darwin can return EREDRIVEOPEN when /dev/ptmx allocation races another opener.
/// XNU's bsd/vfs/vfs_vnops.c retries internally, but preserves that errno at its
/// limit; bsd/sys/errno.h defines the private code as -6.
/// https://github.com/apple-oss-distributions/xnu/tree/xnu-12377.1.9/bsd
#[cfg(any(target_os = "macos", test))]
fn retry_master_open(mut allocate: impl FnMut() -> io::Result<RawFd>) -> io::Result<RawFd> {
    const EREDRIVEOPEN: i32 = -6;
    // At most five attempts; never yield after the final failure.
    for _ in 0..4 {
        match allocate() {
            Err(error) if error.raw_os_error() == Some(EREDRIVEOPEN) => {
                std::thread::yield_now();
            }
            result => return result,
        }
    }
    allocate()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn master_open_returns_the_first_success() {
        for transient_failures in [0, 2, 4] {
            let mut calls = 0;
            let master = retry_master_open(|| {
                calls += 1;
                if calls <= transient_failures {
                    Err(io::Error::from_raw_os_error(-6))
                } else {
                    Ok(42)
                }
            })
            .unwrap();
            assert_eq!(master, 42);
            assert_eq!(calls, transient_failures + 1);
        }
    }

    #[test]
    fn master_open_stops_after_five_attempts_and_keeps_the_error() {
        let mut calls = 0;
        let error = retry_master_open(|| {
            calls += 1;
            Err(io::Error::from_raw_os_error(-6))
        })
        .unwrap_err();
        assert_eq!(calls, 5);
        assert_eq!(error.raw_os_error(), Some(-6));
    }

    #[test]
    fn master_open_preserves_other_errors_without_retrying_them() {
        for code in [libc::ENXIO, libc::EACCES] {
            for transient_failures in [0, 1] {
                let mut calls = 0;
                let error = retry_master_open(|| {
                    calls += 1;
                    let error = if calls <= transient_failures {
                        -6
                    } else {
                        code
                    };
                    Err(io::Error::from_raw_os_error(error))
                })
                .unwrap_err();
                assert_eq!(calls, transient_failures + 1);
                assert_eq!(error.raw_os_error(), Some(code));
            }
        }
    }
}
