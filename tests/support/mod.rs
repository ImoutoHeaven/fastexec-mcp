//! Platform setup shared by the process-level integration tests.

#[cfg(windows)]
mod private_runtime;
#[cfg(windows)]
pub use private_runtime::create_private_runtime;

/// Native FreeBSD queries and cleanup for fixture daemons. Becoming their reaper retains
/// orphaned daemon PIDs until this harness explicitly waits for them, so a checked image
/// cannot turn into an unrelated numeric PID between the ownership check and SIGKILL.
#[cfg(target_os = "freebsd")]
pub mod freebsd {
    use std::io;
    use std::os::unix::ffi::OsStringExt;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    pub fn init() {
        static READY: std::sync::OnceLock<()> = std::sync::OnceLock::new();
        READY.get_or_init(|| {
            // Auto-reaping would release the identity the fixture needs to hold.
            let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
            assert_eq!(
                unsafe { libc::sigaction(libc::SIGCHLD, std::ptr::null(), &mut action) },
                0
            );
            assert_ne!(action.sa_sigaction, libc::SIG_IGN);
            assert_eq!(action.sa_flags & libc::SA_NOCLDWAIT, 0);
            assert_eq!(
                unsafe {
                    libc::procctl(
                        libc::P_PID,
                        0,
                        libc::PROC_REAP_ACQUIRE,
                        std::ptr::null_mut(),
                    )
                },
                0,
                "fixture reaper: {}",
                io::Error::last_os_error()
            );
        });
    }

    fn info(pid: u32) -> io::Result<Option<libc::kinfo_proc>> {
        let pid = i32::try_from(pid)
            .ok()
            .filter(|pid| *pid > 0)
            .ok_or_else(|| io::Error::other("invalid process identifier"))?;
        let mut mib = [libc::CTL_KERN, libc::KERN_PROC, libc::KERN_PROC_PID, pid];
        let mut info: libc::kinfo_proc = unsafe { std::mem::zeroed() };
        let mut length = std::mem::size_of_val(&info);
        // SAFETY: the fixed record and its supplied size match the native sysctl ABI.
        if unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                mib.len() as u32,
                std::ptr::from_mut(&mut info).cast(),
                &mut length,
                std::ptr::null_mut(),
                0,
            )
        } == -1
        {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(libc::ESRCH) {
                Ok(None)
            } else {
                Err(error)
            };
        }
        if length == 0 {
            return Ok(None);
        }
        if length != std::mem::size_of_val(&info) || info.ki_pid != pid {
            return Err(io::Error::other("unexpected kern.proc.pid record"));
        }
        Ok(Some(info))
    }

    fn image(pid: u32) -> io::Result<PathBuf> {
        let pid = i32::try_from(pid).map_err(|_| io::Error::other("invalid process identifier"))?;
        let mut mib = [
            libc::CTL_KERN,
            libc::KERN_PROC,
            libc::KERN_PROC_PATHNAME,
            pid,
        ];
        let mut bytes = vec![0u8; libc::PATH_MAX as usize];
        let mut length = bytes.len();
        // SAFETY: the buffer length is the allocation's size and sysctl writes at most that size.
        if unsafe {
            libc::sysctl(
                mib.as_mut_ptr(),
                mib.len() as u32,
                bytes.as_mut_ptr().cast(),
                &mut length,
                std::ptr::null_mut(),
                0,
            )
        } == -1
        {
            return Err(io::Error::last_os_error());
        }
        if length == 0 || length > bytes.len() {
            return Err(io::Error::other("missing process pathname"));
        }
        let path = std::ffi::CStr::from_bytes_with_nul(&bytes[..length])
            .map_err(|_| io::Error::other("invalid process pathname"))?;
        Ok(PathBuf::from(std::ffi::OsString::from_vec(
            path.to_bytes().to_vec(),
        )))
    }

    pub fn owns_process(pid: u32, directory: &Path) -> bool {
        let Ok(Some(before)) = info(pid) else {
            return false;
        };
        if before.ki_ppid != std::process::id() as i32 || before.ki_stat == libc::SZOMB {
            return false;
        }
        let Ok(path) = image(pid).and_then(std::fs::canonicalize) else {
            return false;
        };
        let Ok(directory) = std::fs::canonicalize(directory) else {
            return false;
        };
        if !path.starts_with(directory) {
            return false;
        }
        let Ok(Some(after)) = info(pid) else {
            return false;
        };
        after.ki_ppid == before.ki_ppid
            && after.ki_start.tv_sec == before.ki_start.tv_sec
            && after.ki_start.tv_usec == before.ki_start.tv_usec
            && after.ki_stat != libc::SZOMB
    }

    /// An unreadable process is unknown and cannot count as confirmed termination.
    pub fn is_alive(pid: u32) -> bool {
        match info(pid) {
            Ok(Some(info)) => info.ki_stat != libc::SZOMB,
            Ok(None) => false,
            Err(_) => true,
        }
    }

    pub fn reap_stopped(pid: u32) -> bool {
        if pid == 0 || i32::try_from(pid).is_err() {
            return false;
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut status = 0;
            // No other fixture waits for this daemon: its number stays reserved until this call.
            let waited = unsafe { libc::waitpid(pid as i32, &mut status, libc::WNOHANG) };
            if waited == pid as i32 {
                return libc::WIFEXITED(status) || libc::WIFSIGNALED(status);
            }
            if waited == -1 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return error.raw_os_error() == Some(libc::ECHILD) && !is_alive(pid);
            }
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    pub fn end_owned_process(pid: u32, directory: &Path) -> bool {
        if !owns_process(pid, directory) {
            return false;
        }
        // This is our unreaped child, validated through its native image and birth identity.
        if unsafe { libc::kill(pid as i32, libc::SIGKILL) } == -1 {
            return false;
        }
        reap_stopped(pid)
    }

    #[test]
    fn native_identity_rejects_other_images_and_recognizes_zombies() {
        init();
        assert_eq!(
            std::fs::canonicalize(image(std::process::id()).unwrap()).unwrap(),
            std::fs::canonicalize(std::env::current_exe().unwrap()).unwrap()
        );
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "exec sleep 60"])
            .spawn()
            .unwrap();
        let pid = child.id();
        assert!(is_alive(pid));
        assert!(!end_owned_process(
            pid,
            Path::new("/nonexistent-fastexec-fixture")
        ));
        assert!(child.try_wait().unwrap().is_none());
        child.kill().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while is_alive(pid) {
            assert!(
                Instant::now() < deadline,
                "the killed child never became a zombie"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(info(pid).unwrap().unwrap().ki_stat, libc::SZOMB);
        child.wait().unwrap();
        assert!(!is_alive(pid));
    }
}
