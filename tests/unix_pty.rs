//! Native PTY pairs must stay matched and private while other threads spawn commands.
#![cfg(unix)]

#[path = "../src/unix_pty.rs"]
mod unix_pty;

use std::ffi::CStr;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, RawFd};
use std::time::{Duration, Instant};

fn ready(fd: RawFd, timeout: i32) -> i16 {
    let mut poll = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: poll is initialized and writable for one descriptor.
    let count = unsafe { libc::poll(&mut poll, 1, timeout) };
    assert!(
        count >= 0,
        "poll({fd}): {}",
        std::io::Error::last_os_error()
    );
    poll.revents
}

#[test]
fn concurrent_pty_allocations_keep_each_master_with_its_slave() {
    const WORKERS: usize = 8;
    let start = std::sync::Barrier::new(WORKERS);
    std::thread::scope(|scope| {
        for worker in 0..WORKERS {
            let start = &start;
            scope.spawn(move || {
                start.wait();
                for iteration in 0..256 {
                    let (cols, rows) = (80 + worker as u16, 24 + (iteration % 32) as u16);
                    let (mut master, mut slave) =
                        unix_pty::open(cols, rows).expect("allocate a matched PTY");
                    for fd in [master.as_raw_fd(), slave.as_raw_fd()] {
                        // SAFETY: these descriptors stay owned by their files during the call.
                        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
                        assert_ne!(flags, -1, "F_GETFD: {}", std::io::Error::last_os_error());
                        assert_ne!(flags & libc::FD_CLOEXEC, 0, "PTY descriptor crosses exec");
                    }
                    let (mut expected, mut actual) = (
                        [0u8; libc::PATH_MAX as usize],
                        [0u8; libc::PATH_MAX as usize],
                    );
                    // SAFETY: master is live and expected contains at least 128 writable bytes.
                    #[cfg(target_os = "macos")]
                    let named = unsafe {
                        libc::ioctl(
                            master.as_raw_fd(),
                            libc::TIOCPTYGNAME as _,
                            expected.as_mut_ptr(),
                        )
                    };
                    // SAFETY: master and its independent writable name buffer are valid.
                    #[cfg(not(target_os = "macos"))]
                    let named = unsafe {
                        libc::ptsname_r(
                            master.as_raw_fd(),
                            expected.as_mut_ptr().cast(),
                            expected.len(),
                        )
                    };
                    assert_eq!(named, 0, "master name: {}", std::io::Error::last_os_error());
                    // SAFETY: slave and its independent writable name buffer are valid.
                    assert_eq!(
                        unsafe {
                            libc::ttyname_r(
                                slave.as_raw_fd(),
                                actual.as_mut_ptr().cast(),
                                actual.len(),
                            )
                        },
                        0,
                        "slave name: {}",
                        std::io::Error::last_os_error()
                    );
                    assert_eq!(
                        CStr::from_bytes_until_nul(&expected).unwrap(),
                        CStr::from_bytes_until_nul(&actual).unwrap(),
                        "PTY pair crossed between workers"
                    );
                    let mut size = libc::winsize {
                        ws_row: 0,
                        ws_col: 0,
                        ws_xpixel: 0,
                        ws_ypixel: 0,
                    };
                    // SAFETY: master is a terminal and size is writable for a winsize.
                    assert_eq!(
                        unsafe {
                            libc::ioctl(master.as_raw_fd(), libc::TIOCGWINSZ as _, &mut size)
                        },
                        0
                    );
                    assert_eq!((size.ws_col, size.ws_row), (cols, rows));
                    let marker = format!("worker-{worker}-iteration-{iteration}");
                    writeln!(slave, "{marker}").unwrap();
                    let expected = format!("{marker}\r\n").into_bytes();
                    let mut received = vec![0; expected.len()];
                    let mut read = 0;
                    let deadline = Instant::now() + Duration::from_secs(5);
                    while read < received.len() {
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        assert!(!remaining.is_zero(), "PTY output did not arrive");
                        assert_ne!(ready(master.as_raw_fd(), remaining.as_millis() as i32), 0);
                        let count = master.read(&mut received[read..]).unwrap();
                        assert_ne!(count, 0, "PTY output ended early");
                        read += count;
                    }
                    assert_eq!(received, expected, "PTY output crossed between workers");
                }
            });
        }
    });
}

#[cfg(target_os = "linux")]
mod inheritance {
    use super::*;
    use std::fs::File;
    use std::os::fd::FromRawFd;
    use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
    use std::os::unix::process::CommandExt;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};

    struct Directory(PathBuf);

    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    struct Probe(Child);

    impl Probe {
        fn start(binary: &Path, fd: RawFd, master: bool) -> (Self, bool) {
            let mut metadata = std::mem::MaybeUninit::<libc::stat>::uninit();
            // SAFETY: the allocator owns fd while paused; fstat initializes metadata on success.
            assert_eq!(unsafe { libc::fstat(fd, metadata.as_mut_ptr()) }, 0);
            let metadata = unsafe { metadata.assume_init() };
            let mut name = [0u8; libc::PATH_MAX as usize];
            // SAFETY: fd is held open and name is a private writable buffer.
            let named = unsafe {
                if master {
                    libc::ptsname_r(fd, name.as_mut_ptr().cast(), name.len())
                } else {
                    libc::ttyname_r(fd, name.as_mut_ptr().cast(), name.len())
                }
            };
            assert_eq!(named, 0);
            let name = CStr::from_bytes_until_nul(&name).unwrap().to_str().unwrap();
            let mut command = Command::new(binary);
            command
                .args([
                    fd.to_string(),
                    u8::from(master).to_string(),
                    metadata.st_dev.to_string(),
                    metadata.st_ino.to_string(),
                    metadata.st_rdev.to_string(),
                    name.to_owned(),
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit());
            // SAFETY: this empty callback forces Rust's fork/exec path and calls no unsafe code.
            unsafe { command.pre_exec(|| Ok(())) };
            let mut probe = Self(command.spawn().unwrap());
            let stdout = probe.0.stdout.as_mut().unwrap();
            assert_ne!(
                ready(stdout.as_raw_fd(), 5000),
                0,
                "the exec probe did not answer"
            );
            let mut answer = [0; 2];
            stdout.read_exact(&mut answer).unwrap();
            assert!(
                answer == *b"0\n" || answer == *b"1\n",
                "invalid probe answer: {answer:?}"
            );
            (probe, answer[0] == b'1')
        }

        fn finish(&mut self) {
            drop(self.0.stdin.take());
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if let Some(status) = self.0.try_wait().unwrap() {
                    assert!(status.success(), "exec probe failed: {status}");
                    return;
                }
                assert!(Instant::now() < deadline, "exec probe did not stop");
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            drop(self.0.stdin.take());
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    // The old allocator is a positive control: stop inside its openpty-to-fcntl window.
    fn old_open_observed(mut allocated: impl FnMut(RawFd)) -> std::io::Result<(File, File)> {
        let (mut master, mut slave) = (-1, -1);
        // SAFETY: all pointers are valid; null name, termios and size are allowed.
        if unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        } == -1
        {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: openpty returned two new descriptors owned only by this function.
        let pair = unsafe { (File::from_raw_fd(master), File::from_raw_fd(slave)) };
        allocated(master);
        allocated(slave);
        for fd in [master, slave] {
            // SAFETY: pair owns fd; this intentionally retains the old non-atomic behavior.
            if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } == -1 {
                return Err(std::io::Error::last_os_error());
            }
        }
        Ok(pair)
    }

    #[test]
    fn pty_allocations_do_not_escape_through_a_concurrent_exec() {
        let mut nonce = [0u8; 16];
        getrandom::fill(&mut nonce).unwrap();
        let nonce: String = nonce.iter().map(|byte| format!("{byte:02x}")).collect();
        let path = std::env::temp_dir().join(format!("fastexec-pty-exec-{nonce}"));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        let directory = Directory(path);
        let source = directory.0.join("probe.c");
        let binary = directory.0.join("probe");
        std::fs::write(
            &source,
            r#"#define _GNU_SOURCE
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
int main(int argc, char **argv) {
  if (argc != 7) return 2;
  alarm(15);
  int fd = atoi(argv[1]);
  struct stat st;
  char name[PATH_MAX] = {0};
  int inherited = fstat(fd, &st) == 0 && S_ISCHR(st.st_mode) &&
    (uintmax_t)st.st_dev == strtoumax(argv[3], NULL, 10) &&
    (uintmax_t)st.st_ino == strtoumax(argv[4], NULL, 10) &&
    (uintmax_t)st.st_rdev == strtoumax(argv[5], NULL, 10);
  if (inherited) {
    int named = atoi(argv[2]) ? ptsname_r(fd, name, sizeof(name)) : ttyname_r(fd, name, sizeof(name));
    inherited = named == 0 && strcmp(name, argv[6]) == 0;
  }
  if (printf("%d\n", inherited) < 0 || fflush(stdout) != 0) return 1;
  char stop;
  while (read(STDIN_FILENO, &stop, 1) < 0 && errno == EINTR) {}
  return 0;
}
"#,
        )
        .unwrap();
        assert!(
            Command::new("cc")
                .args(["-std=c99", "-Wall", "-Wextra", "-Werror"])
                .arg(&source)
                .arg("-o")
                .arg(&binary)
                .status()
                .unwrap()
                .success(),
            "compile the ordinary exec probe"
        );
        assert_eq!(
            std::fs::metadata(&binary).unwrap().permissions().mode() & 0o6000,
            0
        );

        for old in [true, false] {
            let (created_tx, created_rx) = std::sync::mpsc::channel();
            let (resume_tx, resume_rx) = std::sync::mpsc::sync_channel(0);
            let mut probes = Vec::new();
            let (mut master, slave) = std::thread::scope(|scope| {
                let allocator = scope.spawn(move || {
                    let observed = |fd| {
                        created_tx.send(fd).unwrap();
                        resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                    };
                    if old {
                        old_open_observed(observed)
                    } else {
                        unix_pty::open_observed(80, 24, observed)
                    }
                });
                for is_master in [true, false] {
                    let fd = created_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                    let (probe, inherited) = Probe::start(&binary, fd, is_master);
                    probes.push(probe);
                    assert_eq!(
                        inherited, old,
                        "old={old}, master={is_master}: descriptor crossed exec"
                    );
                    resume_tx.send(()).unwrap();
                }
                allocator.join().unwrap().unwrap()
            });
            drop(slave);
            if old {
                assert_eq!(
                    ready(master.as_raw_fd(), 200),
                    0,
                    "the positive control must keep the terminal alive through its leaked slave"
                );
            } else {
                assert_ne!(
                    ready(master.as_raw_fd(), 5000) & libc::POLLHUP,
                    0,
                    "an unrelated executed child kept the terminal alive"
                );
            }
            for probe in &mut probes {
                probe.finish();
            }
            assert_ne!(ready(master.as_raw_fd(), 5000) & libc::POLLHUP, 0);
            let mut byte = [0];
            match master.read(&mut byte) {
                Ok(0) => {}
                Err(error) if error.raw_os_error() == Some(libc::EIO) => {}
                result => {
                    panic!("the terminal did not reach EOF after all slaves closed: {result:?}")
                }
            }
        }
    }
}
