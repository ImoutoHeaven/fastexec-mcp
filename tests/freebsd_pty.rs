//! FreeBSD must retain queued terminal output until its consumer reads it.
#![cfg(target_os = "freebsd")]

#[path = "../src/unix_pty.rs"]
mod unix_pty;

use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::process::{Child, Command};
use std::time::{Duration, Instant};

struct Host {
    child: Child,
    status: std::io::PipeReader,
    master: Option<File>,
    lifeline: Option<std::io::PipeWriter>,
}

impl Host {
    fn start(program: &str, args: &[&str]) -> Host {
        let (master, slave) = unix_pty::open(120, 30).expect("allocate a PTY for the task host");
        let (status, writer) = std::io::pipe().unwrap();
        let (reader, lifeline) = std::io::pipe().unwrap();
        let passed = [writer.as_raw_fd(), reader.as_raw_fd()];
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        command
            .arg("--fastexec-task-host")
            .args(passed.iter().map(|fd| fd.to_string()))
            .arg(program)
            .args(args)
            .stdin(slave.try_clone().unwrap())
            .stdout(slave.try_clone().unwrap())
            .stderr(slave);
        // SAFETY: only async-signal-safe syscalls run after fork; descriptors are owned above.
        unsafe {
            command.pre_exec(move || {
                if libc::setsid() == -1 || libc::ioctl(0, libc::TIOCSCTTY, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                for fd in passed {
                    if libc::fcntl(fd, libc::F_SETFD, 0) == -1 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                Ok(())
            });
        }
        let child = command.spawn().unwrap();
        drop(command);
        drop((writer, reader));
        let mut host = Host {
            child,
            status,
            master: Some(master),
            lifeline: Some(lifeline),
        };
        host.word(0);
        host
    }

    fn word(&mut self, expected: i32) {
        ready(self.status.as_raw_fd(), 5000);
        let mut bytes = [0; 4];
        self.status.read_exact(&mut bytes).unwrap();
        assert_eq!(i32::from_le_bytes(bytes), expected);
    }

    fn output(&mut self) -> Vec<u8> {
        let master = self.master.as_mut().unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut output = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "terminal did not reach EOF: {output:?}"
            );
            ready(master.as_raw_fd(), remaining.as_millis() as i32);
            let mut bytes = [0; 4096];
            match master.read(&mut bytes) {
                Ok(0) => return output,
                Ok(count) => output.extend_from_slice(&bytes[..count]),
                Err(error) if error.raw_os_error() == Some(libc::EIO) => return output,
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => panic!("terminal read: {error}"),
            }
        }
    }

    fn finish(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.child.try_wait().unwrap().is_none() {
            assert!(Instant::now() < deadline, "the task host did not exit");
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Host {
    fn drop(&mut self) {
        // Closing the reader also releases a host waiting in tcdrain after an assertion fails.
        drop(self.master.take());
        drop(self.lifeline.take());
        let deadline = Instant::now() + Duration::from_secs(5);
        while matches!(self.child.try_wait(), Ok(None)) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn ready(fd: i32, timeout: i32) {
    let mut poll = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    assert!(
        unsafe { libc::poll(&mut poll, 1, timeout) } > 0,
        "descriptor {fd} did not become readable: {}",
        std::io::Error::last_os_error()
    );
}

#[test]
fn an_exited_command_keeps_queued_pty_bytes_until_the_master_reads() {
    for failed_spawn in [false, true] {
        let (program, args): (&str, &[&str]) = if failed_spawn {
            ("/fastexec-test-missing-command/bin/bash", &[])
        } else {
            ("/bin/sh", &["-c", "printf 'queued-output\\n'; exit 17"])
        };
        let mut host = Host::start(program, args);
        host.word(if failed_spawn { 127 } else { 17 });
        // FreeBSD's last-slave close flushes after one second without reader progress.
        std::thread::sleep(Duration::from_millis(1200));
        assert!(
            host.child.try_wait().unwrap().is_none(),
            "the host ended before its queued PTY output was consumed"
        );
        let bytes = host.output();
        if failed_spawn {
            assert!(
                String::from_utf8_lossy(&bytes).contains("fastexec: cannot start"),
                "spawn diagnostic disappeared: {bytes:?}"
            );
        } else {
            assert_eq!(bytes, b"queued-output\r\n");
        }
        host.finish();
    }
}

#[test]
fn late_control_keys_cannot_stop_or_flush_a_finished_commands_output() {
    for key in [0x13, 0x03, 0x0f] {
        let mut host = Host::start("/bin/sh", &["-c", "printf 'final-output\\n'; exit 0"]);
        host.word(0);
        std::thread::sleep(Duration::from_millis(250));
        assert!(host.child.try_wait().unwrap().is_none());
        host.master.as_mut().unwrap().write_all(&[key]).unwrap();
        assert_eq!(
            host.output(),
            b"final-output\r\n",
            "late control key {key:#x} changed queued output"
        );
        host.finish();
    }
}

#[test]
fn closing_the_lifeline_drains_output_and_closing_the_master_unblocks_it() {
    let mut host = Host::start("/bin/sh", &["-c", "printf 'before-kill\\n'; exec sleep 20"]);
    ready(host.master.as_ref().unwrap().as_raw_fd(), 5000);
    drop(host.lifeline.take());
    std::thread::sleep(Duration::from_millis(250));
    assert!(
        host.child.try_wait().unwrap().is_none(),
        "lifeline cleanup killed the host before its queued output was read"
    );
    assert_eq!(host.output(), b"before-kill\r\n");
    host.finish();

    let mut host = Host::start("/bin/sh", &["-c", "printf 'unread\\n'; exit 0"]);
    host.word(0);
    drop(host.master.take());
    host.finish();
}

#[test]
fn the_host_drains_after_a_command_leaves_another_group_in_the_foreground() {
    let directory =
        std::env::temp_dir().join(format!("fastexec-pty-foreground-{}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    struct Remove(std::path::PathBuf);
    impl Drop for Remove {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _remove = Remove(directory.clone());
    let source = directory.join("foreground.c");
    let binary = directory.join("foreground");
    let mut file = File::create(&source).unwrap();
    file.write_all(include_bytes!("fixtures/freebsd-pty.c"))
        .unwrap();
    drop(file);
    assert!(
        Command::new("cc")
            .arg(&source)
            .arg("-o")
            .arg(&binary)
            .status()
            .unwrap()
            .success()
    );
    for args in [&[][..], &["stopped"][..]] {
        let mut host = Host::start(binary.to_str().unwrap(), args);
        host.word(19);
        std::thread::sleep(Duration::from_millis(250));
        assert!(
            host.child.try_wait().unwrap().is_none(),
            "a background host discarded its queued terminal output"
        );
        assert_eq!(host.output(), b"foreground-output\r\n");
        host.finish();
    }
}
