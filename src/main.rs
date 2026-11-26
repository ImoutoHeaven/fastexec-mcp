//! Fastexec MCP server binary root.

mod cli;
mod codemode;
mod config;
mod daemon;
mod endpoint;
mod env;
mod events;
mod files;
mod http;
mod http_wire;
mod id;
mod ipc;
mod lease;
mod mcp;
mod media;
mod platform;
mod render;
mod service;
mod tasks;
mod term;
mod transfer;
mod transport;
mod wait;

fn main() {
    const CLI_USAGE: &str = "\
usage: fastexec [--help | --version]
       fastexec daemon start [--http <addr>] [--idle-exit on|off]
       fastexec daemon run [--http <addr>]
       fastexec daemon status
       fastexec daemon stop
       fastexec daemon emit [--namespace <ns>] [--source <src>] [--topic <topic>] [--stream]

With no arguments, fastexec serves MCP over stdio.";

    #[cfg(unix)]
    // Keep every child waitable before starting any thread or helper. Inheriting SIG_IGN or
    // SA_NOCLDWAIT would let the kernel reuse a task host's PID before its Tree is finished.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = libc::SIG_DFL;
        libc::sigemptyset(&mut action.sa_mask);
        if libc::sigaction(libc::SIGCHLD, &action, std::ptr::null_mut()) == -1 {
            eprintln!(
                "fastexec: cannot retain child exit statuses: {}",
                std::io::Error::last_os_error()
            );
            std::process::exit(1);
        }
    }
    let args: Vec<std::ffi::OsString> = std::env::args_os().skip(1).collect();
    #[cfg(unix)]
    {
        if args.first().is_some_and(|arg| arg == platform::host::FLAG) {
            platform::host::main(&args[1..]);
        }
    }
    if args
        .first()
        .is_some_and(|arg| arg == platform::process::ENV_FLAG)
    {
        platform::process::dump_env();
    }
    if args.first().is_some_and(|arg| arg == "daemon") {
        std::process::exit(cli::daemon::daemon_main(&args[1..]));
    }
    match args.as_slice() {
        [] => cli::stdio::serve(),
        [arg] if arg == "--version" || arg == "-V" => {
            println!("fastexec {}", env!("CARGO_PKG_VERSION"));
            std::process::exit(0);
        }
        [arg] if arg == "--help" || arg == "-h" => {
            println!("{CLI_USAGE}");
            std::process::exit(0);
        }
        [first, extra, ..]
            if first == "--version" || first == "-V" || first == "--help" || first == "-h" =>
        {
            eprintln!(
                "fastexec: unknown argument {}\n{CLI_USAGE}",
                extra.to_string_lossy()
            );
            std::process::exit(2);
        }
        [unknown, ..] => {
            eprintln!(
                "fastexec: unknown argument {}\n{CLI_USAGE}",
                unknown.to_string_lossy()
            );
            std::process::exit(2);
        }
    }
}
