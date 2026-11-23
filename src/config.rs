//! Process configuration: environment parsing, listen-address checks, and server instructions.

use crate::files;
use crate::http;
use crate::service::{self, Render};
use crate::session::{self, LaunchContext};
use std::sync::Arc;

/// Optional fixed `host:port` listener address for standalone `/t/` transfers.
pub const TRANSFER_ADDR: &str = "FASTEXEC_TRANSFER_ADDR";
/// Optional path to a plain UTF-8 file appended to MCP server instructions at startup.
pub const INSTRUCTIONS_FILE: &str = "FASTEXEC_INSTRUCTIONS_FILE";
/// Controls whether server instructions are also exposed as an MCP tool (`off`, `move`, `copy`).
pub const INSTRUCTIONS_TOOL: &str = "FASTEXEC_INSTRUCTIONS_TOOL";
/// Maximum raw byte size accepted for `FASTEXEC_INSTRUCTIONS_FILE`.
const MAX_INSTRUCTIONS_BYTES: usize = 16 * 1024;
pub(crate) const MOVE_INSTRUCTIONS_POINTER: &str =
    "See the instructions tool for host-specific guidance.";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum InstructionsToolMode {
    #[default]
    Off,
    Move,
    Copy,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Tools {
    pub codemode: bool,
    pub transfer: bool,
    pub yolo: bool,
}

impl Tools {
    pub fn from_env() -> Result<Self, String> {
        session::parse_image_compress(std::env::var_os(session::IMAGE_COMPRESS).as_deref())?;
        session::parse_max_media_bytes(std::env::var_os(session::MAX_MEDIA_BYTES).as_deref())?;
        Ok(Self {
            codemode: codemode()?,
            transfer: transfer()?,
            yolo: yolo()?,
        })
    }
}

/// Server instructions say when to use the tools; hosts that show one line take the first. The
/// second line states how long tasks live, and codemode adds a routing hint when enabled.
pub(crate) fn server_instructions(
    daemon_owned: bool,
    codemode: bool,
    extra: Option<&str>,
) -> Arc<str> {
    let lifetime = if daemon_owned {
        "tasks run in a shared background daemon, outlive this session, and are visible to every session using it."
    } else {
        "tasks end when the session ends."
    };
    let mut text = if codemode {
        format!(
            "Run bash tasks with exec, orchestrate multi-step tool workflows in JavaScript with codemode, and read, write, and edit files on this host.\nPrefer exec over one-shot shell tools when a command may outlive a single call or needs input; {lifetime}\nPrefer codemode over multi-turn exec or read calls when aggregating across multiple files, looping or branching on tool results, or filtering bulky output in-process to save context."
        )
    } else {
        format!(
            "Run bash tasks with exec and read, write, and edit files on this host.\nPrefer it over one-shot shell tools when a command may outlive a single call or needs input; {lifetime}"
        )
    };
    if let Some(extra) = extra {
        text.push_str("\n\n");
        text.push_str(extra);
    }
    Arc::from(text)
}

/// Composes the protocol `instructions` snapshot and optional `instructions` tool snapshot.
pub(crate) fn compose_instructions(
    daemon_owned: bool,
    codemode: bool,
    extra: Option<&str>,
    mode: InstructionsToolMode,
) -> (Arc<str>, Option<Arc<str>>) {
    match mode {
        InstructionsToolMode::Off => (server_instructions(daemon_owned, codemode, extra), None),
        InstructionsToolMode::Move => match extra {
            Some(extra) => (
                server_instructions(daemon_owned, codemode, Some(MOVE_INSTRUCTIONS_POINTER)),
                Some(Arc::from(extra)),
            ),
            None => {
                let base = server_instructions(daemon_owned, codemode, None);
                (Arc::clone(&base), Some(base))
            }
        },
        InstructionsToolMode::Copy => {
            let full = server_instructions(daemon_owned, codemode, extra);
            (Arc::clone(&full), Some(full))
        }
    }
}

/// Loads and normalizes `FASTEXEC_INSTRUCTIONS_FILE` once at startup.
pub(crate) fn instructions_file(launch: &LaunchContext) -> Result<Option<String>, String> {
    use std::io::Read;

    let Some(value) = launch.var(INSTRUCTIONS_FILE) else {
        return Ok(None);
    };
    let Some(raw) = value.to_str() else {
        return Err(format!("{INSTRUCTIONS_FILE} is not UTF-8"));
    };
    if raw.is_empty() {
        return Err(format!("{INSTRUCTIONS_FILE} must not be empty"));
    }
    let resolved = files::resolve(raw, launch).map_err(|error| {
        format!(
            "invalid {INSTRUCTIONS_FILE}: {}",
            error.trim_end_matches('.')
        )
    })?;
    let shown_path = files::display_path(&resolved);
    let (file, _) = files::open_regular(&resolved)
        .map_err(|error| format!("cannot read {INSTRUCTIONS_FILE} {shown_path}: {error}"))?;
    let mut bytes = Vec::new();
    file.take(MAX_INSTRUCTIONS_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| format!("cannot read {INSTRUCTIONS_FILE} {shown_path}: {error}"))?;
    if bytes.len() > MAX_INSTRUCTIONS_BYTES {
        return Err(format!(
            "{INSTRUCTIONS_FILE} {shown_path} exceeds {MAX_INSTRUCTIONS_BYTES} bytes"
        ));
    }
    if bytes.contains(&0) {
        return Err(format!(
            "{INSTRUCTIONS_FILE} {shown_path} contains a NUL byte"
        ));
    }
    let text = String::from_utf8(bytes)
        .map_err(|_| format!("{INSTRUCTIONS_FILE} {shown_path} is not valid UTF-8"))?;
    let normalized = files::normalize_to_lf(&text);
    let content = normalized.trim_start_matches('\u{feff}');
    if content.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(content.to_owned()))
}

/// The output mode `FASTEXEC_OUTPUT_MODE` names, or the message saying what it held instead.
pub fn output_mode() -> Result<service::OutputMode, String> {
    output_mode_of(std::env::var_os("FASTEXEC_OUTPUT_MODE").as_deref())
}

fn output_mode_of(value: Option<&std::ffi::OsStr>) -> Result<service::OutputMode, String> {
    let Some(value) = value else {
        return Ok(service::OutputMode::Text);
    };
    match value.to_str() {
        Some("text") => Ok(service::OutputMode::Text),
        Some("both") => Ok(service::OutputMode::Both),
        Some("structured") => Ok(service::OutputMode::Structured),
        _ => Err(format!(
            "invalid FASTEXEC_OUTPUT_MODE {value:?}; expected text, both, or structured"
        )),
    }
}

/// What `FASTEXEC_STRUCTURED_CONTENT` asks for, or the message saying what it held instead.
pub fn structured_content() -> Result<bool, String> {
    parse_bool_env(
        "FASTEXEC_STRUCTURED_CONTENT",
        std::env::var_os("FASTEXEC_STRUCTURED_CONTENT").as_deref(),
        true,
    )
}

/// How this process renders results: both settings, or the message naming the one that is wrong.
pub fn render() -> Result<Render, String> {
    Ok(Render {
        output_mode: output_mode()?,
        structured_content: structured_content()?,
    })
}

/// What `FASTEXEC_YOLO` asks for, or the message saying what it held instead.
pub fn yolo() -> Result<bool, String> {
    parse_bool_env(
        "FASTEXEC_YOLO",
        std::env::var_os("FASTEXEC_YOLO").as_deref(),
        false,
    )
}

/// Whether this server advertises JavaScript task orchestration.
pub fn codemode() -> Result<bool, String> {
    parse_bool_env(
        "FASTEXEC_CODEMODE",
        std::env::var_os("FASTEXEC_CODEMODE").as_deref(),
        false,
    )
}

/// Whether this server advertises `/t/` file transfer capabilities.
pub fn transfer() -> Result<bool, String> {
    parse_bool_env(
        "FASTEXEC_TRANSFER",
        std::env::var_os("FASTEXEC_TRANSFER").as_deref(),
        false,
    )
}

/// How `FASTEXEC_INSTRUCTIONS_TOOL` exposes server instructions as an MCP tool.
pub fn instructions_tool_mode() -> Result<InstructionsToolMode, String> {
    instructions_tool_mode_of(std::env::var_os(INSTRUCTIONS_TOOL).as_deref())
}

pub(crate) fn instructions_tool_mode_of(
    value: Option<&std::ffi::OsStr>,
) -> Result<InstructionsToolMode, String> {
    let Some(value) = value else {
        return Ok(InstructionsToolMode::Off);
    };
    match value.to_str() {
        Some("off") => Ok(InstructionsToolMode::Off),
        Some("move") => Ok(InstructionsToolMode::Move),
        Some("copy") => Ok(InstructionsToolMode::Copy),
        _ => Err(format!(
            "invalid {INSTRUCTIONS_TOOL} {value:?}; expected off, move, or copy"
        )),
    }
}

fn parse_bool_env(
    name: &str,
    value: Option<&std::ffi::OsStr>,
    default: bool,
) -> Result<bool, String> {
    let Some(value) = value else {
        return Ok(default);
    };
    match value.to_str() {
        Some("false") => Ok(false),
        Some("true") => Ok(true),
        _ => Err(format!("invalid {name} {value:?}; expected true or false")),
    }
}

/// The tokens a `--http` daemon admits, separated by ASCII whitespace. Diagnostics name
/// positions only, so a refused configuration never exposes a credential.
pub fn http_token() -> Result<Vec<String>, String> {
    http_tokens(std::env::var_os("FASTEXEC_HTTP_TOKEN").as_deref())
}

fn http_tokens(value: Option<&std::ffi::OsStr>) -> Result<Vec<String>, String> {
    let value = value.map_or(&[][..], std::ffi::OsStr::as_encoded_bytes);
    let mut tokens = Vec::new();
    for (index, token) in value
        .split(u8::is_ascii_whitespace)
        .filter(|token| !token.is_empty())
        .enumerate()
    {
        let position = index + 1;
        if token.len() < 16 || !token.iter().all(|byte| (0x21..=0x7e).contains(byte)) {
            return Err(format!(
                "FASTEXEC_HTTP_TOKEN token {position} must be at least 16 visible ASCII characters"
            ));
        }
        let token = std::str::from_utf8(token).expect("visible ASCII");
        if let Some(previous) = tokens.iter().position(|prior| prior == token) {
            return Err(format!(
                "FASTEXEC_HTTP_TOKEN token {position} duplicates token {}",
                previous + 1
            ));
        }
        tokens.push(token.to_owned());
    }
    if tokens.is_empty() {
        return Err(
            "FASTEXEC_HTTP_TOKEN token 1 is missing; at least one token is required".to_owned(),
        );
    }
    Ok(tokens)
}

pub(crate) fn check_listen_addr(
    label: &str,
    value: &str,
    allow_remote: bool,
) -> Result<std::net::SocketAddr, String> {
    let addr: std::net::SocketAddr = value
        .parse()
        .map_err(|_| format!("{label} {value} is not an address (host:port)"))?;
    if !addr.ip().is_loopback() && !allow_remote {
        return Err(format!(
            "{label} {value} is not a loopback address; set FASTEXEC_HTTP_ALLOW_REMOTE=1 only behind a TLS-terminating proxy"
        ));
    }
    Ok(addr)
}

pub(crate) fn check_routable_addr(
    label: &str,
    addr: std::net::SocketAddr,
    has_public_root: bool,
) -> Result<(), String> {
    if !has_public_root && addr.ip().is_unspecified() {
        return Err(format!(
            "{label} {addr} is an unspecified address; set {} to the URL clients reach",
            http::PUBLIC_URL
        ));
    }
    Ok(())
}

/// The address a `--http` argument names, refused when it is outside loopback and serving there
/// was not allowed.
pub fn http_addr(value: &str) -> Result<std::net::SocketAddr, String> {
    check_listen_addr("--http", value, http::remote_allowed())
}

pub(crate) fn transfer_addr_of(
    value: Option<&std::ffi::OsStr>,
    allow_remote: bool,
) -> Result<Option<std::net::SocketAddr>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(text) = value.to_str() else {
        return Err(format!("{TRANSFER_ADDR} is not UTF-8"));
    };
    check_listen_addr(TRANSFER_ADDR, text, allow_remote).map(Some)
}

pub fn transfer_addr() -> Result<Option<std::net::SocketAddr>, String> {
    transfer_addr_of(
        std::env::var_os(TRANSFER_ADDR).as_deref(),
        http::remote_allowed(),
    )
}

#[cfg(test)]
mod http_token_tests {
    use super::http_tokens;
    use std::ffi::OsStr;

    #[test]
    fn several_tokens_keep_their_positions_across_ascii_whitespace() {
        let first = "0123456789abcdef";
        let second = "fedcba9876543210";
        let third = "visible!@#$%^&*()";
        let value = format!(" \t{first}\n{second}\r\n{third}\x0c ");
        assert_eq!(
            http_tokens(Some(OsStr::new(&value))).unwrap(),
            [first, second, third]
        );
    }

    #[test]
    fn missing_short_non_ascii_and_duplicate_tokens_name_positions_only() {
        let good = "0123456789abcdef";
        for value in [None, Some(""), Some(" \t\r\n\x0c")] {
            let error = http_tokens(value.map(OsStr::new)).unwrap_err();
            assert!(error.contains("token 1"), "{error}");
        }
        for bad in ["secret-short", "0123456789abcdefé", "0123456789abcdef\0"] {
            let value = format!("{good} {bad}");
            let error = http_tokens(Some(OsStr::new(&value))).unwrap_err();
            assert!(error.contains("token 2"), "{error}");
            assert!(!error.contains(good) && !error.contains(bad), "{error}");
        }
        let value = format!("{good} fedcba9876543210 {good}");
        let error = http_tokens(Some(OsStr::new(&value))).unwrap_err();
        assert!(error.contains("token 3 duplicates token 1"), "{error}");
        assert!(!error.contains(good), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_tokens_are_refused_at_their_position() {
        use std::os::unix::ffi::OsStrExt;
        let error = http_tokens(Some(OsStr::from_bytes(
            b"0123456789abcdef abcdef0123456789\xff",
        )))
        .unwrap_err();
        assert!(error.contains("token 2"), "{error}");
        assert!(!error.contains("abcdef"), "{error}");
    }
}

#[cfg(test)]
mod yolo_tests {
    use super::{
        TRANSFER_ADDR, check_listen_addr, check_routable_addr, output_mode_of, parse_bool_env,
        transfer_addr_of,
    };
    use crate::service::OutputMode;
    use std::ffi::OsStr;

    #[test]
    fn valid_and_invalid_values_match_contract() {
        assert_eq!(parse_bool_env("FASTEXEC_YOLO", None, false), Ok(false));
        assert_eq!(parse_bool_env("FASTEXEC_TRANSFER", None, false), Ok(false));
        assert_eq!(transfer_addr_of(None, false), Ok(None));
        assert_eq!(
            transfer_addr_of(Some(OsStr::new("127.0.0.1:0")), false),
            Ok(Some(std::net::SocketAddr::from(([127, 0, 0, 1], 0))))
        );
        assert!(transfer_addr_of(Some(OsStr::new("0.0.0.0:0")), false).is_err());
        assert!(transfer_addr_of(Some(OsStr::new("0.0.0.0:0")), true).is_ok());
        for unspecified in ["0.0.0.0:0", "[::]:0"] {
            let addr: std::net::SocketAddr = unspecified.parse().unwrap();
            assert_eq!(
                check_routable_addr(TRANSFER_ADDR, addr, false).unwrap_err(),
                format!(
                    "{TRANSFER_ADDR} {addr} is an unspecified address; set FASTEXEC_PUBLIC_URL to the URL clients reach"
                )
            );
            assert!(check_routable_addr(TRANSFER_ADDR, addr, true).is_ok());
        }
        for specific in ["127.0.0.1:0", "[::1]:0", "192.0.2.1:8080"] {
            let addr: std::net::SocketAddr = specific.parse().unwrap();
            assert!(check_routable_addr(TRANSFER_ADDR, addr, false).is_ok());
        }
        assert_eq!(
            check_listen_addr("--http", "0.0.0.0:0", false).unwrap_err(),
            "--http 0.0.0.0:0 is not a loopback address; set FASTEXEC_HTTP_ALLOW_REMOTE=1 only behind a TLS-terminating proxy"
        );
        assert_eq!(
            parse_bool_env("FASTEXEC_YOLO", Some(OsStr::new("false")), false),
            Ok(false)
        );
        assert_eq!(
            parse_bool_env("FASTEXEC_YOLO", Some(OsStr::new("true")), false),
            Ok(true)
        );
        assert_eq!(
            parse_bool_env("FASTEXEC_STRUCTURED_CONTENT", None, true),
            Ok(true)
        );
        assert_eq!(output_mode_of(None), Ok(OutputMode::Text));
        assert_eq!(
            output_mode_of(Some(OsStr::new("both"))),
            Ok(OutputMode::Both)
        );
        for bad in ["", "on", "off", "1", "0", "TRUE", "True", " true "] {
            let error = parse_bool_env("FASTEXEC_YOLO", Some(OsStr::new(bad)), false).unwrap_err();
            assert_eq!(
                error,
                format!("invalid FASTEXEC_YOLO {bad:?}; expected true or false")
            );
            let mode_error = output_mode_of(Some(OsStr::new(bad))).unwrap_err();
            assert_eq!(
                mode_error,
                format!("invalid FASTEXEC_OUTPUT_MODE {bad:?}; expected text, both, or structured")
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_value_is_refused() {
        use std::os::unix::ffi::OsStrExt;
        let bad = OsStr::from_bytes(b"\xff");
        let error = parse_bool_env("FASTEXEC_YOLO", Some(bad), false).unwrap_err();
        assert!(
            error.starts_with("invalid FASTEXEC_YOLO ")
                && error.ends_with("; expected true or false"),
            "{error}"
        );
        let mode_error = output_mode_of(Some(bad)).unwrap_err();
        assert!(
            mode_error.starts_with("invalid FASTEXEC_OUTPUT_MODE ")
                && mode_error.ends_with("; expected text, both, or structured"),
            "{mode_error}"
        );
    }

    #[cfg(windows)]
    #[test]
    fn unpaired_surrogate_value_is_refused() {
        use std::os::windows::ffi::OsStringExt;
        let value = std::ffi::OsString::from_wide(&[0xD800]);
        let error = parse_bool_env("FASTEXEC_YOLO", Some(&value), false).unwrap_err();
        assert!(
            error.starts_with("invalid FASTEXEC_YOLO ")
                && error.ends_with("; expected true or false"),
            "{error}"
        );
        let mode_error = output_mode_of(Some(&value)).unwrap_err();
        assert!(
            mode_error.starts_with("invalid FASTEXEC_OUTPUT_MODE ")
                && mode_error.ends_with("; expected text, both, or structured"),
            "{mode_error}"
        );
    }
}

#[cfg(test)]
mod instructions_file_tests {
    use super::*;
    use crate::tasks;
    use std::ffi::OsString;
    use std::path::PathBuf;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "fastexec-instructions-{label}-{}",
                tasks::random_chars(10).unwrap()
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn launch_with(cwd: &std::path::Path, value: Option<OsString>) -> LaunchContext {
        let mut env = Vec::new();
        if let Some(value) = value {
            env.push((OsString::from(INSTRUCTIONS_FILE), value));
        }
        LaunchContext::new(cwd.to_path_buf(), env)
    }

    #[test]
    fn env_var_unset_empty_and_non_utf8() {
        let dir = TempDir::new("env");
        assert_eq!(instructions_file(&launch_with(&dir.0, None)), Ok(None));

        let empty_err =
            instructions_file(&launch_with(&dir.0, Some(OsString::from("")))).unwrap_err();
        assert_eq!(empty_err, "FASTEXEC_INSTRUCTIONS_FILE must not be empty");

        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStringExt;
            let bad = OsString::from_vec(vec![b'a', 0xff]);
            let err = instructions_file(&launch_with(&dir.0, Some(bad))).unwrap_err();
            assert_eq!(err, "FASTEXEC_INSTRUCTIONS_FILE is not UTF-8");
        }
    }

    #[test]
    fn rejects_missing_directory_nul_invalid_utf8_and_oversized_split_multibyte() {
        let dir = TempDir::new("rejects");

        let missing = instructions_file(&launch_with(
            &dir.0,
            Some(OsString::from("does-not-exist.txt")),
        ))
        .unwrap_err();
        assert!(
            missing.starts_with("cannot read FASTEXEC_INSTRUCTIONS_FILE "),
            "{missing}"
        );

        let subdir = dir.0.join("subdir");
        std::fs::create_dir(&subdir).unwrap();
        let dir_err =
            instructions_file(&launch_with(&dir.0, Some(OsString::from("subdir")))).unwrap_err();
        assert!(
            dir_err.starts_with("cannot read FASTEXEC_INSTRUCTIONS_FILE ")
                && dir_err.contains("it is a directory")
                && !dir_err.contains("list it with exec"),
            "{dir_err}"
        );

        let nul_path = dir.0.join("has-nul.txt");
        std::fs::write(&nul_path, b"hello\0world").unwrap();
        let nul_err = instructions_file(&launch_with(&dir.0, Some(OsString::from("has-nul.txt"))))
            .unwrap_err();
        assert!(nul_err.ends_with(" contains a NUL byte"), "{nul_err}");

        let bad_utf8 = dir.0.join("bad.txt");
        std::fs::write(&bad_utf8, b"hello\xff").unwrap();
        let utf8_err =
            instructions_file(&launch_with(&dir.0, Some(OsString::from("bad.txt")))).unwrap_err();
        assert!(utf8_err.ends_with(" is not valid UTF-8"), "{utf8_err}");

        // 16_383 ASCII bytes + 3-byte UTF-8 char = 16_386 bytes; truncated read splits '中'
        // at byte 16_385, so size check must run before UTF-8 validation.
        let oversized = dir.0.join("oversized.txt");
        let mut payload = vec![b'a'; 16_383];
        payload.extend_from_slice("中".as_bytes());
        assert_eq!(payload.len(), 16_386);
        std::fs::write(&oversized, &payload).unwrap();
        let size_err =
            instructions_file(&launch_with(&dir.0, Some(OsString::from("oversized.txt"))))
                .unwrap_err();
        assert!(size_err.ends_with(" exceeds 16384 bytes"), "{size_err}");
    }

    #[test]
    fn exact_cap_normalization_preservation_and_blank_files() {
        let dir = TempDir::new("valid");

        let exact = dir.0.join("exact.txt");
        let exact_text = "x".repeat(MAX_INSTRUCTIONS_BYTES);
        std::fs::write(&exact, &exact_text).unwrap();
        assert_eq!(
            instructions_file(&launch_with(&dir.0, Some(OsString::from("exact.txt")))),
            Ok(Some(exact_text))
        );

        let formatted = dir.0.join("formatted.txt");
        std::fs::write(&formatted, "\u{feff}    indented\r\nline 2\rline 3\n").unwrap();
        assert_eq!(
            instructions_file(&launch_with(&dir.0, Some(OsString::from("formatted.txt")))),
            Ok(Some("    indented\nline 2\nline 3\n".to_owned()))
        );

        let ws_only = dir.0.join("ws.txt");
        std::fs::write(&ws_only, "  \r\n\t \n").unwrap();
        assert_eq!(
            instructions_file(&launch_with(&dir.0, Some(OsString::from("ws.txt")))),
            Ok(None)
        );

        let bom_only = dir.0.join("bom.txt");
        std::fs::write(&bom_only, "\u{feff}\u{feff}  \r\n").unwrap();
        assert_eq!(
            instructions_file(&launch_with(&dir.0, Some(OsString::from("bom.txt")))),
            Ok(None)
        );
    }

    #[test]
    fn server_instructions_appends_extra_and_keeps_first_line() {
        for daemon_owned in [false, true] {
            for codemode in [false, true] {
                let base = server_instructions(daemon_owned, codemode, None);
                let with_extra = server_instructions(
                    daemon_owned,
                    codemode,
                    Some("    custom line\ntrailing\n"),
                );
                assert_eq!(base.lines().next(), with_extra.lines().next());
                assert_eq!(
                    with_extra.as_ref(),
                    format!("{base}\n\n    custom line\ntrailing\n")
                );
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn unix_fifo_fails_without_blocking() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;

        let dir = TempDir::new("fifo");
        let fifo = dir.0.join("pipe");
        let c_path = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        // SAFETY: `c_path` is a valid NUL-terminated path string.
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);

        let started = std::time::Instant::now();
        let err =
            instructions_file(&launch_with(&dir.0, Some(OsString::from("pipe")))).unwrap_err();
        assert!(
            started.elapsed() < std::time::Duration::from_secs(2),
            "FIFO check took {:?}",
            started.elapsed()
        );
        assert!(
            err.contains("it is not a regular file"),
            "unexpected FIFO error: {err}"
        );
    }
}
