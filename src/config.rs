//! Process configuration: environment parsing, listen-address checks, and server instructions.

use crate::env::{self, LaunchContext};
use crate::files;
use std::sync::Arc;

/// Optional fixed `host:port` listener address for standalone `/t/` transfers.
pub const TRANSFER_ADDR: &str = "FASTEXEC_TRANSFER_ADDR";
/// The value that names the address clients reach, when a proxy stands in front of the listener.
pub const PUBLIC_URL: &str = "FASTEXEC_PUBLIC_URL";
/// Allows `--http` and `/t/` listeners to bind outside loopback when set to `1`.
pub const HTTP_ALLOW_REMOTE: &str = "FASTEXEC_HTTP_ALLOW_REMOTE";
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
        env::parse_image_compress(std::env::var_os(env::IMAGE_COMPRESS).as_deref())?;
        env::parse_max_media_bytes(std::env::var_os(env::MAX_MEDIA_BYTES).as_deref())?;
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
    let resolved = files::path::resolve(raw, launch).map_err(|error| {
        format!(
            "invalid {INSTRUCTIONS_FILE}: {}",
            error.trim_end_matches('.')
        )
    })?;
    let shown_path = files::path::display_path(&resolved);
    let (file, _) = files::path::open_regular(&resolved)
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
    let normalized = files::edit::normalize_to_lf(&text);
    let content = normalized.trim_start_matches('\u{feff}');
    if content.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(content.to_owned()))
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
            "{label} {value} is not a loopback address; set {HTTP_ALLOW_REMOTE}=1 only behind a TLS-terminating proxy"
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
            "{label} {addr} is an unspecified address; set {PUBLIC_URL} to the URL clients reach"
        ));
    }
    Ok(())
}

/// The address a `--http` argument names, refused when it is outside loopback and serving there
/// was not allowed.
pub fn http_addr(value: &str) -> Result<std::net::SocketAddr, String> {
    check_listen_addr("--http", value, remote_allowed())
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
    transfer_addr_of(std::env::var_os(TRANSFER_ADDR).as_deref(), remote_allowed())
}

/// Whether a bind outside loopback was allowed: the daemon then trusts a TLS-terminating proxy in
/// front of it for both the host and the origin.
pub fn remote_allowed() -> bool {
    std::env::var(HTTP_ALLOW_REMOTE).is_ok_and(|value| value == "1")
}

/// The root this process was started with: `None` when the variable is unset, the normalized root
/// when it is valid, and the reason it cannot be used otherwise. Called once when an HTTP daemon
/// starts, so nothing rereads the environment while a URL is minted.
pub fn public_root() -> Result<Option<String>, String> {
    root_of(std::env::var_os(PUBLIC_URL).as_deref())
}

/// [`public_root`] over a value that is already in hand, so the unset, the valid, the empty and
/// the non-UTF-8 cases are all one function.
pub(crate) fn root_of(value: Option<&std::ffi::OsStr>) -> Result<Option<String>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(text) = value.to_str() else {
        return Err(format!("{PUBLIC_URL} is not UTF-8"));
    };
    valid_root(text).map(Some)
}

/// A public root, validated and normalized: a scheme, a host, an optional port, and a path prefix
/// that ends in one `/`. Every rejection names the variable and the reason.
///
/// The value crosses trust boundaries, so it is a small wrapper over [`::http::Uri`] rather than a
/// URL normalizer: what it states is kept byte for byte, and only the scheme case and the one
/// trailing `/` are settled here.
pub fn valid_root(text: &str) -> Result<String, String> {
    // The value itself stays out of the message: a mistyped value can carry userinfo, and a
    // refusal is written to a terminal and to a daemon log.
    let refuse = |why: &str| Err(format!("{PUBLIC_URL} {why}"));
    if text
        .chars()
        .any(|character| character.is_whitespace() || character.is_control())
    {
        return refuse("contains whitespace or a control character");
    }
    if text.contains('?') || text.contains('#') {
        return refuse("carries a query or a fragment");
    }
    let Ok(uri) = text.parse::<::http::Uri>() else {
        return refuse(unparsed_form(text));
    };
    let Some(scheme) = uri.scheme_str() else {
        return refuse("must start with http:// or https://");
    };
    if !scheme.eq_ignore_ascii_case("http") && !scheme.eq_ignore_ascii_case("https") {
        return refuse("must start with http:// or https://");
    }
    let scheme = scheme.to_ascii_lowercase();
    let Some(authority) = uri.authority() else {
        return refuse("names no host");
    };
    // The raw authority is what a port is judged on: `Authority::port()` answers `None` for a port
    // it refuses, which would read as "no port at all".
    let raw = authority.as_str();
    if raw.contains('@') {
        return refuse("carries userinfo");
    }
    if authority.host().is_empty() {
        return refuse("names no host");
    }
    let port = match authority_port(raw) {
        Ok(port) => port,
        Err(why) => return refuse(why),
    };
    if let Some(port) = port {
        if port.is_empty() {
            return refuse("has an empty port");
        }
        if !port.bytes().all(|byte| byte.is_ascii_digit()) {
            return refuse("has a port that is not a decimal number");
        }
        match port.parse::<u32>() {
            Ok(number) if (1..=65535).contains(&number) => {}
            _ => return refuse("has a port outside 1-65535"),
        }
    }
    let path = uri.path();
    // The one trailing `/` a value may state is not an interior segment; anything else empty, and
    // any literal dot segment, is refused rather than normalized away.
    let mut segments: Vec<&str> = path.split('/').skip(1).collect();
    if segments.last().is_some_and(|last| last.is_empty()) {
        segments.pop();
    }
    for segment in segments {
        if segment.is_empty() {
            return refuse("has an empty path segment");
        }
        if segment == "." || segment == ".." {
            return refuse("has a dot path segment");
        }
    }
    let trailing = if path.ends_with('/') { "" } else { "/" };
    Ok(format!("{scheme}://{raw}{path}{trailing}"))
}

/// Why a value that `http::Uri` refused is not a URL: a port that is not decimal digits is the
/// common mistake worth naming.
fn unparsed_form(text: &str) -> &'static str {
    let Some((_, rest)) = text.split_once("://") else {
        return "must start with http:// or https://";
    };
    let authority = rest.split('/').next().unwrap_or_default();
    if authority.is_empty() {
        return "names no host";
    }
    match authority_port(authority) {
        Err(why) => why,
        Ok(Some(port)) if !port.bytes().all(|byte| byte.is_ascii_digit()) => {
            "has a port that is not a decimal number"
        }
        _ => "is not a URL",
    }
}

/// The port a raw authority states, or why the part after its host is not one.
///
/// `http::Uri` reads a bracketed authority loosely: `[]` parses with a host of `[]`, and anything
/// behind the closing `]` is dropped from `host()` and answers `None` from `port()`. The raw text is
/// therefore read here; brackets must contain an IPv6 address, optionally followed by a port.
fn authority_port(authority: &str) -> Result<Option<&str>, &'static str> {
    let Some(rest) = authority.strip_prefix('[') else {
        let mut parts = authority.rsplitn(2, ':');
        let last = parts.next().unwrap_or_default();
        return Ok(parts.next().map(|_| last));
    };
    let Some((host, after)) = rest.split_once(']') else {
        return Err("has an unclosed bracketed host");
    };
    if host.is_empty() {
        return Err("names no host");
    }
    if host.parse::<std::net::Ipv6Addr>().is_err() {
        return Err("has a bracketed host that is not an IPv6 address");
    }
    match after.strip_prefix(':') {
        Some(port) => Ok(Some(port)),
        None if after.is_empty() => Ok(None),
        None => Err("has text after a bracketed host"),
    }
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
        TRANSFER_ADDR, check_listen_addr, check_routable_addr, parse_bool_env, transfer_addr_of,
    };
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
        for bad in ["", "on", "off", "1", "0", "TRUE", "True", " true "] {
            let error = parse_bool_env("FASTEXEC_YOLO", Some(OsStr::new(bad)), false).unwrap_err();
            assert_eq!(
                error,
                format!("invalid FASTEXEC_YOLO {bad:?}; expected true or false")
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
    }
}

#[cfg(test)]
mod instructions_file_tests {
    use super::*;
    use std::ffi::OsString;
    use std::path::PathBuf;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "fastexec-instructions-{label}-{}",
                crate::id::random_chars(10).unwrap()
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

#[cfg(test)]
mod public_url_tests {
    use super::{root_of, valid_root};

    /// Every value that names an address clients reach, normalized to what a URL is minted from.
    #[test]
    fn a_public_root_keeps_what_it_states_and_gains_one_trailing_slash() {
        let valid = [
            ("http://h", "http://h/"),
            ("http://h/", "http://h/"),
            ("https://h.example/mac", "https://h.example/mac/"),
            ("https://h.example/mac/", "https://h.example/mac/"),
            ("HTTP://Host.Example/Mac", "http://Host.Example/Mac/"),
            (
                "https://HtTpS.example:8443/mac/",
                "https://HtTpS.example:8443/mac/",
            ),
            ("http://127.0.0.1:8765/mac/", "http://127.0.0.1:8765/mac/"),
            ("https://h:65535/", "https://h:65535/"),
            // A bracketed IPv6 host is an authority like any other, with or without a port.
            ("http://[::1]", "http://[::1]/"),
            ("http://[::1]:80/", "http://[::1]:80/"),
            ("http://[::1]:8080/mac", "http://[::1]:8080/mac/"),
            (
                "https://[2001:db8:0:0:0:0:0:1]:443/files",
                "https://[2001:db8:0:0:0:0:0:1]:443/files/",
            ),
            ("http://[::ffff:127.0.0.1]/", "http://[::ffff:127.0.0.1]/"),
            // Encoded bytes are what a proxy rewrites to, so they are kept as stated.
            ("https://h/a%20b/", "https://h/a%20b/"),
            ("https://h/%2E%2E/%2e/", "https://h/%2E%2E/%2e/"),
        ];
        for (value, expected) in valid {
            assert_eq!(valid_root(value).as_deref(), Ok(expected), "{value:?}");
        }
    }

    /// Each way a value cannot name an address, with the reason it is refused for.
    #[test]
    fn a_public_root_that_cannot_be_used_is_refused_with_its_reason() {
        let refused = [
            ("", "must start with http:// or https://"),
            (
                "http://h/ mac/",
                "contains whitespace or a control character",
            ),
            (
                "http://h/a\u{7}",
                "contains whitespace or a control character",
            ),
            ("https://h/a?b=c", "carries a query or a fragment"),
            ("https://h/a#b", "carries a query or a fragment"),
            ("ftp://h/mac/", "must start with http:// or https://"),
            ("h/mac/", "must start with http:// or https://"),
            ("http:///mac/", "names no host"),
            ("http://user@h/mac/", "carries userinfo"),
            ("http://user:secret@h/mac/", "carries userinfo"),
            ("http://h:/", "has an empty port"),
            ("http://h:/mac/", "has an empty port"),
            ("http://h:0/", "has a port outside 1-65535"),
            ("http://h:65536/", "has a port outside 1-65535"),
            ("http://h:abc/", "has a port that is not a decimal number"),
            ("http://h:8a80/", "has a port that is not a decimal number"),
            ("http://h:-1/", "has a port that is not a decimal number"),
            ("http://[::1]:/", "has an empty port"),
            ("http://[::1]:70000/", "has a port outside 1-65535"),
            // `http::Uri` reads a bracketed authority loosely, so these are judged on the raw text.
            ("http://[]/", "names no host"),
            ("http://[]:80/mac/", "names no host"),
            (
                "http://[localhost]/",
                "has a bracketed host that is not an IPv6 address",
            ),
            (
                "http://[127.0.0.1]:80/mac/",
                "has a bracketed host that is not an IPv6 address",
            ),
            (
                "http://[:::1]/",
                "has a bracketed host that is not an IPv6 address",
            ),
            (
                "http://[::1::]/",
                "has a bracketed host that is not an IPv6 address",
            ),
            ("http://[::1]junk/mac/", "has text after a bracketed host"),
            (
                "http://[::1]junk:abc/mac/",
                "has text after a bracketed host",
            ),
            ("http://[::1:8080/mac/", "has an unclosed bracketed host"),
            ("http://h/./mac/", "has a dot path segment"),
            ("http://h/../mac/", "has a dot path segment"),
            ("http://h/mac/.", "has a dot path segment"),
            ("http://h//mac/", "has an empty path segment"),
            ("http://h/mac//deep/", "has an empty path segment"),
        ];
        for (value, reason) in refused {
            let message = valid_root(value).expect_err(value);
            // The reason and the variable, and never the value: a mistyped value can carry userinfo
            // and this message reaches a terminal and a daemon log.
            assert_eq!(
                message,
                format!("FASTEXEC_PUBLIC_URL {reason}"),
                "{value:?}"
            );
        }
    }

    /// The variable is unset or it is a value; a value that is not UTF-8 cannot be a URL.
    #[test]
    fn an_unset_public_root_is_no_root_and_an_unreadable_one_is_refused() {
        assert_eq!(root_of(None), Ok(None));
        assert_eq!(
            root_of(Some(std::ffi::OsStr::new("https://h/mac"))),
            Ok(Some("https://h/mac/".to_owned()))
        );
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            let message = root_of(Some(std::ffi::OsStr::from_bytes(b"https://h/\xff")))
                .expect_err("a value that is not UTF-8");
            assert!(
                message.starts_with("FASTEXEC_PUBLIC_URL ") && message.contains("UTF-8"),
                "{message}"
            );
        }
    }
}
