use std::path::Path;

/// Builds an RFC 8089 `file://` URI from `path` via `Path::components()`.
pub fn file_uri(path: &Path) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        use std::path::Component;

        let mut out = String::from("file://");
        let mut wrote_segment = false;
        for component in path.components() {
            match component {
                Component::RootDir => {
                    if !wrote_segment {
                        out.push('/');
                    }
                }
                Component::CurDir => {}
                Component::ParentDir => {
                    if !out.ends_with('/') {
                        out.push('/');
                    }
                    out.push_str("..");
                    wrote_segment = true;
                }
                Component::Normal(seg) => {
                    if !out.ends_with('/') {
                        out.push('/');
                    }
                    percent_encode_bytes(&mut out, seg.as_bytes());
                    wrote_segment = true;
                }
                Component::Prefix(_) => {}
            }
        }
        if out == "file://" {
            out.push('/');
        }
        out
    }
    #[cfg(windows)]
    {
        use std::path::{Component, Prefix};

        let mut owned: Vec<Vec<u8>> = Vec::new();
        for component in path.components() {
            match component {
                Component::Prefix(prefix) => match prefix.kind() {
                    Prefix::Disk(_) | Prefix::VerbatimDisk(_) => {}
                    Prefix::UNC(server, share) | Prefix::VerbatimUNC(server, share) => {
                        owned.push(windows_os_bytes(server));
                        owned.push(windows_os_bytes(share));
                    }
                    Prefix::Verbatim(name) | Prefix::DeviceNS(name) => {
                        owned.push(windows_os_bytes(name));
                    }
                },
                Component::Normal(seg) => owned.push(windows_os_bytes(seg)),
                Component::RootDir | Component::CurDir | Component::ParentDir => {}
            }
        }
        let mut idx = 0usize;
        let mut parts = Vec::new();
        for component in path.components() {
            match component {
                Component::Prefix(prefix) => match prefix.kind() {
                    Prefix::Disk(drive) | Prefix::VerbatimDisk(drive) => {
                        parts.push(WinUriPart::Disk(drive));
                    }
                    Prefix::UNC(_, _) | Prefix::VerbatimUNC(_, _) => {
                        let server = &owned[idx];
                        let share = &owned[idx + 1];
                        idx += 2;
                        parts.push(WinUriPart::Unc { server, share });
                    }
                    Prefix::Verbatim(_) | Prefix::DeviceNS(_) => {
                        let name = &owned[idx];
                        idx += 1;
                        parts.push(WinUriPart::OtherPrefix(name));
                    }
                },
                Component::RootDir => parts.push(WinUriPart::RootDir),
                Component::CurDir => {}
                Component::ParentDir => parts.push(WinUriPart::ParentDir),
                Component::Normal(_) => {
                    let seg = &owned[idx];
                    idx += 1;
                    parts.push(WinUriPart::Normal(seg));
                }
            }
        }
        format_windows_uri(parts)
    }
}

#[cfg(windows)]
pub(super) fn windows_os_bytes(text: &std::ffi::OsStr) -> Vec<u8> {
    use std::os::windows::ffi::OsStrExt;
    windows_utf16_bytes(text.encode_wide())
}

#[cfg(any(windows, test))]
pub(super) fn windows_utf16_bytes(units: impl IntoIterator<Item = u16>) -> Vec<u8> {
    let mut out = Vec::new();
    for decoded in char::decode_utf16(units) {
        match decoded {
            Ok(ch) => {
                let mut buf = [0u8; 4];
                out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
            }
            Err(error) => {
                // WTF-8 preserves an unpaired native Windows code unit.
                // decode_utf16 already combined every valid surrogate pair.
                let unit = error.unpaired_surrogate();
                out.extend_from_slice(&[
                    0xE0 | (unit >> 12) as u8,
                    0x80 | ((unit >> 6) as u8 & 0x3F),
                    0x80 | (unit as u8 & 0x3F),
                ]);
            }
        }
    }
    out
}

#[cfg(any(windows, test))]
pub(super) enum WinUriPart<'a> {
    Disk(u8),
    Unc { server: &'a [u8], share: &'a [u8] },
    OtherPrefix(&'a [u8]),
    RootDir,
    ParentDir,
    Normal(&'a [u8]),
}

#[cfg(any(windows, test))]
pub(super) fn format_windows_uri<'a>(parts: impl IntoIterator<Item = WinUriPart<'a>>) -> String {
    let mut out = String::from("file://");
    for part in parts {
        match part {
            WinUriPart::Disk(drive) => {
                out.push('/');
                out.push((drive as char).to_ascii_uppercase());
                out.push(':');
            }
            WinUriPart::Unc { server, share } => {
                encode_authority_host(&mut out, server);
                out.push('/');
                percent_encode_bytes(&mut out, share);
            }
            WinUriPart::OtherPrefix(name) => {
                out.push('/');
                percent_encode_bytes(&mut out, name);
            }
            WinUriPart::RootDir => {
                if !out.ends_with('/') {
                    out.push('/');
                }
            }
            WinUriPart::ParentDir => {
                if !out.ends_with('/') {
                    out.push('/');
                }
                out.push_str("..");
            }
            WinUriPart::Normal(seg) => {
                if !out.ends_with('/') {
                    out.push('/');
                }
                percent_encode_bytes(&mut out, seg);
            }
        }
    }
    if out == "file://" {
        out.push('/');
    }
    out
}

#[cfg(any(windows, test))]
fn encode_authority_host(out: &mut String, host: &[u8]) {
    let inner = host
        .strip_prefix(b"[")
        .and_then(|s| s.strip_suffix(b"]"))
        .unwrap_or(host);
    if inner.contains(&b':') {
        out.push('[');
        for &b in inner {
            if is_unreserved(b) || b == b':' {
                out.push(b as char);
            } else {
                push_pct(out, b);
            }
        }
        out.push(']');
        return;
    }
    for &b in inner {
        if is_unreserved(b) {
            out.push(b as char);
        } else {
            push_pct(out, b);
        }
    }
}

fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

fn push_pct(out: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    out.push('%');
    out.push(HEX[(byte >> 4) as usize] as char);
    out.push(HEX[(byte & 0x0F) as usize] as char);
}

fn percent_encode_bytes(out: &mut String, bytes: &[u8]) {
    for &b in bytes {
        if is_unreserved(b) {
            out.push(b as char);
        } else {
            push_pct(out, b);
        }
    }
}
