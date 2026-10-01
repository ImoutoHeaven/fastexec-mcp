//! Key names to terminal input bytes, following tmux `send-keys`.
//!
//! Names follow tmux `key-string.c` (`key_string_lookup_string`); bytes follow tmux
//! `input-keys.c` in its standard (non-extended) key mode. tmux silently drops a modifier
//! that has no legacy encoding (C-Enter, S-a, C-é); here such a key is an error instead.

/// The program's keyboard modes, read from the terminal emulator.
#[derive(Clone, Copy, Default)]
pub struct Modes {
    /// DECCKM: arrow keys send `ESC O x`.
    pub cursor: bool,
    /// DECKPAM: keypad keys send `ESC O x`.
    pub keypad: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Base {
    /// A character key: printable ASCII, a C0 control, or a Unicode character.
    Char(char),
    BSpace,
    BTab,
    /// A key with a CSI form: unmodified bytes, application-cursor bytes, and the modified
    /// form `ESC [ <param> ; <m> <last>`.
    Csi {
        plain: &'static str,
        app: Option<&'static str>,
        param: &'static str,
        last: char,
    },
    /// A keypad key: its normal-mode bytes and the final byte of its `ESC O x` form.
    Keypad {
        plain: &'static str,
        app: char,
    },
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Key {
    base: Base,
    ctrl: bool,
    meta: bool,
    shift: bool,
}

const fn csi(plain: &'static str, param: &'static str, last: char) -> Base {
    Base::Csi {
        plain,
        app: None,
        param,
        last,
    }
}

const fn arrow(plain: &'static str, app: &'static str, last: char) -> Base {
    Base::Csi {
        plain,
        app: Some(app),
        param: "1",
        last,
    }
}

const fn kp(plain: &'static str, app: char) -> Base {
    Base::Keypad { plain, app }
}

/// tmux `key_string_table`, without its mouse and internal entries.
const TABLE: &[(&str, Base)] = &[
    ("F1", csi("\x1bOP", "1", 'P')),
    ("F2", csi("\x1bOQ", "1", 'Q')),
    ("F3", csi("\x1bOR", "1", 'R')),
    ("F4", csi("\x1bOS", "1", 'S')),
    ("F5", csi("\x1b[15~", "15", '~')),
    ("F6", csi("\x1b[17~", "17", '~')),
    ("F7", csi("\x1b[18~", "18", '~')),
    ("F8", csi("\x1b[19~", "19", '~')),
    ("F9", csi("\x1b[20~", "20", '~')),
    ("F10", csi("\x1b[21~", "21", '~')),
    ("F11", csi("\x1b[23~", "23", '~')),
    ("F12", csi("\x1b[24~", "24", '~')),
    ("IC", csi("\x1b[2~", "2", '~')),
    ("Insert", csi("\x1b[2~", "2", '~')),
    ("DC", csi("\x1b[3~", "3", '~')),
    ("Delete", csi("\x1b[3~", "3", '~')),
    ("Home", csi("\x1b[1~", "1", 'H')),
    ("End", csi("\x1b[4~", "1", 'F')),
    ("NPage", csi("\x1b[6~", "6", '~')),
    ("PageDown", csi("\x1b[6~", "6", '~')),
    ("PgDn", csi("\x1b[6~", "6", '~')),
    ("PPage", csi("\x1b[5~", "5", '~')),
    ("PageUp", csi("\x1b[5~", "5", '~')),
    ("PgUp", csi("\x1b[5~", "5", '~')),
    ("BTab", Base::BTab),
    ("Space", Base::Char(' ')),
    ("BSpace", Base::BSpace),
    ("[NUL]", Base::Char('\x00')),
    ("[SOH]", Base::Char('\x01')),
    ("[STX]", Base::Char('\x02')),
    ("[ETX]", Base::Char('\x03')),
    ("[EOT]", Base::Char('\x04')),
    ("[ENQ]", Base::Char('\x05')),
    ("[ASC]", Base::Char('\x06')),
    ("[BEL]", Base::Char('\x07')),
    ("[BS]", Base::Char('\x08')),
    ("Tab", Base::Char('\t')),
    ("[LF]", Base::Char('\n')),
    ("[VT]", Base::Char('\x0b')),
    ("[FF]", Base::Char('\x0c')),
    ("Enter", Base::Char('\r')),
    ("[SO]", Base::Char('\x0e')),
    ("[SI]", Base::Char('\x0f')),
    ("[DLE]", Base::Char('\x10')),
    ("[DC1]", Base::Char('\x11')),
    ("[DC2]", Base::Char('\x12')),
    ("[DC3]", Base::Char('\x13')),
    ("[DC4]", Base::Char('\x14')),
    ("[NAK]", Base::Char('\x15')),
    ("[SYN]", Base::Char('\x16')),
    ("[ETB]", Base::Char('\x17')),
    ("[CAN]", Base::Char('\x18')),
    ("[EM]", Base::Char('\x19')),
    ("[SUB]", Base::Char('\x1a')),
    ("Escape", Base::Char('\x1b')),
    ("[FS]", Base::Char('\x1c')),
    ("[GS]", Base::Char('\x1d')),
    ("[RS]", Base::Char('\x1e')),
    ("[US]", Base::Char('\x1f')),
    ("Up", arrow("\x1b[A", "\x1bOA", 'A')),
    ("Down", arrow("\x1b[B", "\x1bOB", 'B')),
    ("Right", arrow("\x1b[C", "\x1bOC", 'C')),
    ("Left", arrow("\x1b[D", "\x1bOD", 'D')),
    ("KP/", kp("/", 'o')),
    ("KP*", kp("*", 'j')),
    ("KP-", kp("-", 'm')),
    ("KP7", kp("7", 'w')),
    ("KP8", kp("8", 'x')),
    ("KP9", kp("9", 'y')),
    ("KP+", kp("+", 'k')),
    ("KP4", kp("4", 't')),
    ("KP5", kp("5", 'u')),
    ("KP6", kp("6", 'v')),
    ("KP1", kp("1", 'q')),
    ("KP2", kp("2", 'r')),
    ("KP3", kp("3", 's')),
    ("KPEnter", kp("\n", 'M')),
    ("KP0", kp("0", 'p')),
    ("KP.", kp(".", 'n')),
];

/// Parses one tmux key name.
pub fn parse(name: &str) -> Result<Key, String> {
    let unknown = || {
        format!(
            "Unknown key {name:?}; use a tmux key name such as Enter, C-c, Up, Escape, F5, or one character with optional C-/M-/S- prefixes, and put literal text in input."
        )
    };
    let mut key = Key {
        base: Base::Char('\0'),
        ctrl: false,
        meta: false,
        shift: false,
    };
    if let Some(hex) = name.strip_prefix("0x") {
        let code = u32::from_str_radix(hex, 16).map_err(|_| unknown())?;
        key.base = Base::Char(char::from_u32(code).ok_or_else(unknown)?);
        return Ok(key);
    }
    let mut rest = name;
    if let Some(after) = rest.strip_prefix('^')
        && !after.is_empty()
    {
        let mut chars = after.chars();
        if let (Some(c), None) = (chars.next(), chars.clone().next())
            && c.is_ascii()
        {
            key.base = Base::Char(c.to_ascii_lowercase());
            key.ctrl = true;
            return Ok(key);
        }
        key.ctrl = true;
        rest = after;
    }
    while rest.len() >= 2 && rest.as_bytes()[1] == b'-' {
        match rest.as_bytes()[0].to_ascii_uppercase() {
            b'C' => key.ctrl = true,
            b'M' => key.meta = true,
            b'S' => key.shift = true,
            _ => return Err(unknown()),
        }
        rest = &rest[2..];
    }
    let mut chars = rest.chars();
    key.base = match (chars.next(), chars.next()) {
        (None, _) => return Err(unknown()),
        (Some(c), None) if c.is_ascii() && (c as u32) < 0x20 => return Err(unknown()),
        (Some(c), None) => Base::Char(c),
        _ => TABLE
            .iter()
            .find(|(entry, _)| entry.eq_ignore_ascii_case(rest))
            .map(|(_, base)| *base)
            .ok_or_else(unknown)?,
    };
    Ok(key)
}

/// Encodes a parsed key for a program in `modes`.
pub fn encode(name: &str, key: Key, modes: Modes) -> Result<Vec<u8>, String> {
    let lost = |modifier: &str| {
        Err(format!(
            "Key {name:?}: a terminal cannot send {modifier} with this key; drop the modifier or send a different key."
        ))
    };
    let mut bytes = Vec::new();
    match key.base {
        Base::Csi {
            plain,
            app,
            param,
            last,
        } => {
            let m = 1 + u8::from(key.shift) + 2 * u8::from(key.meta) + 4 * u8::from(key.ctrl);
            if m > 1 {
                bytes.extend_from_slice(format!("\x1b[{param};{m}{last}").as_bytes());
            } else {
                let sequence = app.filter(|_| modes.cursor).unwrap_or(plain);
                bytes.extend_from_slice(sequence.as_bytes());
            }
        }
        Base::Keypad { plain, app } => {
            if key.ctrl || key.shift {
                return lost("Ctrl or Shift");
            }
            if key.meta {
                bytes.push(0x1b);
            }
            if modes.keypad {
                bytes.extend_from_slice(&[0x1b, b'O', app as u8]);
            } else {
                bytes.extend_from_slice(plain.as_bytes());
            }
        }
        Base::BTab => {
            if key.ctrl || key.meta || key.shift {
                return lost("a modifier");
            }
            bytes.extend_from_slice(b"\x1b[Z");
        }
        Base::BSpace => {
            if key.ctrl || key.shift {
                return lost("Ctrl or Shift");
            }
            if key.meta {
                bytes.push(0x1b);
            }
            bytes.push(0x7f);
        }
        Base::Char(c) => {
            if key.shift {
                return lost(
                    "Shift; send the shifted character itself, such as A, or BTab for S-Tab",
                );
            }
            let mut c = c;
            if key.ctrl {
                c = match ctrl(c) {
                    Some(c) => c,
                    None => return lost("Ctrl"),
                };
            }
            if key.meta {
                bytes.push(0x1b);
            }
            let mut buffer = [0_u8; 4];
            bytes.extend_from_slice(c.encode_utf8(&mut buffer).as_bytes());
        }
    }
    Ok(bytes)
}

/// The byte a terminal sends for Ctrl plus `c` (tmux `input_key_vt10x`).
fn ctrl(c: char) -> Option<char> {
    const FROM: &[u8] = b"1!9(0)=+;:'\",<.>/-8? 2";
    const TO: &[u8] = b"119900=+;;'',,..\x1f\x1f\x7f\x7f\0\0";
    let byte = u8::try_from(c).ok().filter(u8::is_ascii)?;
    if let Some(index) = FROM.iter().position(|&from| from == byte) {
        return Some(TO[index] as char);
    }
    match byte {
        b'3'..=b'7' => Some((byte - 0x18) as char),
        b'@'..=b'~' => Some((byte & 0x1f) as char),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn send(name: &str, modes: Modes) -> Result<Vec<u8>, String> {
        encode(name, parse(name)?, modes)
    }

    /// Expected bytes come from tmux `input-keys.c` (standard mode) and `key-string.c`.
    #[test]
    fn key_names_send_the_bytes_tmux_sends() {
        let normal = Modes::default();
        let app = Modes {
            cursor: true,
            keypad: true,
        };
        for (name, modes, expected) in [
            ("Enter", normal, &b"\r"[..]),
            ("enter", normal, b"\r"),
            ("Tab", normal, b"\t"),
            ("Escape", normal, b"\x1b"),
            ("Space", normal, b" "),
            ("BSpace", normal, b"\x7f"),
            ("BTab", normal, b"\x1b[Z"),
            ("[ETX]", normal, b"\x03"),
            ("a", normal, b"a"),
            ("é", normal, "é".as_bytes()),
            ("0x41", normal, b"A"),
            ("0x03", normal, b"\x03"),
            ("0xe9", normal, "é".as_bytes()),
            ("C-c", normal, b"\x03"),
            ("c-C", normal, b"\x03"),
            ("^C", normal, b"\x03"),
            ("^[", normal, b"\x1b"),
            ("C-Space", normal, b"\x00"),
            ("C-@", normal, b"\x00"),
            ("C-2", normal, b"\x00"),
            ("C-3", normal, b"\x1b"),
            ("C-7", normal, b"\x1f"),
            ("C-/", normal, b"\x1f"),
            ("C-?", normal, b"\x7f"),
            ("C-1", normal, b"1"),
            ("M-x", normal, b"\x1bx"),
            ("C-M-a", normal, b"\x1b\x01"),
            ("M-Enter", normal, b"\x1b\r"),
            ("M-BSpace", normal, b"\x1b\x7f"),
            ("M-é", normal, "\x1bé".as_bytes()),
            ("Up", normal, b"\x1b[A"),
            ("Up", app, b"\x1bOA"),
            ("left", app, b"\x1bOD"),
            ("S-Up", normal, b"\x1b[1;2A"),
            ("M-Up", app, b"\x1b[1;3A"),
            ("C-Right", normal, b"\x1b[1;5C"),
            ("C-M-S-Down", normal, b"\x1b[1;8B"),
            ("Home", normal, b"\x1b[1~"),
            ("Home", app, b"\x1b[1~"),
            ("End", normal, b"\x1b[4~"),
            ("C-Home", normal, b"\x1b[1;5H"),
            ("S-End", normal, b"\x1b[1;2F"),
            ("PgUp", normal, b"\x1b[5~"),
            ("NPage", normal, b"\x1b[6~"),
            ("M-PageDown", normal, b"\x1b[6;3~"),
            ("Insert", normal, b"\x1b[2~"),
            ("DC", normal, b"\x1b[3~"),
            ("C-Delete", normal, b"\x1b[3;5~"),
            ("F1", normal, b"\x1bOP"),
            ("S-F1", normal, b"\x1b[1;2P"),
            ("F5", normal, b"\x1b[15~"),
            ("F11", normal, b"\x1b[23~"),
            ("C-F12", normal, b"\x1b[24;5~"),
            ("KP5", normal, b"5"),
            ("KP5", app, b"\x1bOu"),
            ("KPEnter", normal, b"\n"),
            ("KPEnter", app, b"\x1bOM"),
            ("M-KP.", app, b"\x1b\x1bOn"),
            ("C--", normal, b"\x1f"),
            ("-", normal, b"-"),
        ] {
            assert_eq!(
                send(name, modes).as_deref(),
                Ok(expected),
                "{name} (cursor={}, keypad={})",
                modes.cursor,
                modes.keypad
            );
        }
    }

    #[test]
    fn unknown_names_and_unsendable_modifiers_are_errors() {
        for name in [
            "",
            "Enterr",
            "X-a",
            "C-",
            "ab",
            "\t",
            "MouseDown1",
            "None",
            "0xzz",
            "C-0x41",
            "C-Enter",
            "C-Tab",
            "S-Enter",
            "S-a",
            "C-é",
            "C-Escape",
            "C-BSpace",
            "M-BTab",
            "C-KP5",
        ] {
            assert!(
                send(name, Modes::default()).is_err(),
                "{name:?} should be rejected"
            );
        }
    }
}
