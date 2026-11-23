//! Key names to terminal input bytes.
//!
//! Names follow tmux `key-string.c` (`key_string_lookup_string`). A key is a base key and
//! modifiers: an uppercase or shifted character is Shift plus its base on a US layout (`A` is
//! S-a, `!` is S-1), except that Ctrl with an uppercase letter and no explicit S- is Ctrl with
//! the letter (`C-C` is `^C`), and a C0 control is Ctrl plus its letter, as in tmux (`[ETX]` is
//! C-c), apart from Tab, Enter, and Escape.
//!
//! Bytes follow the program's keyboard mode. By default they follow tmux `input-keys.c` in its
//! standard (non-extended) key mode; tmux silently drops a modifier that has no legacy
//! encoding (C-Enter, C-S-a, C-é), and here such a key is an error instead. When the program
//! enables the kitty keyboard protocol (<https://sw.kovidgoyal.net/kitty/keyboard-protocol/>),
//! bytes follow it as Windows Terminal `terminalInput.cpp` encodes it, for a US layout with no
//! lock key on. A key is pressed and, when the program asks for event types, released.

/// The program's keyboard modes, read from the terminal emulator.
#[derive(Clone, Copy, Default)]
pub struct Modes {
    /// DECCKM: arrow keys send `ESC O x`.
    pub cursor: bool,
    /// DECKPAM: keypad keys send `ESC O x`.
    pub keypad: bool,
    /// Kitty keyboard protocol enhancement flags; 0 when the protocol is off.
    pub kitty: u8,
}

const DISAMBIGUATE: u8 = 1;
const EVENT_TYPES: u8 = 2;
const ALTERNATE_KEYS: u8 = 4;
const ALL_KEYS: u8 = 8;
const ASSOCIATED_TEXT: u8 = 16;

/// Kitty modifier bits.
const SHIFT: u32 = 1;

/// Kitty key codes of KP_0, the lowest keypad key, and KP_ENTER.
const KP_0: u32 = 57399;
const KP_ENTER: u32 = 57414;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Base {
    /// A character key without Shift applied; also Tab `\t`, Enter `\r`, and Escape `\x1b`.
    Char(char),
    BSpace,
    /// A key with a CSI form: unmodified bytes, application-cursor bytes, and the modified
    /// form `ESC [ <param> ; <m> <last>`.
    Csi {
        plain: &'static str,
        app: Option<&'static str>,
        param: &'static str,
        last: char,
    },
    /// A keypad key: its normal-mode bytes, the final byte of its `ESC O x` form, and its
    /// kitty key code.
    Keypad {
        plain: &'static str,
        app: char,
        code: u32,
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

const fn kp(plain: &'static str, app: char, code: u32) -> Base {
    Base::Keypad { plain, app, code }
}

/// tmux `key_string_table`, without its mouse and internal entries. BTab is S-Tab, and the C0
/// names (`[ETX]`) are their characters, which parsing turns into keys.
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
    ("KP/", kp("/", 'o', 57410)),
    ("KP*", kp("*", 'j', 57411)),
    ("KP-", kp("-", 'm', 57412)),
    ("KP7", kp("7", 'w', 57406)),
    ("KP8", kp("8", 'x', 57407)),
    ("KP9", kp("9", 'y', 57408)),
    ("KP+", kp("+", 'k', 57413)),
    ("KP4", kp("4", 't', 57403)),
    ("KP5", kp("5", 'u', 57404)),
    ("KP6", kp("6", 'v', 57405)),
    ("KP1", kp("1", 'q', 57400)),
    ("KP2", kp("2", 'r', 57401)),
    ("KP3", kp("3", 's', 57402)),
    ("KPEnter", kp("\n", 'M', KP_ENTER)),
    ("KP0", kp("0", 'p', KP_0)),
    ("KP.", kp(".", 'n', 57409)),
];

/// The US layout's shifted characters, after their base keys.
const US_SHIFT: &[(char, char)] = &[
    ('`', '~'),
    ('1', '!'),
    ('2', '@'),
    ('3', '#'),
    ('4', '$'),
    ('5', '%'),
    ('6', '^'),
    ('7', '&'),
    ('8', '*'),
    ('9', '('),
    ('0', ')'),
    ('-', '_'),
    ('=', '+'),
    ('[', '{'),
    (']', '}'),
    ('\\', '|'),
    (';', ':'),
    ('\'', '"'),
    (',', '<'),
    ('.', '>'),
    ('/', '?'),
];

/// The character Shift turns `base` into, if any.
fn shifted(base: char) -> Option<char> {
    if let Some(&(_, shifted)) = US_SHIFT.iter().find(|(from, _)| *from == base) {
        return Some(shifted);
    }
    let mut upper = base.to_uppercase();
    match (upper.next(), upper.next()) {
        (Some(upper), None) if upper != base => Some(upper),
        _ => None,
    }
}

/// The base key Shift turns into `c`, if `c` is shifted.
fn unshifted(c: char) -> Option<char> {
    if let Some(&(base, _)) = US_SHIFT.iter().find(|(_, to)| *to == c) {
        return Some(base);
    }
    let mut lower = c.to_lowercase();
    match (lower.next(), lower.next()) {
        (Some(lower), None) if lower != c && shifted(lower) == Some(c) => Some(lower),
        _ => None,
    }
}

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
        return Ok(normalize(key, false));
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
            return Ok(normalize(key, false));
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
    let explicit_shift = key.shift;
    let mut chars = rest.chars();
    key.base = match (chars.next(), chars.next()) {
        (None, _) => return Err(unknown()),
        (Some(c), None) if c.is_ascii() && (c as u32) < 0x20 => return Err(unknown()),
        (Some(c), None) => Base::Char(c),
        _ if rest.eq_ignore_ascii_case("BTab") => {
            key.shift = true;
            Base::Char('\t')
        }
        _ => TABLE
            .iter()
            .find(|(entry, _)| entry.eq_ignore_ascii_case(rest))
            .map(|(_, base)| *base)
            .ok_or_else(unknown)?,
    };
    Ok(normalize(key, explicit_shift))
}

/// Turns a character into its base key and modifiers.
fn normalize(mut key: Key, explicit_shift: bool) -> Key {
    let Base::Char(c) = key.base else {
        return key;
    };
    match c {
        '\t' | '\r' | '\x1b' => {}
        '\x7f' => key.base = Base::BSpace,
        '\0' => {
            key.base = Base::Char(' ');
            key.ctrl = true;
        }
        '\x01'..='\x1f' => {
            key.ctrl = true;
            return normalize(
                Key {
                    base: Base::Char((c as u8 | 0x40).to_ascii_lowercase() as char),
                    ..key
                },
                explicit_shift,
            );
        }
        _ if key.ctrl && !explicit_shift && c.is_ascii_uppercase() => {
            key.base = Base::Char(c.to_ascii_lowercase());
        }
        _ => {
            if let Some(base) = unshifted(c) {
                key.base = Base::Char(base);
                key.shift = true;
            }
        }
    }
    key
}

/// Encodes a parsed key for a program in `modes`.
pub fn encode(name: &str, key: Key, modes: Modes) -> Result<Vec<u8>, String> {
    if modes.kitty & (DISAMBIGUATE | EVENT_TYPES | ALL_KEYS) == 0 {
        return legacy(name, key, modes);
    }
    let mut bytes = kitty(key, modes, false);
    if modes.kitty & EVENT_TYPES != 0 {
        bytes.extend(kitty(key, modes, true));
    }
    Ok(bytes)
}

fn lost(name: &str, modifier: &str) -> String {
    format!(
        "Key {name:?}: the program's keyboard mode cannot carry {modifier} with this key (the kitty keyboard protocol can, once the program enables it); drop the modifier or send a different key."
    )
}

/// The bytes of a key in the legacy (tmux standard) encoding.
fn legacy(name: &str, key: Key, modes: Modes) -> Result<Vec<u8>, String> {
    let lost = |modifier: &str| Err(lost(name, modifier));
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
        Base::Keypad { plain, app, .. } => {
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
        Base::Char('\t') if key.shift => {
            if key.ctrl || key.meta {
                return lost("a modifier with Shift");
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
        Base::Char(base) => {
            let mut c = base;
            if key.shift {
                // Ctrl with a letter sends the same byte whether Shift is held or not.
                match shifted(base) {
                    Some(upper) if !(key.ctrl && base.is_alphabetic()) => c = upper,
                    _ => return lost("Shift"),
                }
            }
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

/// The character a character key types: its base, shifted when Shift changes it.
fn typed(base: char, shift: bool) -> char {
    shifted(base).filter(|_| shift).unwrap_or(base)
}

/// The bytes of a key press or release under the kitty keyboard protocol, following Windows
/// Terminal `TerminalInput::_encodeKitty`.
fn kitty(key: Key, modes: Modes, release: bool) -> Vec<u8> {
    let flags = modes.kitty;
    let mods = u32::from(key.shift) | u32::from(key.meta) << 1 | u32::from(key.ctrl) << 2;
    let (functional, text_key) = match key.base {
        Base::Char('\x1b') => (Some(27), None),
        Base::Char('\r') => (Some(13), None),
        Base::Char('\t') => (Some(9), None),
        Base::BSpace => (Some(127), None),
        Base::Keypad { code, .. } => (Some(code), None),
        Base::Char(c) => (None, Some(c)),
        Base::Csi { .. } => (None, None),
    };
    // Enter, Tab, and Backspace report a release only when every key is an escape code.
    if release && flags & ALL_KEYS == 0 && matches!(functional, Some(9 | 13 | 127)) {
        return Vec::new();
    }
    let mut code = None;
    if flags & DISAMBIGUATE != 0
        && let Some(functional) = functional
        && (functional == 27 || (functional <= 127 && mods != 0) || functional >= KP_0)
    {
        code = Some(functional);
    }
    let mut text = None;
    if flags & ALL_KEYS != 0
        || (flags & DISAMBIGUATE != 0 && text_key.is_some() && mods > SHIFT)
        || release
    {
        code = functional.or(text_key.map(u32::from));
        if flags & ASSOCIATED_TEXT != 0 && !release && !key.ctrl {
            text = match key.base {
                Base::Char(c) => Some(typed(c, key.shift)),
                Base::Keypad { plain, .. } => plain.chars().next(),
                _ => None,
            }
            .filter(|c| !c.is_control());
        }
    }
    if let Some(code) = code {
        let alternate = (flags & ALTERNATE_KEYS != 0 && key.shift)
            .then(|| text_key.and_then(shifted))
            .flatten();
        return csi_sequence(code, alternate, mods, release, text, 'u');
    }
    // The keys left keep their legacy forms as Windows Terminal `_encodeRegular` and
    // `_formatFallback` encode them under the protocol. Only a press is left for every key
    // but the CSI ones: every other release has its code.
    let mut bytes = Vec::new();
    let alt_prefix = |bytes: &mut Vec<u8>| {
        if key.meta {
            bytes.push(0x1b);
        }
    };
    match key.base {
        // F3 is `CSI 13 ~`; the arrows, Home, and End keep their CSI forms in either cursor
        // mode.
        Base::Csi { param, last, .. } => {
            return match (param, last) {
                (_, 'R') => csi_sequence(13, None, mods, release, None, '~'),
                (param, '~') => {
                    csi_sequence(param.parse().unwrap_or(1), None, mods, release, None, '~')
                }
                (_, last) => csi_sequence(0, None, mods, release, None, last),
            };
        }
        Base::Char('\t') => {
            alt_prefix(&mut bytes);
            bytes.extend_from_slice(if key.shift { b"\x1b[Z" } else { b"\t" });
        }
        Base::BSpace => {
            alt_prefix(&mut bytes);
            bytes.push(if key.ctrl { 0x08 } else { 0x7f });
        }
        // Keypad keys in application keypad mode, with Alt only on KPEnter.
        Base::Keypad { app, code, .. } if modes.keypad => {
            if code == KP_ENTER {
                alt_prefix(&mut bytes);
            }
            bytes.extend_from_slice(&[0x1b, b'O', app as u8]);
        }
        Base::Char('\r') | Base::Keypad { code: KP_ENTER, .. } => {
            alt_prefix(&mut bytes);
            bytes.push(if key.ctrl { b'\n' } else { b'\r' });
        }
        Base::Keypad { plain, .. } => {
            let c = plain.chars().next().unwrap_or(' ');
            fallback(&mut bytes, key, c, None);
        }
        Base::Char(c) => fallback(&mut bytes, key, typed(c, key.shift), Some(c)),
    }
    bytes
}

/// Windows Terminal `_formatFallback`: the typed character, Ctrl applied by `_makeCtrlChar`
/// and, when that leaves a printable character, applied again to the virtual key of a digit
/// 2–9 or letter `base`; after `ESC` for Alt.
fn fallback(bytes: &mut Vec<u8>, key: Key, mut c: char, base: Option<char>) {
    if key.ctrl {
        c = make_ctrl(c);
        if let Some(base) = base.filter(|base| matches!(base, '2'..='9' | 'a'..='z'))
            && c >= ' '
        {
            c = make_ctrl(base.to_ascii_uppercase());
        }
    }
    if key.meta {
        bytes.push(0x1b);
    }
    let mut buffer = [0_u8; 4];
    bytes.extend_from_slice(c.encode_utf8(&mut buffer).as_bytes());
}

/// Windows Terminal `_makeCtrlChar`.
fn make_ctrl(c: char) -> char {
    match c {
        '@'..='~' => char::from(c as u8 & 0x1f),
        ' ' => '\0',
        '/' => '\x1f',
        '?' => '\x7f',
        '2'..='8' => char::from([0, 27, 28, 29, 30, 31, 127][c as usize - '2' as usize]),
        c => c,
    }
}

/// `CSI code[:alternate] [; modifiers[:event]] [; text] final`, as Windows Terminal
/// formats it; a code of 0 is omitted.
fn csi_sequence(
    code: u32,
    alternate: Option<char>,
    mods: u32,
    release: bool,
    text: Option<char>,
    last: char,
) -> Vec<u8> {
    let modified = mods != 0 || release;
    let code = if modified { code.max(1) } else { code };
    let mut sequence = String::from("\x1b[");
    if code != 0 {
        sequence += &code.to_string();
    }
    if let Some(alternate) = alternate {
        sequence += &format!(":{}", u32::from(alternate));
    }
    if modified || text.is_some() {
        sequence.push(';');
        if modified {
            sequence += &(mods + 1).to_string();
            if release {
                sequence += ":3";
            }
        }
        if let Some(text) = text {
            sequence += &format!(";{}", u32::from(text));
        }
    }
    sequence.push(last);
    sequence.into_bytes()
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
            kitty: 0,
        };
        for (name, modes, expected) in [
            ("Enter", normal, &b"\r"[..]),
            ("enter", normal, b"\r"),
            ("Tab", normal, b"\t"),
            ("Escape", normal, b"\x1b"),
            ("Space", normal, b" "),
            ("BSpace", normal, b"\x7f"),
            ("BTab", normal, b"\x1b[Z"),
            ("S-Tab", normal, b"\x1b[Z"),
            ("[ETX]", normal, b"\x03"),
            ("[BS]", normal, b"\x08"),
            ("[NUL]", normal, b"\x00"),
            ("a", normal, b"a"),
            ("A", normal, b"A"),
            ("S-a", normal, b"A"),
            ("S-1", normal, b"!"),
            ("M-A", normal, b"\x1bA"),
            ("é", normal, "é".as_bytes()),
            ("S-é", normal, "É".as_bytes()),
            ("0x41", normal, b"A"),
            ("0x03", normal, b"\x03"),
            ("0x7f", normal, b"\x7f"),
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
            ("C-^", normal, b"\x1e"),
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
            "S-Space",
            "C-S-a",
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

    /// Expected bytes come from Windows Terminal's `kittyKeyboardProtocol.cpp` cases and
    /// kitty's `kitty_tests/keys.py`, with a US layout where those use another one.
    #[test]
    fn keys_follow_the_kitty_keyboard_protocol_the_program_enables() {
        let (d, e, a, k, t) = (
            DISAMBIGUATE,
            EVENT_TYPES,
            ALTERNATE_KEYS,
            ALL_KEYS,
            ASSOCIATED_TEXT,
        );
        for (name, flags, expected) in [
            // Disambiguation: Escape and modified text keys become CSI u.
            ("a", d, "a"),
            ("Escape", d, "\x1b[27u"),
            ("C-a", d, "\x1b[97;5u"),
            ("M-S-a", d, "\x1b[97;4u"),
            ("A", d, "A"),
            ("C-i", d, "\x1b[105;5u"),
            ("Tab", d, "\t"),
            ("C-m", d, "\x1b[109;5u"),
            ("Enter", d, "\r"),
            ("C-[", d, "\x1b[91;5u"),
            ("[ETX]", d, "\x1b[99;5u"),
            ("C-Space", d, "\x1b[32;5u"),
            ("S-Enter", d, "\x1b[13;2u"),
            ("C-Enter", d, "\x1b[13;5u"),
            ("BTab", d, "\x1b[9;2u"),
            ("BSpace", d, "\x7f"),
            ("M-BSpace", d, "\x1b[127;3u"),
            ("KP0", d, "\x1b[57399u"),
            ("KP5", d, "\x1b[57404u"),
            ("S-KP5", d, "\x1b[57404;2u"),
            ("KP.", d, "\x1b[57409u"),
            ("KP/", d, "\x1b[57410u"),
            ("Up", d, "\x1b[A"),
            ("C-Up", d, "\x1b[1;5A"),
            ("F1", d, "\x1b[P"),
            ("F3", d, "\x1b[13~"),
            ("S-F1", d, "\x1b[1;2P"),
            ("F5", d, "\x1b[15~"),
            ("C-Home", d, "\x1b[1;5H"),
            ("End", d, "\x1b[F"),
            // Every key as an escape code.
            ("a", k, "\x1b[97u"),
            ("A", k, "\x1b[97;2u"),
            ("M-a", k, "\x1b[97;3u"),
            ("C-S-a", k, "\x1b[97;6u"),
            ("!", k, "\x1b[49;2u"),
            ("Enter", k, "\x1b[13u"),
            ("Tab", k, "\x1b[9u"),
            ("BSpace", k, "\x1b[127u"),
            ("C-Tab", k, "\x1b[9;5u"),
            ("M-S-C-Escape", k, "\x1b[27;8u"),
            ("F12", k, "\x1b[24~"),
            ("S-F5", k, "\x1b[15;2~"),
            ("Up", k, "\x1b[A"),
            ("S-Up", k, "\x1b[1;2A"),
            ("Insert", k, "\x1b[2~"),
            // Event types: each key is pressed, then released.
            ("Escape", d | e, "\x1b[27u\x1b[27;1:3u"),
            ("S-Escape", d | e, "\x1b[27;2u\x1b[27;2:3u"),
            ("Enter", d | e, "\r"),
            ("BSpace", d | e, "\x7f"),
            ("KPEnter", d | e, "\x1b[57414u\x1b[57414;1:3u"),
            ("a", e, "a\x1b[97;1:3u"),
            ("a", e | k, "\x1b[97u\x1b[97;1:3u"),
            ("Enter", e | k, "\x1b[13u\x1b[13;1:3u"),
            ("F1", e | k, "\x1b[P\x1b[1;1:3P"),
            ("F5", e | k, "\x1b[15~\x1b[15;1:3~"),
            ("Up", e | k, "\x1b[A\x1b[1;1:3A"),
            ("Insert", e | k, "\x1b[2~\x1b[2;1:3~"),
            // Alternate keys and associated text.
            ("A", a | k, "\x1b[97:65;2u"),
            ("C-S-a", a | k, "\x1b[97:65;6u"),
            ("S-1", a | k, "\x1b[49:33;2u"),
            ("a", a | k, "\x1b[97u"),
            ("A", k | t, "\x1b[97;2;65u"),
            ("!", k | t, "\x1b[49;2;33u"),
            ("C-a", k | t, "\x1b[97;5u"),
            ("a", k | t, "\x1b[97;;97u"),
            ("a", e | k | t, "\x1b[97;;97u\x1b[97;1:3u"),
            ("Escape", k | t, "\x1b[27u"),
            ("A", a | k | t, "\x1b[97:65;2;65u"),
            ("S-Space", k | t, "\x1b[32;2;32u"),
            // Presses the protocol leaves in legacy form use Windows Terminal's legacy bytes.
            ("S-Space", d, " "),
            ("C-Enter", e, "\n"),
            ("KPEnter", e, "\r\x1b[57414;1:3u"),
            ("C--", e, "-\x1b[45;5:3u"),
            ("C-/", e, "\x1f\x1b[47;5:3u"),
            ("M-Escape", e, "\x1b\x1b\x1b[27;3:3u"),
            ("C-S-3", e, "\x1b\x1b[51;6:3u"),
            ("C-S-9", e, "9\x1b[57;6:3u"),
        ] {
            let modes = Modes {
                kitty: flags,
                ..Modes::default()
            };
            assert_eq!(
                send(name, modes).as_deref(),
                Ok(expected.as_bytes()),
                "{name} (flags={flags})"
            );
        }
        // The protocol's arrows do not depend on the cursor mode; its keypad keys do.
        let app = Modes {
            cursor: true,
            keypad: true,
            kitty: e,
        };
        assert_eq!(send("Up", app).as_deref(), Ok(&b"\x1b[A\x1b[1;1:3A"[..]));
        assert_eq!(
            send("M-KPEnter", app).as_deref(),
            Ok(&b"\x1b\x1bOM\x1b[57414;3:3u"[..])
        );
    }
}
