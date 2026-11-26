//! UTF-16 command lines and environment blocks for native Windows launches.
//!
//! Command-line quoting follows portable-pty 0.9's Windows implementation
//! (https://github.com/wez/wezterm, `src/cmdbuilder.rs`), Copyright 2018 Wez Furlong,
//! licensed under the MIT license in `vendor/conpty/LICENSE`; see NOTICE.

use std::cmp::Ordering;
use std::ffi::{OsStr, OsString};
use std::os::windows::ffi::OsStrExt;
use windows_sys::Win32::Globalization::{
    CSTR_EQUAL, CSTR_GREATER_THAN, CSTR_LESS_THAN, CompareStringOrdinal,
};

pub(crate) fn command_line_of(program: &OsStr, args: &[OsString]) -> std::io::Result<Vec<u16>> {
    let mut line = Vec::new();
    append_quoted(program, &mut line)?;
    for arg in args {
        line.push(' ' as u16);
        append_quoted(arg, &mut line)?;
    }
    line.push(0);
    Ok(line)
}

/// Appends one argument quoted by the rules the Microsoft C runtime parses: a run of backslashes
/// is literal before an ordinary unit, doubled before a quote, and doubled before the closing
/// quote; a quote itself is escaped by one more backslash.
fn append_quoted(arg: &OsStr, line: &mut Vec<u16>) -> std::io::Result<()> {
    const BACKSLASH: u16 = b'\\' as u16;
    const QUOTE: u16 = b'"' as u16;
    let arg = units(arg)?;
    let plain = !arg.is_empty()
        && !arg
            .iter()
            .any(|&unit| matches!(unit, 32 | 9 | 10 | 11) || unit == QUOTE);
    if plain {
        line.extend_from_slice(&arg);
        return Ok(());
    }
    line.push(QUOTE);
    let mut index = 0;
    while index < arg.len() {
        let run = arg[index..]
            .iter()
            .take_while(|&&unit| unit == BACKSLASH)
            .count();
        index += run;
        if index == arg.len() {
            // Trailing backslashes sit before the closing quote, so they are doubled.
            line.extend(std::iter::repeat_n(BACKSLASH, run * 2));
            break;
        }
        let escape = arg[index] == QUOTE;
        let written = if escape { run * 2 + 1 } else { run };
        line.extend(std::iter::repeat_n(BACKSLASH, written));
        line.push(arg[index]);
        index += 1;
    }
    line.push(QUOTE);
    Ok(())
}

/// Builds the sorted environment block from `(name, value)` pairs. Windows compares names without
/// regard to case, but msys bash imports the spelling it is given into its own case-sensitive
/// environment, so a later pair of the same name replaces the whole entry, spelling included: a
/// forced `LANG` reaches bash as `LANG`, never as the caller's `lang`.
pub(super) fn block<'a>(
    pairs: impl Iterator<Item = (&'a OsStr, &'a OsStr)>,
) -> std::io::Result<Vec<u16>> {
    // (name, value)
    let mut entries: Vec<(Vec<u16>, Vec<u16>)> = Vec::new();
    for (name, value) in pairs {
        let name = units(name)?;
        check_name(&name)?;
        let value = units(value)?;
        match entries
            .iter_mut()
            .find(|(existing, _)| same_name(existing, &name))
        {
            Some(entry) => *entry = (name, value),
            None => entries.push((name, value)),
        }
    }
    entries.sort_by(|left, right| compare_names(&left.0, &right.0));
    let mut block = Vec::new();
    for (name, value) in entries {
        block.extend_from_slice(&name);
        block.push(b'=' as u16);
        block.extend_from_slice(&value);
        block.push(0);
    }
    // An empty block is two terminators, as a full one ends with one.
    if block.is_empty() {
        block.push(0);
    }
    block.push(0);
    Ok(block)
}

/// A name is `name=value`, so it cannot be empty and cannot hold `=` after the first unit. A
/// leading `=` is a name of its own, as in the per-drive current directory `=C:` other Windows
/// entries use, and `std`'s own environment reader keeps such a name.
fn check_name(name: &[u16]) -> std::io::Result<()> {
    // A lone `=` is no name at all; every other leading `=` starts one.
    let valid = !name.is_empty()
        && !name[1..].contains(&(b'=' as u16))
        && (name[0] != b'=' as u16 || name.len() > 1);
    if valid {
        return Ok(());
    }
    Err(std::io::Error::other(format!(
        "an environment name cannot be {:?}",
        String::from_utf16_lossy(name)
    )))
}

/// Whether two names are the same name, which Windows decides without regard to case.
fn same_name(left: &[u16], right: &[u16]) -> bool {
    matches!(compare(left, right), Some(Ordering::Equal))
}

/// The order `CreateProcessW` wants environment names in: ordinal, without regard to case, on the
/// UTF-16 units themselves, so an unpaired surrogate is a unit like any other. Units the system
/// calls equal then fall back to their exact order, which keeps the sort total.
fn compare_names(left: &[u16], right: &[u16]) -> Ordering {
    match compare(left, right) {
        Some(order) => order.then_with(|| left.cmp(right)),
        None => left.cmp(right),
    }
}

/// The system's ordinal comparison of two names, or `None` when it gives no answer.
fn compare(left: &[u16], right: &[u16]) -> Option<Ordering> {
    let (left_len, right_len) = (
        i32::try_from(left.len()).ok()?,
        i32::try_from(right.len()).ok()?,
    );
    // SAFETY: both slices are alive for the call and their lengths are given explicitly, so
    // neither needs a terminator.
    let result =
        unsafe { CompareStringOrdinal(left.as_ptr(), left_len, right.as_ptr(), right_len, 1) };
    match result {
        CSTR_LESS_THAN => Some(Ordering::Less),
        CSTR_GREATER_THAN => Some(Ordering::Greater),
        CSTR_EQUAL => Some(Ordering::Equal),
        // A failure is not an equality: exact order keeps distinct names apart.
        _ => None,
    }
}

/// A value's UTF-16 units. An unpaired surrogate survives, which is why the wide form is used.
fn units(value: &OsStr) -> std::io::Result<Vec<u16>> {
    let units: Vec<u16> = value.encode_wide().collect();
    if units.contains(&0) {
        return Err(std::io::Error::other(format!(
            "a NUL cannot appear in {value:?}"
        )));
    }
    Ok(units)
}

pub(super) fn nul_terminated(value: &OsStr) -> std::io::Result<Vec<u16>> {
    let mut units = units(value)?;
    units.push(0);
    Ok(units)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::os::windows::ffi::OsStringExt;
    use windows_sys::Win32::UI::Shell::CommandLineToArgvW;

    /// The arguments Windows' own splitter reads from a command line, the way a child's `argv`
    /// comes out of one. The first token is the program, which is dropped.
    fn argv(line: &[u16]) -> Vec<OsString> {
        let mut full = line.to_vec();
        full.push(0);
        let mut count = 0;
        // SAFETY: the line is NUL-terminated and outlives the call; the array is freed below.
        unsafe {
            let array = CommandLineToArgvW(full.as_ptr(), &mut count);
            assert!(!array.is_null(), "the command line cannot be parsed");
            let args = (0..count as usize)
                .skip(1)
                .map(|index| {
                    let entry = *array.add(index);
                    let mut length = 0;
                    while *entry.add(length) != 0 {
                        length += 1;
                    }
                    OsString::from_wide(std::slice::from_raw_parts(entry, length))
                })
                .collect();
            windows_sys::Win32::Foundation::LocalFree(array as *mut std::ffi::c_void);
            args
        }
    }

    /// Each argument reaches a child whole: the quoting the C runtime parses survives an empty
    /// argument, spaces, tabs, newlines, quotes, and backslashes that are ordinary, quoted, or
    /// trailing.
    #[test]
    fn every_argument_reaches_a_child_whole() {
        let cases = [
            "",
            "plain",
            "two words",
            "tab\there",
            "line\nbreak",
            "say \"hi\"",
            // Without quotes a backslash needs nothing; inside them an ordinary one stays one.
            "back\\slash",
            "a b\\c",
            "a\\b c\\d",
            // One backslash before a quote doubles, plus one: `\"` reads back as `"`.
            "a\\\"b",
            // Trailing backslashes sit before the closing quote, so each is doubled.
            "space then\\",
            "space then\\\\",
            "\\",
            "\\\\",
        ];
        for case in cases {
            let args: Vec<OsString> = vec![case.into()];
            let line = command_line_of(OsStr::new("bash"), &args).expect("a command line");
            // The trailing NUL is the terminator, not part of the line to parse.
            assert_eq!(argv(&line[..line.len() - 1]), args, "{case:?}");
        }
    }

    /// The same, for several arguments in one line, where a missing separator would show.
    #[test]
    fn several_arguments_keep_their_boundaries() {
        let args: Vec<OsString> = vec![
            "-c".into(),
            "printf '<%s>' \"$@\"".into(),
            "".into(),
            "two words".into(),
            "a\\\"b".into(),
            "c:\\work dir\\".into(),
        ];
        let line = command_line_of(OsStr::new("C:\\Program Files\\Git\\bin\\bash.exe"), &args)
            .expect("a command line");
        assert_eq!(argv(&line[..line.len() - 1]), args);
    }

    /// A name given twice, differing only in case, is one entry holding the later value.
    #[test]
    fn a_name_given_twice_in_different_case_is_one_entry() {
        let pairs = [
            (OsStr::new("Path"), OsStr::new("first")),
            (OsStr::new("PATH"), OsStr::new("second")),
            (OsStr::new("Path"), OsStr::new("third")),
        ];
        let block = block(pairs.iter().copied()).expect("a block");
        assert_eq!(block, expected(&["Path=third"]));
    }

    /// A forced value arrives under its own spelling: a caller's `lang` does not survive as the
    /// name bash would read, because Windows calls it the same name and msys bash is case-sensitive.
    #[test]
    fn a_forced_value_replaces_the_callers_spelling() {
        let pairs = [
            (OsStr::new("lang"), OsStr::new("caller")),
            (OsStr::new("LANG"), OsStr::new("C.UTF-8")),
        ];
        assert_eq!(
            block(pairs.iter().copied()).unwrap(),
            expected(&["LANG=C.UTF-8"])
        );

        // And the other order keeps the caller's spelling, which is what a caller's own value is.
        let pairs = [
            (OsStr::new("LANG"), OsStr::new("C.UTF-8")),
            (OsStr::new("lang"), OsStr::new("caller")),
        ];
        assert_eq!(
            block(pairs.iter().copied()).unwrap(),
            expected(&["lang=caller"])
        );
    }

    /// Names sort in ordinal ignore-case order, not in the order they were given.
    #[test]
    fn names_sort_without_regard_to_case() {
        let pairs = [
            (OsStr::new("beta"), OsStr::new("2")),
            (OsStr::new("Alpha"), OsStr::new("1")),
            (OsStr::new("Zulu"), OsStr::new("3")),
        ];
        let block = block(pairs.iter().copied()).expect("a block");
        assert_eq!(block, expected(&["Alpha=1", "beta=2", "Zulu=3"]));
    }

    /// Case folding is the system's: `é` and `É` are one name, and they sort where the folded
    /// `É` belongs, before `×`. A fold that only knew ASCII would place `é` after it.
    #[test]
    fn non_ascii_names_fold_together_and_sort_folded() {
        let pairs = [
            (OsStr::new("\u{e9}"), OsStr::new("lower")),
            (OsStr::new("\u{d7}"), OsStr::new("times")),
            (OsStr::new("\u{c9}"), OsStr::new("upper")),
        ];
        let block = block(pairs.iter().copied()).expect("a block");
        // One entry, holding the later value under the later spelling, sorted where the folded
        // name belongs.
        assert_eq!(block, expected(&["\u{c9}=upper", "\u{d7}=times"]));
    }

    /// A value that is not valid UTF-16 reaches the child unchanged.
    #[test]
    fn an_unpaired_surrogate_survives() {
        let value = OsString::from_wide(&[0xD800, b'x' as u16]);
        let pairs = [(OsStr::new("KEEP"), value.as_os_str())];
        let block = block(pairs.iter().copied()).expect("a block");
        assert_eq!(
            block,
            vec![
                b'K' as u16,
                b'E' as u16,
                b'E' as u16,
                b'P' as u16,
                b'=' as u16,
                0xD800,
                b'x' as u16,
                0,
                0
            ]
        );
    }

    /// No names means two terminators, an empty block and not a missing one.
    #[test]
    fn an_empty_environment_is_two_terminators() {
        let block = block(std::iter::empty()).expect("a block");
        assert_eq!(block, vec![0, 0]);
    }

    /// A value may hold `=`; a name that is empty or holds one past its first unit is refused
    /// rather than silently left out of the block.
    #[test]
    fn an_invalid_name_is_refused_and_a_value_may_hold_an_equals_sign() {
        let pairs = [(OsStr::new("EVEN"), OsStr::new("a=b=c"))];
        assert_eq!(
            block(pairs.iter().copied()).unwrap(),
            expected(&["EVEN=a=b=c"])
        );

        for name in ["o=dd", "a=b", "", "="] {
            let pairs = [(OsStr::new(name), OsStr::new("x"))];
            let error = block(pairs.iter().copied()).expect_err("the name is invalid");
            assert!(error.to_string().contains("environment name"), "{error}");
        }
    }

    /// The per-drive current directory `CreateProcessW` documents, `=C:`, is a name of its own.
    #[test]
    fn a_drive_current_directory_is_a_name() {
        let pairs = [
            (OsStr::new("=C:"), OsStr::new("C:\\work")),
            (OsStr::new("A"), OsStr::new("1")),
        ];
        let block = block(pairs.iter().copied()).expect("a block");
        assert_eq!(block, expected(&["=C:=C:\\work", "A=1"]));
    }

    /// The wide form of the `name=value` entries, then the block's terminator.
    fn expected(entries: &[&str]) -> Vec<u16> {
        let mut units = Vec::new();
        for entry in entries {
            units.extend(entry.encode_utf16());
            units.push(0);
        }
        units.push(0);
        units
    }
}
