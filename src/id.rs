//! Crockford base32 identifiers, random keys, and task reader handles.

/// Crockford's base32 alphabet in lowercase.
pub(crate) const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// `value` in Crockford's base32, most significant digit first, no padding beyond `digits`.
pub(crate) fn base32(mut value: u128, digits: usize) -> String {
    let digits = digits.max(1);
    let mut out = Vec::with_capacity(digits);
    loop {
        out.push(ALPHABET[(value & 31) as usize] as char);
        value >>= 5;
        if value == 0 && out.len() >= digits {
            break;
        }
    }
    out.iter().rev().collect()
}

/// `digits` characters (at most 26) from 128 random bits, for the epoch and session names.
pub fn random_chars(digits: usize) -> std::io::Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| {
        std::io::Error::other(format!("Cannot read the OS random source: {error}."))
    })?;
    let out = base32(u128::from_be_bytes(bytes), 26);
    Ok(out[26 - digits..].into())
}

/// Whether `id` has the bare task ID shape `<8-char base32 epoch>-<base32 counter>`.
pub fn is_valid_task_id(id: &str) -> bool {
    let Some((epoch, counter)) = id.split_once('-') else {
        return false;
    };
    epoch.len() == 8
        && !counter.is_empty()
        && counter.len() <= 13
        && epoch.bytes().all(|byte| ALPHABET.contains(&byte))
        && counter.bytes().all(|byte| ALPHABET.contains(&byte))
}

/// Splits an optional reader suffix before lookup or any task side effect.
pub fn parse_handle(handle: &str) -> Result<(&str, Option<&str>), String> {
    parse_handle_for(handle, "taskId")
}

/// Splits an optional reader suffix, naming `field` on malformed handles.
pub fn parse_handle_for<'a>(
    handle: &'a str,
    field: &str,
) -> Result<(&'a str, Option<&'a str>), String> {
    let Some((id, key)) = handle.split_once('.') else {
        return Ok((handle, None));
    };
    if id.is_empty() || key.len() != 4 || !key.bytes().all(|byte| ALPHABET.contains(&byte)) {
        return Err(format!(
            "Unknown {field} {handle:?}. Use a bare task ID or <taskId>.<reader>, with 4 Crockford base32 reader characters."
        ));
    }
    Ok((id, Some(key)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base32_is_minimal_and_most_significant_first() {
        assert_eq!(base32(1, 1), "1");
        assert_eq!(base32(31, 1), "z");
        assert_eq!(base32(32, 1), "10");
        assert_eq!(base32(0, 1), "0");
        // A zero source still fills the whole epoch width.
        assert_eq!(base32(0, 26), "0".repeat(26));
    }

    #[test]
    fn short_random_keys_use_the_full_alphabet_at_every_position() {
        let mut high = [false; 4];
        for _ in 0..128 {
            let key = random_chars(4).unwrap();
            assert_eq!(key.len(), 4);
            for (i, byte) in key.bytes().enumerate() {
                assert!(ALPHABET.contains(&byte));
                high[i] |= ALPHABET[8..].contains(&byte);
            }
        }
        assert!(high.into_iter().all(|seen| seen));
    }

    #[test]
    fn task_id_validation_checks_epoch_width_counter_bounds_and_alphabet() {
        let epoch = random_chars(8).unwrap();
        assert_eq!(epoch.len(), 8, "{epoch}");
        assert!(epoch.bytes().all(|b| ALPHABET.contains(&b)), "{epoch}");
        assert!(is_valid_task_id(&format!("{epoch}-{}", base32(1, 1))));
        assert!(is_valid_task_id(&format!(
            "{epoch}-{}",
            base32(u64::MAX as u128, 1)
        )));
        for malformed in [
            "",
            "t1",
            "foo-bar",
            "7yfh1xz-1",
            "7yfh1xz30-1",
            "7yfh1xz3-",
            "7yfh1xz3-0123456789abcd",
            "7yfh1xz3-1.abcd",
            "7yfh1xz3-I",
            "7yfh1xzI-1",
        ] {
            assert!(!is_valid_task_id(malformed), "{malformed}");
        }
    }

    #[test]
    fn handles_are_bare_ids_or_exact_reader_suffixes() {
        assert_eq!(parse_handle("task-1").unwrap(), ("task-1", None));
        assert_eq!(
            parse_handle("task-1.abcd").unwrap(),
            ("task-1", Some("abcd"))
        );
        for invalid in [
            "a.b.abcd", ".abcd", "a.abc", "a.abcde", "a.ABCD", "a.ilou", "a.abç",
        ] {
            let message = parse_handle(invalid).unwrap_err();
            assert!(message.contains("Unknown taskId"), "{message}");
            assert!(message.contains("<taskId>.<reader>"), "{message}");
        }
        let custom = parse_handle_for("a.abc", "parentTaskId").unwrap_err();
        assert!(custom.contains("Unknown parentTaskId"), "{custom}");
    }
}
