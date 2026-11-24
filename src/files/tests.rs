use super::edit::*;
use super::path::*;
use super::read::*;
use super::write::*;
use super::*;
use crate::lease::{BytePool, Lease};
use crate::service::render::{OutputMode, Render};
use crate::session::LaunchContext;
use serde_json::{Value, json};
use std::fs::File;
#[cfg(any(target_os = "freebsd", target_os = "macos"))]
use std::fs::OpenOptions;
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use unicode_normalization::UnicodeNormalization;

/// A directory of its own under the temp directory, removed when the test ends.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("fastexec-files-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("a scratch directory");
        Self(path)
    }

    fn file(&self, name: &str, content: &[u8]) -> PathBuf {
        let path = self.0.join(name);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("a parent directory");
        }
        std::fs::write(&path, content).expect("a file");
        path
    }

    fn read(&self, name: &str) -> Vec<u8> {
        std::fs::read(self.0.join(name)).expect("the file")
    }

    fn path(&self, name: &str) -> String {
        display_path(&self.0.join(name))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A launch context whose working directory is `cwd` and whose home is `home`.
fn context(cwd: &Path, home: &Path) -> LaunchContext {
    LaunchContext::new(
        cwd.to_path_buf(),
        vec![
            ("HOME".into(), home.as_os_str().to_owned()),
            ("USERPROFILE".into(), home.as_os_str().to_owned()),
        ],
    )
}

fn read_text(
    request: &ReadRequest,
    launch: &LaunchContext,
    transfer: bool,
    policy: ReadPolicy,
) -> Result<Outcome, String> {
    let owned = read_blocking(request, launch, transfer, policy, None);
    let owner = owned.owner.clone();
    owned
        .run(|read| read)
        .map(|data| read_data_to_outcome(data, policy, owner))
        .map_err(FileError::into_message)
}

fn edit_owned_outcome(
    request: &EditRequest,
    launch: &LaunchContext,
) -> crate::ipc::Owned<BlockingOutcome> {
    let owned = edit_blocking(request, launch);
    let owner = owned.owner.clone();
    let value = owned.run(|res| match res {
        Ok(data) => Ok(edit_data_to_agent(data)),
        Err(err) => {
            let structured = err.failures.map(|failures| {
                json!({
                    "ok": false,
                    "error": err.error.message(),
                    "path": err.path.unwrap_or_default(),
                    "failures": failures,
                })
            });
            Err((err.error.into_message(), structured))
        }
    });
    crate::ipc::Owned::new(value, owner)
}

fn edit_outcome(request: &EditRequest, launch: &LaunchContext) -> BlockingOutcome {
    edit_owned_outcome(request, launch).run(|outcome| outcome)
}

fn edit_text(request: &EditRequest, launch: &LaunchContext) -> Result<String, String> {
    edit_outcome(request, launch)
        .map(|(text, _)| text)
        .map_err(|(msg, _)| msg)
}

fn whole(path: &str, launch: &LaunchContext, transfer: bool) -> Result<String, String> {
    read_text(
        &ReadRequest {
            path: path.to_owned(),
            offset: None,
            limit: None,
        },
        launch,
        transfer,
        ReadPolicy::AGENT,
    )
    .map(|outcome| outcome.text)
}

fn window(
    path: &str,
    offset: Option<i64>,
    limit: Option<u64>,
    launch: &LaunchContext,
) -> Result<String, String> {
    read_text(
        &ReadRequest {
            path: path.to_owned(),
            offset,
            limit,
        },
        launch,
        false,
        ReadPolicy::AGENT,
    )
    .map(|outcome| outcome.text)
}

fn save_text(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let pool = crate::lease::process_pool();
    let target = write_target(path, pool)?;
    let mut lease = Lease::empty(pool);
    reserve_target_workspace(&target.value, &mut lease, pool)?;
    save_target(&target.value, bytes, None, pool)
}

fn change(path: &str, edits: &[(&str, &str)], launch: &LaunchContext) -> Result<String, String> {
    edit_text(
        &EditRequest {
            path: path.to_owned(),
            edits: edits
                .iter()
                .map(|(old, new)| Edit {
                    old_text: (*old).to_owned(),
                    new_text: (*new).to_owned(),
                    start_line: None,
                    end_line: None,
                })
                .collect(),
        },
        launch,
    )
}

#[test]
fn paths_resolve_against_the_launch_context() {
    let scratch = Scratch::new("resolve");
    let home = scratch.0.join("home");
    std::fs::create_dir_all(&home).unwrap();
    let launch = context(&scratch.0, &home);
    assert_eq!(resolve("~", &launch).unwrap(), home);
    assert_eq!(
        resolve("~/notes/a.txt", &launch).unwrap(),
        home.join("notes/a.txt")
    );
    assert_eq!(
        resolve("rel.txt", &launch).unwrap(),
        scratch.0.join("rel.txt")
    );
    let absolute = scratch.0.join("abs.txt");
    assert_eq!(
        resolve(&absolute.display().to_string(), &launch).unwrap(),
        absolute
    );
    // A home nobody knows is an error rather than a silent fallback.
    let homeless = LaunchContext::new(scratch.0.clone(), Vec::new());
    let message = resolve("~/x", &homeless).unwrap_err();
    assert!(
        message.contains("the home directory is unknown"),
        "{message}"
    );
}

#[test]
fn a_relative_home_stays_anchored_to_the_session_directory() {
    let scratch = Scratch::new("relative-home");
    let launch = context(&scratch.0, Path::new("local-home"));
    let nested = launch.with_cwd(scratch.0.join("other"));
    let file = scratch.file("local-home/notes", b"session home");
    let home = scratch.0.join("local-home");
    assert_eq!(nested.home().as_deref(), Some(home.as_path()));
    assert!(
        crate::mcp::host_line(&nested).contains(&format!(" · home {} · ", display_path(&home)))
    );
    assert_eq!(resolve("~/notes", &nested).unwrap(), file);
    assert_eq!(whole("~/notes", &nested, false).unwrap(), "session home");
}

#[test]
fn home_expansion_keeps_repeated_separators_under_home() {
    let scratch = Scratch::new("home-separators");
    let home = scratch.0.join("home");
    std::fs::create_dir_all(home.join("notes")).unwrap();
    let path = home.join("notes/a.txt");
    std::fs::write(&path, b"home file\n").unwrap();
    let launch = context(&scratch.0, &home);
    for input in ["~/notes/a.txt", "~//notes///a.txt", "~///notes/a.txt"] {
        assert_eq!(resolve(input, &launch).unwrap(), path);
        assert_eq!(whole(input, &launch, false).unwrap(), "home file\n");
    }
    #[cfg(windows)]
    for input in [r"~\\notes\\a.txt", r"~\\/notes//a.txt"] {
        assert_eq!(resolve(input, &launch).unwrap(), path);
    }
    write_text(
        &WriteRequest {
            path: "~//notes/a.txt".into(),
            content: "updated\n".into(),
        },
        &launch,
    )
    .unwrap();
    assert_eq!(std::fs::read(path).unwrap(), b"updated\n");
}

#[cfg(windows)]
#[test]
fn home_expansion_refuses_windows_prefixes_after_separators() {
    let launch = context(Path::new(r"C:\cwd"), Path::new(r"D:\home"));
    for input in [
        "~/C:notes.txt",
        "~/D:/value",
        "~//C:notes.txt",
        "~///D:/value",
        r"~\C:notes.txt",
        r"~\\D:\value",
        r"~\/C:notes.txt",
    ] {
        let error = resolve(input, &launch).unwrap_err();
        assert!(
            error.contains("the path after ~ must be relative to the home directory"),
            "{input}: {error}"
        );
    }
}

#[cfg(unix)]
#[test]
fn home_expansion_keeps_colons_in_unix_file_names() {
    let launch = context(Path::new("/cwd"), Path::new("/home/test"));
    for input in ["C:notes.txt", "D:/value"] {
        assert_eq!(
            resolve(&format!("~/{input}"), &launch).unwrap(),
            Path::new("/home/test").join(input)
        );
    }
}

#[cfg(windows)]
#[test]
fn verbatim_paths_keep_parent_components_when_resolved_and_appended() {
    for base in [r"\\?\C:\base", r"\\?\UNC\server\share\base"] {
        let launch = context(Path::new(base), Path::new(base));
        let expected = PathBuf::from(format!(r"{base}\link\..\value"));
        let absolute = format!(r"{base}\.\link\\..\value");
        assert_eq!(resolve(&absolute, &launch).unwrap(), expected);
        for input in [
            "link/../value",
            r"link\..\value",
            "./link//../value",
            "~/link/../value",
            r"~\link\..\value",
        ] {
            assert_eq!(resolve(input, &launch).unwrap(), expected, "{input}");
        }
        let parent = PathBuf::from(format!(r"{base}\link\.."));
        assert_eq!(
            append_path(&parent, Path::new("staged")),
            PathBuf::from(format!(r"{base}\link\..\staged"))
        );
    }
}

#[cfg(windows)]
#[test]
fn appending_windows_paths_preserves_roots_and_parent_components() {
    for (base, prefix) in [
        (r"C:\base\dir", r"C:"),
        (r"\\?\C:\base\dir", r"\\?\C:"),
        (r"\\server\share\base\dir", r"\\server\share"),
        (r"\\?\UNC\server\share\base\dir", r"\\?\UNC\server\share"),
    ] {
        for rooted in [r"\other\link\..\value", "/other/link/../value"] {
            assert_eq!(
                append_path(Path::new(base), Path::new(rooted)),
                PathBuf::from(format!(r"{prefix}\other\link\..\value")),
                "{base} + {rooted}"
            );
        }
        for replacement in [
            r"D:\other\link\..\value",
            r"\\?\D:\other\link\..\value",
            r"\\other\share\link\..\value",
            r"\\?\UNC\other\share\link\..\value",
            r"D:link\..\value",
        ] {
            assert_eq!(
                append_path(Path::new(base), Path::new(replacement)),
                PathBuf::from(replacement),
                "{base} + {replacement}"
            );
        }
    }
}

#[test]
fn read_returns_the_window_it_was_asked_for() {
    let scratch = Scratch::new("window");
    let text: String = (1..=10).map(|line| format!("line {line}\n")).collect();
    scratch.file("ten.txt", text.as_bytes());
    let launch = context(&scratch.0, &scratch.0);
    let path = scratch.path("ten.txt");
    // Ten terminated lines are ten real lines, with terminators kept intact.
    assert_eq!(whole(&path, &launch, false).unwrap(), text);
    assert_eq!(
        window(&path, Some(2), Some(3), &launch).unwrap(),
        "line 2\nline 3\nline 4\n\n[6 more lines in file. Use offset=5 to continue.]"
    );
    assert_eq!(
        window(&path, None, Some(0), &launch).unwrap_err(),
        "limit must be at least 1."
    );
    assert_eq!(window(&path, Some(10), None, &launch).unwrap(), "line 10\n");
    assert_eq!(
        window(&path, Some(11), None, &launch).unwrap_err(),
        "Offset 11 is beyond end of file (10 lines total)"
    );
}

#[test]
fn read_counts_selected_empty_lines_before_the_end_of_file() {
    let scratch = Scratch::new("selected-empty-lines");
    let launch = context(&scratch.0, &scratch.0);
    scratch.file("three.txt", b"a\n\nb");
    for (offset, limit, body, shown, end) in [(1, 2, "a\n\n", 2, 2), (2, 1, "\n", 1, 2)] {
        let outcome = read_text(
            &ReadRequest {
                path: scratch.path("three.txt"),
                offset: Some(offset),
                limit: Some(limit),
            },
            &launch,
            false,
            ReadPolicy::AGENT,
        )
        .unwrap();
        assert_eq!(outcome.body.as_deref(), Some(body));
        assert_eq!(outcome.structured["shownLines"], shown);
        assert_eq!(outcome.structured["endLine"], end);
        assert_eq!(outcome.structured["nextOffset"], 3);
    }
    scratch.file("empty.txt", b"");
    let empty = read_text(
        &ReadRequest {
            path: scratch.path("empty.txt"),
            offset: None,
            limit: None,
        },
        &launch,
        false,
        ReadPolicy::AGENT,
    )
    .unwrap();
    assert_eq!(empty.structured["shownLines"], 0);
    assert!(empty.structured["endLine"].is_null());
}

#[test]
fn read_cuts_at_the_line_and_byte_budgets() {
    let scratch = Scratch::new("budgets");
    let launch = context(&scratch.0, &scratch.0);
    let many: String = (1..=2500).map(|line| format!("line {line}\n")).collect();
    scratch.file("many.txt", many.as_bytes());
    let path = scratch.path("many.txt");
    let text = whole(&path, &launch, false).unwrap();
    assert!(
        text.ends_with("[Showing lines 1-2000 of 2500. Use offset=2001 to continue.]"),
        "{text}"
    );
    assert!(text.starts_with("line 1\nline 2\n"), "{text}");
    assert!(text.contains("line 2000\n\n"), "{text}");
    assert!(!text.contains("line 2001"), "{text}");

    // Fifty-nine-byte lines: the byte budget runs out at the 853rd line.
    let wide: String = (0..900).map(|_| format!("{}\n", "x".repeat(59))).collect();
    scratch.file("wide.txt", wide.as_bytes());
    let path = scratch.path("wide.txt");
    let text = whole(&path, &launch, false).unwrap();
    assert!(
        text.ends_with("[Showing lines 1-853 of 900 (50.0KB limit). Use offset=854 to continue.]"),
        "{text}"
    );

    // A first line over the budget gets the shell hint instead of a body.
    let huge = format!("{}\n", "y".repeat(MAX_BYTES as usize + 1));
    scratch.file("huge.txt", huge.as_bytes());
    let path = scratch.path("huge.txt");
    assert_eq!(
        whole(&path, &launch, false).unwrap(),
        format!(
            "[Line 1 is 50.0KB, exceeds 50.0KB limit. Use exec: sed -n '1p' '{path}' | head -c {MAX_BYTES}]"
        )
    );
    let nested_huge = read_text(
        &ReadRequest {
            path: path.clone(),
            offset: None,
            limit: None,
        },
        &launch,
        false,
        ReadPolicy::PROGRAM,
    )
    .unwrap();
    assert_eq!(nested_huge.body.as_deref(), Some(huge.as_str()));
    assert_eq!(nested_huge.structured["truncated"], false);
    assert_eq!(nested_huge.structured["lineTooLong"], false);
    assert_eq!(nested_huge.structured["shownLines"], 1);

    // P0-2: nested whole-file reads have no line cap up to 4 MiB.
    let nested_many = "x\n".repeat(50_003);
    scratch.file("nested-many.txt", nested_many.as_bytes());
    let nested_many_path = scratch.path("nested-many.txt");
    let nested_whole = read_text(
        &ReadRequest {
            path: nested_many_path,
            offset: None,
            limit: None,
        },
        &launch,
        false,
        ReadPolicy::PROGRAM,
    )
    .unwrap();
    assert_eq!(nested_whole.body.as_deref(), Some(nested_many.as_str()));
    assert_eq!(nested_whole.structured["totalLines"], 50_003);
    assert_eq!(nested_whole.structured["shownLines"], 50_003);
    assert_eq!(nested_whole.structured["truncated"], false);
    assert!(nested_whole.structured["nextOffset"].is_null());

    // First line 1,048,577 B + "\ny\n" succeeds whole in nested whole-file mode.
    let one_mib_plus = format!("{}\ny\n", "a".repeat(1_048_577));
    scratch.file("one-mib-plus.txt", one_mib_plus.as_bytes());
    let whole_mib = read_text(
        &ReadRequest {
            path: scratch.path("one-mib-plus.txt"),
            offset: None,
            limit: None,
        },
        &launch,
        false,
        ReadPolicy::PROGRAM,
    )
    .unwrap();
    assert_eq!(whole_mib.body.as_deref(), Some(one_mib_plus.as_str()));
    assert_eq!(whole_mib.structured["totalLines"], 2);

    // 4 MiB + 1 B rejects whole-file mode naming offset & limit; slice mode works.
    let over_nested = "z".repeat(NESTED_MAX_BYTES as usize + 1);
    scratch.file("over-nested.txt", over_nested.as_bytes());
    let over_nested_path = scratch.path("over-nested.txt");
    let whole_err = read_text(
        &ReadRequest {
            path: over_nested_path.clone(),
            offset: None,
            limit: None,
        },
        &launch,
        false,
        ReadPolicy::PROGRAM,
    )
    .err()
    .unwrap();
    assert!(
        whole_err.contains("offset") && whole_err.contains("limit") && whole_err.contains("4 MiB"),
        "{whole_err}"
    );
    let slice_long = read_text(
        &ReadRequest {
            path: over_nested_path.clone(),
            offset: Some(1),
            limit: None,
        },
        &launch,
        false,
        ReadPolicy::PROGRAM,
    )
    .unwrap();
    assert_eq!(slice_long.structured["lineTooLong"], true);
    assert_eq!(slice_long.structured["shownLines"], 0);

    // Invalid UTF-8 (6f ff 6b 0a) rejects whole-file mode, succeeds with lossy: true on offset: 1.
    scratch.file("invalid-utf8.txt", &[0x6f, 0xff, 0x6b, 0x0a]);
    let inv_path = scratch.path("invalid-utf8.txt");
    let inv_err = read_text(
        &ReadRequest {
            path: inv_path.clone(),
            offset: None,
            limit: None,
        },
        &launch,
        false,
        ReadPolicy::PROGRAM,
    )
    .err()
    .unwrap();
    assert!(
        inv_err.contains("not valid UTF-8")
            && inv_err.contains("offset")
            && inv_err.contains("limit"),
        "{inv_err}"
    );
    let inv_slice = read_text(
        &ReadRequest {
            path: inv_path,
            offset: Some(1),
            limit: None,
        },
        &launch,
        false,
        ReadPolicy::PROGRAM,
    )
    .unwrap();
    assert_eq!(inv_slice.structured["lossy"], true);
    assert_eq!(inv_slice.body.as_deref(), Some("o\u{fffd}k\n"));

    // 5 MiB multi-line file with offset: 1, limit: 100 returns lines 1-100 with nextOffset: 101.
    let five_mib: String = (1..=100_000)
        .map(|i| format!("row-{i:06}-padding-abcdefghij-0123456789-xyz-0123456789\n"))
        .collect();
    assert!(five_mib.len() > 5 << 20);
    scratch.file("five-mib.txt", five_mib.as_bytes());
    let five_slice = read_text(
        &ReadRequest {
            path: scratch.path("five-mib.txt"),
            offset: Some(1),
            limit: Some(100),
        },
        &launch,
        false,
        ReadPolicy::PROGRAM,
    )
    .unwrap();
    assert_eq!(five_slice.structured["startLine"], 1);
    assert_eq!(five_slice.structured["endLine"], 100);
    assert_eq!(five_slice.structured["shownLines"], 100);
    assert_eq!(five_slice.structured["truncated"], true);
    assert_eq!(five_slice.structured["nextOffset"], 101);
    assert_eq!(
        nested_whole
            .structured
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        five_slice
            .structured
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>()
    );
}

#[cfg(unix)]
#[test]
fn the_long_line_hint_reads_paths_with_spaces_and_quotes() {
    let scratch = Scratch::new("quoted-line");
    let launch = context(&scratch.0, &scratch.0);
    let body = "a".repeat(MAX_BYTES as usize + 100);
    let path = scratch.file("O'Reilly notes.txt", body.as_bytes());
    let answer = whole(path.to_str().unwrap(), &launch, false).unwrap();
    let (_, command) = answer.split_once("Use exec: ").unwrap();
    let output = std::process::Command::new("sh")
        .args(["-c", command.strip_suffix(']').unwrap()])
        .output()
        .unwrap();
    assert!(
        output.status.success() && output.stderr.is_empty(),
        "{output:?}"
    );
    assert_eq!(output.stdout, body.as_bytes()[..MAX_BYTES as usize]);
}

#[test]
fn read_names_a_binary_file_and_its_transfer_hint() {
    let scratch = Scratch::new("binary");
    scratch.file("blob.bin", b"\x00\x01\x02\x03");
    let launch = context(&scratch.0, &scratch.0);
    let path = scratch.path("blob.bin");
    assert_eq!(
        whole(&path, &launch, false).unwrap_err(),
        format!("{path} is a binary file (4 bytes). Inspect it with exec.")
    );
    assert_eq!(
        whole(&path, &launch, true).unwrap_err(),
        format!(
            "{path} is a binary file (4 bytes). Inspect it with exec or download it with transfer."
        )
    );
}

#[test]
fn read_refuses_a_directory_and_notes_replaced_bytes() {
    let scratch = Scratch::new("lossy");
    let launch = context(&scratch.0, &scratch.0);
    let directory = scratch.0.display().to_string();
    let shown_dir = display_path(&scratch.0);
    assert_eq!(
        whole(&directory, &launch, false).unwrap_err(),
        format!("Cannot read {shown_dir}: it is a directory; list it with exec.")
    );
    scratch.file("bad.txt", b"ok\n\xffbad\n");
    let path = scratch.path("bad.txt");
    assert_eq!(
        whole(&path, &launch, false).unwrap(),
        "ok\n\u{fffd}bad\n\n[Invalid UTF-8 was replaced with U+FFFD.]"
    );
    let missing = scratch.path("missing.txt");
    let message = whole(&missing, &launch, false).unwrap_err();
    assert!(
        message.starts_with(&format!("Cannot read {missing}: ")),
        "{message}"
    );
}

#[test]
fn read_keeps_a_character_split_by_a_read_boundary() {
    let scratch = Scratch::new("boundary");
    let launch = context(&scratch.0, &scratch.0);
    // The reader's buffer is 8 KiB, so a character starting at its last byte is cut in two.
    let line = format!("{}{}tail\n", "a".repeat(8191), "€");
    scratch.file("split.txt", line.as_bytes());
    let path = scratch.path("split.txt");
    let text = whole(&path, &launch, false).unwrap();
    assert_eq!(text, line);
    assert!(!text.contains("U+FFFD"), "{text}");
}

#[test]
fn read_flushes_an_unfinished_character_at_the_end() {
    let scratch = Scratch::new("unfinished");
    let launch = context(&scratch.0, &scratch.0);
    // An incomplete character at the end is one replacement character, never dropped.
    for (name, bytes) in [("c2.txt", vec![0xc2u8]), ("e282.txt", vec![0xe2u8, 0x82])] {
        scratch.file(name, &bytes);
        let path = scratch.path(name);
        assert_eq!(
            whole(&path, &launch, false).unwrap(),
            "\u{fffd}\n\n[Invalid UTF-8 was replaced with U+FFFD.]"
        );
    }
    // The same after a whole read buffer: the carry is flushed at the real end of the file.
    let mut long = vec![b'a'; 8192];
    long.push(0xe2);
    scratch.file("long.txt", &long);
    let path = scratch.path("long.txt");
    assert_eq!(
        whole(&path, &launch, false).unwrap(),
        format!(
            "{}\u{fffd}\n\n[Invalid UTF-8 was replaced with U+FFFD.]",
            "a".repeat(8192)
        )
    );
}

#[test]
fn read_keeps_a_final_newline_without_a_false_notice() {
    let scratch = Scratch::new("final-newline");
    let launch = context(&scratch.0, &scratch.0);
    // 2000 terminated lines are 2000 lines, not 2001: nothing is truncated.
    let text: String = "x\n".repeat(2000);
    scratch.file("many.txt", text.as_bytes());
    let path = scratch.path("many.txt");
    assert_eq!(whole(&path, &launch, false).unwrap(), text);
    // The last 2000 lines of a longer file are a window, not a truncation.
    let long: String = (1..=2001).map(|line| format!("line {line}\n")).collect();
    scratch.file("long.txt", long.as_bytes());
    let path = scratch.path("long.txt");
    let tail: String = (2..=2001).map(|line| format!("line {line}\n")).collect();
    assert_eq!(window(&path, Some(2), None, &launch).unwrap(), tail);
}

#[test]
fn read_names_a_first_line_at_the_byte_limit_as_a_line_count() {
    let scratch = Scratch::new("at-limit");
    let launch = context(&scratch.0, &scratch.0);
    // A line whose terminator brings it to MAX_BYTES fits; a second line triggers the notice.
    let line = "z".repeat(MAX_BYTES as usize - 1);
    scratch.file("at.txt", format!("{line}\nsecond\n").as_bytes());
    let path = display_path(&scratch.0.join("at.txt"));
    assert_eq!(
        whole(&path, &launch, false).unwrap(),
        format!("{line}\n\n[Showing lines 1-1 of 2 (50.0KB limit). Use offset=2 to continue.]")
    );
    // A terminator straddling MAX_BYTES pushes line 1 to MAX_BYTES + 1 bytes -> lineTooLong.
    let straddle = "z".repeat(MAX_BYTES as usize);
    scratch.file("straddle.txt", format!("{straddle}\n").as_bytes());
    let straddle_path = display_path(&scratch.0.join("straddle.txt"));
    assert_eq!(
        whole(&straddle_path, &launch, false).unwrap(),
        format!(
            "[Line 1 is 50.0KB, exceeds 50.0KB limit. Use exec: sed -n '1p' '{straddle_path}' | head -c {MAX_BYTES}]"
        )
    );
}

#[test]
fn sizes_round_the_way_a_fixed_decimal_rounds() {
    assert_eq!(format_size(1023), "1023B");
    assert_eq!(format_size(1024), "1.0KB");
    assert_eq!(format_size(51200), "50.0KB");
    assert_eq!(format_size(51456), "50.3KB");
    assert_eq!(format_size(80000), "78.1KB");
    assert_eq!(format_size(1310720), "1.3MB");
}

#[test]
fn read_accepts_the_largest_offset_and_limit() {
    let scratch = Scratch::new("max-limit");
    let launch = context(&scratch.0, &scratch.0);
    scratch.file("two.txt", b"one\ntwo\n");
    let path = scratch.path("two.txt");
    assert_eq!(
        window(&path, Some(2), Some(u64::MAX), &launch).unwrap(),
        "two\n"
    );
}

#[test]
fn edit_counts_a_whitespace_needle_as_the_content_spells_it() {
    let scratch = Scratch::new("blank-needle");
    let launch = context(&scratch.0, &scratch.0);
    let shown_a = display_path(&scratch.0.join("a.txt"));
    let shown_b = display_path(&scratch.0.join("b.txt"));
    let shown_c = display_path(&scratch.0.join("c.txt"));
    // A unique blank is a real target, and it is counted where the content spells it.
    scratch.file("a.txt", b"a b");
    assert_eq!(
        change("a.txt", &[(" ", "X")], &launch).unwrap(),
        format!("Successfully replaced 1 block(s) in {shown_a}.\nedits[0]: line 1")
    );
    assert_eq!(scratch.read("a.txt"), b"aXb");
    // Two blanks are two candidates.
    scratch.file("c.txt", b"a b c");
    assert_eq!(
        change("c.txt", &[(" ", "X")], &launch).unwrap_err(),
        format!(
            "Cannot edit {shown_c}; nothing was changed:\n- edits[0]: oldText occurs 2 times, starting at lines 1, 1; add surrounding text to make it unique."
        )
    );
    assert_eq!(scratch.read("c.txt"), b"a b c");
    // A blank the content does not have is not a target at all.
    scratch.file("b.txt", "\u{1f600}\u{1f600}".as_bytes());
    assert_eq!(
        change("b.txt", &[("\u{3000}", "X")], &launch).unwrap_err(),
        format!(
            "Cannot edit {shown_b}; nothing was changed:\n- edits[0]: oldText was not found, even ignoring trailing whitespace, Unicode compatibility forms, curly quotes, dashes, and special spaces."
        )
    );
    assert_eq!(scratch.read("b.txt"), "\u{1f600}\u{1f600}".as_bytes());
}

#[test]
fn write_creates_parents_and_overwrites_in_place() {
    let scratch = Scratch::new("write");
    let launch = context(&scratch.0, &scratch.0);
    let (written, shown) = write_text(
        &WriteRequest {
            path: "deep/er/file.txt".to_owned(),
            content: "first".to_owned(),
        },
        &launch,
    )
    .unwrap();
    assert_eq!(shown, display_path(&scratch.0.join("deep/er/file.txt")));
    assert_eq!(written, format!("Successfully wrote to {shown}"));
    assert_eq!(scratch.read("deep/er/file.txt"), b"first");
    write_text(
        &WriteRequest {
            path: "deep/er/file.txt".to_owned(),
            content: "second".to_owned(),
        },
        &launch,
    )
    .unwrap();
    assert_eq!(scratch.read("deep/er/file.txt"), b"second");
}

#[cfg(unix)]
#[test]
fn write_keeps_an_existing_files_mode() {
    use std::os::unix::fs::PermissionsExt;
    let scratch = Scratch::new("mode");
    let launch = context(&scratch.0, &scratch.0);
    let path = scratch.file("script.sh", b"#!/bin/sh\n");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o750)).unwrap();
    write_text(
        &WriteRequest {
            path: "script.sh".to_owned(),
            content: "#!/bin/sh\necho hi\n".to_owned(),
        },
        &launch,
    )
    .unwrap();
    let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o750);
}

#[test]
fn edit_maps_cross_line_matches_and_reports_cr_lines() {
    let scratch = Scratch::new("mapped-endings");
    let launch = context(&scratch.0, &scratch.0);
    scratch.file("mixed.txt", "é\r\n中\nneedle\rtail\r\nend\n".as_bytes());
    let reply = change(
        "mixed.txt",
        &[("中\nneedle", "新\nvalue"), ("end", "END")],
        &launch,
    )
    .unwrap();
    assert!(reply.contains("edits[0]: line 2"), "{reply}");
    assert!(reply.contains("edits[1]: line 4"), "{reply}");
    assert_eq!(
        scratch.read("mixed.txt"),
        "é\r\n新\r\nvalue\rtail\r\nEND\n".as_bytes()
    );
    scratch.file("same.txt", b"a\r\nb\nc\r");
    assert!(change("same.txt", &[("a\nb\nc\n", "a\nb\nc\n")], &launch).is_err());
    assert_eq!(scratch.read("same.txt"), b"a\r\nb\nc\r");
}

#[cfg(unix)]
#[test]
fn atomic_saves_follow_existing_and_dangling_symlinks() {
    use std::os::unix::fs::symlink;
    let scratch = Scratch::new("save-links");
    let launch = context(&scratch.0, &scratch.0);
    let target = scratch.file("target.txt", b"old");
    let link = scratch.0.join("link.txt");
    symlink("target.txt", &link).unwrap();
    change("link.txt", &[("old", "new")], &launch).unwrap();
    assert!(
        std::fs::symlink_metadata(&link)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(std::fs::read(&target).unwrap(), b"new");
    save_text(&link, b"again").unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"again");
    let dangling = scratch.0.join("dangling.txt");
    symlink("created.txt", &dangling).unwrap();
    save_text(&dangling, b"created").unwrap();
    assert!(
        std::fs::symlink_metadata(&dangling)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(scratch.read("created.txt"), b"created");
}

fn file_symlink(target: &Path, link: &Path) -> bool {
    #[cfg(unix)]
    let result = std::os::unix::fs::symlink(target, link);
    #[cfg(windows)]
    let result = std::os::windows::fs::symlink_file(target, link);
    match result {
        Ok(()) => true,
        Err(error) => {
            #[cfg(windows)]
            if error.raw_os_error() == Some(1314) && std::env::var_os("CI").is_none() {
                eprintln!(
                    "skipped: creating a Windows symlink requires Developer Mode or its privilege"
                );
                return false;
            }
            panic!("cannot create fixture symlink {}: {error}", link.display());
        }
    }
}

#[cfg(windows)]
#[test]
fn windows_root_relative_symlink_write_matches_native_resolution() {
    // A root-relative target uses the current drive, which may differ from the temp drive.
    let scratch = Scratch(std::env::current_dir().unwrap().join(format!(
        "fastexec-rooted-link-{}",
        crate::tasks::random_chars(8).unwrap()
    )));
    std::fs::create_dir(&scratch.0).unwrap();
    let launch = context(&scratch.0, &scratch.0);
    let target = scratch.file("target.txt", b"before");
    let shown = display_path(&target);
    assert_eq!(shown.as_bytes().get(1), Some(&b':'), "{shown}");
    let rooted = PathBuf::from(shown[2..].replace('/', "\\"));
    let link = scratch.0.join("link.txt");
    if !file_symlink(&rooted, &link) {
        return;
    }
    assert_eq!(std::fs::read(&link).unwrap(), b"before");
    write_text(
        &WriteRequest {
            path: display_path(&link),
            content: "after".into(),
        },
        &launch,
    )
    .unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"after");
    change("link.txt", &[("after", "edited")], &launch).unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"edited");
    // Recreating a dangling root-relative target must still leave the link in place.
    std::fs::remove_file(&target).unwrap();
    write_text(
        &WriteRequest {
            path: display_path(&link),
            content: "created".into(),
        },
        &launch,
    )
    .unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"created");
    assert_eq!(std::fs::read(&link).unwrap(), b"created");
    assert!(
        std::fs::symlink_metadata(link)
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[test]
fn edit_rejects_a_retargeted_symlink_without_changing_either_file() {
    let scratch = Scratch::new("edit-retargeted-link");
    let first = scratch.file("first.txt", b"old content");
    let other = scratch.file("other.txt", b"must stay intact");
    let link = scratch.0.join("link.txt");
    if !file_symlink(&first, &link) {
        return;
    }
    let source = EditSource::read(link.clone(), crate::lease::process_pool()).unwrap();
    std::fs::remove_file(&link).unwrap();
    assert!(file_symlink(&other, &link));
    let error = source
        .save(b"edited content", crate::lease::process_pool())
        .unwrap_err();
    assert!(
        error.to_string().contains("changed after it was read"),
        "{error}"
    );
    assert_eq!(scratch.read("first.txt"), b"old content");
    assert_eq!(scratch.read("other.txt"), b"must stay intact");
}

#[test]
fn edit_rejects_another_inode_even_when_its_content_matches() {
    let scratch = Scratch::new("edit-replaced-inode");
    let path = scratch.file("target.txt", b"old content");
    let source = EditSource::read(path.clone(), crate::lease::process_pool()).unwrap();
    std::fs::rename(&path, scratch.0.join("retired.txt")).unwrap();
    std::fs::write(&path, b"old content").unwrap();
    let error = source
        .save(b"edited content", crate::lease::process_pool())
        .unwrap_err();
    assert!(
        error.to_string().contains("changed after it was read"),
        "{error}"
    );
    assert_eq!(scratch.read("target.txt"), b"old content");
    assert_eq!(scratch.read("retired.txt"), b"old content");
    assert_eq!(std::fs::read_dir(&scratch.0).unwrap().count(), 2);
}

#[test]
fn edit_rejects_in_place_changes_to_the_file_it_read() {
    let scratch = Scratch::new("edit-changed-content");
    for changed in [b"other value".as_slice(), b"old content plus bytes", b"old"] {
        let path = scratch.file("target.txt", b"old content");
        let source = EditSource::read(path.clone(), crate::lease::process_pool()).unwrap();
        std::fs::write(&path, changed).unwrap();
        let error = source
            .save(b"edited content", crate::lease::process_pool())
            .unwrap_err();
        assert!(
            error.to_string().contains("changed after it was read"),
            "{error}"
        );
        assert_eq!(scratch.read("target.txt"), changed);
        assert_eq!(std::fs::read_dir(&scratch.0).unwrap().count(), 1);
    }
}

#[cfg(unix)]
#[test]
fn edit_rejects_a_retargeted_parent_directory_link() {
    use std::os::unix::fs::symlink;
    let scratch = Scratch::new("edit-retargeted-parent");
    scratch.file("first/target.txt", b"old content");
    scratch.file("other/target.txt", b"must stay intact");
    let link = scratch.0.join("directory");
    symlink(scratch.0.join("first"), &link).unwrap();
    let source = EditSource::read(link.join("target.txt"), crate::lease::process_pool()).unwrap();
    std::fs::remove_file(&link).unwrap();
    symlink(scratch.0.join("other"), &link).unwrap();
    let error = source
        .save(b"edited content", crate::lease::process_pool())
        .unwrap_err();
    assert!(
        error.to_string().contains("changed after it was read"),
        "{error}"
    );
    assert_eq!(scratch.read("first/target.txt"), b"old content");
    assert_eq!(scratch.read("other/target.txt"), b"must stay intact");
}

#[test]
fn write_creates_dangling_symlink_target_parents() {
    let scratch = Scratch::new("write-link-parents");
    let launch = context(&scratch.0, &scratch.0);
    let links = scratch.0.join("links");
    std::fs::create_dir(&links).unwrap();
    for (name, absolute, chained) in [
        ("relative", false, false),
        ("absolute", true, false),
        ("chained", false, true),
    ] {
        let target = scratch.0.join(name).join("missing").join("nested.txt");
        let stored = if absolute {
            target.clone()
        } else {
            Path::new("..")
                .join(name)
                .join("missing")
                .join("nested.txt")
        };
        let direct = links.join(format!("{name}.link"));
        if !file_symlink(&stored, &direct) {
            return;
        }
        let direct_before = std::fs::read_link(&direct).unwrap();
        let link = if chained {
            let link = links.join("chain.link");
            if !file_symlink(direct.file_name().unwrap().as_ref(), &link) {
                return;
            }
            link
        } else {
            direct.clone()
        };
        let link_before = std::fs::read_link(&link).unwrap();
        let path = link.to_string_lossy().into_owned();
        write_text(
            &WriteRequest {
                path: path.clone(),
                content: "created".to_owned(),
            },
            &launch,
        )
        .unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"created");
        assert_eq!(std::fs::read(&link).unwrap(), b"created");
        assert_eq!(std::fs::read_link(&direct).unwrap(), direct_before);
        assert_eq!(std::fs::read_link(&link).unwrap(), link_before);
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        change(&path, &[("created", "edited")], &launch).unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"edited");
        assert_eq!(std::fs::read_link(&direct).unwrap(), direct_before);
        assert_eq!(std::fs::read_link(&link).unwrap(), link_before);
        assert!(
            std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink()
        );
    }
}

#[cfg(unix)]
#[test]
fn directory_only_spellings_never_overwrite_regular_files() {
    let scratch = Scratch::new("directory-suffix");
    let launch = context(&scratch.0, &scratch.0);
    let original = scratch.file("target", b"original");
    for suffix in ["/", "/."] {
        let name = format!("target{suffix}");
        let link = scratch.0.join("link");
        std::os::unix::fs::symlink(&name, &link).unwrap();
        for requested in [&name[..], "link"] {
            assert!(
                whole(requested, &launch, false).is_err(),
                "{name}: {requested}"
            );
            assert!(
                change(requested, &[("original", "bad")], &launch).is_err(),
                "{name}: {requested}"
            );
            assert!(
                crate::service::schema::validate_cwd(requested, &launch).is_err(),
                "{name}: {requested}"
            );
            assert!(
                write_text(
                    &WriteRequest {
                        path: requested.into(),
                        content: "bad".into()
                    },
                    &launch
                )
                .is_err()
            );
        }
        assert_eq!(std::fs::read(&original).unwrap(), b"original");
        assert_eq!(
            std::fs::read_link(&link).unwrap().as_os_str(),
            Path::new(&name).as_os_str()
        );
        std::fs::remove_file(&link).unwrap();
        std::os::unix::fs::symlink("target", &link).unwrap();
        let opened = EditSource::read(link.clone(), crate::lease::process_pool()).unwrap();
        std::fs::remove_file(&link).unwrap();
        std::os::unix::fs::symlink(&name, &link).unwrap();
        assert!(
            opened.save(b"bad", crate::lease::process_pool()).is_err(),
            "{name}"
        );
        assert_eq!(std::fs::read(&original).unwrap(), b"original");
        std::fs::remove_file(link).unwrap();
    }
    let sibling = scratch.file("sibling.txt", b"keep sibling");
    let invalid = "target/../sibling.txt";
    assert!(whole(invalid, &launch, false).is_err());
    assert!(change(invalid, &[("keep sibling", "bad")], &launch).is_err());
    assert_eq!(std::fs::read(sibling).unwrap(), b"keep sibling");
}

#[test]
fn edit_does_not_create_dangling_symlink_target_parents() {
    let scratch = Scratch::new("edit-link-parents");
    let launch = context(&scratch.0, &scratch.0);
    let link = scratch.0.join("link.txt");
    if !file_symlink(&Path::new("missing").join("nested.txt"), &link) {
        return;
    }
    let link_before = std::fs::read_link(&link).unwrap();
    assert!(change("link.txt", &[("old", "new")], &launch).is_err());
    assert!(!scratch.0.join("missing").exists());
    assert_eq!(std::fs::read_link(&link).unwrap(), link_before);
}

#[test]
fn write_does_not_replace_a_symlink_targets_non_directory_parent() {
    let scratch = Scratch::new("write-link-obstruction");
    let launch = context(&scratch.0, &scratch.0);
    scratch.file("blocked", b"keep this file");
    let link = scratch.0.join("link.txt");
    let target = Path::new("blocked").join("missing").join("file.txt");
    if !file_symlink(&target, &link) {
        return;
    }
    let link_before = std::fs::read_link(&link).unwrap();
    assert!(
        write_text(
            &WriteRequest {
                path: "link.txt".to_owned(),
                content: "new".to_owned()
            },
            &launch,
        )
        .is_err()
    );
    assert_eq!(scratch.read("blocked"), b"keep this file");
    assert_eq!(std::fs::read_link(&link).unwrap(), link_before);
}

#[test]
fn atomic_saves_refuse_to_silently_split_hard_links() {
    let scratch = Scratch::new("save-hard-links");
    let launch = context(&scratch.0, &scratch.0);
    let original = scratch.file("original.txt", b"original");
    std::fs::hard_link(&original, scratch.0.join("alias.txt")).unwrap();
    let error = save_text(&original, b"replacement")
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("hard links") && error.contains("exec"),
        "{error}"
    );
    assert!(change("original.txt", &[("original", "replacement")], &launch).is_err());
    assert_eq!(scratch.read("original.txt"), b"original");
    assert_eq!(scratch.read("alias.txt"), b"original");
}

#[cfg(target_os = "freebsd")]
fn freebsd_acl_fixture(scratch: &Scratch) -> Option<bool> {
    let kind =
        crate::platform::freebsd_fs::test_acl_is_nfs4(&File::open(&scratch.0).unwrap()).unwrap();
    if kind.is_none() {
        assert_ne!(
            std::env::var("FASTEXEC_REQUIRE_ACL_TEST").as_deref(),
            Ok("1"),
            "the mandatory FreeBSD ACL fixture does not support ACLs"
        );
        eprintln!("ACL filesystem cases run in scripts/ci/freebsd-acl-tests.sh");
    }
    if kind.is_some() {
        // SAFETY: geteuid has no preconditions; UID 1 is the separate ACL test subject.
        assert_ne!(
            unsafe { libc::geteuid() },
            1,
            "the ACL fixture must not run as its peer UID"
        );
    }
    kind
}

#[cfg(target_os = "freebsd")]
fn freebsd_set_acl(path: &Path, args: &[&str]) {
    let output = std::process::Command::new("setfacl")
        .args(args)
        .arg(path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "setfacl failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(target_os = "freebsd")]
fn freebsd_inherit_acl(path: &Path, nfs4: bool) {
    if nfs4 {
        freebsd_set_acl(
            path,
            &[
                "-a",
                "0",
                "owner@:rwxpDdaARWcCos:fd:allow,user:1:rwxpDdaARWcCos:fd:allow",
            ],
        );
    } else {
        freebsd_set_acl(path, &["-d", "-m", "u::rwx,u:1:rwx,g::r-x,m::rwx,o::---"]);
    }
}

#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_user_attributes_survive_write_and_edit() {
    use std::os::fd::AsRawFd;
    let scratch = Scratch::new("save-freebsd-attributes");
    let path = scratch.file("original.txt", b"original");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let value = b"binary\0attribute\xff";
    // SAFETY: the descriptor, terminated name and byte buffer are live.
    assert_eq!(
        unsafe {
            libc::extattr_set_fd(
                file.as_raw_fd(),
                libc::EXTATTR_NAMESPACE_USER,
                c"fastexec-test".as_ptr(),
                value.as_ptr().cast(),
                value.len(),
            )
        },
        value.len() as isize
    );
    save_text(&path, b"replacement").unwrap();
    let launch = context(&scratch.0, &scratch.0);
    change("original.txt", &[("replacement", "edited")], &launch).unwrap();
    let saved = File::open(&path).unwrap();
    let mut actual = [0u8; 64];
    // SAFETY: the live descriptor names the replacement and the output buffer is valid.
    let count = unsafe {
        libc::extattr_get_fd(
            saved.as_raw_fd(),
            libc::EXTATTR_NAMESPACE_USER,
            c"fastexec-test".as_ptr(),
            actual.as_mut_ptr().cast(),
            actual.len(),
        )
    };
    assert_eq!(count, value.len() as isize);
    assert_eq!(&actual[..value.len()], value);
    assert_eq!(scratch.read("original.txt"), b"edited");
    assert_eq!(
        std::fs::read_dir(&scratch.0).unwrap().count(),
        1,
        "staging leaked"
    );
}

#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_acl_original_survives_write_and_edit() {
    let scratch = Scratch::new("save-freebsd-acl");
    let Some(nfs4) = freebsd_acl_fixture(&scratch) else {
        return;
    };
    let path = scratch.file("original.txt", b"original");
    if nfs4 {
        freebsd_set_acl(&path, &["-a", "0", "user:1:r-----a-R-c--s::allow"]);
    } else {
        freebsd_set_acl(&path, &["-m", "u:1:r--,m::r--"]);
    }
    let original = File::open(&path).unwrap();
    save_text(&path, b"replacement").unwrap();
    let launch = context(&scratch.0, &scratch.0);
    change("original.txt", &[("replacement", "edited")], &launch).unwrap();
    assert!(
        crate::platform::freebsd_fs::test_equal_acl(&original, &File::open(&path).unwrap())
            .unwrap()
    );
    assert_eq!(scratch.read("original.txt"), b"edited");
}

#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_acl_staging_is_private_and_new_files_keep_the_default_acl() {
    use std::os::unix::fs::PermissionsExt;
    let scratch = Scratch::new("save-freebsd-inherit");
    let Some(nfs4) = freebsd_acl_fixture(&scratch) else {
        return;
    };
    freebsd_inherit_acl(&scratch.0, nfs4);
    let probe = Staged::new(&scratch.0, false).unwrap();
    let private = Staged::new(&scratch.0, true).unwrap();
    let directory = private.directory.as_ref().unwrap();
    crate::platform::runtime::create_private_unix_check(directory).unwrap();
    assert_eq!(
        private
            .file
            .as_ref()
            .unwrap()
            .metadata()
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let complete = scratch.0.join("complete.txt");
    save_text(&complete, b"complete content").unwrap();
    assert!(
        crate::platform::freebsd_fs::test_equal_acl(
            probe.file.as_ref().unwrap(),
            &File::open(&complete).unwrap(),
        )
        .unwrap()
    );
    assert_eq!(std::fs::read(&complete).unwrap(), b"complete content");
    drop(private);
    drop(probe);
    assert_eq!(
        std::fs::read_dir(&scratch.0).unwrap().count(),
        1,
        "staging leaked"
    );
}

#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_acl_existing_runtime_policy_is_refused_without_changing_it() {
    let scratch = Scratch::new("runtime-freebsd-existing-acl");
    let Some(nfs4) = freebsd_acl_fixture(&scratch) else {
        return;
    };
    let runtime = scratch.0.join("runtime");
    crate::platform::runtime::create_private_new(&runtime).unwrap();
    if nfs4 {
        freebsd_set_acl(&runtime, &["-a", "0", "user:1:r-x---a-R-c--s::allow"]);
    } else {
        freebsd_set_acl(&runtime, &["-m", "u:1:r-x,m::---"]);
    }
    let listing = |path: &Path| {
        let output = std::process::Command::new("getfacl")
            .arg("-q")
            .arg(path)
            .output()
            .unwrap();
        assert!(output.status.success());
        output.stdout
    };
    let before = listing(&runtime);
    assert!(crate::platform::runtime::create_private(&runtime).is_err());
    assert_eq!(listing(&runtime), before);
}

#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_acl_new_runtime_directories_block_inheritance() {
    let scratch = Scratch::new("runtime-freebsd-inherit");
    let Some(nfs4) = freebsd_acl_fixture(&scratch) else {
        return;
    };
    freebsd_inherit_acl(&scratch.0, nfs4);
    let middle = scratch.0.join("middle");
    let leaf = middle.join("runtime");
    crate::platform::runtime::create_private(&leaf).unwrap();
    crate::platform::runtime::create_private_unix_check(&middle).unwrap();
    crate::platform::runtime::create_private_unix_check(&leaf).unwrap();
    let child = leaf.join("child");
    crate::platform::runtime::create_private_new(&child).unwrap();
    crate::platform::runtime::create_private_unix_check(&child).unwrap();
}

#[cfg(target_os = "freebsd")]
#[test]
fn freebsd_acl_an_inaccessible_new_directory_is_refused_before_use() {
    let scratch = Scratch::new("runtime-freebsd-denied-inherit");
    let Some(nfs4) = freebsd_acl_fixture(&scratch) else {
        return;
    };
    let require_denied = std::env::var("FASTEXEC_REQUIRE_DENIED_ACL_TEST").as_deref() == Ok("1");
    if !nfs4 {
        assert!(
            !require_denied,
            "the required denied-ACL fixture needs NFSv4 ACLs"
        );
        return;
    }
    let target = scratch.file("original.txt", b"original");
    let original = File::open(&target).unwrap();
    freebsd_set_acl(&scratch.0, &["-a", "0", "owner@:r-x-----------:di:deny"]);
    // UFS may constrain inherited denies by the requested mode; ZFS restricted preserves them.
    let probe = scratch.0.join("probe");
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&probe)
        .unwrap();
    let readable = File::open(&probe).is_ok();
    if require_denied {
        assert!(
            !readable,
            "the mandatory non-root restricted fixture must deny directory reads"
        );
    }
    let denied = scratch.0.join("denied");
    let result = crate::platform::runtime::create_private_new(&denied);
    if readable {
        result.unwrap();
        crate::platform::runtime::create_private_unix_check(&denied).unwrap();
        save_text(&target, b"replacement").unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"replacement");
    } else {
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
        assert!(
            !denied.exists(),
            "an unusable empty directory was not cleaned up"
        );
        assert!(save_text(&target, b"replacement").is_err());
        assert_eq!(std::fs::read(&target).unwrap(), b"original");
        assert!(
            crate::platform::freebsd_fs::test_equal_acl(&original, &File::open(&target).unwrap())
                .unwrap()
        );
        assert_eq!(
            std::fs::read_dir(&scratch.0).unwrap().count(),
            2,
            "failed staging leaked"
        );
    }
}

#[cfg(target_os = "macos")]
fn macos_chmod_acl(path: &Path, entry: Option<&str>) {
    let mut command = std::process::Command::new("/bin/chmod");
    if let Some(entry) = entry {
        command.args(["+a", entry]);
    } else {
        command.arg("-N");
    }
    let output = command.arg(path).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(target_os = "macos")]
#[test]
fn macos_staging_rejects_parent_acl_inheritance_at_creation() {
    use std::os::unix::fs::MetadataExt;
    let scratch = Scratch::new("save-macos-private-acl");
    macos_chmod_acl(&scratch.0, Some("everyone allow read,file_inherit"));
    let ordinary_path = scratch.0.join("ordinary");
    let ordinary = create_staging_file(&ordinary_path, false).unwrap();
    assert!(
        macos_access::has_entries(&ordinary).unwrap(),
        "fixture must inherit its parent ACL"
    );
    let private_path = scratch.0.join("private");
    let private = create_staging_file(&private_path, true).unwrap();
    assert_eq!(private.metadata().unwrap().mode() & 0o077, 0);
    assert!(!macos_access::has_entries(&private).unwrap());
    // Final new files acquire ordinary defaults only after staging has finished.
    let final_path = scratch.0.join("final");
    save_text(&final_path, b"complete").unwrap();
    let final_file = File::open(&final_path).unwrap();
    assert_eq!(
        macos_access::acl_text(&final_file).unwrap(),
        macos_access::acl_text(&ordinary).unwrap()
    );
    assert_eq!(std::fs::read(final_path).unwrap(), b"complete");
}

#[cfg(target_os = "macos")]
#[test]
fn macos_atomic_save_preserves_an_explicit_deny_acl() {
    let scratch = Scratch::new("save-macos-deny-acl");
    let path = scratch.file("target", b"original");
    macos_chmod_acl(&path, Some("everyone deny read"));
    let original = OpenOptions::new().write(true).open(&path).unwrap();
    let before = macos_access::acl_text(&original).unwrap();
    assert!(before.contains("deny"));
    save_text(&path, b"replacement").unwrap();
    let replaced = OpenOptions::new().write(true).open(&path).unwrap();
    assert_eq!(macos_access::acl_text(&replaced).unwrap(), before);
    // Remove the test's deny entry only after checking it survived the replacement.
    macos_chmod_acl(&path, None);
    assert_eq!(std::fs::read(path).unwrap(), b"replacement");
}

#[cfg(target_os = "macos")]
#[test]
fn macos_atomic_save_does_not_add_parent_grants_to_a_file_without_an_acl() {
    let scratch = Scratch::new("save-macos-no-acl");
    macos_chmod_acl(&scratch.0, Some("everyone allow read,file_inherit"));
    let path = scratch.file("target", b"original");
    macos_chmod_acl(&path, None);
    let original = File::open(&path).unwrap();
    assert!(!macos_access::has_entries(&original).unwrap());
    save_text(&path, b"replacement").unwrap();
    let replaced = File::open(&path).unwrap();
    assert!(!macos_access::has_entries(&replaced).unwrap());
    assert_eq!(std::fs::read(path).unwrap(), b"replacement");
}

#[cfg(windows)]
fn windows_security_text(path: &Path) -> String {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::{ERROR_SUCCESS, HLOCAL, LocalFree};
    use windows_sys::Win32::Security::Authorization::{
        ConvertSecurityDescriptorToStringSecurityDescriptorW, GetSecurityInfo, SDDL_REVISION_1,
        SE_FILE_OBJECT,
    };
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, GROUP_SECURITY_INFORMATION, OWNER_SECURITY_INFORMATION,
    };
    struct Allocation(HLOCAL);
    impl Drop for Allocation {
        fn drop(&mut self) {
            // SAFETY: both security APIs below return LocalAlloc-owned buffers.
            unsafe { LocalFree(self.0) };
        }
    }
    let file = File::open(path).unwrap();
    let mut descriptor = std::ptr::null_mut();
    let information =
        OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION;
    // SAFETY: the file and the descriptor output live for this synchronous query.
    let result = unsafe {
        GetSecurityInfo(
            file.as_raw_handle(),
            SE_FILE_OBJECT,
            information,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut descriptor,
        )
    };
    assert_eq!(result, ERROR_SUCCESS);
    let _descriptor = Allocation(descriptor);
    let (mut text, mut length) = (std::ptr::null_mut(), 0u32);
    // SAFETY: descriptor is live, and both outputs are writable.
    assert_ne!(
        unsafe {
            ConvertSecurityDescriptorToStringSecurityDescriptorW(
                descriptor,
                SDDL_REVISION_1,
                information,
                &mut text,
                &mut length,
            )
        },
        0
    );
    let _text = Allocation(text.cast());
    // SAFETY: the conversion returned length UTF-16 units, including the terminator.
    let units = unsafe { std::slice::from_raw_parts(text, length as usize) };
    String::from_utf16(units.strip_suffix(&[0]).unwrap_or(units)).unwrap()
}

/// Windows may set the auto-inheritance bookkeeping flag when applying a DACL, without
/// changing owner, group, protection, or any ACE. Compare all of those security properties.
#[cfg(windows)]
fn windows_without_auto_inherited(sddl: &str) -> String {
    let Some(start) = sddl.find("D:") else {
        return sddl.to_owned();
    };
    let flags = start + 2;
    let end = sddl[flags..].find('(').map_or(sddl.len(), |at| flags + at);
    format!(
        "{}{}{}",
        &sddl[..flags],
        sddl[flags..end].replace("AI", ""),
        &sddl[end..]
    )
}

#[cfg(windows)]
#[test]
fn windows_atomic_save_preserves_owner_group_dacl_and_named_streams() {
    let scratch = Scratch::new("save-windows-metadata");
    let target = scratch.0.join("target.txt");
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Storage::FileSystem::GetVolumeInformationByHandleW;
    let mut original = create_staging_file(&target, true).unwrap();
    original.write_all(b"original").unwrap();
    let mut flags = 0u32;
    // SAFETY: only flags are requested from the volume holding this live file handle.
    assert_ne!(
        unsafe {
            GetVolumeInformationByHandleW(
                original.as_raw_handle(),
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut flags,
                std::ptr::null_mut(),
                0,
            )
        },
        0
    );
    drop(original);
    let stream = PathBuf::from(format!("{}:fastexec-test", target.display()));
    let named_streams = flags & 0x00040000 != 0;
    if named_streams {
        std::fs::write(&stream, b"a named stream").unwrap();
    }
    let before = (flags & 0x00000008 != 0).then(|| windows_security_text(&target));
    save_text(&target, b"replacement").unwrap();
    assert_eq!(std::fs::read(&target).unwrap(), b"replacement");
    if named_streams {
        assert_eq!(std::fs::read(&stream).unwrap(), b"a named stream");
    }
    if let Some(before) = before {
        assert_eq!(
            windows_without_auto_inherited(&windows_security_text(&target)),
            windows_without_auto_inherited(&before)
        );
    }
    assert_eq!(std::fs::read_dir(&scratch.0).unwrap().count(), 1);
}

#[cfg(windows)]
#[test]
fn windows_publish_checks_the_open_originals_identity() {
    let scratch = Scratch::new("save-windows-identity");
    let target = scratch.file("target.txt", b"original");
    let original = File::open(&target).unwrap();
    unchanged_windows_target(&original, &target).unwrap();
    std::fs::rename(&target, scratch.0.join("retired.txt")).unwrap();
    std::fs::write(&target, b"a different file").unwrap();
    let error = unchanged_windows_target(&original, &target).unwrap_err();
    assert!(error.to_string().contains("target changed"), "{error}");
    assert_eq!(scratch.read("target.txt"), b"a different file");
    assert_eq!(scratch.read("retired.txt"), b"original");
}

#[test]
fn replacement_backup_is_removed_after_success() {
    let scratch = Scratch::new("replace-backup-success");
    let target = scratch.file("target.txt", b"original");
    let staged = scratch.file("staged.txt", b"replacement");
    replace_with_backup(
        &target,
        |backup| {
            std::fs::rename(&target, backup)?;
            std::fs::rename(&staged, &target)
        },
        |_| panic!("successful publication must not roll back"),
    )
    .unwrap();
    assert_eq!(scratch.read("target.txt"), b"replacement");
    assert_eq!(std::fs::read_dir(&scratch.0).unwrap().count(), 1);
}

#[test]
fn replacement_failure_before_move_keeps_the_original() {
    for code in [1175, 1176, 5] {
        let scratch = Scratch::new(&format!("replace-backup-failed-{code}"));
        let target = scratch.file("target.txt", b"original");
        let staged = scratch.file("staged.txt", b"replacement");
        let error = replace_with_backup(
            &target,
            |_| Err(io::Error::from_raw_os_error(code)),
            |_| panic!("this failure leaves the original in place"),
        )
        .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(code));
        assert_eq!(scratch.read("target.txt"), b"original");
        assert_eq!(std::fs::read(staged).unwrap(), b"replacement");
        assert_eq!(std::fs::read_dir(&scratch.0).unwrap().count(), 2);
    }
}

#[test]
fn replacement_failure_after_move_restores_the_original() {
    let scratch = Scratch::new("replace-backup-restored");
    let target = scratch.file("target.txt", b"original");
    let staged = scratch.file("staged.txt", b"replacement");
    let error = replace_with_backup(
        &target,
        |backup| {
            std::fs::rename(&target, backup)?;
            Err(io::Error::from_raw_os_error(1177))
        },
        |backup| publish(backup, &target, false),
    )
    .unwrap_err();
    assert_eq!(error.raw_os_error(), Some(1177));
    assert_eq!(scratch.read("target.txt"), b"original");
    assert_eq!(std::fs::read(staged).unwrap(), b"replacement");
    assert_eq!(std::fs::read_dir(&scratch.0).unwrap().count(), 2);
}

#[test]
fn failed_replacement_and_rollback_keep_a_recoverable_original() {
    let scratch = Scratch::new("replace-backup-retained");
    let target = scratch.file("target.txt", b"original");
    let mut staged = Staged::new(&scratch.0, true).unwrap();
    staged
        .file
        .as_mut()
        .unwrap()
        .write_all(b"replacement")
        .unwrap();
    let recovery = std::cell::RefCell::new(PathBuf::new());
    let error = replace_with_backup(
        &target,
        |backup| {
            *recovery.borrow_mut() = backup.to_path_buf();
            std::fs::rename(&target, backup)?;
            Err(io::Error::from_raw_os_error(1177))
        },
        |_| {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "injected rollback failure",
            ))
        },
    )
    .unwrap_err();
    drop(staged);
    assert!(!target.exists());
    let recovery = recovery.into_inner();
    assert_eq!(std::fs::read(&recovery).unwrap(), b"original");
    assert!(
        error.to_string().contains(&display_path(&recovery)),
        "{error}"
    );
    assert_eq!(std::fs::read_dir(&scratch.0).unwrap().count(), 1);
}

#[test]
fn replacement_rollback_does_not_clobber_a_concurrent_target() {
    let scratch = Scratch::new("replace-backup-concurrent");
    let target = scratch.file("target.txt", b"original");
    let recovery = std::cell::RefCell::new(PathBuf::new());
    let error = replace_with_backup(
        &target,
        |backup| {
            *recovery.borrow_mut() = backup.to_path_buf();
            std::fs::rename(&target, backup)?;
            std::fs::write(&target, b"a concurrent writer")?;
            Err(io::Error::from_raw_os_error(1177))
        },
        |backup| publish(backup, &target, false),
    )
    .unwrap_err();
    assert_eq!(scratch.read("target.txt"), b"a concurrent writer");
    let recovery = recovery.into_inner();
    assert_eq!(std::fs::read(&recovery).unwrap(), b"original");
    assert!(
        error.to_string().contains(&display_path(&recovery)),
        "{error}"
    );
}

#[test]
fn exclusive_publish_does_not_replace_a_raced_target() {
    let scratch = Scratch::new("exclusive-publish");
    let staged = scratch.file("staged.txt", b"new");
    let target = scratch.file("target.txt", b"another writer");
    assert!(publish(&staged, &target, false).is_err());
    assert_eq!(scratch.read("target.txt"), b"another writer");
    assert_eq!(scratch.read("staged.txt"), b"new");
}

#[cfg(unix)]
#[test]
fn atomic_saves_refuse_special_files_without_opening_them() {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::FileTypeExt;
    let scratch = Scratch::new("save-special");
    let fifo = scratch.0.join("fifo");
    let name = CString::new(fifo.as_os_str().as_bytes()).unwrap();
    // SAFETY: the terminated path is owned by the test.
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    assert!(save_text(&fifo, b"must not replace the FIFO").is_err());
    assert!(
        std::fs::symlink_metadata(&fifo)
            .unwrap()
            .file_type()
            .is_fifo()
    );
    assert!(save_text(&scratch.0, b"must not replace the directory").is_err());
    assert!(scratch.0.is_dir());
}

#[cfg(unix)]
#[test]
fn privileged_save_preserves_owner_group_and_mode() {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    // SAFETY: getuid has no side effects.
    if unsafe { libc::geteuid() } != 0 {
        return;
    }
    let scratch = Scratch::new("save-owner");
    let path = scratch.file("owned.txt", b"original");
    let file = File::open(&path).unwrap();
    // SAFETY: only this test's file is changed; the numeric IDs need no account lookup.
    assert_eq!(unsafe { libc::fchown(file.as_raw_fd(), 65534, 65534) }, 0);
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
    save_text(&path, b"changed").unwrap();
    let metadata = std::fs::metadata(&path).unwrap();
    assert_eq!(
        (metadata.uid(), metadata.gid(), metadata.mode() & 0o777),
        (65534, 65534, 0o640)
    );
}

#[cfg(target_os = "linux")]
fn set_test_xattr(path: &Path, name: &std::ffi::CStr, value: &[u8]) {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let path = CString::new(path.as_os_str().as_bytes()).unwrap();
    // SAFETY: all buffers and terminated strings remain live for the call.
    assert_eq!(
        unsafe {
            libc::setxattr(
                path.as_ptr(),
                name.as_ptr(),
                value.as_ptr().cast(),
                value.len(),
                0,
            )
        },
        0,
        "{}",
        io::Error::last_os_error()
    );
}

#[cfg(target_os = "linux")]
fn test_xattr(path: &Path, name: &std::ffi::CStr) -> Option<Vec<u8>> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let path = CString::new(path.as_os_str().as_bytes()).unwrap();
    let mut value = vec![0u8; 1024];
    // SAFETY: the value buffer is writable and the strings are terminated.
    let size = unsafe {
        libc::getxattr(
            path.as_ptr(),
            name.as_ptr(),
            value.as_mut_ptr().cast(),
            value.len(),
        )
    };
    if size < 0 {
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(libc::ENODATA)
        );
        return None;
    }
    value.truncate(size as usize);
    Some(value)
}

#[cfg(target_os = "linux")]
fn test_acl() -> Vec<u8> {
    // Linux POSIX ACL xattr version 2: owner, named user, group, mask, other.
    let mut acl = 2u32.to_le_bytes().to_vec();
    for (tag, permissions, id) in [
        (1u16, 7u16, u32::MAX),
        (2, 6, 65534),
        (4, 0, u32::MAX),
        (16, 6, u32::MAX),
        (32, 0, u32::MAX),
    ] {
        acl.extend_from_slice(&tag.to_le_bytes());
        acl.extend_from_slice(&permissions.to_le_bytes());
        acl.extend_from_slice(&id.to_le_bytes());
    }
    acl
}

#[cfg(target_os = "linux")]
#[test]
fn save_xattrs_hold_both_name_lists_and_current_value() {
    use std::os::fd::AsRawFd;
    let scratch = Scratch::new("save-xattr-budget");
    let source_path = scratch.file("source.txt", b"original");
    let staged_path = scratch.file("staged.txt", b"candidate");
    let name = c"user.fastexec-budget";
    let inherited_name = c"user.fastexec-inherited";
    let value = b"small retained value";
    set_test_xattr(&source_path, name, value);
    let source = File::open(&source_path).unwrap();
    let staged = File::open(&staged_path).unwrap();
    let list_size = |file: &File| {
        // SAFETY: a null buffer and zero length query only the required size.
        let count = unsafe { libc::flistxattr(file.as_raw_fd(), std::ptr::null_mut(), 0) };
        assert!(count >= 0);
        count as usize
    };
    set_test_xattr(&staged_path, inherited_name, b"remove");
    let required = list_size(&source) + list_size(&staged) + value.len();
    drop(staged);
    for cap in [required - 1, required, required + 1] {
        // Recreate the original name list for each admission boundary.
        std::fs::remove_file(&staged_path).unwrap();
        scratch.file("staged.txt", b"candidate");
        set_test_xattr(&staged_path, inherited_name, b"remove");
        let staged = File::open(&staged_path).unwrap();
        let pool = BytePool::new(cap);
        let result = preserve_xattrs(&source, &staged, &pool);
        if cap < required {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::OutOfMemory);
        } else {
            result.unwrap();
            assert_eq!(test_xattr(&staged_path, name), Some(value.to_vec()));
            assert_eq!(test_xattr(&staged_path, inherited_name), None);
        }
        assert_eq!(test_xattr(&source_path, name), Some(value.to_vec()));
        assert_eq!(scratch.read("source.txt"), b"original");
        assert_eq!(pool.used(), 0);
    }
    let pool = BytePool::new(0);
    let error = save_target(&source_path, b"changed", None, &pool).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
    assert_eq!(scratch.read("source.txt"), b"original");
    assert_eq!(test_xattr(&source_path, name), Some(value.to_vec()));
    assert_eq!(pool.used(), 0);
}

#[cfg(windows)]
#[test]
fn save_windows_descriptor_admits_both_rounded_buffers() {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::ERROR_INSUFFICIENT_BUFFER;
    use windows_sys::Win32::Security::{
        DACL_SECURITY_INFORMATION, GROUP_SECURITY_INFORMATION, GetKernelObjectSecurity,
        OWNER_SECURITY_INFORMATION,
    };
    let scratch = Scratch::new("save-descriptor-budget");
    let path = scratch.file("source.txt", b"original");
    let source = File::open(&path).unwrap();
    let staged = Staged::new(&scratch.0, true).unwrap();
    let file = staged.file.as_ref().unwrap();
    let ownership = OWNER_SECURITY_INFORMATION | GROUP_SECURITY_INFORMATION;
    let size = |file: &File, information| {
        let mut needed = 0u32;
        // SAFETY: the zero-length query cannot write a descriptor.
        let result = unsafe {
            GetKernelObjectSecurity(
                file.as_raw_handle(),
                information,
                std::ptr::null_mut(),
                0,
                &mut needed,
            )
        };
        assert_eq!(result, 0);
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(ERROR_INSUFFICIENT_BUFFER as i32)
        );
        let unit = std::mem::size_of::<usize>();
        (needed as usize).div_ceil(unit) * unit
    };
    let required = size(&source, ownership | DACL_SECURITY_INFORMATION) + size(file, ownership);
    for cap in [required - 1, required, required + 1] {
        let pool = BytePool::new(cap);
        let result = preserve_windows_access(&source, file, true, &pool);
        if cap < required {
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::OutOfMemory);
        } else {
            result.unwrap();
        }
        assert_eq!(scratch.read("source.txt"), b"original");
        assert_eq!(pool.used(), 0);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn atomic_save_preserves_existing_acl_and_xattrs() {
    let scratch = Scratch::new("save-attributes");
    let path = scratch.file("target.txt", b"original");
    let acl = test_acl();
    set_test_xattr(&path, c"user.fastexec-test", b"retained");
    set_test_xattr(&path, c"system.posix_acl_access", &acl);
    save_text(&path, b"changed").unwrap();
    assert_eq!(
        test_xattr(&path, c"user.fastexec-test"),
        Some(b"retained".to_vec())
    );
    assert_eq!(test_xattr(&path, c"system.posix_acl_access"), Some(acl));
}

#[cfg(target_os = "linux")]
#[test]
fn atomic_save_does_not_gain_the_directories_default_acl() {
    use std::os::unix::fs::PermissionsExt;
    let scratch = Scratch::new("save-inherited-acl");
    let path = scratch.file("target.txt", b"original");
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
    assert!(test_xattr(&path, c"system.posix_acl_access").is_none());
    set_test_xattr(&scratch.0, c"system.posix_acl_default", &test_acl());
    save_text(&path, b"changed").unwrap();
    assert!(test_xattr(&path, c"system.posix_acl_access").is_none());
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o640
    );
}

#[test]
fn regression_edit_preserves_unmatched_line_endings() {
    let scratch = Scratch::new("mixed-endings");
    let launch = context(&scratch.0, &scratch.0);
    let original = b"\xef\xbb\xbffirst\r\nneedle\r\nlast\ncarriage\rreturn\n";
    scratch.file("mixed.txt", original);
    change("mixed.txt", &[("needle", "CHANGED")], &launch).unwrap();
    assert_eq!(
        scratch.read("mixed.txt"),
        b"\xef\xbb\xbffirst\r\nCHANGED\r\nlast\ncarriage\rreturn\n"
    );
    scratch.file("cr.txt", b"one\rtwo\rthree\r");
    let answer = change("cr.txt", &[("two", "TWO")], &launch).unwrap();
    assert!(answer.ends_with("line 1"), "{answer}");
    assert_eq!(scratch.read("cr.txt"), b"one\rTWO\rthree\r");
}

#[cfg(unix)]
fn failed_save_preserves_original(operation: &str) {
    use std::os::unix::process::CommandExt;
    let scratch = Scratch::new(&format!("failed-{operation}"));
    let original = format!("{}needle\n", "original ".repeat(50));
    let path = scratch.file("target.txt", original.as_bytes());
    let mut worker = std::process::Command::new(std::env::current_exe().unwrap());
    worker
        .arg("--exact")
        .arg("files::tests::failed_save_worker")
        .arg("--nocapture")
        .env("FASTEXEC_TEST_SAVE_FAILURE_PATH", &path)
        .env("FASTEXEC_TEST_SAVE_FAILURE_OPERATION", operation);
    // The limit belongs only to a fresh test child, never to other tests or the MCP host.
    unsafe {
        worker.pre_exec(|| {
            libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
            let limit = libc::rlimit {
                rlim_cur: 1024,
                rlim_max: 1024,
            };
            if libc::setrlimit(libc::RLIMIT_FSIZE, &limit) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let output = worker.output().unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        scratch.read("target.txt"),
        original.as_bytes(),
        "{operation} reported an error but changed the original file"
    );
    assert_eq!(
        std::fs::read_dir(&scratch.0).unwrap().count(),
        1,
        "no staging file is left"
    );
}

#[cfg(unix)]
#[test]
fn failed_save_worker() {
    let Some(path) = std::env::var_os("FASTEXEC_TEST_SAVE_FAILURE_PATH") else {
        return;
    };
    let path = PathBuf::from(path);
    let launch = context(path.parent().unwrap(), path.parent().unwrap());
    let operation = std::env::var("FASTEXEC_TEST_SAVE_FAILURE_OPERATION").unwrap();
    let result = if operation == "write" {
        write_text(
            &WriteRequest {
                path: path.display().to_string(),
                content: "N".repeat(2048),
            },
            &launch,
        )
        .map(|(text, _)| text)
    } else {
        change(
            &path.display().to_string(),
            &[("needle", &"N".repeat(2048))],
            &launch,
        )
    };
    assert!(
        result.is_err(),
        "the OS file-size limit must reject the save"
    );
}

#[cfg(unix)]
#[test]
fn regression_failed_write_preserves_original() {
    failed_save_preserves_original("write");
}

#[cfg(unix)]
#[test]
fn regression_failed_edit_preserves_original() {
    failed_save_preserves_original("edit");
}

#[test]
fn edit_replaces_exact_text_and_names_the_line() {
    let scratch = Scratch::new("exact");
    let launch = context(&scratch.0, &scratch.0);
    let shown = display_path(&scratch.0.join("a.txt"));
    scratch.file("a.txt", b"one\ntwo\nthree\n");
    assert_eq!(
        change("a.txt", &[("two", "TWO")], &launch).unwrap(),
        format!("Successfully replaced 1 block(s) in {shown}.\nedits[0]: line 2")
    );
    assert_eq!(scratch.read("a.txt"), b"one\nTWO\nthree\n");
}

#[test]
fn edit_reports_original_lines_after_an_earlier_edit_adds_lines() {
    let scratch = Scratch::new("shift");
    let launch = context(&scratch.0, &scratch.0);
    let shown = display_path(&scratch.0.join("a.txt"));
    scratch.file("a.txt", b"1\n2\n3\n4\n");
    assert_eq!(
        change("a.txt", &[("2", "2\n2b\n2c"), ("4", "4x")], &launch).unwrap(),
        format!("Successfully replaced 2 block(s) in {shown}.\nedits[0]: line 2\nedits[1]: line 4")
    );
    assert_eq!(scratch.read("a.txt"), b"1\n2\n2b\n2c\n3\n4x\n");
}

#[test]
fn edit_reports_original_lines_after_deletion_and_bom_restoration() {
    let scratch = Scratch::new("original-lines");
    let launch = context(&scratch.0, &scratch.0);
    scratch.file(
        "mixed.txt",
        "\u{feff}before\r\nmiddle\ron same\nlast\r\n".as_bytes(),
    );
    let (_, result) = edit_outcome(
        &EditRequest {
            path: "mixed.txt".into(),
            edits: vec![
                Edit {
                    old_text: "last".into(),
                    new_text: "LAST".into(),
                    start_line: None,
                    end_line: None,
                },
                Edit {
                    old_text: "\u{feff}before\n".into(),
                    new_text: String::new(),
                    start_line: None,
                    end_line: None,
                },
            ],
        },
        &launch,
    )
    .unwrap();
    assert_eq!(
        result["edits"],
        json!([{"line": 3, "fuzzy": false}, {"line": 1, "fuzzy": false}])
    );
    assert_eq!(
        scratch.read("mixed.txt"),
        "\u{feff}middle\ron same\nLAST\r\n".as_bytes()
    );
}

#[test]
fn edit_refuses_input_it_cannot_apply() {
    let scratch = Scratch::new("refuse");
    let launch = context(&scratch.0, &scratch.0);
    let shown_a = display_path(&scratch.0.join("a.txt"));
    let shown_u = display_path(&scratch.0.join("u.txt"));
    let shown_b = display_path(&scratch.0.join("b.txt"));
    scratch.file("a.txt", b"x\nx\n");
    assert_eq!(
        change("a.txt", &[], &launch).unwrap_err(),
        "Edit tool input is invalid. edits must contain at least one replacement."
    );
    assert_eq!(
        change("a.txt", &[("", "y")], &launch).unwrap_err(),
        "edits[0].oldText must not be empty."
    );
    assert_eq!(
        change("a.txt", &[("zzz", "y")], &launch).unwrap_err(),
        format!(
            "Cannot edit {shown_a}; nothing was changed:\n- edits[0]: oldText was not found, even ignoring trailing whitespace, Unicode compatibility forms, curly quotes, dashes, and special spaces."
        )
    );
    assert_eq!(
        change("a.txt", &[("zzz", "y"), ("qqq", "z")], &launch).unwrap_err(),
        format!(
            "Cannot edit {shown_a}; nothing was changed:\n- edits[0]: oldText was not found, even ignoring trailing whitespace, Unicode compatibility forms, curly quotes, dashes, and special spaces.\n- edits[1]: oldText was not found, even ignoring trailing whitespace, Unicode compatibility forms, curly quotes, dashes, and special spaces."
        )
    );
    assert_eq!(
        change("a.txt", &[("x", "y")], &launch).unwrap_err(),
        format!(
            "Cannot edit {shown_a}; nothing was changed:\n- edits[0]: oldText occurs 2 times, starting at lines 1, 2; add surrounding text to make it unique."
        )
    );
    assert_eq!(
        change("a.txt", &[("x", "y"), ("x", "z")], &launch).unwrap_err(),
        format!(
            "Cannot edit {shown_a}; nothing was changed:\n- edits[0]: oldText occurs 2 times, starting at lines 1, 2; add surrounding text to make it unique.\n- edits[1]: oldText occurs 2 times, starting at lines 1, 2; add surrounding text to make it unique."
        )
    );
    scratch.file("u.txt", b"unique\n");
    assert_eq!(
        change("u.txt", &[("unique", "unique")], &launch).unwrap_err(),
        format!(
            "Cannot edit {shown_u}; nothing was changed: the replacement produced identical content."
        )
    );
    scratch.file("b.txt", b"abc\n");
    assert_eq!(
        change("b.txt", &[("ab", "X"), ("bc", "Y")], &launch).unwrap_err(),
        format!(
            "Cannot edit {shown_b}; nothing was changed:\n- edits[0] overlaps edits[1]; merge them into one edit or target disjoint regions.\n- edits[1] overlaps edits[0]; merge them into one edit or target disjoint regions."
        )
    );
    // Nothing above wrote anything.
    assert_eq!(scratch.read("a.txt"), b"x\nx\n");
    assert_eq!(scratch.read("b.txt"), b"abc\n");
}

#[test]
fn edit_keeps_crlf_and_a_bom() {
    let scratch = Scratch::new("endings");
    let launch = context(&scratch.0, &scratch.0);
    scratch.file("crlf.txt", b"one\r\ntwo\r\n");
    change("crlf.txt", &[("two", "TWO")], &launch).unwrap();
    assert_eq!(scratch.read("crlf.txt"), b"one\r\nTWO\r\n");
    scratch.file("bom.txt", "\u{feff}one\ntwo\n".as_bytes());
    change("bom.txt", &[("two", "TWO")], &launch).unwrap();
    assert_eq!(scratch.read("bom.txt"), "\u{feff}one\nTWO\n".as_bytes());
}

#[test]
fn edit_accepts_the_bom_returned_by_read() {
    let scratch = Scratch::new("read-edit-bom");
    let launch = context(&scratch.0, &scratch.0);
    scratch.file("bom.txt", "\u{feff}one\ntwo\n".as_bytes());
    let read_back = whole(&scratch.path("bom.txt"), &launch, false).unwrap();
    change(
        "bom.txt",
        &[(&read_back, &read_back.replace("one", "ONE"))],
        &launch,
    )
    .unwrap();
    assert_eq!(scratch.read("bom.txt"), "\u{feff}ONE\ntwo\n".as_bytes());
    change("bom.txt", &[("\u{feff}ONE", "one")], &launch).unwrap();
    assert_eq!(scratch.read("bom.txt"), "\u{feff}one\ntwo\n".as_bytes());
    assert!(change("bom.txt", &[("\u{feff}one", "one")], &launch).is_err());
    scratch.file("repeated.txt", "\u{feff}one\none\n".as_bytes());
    change("repeated.txt", &[("\u{feff}one", "\u{feff}ONE")], &launch).unwrap();
    assert_eq!(
        scratch.read("repeated.txt"),
        "\u{feff}ONE\none\n".as_bytes()
    );
    scratch.file("internal.txt", "\u{feff}one\nx\u{feff}two\n".as_bytes());
    change("internal.txt", &[("one", "ONE")], &launch).unwrap();
    assert_eq!(
        scratch.read("internal.txt"),
        "\u{feff}ONE\nx\u{feff}two\n".as_bytes()
    );
    scratch.file("only-bom.txt", "\u{feff}".as_bytes());
    change("only-bom.txt", &[("\u{feff}", "\u{feff}one")], &launch).unwrap();
    assert_eq!(scratch.read("only-bom.txt"), "\u{feff}one".as_bytes());
}

#[test]
fn edit_matches_a_typographic_copy_and_keeps_the_bytes_around_it() {
    let scratch = Scratch::new("fuzzy");
    let launch = context(&scratch.0, &scratch.0);
    let shown = display_path(&scratch.0.join("a.txt"));
    scratch.file(
        "a.txt",
        "note: \u{201c}keep\u{201d} it\u{2019}s here   \nsecond line\n".as_bytes(),
    );
    assert_eq!(
        change("a.txt", &[("it's here", "it's fine")], &launch).unwrap(),
        format!("Successfully replaced 1 block(s) in {shown}.\nedits[0]: line 1 (fuzzy match)")
    );
    // Only the matched bytes change: the quotes before them and the trailing whitespace stay.
    assert_eq!(
        scratch.read("a.txt"),
        "note: \u{201c}keep\u{201d} it's fine   \nsecond line\n".as_bytes()
    );
}

#[test]
fn edit_changes_only_the_bytes_a_fuzzy_match_names() {
    let scratch = Scratch::new("fuzzy-json");
    let launch = context(&scratch.0, &scratch.0);
    let shown = display_path(&scratch.0.join("case.json"));
    let original = "{\"quote\":\"She said \u{201c}hello\u{201d}\",\"author\":\"O\u{2019}Reilly\"}";
    scratch.file("case.json", original.as_bytes());
    assert_eq!(
        change("case.json", &[("O'Reilly", "Publisher")], &launch).unwrap(),
        format!("Successfully replaced 1 block(s) in {shown}.\nedits[0]: line 1 (fuzzy match)")
    );
    let edited = scratch.read("case.json");
    assert_eq!(
        edited,
        "{\"quote\":\"She said \u{201c}hello\u{201d}\",\"author\":\"Publisher\"}".as_bytes()
    );
    let parsed: serde_json::Value =
        serde_json::from_str(&String::from_utf8(edited).unwrap()).unwrap();
    assert_eq!(parsed["author"], "Publisher");
}

#[test]
fn edit_keeps_an_exact_edits_line_while_another_edit_needs_normalizing() {
    let scratch = Scratch::new("fuzzy-mixed");
    let launch = context(&scratch.0, &scratch.0);
    let shown = display_path(&scratch.0.join("a.txt"));
    scratch.file(
        "a.txt",
        "label = \"\u{2460}\"; enabled = false\nname = \u{201c}old\u{201d}   \n".as_bytes(),
    );
    assert_eq!(
        change(
            "a.txt",
            &[("false", "true"), ("\"old\"", "\"new\"")],
            &launch
        )
        .unwrap(),
        format!(
            "Successfully replaced 2 block(s) in {shown}.\nedits[0]: line 1\nedits[1]: line 2 (fuzzy match)"
        )
    );
    assert_eq!(
        scratch.read("a.txt"),
        "label = \"\u{2460}\"; enabled = true\nname = \"new\"   \n".as_bytes()
    );
}

#[test]
fn edit_refuses_a_needle_with_two_overlapping_starts() {
    let scratch = Scratch::new("overlap");
    let launch = context(&scratch.0, &scratch.0);
    let shown_a = display_path(&scratch.0.join("a.txt"));
    let shown_b = display_path(&scratch.0.join("b.txt"));
    let shown_c = display_path(&scratch.0.join("c.txt"));
    scratch.file("a.txt", b"banana\n");
    assert_eq!(
        change("a.txt", &[("ana", "X")], &launch).unwrap_err(),
        format!(
            "Cannot edit {shown_a}; nothing was changed:\n- edits[0]: oldText occurs 2 times, starting at lines 1, 1; add surrounding text to make it unique."
        )
    );
    assert_eq!(scratch.read("a.txt"), b"banana\n");
    scratch.file("b.txt", b"aaaa\n");
    assert_eq!(
        change("b.txt", &[("aaa", "X")], &launch).unwrap_err(),
        format!(
            "Cannot edit {shown_b}; nothing was changed:\n- edits[0]: oldText occurs 2 times, starting at lines 1, 1; add surrounding text to make it unique."
        )
    );
    assert_eq!(scratch.read("b.txt"), b"aaaa\n");
    scratch.file("c.txt", "\u{e9}\u{e9}\u{e9}\n".as_bytes());
    assert_eq!(
        change("c.txt", &[("\u{e9}\u{e9}", "X")], &launch).unwrap_err(),
        format!(
            "Cannot edit {shown_c}; nothing was changed:\n- edits[0]: oldText occurs 2 times, starting at lines 1, 1; add surrounding text to make it unique."
        )
    );
    assert_eq!(scratch.read("c.txt"), "\u{e9}\u{e9}\u{e9}\n".as_bytes());
}

#[test]
fn edit_replaces_a_whitespace_only_target() {
    let scratch = Scratch::new("whitespace");
    let launch = context(&scratch.0, &scratch.0);
    let shown_tab = display_path(&scratch.0.join("tab.txt"));
    let shown_space = display_path(&scratch.0.join("space.txt"));
    let shown_indent = display_path(&scratch.0.join("indent.txt"));
    let shown_none = display_path(&scratch.0.join("none.txt"));
    scratch.file("tab.txt", b"if x:\n\tfoo()\n");
    assert_eq!(
        change("tab.txt", &[("\t", "    ")], &launch).unwrap(),
        format!("Successfully replaced 1 block(s) in {shown_tab}.\nedits[0]: line 2")
    );
    assert_eq!(scratch.read("tab.txt"), b"if x:\n    foo()\n");
    scratch.file("space.txt", b"foo bar\n");
    assert_eq!(
        change("space.txt", &[(" ", "_")], &launch).unwrap(),
        format!("Successfully replaced 1 block(s) in {shown_space}.\nedits[0]: line 1")
    );
    assert_eq!(scratch.read("space.txt"), b"foo_bar\n");
    scratch.file("indent.txt", b"if x:\n    foo()\n");
    assert_eq!(
        change("indent.txt", &[("    ", "\t")], &launch).unwrap(),
        format!("Successfully replaced 1 block(s) in {shown_indent}.\nedits[0]: line 2")
    );
    assert_eq!(scratch.read("indent.txt"), b"if x:\n\tfoo()\n");
    // A target the content does not hold writes nothing.
    scratch.file("none.txt", b"ab\n");
    assert_eq!(
        change("none.txt", &[(" ", "X")], &launch).unwrap_err(),
        format!(
            "Cannot edit {shown_none}; nothing was changed:\n- edits[0]: oldText was not found, even ignoring trailing whitespace, Unicode compatibility forms, curly quotes, dashes, and special spaces."
        )
    );
    assert_eq!(scratch.read("none.txt"), b"ab\n");
}

#[test]
fn edit_refuses_a_fuzzy_match_that_ends_inside_a_unit() {
    let scratch = Scratch::new("unit-end");
    let launch = context(&scratch.0, &scratch.0);
    let shown = display_path(&scratch.0.join("a.txt"));
    // NFKC turns the ligature into two characters, so a match that ends between them names no
    // bytes of the content.
    scratch.file("a.txt", "\u{2019}\u{fb00}tail\n".as_bytes());
    assert_eq!(
        change("a.txt", &[("'f", "X")], &launch).unwrap_err(),
        format!(
            "Cannot edit {shown}; nothing was changed:\n- edits[0]: oldText was not found, even ignoring trailing whitespace, Unicode compatibility forms, curly quotes, dashes, and special spaces."
        )
    );
    assert_eq!(scratch.read("a.txt"), "\u{2019}\u{fb00}tail\n".as_bytes());
    // A match that covers whole units names them all and replaces them.
    assert_eq!(
        change("a.txt", &[("'ff", "'ff")], &launch).unwrap(),
        format!("Successfully replaced 1 block(s) in {shown}.\nedits[0]: line 1 (fuzzy match)")
    );
    assert_eq!(scratch.read("a.txt"), b"'fftail\n");
}

#[test]
fn edit_matches_a_line_end_that_lost_trailing_whitespace() {
    let scratch = Scratch::new("trimmed-line-end");
    let launch = context(&scratch.0, &scratch.0);
    let shown = display_path(&scratch.0.join("a.txt"));
    // The match includes the physical newline; the later edit reports its original line.
    scratch.file("a.txt", "it\u{2019}s   \nnext\nmore\n".as_bytes());
    assert_eq!(
        change("a.txt", &[("it's\n", "X"), ("more", "MORE")], &launch).unwrap(),
        format!(
            "Successfully replaced 2 block(s) in {shown}.\nedits[0]: line 1 (fuzzy match)\nedits[1]: line 3"
        )
    );
    assert_eq!(scratch.read("a.txt"), b"Xnext\nMORE\n");
}

#[test]
fn edit_matches_from_a_line_end_that_lost_trailing_whitespace() {
    let scratch = Scratch::new("trimmed-line-start");
    let launch = context(&scratch.0, &scratch.0);
    let shown_a = display_path(&scratch.0.join("a.txt"));
    let shown_b = display_path(&scratch.0.join("b.txt"));
    // The match starts on the newline the trim kept, so the removed spaces stay before it.
    scratch.file("a.txt", "a   \n\u{201c}b\u{201d}".as_bytes());
    assert_eq!(
        change("a.txt", &[("\n\"b\"", "X")], &launch).unwrap(),
        format!("Successfully replaced 1 block(s) in {shown_a}.\nedits[0]: line 1 (fuzzy match)")
    );
    assert_eq!(scratch.read("a.txt"), b"a   X");
    // A match that keeps the newline keeps it where it was.
    scratch.file("b.txt", "a   \n\u{201c}b\u{201d}\n".as_bytes());
    assert_eq!(
        change("b.txt", &[("\n\"b\"", "\nB")], &launch).unwrap(),
        format!("Successfully replaced 1 block(s) in {shown_b}.\nedits[0]: line 1 (fuzzy match)")
    );
    assert_eq!(scratch.read("b.txt"), b"a   \nB\n");
}

#[test]
fn edit_refuses_a_run_longer_than_a_unit_holds_promptly() {
    let scratch = Scratch::new("long-run");
    let launch = context(&scratch.0, &scratch.0);
    let shown = display_path(&scratch.0.join("a.txt"));
    // A run of reordering non-starters longer than one unit may hold: the units no longer
    // reproduce the whole-file form, so the edit is refused instead of guessed.
    let content = format!("{}\nit\u{2019}s\n", "\u{0f73}".repeat(8192));
    scratch.file("a.txt", content.as_bytes());
    let started = Instant::now();
    assert_eq!(
        change("a.txt", &[("it's", "X")], &launch).unwrap_err(),
        format!(
            "Cannot edit {shown}; nothing was changed:\n- edits[0]: oldText was not found, even ignoring trailing whitespace, Unicode compatibility forms, curly quotes, dashes, and special spaces."
        )
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(scratch.read("a.txt"), content.as_bytes());
    // An exact edit of the same file still applies.
    assert_eq!(
        change("a.txt", &[("\nit\u{2019}s\n", "\nit's\n")], &launch).unwrap(),
        format!("Successfully replaced 1 block(s) in {shown}.\nedits[0]: line 1")
    );
}

#[test]
fn edit_matches_across_a_short_combining_run() {
    let scratch = Scratch::new("short-run");
    let launch = context(&scratch.0, &scratch.0);
    let shown = display_path(&scratch.0.join("a.txt"));
    scratch.file("a.txt", "cafe\u{301} \u{201c}x\u{201d}\n".as_bytes());
    assert_eq!(
        change("a.txt", &[("caf\u{e9} \"x\"", "caf\u{e9} \"y\"")], &launch).unwrap(),
        format!("Successfully replaced 1 block(s) in {shown}.\nedits[0]: line 1 (fuzzy match)")
    );
    assert_eq!(scratch.read("a.txt"), "caf\u{e9} \"y\"\n".as_bytes());
}

/// The form the units reproduce, the way the map builds and checks it.
fn unit_form(text: &str) -> String {
    let form: String = units_of(text)
        .into_iter()
        .map(|(at, end)| mapped(&nfkc(&text[at..end])))
        .collect();
    trimmed_form(&form).0
}

#[test]
fn units_reproduce_the_fuzzy_form_of_their_text() {
    let samples = [
        "author = O\u{2019}Reilly\nnote = a\u{327}\u{301}\n",
        "title = \u{201c}hello\u{201d}\nword = \u{628}\u{650}\u{651}\u{64e}\n",
        "cafe\u{301} \u{201c}x\u{201d}\n",
        "e\u{301}\u{323}\u{327}\n",
        "\u{1112}\u{1161}\u{11ab}\u{1100}\u{1173}",
        "\u{fb01} \u{2460} \u{216b} \u{ff76}\u{ff9e}",
        "x\u{301}   \ny\t\n   \n",
        "a\u{fe00}b\u{200d}c\u{1f600}",
    ];
    for sample in samples {
        assert_eq!(
            unit_form(sample),
            normalize_for_fuzzy_match(sample),
            "{sample:?}"
        );
    }
}

#[test]
fn edit_matches_a_mark_that_reorders_inside_its_unit() {
    let scratch = Scratch::new("reorder");
    let launch = context(&scratch.0, &scratch.0);
    let shown_a = display_path(&scratch.0.join("a.txt"));
    let shown_b = display_path(&scratch.0.join("b.txt"));
    let shown_c = display_path(&scratch.0.join("c.txt"));
    let shown_d = display_path(&scratch.0.join("d.txt"));
    // A cedilla between a base and an acute is below the acute's combining class, so the whole
    // run is one unit and an edit elsewhere in the file still matches.
    scratch.file(
        "a.txt",
        "author = O\u{2019}Reilly\nnote = a\u{327}\u{301}\n".as_bytes(),
    );
    assert_eq!(
        change("a.txt", &[("O'Reilly", "Publisher")], &launch).unwrap(),
        format!("Successfully replaced 1 block(s) in {shown_a}.\nedits[0]: line 1 (fuzzy match)")
    );
    assert_eq!(
        scratch.read("a.txt"),
        "author = Publisher\nnote = a\u{327}\u{301}\n".as_bytes()
    );
    // The run itself is one unit, so its own normalized form names it.
    scratch.file("b.txt", "note = a\u{327}\u{301}\n".as_bytes());
    assert_eq!(
        change("b.txt", &[("\u{e1}\u{327}", "X")], &launch).unwrap(),
        format!("Successfully replaced 1 block(s) in {shown_b}.\nedits[0]: line 1 (fuzzy match)")
    );
    assert_eq!(scratch.read("b.txt"), b"note = X\n");
    // The same for marks that reorder without composing, as in vocalized Arabic.
    scratch.file(
        "c.txt",
        "title = \u{201c}hello\u{201d}\nword = \u{628}\u{650}\u{651}\u{64e}\n".as_bytes(),
    );
    assert_eq!(
        change("c.txt", &[("\"hello\"", "\"world\"")], &launch).unwrap(),
        format!("Successfully replaced 1 block(s) in {shown_c}.\nedits[0]: line 1 (fuzzy match)")
    );
    assert_eq!(
        scratch.read("c.txt"),
        "title = \"world\"\nword = \u{628}\u{650}\u{651}\u{64e}\n".as_bytes()
    );
    scratch.file("d.txt", "word = \u{628}\u{650}\u{651}\u{64e}\n".as_bytes());
    assert_eq!(
        change("d.txt", &[("\u{628}\u{64e}\u{650}\u{651}", "X")], &launch).unwrap(),
        format!("Successfully replaced 1 block(s) in {shown_d}.\nedits[0]: line 1 (fuzzy match)")
    );
    assert_eq!(scratch.read("d.txt"), b"word = X\n");
}

#[test]
fn edit_counts_overlapping_starts_in_one_pass() {
    let scratch = Scratch::new("overlap-cost");
    let launch = context(&scratch.0, &scratch.0);
    let shown = display_path(&scratch.0.join("a.txt"));
    let content = "a".repeat(131_072);
    scratch.file("a.txt", content.as_bytes());
    let needle = "a".repeat(65_536);
    let started = Instant::now();
    assert_eq!(
        change("a.txt", &[(needle.as_str(), "X")], &launch).unwrap_err(),
        format!(
            "Cannot edit {shown}; nothing was changed:\n- edits[0]: oldText occurs 65537 times, starting at lines 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 (and 65517 more); add surrounding text to make it unique."
        )
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(scratch.read("a.txt"), content.as_bytes());
}

#[test]
fn edit_refuses_a_file_that_is_not_utf8() {
    let scratch = Scratch::new("not-utf8");
    let launch = context(&scratch.0, &scratch.0);
    let shown = display_path(&scratch.0.join("a.txt"));
    scratch.file("a.txt", b"one\n\xfftwo\n");
    assert_eq!(
        change("a.txt", &[("two", "TWO")], &launch).unwrap_err(),
        format!("Could not edit file: {shown}. the file is not valid UTF-8; edit it with exec.")
    );
}

#[test]
fn the_mutation_lock_keeps_every_edit_of_one_file() {
    let scratch = Scratch::new("mutex");
    let launch = context(&scratch.0, &scratch.0);
    const WORKERS: usize = 4;
    const ROUNDS: usize = 25;
    let start: String = (0..WORKERS)
        .map(|worker| format!("worker-{worker}=0\n"))
        .collect();
    scratch.file("counters.txt", start.as_bytes());
    let path = scratch.path("counters.txt");
    std::thread::scope(|scope| {
        for worker in 0..WORKERS {
            let path = path.clone();
            let launch = &launch;
            scope.spawn(move || {
                for round in 0..ROUNDS {
                    let old = format!("worker-{worker}={round}");
                    let request = EditRequest {
                        path: path.clone(),
                        edits: vec![Edit {
                            old_text: old.clone(),
                            new_text: format!("worker-{worker}={}", round + 1),
                            start_line: None,
                            end_line: None,
                        }],
                    };
                    edit_text(&request, launch)
                        .unwrap_or_else(|error| panic!("{old} was lost: {error}"));
                }
            });
        }
    });
    let mut expected = String::new();
    for worker in 0..WORKERS {
        expected.push_str(&format!("worker-{worker}={ROUNDS}\n"));
    }
    assert_eq!(
        String::from_utf8(scratch.read("counters.txt")).unwrap(),
        expected
    );
}
#[test]
fn read_restores_the_final_newline_after_an_empty_last_line() {
    let scratch = Scratch::new("final-empty");
    let launch = context(&scratch.0, &scratch.0);
    // 2000 selected lines whose last one is empty, and the newline after it: the selected
    // content ends in two newlines, and the second one is the empty element.
    let text = format!("{}\n", "x\n".repeat(1999));
    scratch.file("empty-last.txt", text.as_bytes());
    let path = scratch.path("empty-last.txt");
    assert_eq!(whole(&path, &launch, false).unwrap(), text);
}

#[test]
fn read_counts_the_final_separator_against_the_byte_budget() {
    let scratch = Scratch::new("separator");
    let launch = context(&scratch.0, &scratch.0);
    // 1999 lines of 25 bytes (49975 bytes) plus a 2000th line of 1226 bytes (1225 'z' + LF)
    // total 51201 bytes: line 2000's terminator puts it 1 byte over the 50KB budget.
    let mut text = String::new();
    for _ in 0..1999 {
        text.push_str(&"y".repeat(24));
        text.push('\n');
    }
    let shown_prefix = text.clone();
    text.push_str(&"z".repeat(1225));
    text.push('\n');
    scratch.file("at-cap.txt", text.as_bytes());
    let path = scratch.path("at-cap.txt");
    assert_eq!(
        whole(&path, &launch, false).unwrap(),
        format!(
            "{shown_prefix}\n[Showing lines 1-1999 of 2000 (50.0KB limit). Use offset=2000 to continue.]"
        )
    );
}
#[test]
fn read_keeps_an_empty_line_when_a_carry_holds_a_later_character() {
    let scratch = Scratch::new("carry-skip");
    let launch = context(&scratch.0, &scratch.0);
    // The first byte of the last line's `é` is the final byte of the first 8192-byte read,
    // so the decoder holds it while the line before it is still empty: that empty line is
    // real content and must remain available after the 2000-line budget is reached.
    let text = format!("{}\n{}{}tail", "x\n".repeat(2000), "a".repeat(4190), 'é');
    scratch.file("carry.txt", text.as_bytes());
    let path = scratch.path("carry.txt");
    assert_eq!(
        window(&path, None, Some(2001), &launch).unwrap(),
        format!(
            "{}\n[Showing lines 1-2000 of 2002. Use offset=2001 to continue.]",
            "x\n".repeat(2000)
        )
    );
}
/// A small PNG built in memory, so no binary file is committed.
fn png_bytes(width: u32, height: u32) -> Vec<u8> {
    let image = image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(
        width,
        height,
        image::Rgb([7, 8, 9]),
    ));
    let mut bytes = Vec::new();
    image
        .write_to(
            &mut std::io::Cursor::new(&mut bytes),
            image::ImageFormat::Png,
        )
        .expect("the fixture encodes");
    bytes
}

#[test]
fn read_answers_a_png_with_its_bytes_and_its_mime_type() {
    let scratch = Scratch::new("image");
    let launch = context(&scratch.0, &scratch.0);
    let png = png_bytes(8, 6);
    scratch.file("shot.png", &png);
    let path = scratch.path("shot.png");
    let outcome = read_text(
        &ReadRequest {
            path,
            offset: Some(5),
            limit: Some(10),
        },
        &launch,
        false,
        ReadPolicy::AGENT,
    )
    .unwrap();
    assert_eq!(
        outcome.text,
        format!("Read image file [image/png] ({} B)", png.len())
    );
    assert_eq!(outcome.structured["bytes"], png.len() as u64);
    assert_eq!(outcome.structured["attached"], true);
    assert!(outcome.structured.get("width").is_none());
    let block = outcome.attachment.expect("the image travels with the text");
    let img = block.as_image().expect("image block");
    assert_eq!(img.mime_type, "image/png");
    assert_eq!(
        img.data,
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &png)
    );
}

#[test]
fn read_attaches_animated_png_by_default_and_passes_through_under_compress_mode() {
    let scratch = Scratch::new("animated");
    let png = png_bytes(4, 4);
    let mut animated = png[..33].to_vec();
    // Animation metadata can legally follow a long ancillary chunk.
    animated.extend_from_slice(&9000u32.to_be_bytes());
    animated.extend_from_slice(b"tEXt");
    animated.resize(animated.len() + 9000 + 4, 0);
    animated.extend_from_slice(&[0, 0, 0, 8]);
    animated.extend_from_slice(b"acTL");
    animated.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 1]);
    animated.extend_from_slice(&[0, 0, 0, 0]);
    animated.extend_from_slice(&png[33..]);
    scratch.file("moving.png", &animated);
    let path = scratch.path("moving.png");

    for compress in ["false", "true"] {
        let launch = LaunchContext::new(
            scratch.0.clone(),
            vec![
                ("HOME".into(), scratch.0.as_os_str().to_owned()),
                ("FASTEXEC_IMAGE_COMPRESS".into(), compress.into()),
            ],
        );
        let outcome = read_text(
            &ReadRequest {
                path: path.clone(),
                offset: None,
                limit: None,
            },
            &launch,
            false,
            ReadPolicy::AGENT,
        )
        .unwrap();
        assert_eq!(outcome.structured["attached"], true);
        assert_eq!(outcome.structured["bytes"], animated.len() as u64);
        let img = outcome.attachment.unwrap();
        assert_eq!(
            img.as_image().unwrap().data,
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &animated)
        );
    }
}

#[test]
fn media_read_keeps_capacity_charged_after_growth_and_shrink() {
    for (initial, actual) in [(8192, 100), (65536, 65537)] {
        for overhead in [None, Some(128)] {
            let pool = BytePool::new(1 << 20);
            let mut input = std::io::Cursor::new(vec![0; actual]);
            let MediaBytes::Loaded { bytes, lease } = read_media_from_zero(
                &mut input,
                initial,
                MediaStart::Rewind,
                100_000,
                &pool,
                overhead,
                None,
            )
            .unwrap() else {
                panic!("small input must load");
            };
            assert_eq!(bytes.len(), actual);
            assert!(
                lease.bytes() >= bytes.capacity(),
                "capacity {} was charged as {}",
                bytes.capacity(),
                lease.bytes()
            );
            drop(bytes);
            drop(lease);
            assert_eq!(pool.used(), 0);
        }
    }
}

#[test]
fn media_reads_count_from_byte_zero_for_both_prefix_and_rewind_and_handle_growth_and_io_errors() {
    // Test exact N-1, N pass and N+1 reject from byte 0 on both Prefix and Rewind paths.
    let total = 10_000usize;
    let payload: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();
    let pool = BytePool::new(1 << 20);

    for use_prefix in [true, false] {
        for (max_cap, expect_ok) in [
            ((total + 1) as u64, true),
            (total as u64, true),
            ((total - 1) as u64, false),
        ] {
            let mut cursor = std::io::Cursor::new(payload.clone());
            let mut prefix = vec![0u8; SNIFF];
            let sniffed = cursor.read(&mut prefix).unwrap();
            assert_eq!(sniffed, SNIFF);
            let start = if use_prefix {
                MediaStart::Prefix(&prefix[..sniffed])
            } else {
                MediaStart::Rewind
            };
            // Pass initial_len = SNIFF as u64 when testing N+1 growth detection so the streaming
            // loop exercises the exact max + 1 - sniffed / max + 1 byte cap from byte 0.
            let initial_len = if expect_ok {
                payload.len() as u64
            } else {
                SNIFF as u64
            };
            match read_media_from_zero(
                &mut cursor,
                initial_len,
                start,
                max_cap,
                &pool,
                Some(BASE_MEDIA_FRAME_OVERHEAD),
                None,
            )
            .unwrap()
            {
                MediaBytes::Loaded { bytes, lease } => {
                    assert!(expect_ok, "expected rejection for max_cap={max_cap}");
                    assert_eq!(bytes, payload);
                    drop(bytes);
                    drop(lease);
                }
                MediaBytes::Oversized { exact_source_bytes } => {
                    assert!(!expect_ok, "expected pass for max_cap={max_cap}");
                    assert_eq!(
                        exact_source_bytes, None,
                        "concurrent growth past limit must not claim sample length as exact"
                    );
                }
            }
            assert_eq!(pool.used(), 0);
        }
    }

    // True I/O failure mid-stream returns MediaReadError::Io, never Oversized/unattached.
    struct MidFailReader {
        emitted: usize,
    }
    impl Read for MidFailReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.emitted == 0 {
                buf[0] = 1;
                self.emitted = 1;
                Ok(1)
            } else {
                Err(io::Error::other("disk read fault"))
            }
        }
    }
    impl Seek for MidFailReader {
        fn seek(&mut self, _: SeekFrom) -> io::Result<u64> {
            Ok(0)
        }
    }
    let mut failing = MidFailReader { emitted: 0 };
    assert!(matches!(
        read_media_from_zero(
            &mut failing,
            100,
            MediaStart::Rewind,
            1000,
            &pool,
            Some(BASE_MEDIA_FRAME_OVERHEAD),
            None,
        ),
        Err(MediaReadError::Io(_))
    ));
    assert_eq!(pool.used(), 0);

    // Cancellation mid-stream aborts at the 64 KiB chunk boundary and releases the lease.
    let token = tokio_util::sync::CancellationToken::new();
    struct CancelAfterFirstChunkReader {
        read_calls: usize,
        token: tokio_util::sync::CancellationToken,
    }
    impl Read for CancelAfterFirstChunkReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            self.read_calls += 1;
            buf.fill(0x55);
            if self.read_calls == 1 {
                self.token.cancel();
            }
            Ok(buf.len())
        }
    }
    impl Seek for CancelAfterFirstChunkReader {
        fn seek(&mut self, _: SeekFrom) -> io::Result<u64> {
            Ok(0)
        }
    }
    let mut cancellable = CancelAfterFirstChunkReader {
        read_calls: 0,
        token: token.clone(),
    };
    assert!(matches!(
        read_media_from_zero(
            &mut cancellable,
            256 * 1024,
            MediaStart::Rewind,
            1024 * 1024,
            &pool,
            Some(BASE_MEDIA_FRAME_OVERHEAD),
            Some(&token),
        ),
        Err(MediaReadError::Cancelled)
    ));
    assert_eq!(
        cancellable.read_calls, 1,
        "must stop at next 64 KiB chunk boundary"
    );
    assert_eq!(pool.used(), 0);
}

#[test]
fn multimodal_read_enforces_zero_guards_nested_descriptors_and_output_modes() {
    let scratch = Scratch::new("multimodal-modes");
    let pdf = b"%PDF-1.7\n%\xE2\xE3\xCF\xD3\n1 0 obj\n<< /Type /Catalog >>\nendobj\n";
    scratch.file("doc.pdf", pdf);
    let mut wav = b"RIFF\x24\x00\x00\x00WAVEfmt \x10\x00\x00\x00".to_vec();
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&44_100u32.to_le_bytes());
    wav.extend_from_slice(&[0u8; 8]);
    scratch.file("tone.wav", &wav);
    let mut mp4 = Vec::new();
    mp4.extend_from_slice(&20u32.to_be_bytes());
    mp4.extend_from_slice(b"ftypisom\0\0\0\0mp42");
    scratch.file("clip.mp4", &mp4);

    let launch = context(&scratch.0, &scratch.0);

    // offset: 0 and limit: 0 fail on media files just like text files.
    assert!(
        read_text(
            &ReadRequest {
                path: scratch.path("doc.pdf"),
                offset: Some(0),
                limit: None
            },
            &launch,
            false,
            ReadPolicy::AGENT
        )
        .is_err()
    );
    assert!(
        read_text(
            &ReadRequest {
                path: scratch.path("doc.pdf"),
                offset: None,
                limit: Some(0)
            },
            &launch,
            false,
            ReadPolicy::AGENT
        )
        .is_err()
    );

    // Valid offset/limit are silently ignored on PDF, WAV, and MP4, and all output modes omit structured["text"].
    for (file, expected_kind, expected_mime) in [
        ("doc.pdf", "document", "application/pdf"),
        ("tone.wav", "audio", "audio/wav"),
        ("clip.mp4", "video", "video/mp4"),
    ] {
        for mode in [OutputMode::Text, OutputMode::Both, OutputMode::Structured] {
            for sc in [true, false] {
                let out = read_text(
                    &ReadRequest {
                        path: scratch.path(file),
                        offset: Some(-5),
                        limit: Some(2),
                    },
                    &launch,
                    true,
                    ReadPolicy::AGENT,
                )
                .unwrap();
                let tracked = out.into_tracked(
                    Render {
                        output_mode: mode,
                        structured_content: sc,
                    }
                    .effective(),
                );
                assert_eq!(tracked.result.content.len(), 2);
                let head = tracked.result.content[0].as_text().unwrap();
                assert!(
                    head.text
                        .starts_with(&format!("Read {expected_kind} file [{expected_mime}]")),
                    "{}",
                    head.text
                );
                if sc {
                    let meta = tracked.result.structured_content.as_ref().unwrap();
                    assert_eq!(meta["kind"], expected_kind);
                    assert_eq!(meta["mimeType"], expected_mime);
                    assert_eq!(meta["attached"], true);
                    assert!(meta.get("text").is_none());
                } else {
                    assert!(tracked.result.structured_content.is_none());
                }
            }
        }

        // Nested codemode read returns attachable descriptor without base64 attachment.
        let nested = read_text(
            &ReadRequest {
                path: scratch.path(file),
                offset: None,
                limit: None,
            },
            &launch,
            false,
            ReadPolicy::PROGRAM,
        )
        .unwrap();
        assert!(nested.attachment.is_none());
        assert_eq!(nested.structured["kind"], expected_kind);
        assert_eq!(nested.structured["mimeType"], expected_mime);
        assert_eq!(nested.structured["attachable"], true);
        assert!(nested.structured["bytes"].as_u64().is_some());
    }
}

#[test]
fn read_real_line_table_pagination_and_empty_offsets() {
    let scratch = Scratch::new("p1-2-table");
    let launch = context(&scratch.0, &scratch.0);
    for (idx, (sample, expected_total)) in [
        ("", 0_u64),
        ("a", 1),
        ("a\n", 1),
        ("a\nb", 2),
        ("a\n\n", 2),
        ("a\r\nb\r\n", 2),
        ("a\rb\r", 1),
    ]
    .into_iter()
    .enumerate()
    {
        let name = format!("case-{idx}.txt");
        scratch.file(&name, sample.as_bytes());
        let out = read_text(
            &ReadRequest {
                path: name.clone(),
                offset: Some(1),
                limit: None,
            },
            &launch,
            false,
            ReadPolicy::AGENT,
        )
        .unwrap();
        assert_eq!(out.structured["totalLines"], expected_total, "{sample:?}");
        assert_eq!(out.body.as_deref(), Some(sample), "{sample:?}");
        assert_eq!(out.structured["truncated"], false, "{sample:?}");
        if expected_total == 0 {
            assert_eq!(out.structured["shownLines"], 0);
            assert!(out.structured["endLine"].is_null());
        } else {
            let mut joined = String::new();
            let mut next = Some(1_i64);
            let mut pages = 0_u64;
            while let Some(off) = next {
                let page = read_text(
                    &ReadRequest {
                        path: name.clone(),
                        offset: Some(off),
                        limit: Some(1),
                    },
                    &launch,
                    false,
                    ReadPolicy::AGENT,
                )
                .unwrap();
                joined.push_str(page.body.as_deref().unwrap());
                next = page.structured["nextOffset"].as_i64();
                pages += 1;
            }
            assert_eq!(pages, expected_total, "{sample:?}");
            assert_eq!(joined, sample, "{sample:?}");
        }
    }
    scratch.file("empty.txt", b"");
    assert!(window("empty.txt", Some(0), None, &launch).is_err());
    assert_eq!(window("empty.txt", Some(1), None, &launch).unwrap(), "");
    assert_eq!(
        window("empty.txt", Some(2), None, &launch).unwrap_err(),
        "Offset 2 is beyond end of file (0 lines total)"
    );
}

#[test]
fn edit_collects_all_failures_containment_overlaps_nfkc_cr_lines_and_caps() {
    let scratch = Scratch::new("p1-3-failures");
    let launch = context(&scratch.0, &scratch.0);
    scratch.file("mix.txt", b"dup\ndup\nabcdef\n");
    let err = edit_outcome(
        &EditRequest {
            path: "mix.txt".into(),
            edits: vec![
                Edit {
                    old_text: "dup".into(),
                    new_text: "D".into(),
                    start_line: None,
                    end_line: None,
                },
                Edit {
                    old_text: "missing".into(),
                    new_text: "M".into(),
                    start_line: None,
                    end_line: None,
                },
                Edit {
                    old_text: "abcdef".into(),
                    new_text: "X".into(),
                    start_line: None,
                    end_line: None,
                },
                Edit {
                    old_text: "bc".into(),
                    new_text: "Y".into(),
                    start_line: None,
                    end_line: None,
                },
                Edit {
                    old_text: "ef".into(),
                    new_text: "Z".into(),
                    start_line: None,
                    end_line: None,
                },
            ],
        },
        &launch,
    )
    .unwrap_err()
    .1
    .unwrap();
    assert_eq!(
        err["failures"],
        json!([
            {"index": 0, "problem": "ambiguous", "lines": [1, 2]},
            {"index": 1, "problem": "not_found"},
            {"index": 2, "problem": "overlap", "with": [3, 4]},
            {"index": 3, "problem": "overlap", "with": [2]},
            {"index": 4, "problem": "overlap", "with": [2]},
        ])
    );
    assert_eq!(scratch.read("mix.txt"), b"dup\ndup\nabcdef\n");

    scratch.file("nfkc.txt", "a\rf\n\u{fb00}\rb\n".as_bytes());
    let nfkc_err = edit_outcome(
        &EditRequest {
            path: "nfkc.txt".into(),
            edits: vec![Edit {
                old_text: "f".into(),
                new_text: "F".into(),
                start_line: None,
                end_line: None,
            }],
        },
        &launch,
    )
    .unwrap_err()
    .1
    .unwrap();
    assert_eq!(
        nfkc_err["failures"],
        json!([{"index": 0, "problem": "ambiguous", "lines": [1, 2, 2]}])
    );

    let alphabet: String = (0..25).map(|i| format!("tok{i:02} ")).collect();
    scratch.file("many-ov.txt", alphabet.as_bytes());
    let mut ov_edits = vec![Edit {
        old_text: alphabet.clone(),
        new_text: "ALL".into(),
        start_line: None,
        end_line: None,
    }];
    for i in 0..22 {
        ov_edits.push(Edit {
            old_text: format!("tok{i:02}"),
            new_text: "T".into(),
            start_line: None,
            end_line: None,
        });
    }
    let ov_err = edit_outcome(
        &EditRequest {
            path: "many-ov.txt".into(),
            edits: ov_edits,
        },
        &launch,
    )
    .unwrap_err()
    .1
    .unwrap();
    assert_eq!(ov_err["failures"][0]["with"].as_array().unwrap().len(), 20);
    assert_eq!(ov_err["failures"][0]["more"], 2);
}

#[test]
fn edit_counts_exact_matches_absorbed_by_normalization_and_partial_fuzzy_copies() {
    let request = EditRequest {
        path: "sources.txt".into(),
        edits: vec![Edit {
            old_text: "e".into(),
            new_text: "E".into(),
            start_line: None,
            end_line: None,
        }],
    };
    for (sample, lines) in [
        ("e\u{301}\nｅ\nｅ\n", vec![1, 2, 3]),
        ("e\u{301} ｅ ｅ", vec![1, 1, 1]),
        ("ｅ\ne\u{301}\nｅ\n", vec![1, 2, 3]),
        ("e\u{301}\n⒠", vec![1, 2]), // A fuzzy start inside the expansion "(e)".
    ] {
        let failures = apply_edits(sample, &request, "sources.txt")
            .unwrap_err()
            .failures
            .unwrap();
        assert_eq!(failures[0].lines, Some(lines), "{sample:?}");
    }
    let same_line = format!("e\u{301} ｅ Z{}\u{0323}\n", "\u{301}".repeat(36));
    let failures = apply_edits(&same_line, &request, "sources.txt")
        .unwrap_err()
        .failures
        .unwrap();
    assert_eq!(failures[0].lines, Some(vec![1, 1]));
    let mark_request = EditRequest {
        path: "marks.txt".into(),
        edits: vec![Edit {
            old_text: "\u{301}".into(),
            new_text: "X".into(),
            start_line: None,
            end_line: None,
        }],
    };
    let failures = apply_edits("a\u{301}\u{341}", &mark_request, "marks.txt")
        .unwrap_err()
        .failures
        .unwrap();
    assert_eq!(failures[0].lines, Some(vec![1, 1]));
    // A mark that moved past a different mark still comes from the exact span.
    assert_eq!(
        apply_edits("q\u{301}\u{327}", &mark_request, "marks.txt")
            .unwrap()
            .0,
        "qX\u{327}"
    );
    let trimmed_request = EditRequest {
        path: "trim.txt".into(),
        edits: vec![Edit {
            old_text: "é ".into(),
            new_text: "E".into(),
            start_line: None,
            end_line: None,
        }],
    };
    assert_eq!(
        apply_edits("é \u{301}!", &trimmed_request, "trim.txt")
            .unwrap()
            .0,
        "E\u{301}!"
    );
    let sample = format!("e\u{301}\n{}", "ｅ\n".repeat(23));
    let failures = apply_edits(&sample, &request, "sources.txt")
        .unwrap_err()
        .failures
        .unwrap();
    assert_eq!(failures[0].lines, Some((1..=20).collect::<Vec<_>>()));
    assert_eq!(failures[0].more, Some(4));
}

#[test]
fn windows_display_and_git_bash_drive_helpers_format_every_form() {
    let volume = r"\\?\Volume{01234567-89ab-cdef-0123-456789abcdef}\dir\file";
    let shown = format_windows_display(volume);
    assert_eq!(git_bash_drive_path(&shown).as_deref(), Some(volume));
    #[cfg(windows)]
    {
        let launch = context(Path::new(r"C:\cwd"), Path::new(r"C:\home"));
        assert_eq!(
            resolve(&shown, &launch).unwrap().as_os_str(),
            resolve(volume, &launch).unwrap().as_os_str()
        );
    }
    assert_eq!(git_bash_drive_path("/c"), Some("C:/".into()));
    assert_eq!(
        git_bash_drive_path("/d/work/a.txt"),
        Some("D:/work/a.txt".into())
    );
    assert_eq!(git_bash_drive_path("/tmp/x"), None);
    assert_eq!(
        git_bash_drive_path("//srv/share/dir/a.txt"),
        Some(r"\\srv\share\dir\a.txt".into())
    );
    assert_eq!(git_bash_drive_path("///tmp"), None);
    assert_eq!(
        format_windows_display(r"c:\Users\Eden\file.txt"),
        "C:/Users/Eden/file.txt"
    );
    assert_eq!(
        format_windows_display(r"\\?\c:\Users\file.txt"),
        "C:/Users/file.txt"
    );
    assert_eq!(
        format_windows_display(r"\\server\share\dir\f"),
        "//server/share/dir/f"
    );
    assert_eq!(
        format_windows_display(r"\\?\UNC\server\share\dir\f"),
        "//server/share/dir/f"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn write_accepts_forty_symlinks_and_refuses_the_next() {
    use std::os::unix::fs::symlink;
    let scratch = Scratch::new("symlink-limit");
    let launch = context(&scratch.0, &scratch.0);
    scratch.file("target", b"before\n");
    for index in 1..=41 {
        let target = if index == 1 {
            "target".to_owned()
        } else {
            format!("link{}", index - 1)
        };
        symlink(target, scratch.0.join(format!("link{index}"))).unwrap();
    }
    assert_eq!(whole("link40", &launch, false).unwrap(), "before\n");
    write_text(
        &WriteRequest {
            path: "link40".into(),
            content: "after\n".into(),
        },
        &launch,
    )
    .unwrap();
    symlink(".", scratch.0.join("alias")).unwrap();
    for path in ["link41", "alias/link40"] {
        let native = std::fs::metadata(scratch.0.join(path)).unwrap_err();
        assert_eq!(native.raw_os_error(), Some(libc::ELOOP));
        let error = write_text(
            &WriteRequest {
                path: path.into(),
                content: "wrong\n".into(),
            },
            &launch,
        )
        .unwrap_err();
        assert!(error.contains(&native.to_string()), "{error}");
    }
    assert_eq!(scratch.read("target"), b"after\n");
    assert!(
        std::fs::symlink_metadata(scratch.0.join("link40"))
            .unwrap()
            .file_type()
            .is_symlink()
    );
}

#[cfg(unix)]
#[test]
fn resolve_keeps_dotdot_for_kernel_symlink_traversal() {
    use std::os::unix::fs::symlink;
    let scratch = Scratch::new("symlink-dotdot");
    std::fs::create_dir_all(scratch.0.join("base")).unwrap();
    std::fs::create_dir_all(scratch.0.join("target/sub")).unwrap();
    std::fs::write(scratch.0.join("base/value"), b"wrong-base\n").unwrap();
    std::fs::write(scratch.0.join("target/value"), b"right-target\n").unwrap();
    symlink("../target/sub", scratch.0.join("base/link")).unwrap();
    let launch = context(&scratch.0, &scratch.0);
    let resolved = resolve("./base//link/../value", &launch).unwrap();
    assert!(
        resolved.to_string_lossy().ends_with("base/link/../value"),
        "{resolved:?}"
    );
    assert_eq!(
        whole("./base/link/../value", &launch, false).unwrap(),
        "right-target\n"
    );
    change(
        "./base/link/../value",
        &[("right-target", "updated-target")],
        &launch,
    )
    .unwrap();
    assert_eq!(
        std::fs::read(scratch.0.join("target/value")).unwrap(),
        b"updated-target\n"
    );
    assert_eq!(
        std::fs::read(scratch.0.join("base/value")).unwrap(),
        b"wrong-base\n"
    );
}

#[tokio::test]
async fn cancelled_nested_file_read_retains_io_permit_until_blocking_worker_finishes() {
    let scratch = Scratch::new("nested-read-permit");
    let path = scratch.0.join("sample.txt");
    std::fs::write(&path, "alpha\nbeta\n").unwrap();
    let launch = Arc::new(context(&scratch.0, &scratch.0));
    let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
    let render = Render {
        structured_content: true,
        output_mode: OutputMode::Text,
    };

    // Hold MUTATION on a dedicated OS thread while write_nested enters spawn_blocking, then
    // abort the awaiting future and verify the permit stays taken until MUTATION unlocks.
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let blocker = std::thread::spawn(move || {
        let _guard = MUTATION.lock().unwrap();
        locked_tx.send(()).unwrap();
        let _ = release_rx.recv();
    });
    locked_rx.recv().unwrap();

    let permit = semaphore.clone().acquire_owned().await.unwrap();
    let write_launch = Arc::clone(&launch);
    let write_path = path.display().to_string();
    let mut fut = Box::pin(write_nested(
        WriteRequest {
            path: write_path,
            content: "updated\n".into(),
        },
        write_launch,
        render,
        permit,
        Default::default(),
    ));
    let polled_once =
        std::future::poll_fn(|cx| std::task::Poll::Ready(fut.as_mut().poll(cx).is_pending())).await;
    assert!(
        polled_once,
        "write_nested must be pending while MUTATION is held"
    );
    drop(fut);
    assert_eq!(
        semaphore.available_permits(),
        0,
        "dropping the awaiting future must not release the permit before blocking I/O finishes"
    );
    release_tx.send(()).unwrap();
    blocker.join().unwrap();
    let reacquired =
        tokio::time::timeout(std::time::Duration::from_secs(2), semaphore.acquire_owned())
            .await
            .expect("permit released once blocking worker finishes")
            .unwrap();
    drop(reacquired);
}

#[tokio::test]
async fn text_buffer_file_worker_results_keep_input_owner() {
    let scratch = Scratch::new("text-result-owner");
    let pool = BytePool::new(1024 * 1024);
    let input_pool = BytePool::new(512);
    let launch = Arc::new(context(&scratch.0, &scratch.0).with_memory(Arc::clone(&pool)));
    let render = Render {
        output_mode: OutputMode::Both,
        structured_content: true,
    };
    for (tool, arguments, error) in [
        (
            "write",
            json!({"path":"small.txt", "content":"alpha\n"}),
            false,
        ),
        (
            "edit",
            json!({"path":"small.txt", "edits":[{"oldText":"alpha", "newText":"beta"}]}),
            false,
        ),
        ("read", json!({"path":"small.txt", "limit":1}), false),
        ("read", json!({"path":"missing.txt"}), true),
    ] {
        let tracked = call_tracked(
            tool,
            arguments,
            Arc::clone(&launch),
            false,
            render,
            None,
            crate::lease::Owner::new(input_pool.try_acquire(512).unwrap()),
        )
        .await;
        assert_eq!(tracked.result.is_error, Some(error), "{:?}", tracked.result);
        assert_eq!(input_pool.used(), 512);
        assert!(pool.used() > 0);
        drop(tracked);
        assert_eq!(input_pool.used(), 0);
        assert_eq!(pool.used(), 0);
    }
}

#[tokio::test]
async fn path_workspace_admits_missing_file_errors_and_keeps_their_owner() {
    let scratch = Scratch::new("path-missing");
    let base = context(&scratch.0, &scratch.0);
    let path = "missing-普通.txt";
    let required = path_workspace_bytes(path, &base, SNIFF);
    for budget in [required - 1, required, required + 1] {
        let pool = BytePool::new(budget);
        let result = read_tracked(
            ReadRequest {
                path: path.into(),
                offset: None,
                limit: None,
            },
            Arc::new(context(&scratch.0, &scratch.0).with_memory(Arc::clone(&pool))),
            false,
            Render {
                output_mode: OutputMode::Both,
                structured_content: true,
            },
            None,
            Default::default(),
        )
        .await;
        assert_eq!(result.result.is_error, Some(true));
        assert_eq!(result.overload, budget < required);
        let text = result.result.content[0].as_text().unwrap();
        if budget < required {
            assert!(text.text.contains("not dispatched"));
            assert_eq!(pool.used(), 0);
        } else {
            assert!(text.text.contains("missing-普通.txt"));
            assert_eq!(pool.used(), required);
        }
        drop(result);
        assert_eq!(pool.used(), 0);
    }
}

#[tokio::test]
async fn path_workspace_media_and_binary_errors_do_not_require_text_workspace() {
    let scratch = Scratch::new("path-media");
    scratch.file("small.png", &png_bytes(1, 1));
    scratch.file("binary.bin", b"\0ordinary\0");
    scratch.file("small.txt", b"hello\n");
    for (path, error, overload) in [
        ("small.png", false, false),
        ("binary.bin", true, false),
        ("missing.txt", true, false),
        ("small.txt", true, true),
    ] {
        let pool = BytePool::new(256 * 1024);
        let result = read_tracked(
            ReadRequest {
                path: path.into(),
                offset: None,
                limit: None,
            },
            Arc::new(context(&scratch.0, &scratch.0).with_memory(Arc::clone(&pool))),
            false,
            Render {
                output_mode: OutputMode::Both,
                structured_content: true,
            },
            None,
            Default::default(),
        )
        .await;
        assert_eq!(
            result.result.is_error,
            Some(error),
            "{path}: {:?}",
            result.result
        );
        assert_eq!(result.overload, overload, "{path}");
        assert!(pool.used() >= path_workspace_bytes(path, &context(&scratch.0, &scratch.0), SNIFF));
        drop(result);
        assert_eq!(pool.used(), 0);
    }
}

#[test]
fn edit_readers_retry_interrupted_and_keep_other_errors() {
    struct Interrupted<'a> {
        bytes: &'a [u8],
        interrupt: bool,
    }
    impl Read for Interrupted<'_> {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            self.interrupt = !self.interrupt;
            if self.interrupt {
                return Err(io::ErrorKind::Interrupted.into());
            }
            let end = buffer.len().min(3);
            self.bytes.read(&mut buffer[..end])
        }
    }
    let original = "alpha\r\nbéta\n";
    let reader = || Interrupted {
        bytes: original.as_bytes(),
        interrupt: false,
    };
    let pool = BytePool::new(TEXT_BLOCK + 64);
    let (content, lease) = read_edit_content(&mut reader(), &pool).unwrap();
    assert_eq!(content, original);
    assert!(pool.used() >= TEXT_BLOCK + content.capacity());
    verify_edit_content(&mut reader(), original).unwrap();
    assert!(verify_edit_content(&mut reader(), "changed").is_err());
    drop(content);
    drop(lease);
    assert_eq!(pool.used(), 0);

    struct Failed;
    impl Read for Failed {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::ErrorKind::UnexpectedEof.into())
        }
    }
    assert_eq!(
        read_edit_content(&mut Failed, &pool).unwrap_err().kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert_eq!(
        verify_edit_content(&mut Failed, original)
            .unwrap_err()
            .kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert_eq!(pool.used(), 0);
}

#[test]
fn save_buffers_admit_their_rounded_capacity_and_keep_owners() {
    let required = 3 * std::mem::size_of::<usize>();
    for cap in [required - 1, required, required + 1] {
        let pool = BytePool::new(cap);
        let first = SaveBuffer::<usize>::new(2, &pool).unwrap();
        let second = SaveBuffer::<usize>::new(1, &pool);
        if cap < required {
            assert_eq!(second.err().unwrap().kind(), io::ErrorKind::OutOfMemory);
            assert_eq!(pool.used(), 2 * std::mem::size_of::<usize>());
        } else {
            let second = second.unwrap();
            assert_eq!(pool.used(), required);
            drop(second);
            assert_eq!(pool.used(), 2 * std::mem::size_of::<usize>());
        }
        drop(first);
        assert_eq!(pool.used(), 0);
    }
    let pool = BytePool::new(64);
    assert_eq!(
        SaveBuffer::<usize>::new(usize::MAX, &pool)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::OutOfMemory
    );
    assert_eq!(pool.used(), 0);
}

#[test]
fn edit_workspace_read_growth_is_reserved_and_owned() {
    let scratch = Scratch::new("edit-read-budget");
    let original = "x".repeat(TEXT_BLOCK + 5);
    let path = scratch.file("small.txt", original.as_bytes());
    // The second read can fall back from doubling to exactly the needed
    // capacity, while the first allocation and fixed read block remain live.
    let required = TEXT_BLOCK + TEXT_BLOCK + original.len();
    for cap in [required - 1, required, required + 1] {
        let pool = BytePool::new(cap);
        let source = read_edit_content(&mut File::open(&path).unwrap(), &pool);
        if cap < required {
            let error = source.expect_err("growth needs the complete peak");
            assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
            assert_eq!(pool.used(), 0);
        } else {
            let (content, lease) = source.unwrap();
            assert_eq!(content, original);
            assert_eq!(pool.used(), TEXT_BLOCK + content.capacity());
            drop(content);
            drop(lease);
            assert_eq!(pool.used(), 0);
        }
        assert_eq!(scratch.read("small.txt"), original.as_bytes());
    }
}

fn canonical_peak(path: &Path, output: &Path) -> usize {
    #[cfg(unix)]
    let output_work = {
        let _ = output;
        libc::PATH_MAX as usize
    };
    #[cfg(windows)]
    let output_work = {
        use std::os::windows::ffi::OsStrExt;
        let count = output.as_os_str().encode_wide().count();
        2 * (count + 1) + edit_growth(3 * count, 1).unwrap()
    };
    native_path_bytes(path).unwrap() + output_work
}

#[test]
fn canonical_target_admits_native_output_and_keeps_its_owner() {
    let scratch = Scratch::new("canonical-budget");
    let path = scratch.file("ordinary-文字.txt", b"unchanged");
    let expected = std::fs::canonicalize(&path).unwrap();
    let required = canonical_peak(&path, &expected);
    for cap in [required - 1, required, required + 1] {
        let pool = BytePool::new(cap);
        let result = canonical_target(&path, &pool);
        if cap < required {
            assert_eq!(result.err().unwrap().kind(), io::ErrorKind::OutOfMemory);
            assert_eq!(pool.used(), 0);
        } else {
            let result = result.unwrap();
            assert_eq!(result.value, expected);
            assert_eq!(pool.used(), result.value.capacity());
            drop(result);
            assert_eq!(pool.used(), 0);
        }
        assert_eq!(scratch.read("ordinary-文字.txt"), b"unchanged");
    }
}

#[cfg(unix)]
#[test]
fn native_path_arguments_preserve_non_utf8_bytes_until_released() {
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    let path = PathBuf::from(std::ffi::OsString::from_vec(b"ordinary-\xff.txt".to_vec()));
    let pool = BytePool::new(64 * 1024);
    let input = native_path_input(&path, &pool).unwrap();
    assert_eq!(input.value.to_bytes(), path.as_os_str().as_bytes());
    assert!(pool.used() > path.as_os_str().as_bytes().len());
    drop(input);
    assert_eq!(pool.used(), 0);
}

// The macOS CI filesystem rejects this name with EILSEQ before resolution.
// Byte preservation there is covered above without requiring a creatable filename.
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
#[test]
fn canonical_and_link_outputs_keep_non_utf8_native_names() {
    use std::os::unix::ffi::OsStringExt;
    let scratch = Scratch::new("native-target-budget");
    let name = std::ffi::OsString::from_vec(b"ordinary-\xff.txt".to_vec());
    let target = scratch.0.join(name);
    std::fs::write(&target, b"unchanged").unwrap();
    let link = scratch.0.join("short");
    std::os::unix::fs::symlink(&target, &link).unwrap();
    let pool = BytePool::new(128 * 1024);
    let canonical = canonical_target(&link, &pool).unwrap();
    assert_eq!(canonical.value, target);
    let followed = write_target(&link, &pool).unwrap();
    assert_eq!(followed.value, target);
    assert_eq!(
        pool.used(),
        canonical.value.capacity() + followed.value.capacity()
    );
    drop(canonical);
    assert_eq!(pool.used(), followed.value.capacity());
    drop(followed);
    assert_eq!(pool.used(), 0);
}

#[cfg(unix)]
#[test]
fn native_link_growth_is_admitted_before_the_next_read() {
    let scratch = Scratch::new("native-link-growth");
    let target = scratch
        .0
        .join("part".repeat(35))
        .join("more".repeat(35))
        .join("file.txt");
    std::fs::create_dir_all(target.parent().unwrap()).unwrap();
    std::fs::write(&target, b"unchanged").unwrap();
    let link = scratch.0.join("short");
    std::os::unix::fs::symlink(&target, &link).unwrap();
    let length = target.as_os_str().as_encoded_bytes().len();
    assert!((256..512).contains(&length));
    let required = native_path_bytes(&link).unwrap() + 512;
    for cap in [required - 1, required, required + 1] {
        let pool = BytePool::new(cap);
        let result = read_target_link(&link, &pool);
        if cap < required {
            assert_eq!(result.err().unwrap().kind(), io::ErrorKind::OutOfMemory);
        } else {
            let result = result.unwrap();
            assert_eq!(result.value, target);
            assert_eq!(pool.used(), 512);
            drop(result);
        }
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read(&target).unwrap(), b"unchanged");
    }
}

#[test]
fn edit_workspace_admits_all_stages_before_publish() {
    let scratch = Scratch::new("edit-admission");
    let path = "small.txt";
    let original = "alpha\nbeta\ngamma\ndelta\n";
    let request: EditRequest = serde_json::from_value(json!({
        "path": path,
        "edits": [
            {"oldText":"alpha", "newText":"ALPHA"},
            {"oldText":"beta", "newText":"BETA"},
            {"oldText":"gamma", "newText":"GAMMA"},
            {"oldText":"delta", "newText":"DELTA"}
        ]
    }))
    .unwrap();
    let probe = BytePool::new(1024 * 1024);
    let launch = context(&scratch.0, &scratch.0).with_memory(Arc::clone(&probe));
    let path_bytes = path_workspace_bytes(path, &launch, 0);
    let resolved = scratch.file(path, original.as_bytes());
    let source = EditSource::read(resolved.clone(), &probe).unwrap();
    let source_held = probe.used();
    let verification_peak = canonical_peak(&resolved, &source.target.value);
    drop(source);
    assert_eq!(probe.used(), 0);
    let mut lease = probe.try_acquire(path_bytes).unwrap();
    let result = edit_outcome_reserved(&request, &launch, &mut lease).unwrap();
    // Matching reservations stay live during save. The prepublication name
    // verification also holds a second canonical caller buffer, while the
    // source's original canonical path and file content remain owned.
    let required = lease.bytes() + source_held + verification_peak;
    assert!(required > path_bytes + TEXT_BLOCK + original.len());
    drop(result);
    drop(lease);
    assert_eq!(probe.used(), 0);
    for cap in [required - 1, required, required + 1] {
        scratch.file(path, original.as_bytes());
        let pool = BytePool::new(cap);
        let launch = context(&scratch.0, &scratch.0).with_memory(Arc::clone(&pool));
        let outcome = edit_owned_outcome(&request, &launch);
        if cap < required {
            assert!(
                outcome
                    .value
                    .as_ref()
                    .unwrap_err()
                    .0
                    .contains("active memory budget")
            );
            assert_eq!(scratch.read(path), original.as_bytes());
        } else {
            assert!(outcome.value.is_ok(), "{:?}", outcome.value);
            assert_eq!(scratch.read(path), b"ALPHA\nBETA\nGAMMA\nDELTA\n");
        }
        assert!(pool.used() >= path_bytes, "result/error retains its owner");
        drop(outcome);
        assert_eq!(pool.used(), 0);
    }
}

#[test]
fn edit_workspace_preserves_bom_crlf_fuzzy_and_no_change() {
    let scratch = Scratch::new("edit-normalization");
    for (original, old, new, expected, error) in [
        (
            "\u{feff}alpha\r\nbeta\r\n",
            "alpha",
            "omega",
            "\u{feff}omega\r\nbeta\r\n",
            false,
        ),
        (
            "prefix ｃａｆｅ\u{301}\r\n",
            "café",
            "tea",
            "prefix tea\r\n",
            false,
        ),
        ("ordinary\n", "ordinary", "ordinary", "ordinary\n", true),
    ] {
        scratch.file("small.txt", original.as_bytes());
        let pool = BytePool::new(256 * 1024);
        let launch = context(&scratch.0, &scratch.0).with_memory(Arc::clone(&pool));
        let request = serde_json::from_value(json!({
            "path":"small.txt", "edits":[{"oldText":old,"newText":new}]
        }))
        .unwrap();
        let outcome = edit_owned_outcome(&request, &launch);
        assert_eq!(outcome.value.is_err(), error, "{:?}", outcome.value);
        assert_eq!(scratch.read("small.txt"), expected.as_bytes());
        assert!(pool.used() > 0);
        drop(outcome);
        assert_eq!(pool.used(), 0);
    }
    for text in [
        "ordinary ASCII",
        "é",
        "e\u{301}",
        "\u{fdfa}",
        "\u{1100}\u{1161}",
        "q\u{301}\u{327}",
    ] {
        let bound = EditNormalization::count(text).unwrap();
        assert!(nfkc(text).len() <= bound.bytes);
        assert_eq!(text.nfkd().count(), bound.decomposed);
    }
    assert!(edit_growth(usize::MAX, 1).is_none());
    assert!(edit_growth(1, usize::MAX).is_none());
}

#[test]
fn edit_workspace_does_not_reserve_an_unused_ambiguous_map() {
    let scratch = Scratch::new("edit-ambiguous-budget");
    let original = "x".repeat(512) + " ａ\nａ\nａ\n";
    scratch.file("small.txt", original.as_bytes());
    let pool = BytePool::new(128 * 1024);
    let launch = context(&scratch.0, &scratch.0).with_memory(Arc::clone(&pool));
    let request = serde_json::from_value(json!({
        "path":"small.txt",
        "edits":[{"oldText":"a","newText":"b","startLine":1,"endLine":2}]
    }))
    .unwrap();
    let outcome = edit_owned_outcome(&request, &launch);
    let (message, structured) = outcome.value.as_ref().unwrap_err();
    assert!(message.contains("occurs 2 times"), "{message}");
    assert_eq!(
        structured.as_ref().unwrap()["failures"][0]["problem"],
        "ambiguous"
    );
    assert_eq!(scratch.read("small.txt"), original.as_bytes());
    drop(outcome);
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn edit_workspace_worker_keeps_its_buffers_after_waiter_drop() {
    let scratch = Scratch::new("edit-cancel");
    scratch.file("small.txt", "ordinary ﬁne text\n".as_bytes());
    let pool = BytePool::new(256 * 1024);
    let launch = context(&scratch.0, &scratch.0).with_memory(Arc::clone(&pool));
    let worker_pool = Arc::clone(&pool);
    let (ready, observed) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let worker = tokio::task::spawn_blocking(move || {
        let request: EditRequest = serde_json::from_value(json!({
            "path":"small.txt", "edits":[{"oldText":"fine","newText":"good"}]
        }))
        .unwrap();
        with_path_workspace(&request.path, &launch, 0, |lease| {
            let source = EditSource::read(launch.cwd.join("small.txt"), &worker_pool).unwrap();
            let mut workspace = EditWorkspace::new(lease, worker_pool.limit());
            let result =
                apply_edits_budgeted(&source.content, &request, "small.txt", &mut workspace)
                    .unwrap();
            ready.send(worker_pool.used()).unwrap();
            blocked
                .recv_timeout(std::time::Duration::from_secs(5))
                .unwrap();
            result
        })
        .unwrap()
    });
    let retained = observed.await.unwrap();
    assert!(retained > TEXT_BLOCK);
    assert_eq!(pool.used(), retained);
    drop(worker);
    assert_eq!(
        pool.used(),
        retained,
        "the blocking worker still owns its source and output"
    );
    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while pool.used() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the worker drops its undelivered output before the owner");
    assert_eq!(scratch.read("small.txt"), "ordinary ﬁne text\n".as_bytes());
}

#[tokio::test]
async fn write_target_workspace_admits_before_publish_and_retains_errors() {
    let scratch = Scratch::new("write-target-admission");
    let path = "small.txt";
    let resolved = scratch.file(path, b"original");
    let probe = BytePool::new(256 * 1024);
    let launch = context(&scratch.0, &scratch.0).with_memory(Arc::clone(&probe));
    let base = path_workspace_bytes(path, &launch, 0);
    let target = write_target(&resolved, &probe).unwrap();
    let held = probe.used();
    let native = native_path_bytes(&target.value).unwrap();
    let mut lease = probe.try_acquire(base).unwrap();
    reserve_target_workspace(&target.value, &mut lease, &probe).unwrap();
    let retained = lease.bytes();
    let required = retained + held + native;
    drop(target);
    drop(lease);
    assert_eq!(probe.used(), 0);
    for cap in [required - 1, required, required + 1] {
        scratch.file(path, b"original");
        let pool = BytePool::new(cap);
        let result = call_tracked(
            "write",
            json!({"path":path, "content":"changed"}),
            Arc::new(context(&scratch.0, &scratch.0).with_memory(Arc::clone(&pool))),
            false,
            Render {
                output_mode: OutputMode::Both,
                structured_content: true,
            },
            None,
            Default::default(),
        )
        .await;
        assert_eq!(result.result.is_error, Some(cap < required));
        assert_eq!(
            scratch.read(path),
            if cap < required {
                b"original".as_slice()
            } else {
                b"changed".as_slice()
            }
        );
        assert_eq!(pool.used(), retained);
        drop(result);
        assert_eq!(pool.used(), 0);
    }
}

#[tokio::test]
async fn native_target_owner_survives_a_dropped_worker_waiter() {
    let scratch = Scratch::new("canonical-cancel");
    let path = scratch.file("ordinary.txt", b"unchanged");
    let pool = BytePool::new(64 * 1024);
    let worker_pool = Arc::clone(&pool);
    let (ready, observed) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let worker = tokio::task::spawn_blocking(move || {
        let target = canonical_target(&path, &worker_pool).unwrap();
        ready.send(()).unwrap();
        blocked.recv_timeout(Duration::from_secs(5)).unwrap();
        target
    });
    observed.await.unwrap();
    let held = pool.used();
    assert!(held > 0);
    drop(worker);
    assert_eq!(pool.used(), held);
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while pool.used() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(scratch.read("ordinary.txt"), b"unchanged");
}

#[tokio::test]
async fn path_workspace_mutations_admit_before_touching_the_file() {
    let scratch = Scratch::new("path-write");
    let path = "small.txt";
    let required = path_workspace_bytes(path, &context(&scratch.0, &scratch.0), 0);
    for tool in ["write", "edit"] {
        for budget in [required - 1, required, required + 1] {
            scratch.file(path, b"alpha");
            let arguments = if tool == "write" {
                json!({"path":path, "content":"beta"})
            } else {
                json!({"path":path, "edits":[{"oldText":"alpha", "newText":"beta"}]})
            };
            let pool = BytePool::new(budget);
            let result = call_tracked(
                tool,
                arguments,
                Arc::new(context(&scratch.0, &scratch.0).with_memory(Arc::clone(&pool))),
                false,
                Render {
                    output_mode: OutputMode::Both,
                    structured_content: true,
                },
                None,
                Default::default(),
            )
            .await;
            // Native targets and edit contents have independent reservations;
            // a request-path-only budget must leave both operations unchanged.
            let refused = true;
            assert_eq!(result.result.is_error, Some(refused), "{tool}");
            assert_eq!(
                scratch.read(path),
                if refused {
                    b"alpha".as_slice()
                } else {
                    b"beta".as_slice()
                }
            );
            if budget >= required {
                assert!(
                    result.result.content[0]
                        .as_text()
                        .unwrap()
                        .text
                        .contains("active memory budget")
                );
            }
            assert_eq!(pool.used(), if budget < required { 0 } else { required });
            drop(result);
            assert_eq!(pool.used(), 0);
        }
    }
}

#[test]
fn text_buffer_reads_admit_before_copy_and_keep_the_result_owned() {
    let scratch = Scratch::new("text-buffer");
    let path = scratch.0.join("small.txt");
    let source = "alpha\nbeta\n";
    std::fs::write(&path, source).unwrap();
    for (limits, selected, retained, expected) in [
        (ReadPolicy::PROGRAM, None, source.len() + 1, source),
        (ReadPolicy::AGENT, Some(1), "alpha\n".len(), "alpha\n"),
    ] {
        let request = ReadRequest {
            path: "small.txt".into(),
            offset: None,
            limit: selected,
        };
        let peak = path_workspace_bytes(&request.path, &context(&scratch.0, &scratch.0), SNIFF)
            + TEXT_FIXED_WORKSPACE
            - SNIFF
            + 16 * retained;
        for (budget, fits) in [(peak - 1, false), (peak, true)] {
            for mode in [OutputMode::Text, OutputMode::Both, OutputMode::Structured] {
                let pool = BytePool::new(budget);
                let launch = context(&scratch.0, &scratch.0).with_memory(Arc::clone(&pool));
                let owned = read_blocking(&request, &launch, false, limits, None);
                if !fits {
                    assert!(matches!(owned.value, Err(FileError::Overload(_))));
                    drop(owned);
                    assert_eq!(pool.used(), 0);
                    continue;
                }
                let owner = owned.owner.clone();
                let outcome = match owned.run(|read| read) {
                    Ok(data) => read_data_to_outcome(data, limits, owner),
                    Err(_) => panic!("the exact text workspace must fit"),
                };
                assert_eq!(outcome.body.as_deref(), Some(expected));
                assert_eq!(pool.used(), peak);
                let tracked = outcome.into_tracked(Render {
                    output_mode: mode,
                    structured_content: true,
                });
                assert!(
                    tracked.lease.as_ref().unwrap().bytes()
                        >= service_render::result_payload_capacity(&tracked.result)
                );
                let data = tracked.result.structured_content.as_ref().unwrap();
                if mode == OutputMode::Text {
                    assert!(data.get("text").is_none());
                } else {
                    assert_eq!(data["text"], expected);
                }
                assert_eq!(pool.used(), peak);
                drop(tracked);
                assert_eq!(pool.used(), 0);
            }
        }
    }
}

#[test]
fn text_buffer_growth_uses_nonblocking_admission() {
    let scratch = Scratch::new("text-growth");
    let path = scratch.0.join("small.txt");
    std::fs::write(&path, "hello\n").unwrap();
    for (kept, fits) in [(5, false), (6, true)] {
        let pool = BytePool::new(TEXT_FIXED_WORKSPACE + 32 * "small.txt".len() + 16 * kept);
        let workspace = TextWorkspace::new(&pool, "small.txt".len()).unwrap();
        let mut file = File::open(&path).unwrap();
        let result = read_whole_utf8(&mut file, 0, &[], "small.txt", 64, None, workspace);
        assert_eq!(result.is_ok(), fits);
        if fits {
            assert_eq!(result.as_ref().unwrap().text, "hello\n");
        } else {
            assert!(matches!(result, Err(FileError::Overload(_))));
            assert!(result.err().unwrap().message().contains("active memory budget"));
            assert_eq!(pool.used(), 0);
            continue;
        }
        drop(result);
        assert_eq!(pool.used(), 0);
    }
}

#[tokio::test]
async fn text_buffer_worker_retains_output_after_its_waiter_is_dropped() {
    let scratch = Scratch::new("text-cancel");
    std::fs::write(scratch.0.join("small.txt"), "hello\n").unwrap();
    let pool = BytePool::new(1024 * 1024);
    let launch = context(&scratch.0, &scratch.0).with_memory(Arc::clone(&pool));
    let (ready, observed) = tokio::sync::oneshot::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let worker = tokio::task::spawn_blocking(move || {
        let outcome = read_blocking(
            &ReadRequest {
                path: "small.txt".into(),
                offset: None,
                limit: None,
            },
            &launch,
            false,
            ReadPolicy::PROGRAM,
            None,
        );
        assert!(outcome.value.is_ok(), "small text read");
        ready.send(()).unwrap();
        blocked
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        outcome
    });
    observed.await.unwrap();
    assert!(pool.used() >= TEXT_FIXED_WORKSPACE);
    drop(worker);
    assert!(pool.used() >= TEXT_FIXED_WORKSPACE);
    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while pool.used() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the completed worker drops its undelivered outcome");
}

#[tokio::test]
async fn ipc_ingress_file_worker_keeps_input_after_wait_is_dropped() {
    use std::future::Future;
    let scratch = Scratch::new("input-owner");
    let launch = Arc::new(context(&scratch.0, &scratch.0));
    let pool = crate::lease::BytePool::new(256);
    let owner = crate::lease::Owner::new(pool.try_acquire(256).unwrap());
    let (locked_tx, locked_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let blocker = std::thread::spawn(move || {
        let _guard = MUTATION.lock().unwrap();
        locked_tx.send(()).unwrap();
        let _ = release_rx.recv_timeout(std::time::Duration::from_secs(5));
    });
    locked_rx.recv().unwrap();
    let request = WriteRequest {
        path: "owned.txt".into(),
        content: "ordinary input\n".into(),
    };
    let render = Render {
        output_mode: OutputMode::Text,
        structured_content: true,
    };
    let mut waiting = Box::pin(write(request, launch, render, owner));
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(waiting.as_mut().poll(cx).is_pending()))
            .await
    );
    drop(waiting);
    assert_eq!(
        pool.used(),
        256,
        "queued or running worker owns the request"
    );
    release_tx.send(()).unwrap();
    blocker.join().unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while pool.used() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(scratch.0.join("owned.txt")).unwrap(),
        "ordinary input\n"
    );
}

#[test]
fn read_supports_negative_offsets_across_sizes_empty_files_and_line_endings() {
    let scratch = Scratch::new("negative-offset");
    let launch = context(&scratch.0, &scratch.0);

    let thousand_path = scratch.0.join("thousand.txt");
    let mut thousand = String::new();
    for i in 1..=1000 {
        thousand.push_str(&format!("line-{i}\n"));
    }
    std::fs::write(&thousand_path, &thousand).unwrap();
    for limits in [ReadPolicy::AGENT, ReadPolicy::PROGRAM] {
        let tail = read_text(
            &ReadRequest {
                path: "thousand.txt".into(),
                offset: Some(-50),
                limit: None,
            },
            &launch,
            false,
            limits,
        )
        .unwrap();
        assert_eq!(tail.structured["startLine"], 951);
        assert_eq!(tail.structured["endLine"], 1000);
        assert_eq!(tail.structured["shownLines"], 50);
        assert_eq!(tail.structured["nextOffset"], Value::Null);
        assert!(tail.body.as_deref().unwrap().starts_with("line-951\n"));
        assert!(tail.body.as_deref().unwrap().ends_with("line-1000\n"));
    }

    let big_path = scratch.0.join("hundred_k.txt");
    let mut big = String::with_capacity(900_000);
    for i in 1..=100_000 {
        big.push_str(&format!("r{i}\n"));
    }
    std::fs::write(&big_path, &big).unwrap();
    let slice = read_text(
        &ReadRequest {
            path: "hundred_k.txt".into(),
            offset: Some(-90_000),
            limit: Some(2),
        },
        &launch,
        false,
        ReadPolicy::AGENT,
    )
    .unwrap();
    assert_eq!(slice.structured["startLine"], 10_001);
    assert_eq!(slice.structured["endLine"], 10_002);
    assert_eq!(slice.body.as_deref(), Some("r10001\nr10002\n"));

    let ten_path = scratch.0.join("ten.txt");
    std::fs::write(
        &ten_path,
        (1..=10).map(|i| format!("{i}\n")).collect::<String>(),
    )
    .unwrap();
    for neg in [-5000_i64, i64::MIN] {
        let clamped = read_text(
            &ReadRequest {
                path: "ten.txt".into(),
                offset: Some(neg),
                limit: Some(3),
            },
            &launch,
            false,
            ReadPolicy::AGENT,
        )
        .unwrap();
        assert_eq!(clamped.structured["startLine"], 1);
        assert_eq!(clamped.structured["endLine"], 3);
    }

    let empty_path = scratch.0.join("empty.txt");
    std::fs::write(&empty_path, b"").unwrap();
    let empty_neg = read_text(
        &ReadRequest {
            path: "empty.txt".into(),
            offset: Some(-1),
            limit: None,
        },
        &launch,
        false,
        ReadPolicy::AGENT,
    )
    .unwrap();
    let empty_pos = read_text(
        &ReadRequest {
            path: "empty.txt".into(),
            offset: Some(1),
            limit: None,
        },
        &launch,
        false,
        ReadPolicy::AGENT,
    )
    .unwrap();
    assert_eq!(empty_neg.structured, empty_pos.structured);
    assert_eq!(empty_neg.body.as_deref(), Some(""));
    assert_eq!(empty_neg.structured["startLine"], 1);
    assert_eq!(empty_neg.structured["shownLines"], 0);

    // Mixed LF, CRLF, lone CR, and unterminated final segment: negative offset matches positive.
    let mixed_path = scratch.0.join("mixed.txt");
    std::fs::write(&mixed_path, b"one\ntwo\r\nthree\rfour\nfive").unwrap();
    for (neg, pos) in [(-1, 4), (-2, 3), (-3, 2), (-4, 1)] {
        let a = read_text(
            &ReadRequest {
                path: "mixed.txt".into(),
                offset: Some(neg),
                limit: None,
            },
            &launch,
            false,
            ReadPolicy::AGENT,
        )
        .unwrap();
        let b = read_text(
            &ReadRequest {
                path: "mixed.txt".into(),
                offset: Some(pos),
                limit: None,
            },
            &launch,
            false,
            ReadPolicy::AGENT,
        )
        .unwrap();
        assert_eq!(a.structured, b.structured, "neg={neg} pos={pos}");
        assert_eq!(a.body, b.body, "neg={neg} pos={pos}");
    }
}

#[test]
fn a_tail_slice_rechecks_its_own_line_count_after_growth_or_shrinkage() {
    let scratch = Scratch::new("changing-tail");
    let path = scratch.0.join("changes.txt");
    for (later, expected, start) in [("1\n2\n3\n4\n5\n", "4\n5\n", 4), ("1\n2\n3\n", "2\n3\n", 2)] {
        std::fs::write(&path, "1\n2\n3\n4\n").unwrap();
        let mut file = File::open(&path).unwrap();
        let initial = count_lines_streaming(&mut file, "changes.txt", None).unwrap();
        std::fs::write(&path, later).unwrap();
        let mut workspace =
            TextWorkspace::new(crate::lease::process_pool(), "changes.txt".len()).unwrap();
        let (observed, window) = scan_from_end(2, initial, "changes.txt", |from| {
            file.rewind().unwrap();
            Window::scan(
                Source::new(BufReader::with_capacity(TEXT_BLOCK, &mut file), None),
                from,
                None,
                ReadPolicy::AGENT,
                &mut workspace,
            )
        })
        .unwrap();
        assert_eq!(observed, start);
        let result = window.into_slice(
            observed,
            None,
            "changes.txt".into(),
            ReadPolicy::AGENT,
            Lease::empty(crate::lease::process_pool()),
        );
        assert_eq!(result.text, expected);
    }

    let mut totals = [5, 6].into_iter();
    let error = scan_from_end(2, 4, "changes.txt", |_| {
        Ok(Window {
            total_lines: totals.next().unwrap(),
            ..Default::default()
        })
    })
    .err()
    .unwrap();
    assert!(error.message().contains("changed while reading; retry"));
}

#[test]
fn outside_range_hints_count_exact_and_editable_fuzzy_sources_once() {
    let outside = |source: &str, old: &str, line| {
        let request = EditRequest {
            path: "sources.txt".into(),
            edits: vec![Edit {
                old_text: old.into(),
                new_text: "updated".into(),
                start_line: Some(line),
                end_line: Some(line),
            }],
        };
        apply_edits(source, &request, "sources.txt")
            .unwrap_err()
            .failures
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
    };
    for (source, old, line, starts) in [
        ("q\u{301}\n“a”\n", "q", 2, vec![1]),
        ("x \nfoo\nz\n", " \nfoo", 3, vec![1]),
        ("e\nｅ\n⒠\ne\u{301}\n", "e", 5, vec![1, 2, 4]),
    ] {
        let failure = outside(source, old, line);
        assert_eq!(failure.lines, Some(starts));
        assert_eq!(failure.more, None);
    }
    let failure = outside(&"e\u{301}\n".repeat(23), "e", 24);
    assert_eq!(failure.lines, Some((1..=20).collect::<Vec<_>>()));
    assert_eq!(failure.more, Some(3));
}

#[test]
fn edit_start_line_and_end_line_disambiguate_report_outside_and_detect_overlap() {
    let scratch = Scratch::new("edit-line-range");
    let launch = context(&scratch.0, &scratch.0);

    let lines_path = scratch.0.join("hundred.txt");
    let original: String = (1..=100)
        .map(|line| {
            if matches!(line, 10 | 50 | 90) {
                "value = 1;\n".to_owned()
            } else {
                format!("line {line}\n")
            }
        })
        .collect();
    std::fs::write(&lines_path, &original).unwrap();

    let amb = edit_outcome(
        &EditRequest {
            path: "hundred.txt".into(),
            edits: vec![Edit {
                old_text: "value = 1;".into(),
                new_text: "value = 2;".into(),
                start_line: None,
                end_line: None,
            }],
        },
        &launch,
    )
    .unwrap_err()
    .1
    .unwrap();
    assert_eq!(amb["failures"][0]["problem"], "ambiguous");
    assert_eq!(amb["failures"][0]["lines"], json!([10, 50, 90]));

    let miss = edit_outcome(
        &EditRequest {
            path: "hundred.txt".into(),
            edits: vec![Edit {
                old_text: "value = 1;".into(),
                new_text: "value = 2;".into(),
                start_line: Some(20),
                end_line: Some(30),
            }],
        },
        &launch,
    )
    .unwrap_err()
    .1
    .unwrap();
    assert_eq!(miss["failures"][0]["problem"], "not_found");
    assert_eq!(miss["failures"][0]["lines"], json!([10, 50, 90]));
    assert_eq!(std::fs::read_to_string(&lines_path).unwrap(), original);

    let half = edit_outcome(
        &EditRequest {
            path: "hundred.txt".into(),
            edits: vec![Edit {
                old_text: "value = 1;".into(),
                new_text: "value = 2;".into(),
                start_line: Some(50),
                end_line: None,
            }],
        },
        &launch,
    )
    .unwrap_err()
    .1
    .unwrap();
    assert_eq!(half["failures"][0]["problem"], "ambiguous");
    assert_eq!(half["failures"][0]["lines"], json!([50, 90]));

    let inverted = edit_outcome(
        &EditRequest {
            path: "hundred.txt".into(),
            edits: vec![Edit {
                old_text: "value = 1;".into(),
                new_text: "value = 2;".into(),
                start_line: Some(60),
                end_line: Some(50),
            }],
        },
        &launch,
    )
    .unwrap_err();
    assert!(inverted.0.contains("startLine"), "{inverted:?}");

    // Overlap between a ranged edit and an unranged edit is still rejected cleanly.
    let overlap = edit_outcome(
        &EditRequest {
            path: "hundred.txt".into(),
            edits: vec![
                Edit {
                    old_text: "value = 1;".into(),
                    new_text: "value = 2;".into(),
                    start_line: Some(45),
                    end_line: Some(55),
                },
                Edit {
                    old_text: "line 49\nvalue = 1;\nline 51".into(),
                    new_text: "block".into(),
                    start_line: None,
                    end_line: None,
                },
            ],
        },
        &launch,
    )
    .unwrap_err()
    .1
    .unwrap();
    assert_eq!(overlap["failures"][0]["problem"], "overlap");
    assert_eq!(std::fs::read_to_string(&lines_path).unwrap(), original);

    // Scoped edit updates only line 50; multi-line oldText starting on endLine also succeeds.
    let ok = edit_outcome(
        &EditRequest {
            path: "hundred.txt".into(),
            edits: vec![Edit {
                old_text: "value = 1;\nline 51".into(),
                new_text: "value = 2;\nline 51".into(),
                start_line: Some(45),
                end_line: Some(50),
            }],
        },
        &launch,
    )
    .unwrap();
    assert_eq!(ok.1["edits"][0]["line"], 50);
    let updated: Vec<String> = std::fs::read_to_string(&lines_path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(updated[9], "value = 1;");
    assert_eq!(updated[49], "value = 2;");
    assert_eq!(updated[89], "value = 1;");

    // Mixed LF, CRLF, and lone CR line endings count LF breaks consistently.
    let mixed = scratch.0.join("mixed_endings.txt");
    std::fs::write(&mixed, b"same\nsame\r\nsame\rtail\nsame\n").unwrap();
    edit_outcome(
        &EditRequest {
            path: "mixed_endings.txt".into(),
            edits: vec![Edit {
                old_text: "same".into(),
                new_text: "picked".into(),
                start_line: Some(3),
                end_line: Some(3),
            }],
        },
        &launch,
    )
    .unwrap();
    assert_eq!(
        std::fs::read(&mixed).unwrap(),
        b"same\nsame\r\npicked\rtail\nsame\n"
    );

    // Concurrent growth past 4 MiB after metadata check (ending mid-UTF-8 codepoint at
    // max_bytes + 1) reports the size limit with actual byte count rather than a UTF-8 error.
    let grown = scratch.0.join("grown.txt");
    let mut multibyte_over_4m = "a".repeat(NESTED_MAX_BYTES as usize - 1);
    multibyte_over_4m.push('中'); // 4_194_303 + 3 = 4_194_306 bytes
    std::fs::write(&grown, &multibyte_over_4m).unwrap();
    let mut f = File::open(&grown).unwrap();
    let grown_err = read_whole_utf8(
        &mut f,
        0, // simulates initial metadata.len() <= 4 MiB before concurrent append
        &[],
        "grown.txt",
        NESTED_MAX_BYTES,
        None,
        TextWorkspace::new(crate::lease::process_pool(), "grown.txt".len()).unwrap(),
    )
    .err()
    .unwrap();
    assert!(matches!(grown_err, FileError::Limit(_)));
    assert!(
        grown_err.message().contains("4194306 bytes")
            && grown_err
                .message()
                .contains("exceeds the 4194304 byte (4 MiB) limit"),
        "{grown_err}"
    );

    // Invalid UTF-8 file <= 4 MiB on disk whose lossy U+FFFD expansion would exceed 4 MiB
    // (2,000,000 x 0xff -> 6,000,000 B if decoded lossily) must report invalid UTF-8, not size.
    let bad_2m = scratch.0.join("bad_single_2m.txt");
    std::fs::write(&bad_2m, vec![0xff_u8; 2_000_000]).unwrap();
    let bad_2m_err = read_text(
        &ReadRequest {
            path: "bad_single_2m.txt".into(),
            offset: None,
            limit: None,
        },
        &launch,
        false,
        ReadPolicy::PROGRAM,
    )
    .err()
    .unwrap();
    assert!(
        bad_2m_err.contains("not valid UTF-8") && !bad_2m_err.contains("exceeds"),
        "{bad_2m_err}"
    );

    // Linear-time streaming over many short invalid-UTF-8 lines (previously O(N^2) in Source).
    let many_lossy = scratch.0.join("many_lossy.txt");
    std::fs::write(&many_lossy, b"\xff\n".repeat(200_000)).unwrap();
    let started = std::time::Instant::now();
    let lossy_slice = read_text(
        &ReadRequest {
            path: "many_lossy.txt".into(),
            offset: Some(1),
            limit: None,
        },
        &launch,
        false,
        ReadPolicy::PROGRAM,
    )
    .unwrap();
    assert!(
        started.elapsed() < std::time::Duration::from_secs(3),
        "took {:?}",
        started.elapsed()
    );
    assert_eq!(lossy_slice.structured["totalLines"], 200_000);
    assert_eq!(lossy_slice.structured["lossy"], true);

    // Partial compatibility decomposition inside or outside range (⒠ -> "(e)") rejected by
    // FuzzyMap::span never counts as an out-of-range candidate, while real matches do.
    let compat_path = scratch.0.join("compat.txt");
    std::fs::write(&compat_path, "alpha\n\u{24a0}\n\u{24a0}\ne\n").unwrap();
    let compat_miss = edit_outcome(
        &EditRequest {
            path: "compat.txt".into(),
            edits: vec![Edit {
                old_text: "e".into(),
                new_text: "E".into(),
                start_line: Some(2),
                end_line: Some(2),
            }],
        },
        &launch,
    )
    .unwrap_err()
    .1
    .unwrap();
    assert_eq!(compat_miss["failures"][0]["problem"], "not_found");
    assert_eq!(compat_miss["failures"][0]["lines"], json!([4]));
}

#[test]
fn open_regular_reports_directory_as_is_a_directory_without_exec_hint() {
    let scratch = Scratch::new("open-regular-dir");
    let err = open_regular(&scratch.0).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::IsADirectory);
    assert_eq!(err.to_string(), "it is a directory");
}

#[test]
fn windows_reserved_device_names_match_dos_rules() {
    for reserved in [
        "CON",
        "con",
        "CON:",
        "PRN",
        "AUX",
        "NUL",
        "NUL:foo",
        "COM1",
        "COM9",
        "LPT1",
        "LPT9",
        "COM1.txt",
        "nul.log",
        "CON .txt",
        "NUL   ",
        "COM¹",
        "COM².txt",
        "com³:stream",
        "LPT¹",
        "lpt².log",
        "LPT³",
    ] {
        assert!(
            is_windows_reserved_device_name(reserved),
            "expected reserved: {reserved}"
        );
    }
    for normal in [
        "COM0",
        "COM10",
        "LPT0",
        "LPT10",
        "CONSOLE",
        "NULL",
        "a.NUL",
        "COM⁴",
        "LPT¹0",
        "日本語",
    ] {
        assert!(
            !is_windows_reserved_device_name(normal),
            "expected normal: {normal}"
        );
    }
}

#[cfg(windows)]
#[test]
fn resolve_from_rejects_windows_device_paths_named_pipes_and_reserved_names() {
    let base = Path::new(r"C:\work");
    for rejected in [
        r"\\.\pipe\demo",
        "//./pipe/demo",
        "//server/pipe/demo",
        r"\\server\pipe\demo",
        r"\\.\NUL",
        r"\\.\PhysicalDrive0",
        "NUL",
        "CON",
        "COM1.txt",
        r"sub\LPT9.log",
        r"C:\work\aux",
    ] {
        assert!(
            resolve_from(Path::new(rejected), base).is_err(),
            "expected rejection for {rejected}"
        );
    }
}

#[tokio::test]
async fn read_core_program_strict_whole_file_and_raw_slices() {
    let scratch = Scratch::new("read-core-program");
    let pool = BytePool::new(16 << 20);
    let launch = Arc::new(context(&scratch.0, &scratch.0).with_memory(Arc::clone(&pool)));

    // Whole-file raw text: no line-number prefixes or notices.
    let sample = "alpha\nbeta\ngamma\n";
    scratch.file("sample.txt", sample.as_bytes());
    let owned = read_core(
        ReadRequest {
            path: "sample.txt".into(),
            offset: None,
            limit: None,
        },
        Arc::clone(&launch),
        false,
        ReadPolicy::PROGRAM,
        None,
        None,
        Default::default(),
    )
    .await;
    assert!(pool.used() > 0);
    match owned.value.as_ref().unwrap() {
        FileReadData::Text(slice) => {
            assert_eq!(slice.text, sample);
            assert_eq!(slice.start_line, 1);
            assert_eq!(slice.end_line, Some(3));
            assert_eq!(slice.shown_lines, 3);
            assert_eq!(slice.total_lines, 3);
            assert!(!slice.truncated);
            assert_eq!(slice.next_offset, None);
            assert!(!slice.lossy);
        }
        FileReadData::Media(_) => panic!("expected text slice"),
    }
    drop(owned);
    assert_eq!(pool.used(), 0);

    // Sliced read: raw lines only, explicit control metadata, zero inline notices.
    let sliced = read_core(
        ReadRequest {
            path: "sample.txt".into(),
            offset: Some(1),
            limit: Some(2),
        },
        Arc::clone(&launch),
        false,
        ReadPolicy::PROGRAM,
        None,
        None,
        Default::default(),
    )
    .await;
    assert!(pool.used() > 0);
    match sliced.value.as_ref().unwrap() {
        FileReadData::Text(slice) => {
            assert_eq!(slice.text, "alpha\nbeta\n");
            assert!(!slice.text.contains("more lines"));
            assert!(!slice.text.contains("Showing lines"));
            assert_eq!(slice.start_line, 1);
            assert_eq!(slice.end_line, Some(2));
            assert_eq!(slice.shown_lines, 2);
            assert_eq!(slice.total_lines, 3);
            assert!(slice.truncated);
            assert_eq!(slice.next_offset, Some(3));
        }
        FileReadData::Media(_) => panic!("expected text slice"),
    }
    drop(sliced);
    assert_eq!(pool.used(), 0);

    // >4 MiB file with strict_whole_file: true returns FileError::Limit.
    scratch.file("over4m.txt", &vec![b'a'; (4 << 20) + 1]);
    let limit_owned = read_core(
        ReadRequest {
            path: "over4m.txt".into(),
            offset: None,
            limit: None,
        },
        Arc::clone(&launch),
        false,
        ReadPolicy::PROGRAM,
        None,
        None,
        Default::default(),
    )
    .await;
    assert!(pool.used() > 0);
    let limit_err = limit_owned.value.as_ref().unwrap_err();
    assert!(matches!(limit_err, FileError::Limit(_)), "{limit_err:?}");
    drop(limit_owned);
    assert_eq!(pool.used(), 0);

    // Invalid UTF-8 file with strict_whole_file: true returns FileError::Io (not Limit).
    scratch.file("bad_utf8.txt", &[b'h', b'i', 0xff, b'\n']);
    let utf8_owned = read_core(
        ReadRequest {
            path: "bad_utf8.txt".into(),
            offset: None,
            limit: None,
        },
        Arc::clone(&launch),
        false,
        ReadPolicy::PROGRAM,
        None,
        None,
        Default::default(),
    )
    .await;
    assert!(pool.used() > 0);
    let utf8_err = utf8_owned.value.as_ref().unwrap_err();
    assert!(matches!(utf8_err, FileError::Io(_)), "{utf8_err:?}");
    assert!(utf8_err.message().contains("not valid UTF-8"), "{utf8_err}");
    drop(utf8_owned);
    assert_eq!(pool.used(), 0);

    // Missing file returns FileError::NotFound.
    let missing_owned = read_core(
        ReadRequest {
            path: "does-not-exist.txt".into(),
            offset: None,
            limit: None,
        },
        Arc::clone(&launch),
        false,
        ReadPolicy::PROGRAM,
        None,
        None,
        Default::default(),
    )
    .await;
    assert!(pool.used() > 0);
    let missing_err = missing_owned.value.as_ref().unwrap_err();
    assert!(
        matches!(missing_err, FileError::NotFound(_)),
        "{missing_err:?}"
    );
    drop(missing_owned);
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn read_core_media_descriptor_only_vs_inline() {
    let scratch = Scratch::new("read-core-media");
    let png = png_bytes(1, 1);
    scratch.file("pixel.png", &png);
    let pool = BytePool::new(16 << 20);
    let launch = Arc::new(context(&scratch.0, &scratch.0).with_memory(Arc::clone(&pool)));

    // Program policy: DescriptorOnly sniffs without loading base64.
    let descriptor = read_core(
        ReadRequest {
            path: "pixel.png".into(),
            offset: None,
            limit: None,
        },
        Arc::clone(&launch),
        false,
        ReadPolicy::PROGRAM,
        None,
        None,
        Default::default(),
    )
    .await;
    assert!(pool.used() > 0);
    match descriptor.value.as_ref().unwrap() {
        FileReadData::Media(MediaPayload::Descriptor {
            kind,
            mime_type,
            bytes,
            source_bytes,
            attachable,
            note,
            ..
        }) => {
            assert_eq!(*kind, crate::media::MediaKind::Image);
            assert_eq!(mime_type, "image/png");
            assert_eq!(*bytes, Some(png.len() as u64));
            assert_eq!(*source_bytes, None);
            assert!(*attachable);
            assert_eq!(*note, None);
        }
        other => panic!("expected MediaPayload::Descriptor, got {other:?}"),
    }
    drop(descriptor);
    assert_eq!(pool.used(), 0);

    // Program policy on oversized media: Descriptor with attachable=false and note.
    let mut big_png = png.clone();
    big_png.resize((1 << 20) + 1, 0);
    scratch.file("big.png", &big_png);
    let small_cap = Arc::new(
        LaunchContext::new(
            scratch.0.clone(),
            vec![
                ("HOME".into(), scratch.0.as_os_str().to_owned()),
                (crate::session::MAX_MEDIA_BYTES.into(), "1048576".into()),
            ],
        )
        .with_memory(Arc::clone(&pool)),
    );
    let oversized = read_core(
        ReadRequest {
            path: "big.png".into(),
            offset: None,
            limit: None,
        },
        small_cap,
        false,
        ReadPolicy::PROGRAM,
        None,
        None,
        Default::default(),
    )
    .await;
    assert!(pool.used() > 0);
    match oversized.value.as_ref().unwrap() {
        FileReadData::Media(MediaPayload::Descriptor {
            attachable,
            bytes,
            source_bytes,
            note,
            ..
        }) => {
            assert!(!attachable);
            assert_eq!(*bytes, None);
            assert_eq!(*source_bytes, Some(big_png.len() as u64));
            assert!(note.as_deref().unwrap_or("").contains("exceeds"));
        }
        other => panic!("expected oversized MediaPayload::Descriptor, got {other:?}"),
    }
    drop(oversized);
    assert_eq!(pool.used(), 0);

    // Agent policy: InlineBlock loads base64 payload and lease.
    let inline = read_core(
        ReadRequest {
            path: "pixel.png".into(),
            offset: None,
            limit: None,
        },
        launch,
        false,
        ReadPolicy::AGENT,
        None,
        None,
        Default::default(),
    )
    .await;
    assert!(pool.used() > 0);
    match inline.value.as_ref().unwrap() {
        FileReadData::Media(MediaPayload::Inline {
            kind,
            encoded_base64,
            bytes,
            ..
        }) => {
            assert_eq!(*kind, crate::media::MediaKind::Image);
            assert!(!encoded_base64.is_empty());
            assert!(*bytes > 0);
        }
        other => panic!("expected MediaPayload::Inline, got {other:?}"),
    }
    drop(inline);
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn write_core_writes_file_and_validates() {
    let scratch = Scratch::new("write-core");
    let pool = BytePool::new(16 << 20);
    let launch = Arc::new(context(&scratch.0, &scratch.0).with_memory(Arc::clone(&pool)));

    let owned = write_core(
        WriteRequest {
            path: "nested/out.txt".into(),
            content: "hello core\n".into(),
        },
        Arc::clone(&launch),
        None,
        Default::default(),
    )
    .await;
    assert!(pool.used() > 0);
    let data = owned.value.as_ref().unwrap();
    assert_eq!(data.path, scratch.path("nested/out.txt"));
    assert_eq!(data.bytes, "hello core\n".len() as u64);
    assert_eq!(scratch.read("nested/out.txt"), b"hello core\n");
    drop(owned);
    assert_eq!(pool.used(), 0);

    let err_owned = write_core(
        WriteRequest {
            path: String::new(),
            content: "x".into(),
        },
        launch,
        None,
        Default::default(),
    )
    .await;
    assert!(pool.used() > 0);
    let err = err_owned.value.as_ref().unwrap_err();
    assert!(matches!(err, FileError::Validation(_)), "{err:?}");
    drop(err_owned);
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn edit_core_returns_matches_and_structured_failures() {
    let scratch = Scratch::new("edit-core");
    let pool = BytePool::new(16 << 20);
    let launch = Arc::new(context(&scratch.0, &scratch.0).with_memory(Arc::clone(&pool)));
    scratch.file("doc.txt", "line1\n\u{201c}quoted\u{201d}\nline3\n".as_bytes());

    let ok_owned = edit_core(
        EditRequest {
            path: "doc.txt".into(),
            edits: vec![
                Edit {
                    old_text: "line1".into(),
                    new_text: "LINE1".into(),
                    start_line: None,
                    end_line: None,
                },
                Edit {
                    old_text: "\"quoted\"".into(),
                    new_text: "\"QUOTED\"".into(),
                    start_line: None,
                    end_line: None,
                },
            ],
        },
        Arc::clone(&launch),
        None,
        Default::default(),
    )
    .await;
    assert!(pool.used() > 0);
    let ok = ok_owned.value.as_ref().unwrap();
    assert_eq!(ok.path, scratch.path("doc.txt"));
    assert_eq!(ok.replacements, 2);
    assert_eq!(
        ok.edits,
        vec![
            EditMatch {
                line: 1,
                fuzzy: false,
            },
            EditMatch {
                line: 2,
                fuzzy: true,
            },
        ]
    );
    drop(ok_owned);
    assert_eq!(pool.used(), 0);

    // Structured failures: not_found + ambiguous + overlap.
    scratch.file("fail.txt", b"dup\ndup\nabcdef\n");
    let err_owned = edit_core(
        EditRequest {
            path: "fail.txt".into(),
            edits: vec![
                Edit {
                    old_text: "missing".into(),
                    new_text: "x".into(),
                    start_line: None,
                    end_line: None,
                },
                Edit {
                    old_text: "dup".into(),
                    new_text: "y".into(),
                    start_line: None,
                    end_line: None,
                },
                Edit {
                    old_text: "abcd".into(),
                    new_text: "1".into(),
                    start_line: None,
                    end_line: None,
                },
                Edit {
                    old_text: "cdef".into(),
                    new_text: "2".into(),
                    start_line: None,
                    end_line: None,
                },
            ],
        },
        Arc::clone(&launch),
        None,
        Default::default(),
    )
    .await;
    assert!(pool.used() > 0);
    let err = err_owned.value.as_ref().unwrap_err();
    assert_eq!(err.path.as_deref(), Some(scratch.path("fail.txt").as_str()));
    let failures = err.failures.as_ref().expect("structured failures");
    assert_eq!(
        failures,
        &vec![
            EditFailure {
                index: 0,
                problem: EditProblem::NotFound,
                lines: None,
                with: None,
                more: None,
            },
            EditFailure {
                index: 1,
                problem: EditProblem::Ambiguous,
                lines: Some(vec![1, 2]),
                with: None,
                more: None,
            },
            EditFailure {
                index: 2,
                problem: EditProblem::Overlap,
                lines: None,
                with: Some(vec![3]),
                more: None,
            },
            EditFailure {
                index: 3,
                problem: EditProblem::Overlap,
                lines: None,
                with: Some(vec![2]),
                more: None,
            },
        ]
    );
    assert_eq!(
        serde_json::to_value(failures).unwrap(),
        json!([
            { "index": 0, "problem": "not_found" },
            { "index": 1, "problem": "ambiguous", "lines": [1, 2] },
            { "index": 2, "problem": "overlap", "with": [3] },
            { "index": 3, "problem": "overlap", "with": [2] },
        ])
    );
    drop(err_owned);
    assert_eq!(pool.used(), 0);

    // Missing file maps to FileError::NotFound with failures: None.
    let missing_owned = edit_core(
        EditRequest {
            path: "missing.txt".into(),
            edits: vec![Edit {
                old_text: "a".into(),
                new_text: "b".into(),
                start_line: None,
                end_line: None,
            }],
        },
        launch,
        None,
        Default::default(),
    )
    .await;
    assert!(pool.used() > 0);
    let missing = missing_owned.value.as_ref().unwrap_err();
    assert!(matches!(missing.error, FileError::NotFound(_)), "{missing:?}");
    assert_eq!(missing.failures, None);
    drop(missing_owned);
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn write_and_edit_tracked_propagate_overload_when_pool_exhausted() {
    let scratch = Scratch::new("write-edit-overload");
    scratch.file("doc.txt", b"hello world\n");
    let pool = BytePool::new(64);
    let _held = pool.try_acquire(64).expect("exhaust pool");
    let launch = Arc::new(context(&scratch.0, &scratch.0).with_memory(Arc::clone(&pool)));
    let render = Render {
        output_mode: OutputMode::Both,
        structured_content: true,
    };

    let write_res = write_tracked(
        WriteRequest {
            path: "out.txt".into(),
            content: "data".into(),
        },
        Arc::clone(&launch),
        render,
        Default::default(),
    )
    .await;
    assert_eq!(write_res.result.is_error, Some(true));
    assert!(write_res.overload, "{:?}", write_res.result);

    let edit_res = edit_tracked(
        EditRequest {
            path: "doc.txt".into(),
            edits: vec![Edit {
                old_text: "hello".into(),
                new_text: "hi".into(),
                start_line: None,
                end_line: None,
            }],
        },
        launch,
        render,
        Default::default(),
    )
    .await;
    assert_eq!(edit_res.result.is_error, Some(true));
    assert!(edit_res.overload, "{:?}", edit_res.result);
}
