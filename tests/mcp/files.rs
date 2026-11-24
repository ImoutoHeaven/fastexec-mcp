#[cfg(windows)]
use super::common::codemode_server;
#[cfg(windows)]
use super::common::log_path_of;
use super::common::{
    Scratch, Server, bare_id, body_of, code_result, code_text, scratch, task_id_of,
};
use serde_json::{Value, json};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

#[test]
fn edit_refuses_ambiguity_hidden_by_normalization() {
    let dir = Scratch::new("edit-ambiguity");
    std::fs::create_dir_all(&*dir).unwrap();
    let mut server = Server::start_with(|command| {
        command.current_dir(&*dir);
    });
    for (content, old, new, batch) in [
        ("cafe\u{301} cafe", "cafe", "coffee", false),
        ("cafe\u{301} cafe\u{301}", "cafe", "coffee", false),
        ("a\u{301} e\u{301}", "\u{301}", "", false),
        (
            "header\nJose\u{301} Martin\nJose\u{301} Perez\n",
            "Jose",
            "Joe",
            true,
        ),
        ("cafe cafe", "cafe", "coffee", false),
        ("e\u{301}e\u{301}e\u{301}", "e\u{301}e", "X", false),
        ("\u{ff41}aa\u{301}", "aa", "X", false),
        ("it's it\u{2019}s", "it's", "it is", false),
        (
            "cafe\u{301} \u{ff43}\u{ff41}\u{ff46}\u{ff45}",
            "cafe",
            "coffee",
            false,
        ),
        (
            "\u{ff43}\u{ff41}\u{ff46}\u{ff45} cafe\u{301}",
            "cafe",
            "coffee",
            false,
        ),
    ] {
        let path = dir.join("case.txt");
        std::fs::write(&path, content).unwrap();
        let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
        let mut edits = Vec::new();
        if batch {
            edits.push(json!({"oldText": "header", "newText": "HEADER"}));
        }
        edits.push(json!({"oldText": old, "newText": new}));
        let (text, error) = server.file("edit", json!({"path": "case.txt", "edits": edits}));
        assert!(error, "{content:?} / {old:?}: {text}");
        #[cfg(unix)]
        let shown = std::fs::canonicalize(&*dir)
            .unwrap()
            .join("case.txt")
            .display()
            .to_string();
        #[cfg(windows)]
        let shown = path.to_string_lossy().replace('\\', "/");
        let target = if batch { "edits[1]" } else { "edits[0]" };
        assert!(
            text.starts_with(&format!("Cannot edit {shown}; nothing was changed:\n- {target}: oldText occurs 2 times, starting at lines ")),
            "{text}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), content.as_bytes());
        assert_eq!(
            std::fs::metadata(&path).unwrap().modified().unwrap(),
            modified
        );
    }
}

#[test]
fn edit_allows_a_unique_exact_match_changed_by_normalization() {
    let dir = Scratch::new("edit-unique");
    std::fs::create_dir_all(&*dir).unwrap();
    let mut server = Server::start_with(|command| {
        command.current_dir(&*dir);
    });
    for (content, old, new, expected) in [
        ("cafe\u{301}", "cafe", "coffee", "coffee\u{301}"),
        ("a\u{301}", "\u{301}", "", "a"),
        ("a ", "a ", "X", "X"),
        ("a \n", "a \n", "X\n", "X\n"),
        (" \na \n", " \na \n", "X\n", "X\n"),
    ] {
        let path = dir.join("case.txt");
        std::fs::write(&path, content).unwrap();
        let (text, error) = server.file(
            "edit",
            json!({"path": "case.txt", "edits": [{"oldText": old, "newText": new}]}),
        );
        #[cfg(unix)]
        let shown = std::fs::canonicalize(&*dir)
            .unwrap()
            .join("case.txt")
            .display()
            .to_string();
        #[cfg(windows)]
        let shown = path.to_string_lossy().replace('\\', "/");
        assert!(!error, "{content:?} / {old:?}: {text}");
        assert_eq!(
            text,
            format!("Successfully replaced 1 block(s) in {shown}.\nedits[0]: line 1")
        );
        assert_eq!(std::fs::read(&path).unwrap(), expected.as_bytes());
    }
}

#[test]
fn file_tools_round_trip_through_the_servers_directory() {
    let dir = Scratch::new("files");
    std::fs::create_dir_all(&*dir).unwrap();
    let mut server = Server::start_with(|command| {
        command.current_dir(&*dir);
    });
    #[cfg(unix)]
    let base_dir = std::fs::canonicalize(&*dir).unwrap();
    #[cfg(windows)]
    let base_dir = dir.to_path_buf();
    let shown_note = base_dir
        .join("notes/a.txt")
        .to_string_lossy()
        .replace('\\', "/");
    let shown_missing = base_dir
        .join("notes/missing.txt")
        .to_string_lossy()
        .replace('\\', "/");
    let (text, error) = server.file(
        "write",
        json!({"path": "notes/a.txt", "content": "one\ntwo\n"}),
    );
    assert!(!error, "{text}");
    assert_eq!(text, format!("Successfully wrote to {shown_note}"));
    let (text, error) = server.file("read", json!({"path": "notes/a.txt"}));
    assert!(!error, "{text}");
    assert_eq!(text, "one\ntwo\n");
    let (text, error) = server.file(
        "edit",
        json!({"path": "notes/a.txt", "edits": [{"oldText": "two", "newText": "TWO"}]}),
    );
    assert!(!error, "{text}");
    assert_eq!(
        text,
        format!("Successfully replaced 1 block(s) in {shown_note}.\nedits[0]: line 2")
    );
    let (text, _) = server.file("read", json!({"path": "notes/a.txt"}));
    assert_eq!(text, "one\nTWO\n");
    // The path resolved against the server's own directory, which is where the file now is.
    assert_eq!(
        std::fs::read(dir.join("notes/a.txt")).unwrap(),
        b"one\nTWO\n"
    );
    // A missing file is an error result, not a protocol failure.
    let (text, error) = server.file("read", json!({"path": "notes/missing.txt"}));
    assert!(error, "{text}");
    assert!(
        text.starts_with(&format!("Cannot read {shown_missing}: ")),
        "{text}"
    );
}

#[test]
fn read_answers_a_png_with_a_text_block_and_an_image_block() {
    let dir = Scratch::new("image");
    std::fs::create_dir_all(&*dir).unwrap();
    let image =
        image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(8, 6, image::Rgb([7, 8, 9])));
    let mut png = Vec::new();
    image
        .write_to(&mut std::io::Cursor::new(&mut png), image::ImageFormat::Png)
        .unwrap();
    std::fs::write(dir.join("shot.png"), &png).unwrap();
    let mut server = Server::start_with(|command| {
        command.current_dir(&*dir);
    });
    let message = server.request(
        "tools/call",
        json!({"name": "read", "arguments": {"path": "shot.png"}}),
    );
    let result = &message["result"];
    assert_eq!(result["isError"], json!(false), "{message}");
    let content = result["content"].as_array().expect("content blocks");
    assert_eq!(content.len(), 2, "{message}");
    assert_eq!(content[0]["type"], json!("text"));
    assert_eq!(
        content[0]["text"],
        json!(format!("Read image file [image/png] ({} B)", png.len()))
    );
    assert_eq!(content[1]["type"], json!("image"));
    assert_eq!(content[1]["mimeType"], json!("image/png"));
    assert_eq!(
        content[1]["data"],
        json!(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            &png
        ))
    );
}

#[test]
fn read_omits_an_image_past_the_encoded_ceiling_without_loading_it() {
    let dir = Scratch::new("image-oversized");
    std::fs::create_dir_all(&*dir).unwrap();
    // Sparse: a PNG signature and IHDR at the front, with size exceeding FASTEXEC_MAX_MEDIA_BYTES
    // (default 100 MiB), so stat refuses it without loading the sparse body.
    let path = dir.join("huge.png");
    let mut file = std::fs::File::create(&path).unwrap();
    std::io::Write::write_all(&mut file, b"\x89PNG\r\n\x1a\n\x00\x00\x00\x0dIHDR").unwrap();
    let size: u64 = (100 << 20) + 1;
    file.set_len(size).unwrap();
    drop(file);
    let mut server = Server::start_with(|command| {
        command.current_dir(&*dir);
    });
    let message = server.request(
        "tools/call",
        json!({"name": "read", "arguments": {"path": "huge.png"}}),
    );
    let result = &message["result"];
    assert_eq!(result["isError"], json!(false), "{message}");
    assert!(result.get("structuredContent").is_none(), "{message}");
    let content = result["content"].as_array().expect("content blocks");
    assert_eq!(content.len(), 1, "no image is attached: {message}");
    let text = content[0]["text"].as_str().unwrap();
    assert!(
        text.contains("image/png")
            && text.contains("exceeds FASTEXEC_MAX_MEDIA_BYTES (104857600 bytes)")
            && text.contains("Inspect it with exec."),
        "{text}"
    );
    let _ = size;
}

#[test]
fn multimodal_read_delivers_pdf_audio_video_image_and_compress_and_env_bounds() {
    let dir = Scratch::new("multimodal-e2e");
    std::fs::create_dir_all(&*dir).unwrap();

    // Invalid FASTEXEC_IMAGE_COMPRESS and FASTEXEC_MAX_MEDIA_BYTES exit 1 at stdio startup.
    for (var, bad) in [
        ("FASTEXEC_IMAGE_COMPRESS", "yes"),
        ("FASTEXEC_MAX_MEDIA_BYTES", "1048575"),
        ("FASTEXEC_MAX_MEDIA_BYTES", "377487361"),
        ("FASTEXEC_MAX_MEDIA_BYTES", "abc"),
    ] {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_fastexec"))
            .env(var, bad)
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(1), "{var}={bad}: {out:?}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains(var), "{stderr}");
    }

    let pdf = b"%PDF-1.7\n%\xE2\xE3\xCF\xD3\n1 0 obj\n<< /Type /Catalog >>\nendobj\n";
    std::fs::write(dir.join("spec #1.pdf"), pdf).unwrap();

    let mut wav = b"RIFF\x24\x00\x00\x00WAVEfmt \x10\x00\x00\x00".to_vec();
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&1u16.to_le_bytes());
    wav.extend_from_slice(&44_100u32.to_le_bytes());
    wav.extend_from_slice(&[0u8; 8]);
    std::fs::write(dir.join("clip.wav"), &wav).unwrap();

    let mut mp4 = Vec::new();
    mp4.extend_from_slice(&20u32.to_be_bytes());
    mp4.extend_from_slice(b"ftypisom\0\0\0\0mp42");
    std::fs::write(dir.join("movie.mp4"), &mp4).unwrap();

    let wide_img = image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(
        2400,
        40,
        image::Rgb([20, 40, 60]),
    ));
    let mut wide_png = Vec::new();
    wide_img
        .write_to(
            &mut std::io::Cursor::new(&mut wide_png),
            image::ImageFormat::Png,
        )
        .unwrap();
    std::fs::write(dir.join("wide.png"), &wide_png).unwrap();

    let b64 = |raw: &[u8]| base64::Engine::encode(&base64::engine::general_purpose::STANDARD, raw);

    // Default `FASTEXEC_IMAGE_COMPRESS=false` passes through exact bytes even for 2400x40 PNG
    // and omits structuredContent on every media read response.
    {
        let mut server = Server::start_with(|cmd| {
            cmd.current_dir(&*dir);
        });

        let pdf_res = &server.request(
            "tools/call",
            json!({"name": "read", "arguments": {"path": "spec #1.pdf", "offset": 2, "limit": 5}}),
        )["result"];
        assert_eq!(pdf_res["isError"], json!(false), "{pdf_res}");
        assert!(pdf_res.get("structuredContent").is_none(), "{pdf_res}");
        assert_eq!(pdf_res["content"][1]["type"], json!("resource"));
        assert_eq!(
            pdf_res["content"][1]["resource"]["mimeType"],
            json!("application/pdf")
        );
        assert_eq!(pdf_res["content"][1]["resource"]["blob"], json!(b64(pdf)));
        let uri = pdf_res["content"][1]["resource"]["uri"].as_str().unwrap();
        assert!(
            uri.starts_with("file:///") && uri.ends_with("spec%20%231.pdf"),
            "{uri}"
        );

        let wav_res = &server.request(
            "tools/call",
            json!({"name": "read", "arguments": {"path": "clip.wav"}}),
        )["result"];
        assert!(wav_res.get("structuredContent").is_none(), "{wav_res}");
        assert_eq!(wav_res["content"][1]["type"], json!("audio"));
        assert_eq!(wav_res["content"][1]["mimeType"], json!("audio/wav"));
        assert_eq!(wav_res["content"][1]["data"], json!(b64(&wav)));

        let mp4_res = &server.request(
            "tools/call",
            json!({"name": "read", "arguments": {"path": "movie.mp4"}}),
        )["result"];
        assert!(mp4_res.get("structuredContent").is_none(), "{mp4_res}");
        assert_eq!(mp4_res["content"][1]["type"], json!("resource"));
        assert_eq!(
            mp4_res["content"][1]["resource"]["mimeType"],
            json!("video/mp4")
        );
        assert_eq!(mp4_res["content"][1]["resource"]["blob"], json!(b64(&mp4)));

        let png_res = &server.request(
            "tools/call",
            json!({"name": "read", "arguments": {"path": "wide.png"}}),
        )["result"];
        assert!(png_res.get("structuredContent").is_none(), "{png_res}");
        assert_eq!(png_res["content"][1]["data"], json!(b64(&wide_png)));
    }

    // With FASTEXEC_IMAGE_COMPRESS=true, wide.png is scaled down and reports original/displayed metadata.
    let mut compressing = Server::start_with(|cmd| {
        cmd.current_dir(&*dir)
            .env("FASTEXEC_IMAGE_COMPRESS", "true");
    });
    let scaled = &compressing.request(
        "tools/call",
        json!({"name": "read", "arguments": {"path": "wide.png"}}),
    )["result"];
    assert!(scaled.get("structuredContent").is_none(), "{scaled}");
    assert!(
        scaled["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("original 2400x40, displayed at 2000x33"),
        "{scaled}"
    );

    #[cfg(debug_assertions)]
    {
        // Leave room for the read chunk, paths and request workspace. The larger
        // PDF stays below the file cap but cannot fit this active-memory pool.
        let memory_limit = 256 * 1024;
        let mut large_pdf = pdf.to_vec();
        large_pdf.resize(memory_limit, b' ');
        std::fs::write(dir.join("budget.pdf"), &large_pdf).unwrap();
        let mut tight = Server::start_with(|cmd| {
            cmd.current_dir(&*dir)
                .env("FASTEXEC_MAX_MEDIA_BYTES", (1024 * 1024).to_string())
                .env("FASTEXEC_TEST_MEMORY_LIMIT", memory_limit.to_string());
        });
        let first = &tight.request(
            "tools/call",
            json!({"name": "read", "arguments": {"path": "spec #1.pdf"}}),
        )["result"];
        assert_eq!(first["isError"], json!(false), "{first}");
        assert!(first.get("structuredContent").is_none(), "{first}");
        assert_eq!(first["content"][1]["resource"]["blob"], json!(b64(pdf)));
        let overloaded = &tight.request(
            "tools/call",
            json!({"name": "read", "arguments": {"path": "budget.pdf"}}),
        )["result"];
        assert_eq!(overloaded["isError"], json!(true), "{overloaded}");
        assert!(
            overloaded.get("structuredContent").is_none(),
            "{overloaded}"
        );
        assert!(
            overloaded["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains(&format!(
                    "server active memory budget ({memory_limit} bytes) is full"
                )),
            "{overloaded}"
        );
        let fits = &tight.request(
            "tools/call",
            json!({"name": "read", "arguments": {"path": "spec #1.pdf"}}),
        )["result"];
        assert_eq!(fits["isError"], json!(false), "{fits}");
        assert!(fits.get("structuredContent").is_none(), "{fits}");
        assert_eq!(fits["content"][1]["resource"]["blob"], json!(b64(pdf)));
    }
}

#[cfg(unix)]
#[test]
fn file_tools_preserve_the_original_after_an_os_write_failure() {
    use std::os::unix::process::CommandExt;
    let directory = Scratch::new("atomic-save-mcp");
    std::fs::create_dir(&directory.0).unwrap();
    let target = directory.join("target.txt");
    let original = format!("{}needle\n", "original ".repeat(50));
    std::fs::write(&target, &original).unwrap();
    let mut server = Server::start_with(|command| {
        // This is a fresh, isolated server; no other test or daemon inherits the limit.
        unsafe {
            command.pre_exec(|| {
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
    });
    for (tool, arguments) in [
        (
            "write",
            json!({"path": target, "content": "N".repeat(2048)}),
        ),
        (
            "edit",
            json!({"path": target, "edits": [{"oldText": "needle", "newText": "N".repeat(2048)}]}),
        ),
    ] {
        let (text, error) = server.file(tool, arguments);
        assert!(error, "{tool} must report the OS failure: {text}");
        assert_eq!(
            std::fs::read(&target).unwrap(),
            original.as_bytes(),
            "{tool} corrupted the original"
        );
        assert_eq!(
            std::fs::read_dir(&directory.0).unwrap().count(),
            1,
            "failed staging files must be removed"
        );
    }
}

#[cfg(unix)]
#[test]
fn editing_a_fifo_returns_an_error_without_blocking_other_file_tools() {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::FileTypeExt;

    let directory = Scratch::new("fifo-edit-mcp");
    std::fs::create_dir(&directory.0).unwrap();
    let fifo = directory.join("pipe");
    let path = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
    // No writer opens this FIFO. A blocking read would hold the global mutation lock forever.
    assert_eq!(unsafe { libc::mkfifo(path.as_ptr(), 0o600) }, 0);
    let target = directory.join("regular.txt");
    let mut server = Server::start();
    for (id, tool, arguments) in [
        (
            1001,
            "edit",
            json!({"path": fifo, "edits": [{"oldText": "needle", "newText": "changed"}]}),
        ),
        (
            1002,
            "write",
            json!({"path": target, "content": "independent file"}),
        ),
        (1003, "read", json!({"path": fifo})),
    ] {
        server.send(json!({"jsonrpc": "2.0", "id": id, "method": "tools/call",
            "params": {"name": tool, "arguments": arguments}}));
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut replies = Vec::new();
    while replies.len() < 3 {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let message = server
            .lines
            .recv_timeout(remaining)
            .expect("a FIFO must not block its edit or an unrelated file mutation");
        if message["id"]
            .as_u64()
            .is_some_and(|id| (1001..=1003).contains(&id))
        {
            replies.push(message);
        } else {
            assert!(message["__stdout_ended"].is_null(), "{message}");
        }
    }
    for id in [1001, 1003] {
        let reply = replies.iter().find(|reply| reply["id"] == id).unwrap();
        assert_eq!(reply["result"]["isError"], true, "{reply}");
        assert!(
            reply["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("not a regular file"),
            "{reply}"
        );
    }
    let written = replies.iter().find(|reply| reply["id"] == 1002).unwrap();
    assert_ne!(written["result"]["isError"], true, "{written}");
    assert_eq!(std::fs::read(&target).unwrap(), b"independent file");
    assert!(
        std::fs::symlink_metadata(&fifo)
            .unwrap()
            .file_type()
            .is_fifo()
    );
}

#[test]
fn file_tools_paginate_nested_and_direct_reads_cleanly() {
    let dir = Scratch::new("file-structured");
    std::fs::create_dir_all(&*dir).unwrap();
    let big: String = (1..=2500)
        .map(|line| format!("line-{line:04}-payload-text\n"))
        .collect();
    std::fs::write(dir.join("big.txt"), &big).unwrap();
    let huge_multi: String = (1..=48_000)
        .map(|line| format!("row-{line:05}-{}\n", "x".repeat(90)))
        .collect();
    std::fs::write(dir.join("huge-multi.txt"), &huge_multi).unwrap();
    let long_single = format!("{{\"blob\":\"{}\"}}\n", "y".repeat(80 * 1024));
    std::fs::write(dir.join("long-single.jsonl"), &long_single).unwrap();

    let mut server = Server::start_with(|command| {
        command
            .current_dir(&*dir)
            .env("FASTEXEC_CODEMODE", "true");
    });
    let code = r#"
const single = await tools.read({path:'big.txt'});
if (single.truncated !== false || single.shownLines !== 2500 || single.totalLines !== 2500) {
  throw new Error('2500-line file must fit in one nested read window');
}
const jsonl = await tools.read({path:'long-single.jsonl'});
if (jsonl.lineTooLong || JSON.parse(jsonl.text).blob.length !== 80 * 1024) {
  throw new Error('nested read must accept >50 KiB single lines up to 1 MiB');
}
const utf8 = "你好，fastexec 🚀";
const encoded = new TextEncoder().encode(utf8);
const roundtrip = new TextDecoder().decode(encoded);
if (roundtrip !== utf8) throw new Error('TextEncoder/TextDecoder roundtrip failed');

let offset = 1;
let pages = 0;
const parts = [];
while (offset != null) {
  const page = await tools.read({path:'huge-multi.txt', offset});
  if (page.text.includes('[Showing lines')) {
    throw new Error('truncation notice leaked into text');
  }
  parts.push(page.text);
  pages++;
  offset = page.nextOffset;
}
const joined = parts.join('');
return {singleBytes: single.text.length, pages, bytes: joined.length};
"#;
    let result = code_result(&mut server, json!({"code":code}));
    assert!(code_text(&result).contains("[exited 0]"), "{result}");
    let summary: Value = serde_json::from_str(body_of(code_text(&result))).unwrap();
    assert_eq!(summary["singleBytes"].as_u64().unwrap(), big.len() as u64);
    assert!(summary["pages"].as_u64().unwrap() >= 2, "{summary}");
    assert_eq!(summary["bytes"].as_u64().unwrap(), huge_multi.len() as u64);

    // Direct MCP read keeps the 2000-line / 50 KiB cap with pure text and no structuredContent.
    let (read_text, is_err) = server.file("read", json!({"path":"big.txt"}));
    assert!(!is_err, "{read_text}");
    assert!(read_text.contains("line-0001-payload-text"), "{read_text}");
    assert!(read_text.contains("[Showing lines 1-"), "{read_text}");
    assert!(read_text.contains("Use offset="), "{read_text}");
}

#[test]
fn paths_read_edit_list_false_null_and_cli_contracts_hold_end_to_end() {
    let (root, _) = scratch("p1-p2-contracts");
    let home_dir = root.join("home");
    let work_dir = root.join("work");
    std::fs::create_dir_all(&home_dir).unwrap();
    std::fs::create_dir_all(work_dir.join("sub")).unwrap();

    let mut server = Server::start_with(|cmd| {
        cmd.current_dir(&work_dir)
            .env("FASTEXEC_CODEMODE", "true")
            .env("HOME", &home_dir)
            .env("USERPROFILE", &home_dir);
    });

    let (w_text, w_err) = server.file(
        "write",
        json!({"path": "sub/empty.txt", "content": ""}),
    );
    assert!(!w_err, "{w_text}");
    assert!(
        !w_text.contains('\\') && w_text.contains("/sub/empty.txt"),
        "{w_text}"
    );

    let (r_empty_text, r_empty_err) = server.file(
        "read",
        json!({"path": "sub/empty.txt", "offset": 1}),
    );
    assert!(!r_empty_err, "{r_empty_text}");
    assert!(
        r_empty_text.is_empty() || r_empty_text.contains("0 lines"),
        "{r_empty_text:?}"
    );

    let (_, w_tilde_err) = server.file(
        "write",
        json!({"path": "~/tilde.txt", "content": "alpha\r\nbeta"}),
    );
    assert!(!w_tilde_err);
    assert_eq!(
        std::fs::read_to_string(home_dir.join("tilde.txt")).unwrap(),
        "alpha\r\nbeta"
    );
    let (r_tilde_text, r_tilde_err) = server.file("read", json!({"path": "~/tilde.txt"}));
    assert!(!r_tilde_err, "{r_tilde_text}");
    assert_eq!(r_tilde_text, "alpha\r\nbeta");

    let (_, r_lim0_err) = server.file("read", json!({"path": "~/tilde.txt", "limit": 0}));
    assert!(r_lim0_err);
    let (past_text, _past_err) =
        server.file("read", json!({"path": "~/tilde.txt", "offset": 99, "limit": 1}));
    assert!(
        !past_text.starts_with('\n'),
        "no leading blank lines on empty body: {past_text:?}"
    );

    #[cfg(unix)]
    {
        let real_a = root.join("real_a");
        let real_b = root.join("real_b");
        std::fs::create_dir_all(real_a.join("inner")).unwrap();
        std::fs::create_dir_all(&real_b).unwrap();
        std::fs::write(real_a.join("sibling.txt"), "via-symlink-parent\n").unwrap();
        std::fs::write(real_b.join("sibling.txt"), "lexical-wrong-target\n").unwrap();
        std::os::unix::fs::symlink(real_a.join("inner"), real_b.join("link")).unwrap();
        let sym_file = real_b
            .join("link/../sibling.txt")
            .to_string_lossy()
            .into_owned();
        let sym_dir = real_b.join("link/..").to_string_lossy().into_owned();

        let (via_read, via_read_err) = server.file("read", json!({"path": &sym_file}));
        assert!(!via_read_err, "{via_read}");
        assert_eq!(via_read, "via-symlink-parent\n");

        let (via_edit, via_edit_err) = server.file(
            "edit",
            json!({
                "path": &sym_file,
                "edits": [{"oldText": "via-symlink-parent", "newText": "edited-via-symlink"}]
            }),
        );
        assert!(!via_edit_err, "{via_edit}");
        assert_eq!(
            std::fs::read_to_string(real_a.join("sibling.txt")).unwrap(),
            "edited-via-symlink\n"
        );
        assert_eq!(
            std::fs::read_to_string(real_b.join("sibling.txt")).unwrap(),
            "lexical-wrong-target\n"
        );

        let sym_written = real_b
            .join("link/../created.txt")
            .to_string_lossy()
            .into_owned();
        let (via_write, via_write_err) = server.file(
            "write",
            json!({"path": &sym_written, "content": "created-in-real-a\n"}),
        );
        assert!(!via_write_err, "{via_write}");
        assert_eq!(
            std::fs::read_to_string(real_a.join("created.txt")).unwrap(),
            "created-in-real-a\n"
        );
        assert!(!real_b.join("created.txt").exists());

        let via_cwd = server.ok(json!({
            "action": "start",
            "loginShell": false,
            "cwd": &sym_dir,
            "command": "cat sibling.txt"
        }));
        assert_eq!(body_of(&via_cwd), "edited-via-symlink", "{via_cwd}");

        let via_cm_cwd = code_result(
            &mut server,
            json!({
                "cwd": &sym_dir,
                "code": "const r = await tools.read({path: 'sibling.txt'}); return r.text.trim();"
            }),
        );
        assert_eq!(
            body_of(code_text(&via_cm_cwd)),
            "edited-via-symlink",
            "{via_cm_cwd}"
        );
    }

    server.file(
        "write",
        json!({"path": "sub/edit.txt", "content": "L1 start\nL2 mid\nL3 end\ndup\ndup\nkeep\n"}),
    );
    let (bad_edit_text, bad_edit_err) = server.file(
        "edit",
        json!({
            "path": "sub/edit.txt",
            "edits": [
                {"oldText": "L1 start\nL2 mid\nL3 end\n", "newText": "SPAN0\n"},
                {"oldText": "L1 start", "newText": "SPAN1"},
                {"oldText": "L3 end", "newText": "SPAN2"},
                {"oldText": "missing-token", "newText": "x"},
                {"oldText": "dup", "newText": "DUP"},
                {"oldText": "keep", "newText": "keep"}
            ]
        }),
    );
    assert!(bad_edit_err, "{bad_edit_text}");
    assert!(
        bad_edit_text.contains("overlaps edits[1], edits[2]")
            && bad_edit_text.contains("not found")
            && bad_edit_text.contains("occurs 2 times"),
        "{bad_edit_text}"
    );
    let (noop_text, noop_err) = server.file(
        "edit",
        json!({
            "path": "sub/edit.txt",
            "edits": [{"oldText": "keep", "newText": "keep"}]
        }),
    );
    assert!(noop_err, "{noop_text}");
    assert!(noop_text.contains("identical content"), "{noop_text}");

    let t_ok = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "true",
        "eof": false,
        "raw": false,
        "screen": false,
        "waitMs": null
    }));
    let bare_t = bare_id(task_id_of(&t_ok));
    let (epoch, _seq) = bare_t.split_once('-').expect("epoch-seq");
    assert_eq!(epoch.len(), 8, "8-char Crockford base32 epoch: {bare_t}");

    let t_run = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "sleep 30",
        "waitMs": 50
    }));
    let run_id = task_id_of(&t_run).to_owned();
    let t_kill = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "sleep 30",
        "waitMs": 50
    }));
    server.ok(json!({"action": "kill", "taskId": task_id_of(&t_kill)}));

    let list_active = server.ok(json!({
        "action": "list",
        "state": "active",
        "raw": false,
        "eof": null
    }));
    assert_eq!(list_active.lines().count(), 1, "{list_active}");
    let list_finished = server.ok(json!({
        "action": "list",
        "state": "finished"
    }));
    assert!(
        list_finished.lines().count() >= 2,
        "{list_finished}"
    );

    let list_lim_text = server.ok(json!({"action": "list", "limit": 2}));
    assert!(list_lim_text.contains("Showing 2 of "), "{list_lim_text}");

    // P1-6: keepTree task after root exit counts as state "active" until killed.
    let kt_out = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "keepTree": true,
        "command": "sleep 30 </dev/null >/dev/null 2>&1 &"
    }));
    let kt_handle = task_id_of(&kt_out).to_owned();
    let kt_bare = bare_id(&kt_handle).to_owned();
    let active_with_kt = server.ok(json!({"action": "list", "state": "active"}));
    assert!(
        active_with_kt
            .lines()
            .any(|line| line.starts_with(&kt_bare) && line.contains("keeps ")),
        "{active_with_kt}"
    );
    server.ok(json!({"action": "kill", "taskId": &kt_handle}));

    // P1-6: A warning on an older unselected task stays out when limit excludes it.
    let _ = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "head -c 5",
        "input": "w".repeat(1 << 20)
    }));
    let text = server.ok(json!({"action": "list", "limit": 1}));
    assert!(text.contains("Warning:"), "{text}");
    assert!(
        text.contains("Showing 1 of ") && text.ends_with("tasks; raise limit to see more."),
        "{text}"
    );
    let bounded = server.ok(json!({"action":"list", "limit":1, "maxBytes":100}));
    let bounded_body = bounded.split("\n\nWarning:").next().unwrap();
    assert!(
        bounded_body.len() <= 100 && bounded.contains("Warning:"),
        "{bounded}"
    );
    assert!(bounded.ends_with("raise limit to see more."), "{bounded}");
    let _ = server.ok(json!({"action": "start", "loginShell": false, "command": "true"}));
    let latest_only_text = server.ok(json!({"action": "list", "limit": 1}));
    assert!(
        !latest_only_text.contains("Warning:"),
        "{latest_only_text}"
    );

    // P1-6: Tiny maxBytes on direct list leaves undelivered background notices due; nested list
    // marks script-owned tasks it delivers.
    let due_bg = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "sleep 0.15; exit 4",
        "waitMs": 10
    }));
    let due_bare = bare_id(task_id_of(&due_bg)).to_owned();
    std::thread::sleep(Duration::from_millis(300));
    let _ = server.ok(json!({"action": "list", "maxBytes": 100}));
    let after_tiny_list =
        server.ok(json!({"action": "start", "loginShell": false, "command": "true"}));
    assert!(
        after_tiny_list.contains(&due_bare),
        "tiny maxBytes list should leave {due_bare} due: {after_tiny_list}"
    );

    let nested_marks = code_result(
        &mut server,
        json!({
            "code": r#"
                const bg = await tools.exec({
                    action: "start",
                    loginShell: false,
                    command: "sleep 0.1; exit 5",
                    waitMs: 0
                });
                const waiter = await tools.exec({
                    action: "start",
                    loginShell: false,
                    command: "sleep 0.25"
                });
                const listed = await tools.exec({ action: "list", limit: 5 });
                const bare = bg.taskId.split(".")[0];
                if (!listed.tasks.some(t => t.taskId === bare && t.exitCode === 5)) {
                    throw new Error("nested list missed bg: " + JSON.stringify(listed));
                }
                return bare;
            "#
        }),
    );
    let nested_bare = body_of(code_text(&nested_marks)).trim().to_owned();
    assert!(
        !code_text(&nested_marks)
            .lines()
            .last()
            .unwrap()
            .contains(&nested_bare),
        "nested list should mark delivered script-owned task {nested_bare}: {}",
        code_text(&nested_marks)
    );

    server.ok(json!({"action": "kill", "taskId": run_id}));

    let (bad_id_msg, bad_id_err) = server.call(json!({"action": "poll", "taskId": "BAD_ID!"}));
    assert!(
        bad_id_err && bad_id_msg.contains("Unknown taskId"),
        "{bad_id_msg}"
    );

    let (true_msg, true_err) = server.call(json!({"action": "list", "eof": true}));
    assert!(
        true_err && true_msg.contains("action list; remove it."),
        "{true_msg}"
    );

    let pty_scr = server.ok(json!({
        "action": "start",
        "pty": true,
        "loginShell": false,
        "command": "printf hi",
        "screen": true,
        "raw": false
    }));
    assert!(pty_scr.contains("cursor row "), "{pty_scr}");

    let cm_done = code_result(&mut server, json!({"code": "return 'cm-ok';"}));
    let cm_id = task_id_of(code_text(&cm_done)).to_owned();
    server.ok(json!({
        "action": "poll",
        "taskId": cm_id,
        "eof": false,
        "screen": false,
        "raw": false,
        "input": null
    }));

    let bin_cmd = || {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        crate::support::scrub_fastexec_env(&mut command);
        command
    };
    for flag in ["--version", "-V"] {
        let out = bin_cmd().arg(flag).output().unwrap();
        assert_eq!(out.status.code(), Some(0), "{flag}: {out:?}");
        assert!(
            String::from_utf8_lossy(&out.stdout).starts_with("fastexec "),
            "{out:?}"
        );
    }
    for flag in ["--help", "-h"] {
        let out = bin_cmd().arg(flag).output().unwrap();
        assert_eq!(out.status.code(), Some(0), "{flag}: {out:?}");
        assert!(
            String::from_utf8_lossy(&out.stdout).contains("daemon"),
            "{out:?}"
        );
    }
    for bad_args in [
        &["bogus"][..],
        &["--version", "extra"][..],
        &["--help", "extra"][..],
    ] {
        let out = bin_cmd().args(bad_args).output().unwrap();
        assert_eq!(out.status.code(), Some(2), "{bad_args:?}: {out:?}");
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("unknown argument"),
            "{out:?}"
        );
    }
    let eof_stdio = bin_cmd().stdin(Stdio::null()).output().unwrap();
    assert_eq!(eof_stdio.status.code(), Some(1), "{eof_stdio:?}");
}

#[cfg(windows)]
#[test]
fn windows_git_bash_drive_paths_and_forward_slash_reports() {
    let (root, _) = scratch("win-paths-p15");
    let spaced = root.join("dir with spaces");
    std::fs::create_dir_all(&spaced).unwrap();
    let native = spaced.to_string_lossy().replace('\\', "/");
    let (drive, rest) = native.split_once(":/").expect("Windows drive letter");
    let git_bash_dir = format!("/{}/{rest}", drive.to_ascii_lowercase());
    let git_bash_file = format!("{git_bash_dir}/hello.txt");

    let mut server = codemode_server();
    let listed = server.request("tools/list", json!({}));
    let exec_desc = listed["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "exec")
        .unwrap()["description"]
        .as_str()
        .unwrap();
    let host_line = exec_desc.lines().last().unwrap();
    assert!(
        !host_line.contains('\\'),
        "Host line has backslash: {host_line}"
    );

    let (w_text, w_err) = server.file(
        "write",
        json!({"path": &git_bash_file, "content": "win-ok\n"}),
    );
    assert!(!w_err, "{w_text}");
    assert!(
        !w_text.contains('\\') && w_text.contains("/dir with spaces/hello.txt"),
        "{w_text}"
    );

    let ran = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "cwd": &git_bash_dir,
        "command": "cat hello.txt"
    }));
    assert_eq!(body_of(&ran), "win-ok", "{ran}");
    if let Some(lp) = log_path_of(&ran) {
        assert!(!lp.contains('\\'), "{ran}");
    }

    let root_drive = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "cwd": format!("/{}", drive.to_ascii_lowercase()),
        "command": "pwd"
    }));
    assert!(root_drive.contains("[exited 0]"), "{root_drive}");

    let (tmp_msg, tmp_err) = server.file("read", json!({"path": "/tmp/x"}));
    assert!(tmp_err && tmp_msg.contains("cygpath -m"), "{tmp_msg}");
    let (rel_msg, rel_err) = server.file("read", json!({"path": format!("{drive}:relative.txt")}));
    assert!(rel_err, "{rel_msg}");

    // P1-1 Windows: default start PATH matches loginShell: true order, git & ls resolve, and
    // non-login snapshot execution runs at non-login speed.
    let path_cmd = "git --version >/dev/null && ls >/dev/null && IFS=: && for p in $PATH; do cygpath -m \"$p\"; done";
    let snap_path = server.ok(json!({
        "action": "start",
        "command": path_cmd
    }));
    let login_path = server.ok(json!({
        "action": "start",
        "loginShell": true,
        "command": path_cmd
    }));
    let snap_entries: Vec<&str> = body_of(&snap_path).lines().take(5).collect();
    let login_entries: Vec<&str> = body_of(&login_path).lines().take(5).collect();
    assert_eq!(
        snap_entries, login_entries,
        "Windows snapshot PATH order must match login shell"
    );
    let begun = Instant::now();
    server.ok(json!({"action": "start", "command": "true"}));
    assert!(
        begun.elapsed() < Duration::from_millis(800),
        "expected fast non-login start on Windows, got {:?}",
        begun.elapsed()
    );
}

#[test]
fn p0_p1_p2_schemas_windows_reads_edits_and_tolerances_hold_end_to_end() {
    let dir = Scratch::new("ergonomics-e2e");
    std::fs::create_dir_all(&*dir).unwrap();
    let mut server = Server::start_with(|command| {
        command
            .current_dir(&*dir)
            .env("FASTEXEC_CODEMODE", "true");
    });

    // Schema assertions (P0-1, P0-2, P1-2, P1-4).
    let listed = server.request("tools/list", json!({}));
    let tools_arr = listed["result"]["tools"].as_array().unwrap();
    let schema_of = |name: &str| -> &Value {
        &tools_arr.iter().find(|t| t["name"] == name).unwrap()["inputSchema"]
    };
    for name in ["exec", "codemode"] {
        let s = schema_of(name);
        assert_eq!(
            s["properties"]["truncate"]["enum"],
            json!(["head_tail", "head", "tail"]),
            "{name} truncate enum"
        );
        assert_eq!(s["properties"]["maxBytes"]["minimum"], 100);
        assert_eq!(s["properties"]["maxBytes"]["maximum"], 4_194_304);
    }
    let read_schema = schema_of("read");
    assert_eq!(read_schema["properties"]["offset"]["type"], "integer");
    assert!(read_schema["properties"]["offset"].get("minimum").is_none());
    assert_eq!(read_schema["properties"]["limit"]["minimum"], 1);
    let edit_item = &schema_of("edit")["properties"]["edits"]["items"];
    assert!(edit_item.get("prefixItems").is_none());
    assert_eq!(edit_item["properties"]["startLine"]["minimum"], 1);
    assert_eq!(edit_item["properties"]["endLine"]["minimum"], 1);

    // P0-1: maxBytes 4194304 accepted with all 3 placements; 4194305 and truncate "none" rejected.
    for placement in ["head_tail", "head", "tail"] {
        let out = server.ok(json!({
            "action": "start",
            "command": "printf ok",
            "truncate": placement,
            "maxBytes": 4_194_304
        }));
        assert_eq!(body_of(&out), "ok", "{out}");
        let cm = code_result(
            &mut server,
            json!({"code": "return 'ok';", "truncate": placement, "maxBytes": 4_194_304}),
        );
        assert_eq!(body_of(code_text(&cm)), "ok", "{cm}");
    }
    let bad_cm_none = code_result(
        &mut server,
        json!({"code": "return 1;", "truncate": "none"}),
    );
    assert_eq!(bad_cm_none["isError"], true);

    // P0-2 & P1-2: directory message, offset: 0 / limit: 0 errors, whole-file vs slice reads, negative offset.
    std::fs::create_dir_all(dir.join("adir")).unwrap();
    let (dir_msg, dir_err) = server.file("read", json!({"path": "adir"}));
    assert!(
        dir_err && dir_msg.contains("it is a directory; list it with exec."),
        "{dir_msg}"
    );
    let (_, off0_err) = server.file("read", json!({"path": "adir", "offset": 0}));
    assert!(off0_err);
    let (_, lim0_err) = server.file("read", json!({"path": "adir", "limit": 0}));
    assert!(lim0_err);

    std::fs::write(dir.join("lines50k.txt"), "x\n".repeat(50_003)).unwrap();
    let mut over_1m_line = "a".repeat(1_048_577);
    over_1m_line.push_str("\ny\n");
    std::fs::write(dir.join("line1m.txt"), &over_1m_line).unwrap();
    std::fs::write(dir.join("over4m.txt"), vec![b'z'; (4 << 20) + 1]).unwrap();
    std::fs::write(dir.join("badutf8.txt"), [0x6f_u8, 0xff, 0x6b, 0x0a]).unwrap();
    let five_m: String = (1..=100_000)
        .map(|i| format!("row-{i:06}-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n"))
        .collect();
    assert!(five_m.len() > 5_000_000);
    std::fs::write(dir.join("five_m.txt"), &five_m).unwrap();

    let p02 = code_result(
        &mut server,
        json!({
            "code": r#"
                const full50k = await tools.read({ path: "lines50k.txt" });
                if (full50k.totalLines !== 50003 || full50k.text.length !== 100006 || full50k.truncated !== false || full50k.nextOffset !== undefined) {
                    throw new Error("50k mismatch: " + JSON.stringify({ ...full50k, text: full50k.text.length }));
                }
                const full1m = await tools.read({ path: "line1m.txt" });
                if (full1m.text.length !== 1048580 || full1m.lineTooLong) {
                    throw new Error("1m line mismatch: " + full1m.text.length);
                }
                let overErr = null;
                try {
                    await tools.read({ path: "over4m.txt" });
                } catch (e) {
                    overErr = e;
                }
                if (!overErr || overErr.name !== "ToolError" || overErr.code !== "LIMIT" || !overErr.message.includes("offset") || !overErr.message.includes("limit") || ("text" in (overErr.result || {}))) {
                    throw new Error("over4m error mismatch: " + (overErr && (overErr.code + ": " + overErr.message)));
                }
                let utfErr = null;
                try {
                    await tools.read({ path: "badutf8.txt" });
                } catch (e) {
                    utfErr = e;
                }
                if (!utfErr || utfErr.name !== "ToolError" || utfErr.code !== "IO") {
                    throw new Error("badutf8 whole-file must reject with IO");
                }
                const lossySlice = await tools.read({ path: "badutf8.txt", offset: 1 });
                if (lossySlice.lossy !== true || lossySlice.text !== "o\uFFFDk\n") {
                    throw new Error("badutf8 slice mismatch: " + JSON.stringify(lossySlice));
                }
                const slice5m = await tools.read({ path: "five_m.txt", offset: 1, limit: 100 });
                if (slice5m.truncated !== true || slice5m.nextOffset !== 101 || slice5m.shownLines !== 100) {
                    throw new Error("slice5m mismatch: " + JSON.stringify({ ...slice5m, text: slice5m.text.length }));
                }
                const negTail = await tools.read({ path: "lines50k.txt", offset: -50 });
                if (negTail.startLine !== 49954 || negTail.endLine !== 50003 || negTail.nextOffset !== undefined) {
                    throw new Error("negTail mismatch: " + JSON.stringify(negTail));
                }
                return "p02-ok";
            "#
        }),
    );
    assert!(code_text(&p02).contains("[exited 0]"), "{p02}");
    assert_eq!(body_of(code_text(&p02)), "p02-ok", "{p02}");

    // Direct negative offset & P1-4 edit startLine/endLine over MCP.
    let (direct_neg, direct_neg_err) =
        server.file("read", json!({"path": "lines50k.txt", "offset": -5}));
    assert!(!direct_neg_err, "{direct_neg}");
    assert_eq!(direct_neg.lines().count(), 5, "{direct_neg}");

    std::fs::write(dir.join("dup.txt"), "v = 1;\nkeep\nv = 1;\n").unwrap();
    let (scoped_edit, scoped_edit_err) = server.file(
        "edit",
        json!({
            "path": "dup.txt",
            "edits": [{"oldText": "v = 1;", "newText": "v = 9;", "startLine": 3, "endLine": 3}]
        }),
    );
    assert!(!scoped_edit_err, "{scoped_edit}");
    assert_eq!(
        std::fs::read_to_string(dir.join("dup.txt")).unwrap(),
        "v = 1;\nkeep\nv = 9;\n"
    );

    // P2-1: keys: [] (with and without keyDelayMs) on pipe, PTY, and codemode; transcript ignores waitMs.
    let pipe_task = server.ok(json!({
        "action": "start",
        "command": "printf pipe-done"
    }));
    let pipe_id = task_id_of(&pipe_task).to_owned();
    let pipe_poll = server.ok(json!({
        "action": "poll",
        "taskId": &pipe_id,
        "keys": [],
        "keyDelayMs": 500,
        "waitMs": 0
    }));
    assert!(pipe_poll.contains("[exited 0]"), "{pipe_poll}");
    let (lone_delay_text, lone_delay_err) = server.call(json!({
        "action": "poll",
        "taskId": &pipe_id,
        "keyDelayMs": 500,
        "waitMs": 0
    }));
    assert!(
        lone_delay_err && lone_delay_text.contains("keyDelayMs applies only with keys"),
        "{lone_delay_text}"
    );

    let pty_task = server.ok(json!({
        "action": "start",
        "pty": true,
        "command": "printf 'pty-ready\\n'; sleep 30",
        "returnWhen": {"outputContains": ["pty-ready"]}
    }));
    let pty_id = task_id_of(&pty_task).to_owned();
    let t0 = Instant::now();
    let pty_poll = server.ok(json!({
        "action": "poll",
        "taskId": &pty_id,
        "keys": [],
        "keyDelayMs": 500,
        "waitMs": 50
    }));
    assert!(pty_poll.contains("[running]"), "{pty_poll}");
    assert!(t0.elapsed() < Duration::from_millis(1500));

    let t_tr = Instant::now();
    let tr_out = server.ok(json!({
        "action": "transcript",
        "taskId": &pty_id,
        "waitMs": 5000
    }));
    assert!(
        t_tr.elapsed() < Duration::from_millis(1500),
        "transcript must ignore waitMs: {:?}",
        t_tr.elapsed()
    );
    assert!(tr_out.contains("pty-ready"), "{tr_out}");
    server.ok(json!({"action": "kill", "taskId": &pty_id}));

    let cm_running = code_result(
        &mut server,
        json!({
            "code": "const p = sleep(30000); text('sleep-started'); await p;",
            "waitMs": 10000,
            "returnWhen": {"outputContains": ["sleep-started"]}
        }),
    );
    let cm_id = task_id_of(code_text(&cm_running)).to_owned();
    let cm_poll = server.ok(json!({
        "action": "poll",
        "taskId": &cm_id,
        "keys": [],
        "keyDelayMs": 250,
        "waitMs": 20
    }));
    assert!(cm_poll.contains("[running]"), "{cm_poll}");
    let cm_killed_text = server.ok(json!({"action": "kill", "taskId": &cm_id}));
    assert!(cm_killed_text.contains("[killed"), "{cm_killed_text}");
    assert!(
        cm_killed_text.contains("cancelled 1 pending call(s): sleep"),
        "{cm_killed_text}"
    );
}
