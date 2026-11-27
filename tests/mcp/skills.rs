use super::common::{Scratch, Server};
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, Instant};

struct RawStdio {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<Value>,
    next_id: u64,
}

impl RawStdio {
    fn start(configure: impl FnOnce(&mut Command)) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        crate::support::scrub_fastexec_env(&mut command);
        configure(&mut command);
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn fastexec");
        let stdout = child.stdout.take().unwrap();
        let (tx, lines) = channel();
        std::thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => {
                        let _ = tx.send(json!({"__ended": "eof"}));
                        return;
                    }
                    Ok(_) => match serde_json::from_str(&line) {
                        Ok(value) => {
                            let _ = tx.send(value);
                        }
                        Err(err) => {
                            let _ = tx.send(json!({"__ended": format!("invalid json: {err}")}));
                            return;
                        }
                    },
                    Err(err) => {
                        let _ = tx.send(json!({"__ended": format!("{err}")}));
                        return;
                    }
                }
            }
        });
        Self {
            stdin: child.stdin.take(),
            child,
            lines,
            next_id: 0,
        }
    }

    fn send(&mut self, msg: Value) {
        let stdin = self.stdin.as_mut().unwrap();
        writeln!(stdin, "{msg}").unwrap();
        stdin.flush().unwrap();
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        self.next_id += 1;
        let id = self.next_id;
        self.send(json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}));
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let msg = self
                .lines
                .recv_timeout(deadline - Instant::now())
                .expect("response in time");
            assert!(msg.get("__ended").is_none(), "stdout ended: {msg}");
            if msg["id"] == json!(id) {
                return msg;
            }
        }
    }

    fn modern(&mut self, method: &str, mut params: Value) -> Value {
        params["_meta"] = json!({
            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
            "io.modelcontextprotocol/clientInfo": {"name": "skills-test", "version": "1"},
            "io.modelcontextprotocol/clientCapabilities": {},
        });
        self.request(method, params)
    }
}

impl Drop for RawStdio {
    fn drop(&mut self) {
        drop(self.stdin.take());
        let deadline = Instant::now() + Duration::from_secs(5);
        while matches!(self.child.try_wait(), Ok(None)) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn write_file(path: &Path, content: &[u8]) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, content).unwrap();
}

fn create_sample_skills(root: &Path) {
    write_file(
        &root.join("alpha/SKILL.md"),
        b"---\nname: alpha\ndescription: Alpha skill\nauthor: test\n---\n# Alpha\n",
    );
    write_file(&root.join("alpha/refs/guide.md"), b"# Guide\nHello\n");
    write_file(&root.join("alpha/mod.wasm"), b"\x00asm\x01\x00\x00\x00");
    write_file(
        &root.join("beta/SKILL.md"),
        b"---\nname: beta\ndescription: Beta skill\n---\n# Beta\n",
    );
}

#[test]
fn skills_stdio_modern_full_pipeline_and_invalid_inputs() {
    let dir = Scratch::new("skills-stdio-modern");
    create_sample_skills(&dir);

    let mut server = RawStdio::start(|cmd| {
        cmd.env("FASTEXEC_SKILLS_DIR", &*dir);
    });

    // Discover advertises resources and skills extension with directoryRead.
    let discover = server.modern("server/discover", json!({}));
    assert!(discover["result"].is_object(), "{discover}");
    let caps = &discover["result"]["capabilities"];
    assert!(caps["resources"].is_object(), "{discover}");
    assert_eq!(
        caps["extensions"]["io.modelcontextprotocol/skills"],
        json!({"directoryRead": true}),
        "{discover}"
    );

    // skills/list returns both skills sorted by name.
    let listed = server.modern("skills/list", json!({}));
    let skills = listed["result"]["skills"].as_array().expect("skills array");
    assert_eq!(skills.len(), 2, "{listed}");
    assert_eq!(skills[0]["uri"], "skill://alpha/SKILL.md");
    assert_eq!(skills[0]["frontmatter"]["name"], "alpha");
    assert_eq!(skills[0]["frontmatter"]["author"], "test");
    assert_eq!(skills[1]["uri"], "skill://beta/SKILL.md");

    // skills/get returns exact skill; unknown or malformed URI returns -32602.
    let got = server.modern("skills/get", json!({"uri": "skill://alpha/SKILL.md"}));
    assert_eq!(got["result"]["skill"]["uri"], "skill://alpha/SKILL.md");
    assert_eq!(
        got["result"]["skill"]["resources"]
            .as_array()
            .unwrap()
            .len(),
        3
    );

    let missing_skill = server.modern("skills/get", json!({"uri": "skill://nope/SKILL.md"}));
    assert_eq!(
        missing_skill["error"]["code"],
        json!(-32602),
        "{missing_skill}"
    );

    let bad_uri = server.modern("skills/get", json!({"uri": "not-a-skill-uri"}));
    assert_eq!(bad_uri["error"]["code"], json!(-32602), "{bad_uri}");

    // resources/read reads text and binary resources; unknown URI returns -32602.
    let text_res = server.modern(
        "resources/read",
        json!({"uri": "skill://alpha/refs/guide.md"}),
    );
    let contents = text_res["result"]["contents"].as_array().unwrap();
    assert_eq!(contents.len(), 1);
    assert_eq!(contents[0]["uri"], "skill://alpha/refs/guide.md");
    assert_eq!(contents[0]["mimeType"], "text/markdown");
    assert_eq!(contents[0]["text"], "# Guide\nHello\n");

    let blob_res = server.modern("resources/read", json!({"uri": "skill://alpha/mod.wasm"}));
    let blob_contents = blob_res["result"]["contents"].as_array().unwrap();
    assert_eq!(blob_contents.len(), 1);
    assert_eq!(blob_contents[0]["mimeType"], "application/wasm");
    assert_eq!(blob_contents[0]["blob"], "AGFzbQEAAAA=");

    let missing_res = server.modern(
        "resources/read",
        json!({"uri": "skill://alpha/missing.txt"}),
    );
    assert_eq!(missing_res["error"]["code"], json!(-32602), "{missing_res}");

    // resources/directory/read lists direct children and marks subdirs as inode/directory.
    let dir_res = server.modern("resources/directory/read", json!({"uri": "skill://alpha"}));
    let entries = dir_res["result"]["resources"].as_array().unwrap();
    assert_eq!(entries.len(), 3, "{dir_res}");
    let refs_dir = entries
        .iter()
        .find(|e| e["name"] == "refs")
        .expect("refs subdir");
    assert_eq!(refs_dir["mimeType"], "inode/directory");

    let bad_dir = server.modern(
        "resources/directory/read",
        json!({"uri": "skill://alpha/nope"}),
    );
    assert_eq!(bad_dir["error"]["code"], json!(-32602), "{bad_dir}");

    // Invalid pagination cursors return -32602.
    for bad_cursor in ["", "0", "01", "abc", "-1", "999"] {
        let err = server.modern("skills/list", json!({"cursor": bad_cursor}));
        assert_eq!(
            err["error"]["code"],
            json!(-32602),
            "skills/list cursor={bad_cursor:?}: {err}"
        );
        let err = server.modern(
            "resources/directory/read",
            json!({"uri": "skill://alpha", "cursor": bad_cursor}),
        );
        assert_eq!(
            err["error"]["code"],
            json!(-32602),
            "resources/directory/read cursor={bad_cursor:?}: {err}"
        );
    }

    // Inline 2026-07-28 session omitting _meta must return -32602 (not -32601).
    let no_meta = server.request("skills/get", json!({"uri": "skill://alpha/SKILL.md"}));
    assert_eq!(
        no_meta["error"]["code"],
        json!(-32602),
        "inline session without _meta should return -32602: {no_meta}"
    );
}

#[test]
fn skills_legacy_stdio_zero_fingerprint_isolation() {
    let dir = Scratch::new("skills-stdio-legacy");
    create_sample_skills(&dir);

    for claimed_version in ["2025-06-18", "2025-11-25", "2026-07-28"] {
        let mut server = RawStdio::start(|cmd| {
            cmd.env("FASTEXEC_SKILLS_DIR", &*dir);
        });
        let init = server.request(
            "initialize",
            json!({
                "protocolVersion": claimed_version,
                "capabilities": {},
                "clientInfo": {"name": "legacy-test", "version": "1"}
            }),
        );
        assert!(init["result"].is_object(), "{init}");
        let caps = &init["result"]["capabilities"];
        assert!(
            caps.get("resources").is_none(),
            "legacy init leaked resources ({claimed_version}): {init}"
        );
        assert!(
            caps.get("extensions")
                .and_then(|e| e.get("io.modelcontextprotocol/skills"))
                .is_none(),
            "legacy init leaked skills extension ({claimed_version}): {init}"
        );
        server.send(json!({"jsonrpc": "2.0", "method": "notifications/initialized"}));

        for (method, params) in [
            ("skills/list", json!({})),
            ("skills/get", json!({"uri": "skill://alpha/SKILL.md"})),
            ("skills/get", json!({"uri": "bad"})),
            ("resources/read", json!({"uri": "skill://alpha/SKILL.md"})),
            ("resources/directory/read", json!({"uri": "skill://alpha"})),
            ("resources/list", json!({})),
            ("resources/templates/list", json!({})),
        ] {
            let reply = server.request(method, params);
            assert_eq!(
                reply["error"]["code"],
                json!(-32601),
                "legacy {claimed_version} {method}: {reply}"
            );
        }
    }

    // Without FASTEXEC_SKILLS_DIR, modern 2026-07-28 also hides skills/resources.
    let mut disabled = RawStdio::start(|_| {});
    let discover = disabled.modern("server/discover", json!({}));
    assert!(
        discover["result"]["capabilities"]
            .get("resources")
            .is_none()
    );
    assert!(
        discover["result"]["capabilities"]["extensions"]
            .get("io.modelcontextprotocol/skills")
            .is_none()
    );
    let reply = disabled.modern("skills/list", json!({}));
    assert_eq!(reply["error"]["code"], json!(-32601), "{reply}");
}

#[test]
fn skills_daemon_on_stdio_full_pipeline() {
    let dir = Scratch::new("skills-daemon-on");
    let skills_root = dir.join("skills");
    let runtime = dir.join("runtime");
    std::fs::create_dir_all(&runtime).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&runtime, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    create_sample_skills(&skills_root);

    let ns = format!("skills-on-{}", std::process::id());
    let mut server = RawStdio::start(|cmd| {
        cmd.env("FASTEXEC_DAEMON", "on")
            .env("FASTEXEC_TEST_RUNTIME_DIR", &runtime)
            .env("FASTEXEC_NAMESPACE", &ns)
            .env("FASTEXEC_SKILLS_DIR", &skills_root);
    });

    let listed = server.modern("skills/list", json!({}));
    assert_eq!(
        listed["result"]["skills"].as_array().unwrap().len(),
        2,
        "{listed}"
    );
    let read = server.modern(
        "resources/read",
        json!({"uri": "skill://alpha/refs/guide.md"}),
    );
    assert_eq!(
        read["result"]["contents"][0]["text"],
        json!("# Guide\nHello\n"),
        "{read}"
    );
    // Exec tool still routes to the background daemon.
    let exec = server.modern(
        "tools/call",
        json!({
            "name": "exec",
            "arguments": {"action": "start", "command": "echo daemon-ok", "waitMs": 5000}
        }),
    );
    assert_ne!(exec["result"]["isError"], json!(true), "{exec}");
    assert!(
        exec["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or_default()
            .contains("daemon-ok"),
        "{exec}"
    );

    drop(server);
    let mut stop = Command::new(env!("CARGO_BIN_EXE_fastexec"));
    crate::support::scrub_fastexec_env(&mut stop);
    let _ = stop
        .args(["daemon", "stop"])
        .env("FASTEXEC_TEST_RUNTIME_DIR", &runtime)
        .env("FASTEXEC_NAMESPACE", &ns)
        .output();
}

#[cfg(debug_assertions)]
#[test]
fn skills_stdio_main_pool_exhausted_returns_32603_and_exec_list_succeeds() {
    let dir = Scratch::new("skills-stdio-oom");
    let big_body = "x".repeat(96 * 1024);
    write_file(
        &dir.join("big/SKILL.md"),
        b"---\nname: big\ndescription: Big skill\n---\n# Big\n",
    );
    write_file(&dir.join("big/payload.txt"), big_body.as_bytes());

    let mut server = RawStdio::start(|cmd| {
        cmd.env("FASTEXEC_SKILLS_DIR", &*dir)
            .env("FASTEXEC_TEST_MEMORY_LIMIT", "65536");
    });

    // Metadata fits in 64 KiB, but reading the 96 KiB resource exhausts the main pool -> -32603.
    let err = server.modern("resources/read", json!({"uri": "skill://big/payload.txt"}));
    assert_eq!(err["error"]["code"], json!(-32603), "{err}");

    // Control pool (8 MiB) remains untouched: exec(action: "list") succeeds immediately.
    let list = server.modern(
        "tools/call",
        json!({
            "name": "exec",
            "arguments": {"action": "list", "scope": "all"}
        }),
    );
    assert!(list["result"].is_object(), "{list}");
    assert_ne!(list["result"]["isError"], json!(true), "{list}");

    // Ensure standard Server helper remains exercised without warnings.
    let _ = std::any::type_name::<Server>();
}
