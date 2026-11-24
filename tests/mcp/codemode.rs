use super::common::{
    Scratch, Server, bare_id, body_of, code_result, code_text, codemode_server, scratch,
};
use serde_json::{Value, json};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn image_count(result: &Value) -> usize {
    result["content"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|block| block["type"] == "image")
        .count()
}

fn image_data_url(width: u32, height: u32) -> String {
    let image = image::DynamicImage::ImageRgb8(image::RgbImage::from_pixel(
        width,
        height,
        image::Rgb([7, 8, 9]),
    ));
    let mut png = Vec::new();
    image
        .write_to(&mut std::io::Cursor::new(&mut png), image::ImageFormat::Png)
        .unwrap();
    format!(
        "data:image/png;base64,{}",
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, png)
    )
}

#[test]
fn codemode_requires_explicit_enable_and_respects_yolo() {
    for setting in [None, Some("false")] {
        let mut server = Server::start_with(|command| {
            if let Some(value) = setting {
                command.env("FASTEXEC_CODEMODE", value);
            }
        });
        let listed = server.request("tools/list", json!({}));
        assert!(
            !listed["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|tool| tool["name"] == "codemode"),
            "{listed}"
        );
        let reply = server.request(
            "tools/call",
            json!({"name":"codemode", "arguments":{"code":"return 1"}}),
        );
        assert!(
            reply["error"].is_object() || reply["result"]["isError"] == true,
            "{reply}"
        );
    }
    for yolo in [false, true] {
        let mut server = Server::start_with(|command| {
            command
                .env("FASTEXEC_CODEMODE", "true")
                .env("FASTEXEC_YOLO", yolo.to_string());
        });
        let listed = server.request("tools/list", json!({}));
        let tools = listed["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 5, "{listed}");
        let tool = tools
            .iter()
            .find(|tool| tool["name"] == "codemode")
            .unwrap();
        assert_eq!(tool["annotations"]["readOnlyHint"], yolo, "{tool}");
        assert_eq!(tool["annotations"]["destructiveHint"], !yolo, "{tool}");
        assert_eq!(tool["annotations"]["openWorldHint"], !yolo, "{tool}");
        assert_eq!(tool["inputSchema"]["additionalProperties"], false, "{tool}");
        assert_eq!(
            tool["inputSchema"]["properties"]["code"]["type"], "string",
            "{tool}"
        );
        assert_eq!(
            tool["inputSchema"]["properties"]["cwd"]["type"],
            json!("string"),
            "{tool}"
        );
        assert!(
            tool["description"]
                .as_str()
                .unwrap()
                .contains("Prefer this over multi-turn exec or read calls"),
            "{tool}"
        );
        assert!(
            tool["inputSchema"]["required"]
                .as_array()
                .unwrap()
                .contains(&json!("code")),
            "{tool}"
        );
        let exec = tools.iter().find(|tool| tool["name"] == "exec").unwrap();
        let description = exec["description"].as_str().unwrap();
        assert!(
            description.contains("poll, kill, and list also take codemode tasks"),
            "{description}"
        );
        assert!(
            description.lines().last().unwrap().starts_with("Host: "),
            "{description}"
        );
    }
    for bad in ["", "on", "off", "1", "0", "bogus"] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        crate::support::scrub_fastexec_env(&mut command);
        let output = command
            .env("FASTEXEC_CODEMODE", bad)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1), "{bad:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("invalid FASTEXEC_CODEMODE")
                && stderr.contains("expected true or false"),
            "{stderr}"
        );
    }
}

#[test]
fn codemode_invalid_arguments_do_not_create_tasks() {
    let mut server = codemode_server();
    // Schema-decoding failures have no structuredContent.
    for args in [
        json!({}),
        json!({"code":42}),
        json!({"code":"return 1", "pty":true}),
        json!({"code":"return 1", "returnWhen":{"screenContains":["x"]}}),
    ] {
        let result = code_result(&mut server, args);
        assert_eq!(result["isError"], true, "{result}");
        assert!(result.get("structuredContent").is_none(), "{result}");
        let (listed, _) = server.ok(json!({"action":"list"}));
        assert!(listed["tasks"].as_array().unwrap().is_empty(), "{listed}");
    }
    // Parameter and business validation failures carry structuredContent with action: "codemode".
    for args in [
        json!({"code":"  "}),
        json!({"code":"x\0y"}),
        json!({"code":"return 1", "cwd":"definitely-missing-relative-path"}),
        json!({"code":"return 1", "cwd":"/nonexistent-fastexec-dir-xyz"}),
        json!({"code":"return 1", "waitMs":240001}),
        json!({"code":"return 1", "returnWhen":{}}),
        json!({"code":"return 1", "returnWhen":{"quietMs":5}}),
        json!({"code":"return 1", "maxBytes":4194305}),
        json!({"code":"x".repeat(256 * 1024 + 1)}),
    ] {
        let result = code_result(&mut server, args);
        assert_eq!(result["isError"], true, "{result}");
        assert_eq!(result["structuredContent"]["ok"], false, "{result}");
        assert_eq!(
            result["structuredContent"]["action"], "codemode",
            "{result}"
        );
        assert!(
            result["structuredContent"]["error"]
                .as_str()
                .is_some_and(|error| !error.is_empty()),
            "{result}"
        );
        let (listed, _) = server.ok(json!({"action":"list"}));
        assert!(listed["tasks"].as_array().unwrap().is_empty(), "{listed}");
    }
}

#[test]
fn codemode_streams_script_results_and_preserves_error_lines() {
    let mut server = codemode_server();
    let result = code_result(
        &mut server,
        json!({"code":"text('first'); await Promise.resolve(); console.log('second', {ok:true}); return {last:3};"}),
    );
    assert_eq!(result["isError"], false, "{result}");
    let task = &result["structuredContent"];
    assert_eq!(task["action"], "codemode", "{result}");
    assert_eq!(task["kind"], "codemode", "{result}");
    assert_eq!(task["exitCode"], 0, "{result}");
    assert_eq!(
        task["output"], "first\nsecond {\"ok\":true}\n{\"last\":3}",
        "{result}"
    );
    for code in [
        "try { exit(); } catch { text('caught'); } text('after');",
        "return undefined;",
    ] {
        let result = code_result(&mut server, json!({"code":code}));
        assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
        assert_eq!(result["structuredContent"]["output"], "", "{result}");
    }
    for (code, expected) in [
        ("text('before');\nthrow new Error('boom');", "codemode.js:2"),
        (
            "await new Promise(() => {});",
            "promise that can never settle",
        ),
        ("const = ;", "SyntaxError"),
        ("Promise.reject(new Error('unhandled'));", "unhandled"),
    ] {
        let result = code_result(&mut server, json!({"code":code}));
        assert_eq!(result["isError"], false, "{result}");
        assert_eq!(result["structuredContent"]["exitCode"], 1, "{result}");
        assert!(
            code_text(&result).contains("Script error:") && code_text(&result).contains(expected),
            "{result}"
        );
    }
}

#[test]
fn codemode_nested_results_keep_their_shape_under_outer_render_settings() {
    let dir = Scratch::new("codemode-files");
    std::fs::create_dir_all(&*dir).unwrap();
    for structured in ["true", "false"] {
        let mut server = Server::start_with(|command| {
            command
                .current_dir(&*dir)
                .env("FASTEXEC_CODEMODE", "true")
                .env("FASTEXEC_OUTPUT_MODE", "structured")
                .env("FASTEXEC_STRUCTURED_CONTENT", structured);
        });
        let code = r#"
const written = await tools.write({path:'note.txt', content:'alpha\nbeta\n'});
const edited = await tools.edit({path:'note.txt', edits:[{oldText:'beta', newText:'BETA'}]});
const [command, file, absentOutcome] = await Promise.all([
  tools.exec({action:'start', command:'printf nested-output', loginShell:false}),
  tools.read({path:'note.txt'}),
  tools.read({path:'absent.txt'}).then(() => null, e => e)
]);
if ('ok' in written || 'structuredContent' in written || written.bytes !== 11) throw new Error('write shape');
if ('ok' in edited || edited.replacements !== 1 || edited.edits[0].line !== 2) throw new Error('edit shape');
if ('ok' in command || 'content' in command || command.output !== 'nested-output') throw new Error('exec shape');
if ('ok' in file || file.text !== 'alpha\nBETA\n' || file.truncated !== false || file.nextOffset !== null || file.totalLines !== 2) throw new Error('file shape');
if (!absentOutcome || absentOutcome.name !== 'ToolError' || absentOutcome.tool !== 'read' || typeof absentOutcome.result?.error !== 'string') throw new Error('absent shape');
const reads = await Promise.all(Array.from({length:20}, () => tools.read({path:'note.txt'})));
if (reads.length !== 20 || reads.some(r => 'ok' in r || r.text !== 'alpha\nBETA\n')) throw new Error('queued reads');
text('nested-ok');
"#;
        let result = code_result(&mut server, json!({"code":code}));
        assert_eq!(result["isError"], false, "{result}");
        let bad = code_result(&mut server, json!({"code":"   "}));
        assert_eq!(bad["isError"], true, "{bad}");
        if structured == "true" {
            assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
            assert_eq!(
                result["structuredContent"]["output"], "nested-ok",
                "{result}"
            );
            assert!(!code_text(&result).contains("nested-ok"), "{result}");
            assert_eq!(bad["structuredContent"]["action"], "codemode", "{bad}");
        } else {
            assert!(result.get("structuredContent").is_none(), "{result}");
            assert!(bad.get("structuredContent").is_none(), "{bad}");
            assert!(
                code_text(&result).contains("nested-ok")
                    && !code_text(&result).contains("Script error:"),
                "{result}"
            );
        }
    }
}

#[test]
fn codemode_rejects_bad_nested_calls_and_survives_changed_prototypes() {
    let mut server = codemode_server();
    let code = r#"
let rejected = 0;
for (const call of [
  () => tools.read(null), () => tools.read({path:'x', bogus:true}),
  () => tools.exec({action:'bogus'}), () => tools.exec({action:'list', bogus:true}),
  () => tools.codemode({code:'return 1'})
]) { try { await call(); } catch (e) { if (e instanceof TypeError) rejected++; } }
if (rejected !== 5) throw new Error('invalid calls resolved: ' + rejected);
for (const call of [
  () => tools.exec({action:'start'}),
  () => tools.exec({action:'start', command:''}),
  () => tools.exec({action:'start', command:'true', maxBytes:99}),
  () => tools.edit({path:'x', edits:[]})
]) {
  let err = null;
  try { await call(); } catch (e) { err = e; }
  if (!err || err.name !== 'ToolError' || typeof err.result?.error !== 'string') throw new Error('validation failure did not reject with ToolError');
}
const longWait = await tools.exec({action:'start', command:'true', waitMs:600000, loginShell:false});
if (longWait.exitCode !== 0) throw new Error('nested waitMs > 240000 must succeed');
const pending = tools.read({path:'a-file-that-does-not-exist'});
Map.prototype.get = Map.prototype.set = Map.prototype.delete = () => { throw new Error('changed Map'); };
Promise.prototype.then = () => { throw new Error('changed Promise'); };
JSON.parse = JSON.stringify = () => { throw new Error('changed JSON'); };
let missingErr = null;
try { await pending; } catch (e) { missingErr = e; }
if (!missingErr || missingErr.name !== 'ToolError') throw new Error('missing file did not reject with ToolError');
text('settled');
"#;
    let result = code_result(&mut server, json!({"code":code}));
    assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
    assert_eq!(result["structuredContent"]["output"], "settled", "{result}");
}

#[test]
fn codemode_kill_reports_pending_calls_and_leaves_started_tasks_running() {
    let mut server = codemode_server();
    let result = code_result(
        &mut server,
        json!({
            "code":"const child = await tools.exec({action:'start',command:'sleep 30',waitMs:0,loginShell:false}); text('child=' + child.taskId); const waiting = tools.exec({action:'poll',taskId:child.taskId}); text('waiting'); await waiting;",
            "waitMs":10000, "returnWhen":{"outputContains":["waiting"]}
        }),
    );
    assert_eq!(result["isError"], false, "{result}");
    let script = result["structuredContent"]["taskId"]
        .as_str()
        .unwrap()
        .to_owned();
    let child = code_text(&result)
        .lines()
        .find_map(|line| line.strip_prefix("child="))
        .unwrap()
        .to_owned();
    let (_, ok_text, ok_err) = server.call(json!({"action":"poll", "taskId":script, "waitMs":0, "eof":false, "screen":false, "raw":false}));
    assert!(!ok_err, "{ok_text}");
    for extra in [
        json!({"input":"x"}),
        json!({"eof":true}),
        json!({"keys":["Enter"]}),
        json!({"keyDelayMs":0}),
        json!({"cols":80}),
        json!({"rows":24}),
        json!({"screen":true}),
        json!({"raw":true}),
        json!({"encoding":"utf-8"}),
        json!({"returnWhen":{"screenContains":["x"]}}),
        json!({"returnWhen":{"screenAppears":["x"]}}),
        json!({"returnWhen":{"screenGone":["x"]}}),
    ] {
        let mut args = json!({"action":"poll", "taskId":script, "waitMs":0});
        args.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let (_, text, error) = server.call(args);
        assert!(
            error && text.contains("A codemode task takes no input"),
            "{text}"
        );
    }
    let (_, text, error) = server.call(json!({"action":"transcript", "taskId":script}));
    assert!(error && text.to_lowercase().contains("terminal"), "{text}");
    let (stopped, text) = server.ok(json!({"action":"kill", "taskId":script}));
    assert_eq!(stopped["state"], "killed", "{stopped}: {text}");
    assert!(stopped["exitCode"].is_null(), "{stopped}");
    assert!(
        text.contains("tasks it started") && text.contains("keep running"),
        "{text}"
    );
    assert!(
        stopped["codemode"]["cancelledCalls"]
            .as_array()
            .unwrap()
            .iter()
            .any(|call| call["tool"] == "exec" && call["action"] == "poll"),
        "{stopped}"
    );
    assert!(
        stopped["codemode"]["startedTasks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|task| task["taskId"] == child && task["state"] == "running"),
        "{stopped}"
    );
    let (listed, _) = server.ok(json!({"action":"list"}));
    assert!(
        listed["tasks"].as_array().unwrap().iter().any(|task| {
            task["taskId"] == bare_id(&script)
                && task["state"] == "killed"
                && task["children"]["activeTasks"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|entry| {
                        entry["taskId"] == bare_id(&child)
                            && entry["state"] == "running"
                            && entry["kind"] == "bash"
                    })
        }),
        "{listed}"
    );
    let (active, _) = server.ok(json!({"action":"list", "state":"active"}));
    assert!(
        active["tasks"].as_array().unwrap().iter().any(|task| {
            task["taskId"] == bare_id(&child)
                && task["state"] == "running"
                && task["kind"] == "bash"
                && task["parentTaskId"] == bare_id(&script)
        }),
        "{active}"
    );
    server.ok(json!({"action":"kill", "taskId":child}));
}

#[test]
fn codemode_deadlines_stop_bytecode_and_microtasks() {
    let mut server = codemode_server();
    for code in ["while (true) {}", "while (true) await null;"] {
        let result = code_result(
            &mut server,
            json!({"code":code,"killAfterMs":300,"waitMs":10000}),
        );
        assert_eq!(result["isError"], false, "{result}");
        assert_eq!(result["structuredContent"]["state"], "killed", "{result}");
        assert_eq!(
            result["structuredContent"]["lifetimeExpired"], true,
            "{result}"
        );
        assert!(
            result["structuredContent"]["exitCode"].is_null(),
            "{result}"
        );
    }
}

#[test]
fn codemode_outer_request_cancellation_only_ends_its_wait() {
    let marker = Scratch::new("codemode-cancel-marker");
    let mut server = codemode_server();
    let code = format!(
        "await tools.write({{path:{},content:'started'}}); while (true) {{}}",
        json!(marker.to_string_lossy())
    );
    server.next_id += 1;
    let request_id = server.next_id;
    server.send(json!({"jsonrpc":"2.0","id":request_id,"method":"tools/call","params":{"name":"codemode","arguments":{"code":code,"waitMs":60000}}}));
    let deadline = Instant::now() + Duration::from_secs(30);
    while !marker.exists() {
        assert!(
            Instant::now() < deadline,
            "script did not create its marker"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    server.send(json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":request_id,"reason":"test cancellation"}}));
    // rmcp may drop a cancelled response; list observes the task after cancellation.
    let (listed, _) = server.ok(json!({"action":"list"}));
    let script = listed["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|task| task["kind"] == "codemode")
        .unwrap();
    assert_eq!(script["state"], "running", "{listed}");
    server.ok(json!({"action":"kill","taskId":script["taskId"]}));
}

#[test]
fn codemode_script_admission_limit_is_released_after_kill() {
    let mut server = codemode_server();
    let mut ids = Vec::new();
    for _ in 0..4 {
        let result = code_result(&mut server, json!({"code":"while (true) {}", "waitMs":0}));
        assert_eq!(result["isError"], false, "{result}");
        ids.push(
            result["structuredContent"]["taskId"]
                .as_str()
                .unwrap()
                .to_owned(),
        );
    }
    let rejected = code_result(&mut server, json!({"code":"return 1", "waitMs":0}));
    assert_eq!(rejected["isError"], true, "{rejected}");
    let (listed, _) = server.ok(json!({"action":"list"}));
    assert_eq!(listed["tasks"].as_array().unwrap().len(), 4, "{listed}");
    for id in ids {
        server.ok(json!({"action":"kill", "taskId":id}));
    }
    let next = code_result(&mut server, json!({"code":"return 'room again'"}));
    assert_eq!(next["structuredContent"]["exitCode"], 0, "{next}");
}

#[test]
fn codemode_attach_packs_across_eof_polls_dedups_bare_task_id_and_skips_oversize() {
    let med_b64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        vec![0x41u8; 1800],
    );
    let med_mp4 = {
        let mut raw = Vec::with_capacity(1800);
        raw.extend_from_slice(&20u32.to_be_bytes());
        raw.extend_from_slice(b"ftypmp42");
        raw.extend_from_slice(&0u32.to_be_bytes());
        raw.extend_from_slice(b"isom");
        raw.resize(1800, 0x55);
        format!(
            "data:video/mp4;base64,{}",
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &raw)
        )
    };
    let _ = med_b64;
    let huge_pdf = {
        let mut raw = b"%PDF-1.7\n%\xe2\xe3\xcf\xd3\n".to_vec();
        raw.resize(4000, b' ');
        format!(
            "data:application/pdf;base64,{}",
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &raw)
        )
    };
    let small_png = image_data_url(4, 4);

    for structured in [true, false] {
        let mut server = Server::start_with(|command| {
            command
                .env("FASTEXEC_CODEMODE", "true")
                .env(
                    "FASTEXEC_STRUCTURED_CONTENT",
                    if structured { "true" } else { "false" },
                )
                .env("FASTEXEC_TEST_OUTBOUND_LIMIT", "4600");
        });
        let code = format!(
            "attach({med_mp4:?}); attach({med_mp4:?}); attach({huge_pdf:?}); attach({small_png:?}); text('tail');"
        );
        let first = code_result(
            &mut server,
            json!({"code": code, "truncate": "tail", "maxBytes": 100}),
        );
        assert_eq!(first["isError"], false, "{first}");
        let blocks1 = first["content"].as_array().unwrap();
        assert_eq!(
            blocks1.iter().filter(|b| b["type"] == "resource").count(),
            1,
            "{first}"
        );
        let text1 = code_text(&first);
        assert!(text1.contains("tail"), "{text1}");
        let handle = if structured {
            assert_eq!(
                first["structuredContent"]["codemode"]["attachmentsDelivered"],
                json!([{"index": 1, "kind": "video", "mimeType": "video/mp4", "bytes": 1800}]),
                "{first}"
            );
            first["structuredContent"]["taskId"]
                .as_str()
                .unwrap()
                .to_owned()
        } else {
            assert!(first["structuredContent"].is_null(), "{first}");
            assert!(
                text1.contains("Attached: #1 (video/mp4, 1800 B)"),
                "{text1}"
            );
            text1
                .lines()
                .find(|line| line.starts_with("[exited 0] "))
                .unwrap()
                .split_whitespace()
                .nth(2)
                .unwrap()
                .to_owned()
        };

        // Text reached EOF in `first`; inputless poll #1 delivers byte-identical attachment #2.
        let poll1 = server.request(
            "tools/call",
            json!({"name": "exec", "arguments": {"action": "poll", "taskId": handle, "waitMs": 0}}),
        )["result"]
            .clone();
        assert_eq!(poll1["isError"], false, "{poll1}");
        assert_eq!(
            poll1["content"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|b| b["type"] == "resource")
                .count(),
            1,
            "{poll1}"
        );
        if structured {
            assert_eq!(
                poll1["structuredContent"]["codemode"]["attachmentsDelivered"][0]["index"], 2,
                "{poll1}"
            );
        } else {
            assert!(
                code_text(&poll1).contains("Attached: #2 (video/mp4, 1800 B)"),
                "{poll1}"
            );
        }

        // Inputless poll #2 delivers #4; across poll #1 and poll #2, oversized #3 is skipped once
        // with a visible warning naming #3.
        let poll2 = server.request(
            "tools/call",
            json!({"name": "exec", "arguments": {"action": "poll", "taskId": handle, "waitMs": 0}}),
        )["result"]
            .clone();
        assert_eq!(poll2["isError"], false, "{poll2}");
        assert_eq!(image_count(&poll2), 1, "{poll2}");
        let combined_poll_text = format!("{}\n{}\n{}", text1, code_text(&poll1), code_text(&poll2));
        assert_eq!(
            combined_poll_text
                .matches("attachment #3 [application/pdf]")
                .count(),
            1,
            "{combined_poll_text}"
        );
        if structured {
            assert_eq!(
                poll2["structuredContent"]["codemode"]["attachmentsDelivered"][0]["index"], 4,
                "{poll2}"
            );
        } else {
            assert!(
                code_text(&poll2).contains("Attached: #4 (image/png,"),
                "{poll2}"
            );
        }

        // Inputless poll #3 is caught up.
        let poll3 = server.request(
            "tools/call",
            json!({"name": "exec", "arguments": {"action": "poll", "taskId": handle, "waitMs": 0}}),
        )["result"]
            .clone();
        assert_eq!(poll3["content"].as_array().unwrap().len(), 1, "{poll3}");

        // Bare taskId attaches a fresh reader from index 1 for deterministic deduplication.
        let fresh = server.request(
            "tools/call",
            json!({"name": "exec", "arguments": {"action": "poll", "taskId": bare_id(&handle), "waitMs": 0}}),
        )["result"]
            .clone();
        assert!(code_text(&fresh).contains("new reader"), "{fresh}");
        if structured {
            assert_eq!(
                fresh["structuredContent"]["codemode"]["attachmentsDelivered"][0]["index"], 1,
                "{fresh}"
            );
        } else {
            assert!(
                code_text(&fresh).contains("Attached: #1 (video/mp4, 1800 B)"),
                "{fresh}"
            );
        }
    }
}

#[test]
fn codemode_attach_covers_compress_mode_r4_4_parity_and_nested_opaque_handles() {
    let (dir, _guard) = scratch("codemode-multimodal-e2e");
    std::fs::create_dir_all(&*dir).unwrap();
    let mp4_bytes = {
        let mut v = Vec::new();
        v.extend_from_slice(&20u32.to_be_bytes());
        v.extend_from_slice(b"ftypmp42");
        v.extend_from_slice(&0u32.to_be_bytes());
        v.extend_from_slice(b"isom");
        v.extend_from_slice(&[0u8; 32]);
        v
    };
    let webm_bytes = {
        let mut v = vec![0x1A, 0x45, 0xDF, 0xA3, 0x93, 0x42, 0x82, 0x84];
        v.extend_from_slice(b"webm");
        v.extend_from_slice(&[0u8; 24]);
        v
    };
    std::fs::write(dir.join("clip.mp4"), &mp4_bytes).unwrap();
    std::fs::write(dir.join("clip.webm"), &webm_bytes).unwrap();
    let mp4_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &mp4_bytes);
    let webm_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &webm_bytes);

    let mut server = Server::start_with(|command| {
        command
            .current_dir(&*dir)
            .env("FASTEXEC_CODEMODE", "true")
            .env("FASTEXEC_OUTPUT_MODE", "structured");
    });

    // 1. Six attachments deliver in one call (no legacy 4-image limit); R4-4 parity across
    //    local path, MCP resource(blob), and data URL for MP4 + WebM; conflicting MIME rejects.
    let producer_code = format!(
        r#"
        if (typeof globalThis.image !== "undefined") throw new Error("image() still exposed");
        const rMp4 = await tools.read({{ path: "clip.mp4" }});
        if (rMp4.attachable !== true || rMp4.data !== undefined) throw new Error("bad descriptor");
        const a1 = attach(rMp4);
        const a2 = attach({{ type: "resource", resource: {{ blob: {mp4_b64:?}, mimeType: "video/mp4", uri: "file:///clip.mp4" }} }});
        const a3 = attach("data:video/mp4;base64," + {mp4_b64:?});
        const a4 = attach("clip.webm");
        const a5 = attach({{ type: "resource", resource: {{ blob: {webm_b64:?}, mimeType: "video/webm" }} }});
        const a6 = attach("data:video/webm;base64," + {webm_b64:?});
        if (a1.mimeType !== a2.mimeType || a2.mimeType !== a3.mimeType) throw new Error("mp4 mismatch");
        if (a4.mimeType !== a5.mimeType || a5.mimeType !== a6.mimeType) throw new Error("webm mismatch");
        let conflict = false;
        try {{
            attach({{ type: "image", data: {mp4_b64:?}, mimeType: "image/png" }});
        }} catch (_e) {{
            conflict = true;
        }}
        if (!conflict) throw new Error("expected MIME conflict rejection");
        return [a1.index, a6.index];
        "#
    );
    let prod = code_result(&mut server, json!({"code": producer_code}));
    assert_eq!(prod["structuredContent"]["exitCode"], 0, "{prod}");
    assert_eq!(
        prod["structuredContent"]["codemode"]["attachmentsDelivered"]
            .as_array()
            .unwrap()
            .len(),
        6,
        "{prod}"
    );
    let prod_bare = bare_id(prod["structuredContent"]["taskId"].as_str().unwrap()).to_owned();

    // 2. Nested `poll` receives unforgeable opaque handles in `codemode.attachments` that can be
    //    re-attached multiple times while forged JSON clones throw TypeError.
    let consumer_code = format!(
        r#"
        const polled = await tools.exec({{ action: "poll", taskId: {prod_bare:?}, waitMs: 0 }});
        const handles = polled.codemode.attachments;
        if (!Array.isArray(handles) || handles.length !== 6) throw new Error("expected 6 handles");
        let forgedFailed = false;
        try {{
            attach(JSON.parse(JSON.stringify(handles[0])));
        }} catch (e) {{
            forgedFailed = e instanceof TypeError;
        }}
        if (!forgedFailed) throw new Error("forged handle must throw TypeError");
        const c1 = attach(handles[0]);
        const c2 = attach(handles[0]);
        return [c1.index, c2.index, c1.mimeType];
        "#
    );
    let cons = code_result(&mut server, json!({"code": consumer_code}));
    assert_eq!(cons["structuredContent"]["exitCode"], 0, "{cons}");
    assert_eq!(
        cons["structuredContent"]["output"], "[1,2,\"video/mp4\"]",
        "{cons}"
    );
    assert_eq!(
        cons["structuredContent"]["codemode"]["attachmentsDelivered"]
            .as_array()
            .unwrap()
            .len(),
        2,
        "{cons}"
    );

    // 3. Opt-in `FASTEXEC_IMAGE_COMPRESS=true` scales 2400x6 PNG to <= 2000px while structured
    //    output omits legacy `[image 1:]` stdout markers and carries `attachmentsDelivered`.
    let mut compressed = Server::start_with(|command| {
        command
            .env("FASTEXEC_CODEMODE", "true")
            .env("FASTEXEC_IMAGE_COMPRESS", "true")
            .env("FASTEXEC_OUTPUT_MODE", "structured");
    });
    let comp_res = code_result(
        &mut compressed,
        json!({"code": format!("attach({}); text('clean-body');", json!(image_data_url(2400, 6)))}),
    );
    assert_eq!(image_count(&comp_res), 1, "{comp_res}");
    let block = comp_res["content"]
        .as_array()
        .unwrap()
        .iter()
        .find(|block| block["type"] == "image")
        .unwrap();
    let data = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        block["data"].as_str().unwrap(),
    )
    .unwrap();
    let decoded = image::load_from_memory(&data).unwrap();
    assert!(decoded.width() <= 2000 && decoded.height() <= 2000);
    assert_eq!(
        comp_res["structuredContent"]["output"], "clean-body",
        "{comp_res}"
    );
    assert_eq!(
        comp_res["structuredContent"]["codemode"]["attachmentsDelivered"][0]["index"], 1,
        "{comp_res}"
    );
}

#[test]
fn codemode_nested_poll_and_kill_do_not_consume_other_tasks_background_notifications() {
    let mut server = codemode_server();
    let (task, _) = server.ok(json!({
        "action":"start", "eof":false, "command":"read -r reply", "loginShell":false, "waitMs":0
    }));
    let other = task["taskId"].as_str().unwrap().to_owned();
    let code = format!(
        "await tools.exec({{action:'poll',taskId:{},input:'done\\n',waitMs:10000}}); await tools.exec({{action:'kill',taskId:{}}}); text('done');",
        json!(other),
        json!(other)
    );
    let result = code_result(&mut server, json!({"code":code}));
    assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
    assert!(
        code_text(&result).contains(bare_id(&other)) && code_text(&result).contains("Background:"),
        "{result}"
    );
}

#[test]
fn codemode_awaited_helper_tasks_do_not_pollute_outer_background_footer() {
    let mut server = codemode_server();
    let code = r#"
for (let i = 0; i < 6; i++) {
  const res = await tools.exec({
    action: 'start',
    command: i % 2 === 0 ? `printf ok-${i}` : `exit ${i}`,
    loginShell: false,
  });
  if (res.state !== 'exited') throw new Error('helper failed');
}
const delayed = await tools.exec({
  action: 'start',
  command: 'sleep 0.05; exit 3',
  loginShell: false,
  waitMs: 0,
});
const polled = await tools.exec({
  action: 'poll',
  taskId: delayed.taskId,
});
if (polled.exitCode !== 3) throw new Error('delayed helper exit');
return 'clean';
"#;
    let result = code_result(&mut server, json!({"code":code}));
    assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
    assert!(
        !code_text(&result).contains("Background:"),
        "awaited helper tasks leaked into outer Background footer: {}",
        code_text(&result)
    );
    let (_, next_text) = server.ok(json!({
        "action":"start", "command":"printf outer", "loginShell":false
    }));
    assert!(
        !next_text.contains("Background:"),
        "later outer call received stale helper notifications: {next_text}"
    );
}

#[test]
fn codemode_script_cwd_applies_to_file_tools_and_nested_exec() {
    let outer = Scratch::new("codemode-outer-cwd");
    let inner = Scratch::new("codemode-inner-cwd");
    std::fs::create_dir_all(&*outer).unwrap();
    std::fs::create_dir_all(&*inner).unwrap();
    let mut server = Server::start_with(|command| {
        command
            .current_dir(&*outer)
            .env("FASTEXEC_CODEMODE", "true")
            .env("FASTEXEC_OUTPUT_MODE", "both");
    });
    let code = format!(
        r#"
await tools.write({{path:'sub/item.txt', content:'one\ntwo\n'}});
await tools.edit({{path:'sub/item.txt', edits:[{{oldText:'two', newText:'TWO'}}]}});
const [readBack, runInner, runOuter] = await Promise.all([
  tools.read({{path:'sub/item.txt'}}),
  tools.exec({{action:'start', command:'printf inner-ok > from-exec.txt', loginShell:false}}),
  tools.exec({{action:'start', command:'printf outer-ok > from-override.txt', cwd:{}, loginShell:false}}),
]);
if (runInner.exitCode !== 0 || runOuter.exitCode !== 0) throw new Error('nested exec failed');
return {{ file: readBack.text }};
"#,
        json!(outer.to_string_lossy())
    );
    let result = code_result(&mut server, json!({"code":code, "cwd": &*inner}));
    assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
    assert!(inner.join("sub/item.txt").is_file());
    assert!(!outer.join("sub/item.txt").exists());
    assert_eq!(
        std::fs::read_to_string(inner.join("sub/item.txt")).unwrap(),
        "one\nTWO\n"
    );
    assert_eq!(
        std::fs::read_to_string(inner.join("from-exec.txt")).unwrap(),
        "inner-ok"
    );
    assert!(!outer.join("from-exec.txt").exists());
    assert_eq!(
        std::fs::read_to_string(outer.join("from-override.txt")).unwrap(),
        "outer-ok"
    );
    assert!(!inner.join("from-override.txt").exists());
    let parsed: Value =
        serde_json::from_str(result["structuredContent"]["output"].as_str().unwrap()).unwrap();
    assert_eq!(parsed["file"], "one\nTWO\n");
}

#[test]
fn codemode_waits_match_verbatim_output_on_start_and_poll() {
    let mut server = codemode_server();
    let marker = "\x1b[31mready";
    let started = code_result(
        &mut server,
        json!({
            "code": format!("text({}); while (true) {{}}", json!(marker)),
            "waitMs": 1000,
            "returnWhen": {"outputContains": [marker]},
        }),
    );
    let start_data = &started["structuredContent"];
    server.ok(json!({"action": "kill", "taskId": start_data["taskId"]}));
    assert_eq!(
        start_data["waitEndedBy"]["condition"], "output_contains",
        "{started}"
    );
    assert_eq!(start_data["waitEndedBy"]["text"], marker, "{started}");
    assert_eq!(start_data["output"], marker, "{started}");

    let (release, release_bash) = scratch("codemode-matcher-release");
    let wait_command = format!("while [ ! -f '{release_bash}' ]; do sleep 0.02; done");
    let started = code_result(
        &mut server,
        json!({
            "code": format!(
                "text('boot'); await tools.exec({{action:'start', command:{}, loginShell:false}}); text({}); while (true) {{}}",
                json!(wait_command), json!(marker)
            ),
            "waitMs": 10000,
            "returnWhen": {"outputContains": ["boot"]},
        }),
    );
    let handle = &started["structuredContent"]["taskId"];
    std::fs::write(&*release, "ready").unwrap();
    let (polled, text) = server.ok(json!({
        "action": "poll", "taskId": handle, "waitMs": 10000,
        "returnWhen": {"outputContains": [marker]},
    }));
    server.ok(json!({"action": "kill", "taskId": handle}));
    assert_eq!(
        polled["waitEndedBy"]["condition"], "output_contains",
        "{text}"
    );
    assert_eq!(polled["waitEndedBy"]["text"], marker, "{text}");
    assert!(
        polled["output"].as_str().unwrap().contains(marker),
        "{text}"
    );
}

#[test]
fn nested_list_marks_tasks_started_by_an_outer_call() {
    let mut server = codemode_server();
    let (started, _) = server.ok(json!({
        "action": "start", "command": "sleep 0.1; exit 4",
        "loginShell": false, "waitMs": 0,
    }));
    let handle = started["taskId"].as_str().unwrap();
    let listed = code_result(
        &mut server,
        json!({
            "code": format!(
                "await tools.exec({{action:'poll', taskId:{}}}); const list = await tools.exec({{action:'list', state:'finished'}}); return list.tasks.find(task => task.taskId === {}).exitCode;",
                json!(handle), json!(bare_id(handle)),
            ),
        }),
    );
    assert_eq!(listed["structuredContent"]["output"], "4", "{listed}");
    assert!(
        !code_text(&listed).contains("(Background:"),
        "the delivered nested list already reported the finished outer task: {listed}",
    );
}

#[test]
fn codemode_drains_inflight_spawns_before_publishing_started_tasks() {
    let mut server = codemode_server();
    // Fire unawaited starts and return immediately; any child that actually spawned must already
    // be present in the initial codemode result's startedTasks.
    let code = r#"
for (let i = 0; i < 4; i++) {
  tools.exec({action:'start', command:'sleep 30', loginShell:false, waitMs:60000});
}
await Promise.resolve();
return 'returned-early';
"#;
    let result = code_result(&mut server, json!({"code":code}));
    assert_eq!(result["structuredContent"]["exitCode"], 0, "{result}");
    let reported: std::collections::HashSet<String> =
        result["structuredContent"]["codemode"]["startedTasks"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|entry| bare_id(entry["taskId"].as_str().unwrap()).to_owned())
            .collect();
    let (listed, _) = server.ok(json!({"action":"list", "state":"active"}));
    let running_bash: Vec<String> = listed["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|task| task["kind"] == "bash")
        .map(|task| task["taskId"].as_str().unwrap().to_owned())
        .collect();
    for id in &running_bash {
        assert!(
            reported.contains(id),
            "spawned bash task {id} missed initial startedTasks report {reported:?}: {result}"
        );
        server.ok(json!({"action":"kill", "taskId":id}));
    }
}

#[test]
fn codemode_preserves_long_json_raw_text_nested_windows_waits_and_sh_quote() {
    let mut server = codemode_server();

    // P0-1: 10,000-char JSON line stays whole and parses cleanly.
    let big_json = code_result(
        &mut server,
        json!({
            "code": "return { payload: 'x'.repeat(10000), ok: 1 };",
            "maxBytes": 16384
        }),
    );
    assert_eq!(big_json["structuredContent"]["exitCode"], 0, "{big_json}");
    assert!(
        big_json["structuredContent"].get("cutLines").is_none(),
        "{big_json}"
    );
    let body = big_json["structuredContent"]["output"].as_str().unwrap();
    let parsed: Value = serde_json::from_str(body.trim()).expect("whole JSON line parses");
    assert_eq!(parsed["payload"].as_str().unwrap().len(), 10_000);

    // P0-1 + P2-2: An 800-byte JSON line fits whole inside maxBytes: 1000 without NOTES_RESERVE
    // shaving it down or forcing ` · log ` into the status line; tight budgets refit cleanly.
    let near_cap = code_result(
        &mut server,
        json!({
            "code": "return { nearCap: 'q'.repeat(780) };",
            "maxBytes": 1000
        }),
    );
    let near_text = code_text(&near_cap);
    assert!(
        body_of(near_text).len() <= 1000,
        "len={} {near_text}",
        body_of(near_text).len()
    );
    assert!(
        near_cap["structuredContent"].get("cutLines").is_none(),
        "{near_cap}"
    );
    assert!(!near_text.contains(" · log "), "{near_text}");
    let parsed_near: Value = serde_json::from_str(
        near_cap["structuredContent"]["output"]
            .as_str()
            .unwrap()
            .trim(),
    )
    .expect("near-cap JSON parses whole");
    assert_eq!(parsed_near["nearCap"].as_str().unwrap().len(), 780);

    for tight in [100_u64, 150, 220, 320] {
        let tight_res = code_result(
            &mut server,
            json!({
                "code": "return 'T'.repeat(2000);",
                "maxBytes": tight
            }),
        );
        let tight_text = code_text(&tight_res);
        let tight_body = tight_res["structuredContent"]["output"].as_str().unwrap();
        assert!(
            tight_body.len() <= tight as usize,
            "tight={tight} len={}: {tight_text}",
            tight_body.len()
        );
        assert!(
            !tight_body.contains(" … [cut]") && tight_body.matches("line cut at ").count() <= 1,
            "double-cut marker at tight={tight}: {tight_body}"
        );
    }

    // P0-1: Over-maxBytes single line cuts within budget, sets cutLines: 1, and names log for recovery.
    let over_budget = code_result(
        &mut server,
        json!({
            "code": "return 'z'.repeat(10000);",
            "maxBytes": 2048
        }),
    );
    let over_text = code_text(&over_budget);
    assert!(
        body_of(over_text).len() <= 2048,
        "len={} {over_text}",
        body_of(over_text).len()
    );
    assert!(over_text.contains(" … [line cut at "), "{over_text}");
    assert!(over_text.contains(" · log "), "{over_text}");
    assert_eq!(
        over_budget["structuredContent"]["cutLines"], 1,
        "{over_budget}"
    );
    let log_path = over_budget["structuredContent"]["logPath"]
        .as_str()
        .unwrap();
    assert_eq!(
        std::fs::read_to_string(log_path).unwrap().trim().len(),
        10_000
    );

    // P0-1: CR and trailing spaces + ANSI sequences are preserved verbatim in codemode output.
    let raw_kept = code_result(
        &mut server,
        json!({
            "code": "text('first\\rsecond  '); text('\\x1b[31mred\\x1b[0m  ');"
        }),
    );
    let raw_out = raw_kept["structuredContent"]["output"].as_str().unwrap();
    assert_eq!(raw_out, "first\rsecond  \n\x1b[31mred\x1b[0m  ");

    // P0-2, P0-3, P2-8: nested uncut lines, >240s waitMs, input 2s poll default, >4 MiB log recovery,
    // nested transcript maxBytes-only tail default, and sh.quote bash round-trip.
    let nested = code_result(
        &mut server,
        json!({
            "code": r#"
                const wide = await tools.exec({
                    action: "start",
                    loginShell: false,
                    command: "awk 'BEGIN { s = sprintf(\"%5000s\", \"\"); gsub(/ /, \"w\", s); print s; for (i = 1; i <= 3000; i++) print i }'"
                });
                if ("cutLines" in wide || "omittedRange" in wide) throw new Error("unexpected cut/omit: " + JSON.stringify(wide));
                const wLines = wide.output.trimEnd().split("\n");
                if (wLines.length !== 3001 || wLines[0].length !== 5000 || wLines[3000] !== "3000") {
                    throw new Error("wide mismatch: len=" + wLines.length + " first=" + wLines[0].length);
                }

                const hiWait = await tools.exec({
                    action: "start",
                    loginShell: false,
                    waitMs: 600000,
                    command: "printf fast"
                });
                if (hiWait.output !== "fast") throw new Error("hiWait: " + JSON.stringify(hiWait));

                const openCat = await tools.exec({
                    action: "start",
                    loginShell: false,
                    eof: false,
                    waitMs: 50,
                    command: "cat"
                });
                const t0 = Date.now();
                const polled = await tools.exec({
                    action: "poll",
                    taskId: openCat.taskId,
                    input: "echoed\n"
                });
                const dt = Date.now() - t0;
                await tools.exec({ action: "kill", taskId: openCat.taskId });
                if (polled.state !== "running" || polled.output !== "echoed" || dt < 1500 || dt > 12000) {
                    throw new Error("poll timing/output: dt=" + dt + " " + JSON.stringify(polled));
                }

                const single4m = await tools.exec({
                    action: "start",
                    loginShell: false,
                    command: "awk 'BEGIN { s = sprintf(\"%1000s\", \"\"); gsub(/ /, \"x\", s); for (i = 0; i < 4200; i++) printf \"%s\", s; printf \"\\n\" }'"
                });
                if (single4m.cutLines !== 1) throw new Error("single4m: " + JSON.stringify(single4m));
                const afterSingle = await tools.exec({ action: "poll", taskId: single4m.taskId, waitMs: 0 });
                if (afterSingle.output !== "") throw new Error("expected empty follow-up poll");

                const many4m = await tools.exec({
                    action: "start",
                    loginShell: false,
                    command: "awk 'BEGIN { s = sprintf(\"%1023s\", \"\"); gsub(/ /, \"y\", s); for (i = 1; i <= 4100; i++) print i \":\" s }'"
                });
                if (!Array.isArray(many4m.omittedRange) || many4m.omittedRange[1] !== 4100) {
                    throw new Error("many4m omittedRange: " + JSON.stringify(many4m.omittedRange));
                }
                const recovered = await tools.read({
                    path: many4m.logPath,
                    offset: many4m.omittedRange[0],
                    limit: 50
                });
                if (!recovered.text.includes("4100:")) throw new Error("log recovery missed 4100");

                const ptyRun = await tools.exec({
                    action: "start",
                    pty: true,
                    loginShell: false,
                    command: "awk 'BEGIN { for (i = 1; i <= 200; i++) printf \"line-%03d-padding-text\\n\", i }'"
                });
                const tr = await tools.exec({
                    action: "transcript",
                    taskId: ptyRun.taskId,
                    maxBytes: 400
                });
                if (!tr.output.includes("line-200-padding-text") || tr.output.includes("line-001-padding-text")) {
                    throw new Error("transcript did not tail: " + tr.output);
                }

                const tricky = "space 'quote' $HOME `uname` \\ \n end";
                const rt = await tools.exec({
                    action: "start",
                    loginShell: false,
                    raw: true,
                    command: "printf '%s' " + sh.quote(tricky)
                });
                if (rt.output !== tricky) throw new Error("sh.quote mismatch: " + JSON.stringify(rt.output));

                const args = ["it's", "$HOME", "`uname`", "\\", "a\nb", ""];
                const expectedLines = args.join("\n");
                const tagCmd = sh`printf '%s\n' ${args}`;
                if (!tagCmd.startsWith("printf '%s\\n' ")) {
                    throw new Error("sh tagged template did not preserve raw \\n: " + JSON.stringify(tagCmd));
                }
                const rtTag = await tools.exec({
                    action: "start",
                    loginShell: false,
                    raw: true,
                    command: tagCmd
                });
                if (rtTag.output !== expectedLines) {
                    throw new Error("sh tagged template mismatch: " + JSON.stringify(rtTag.output));
                }
                const rtJoin = await tools.exec({
                    action: "start",
                    loginShell: false,
                    raw: true,
                    command: "printf '%s\\n' " + sh.join(args)
                });
                if (rtJoin.output !== expectedLines) {
                    throw new Error("sh.join mismatch: " + JSON.stringify(rtJoin.output));
                }
                return "all-nested-checks-ok";
            "#
        }),
    );
    assert_eq!(nested["structuredContent"]["exitCode"], 0, "{nested}");
    assert!(
        nested["structuredContent"]["output"]
            .as_str()
            .unwrap()
            .contains("all-nested-checks-ok"),
        "{nested}"
    );

    // P0-2: Unbounded nested wait over 30s resolves without nested poll while outer waitMs returns a handle.
    let started_long = code_result(
        &mut server,
        json!({
            "code": r#"
                const r = await tools.exec({
                    action: "start",
                    loginShell: false,
                    command: "sleep 31; printf after-thirty-seconds"
                });
                if (r.state !== "exited" || r.output !== "after-thirty-seconds") {
                    throw new Error("unexpected nested wait end: " + JSON.stringify(r));
                }
                return "long-wait-resolved";
            "#,
            "waitMs": 300
        }),
    );
    assert_eq!(
        started_long["structuredContent"]["state"], "running",
        "{started_long}"
    );
    let script_handle = started_long["structuredContent"]["taskId"]
        .as_str()
        .unwrap()
        .to_owned();
    let (polled_long, polled_text) = server.ok(json!({
        "action": "poll",
        "taskId": script_handle,
        "waitMs": 45000
    }));
    assert_eq!(polled_long["state"], "exited", "{polled_text}");
    assert_eq!(polled_long["exitCode"], 0, "{polled_text}");
    assert!(polled_text.contains("long-wait-resolved"), "{polled_text}");
}

#[test]
fn completed_screen_and_empty_codemode_lines_follow_the_rendered_body() {
    let mut server = codemode_server();
    let (screen, text) = server.ok(json!({
        "action":"start", "command":"printf %0220d 0", "loginShell":false,
        "pty":true, "cols":90, "rows":5, "screen":true, "maxBytes":360
    }));
    assert!(screen.get("omittedRows").is_none(), "{text}");
    assert_eq!(
        screen["output"].as_str().unwrap().matches('0').count(),
        220,
        "{text}"
    );
    assert!(!text.contains(" · log "), "{text}");
    assert!(body_of(&text).len() <= 360, "{text}");

    let blank = code_result(&mut server, json!({"code":"text('')"}));
    assert!(
        code_text(&blank).starts_with("\n\n["),
        "{}",
        code_text(&blank)
    );
    let (_, next) =
        server.ok(json!({"action":"poll","taskId":blank["structuredContent"]["taskId"]}));
    assert!(next.starts_with("(no new output)"), "{next}");
}

#[cfg(unix)]
#[test]
fn nested_stream_and_transcript_fit_the_status_after_a_long_log_path_is_shortened() {
    let scratch = Scratch::new("long-window-path");
    let mut dir = scratch.to_path_buf();
    for _ in 0..6 {
        dir.push("x".repeat(110));
    }
    std::fs::create_dir_all(&dir).unwrap();
    let mut server = Server::start_with(|command| {
        command
            .env("TMPDIR", &dir)
            .env("FASTEXEC_OUTPUT_MODE", "both")
            .env("FASTEXEC_CODEMODE", "true");
    });
    let result = code_result(
        &mut server,
        json!({
            "code": r#"
            const task = await tools.exec({
                action: "start", command: "printf '%0400d\\n' 1; sleep 30", loginShell: false,
                pty: true, cols: 500, rows: 5, maxBytes: 1000, returnWhen: {outputContains: ["0000"]}
            });
            const transcript = await tools.exec({action: "transcript", taskId: task.taskId, maxBytes: 1000});
            await tools.exec({action: "kill", taskId: task.taskId});
            return [task.output, transcript.output];
        "#
        }),
    );
    let bodies: Vec<String> =
        serde_json::from_str(result["structuredContent"]["output"].as_str().unwrap())
            .unwrap_or_else(|_| panic!("{result}"));
    let expected = format!("{:0400}", 1);
    assert_eq!(bodies[0], expected);
    assert!(bodies[1].lines().any(|line| line == expected), "{bodies:?}");
}

#[test]
fn kill_delivers_unseen_output_gates_concurrent_polls_and_attaches_codemode_images() {
    let mut server = codemode_server();

    let (started, _) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "printf 'first\\n'; sleep 0.3; printf 'after-start-before-kill\\n'; sleep 60",
        "waitMs": 100
    }));
    let handle = started["taskId"].as_str().unwrap().to_owned();
    let bare = bare_id(&handle).to_owned();
    std::thread::sleep(Duration::from_millis(500));
    let (killed, kill_text) = server.ok(json!({"action": "kill", "taskId": &handle}));
    assert_eq!(killed["state"], "killed", "{kill_text}");
    assert!(killed.get("waitEndedBy").is_none(), "{killed}");
    let combined = format!(
        "{}{}",
        started["output"].as_str().unwrap_or_default(),
        killed["output"].as_str().unwrap_or_default()
    );
    assert!(combined.contains("after-start-before-kill"), "{combined}");
    let (after_kill, _) = server.ok(json!({"action": "poll", "taskId": &handle, "waitMs": 0}));
    assert_eq!(after_kill["output"], "", "{after_kill}");

    let (bare_kill, bare_kill_text) = server.ok(json!({"action": "kill", "taskId": &bare}));
    assert!(
        bare_kill["output"].as_str().unwrap().contains("first"),
        "{bare_kill}"
    );
    assert!(bare_kill_text.contains("new reader"), "{bare_kill_text}");

    let (conc_start, _) = server.ok(json!({
        "action": "start",
        "loginShell": false,
        "command": "printf 'ready\\n'; sleep 0.2; printf 'before-concurrent-kill\\n'; sleep 60",
        "returnWhen": {"outputContains": ["ready"]},
        "waitMs": 5000
    }));
    let conc_id = conc_start["taskId"].as_str().unwrap().to_owned();
    std::thread::sleep(Duration::from_millis(400));
    server.next_id += 1;
    let poll_req = server.next_id;
    server.send(json!({
        "jsonrpc": "2.0",
        "id": poll_req,
        "method": "tools/call",
        "params": {"name": "exec", "arguments": {"action": "poll", "taskId": &conc_id, "waitMs": 30000}}
    }));
    std::thread::sleep(Duration::from_millis(100));
    server.next_id += 1;
    let kill_req = server.next_id;
    server.send(json!({
        "jsonrpc": "2.0",
        "id": kill_req,
        "method": "tools/call",
        "params": {"name": "exec", "arguments": {"action": "kill", "taskId": &conc_id}}
    }));
    // A JSON-RPC client may await either reply first.
    let r2 = server.response(kill_req);
    let r1 = server.response(poll_req);
    assert_eq!(r1["result"]["isError"], false, "{r1}");
    assert_eq!(r2["result"]["isError"], false, "{r2}");
    let out1 = r1["result"]["structuredContent"]["output"]
        .as_str()
        .unwrap_or_default();
    let out2 = r2["result"]["structuredContent"]["output"]
        .as_str()
        .unwrap_or_default();
    let merged = format!("{out1}{out2}");
    assert_eq!(
        merged.matches("before-concurrent-kill").count(),
        1,
        "expected output conserved once across poll+kill: out1={out1:?} out2={out2:?}"
    );

    let url = image_data_url(4, 4);
    let img_script = code_result(
        &mut server,
        json!({
            "code": format!(
                "attach({url:?}); await tools.exec({{action:'start', loginShell:false, command:'sleep 30'}});"
            ),
            "waitMs": 200
        }),
    );
    let cm_handle = img_script["structuredContent"]["taskId"]
        .as_str()
        .unwrap()
        .to_owned();
    let child_tasks: Vec<String> = img_script["structuredContent"]["codemode"]["startedTasks"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|t| t["taskId"].as_str().map(str::to_owned))
                .collect()
        })
        .unwrap_or_default();
    let already_attached = image_count(&img_script);
    let kill_reply = server.request(
        "tools/call",
        json!({"name": "exec", "arguments": {"action": "kill", "taskId": cm_handle}}),
    );
    let kill_attached = image_count(&kill_reply["result"]);
    assert_eq!(
        already_attached + kill_attached,
        1,
        "{img_script} / {kill_reply}"
    );
    for child in child_tasks {
        let _ = server.call(json!({"action": "kill", "taskId": child}));
    }
}

#[test]
fn p0_3_p1_3_p1_5_codemode_permits_sleep_stacks_and_serializable_returns() {
    let dir = Scratch::new("codemode-permits");
    std::fs::create_dir_all(&*dir).unwrap();
    std::fs::write(dir.join("hello.txt"), "hello-world\n").unwrap();
    let mut server = Server::start_with(|command| {
        command
            .current_dir(&*dir)
            .env("FASTEXEC_CODEMODE", "true")
            .env("FASTEXEC_OUTPUT_MODE", "both");
    });

    let concurrency = code_result(
        &mut server,
        json!({
            "code": r#"
                const t0 = Date.now();
                const eightStarts = Promise.all(
                    Array.from({ length: 8 }, () => tools.exec({ action: "start", command: "sleep 60" }))
                );
                const activeBash = (listed) => listed.tasks
                    .flatMap(t => t.kind === "codemode" ? (t.children?.activeTasks ?? []) : [t])
                    .filter(t => t.kind === "bash");
                let active = [];
                while (active.length < 8) {
                    await sleep(20);
                    const listed = await tools.exec({ action: "list", state: "active" });
                    active = activeBash(listed);
                }
                const readDuringStarts = await tools.read({ path: "hello.txt" });
                if (readDuringStarts.text !== "hello-world\n") {
                    throw new Error("read failed while starts saturated");
                }
                await Promise.all(active.map(t => tools.exec({ action: "kill", taskId: t.taskId })));
                const endedEight = await eightStarts;
                const dtEight = Date.now() - t0;
                if (dtEight > 20000 || endedEight.some(r => r.state !== "killed")) {
                    throw new Error("8 starts kill mismatch: dt=" + dtEight + " states=" + endedEight.map(r => r.state));
                }

                // 8 concurrent observation polls on distinct readers do not block read.
                const holder = await tools.exec({
                    action: "start",
                    command: "printf 'holder-ready\\n'; sleep 30",
                    returnWhen: { outputContains: ["holder-ready"] }
                });
                const bare = holder.taskId.split(".")[0];
                const polls = Array.from({ length: 8 }, (_, idx) =>
                    tools.exec({
                        action: "poll",
                        taskId: bare + ".r00" + idx,
                        waitMs: 2500
                    }).then(() => "polls-ended")
                );
                await sleep(20);
                const readWinner = await Promise.race([
                    tools.read({ path: "hello.txt" }).then(r => r.text),
                    Promise.all(polls).then(() => "polls-won")
                ]);
                await tools.exec({ action: "kill", taskId: holder.taskId });
                await Promise.allSettled(polls);
                if (readWinner !== "hello-world\n") {
                    throw new Error("read was blocked by observation polls: " + readWinner);
                }

                // 9 sleep 0.3 starts succeed via Promise.all.
                const nine = await Promise.all(
                    Array.from({ length: 9 }, () => tools.exec({ action: "start", command: "sleep 0.3" }))
                );
                if (nine.some(r => r.exitCode !== 0)) {
                    throw new Error("9 starts failed");
                }

                // Promise.race with sleep(100).
                const raceHolder = tools.exec({ action: "start", command: "sleep 10" });
                const winner = await Promise.race([
                    raceHolder,
                    sleep(100).then(() => "timeout")
                ]);
                if (winner !== "timeout") throw new Error("expected timeout winner");
                const remaining = activeBash(await tools.exec({ action: "list", state: "active" }));
                await Promise.all(remaining.map(t => tools.exec({ action: "kill", taskId: t.taskId })));
                await raceHolder;

                // sleep(-3) resolves; sleep(NaN) and sleep("1") reject with TypeError.
                await sleep(-3);
                for (const bad of [NaN, "1"]) {
                    let caught = null;
                    try {
                        await sleep(bad);
                    } catch (e) {
                        caught = e;
                    }
                    if (!caught || caught.name !== "TypeError") {
                        throw new Error("expected TypeError for sleep(" + bad + ")");
                    }
                }

                // 1025th pending call rejects with RangeError and caller stack.
                const sleepers = Array.from({ length: 1024 }, () => sleep(200));
                let capErr = null;
                try {
                    await sleep(200);
                } catch (e) {
                    capErr = e;
                }
                await Promise.all(sleepers);
                if (!capErr || capErr.name !== "RangeError" || !String(capErr.stack).includes("codemode.js:")) {
                    throw new Error("1025th call mismatch: " + (capErr && capErr.stack));
                }
                return "concurrency-ok";
            "#
        }),
    );
    assert_eq!(
        concurrency["structuredContent"]["exitCode"], 0,
        "{concurrency}"
    );
    assert_eq!(concurrency["structuredContent"]["output"], "concurrency-ok");

    // P1-3: Line 2 uncaught async ToolError names codemode.js:2; caught stack also names it;
    // unserializable returns exit 1 while null and "a\nb" succeed.
    let uncaught_line2 = code_result(
        &mut server,
        json!({"code": "const x = 1;\nawait tools.read({ path: 'missing-on-line-2.txt' });\nreturn x;"}),
    );
    assert_eq!(uncaught_line2["structuredContent"]["exitCode"], 1);
    assert!(
        uncaught_line2["structuredContent"]["output"]
            .as_str()
            .unwrap()
            .contains("codemode.js:2"),
        "{uncaught_line2}"
    );

    let caught_stacks = code_result(
        &mut server,
        json!({
            "code": "let s1 = '';\ntry { await tools.read({ path: 'nope.txt' }); } catch (e) { s1 = e.stack; }\nlet s2 = '';\ntry { await tools.exec({ action: 123 }); } catch (e) { s2 = e.stack; }\nreturn [s1.includes('codemode.js:2'), s2.includes('codemode.js:4')];"
        }),
    );
    assert_eq!(caught_stacks["structuredContent"]["output"], "[true,true]");

    for bad_ret in ["return Symbol('x');", "return () => 1;", "return 1n;"] {
        let bad = code_result(&mut server, json!({"code": bad_ret}));
        assert_eq!(bad["structuredContent"]["exitCode"], 1, "{bad_ret}: {bad}");
        assert!(
            bad["structuredContent"]["output"]
                .as_str()
                .unwrap()
                .contains("Script error: TypeError: the return value is not JSON-serializable"),
            "{bad_ret}: {bad}"
        );
    }
    let ret_null = code_result(&mut server, json!({"code": "return null;"}));
    assert_eq!(ret_null["structuredContent"]["exitCode"], 0);
    assert_eq!(ret_null["structuredContent"]["output"], "null");
    let ret_lines = code_result(&mut server, json!({"code": "return 'a\\nb';"}));
    assert_eq!(ret_lines["structuredContent"]["exitCode"], 0);
    assert_eq!(ret_lines["structuredContent"]["output"], "a\nb");
}

#[test]
fn codemode_list_folds_batch_and_preserves_earlier_top_level_tasks() {
    let mut server = codemode_server();
    let (earlier, _) = server.ok(json!({
        "action": "start",
        "command": "printf earlier-top-level",
        "loginShell": false,
    }));
    let earlier_bare = bare_id(earlier["taskId"].as_str().unwrap()).to_owned();

    let batch = code_result(
        &mut server,
        json!({
            "code": r#"
                const results = await Promise.all(
                    Array.from({ length: 30 }, () =>
                        tools.exec({ action: "start", command: "true", loginShell: false })
                    )
                );
                if (results.some(r => r.exitCode !== 0)) throw new Error("batch child failed");
                return "batch-done";
            "#
        }),
    );
    assert_eq!(batch["structuredContent"]["exitCode"], 0, "{batch}");
    let parent_bare = bare_id(batch["structuredContent"]["taskId"].as_str().unwrap()).to_owned();

    let (listed, text) = server.ok(json!({"action": "list"}));
    let tasks = listed["tasks"].as_array().unwrap();
    assert_eq!(listed["totalCount"], 2, "{listed}");
    assert_eq!(tasks.len(), 2, "{listed}");
    assert_eq!(tasks[0]["taskId"], parent_bare, "{listed}");
    assert_eq!(tasks[0]["children"]["total"], 30, "{listed}");
    assert_eq!(tasks[0]["children"]["done"], 30, "{listed}");
    assert!(
        tasks[0]["children"].get("activeTasks").is_none(),
        "{listed}"
    );
    assert_eq!(tasks[1]["taskId"], earlier_bare, "{listed}");
    assert!(text.contains("30 tasks (30 done)"), "{text}");
    assert!(
        text.contains("30 folded codemode children hidden; pass parentTaskId to inspect."),
        "{text}"
    );
    assert!(text.contains(&earlier_bare), "{text}");
}

#[test]
fn codemode_kill_shows_orphans_cancelled_starts_and_clears_background() {
    let mut server = codemode_server();
    let (release, release_bash) = scratch("codemode-kill-orphans-release");
    let wait_cmd = format!("while [ ! -f '{release_bash}' ]; do sleep 0.02; done");

    let started = code_result(
        &mut server,
        json!({
            "code": format!(
                "return await Promise.all(Array.from({{ length: 10 }}, () => tools.exec({{ action: 'start', command: {}, loginShell: false }})));",
                json!(wait_cmd)
            ),
            "waitMs": 50
        }),
    );
    assert_eq!(
        started["structuredContent"]["state"], "running",
        "{started}"
    );
    let script_handle = started["structuredContent"]["taskId"]
        .as_str()
        .unwrap()
        .to_owned();
    let parent_bare = bare_id(&script_handle).to_owned();

    // Wait until all 8 START_PERMITS are active and 2 starts are queued.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (listed, _) = server.ok(json!({"action": "list"}));
        let parent = listed["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["taskId"] == parent_bare)
            .unwrap();
        if parent["children"]["active"] == 8 && parent["children"]["queued"] == 2 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for 8 active + 2 queued: {listed}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    let (killed, _) = server.ok(json!({"action": "kill", "taskId": &script_handle}));
    assert_eq!(killed["state"], "killed", "{killed}");

    let (listed, text) = server.ok(json!({"action": "list"}));
    let parent = &listed["tasks"][0];
    assert_eq!(parent["taskId"], parent_bare, "{listed}");
    assert_eq!(parent["state"], "killed", "{listed}");
    assert_eq!(parent["children"]["total"], 8, "{listed}");
    assert_eq!(parent["children"]["active"], 8, "{listed}");
    assert_eq!(parent["children"]["cancelled"], 2, "{listed}");
    assert_eq!(
        parent["children"]["activeTasks"].as_array().unwrap().len(),
        8,
        "{listed}"
    );
    assert!(text.contains("8 tasks (8 active, 2 cancelled)"), "{text}");
    assert_eq!(
        text.lines().filter(|l| l.starts_with("  └─ ")).count(),
        8,
        "{text}"
    );

    let (active, active_text) = server.ok(json!({"action": "list", "state": "active"}));
    let active_tasks = active["tasks"].as_array().unwrap();
    assert_eq!(active_tasks.len(), 8, "{active}");
    let via_tag = format!(" · via {parent_bare}");
    for task in active_tasks {
        assert_eq!(task["parentTaskId"], parent_bare, "{active}");
        assert_eq!(task["state"], "running", "{active}");
    }
    assert_eq!(active_text.matches(&via_tag).count(), 8, "{active_text}");

    // Release all 8 orphans and wait for them to finish without reporting finished tasks.
    std::fs::write(&*release, "go").unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let (active_now, _) = server.ok(json!({"action": "list", "state": "active"}));
        if active_now["tasks"].as_array().unwrap().is_empty() {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "orphans did not finish after release: {active_now}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }

    let (_, nag_text) = server.ok(json!({
        "action": "start",
        "command": "true",
        "loginShell": false,
    }));
    assert!(
        nag_text.contains("(Background:") && nag_text.contains("use list."),
        "expected unreported finished orphans in background footer: {nag_text}"
    );

    // One default list reports the folded finished orphans via the parent summary.
    let (after_list, after_text) = server.ok(json!({"action": "list"}));
    let parent_after = after_list["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["taskId"] == parent_bare)
        .unwrap();
    assert_eq!(parent_after["children"]["done"], 8, "{after_list}");
    assert_eq!(parent_after["children"]["cancelled"], 2, "{after_list}");
    assert!(
        after_text.contains("8 tasks (8 done, 2 cancelled)"),
        "{after_text}"
    );

    let (_, cleared_text) = server.ok(json!({
        "action": "start",
        "command": "true",
        "loginShell": false,
    }));
    assert!(
        !cleared_text.contains("(Background:"),
        "background nag persisted after list reported parent summary: {cleared_text}"
    );
}

#[test]
fn codemode_list_reports_live_queued_starts_accurately() {
    let mut server = codemode_server();
    let (release, release_bash) = scratch("codemode-queued-release");
    let wait_cmd = format!("while [ ! -f '{release_bash}' ]; do sleep 0.02; done");

    let started = code_result(
        &mut server,
        json!({
            "code": format!(
                "const all = await Promise.all(Array.from({{ length: 12 }}, () => tools.exec({{ action: 'start', command: {}, loginShell: false }}))); return all.length;",
                json!(wait_cmd)
            ),
            "waitMs": 50
        }),
    );
    assert_eq!(
        started["structuredContent"]["state"], "running",
        "{started}"
    );
    let script_handle = started["structuredContent"]["taskId"]
        .as_str()
        .unwrap()
        .to_owned();
    let parent_bare = bare_id(&script_handle).to_owned();

    let deadline = Instant::now() + Duration::from_secs(15);
    let (matched_list, matched_text) = loop {
        let (listed, text) = server.ok(json!({"action": "list"}));
        let parent = listed["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["taskId"] == parent_bare)
            .unwrap();
        let active = parent["children"]["active"].as_u64().unwrap_or(0);
        let queued = parent["children"]["queued"].as_u64().unwrap_or(0);
        assert!(
            active + queued <= 12,
            "double-counted active ({active}) + queued ({queued}) > 12: {listed}"
        );
        if active == 8 {
            break (listed, text);
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for 8 active starts: {listed}"
        );
        std::thread::sleep(Duration::from_millis(25));
    };

    let parent = matched_list["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["taskId"] == parent_bare)
        .unwrap();
    assert_eq!(parent["children"]["total"], 8, "{matched_list}");
    assert_eq!(parent["children"]["active"], 8, "{matched_list}");
    assert_eq!(parent["children"]["queued"], 4, "{matched_list}");
    assert!(
        matched_text.contains("8 tasks (8 active, 4 queued)"),
        "{matched_text}"
    );
    assert_eq!(
        matched_text
            .lines()
            .filter(|l| l.starts_with("  └─ "))
            .count(),
        8,
        "{matched_text}"
    );

    std::fs::write(&*release, "go").unwrap();
    let (done, _) = server.ok(json!({
        "action": "poll",
        "taskId": &script_handle,
        "waitMs": 15000
    }));
    assert_eq!(done["exitCode"], 0, "{done}");
    assert_eq!(done["output"], "12", "{done}");
}

#[test]
fn codemode_list_parent_task_id_drill_down_and_cross_session() {
    struct DaemonStopGuard(Command);
    impl Drop for DaemonStopGuard {
        fn drop(&mut self) {
            let _ = self.0.output();
        }
    }

    let runtime = Scratch::new("codemode-parent-drilldown-rt");
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&*runtime)
            .unwrap();
    }
    #[cfg(windows)]
    crate::support::create_private_runtime(&runtime);

    let namespace = format!("cm-drill-{}", std::process::id());
    let daemon_cmd = |action: &str| {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_fastexec"));
        crate::support::scrub_fastexec_env(&mut cmd);
        cmd.args(["daemon", action])
            .env("FASTEXEC_TEST_RUNTIME_DIR", &*runtime)
            .env("FASTEXEC_NAMESPACE", &namespace)
            .stdin(Stdio::null());
        cmd
    };

    let _stop = DaemonStopGuard(daemon_cmd("stop"));
    let start_out = daemon_cmd("start").output().unwrap();
    assert!(
        start_out.status.success(),
        "daemon start failed: {}{}",
        String::from_utf8_lossy(&start_out.stdout),
        String::from_utf8_lossy(&start_out.stderr)
    );

    let start_session = || {
        Server::start_with(|cmd| {
            cmd.env("FASTEXEC_DAEMON", "on")
                .env("FASTEXEC_TEST_RUNTIME_DIR", &*runtime)
                .env("FASTEXEC_NAMESPACE", &namespace)
                .env("FASTEXEC_CODEMODE", "true")
                .env("FASTEXEC_OUTPUT_MODE", "both");
        })
    };

    let mut session_a = start_session();
    let mut session_b = start_session();

    let script = code_result(
        &mut session_a,
        json!({
            "code": r#"
                for (const label of ["a1", "a2", "a3"]) {
                    const r = await tools.exec({
                        action: "start",
                        command: "printf " + label,
                        loginShell: false
                    });
                    if (r.exitCode !== 0) throw new Error("child failed");
                }
                return "three-done";
            "#
        }),
    );
    assert_eq!(script["structuredContent"]["exitCode"], 0, "{script}");
    let script_handle = script["structuredContent"]["taskId"]
        .as_str()
        .unwrap()
        .to_owned();
    let script_bare = bare_id(&script_handle).to_owned();

    let (plain, _) = session_a.ok(json!({
        "action": "start",
        "command": "printf plain-bash",
        "loginShell": false,
    }));
    let plain_bare = bare_id(plain["taskId"].as_str().unwrap()).to_owned();

    // Same-session drill-down accepts both bare task ID and reader handle.
    for query_id in [&script_bare, &script_handle] {
        let (listed, _) = session_a.ok(json!({
            "action": "list",
            "parentTaskId": query_id,
        }));
        let tasks = listed["tasks"].as_array().unwrap();
        assert_eq!(listed["totalCount"], 3, "{query_id}: {listed}");
        assert_eq!(tasks.len(), 3, "{query_id}: {listed}");
        for task in tasks {
            assert_eq!(task["parentTaskId"], script_bare, "{listed}");
            assert_eq!(task["kind"], "bash", "{listed}");
            assert!(task.get("children").is_none(), "{listed}");
        }
    }

    // Plain bash task ID returns an empty list.
    let (plain_listed, plain_text) = session_a.ok(json!({
        "action": "list",
        "parentTaskId": &plain_bare,
    }));
    assert_eq!(plain_text, "No tasks.", "{plain_text}");
    assert_eq!(plain_listed["totalCount"], 0, "{plain_listed}");
    assert!(
        plain_listed["tasks"].as_array().unwrap().is_empty(),
        "{plain_listed}"
    );

    // Unknown parentTaskId errors cleanly.
    let (_, err_text, is_err) = session_a.call(json!({
        "action": "list",
        "parentTaskId": "nonexistent-99",
    }));
    assert!(
        is_err,
        "expected error for unknown parentTaskId: {err_text}"
    );
    assert!(err_text.contains("Unknown parentTaskId"), "{err_text}");

    // Cross-session drill-down works by default for both bare ID and reader handle.
    for query_id in [&script_bare, &script_handle] {
        let (cross, _) = session_b.ok(json!({
            "action": "list",
            "parentTaskId": query_id,
        }));
        assert_eq!(cross["totalCount"], 3, "{query_id}: {cross}");
        assert_eq!(
            cross["tasks"].as_array().unwrap().len(),
            3,
            "{query_id}: {cross}"
        );
        for task in cross["tasks"].as_array().unwrap() {
            assert_eq!(task["parentTaskId"], script_bare, "{cross}");
        }
    }

    // Explicit scope: "session" restricts drill-down to caller's session.
    let (scoped, scoped_text) = session_b.ok(json!({
        "action": "list",
        "parentTaskId": &script_bare,
        "scope": "session",
    }));
    assert_eq!(scoped_text, "No tasks.", "{scoped_text}");
    assert_eq!(scoped["totalCount"], 0, "{scoped}");
    assert!(scoped["tasks"].as_array().unwrap().is_empty(), "{scoped}");
}

#[test]
fn codemode_list_finished_state_promotes_finished_children_of_running_parent() {
    let mut server = codemode_server();
    let (release, release_bash) = scratch("codemode-finished-promote-release");
    let wait_cmd = format!("while [ ! -f '{release_bash}' ]; do sleep 0.02; done");

    let started = code_result(
        &mut server,
        json!({
            "code": format!(
                r#"
                await tools.exec({{ action: "start", command: "printf done-1", loginShell: false }});
                await tools.exec({{ action: "start", command: "printf done-2", loginShell: false }});
                text("children-done");
                await tools.exec({{ action: "start", command: {}, loginShell: false }});
                return "all-done";
                "#,
                json!(wait_cmd)
            ),
            "waitMs": 10000,
            "returnWhen": {"outputContains": ["children-done"]},
        }),
    );
    assert_eq!(
        started["structuredContent"]["state"], "running",
        "{started}"
    );
    let script_handle = started["structuredContent"]["taskId"]
        .as_str()
        .unwrap()
        .to_owned();
    let parent_bare = bare_id(&script_handle).to_owned();

    let (finished, text) = server.ok(json!({"action": "list", "state": "finished"}));
    let tasks = finished["tasks"].as_array().unwrap();
    assert_eq!(finished["totalCount"], 2, "{finished}");
    assert_eq!(tasks.len(), 2, "{finished}");
    let via_tag = format!(" · via {parent_bare}");
    for task in tasks {
        assert_eq!(task["kind"], "bash", "{finished}");
        assert_eq!(task["state"], "exited", "{finished}");
        assert_eq!(task["parentTaskId"], parent_bare, "{finished}");
    }
    assert_eq!(text.matches(&via_tag).count(), 2, "{text}");

    std::fs::write(&*release, "go").unwrap();
    let (ended, _) = server.ok(json!({
        "action": "poll",
        "taskId": &script_handle,
        "waitMs": 10000,
    }));
    assert_eq!(ended["exitCode"], 0, "{ended}");
}

#[test]
fn codemode_list_sorts_by_active_child_recency_with_stable_total_count() {
    let mut server = codemode_server();
    let (spawn_gate, spawn_gate_bash) = scratch("codemode-recency-spawn-gate");

    let p_old = code_result(
        &mut server,
        json!({
            "code": format!(
                r#"
                text("ready");
                await tools.exec({{ action: "start", command: "while [ ! -f '{spawn_gate_bash}' ]; do sleep 0.02; done", loginShell: false }});
                const child = await tools.exec({{ action: "start", command: "sleep 30", waitMs: 0, loginShell: false }});
                text("spawned=" + child.taskId);
                await sleep(30000);
                "#
            ),
            "waitMs": 10000,
            "returnWhen": {"outputContains": ["ready"]},
        }),
    );
    assert_eq!(p_old["structuredContent"]["state"], "running", "{p_old}");
    let p_old_handle = p_old["structuredContent"]["taskId"]
        .as_str()
        .unwrap()
        .to_owned();
    let p_old_bare = bare_id(&p_old_handle).to_owned();

    // Create two newer finished top-level tasks after P_old.
    let (p1, _) = server.ok(json!({
        "action": "start",
        "command": "printf p1",
        "loginShell": false,
    }));
    let p1_bare = bare_id(p1["taskId"].as_str().unwrap()).to_owned();
    let (p2, _) = server.ok(json!({
        "action": "start",
        "command": "printf p2",
        "loginShell": false,
    }));
    let p2_bare = bare_id(p2["taskId"].as_str().unwrap()).to_owned();

    // Now trigger P_old to spawn fresh active child C_old (newer than P1 and P2).
    std::fs::write(&*spawn_gate, "go").unwrap();
    let (polled, polled_text) = server.ok(json!({
        "action": "poll",
        "taskId": &p_old_handle,
        "waitMs": 10000,
        "returnWhen": {"outputContains": ["spawned="]},
    }));
    let c_old_handle = polled["output"]
        .as_str()
        .unwrap()
        .lines()
        .find_map(|l| l.strip_prefix("spawned="))
        .unwrap_or_else(|| panic!("missing spawned= line: {polled_text}"))
        .to_owned();
    let c_old_bare = bare_id(&c_old_handle).to_owned();

    let (limited, limited_text) = server.ok(json!({"action": "list", "limit": 2}));
    let limited_tasks = limited["tasks"].as_array().unwrap();
    assert_eq!(limited["totalCount"], 3, "{limited}");
    assert_eq!(limited_tasks.len(), 2, "{limited}");
    assert_eq!(limited_tasks[0]["taskId"], p_old_bare, "{limited}");
    assert_eq!(
        limited_tasks[0]["children"]["activeTasks"][0]["taskId"], c_old_bare,
        "{limited}"
    );
    assert_eq!(limited_tasks[1]["taskId"], p2_bare, "{limited}");
    let first_line = limited_text.lines().next().unwrap();
    let second_line = limited_text.lines().nth(1).unwrap();
    assert!(first_line.starts_with(&p_old_bare), "{limited_text}");
    assert!(
        second_line.starts_with(&format!("  └─ {c_old_bare}")),
        "{limited_text}"
    );

    let (full, _) = server.ok(json!({"action": "list", "limit": 20}));
    assert_eq!(full["totalCount"], limited["totalCount"], "{full}");
    let full_ids: Vec<&str> = full["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["taskId"].as_str().unwrap())
        .collect();
    assert_eq!(
        full_ids,
        vec![p_old_bare.as_str(), p2_bare.as_str(), p1_bare.as_str()],
        "{full}"
    );

    server.ok(json!({"action": "kill", "taskId": &p_old_handle}));
    server.ok(json!({"action": "kill", "taskId": &c_old_handle}));
}

#[test]
fn codemode_list_fit_lines_atomic_blocks_and_outbound_limit_no_partial_report() {
    // Part 1: atomic fit_lines under small maxBytes never strands an indented child line.
    let mut server = codemode_server();
    for i in 1..=3 {
        server.ok(json!({
            "action": "start",
            "command": format!("printf top-{i}"),
            "loginShell": false,
        }));
    }
    let started = code_result(
        &mut server,
        json!({
            "code": r#"
                const c1 = await tools.exec({ action: "start", command: "sleep 30", waitMs: 0, loginShell: false });
                const c2 = await tools.exec({ action: "start", command: "sleep 30", waitMs: 0, loginShell: false });
                return [c1.taskId, c2.taskId];
            "#
        }),
    );
    assert_eq!(started["structuredContent"]["exitCode"], 0, "{started}");
    let child_handles: Vec<String> =
        serde_json::from_str(started["structuredContent"]["output"].as_str().unwrap()).unwrap();

    for truncate in ["head_tail", "tail"] {
        for max_bytes in [150_u64, 250, 350, 450, 600] {
            let (_, text) = server.ok(json!({
                "action": "list",
                "maxBytes": max_bytes,
                "truncate": truncate,
            }));
            let lines: Vec<&str> = text.lines().collect();
            for (idx, line) in lines.iter().enumerate() {
                if line.starts_with("  └─ ") {
                    assert!(
                        idx > 0,
                        "orphaned child line at top (truncate={truncate}, maxBytes={max_bytes}): {text}"
                    );
                    let prev = lines[idx - 1];
                    assert!(
                        !prev.starts_with("... ["),
                        "orphaned child line after omission marker (truncate={truncate}, maxBytes={max_bytes}): {text}"
                    );
                }
            }
        }
    }
    for handle in child_handles {
        server.ok(json!({"action": "kill", "taskId": handle}));
    }

    // Part 2: FASTEXEC_TEST_OUTBOUND_LIMIT rejects an oversized list with 8 inline active children
    // cleanly and does not partially mark unreported tasks reported.
    let mut bounded = Server::start_with(|cmd| {
        cmd.env("FASTEXEC_CODEMODE", "true")
            .env("FASTEXEC_OUTPUT_MODE", "both")
            .env("FASTEXEC_TEST_OUTBOUND_LIMIT", "2600");
    });

    let eight = code_result(
        &mut bounded,
        json!({
            "code": r#"
                const ids = [];
                for (let i = 0; i < 8; i++) {
                    const c = await tools.exec({ action: "start", command: "sleep 30", waitMs: 0, loginShell: false });
                    ids.push(c.taskId);
                }
                return ids;
            "#
        }),
    );
    assert_eq!(eight["structuredContent"]["exitCode"], 0, "{eight}");
    let eight_handles: Vec<String> =
        serde_json::from_str(eight["structuredContent"]["output"].as_str().unwrap()).unwrap();

    // Start an unreported failing task after `eight` so `eight`'s response cannot report it.
    let (unreported, _) = bounded.ok(json!({
        "action": "start",
        "command": "sleep 0.05; exit 2",
        "loginShell": false,
        "waitMs": 0,
    }));
    let unreported_bare = bare_id(unreported["taskId"].as_str().unwrap()).to_owned();

    // Wait for the background task to exit before calling list.
    std::thread::sleep(Duration::from_millis(150));

    let (_, err_text, is_err) = bounded.call(json!({"action": "list"}));
    assert!(is_err, "expected outbound limit error on list: {err_text}");
    assert!(
        err_text.contains("over the frame limit"),
        "unexpected outbound error: {err_text}"
    );

    // Unreported finished task must still surface in the background footer.
    let (_, bg_text) = bounded.ok(json!({
        "action": "start",
        "command": "true",
        "loginShell": false,
    }));
    assert!(
        bg_text.contains("(Background:") && bg_text.contains(&unreported_bare),
        "failed list partially marked {unreported_bare} reported: {bg_text}"
    );

    for handle in eight_handles {
        bounded.ok(json!({"action": "kill", "taskId": handle}));
    }
}

#[test]
fn codemode_list_overflows_over_8_active_children_and_status_line_hints() {
    let mut server = codemode_server();

    // Case A: normal exited codemode with 10 active keepTree children (> 8 overflow + > 3 hint).
    let exited = code_result(
        &mut server,
        json!({
            "code": r#"
                for (let i = 0; i < 10; i++) {
                    const r = await tools.exec({
                        action: "start",
                        command: "sleep 30 &",
                        keepTree: true,
                        loginShell: false,
                    });
                    if (r.exitCode !== 0 || !r.keptRunning) {
                        throw new Error("expected keptRunning child: " + JSON.stringify(r));
                    }
                }
                return "ten-kept";
            "#
        }),
    );
    assert_eq!(exited["structuredContent"]["state"], "exited", "{exited}");
    assert_eq!(exited["structuredContent"]["exitCode"], 0, "{exited}");
    let exited_bare = bare_id(exited["structuredContent"]["taskId"].as_str().unwrap()).to_owned();
    let expected_exited_hint =
        format!("; list active with exec list parentTaskId=\"{exited_bare}\" state=\"active\"");
    assert!(
        code_text(&exited).contains(&expected_exited_hint),
        "missing exited status_line hint: {}",
        code_text(&exited)
    );

    let (listed, list_text) = server.ok(json!({"action": "list"}));
    let parent = &listed["tasks"][0];
    assert_eq!(parent["taskId"], exited_bare, "{listed}");
    assert_eq!(parent["children"]["total"], 10, "{listed}");
    assert_eq!(parent["children"]["active"], 10, "{listed}");
    assert_eq!(
        parent["children"]["activeTasks"].as_array().unwrap().len(),
        8,
        "{listed}"
    );
    assert!(list_text.contains("10 tasks (10 active)"), "{list_text}");
    let overflow_line = format!(
        "  └─ ... 2 more active; list with parentTaskId=\"{exited_bare}\" state=\"active\""
    );
    assert!(
        list_text.lines().any(|l| l == overflow_line),
        "missing overflow hint line: {list_text}"
    );
    let indented_task_lines = list_text
        .lines()
        .filter(|l| l.starts_with("  └─ ") && !l.starts_with("  └─ ... "))
        .count();
    assert_eq!(indented_task_lines, 8, "{list_text}");

    // Clean up all 10 active children via parentTaskId drill-down.
    let (children, _) = server.ok(json!({
        "action": "list",
        "parentTaskId": &exited_bare,
        "state": "active",
    }));
    assert_eq!(children["totalCount"], 10, "{children}");
    for child in children["tasks"].as_array().unwrap() {
        server.ok(json!({"action": "kill", "taskId": child["taskId"]}));
    }

    // Case B: killed codemode with > 3 active children also appends the status_line hint.
    let running = code_result(
        &mut server,
        json!({
            "code": r#"
                for (let i = 0; i < 4; i++) {
                    await tools.exec({
                        action: "start",
                        command: "sleep 30 &",
                        keepTree: true,
                        loginShell: false,
                    });
                }
                text("four-ready");
                await sleep(30000);
            "#,
            "waitMs": 10000,
            "returnWhen": {"outputContains": ["four-ready"]},
        }),
    );
    let killed_handle = running["structuredContent"]["taskId"]
        .as_str()
        .unwrap()
        .to_owned();
    let killed_bare = bare_id(&killed_handle).to_owned();
    let (killed, kill_text) = server.ok(json!({"action": "kill", "taskId": &killed_handle}));
    assert_eq!(killed["state"], "killed", "{kill_text}");
    let expected_killed_hint =
        format!("; list active with exec list parentTaskId=\"{killed_bare}\" state=\"active\"");
    assert!(
        kill_text.contains(&expected_killed_hint),
        "missing killed status_line hint: {kill_text}"
    );

    let (killed_children, _) = server.ok(json!({
        "action": "list",
        "parentTaskId": &killed_bare,
        "state": "active",
    }));
    assert_eq!(killed_children["totalCount"], 4, "{killed_children}");
    for child in killed_children["tasks"].as_array().unwrap() {
        server.ok(json!({"action": "kill", "taskId": child["taskId"]}));
    }
}

#[test]
fn list_does_not_report_warnings_from_trimmed_active_children() {
    let mut server = codemode_server();
    let started = code_result(
        &mut server,
        json!({
            "code": "await tools.exec({action:'start',command:'sleep 30 &',keepTree:true,waitMs:5000}); await sleep(30000);",
            "waitMs": 0
        }),
    );
    let parent = started["structuredContent"]["taskId"]
        .as_str()
        .unwrap()
        .to_owned();
    let parent_id = bare_id(&parent).to_owned();
    let deadline = Instant::now() + Duration::from_secs(15);
    let child_id = loop {
        let (listed, _) =
            server.ok(json!({"action":"list", "parentTaskId":parent_id, "state":"active"}));
        if let Some(child) = listed["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|task| task["outputOpen"] == true)
        {
            break child["taskId"].as_str().unwrap().to_owned();
        }
        assert!(Instant::now() < deadline, "{listed}");
        std::thread::sleep(Duration::from_millis(25));
    };
    let (listed, text) = server.ok(json!({"action":"list","limit":1,"maxBytes":100}));
    assert!(text.contains(&parent_id), "{text}");
    assert!(!text.contains(&child_id), "{text}");
    assert!(listed.get("warnings").is_none(), "{listed}");
    assert!(!text.contains("Warning:"), "{text}");
    assert!(
        listed["tasks"][0]["children"]["activeTasks"][0]["warnings"].is_array(),
        "{listed}"
    );
    server.ok(json!({"action":"kill","taskId":parent}));
    server.ok(json!({"action":"kill","taskId":child_id}));
}
