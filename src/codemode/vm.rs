//! QuickJS runtime setup, event loop, and JavaScript prelude.

use super::bridge::{
    BRIDGE_MEMORY_RESULT_ERROR, BRIDGE_RESULT_ERROR, DIAGNOSTIC_RESERVE, DIAGNOSTIC_WORKSPACE,
    Host, IO_PERMITS, Output, START_PERMITS, SyncCallError,
};
use super::control::{BridgeHandle, Outcome, SettledCall, lock};
use crate::env::LaunchContext;
use crate::files;
use crate::media::{self, CompressFailure, MediaKind};
use crate::service::Service;
use crate::tasks::script::CallInfo;
use crate::tasks::session::Session;
use crate::tasks::state::Task;
use crate::tasks::{ScriptIo, Tasks};
use base64::Engine;
use rquickjs::context::EvalOptions;
use rquickjs::{Context, Ctx, Exception, Function, Object, Persistent, Runtime, Value};
use serde_json::json;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::sync::{Arc, mpsc};
use tokio::runtime::Handle;
use tokio::sync::Semaphore;

pub(crate) struct Work {
    pub(super) service: Arc<Service>,
    pub(super) session: Arc<Session>,
    pub(super) launch: Arc<LaunchContext>,
    pub(super) transfer: bool,
    pub(super) call_id: Option<u64>,
    pub(super) code: String,
    pub(super) runtime: Handle,
    pub(super) input_owner: crate::lease::Owner,
    #[cfg(test)]
    pub(super) vm_memory_limit: Option<usize>,
}

// QuickJS mixes column origins on its first physical line; user code starts on the next one.
const SCRIPT_PREFIX: &str = "(async () => {\n";
const MEMORY_LIMIT: usize = 256 * 1024 * 1024;
pub(super) const STALLED: &str =
    "The script is waiting on a promise that can never settle: no tool call or sleep is pending.";

pub(super) fn pending_text(calls: &[CallInfo]) -> String {
    use std::fmt::Write;
    let limit = DIAGNOSTIC_RESERVE as usize / 2;
    let mut text = String::new();
    for (index, call) in calls.iter().enumerate() {
        if index != 0 {
            text.push_str(", ");
        }
        for (part_index, part) in [
            Some(call.tool.as_str()),
            call.action.as_deref(),
            call.task_id.as_deref(),
        ]
        .into_iter()
        .flatten()
        .enumerate()
        {
            if part_index != 0 {
                text.push(' ');
            }
            let room = limit.saturating_sub(text.len() + 64);
            if part.len() > room {
                let mut end = room;
                while !part.is_char_boundary(end) {
                    end -= 1;
                }
                text.push_str(&part[..end]);
                let _ = write!(
                    &mut text,
                    "... ({} call(s) omitted)",
                    calls.len() - index - 1
                );
                return text;
            }
            text.push_str(part);
        }
    }
    text
}

pub(super) fn run(
    io: ScriptIo,
    work: Work,
    script: BridgeHandle,
    settled_rx: mpsc::Receiver<SettledCall>,
) {
    let ScriptIo {
        task,
        tasks,
        writer,
        captured,
    } = io;
    let Work {
        service,
        session,
        launch,
        transfer,
        call_id,
        code,
        runtime,
        input_owner,
        #[cfg(test)]
        vm_memory_limit,
    } = work;
    let code = crate::lease::Owned::new(code, input_owner);
    let output = Rc::new(RefCell::new(Output { writer, written: 0 }));
    let Some(diagnostic_lease) = launch.control_memory().try_acquire(DIAGNOSTIC_WORKSPACE) else {
        output.borrow_mut().diagnostic(&[
            "Script error: diagnostic memory budget is full; the script was not started.",
        ]);
        drop(code);
        drop(settled_rx);
        drop(output);
        tasks.finish_script(&task, Some(1), captured);
        return;
    };
    let host = Rc::new(RefCell::new(Host {
        service,
        session,
        launch,
        transfer,
        call_id,
        parent_task_id: task.id.clone(),
        runtime,
        script: script.clone(),
        start_permits: Arc::new(Semaphore::new(START_PERMITS)),
        io_permits: Arc::new(Semaphore::new(IO_PERMITS)),
        next: 1,
        starts: 0,
    }));
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_vm(
            &code.value,
            &script,
            &settled_rx,
            Rc::clone(&host),
            Rc::clone(&output),
            (&task, &tasks),
            #[cfg(test)]
            vm_memory_limit,
        )
    }))
    .unwrap_or_else(|_| Err("The JavaScript worker panicked.".into()));
    drop(code);
    drop(host);
    script.cancel_and_drain();
    for settled in settled_rx.try_iter() {
        if !matches!(settled.outcome, Outcome::Cancelled(_)) {
            lock(&script.0.report).remove(settled.id);
        }
    }
    let pending = script.finish();
    let exit_code = if script.control().is_stopped() {
        let count = pending.value.len().to_string();
        let pending = pending_text(&pending.value);
        output.borrow_mut().diagnostic(&[
            "Script stopped: cancelled ",
            &count,
            " pending call(s): ",
            &pending,
        ]);
        None
    } else if script.0.exited.load(Ordering::Acquire) {
        Some(0)
    } else if let Err(message) = &result {
        let pending = pending_text(&pending.value);
        let prefix = if pending.is_empty() {
            ""
        } else {
            "\nPending calls: "
        };
        output
            .borrow_mut()
            .diagnostic(&["Script error: ", message, prefix, &pending]);
        Some(1)
    } else {
        Some(0)
    };
    drop(settled_rx);
    drop(pending);
    drop(result);
    drop(output);
    drop(diagnostic_lease);
    tasks.finish_script(&task, exit_code, captured);
}

fn push_diagnostic(text: &mut String, part: &str) -> bool {
    let room = (DIAGNOSTIC_RESERVE as usize).saturating_sub(text.len());
    if part.len() <= room {
        text.push_str(part);
        return true;
    }
    let mut end = part.len().min(room);
    while !part.is_char_boundary(end) {
        end -= 1;
    }
    text.push_str(&part[..end]);
    let mut end = text.len().min(DIAGNOSTIC_RESERVE as usize - 3);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text.push_str("...");
    false
}

pub(super) fn describe<'js>(ctx: &Ctx<'js>, value: Value<'js>) -> String {
    use rquickjs::{Coerced, FromJs};
    // CString remains owned by QuickJS; only the bounded description is copied to Rust.
    let prop = |object: &Object<'js>, key: &str| -> Option<rquickjs::CString<'js>> {
        let result = object
            .get::<_, Option<Coerced<rquickjs::String<'js>>>>(key)
            .and_then(|value| value.map(|coerced| coerced.0.to_cstring()).transpose());
        match result {
            Ok(value) => value,
            Err(error) => {
                if error.is_exception() {
                    let _ = ctx.catch();
                }
                None
            }
        }
    };
    let mut text = String::with_capacity(DIAGNOSTIC_RESERVE as usize);
    if let Some(exception) = value.as_exception() {
        let object = exception.as_object();
        if !push_diagnostic(
            &mut text,
            prop(object, "name").as_deref().unwrap_or("Error"),
        ) || !push_diagnostic(&mut text, ": ")
            || !push_diagnostic(&mut text, prop(object, "message").as_deref().unwrap_or(""))
        {
            return text;
        }
        if let Some(stack) = prop(object, "stack") {
            for line in stack
                .lines()
                .filter(|line| !line.contains("codemode-prelude.js"))
            {
                if !push_diagnostic(&mut text, "\n") || !push_diagnostic(&mut text, line) {
                    break;
                }
            }
        }
    } else {
        match Coerced::<rquickjs::String>::from_js(ctx, value)
            .and_then(|value| value.0.to_cstring())
        {
            Ok(value) => {
                push_diagnostic(&mut text, &value);
            }
            Err(error) => {
                if error.is_exception() {
                    let _ = ctx.catch();
                }
                push_diagnostic(
                    &mut text,
                    "The script threw a value that cannot be described.",
                );
            }
        }
    }
    text
}

fn is_js_whitespace(ch: char) -> bool {
    matches!(
        ch,
        '\u{0009}'..='\u{000D}'
            | '\u{0020}'
            | '\u{00A0}'
            | '\u{1680}'
            | '\u{2000}'..='\u{200A}'
            | '\u{2028}'
            | '\u{2029}'
            | '\u{202F}'
            | '\u{205F}'
            | '\u{3000}'
            | '\u{FEFF}'
    )
}

fn js_error(ctx: &Ctx<'_>, error: rquickjs::Error) -> String {
    if error.is_exception() {
        describe(ctx, ctx.catch())
    } else {
        error.to_string()
    }
}

fn run_vm(
    code: &str,
    script: &BridgeHandle,
    settled_rx: &mpsc::Receiver<SettledCall>,
    host: Rc<RefCell<Host>>,
    output: Rc<RefCell<Output>>,
    (task, tasks): (&Arc<Task>, &Arc<Tasks>),
    #[cfg(test)] vm_memory_limit: Option<usize>,
) -> Result<(), String> {
    if script.interrupted() {
        return Ok(());
    }
    let runtime = Runtime::new().map_err(|error| error.to_string())?;
    #[cfg(test)]
    let memory_limit = vm_memory_limit.unwrap_or(MEMORY_LIMIT);
    #[cfg(not(test))]
    let memory_limit = MEMORY_LIMIT;
    runtime.set_memory_limit(memory_limit);
    runtime.set_max_stack_size(512 * 1024);
    let interrupt = script.clone();
    runtime.set_interrupt_handler(Some(Box::new(move || interrupt.interrupted())));
    let rejections = Rc::new(RefCell::new(HashMap::<
        Persistent<Value<'static>>,
        Persistent<Value<'static>>,
    >::new()));
    let tracking = Rc::clone(&rejections);
    runtime.set_host_promise_rejection_tracker(Some(Box::new(
        move |ctx, promise, reason, handled| {
            let promise = Persistent::save(&ctx, promise);
            if handled {
                tracking.borrow_mut().remove(&promise);
            } else {
                tracking
                    .borrow_mut()
                    .insert(promise, Persistent::save(&ctx, reason));
            }
        },
    )));
    let context = Context::full(&runtime).map_err(|error| error.to_string())?;
    let result = (|| {
        let api = context.with(|ctx| -> Result<Persistent<Object<'static>>, String> {
            let mut options = EvalOptions::default();
            options.filename = Some("codemode-prelude.js".into());
            let factory: Function = ctx
                .eval_with_options(PRELUDE, options)
                .map_err(|error| js_error(&ctx, error))?;
            let writing = Rc::clone(&output);
            let control = script.clone();
            let write = Function::new(ctx.clone(), move |ctx: Ctx<'_>, text: rquickjs::CString<'_>| {
                if control.interrupted() {
                    return Err(Exception::throw_message(&ctx, "The script is ending."));
                }
                writing
                    .borrow_mut()
                    .write(&text)
                    .map_err(|error| Exception::throw_range(&ctx, &error))
            })
            .map_err(|error| js_error(&ctx, error))?;
            let launch_ctx = Arc::clone(&host.borrow().launch);
            let call = Function::new(
                ctx.clone(),
                move |ctx: Ctx<'_>,
                      tool: rquickjs::CString<'_>,
                      args: rquickjs::CString<'_>|
                      -> rquickjs::Result<String> {
                    match host.borrow_mut().call(tool.as_str(), args.as_str()) {
                        Ok(id) => Ok(id.to_string()),
                        Err(error) => match &error.value {
                            SyncCallError::Type(message) => Err(Exception::throw_type(&ctx, message)),
                            SyncCallError::Limit(message) => {
                                Ok(json!({ "code": "LIMIT", "message": message }).to_string())
                            }
                            SyncCallError::Overload(message) => {
                                Ok(json!({ "code": "OVERLOAD", "message": message }).to_string())
                            }
                        },
                    }
                },
            )
            .map_err(|error| js_error(&ctx, error))?;
            let task = Arc::clone(task);
            let tasks = Arc::clone(tasks);
            let control = script.clone();
            let attach = Function::new(
                ctx.clone(),
                move |ctx: Ctx<'_>,
                      mode: u8,
                      payload: rquickjs::CString<'_>,
                      declared_mime: rquickjs::CString<'_>,
                      expected_kind: rquickjs::CString<'_>|
                      -> rquickjs::Result<String> {
                    if control.interrupted() {
                        return Err(Exception::throw_message(&ctx, "The script is ending."));
                    }
                    // The callback borrows VM strings. Path and MIME diagnostics
                    // can create Rust copies independently of the media bytes.
                    let copied = declared_mime.len().saturating_add(expected_kind.len());
                    let _argument_workspace = if mode == 1 {
                        files::path::with_path_workspace(
                            payload.as_str(),
                            &launch_ctx,
                            copied.saturating_mul(16),
                            |_| (),
                        )
                        .map_err(|error| Exception::throw_range(&ctx, &error))?
                        .owner
                    } else {
                        crate::lease::Owner::new(launch_ctx.memory()
                            .try_acquire(copied.saturating_mul(16).saturating_add(4096))
                            .ok_or_else(|| Exception::throw_range(
                                &ctx, "Server active memory budget is full."
                            ))?)
                    };
                    let max_bytes = launch_ctx
                        .max_media_bytes()
                        .map_err(|err| Exception::throw_type(&ctx, &err))?;
                    let compress = launch_ctx
                        .image_compress()
                        .map_err(|err| Exception::throw_type(&ctx, &err))?;

                    let (bytes, mut lease, kind, mime): (
                        Vec<u8>,
                        crate::lease::Lease,
                        MediaKind,
                        String,
                    ) = match mode {
                        0 => {
                            let hid: u64 = payload.parse().map_err(|_| {
                                Exception::throw_type(&ctx, "Invalid attachment handle.")
                            })?;
                            let Some(att_lease) = control.get_attachment(hid) else {
                                return Err(Exception::throw_type(
                                    &ctx,
                                    "Invalid or expired attachment handle.",
                                ));
                            };
                            let src_meta = att_lease.meta().clone();
                            let mut file = std::fs::File::open(&src_meta.path).map_err(|err| {
                                Exception::throw_type(
                                    &ctx,
                                    &format!("Cannot read source attachment: {err}."),
                                )
                            })?;
                            let (loaded, loaded_lease) = match files::read::read_media_from_zero(
                                &mut file,
                                src_meta.size,
                                files::read::MediaStart::Rewind,
                                max_bytes,
                                launch_ctx.memory(),
                                None,
                                Some(control.control().cancel_token()),
                            ) {
                                Ok(files::read::MediaBytes::Loaded { bytes, lease }) => (bytes, lease),
                                Ok(files::read::MediaBytes::Oversized { .. }) => {
                                    return Err(Exception::throw_range(
                                        &ctx,
                                        &format!(
                                            "Attachment exceeds FASTEXEC_MAX_MEDIA_BYTES ({max_bytes} bytes)."
                                        ),
                                    ));
                                }
                                Err(files::read::MediaReadError::Overload) => {
                                    return Err(Exception::throw_range(
                                        &ctx,
                                        "Server active memory budget is full.",
                                    ));
                                }
                                Err(files::read::MediaReadError::Cancelled) => {
                                    return Err(Exception::throw_message(
                                        &ctx,
                                        "The script is ending.",
                                    ));
                                }
                                Err(files::read::MediaReadError::Io(err)) => {
                                    return Err(Exception::throw_type(
                                        &ctx,
                                        &format!("Cannot read source attachment: {err}."),
                                    ));
                                }
                            };
                            drop(att_lease);
                            if loaded.len() as u64 != src_meta.size {
                                drop(loaded);
                                drop(loaded_lease);
                                return Err(Exception::throw_type(
                                    &ctx,
                                    "Source attachment size changed on disk.",
                                ));
                            }
                            (loaded, loaded_lease, src_meta.kind, src_meta.mime)
                        }
                        1 => {
                            let resolved = files::path::resolve(&payload, &launch_ctx)
                                .map_err(|err| Exception::throw_type(&ctx, &err))?;
                            let shown = files::path::display_path(&resolved);
                            let (mut file, meta) =
                                files::path::open_regular(&resolved).map_err(|err| {
                                    Exception::throw_type(
                                        &ctx,
                                        &format!("Cannot read {shown}: {err}."),
                                    )
                                })?;
                            let mut prefix = [0u8; media::SNIFF];
                            let mut sniffed = 0;
                            while sniffed < prefix.len() {
                                match std::io::Read::read(&mut file, &mut prefix[sniffed..]) {
                                    Ok(0) => break,
                                    Ok(n) => sniffed += n,
                                    Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {
                                        continue;
                                    }
                                    Err(err) => {
                                        return Err(Exception::throw_type(
                                            &ctx,
                                            &format!("Cannot read {shown}: {err}."),
                                        ));
                                    }
                                }
                            }
                            let Some(det) =
                                media::detect_path(&prefix[..sniffed], Some(&resolved))
                            else {
                                return Err(Exception::throw_type(
                                    &ctx,
                                    &format!(
                                        "Cannot attach {shown}: file is not a supported image, audio, PDF, or video format."
                                    ),
                                ));
                            };
                            match files::read::read_media_from_zero(
                                &mut file,
                                meta.len(),
                                files::read::MediaStart::Prefix(&prefix[..sniffed]),
                                max_bytes,
                                launch_ctx.memory(),
                                None,
                                Some(control.control().cancel_token()),
                            ) {
                                Ok(files::read::MediaBytes::Loaded { bytes, lease }) => {
                                    (bytes, lease, det.kind, det.mime.to_owned())
                                }
                                Ok(files::read::MediaBytes::Oversized { .. }) => {
                                    return Err(Exception::throw_range(
                                        &ctx,
                                        &format!(
                                            "Cannot attach {shown}: exceeds FASTEXEC_MAX_MEDIA_BYTES ({max_bytes} bytes)."
                                        ),
                                    ));
                                }
                                Err(files::read::MediaReadError::Overload) => {
                                    return Err(Exception::throw_range(
                                        &ctx,
                                        "Server active memory budget is full.",
                                    ));
                                }
                                Err(files::read::MediaReadError::Cancelled) => {
                                    return Err(Exception::throw_message(
                                        &ctx,
                                        "The script is ending.",
                                    ));
                                }
                                Err(files::read::MediaReadError::Io(err)) => {
                                    return Err(Exception::throw_type(
                                        &ctx,
                                        &format!("Cannot read {shown}: {err}."),
                                    ));
                                }
                            }
                        }
                        _ => {
                            let clean_len: usize = payload.chars()
                                .filter(|ch| !is_js_whitespace(*ch))
                                .map(char::len_utf8).sum();
                            let needs_cleaning = clean_len != payload.len();
                            let unpadded_len: usize = payload.chars().rev()
                                .skip_while(|ch| is_js_whitespace(*ch) || *ch == '=')
                                .filter(|ch| !is_js_whitespace(*ch))
                                .map(char::len_utf8).sum();
                            let min_decoded = (unpadded_len / 4)
                                .saturating_mul(3)
                                .saturating_add((unpadded_len % 4).saturating_sub(1));
                            if min_decoded as u64 > max_bytes {
                                return Err(Exception::throw_range(
                                    &ctx,
                                    &format!(
                                        "Inline media exceeds FASTEXEC_MAX_MEDIA_BYTES ({max_bytes} bytes)."
                                    ),
                                ));
                            }
                            let est_decoded =
                                (clean_len / 4).saturating_mul(3).saturating_add(3);
                            let Some(mut inline_lease) = (if needs_cleaning { clean_len } else { 0usize })
                                .checked_add(est_decoded)
                                .and_then(|peak| launch_ctx.memory().try_acquire(peak))
                            else {
                                return Err(Exception::throw_range(
                                    &ctx,
                                    "Server active memory budget is full.",
                                ));
                            };
                            let cleaned_owned = needs_cleaning.then(|| {
                                let mut cleaned = String::with_capacity(clean_len);
                                cleaned.extend(payload.chars().filter(|ch| !is_js_whitespace(*ch)));
                                cleaned
                            });
                            let b64_str = cleaned_owned.as_deref().unwrap_or(payload.as_str());
                            let raw = base64::engine::general_purpose::STANDARD
                                .decode(b64_str)
                                .map_err(|_| {
                                    Exception::throw_type(
                                        &ctx,
                                        "Inline media data is not valid base64.",
                                    )
                                })?;
                            drop(cleaned_owned);
                            inline_lease.shrink_to(raw.capacity());
                            if raw.len() as u64 > max_bytes {
                                return Err(Exception::throw_range(
                                    &ctx,
                                    &format!(
                                        "Inline media exceeds FASTEXEC_MAX_MEDIA_BYTES ({max_bytes} bytes)."
                                    ),
                                ));
                            }
                            let hint = (!declared_mime.is_empty()).then_some(declared_mime.as_str());
                            let det = media::detect_inline(&raw, hint)
                                .map_err(|err| Exception::throw_type(&ctx, &err))?;
                            if !expected_kind.is_empty() && det.kind.as_str() != expected_kind.as_str() {
                                return Err(Exception::throw_type(
                                    &ctx,
                                    &format!(
                                        "MCP {} block contains {} ({}) bytes.",
                                        expected_kind.as_str(),
                                        det.kind.as_str(),
                                        det.mime
                                    ),
                                ));
                            }
                            (raw, inline_lease, det.kind, det.mime.to_owned())
                        }
                    };

                    let (final_bytes, final_mime) = if compress
                        && kind == MediaKind::Image
                        && media::is_compressible_image(&mime, &bytes)
                    {
                        match media::compress_image(bytes, &mime, &mut lease) {
                            Ok(img) => (img.bytes, img.mime),
                            Err(CompressFailure::Overload) => {
                                return Err(Exception::throw_range(
                                    &ctx,
                                    "Server active memory budget is full.",
                                ));
                            }
                            Err(CompressFailure::Rejected(reason)) => {
                                return Err(Exception::throw_type(&ctx, &reason));
                            }
                        }
                    } else {
                        (bytes, mime)
                    };
                    let stored = tasks
                        .store_attachment(&task, kind, &final_mime, &final_bytes)
                        .map_err(|error| Exception::throw_range(&ctx, &error))?;
                    drop(final_bytes);
                    drop(lease);
                    Ok(json!({
                        "index": stored.index,
                        "kind": stored.kind.as_str(),
                        "mimeType": stored.mime,
                        "bytes": stored.size,
                    })
                    .to_string())
                },
            )
            .map_err(|error| js_error(&ctx, error))?;
            let control = script.clone();
            let exit = Function::new(ctx.clone(), move || {
                control.0.exited.store(true, Ordering::Release);
            })
            .map_err(|error| js_error(&ctx, error))?;
            let api: Object = factory
                .call((write, call, attach, exit))
                .map_err(|error| js_error(&ctx, error))?;
            let mut options = EvalOptions::default();
            options.filename = Some("codemode.js".into());
            let function: Function = ctx
                .eval_with_options(format!("{SCRIPT_PREFIX}{code}\n}})"), options)
                .map_err(|error| js_error(&ctx, error))?;
            let run: Function = api.get("run").map_err(|error| js_error(&ctx, error))?;
            run.call::<_, ()>((function,))
                .map_err(|error| js_error(&ctx, error))?;
            Ok(Persistent::save(&ctx, api))
        })?;
        loop {
            if script.interrupted() {
                return Ok(());
            }
            let mut jobs = 0;
            while runtime.is_job_pending() {
                runtime
                    .execute_pending_job()
                    .map_err(|error| error.0.with(|ctx| describe(&ctx, ctx.catch())))?;
                jobs += 1;
                if jobs % 50 == 0 && script.interrupted() {
                    return Ok(());
                }
            }
            if script.interrupted() {
                return Ok(());
            }
            let unhandled = rejections.borrow().values().next().cloned();
            if let Some(reason) = unhandled {
                return Err(context.with(|ctx| {
                    reason
                        .restore(&ctx)
                        .map(|value| describe(&ctx, value))
                        .unwrap_or_else(|error| error.to_string())
                }));
            }
            let finished = context.with(|ctx| -> Result<bool, String> {
                let api = api
                    .clone()
                    .restore(&ctx)
                    .map_err(|error| js_error(&ctx, error))?;
                let status: Function = api.get("status").map_err(|error| js_error(&ctx, error))?;
                let state: i32 = status.call(()).map_err(|error| js_error(&ctx, error))?;
                match state {
                    0 => Ok(false),
                    1 => Ok(true),
                    2 => {
                        let failure: Function =
                            api.get("failure").map_err(|error| js_error(&ctx, error))?;
                        let value: Value =
                            failure.call(()).map_err(|error| js_error(&ctx, error))?;
                        Err(describe(&ctx, value))
                    }
                    _ => Err(STALLED.into()),
                }
            })?;
            if finished {
                return Ok(());
            }
            match settled_rx.try_recv() {
                Ok(SettledCall { id, outcome }) => {
                    if matches!(outcome, Outcome::Cancelled(_)) {
                        return Ok(());
                    }
                    let settled = context.with(|ctx| -> Result<(), String> {
                        let api = api
                            .clone()
                            .restore(&ctx)
                            .map_err(|error| js_error(&ctx, error))?;
                        let settle: Function =
                            api.get("settle").map_err(|error| js_error(&ctx, error))?;
                        let (ok, err_code, payload, task_id, log_path) = match &outcome {
                            Outcome::Value(value) => (true, "", value.value.as_str(), "", ""),
                            Outcome::Error(error) => (false, "IO", error.as_ref(), "", ""),
                            Outcome::Cancelled(error) => {
                                (false, "CANCELLED", error.as_ref(), "", "")
                            }
                            Outcome::Limit => (false, "LIMIT", BRIDGE_RESULT_ERROR, "", ""),
                            Outcome::Memory(Some((tid, lpath))) => (
                                false,
                                "OVERLOAD",
                                BRIDGE_MEMORY_RESULT_ERROR,
                                tid.as_str(),
                                lpath.as_str(),
                            ),
                            Outcome::Memory(None) => {
                                (false, "OVERLOAD", BRIDGE_MEMORY_RESULT_ERROR, "", "")
                            }
                        };
                        settle
                            .call::<_, ()>((id, ok, payload, err_code, task_id, log_path))
                            .map_err(|error| js_error(&ctx, error))
                    });
                    lock(&script.0.report).remove(id);
                    settled?;
                }
                Err(mpsc::TryRecvError::Disconnected) => return Ok(()),
                Err(mpsc::TryRecvError::Empty) => {
                    if !script.interrupted() {
                        std::thread::park();
                    }
                }
            }
        }
    })();
    runtime.set_host_promise_rejection_tracker(None);
    rejections.borrow_mut().clear();
    result
}

const PRELUDE: &str = include_str!("prelude.js");
